package kubernetes

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestControlledKubernetesCanceledStartReattachesSamePendingPod(t *testing.T) {
	old := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, old) })
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := controlledPodRequest(types.Sync)
	req.AllowLaunch = false
	req.IndexRequired = true
	pod := controlledPod(t, k, req)
	pod.Status.Phase = corev1.PodPending
	if err := client.Tracker().Add(pod); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req.HeartbeatFunc = func(context.Context, ...interface{}) { cancel() }
	if _, err := k.Execute(ctx, req, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled monitor error=%v", err)
	}
	if req.ResourceUID != string(pod.UID) || req.ResourceID != pod.Name {
		t.Fatal("pending monitor lost immutable resource identity")
	}
	req.HeartbeatFunc = nil
	reads := 0
	client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		reads++
		current := pod.DeepCopy()
		if reads > 1 {
			current.Status.Phase = corev1.PodSucceeded
			current.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "connector", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
		}
		return true, current, nil
	})
	logs, err := k.Execute(context.Background(), req, t.TempDir())
	if err != nil || logs != "fake logs" {
		t.Fatalf("reattached monitor logs=%q error=%v", logs, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" || action.GetResource().Resource != "pods" {
			t.Fatalf("attachment mutated resources: %v", action)
		}
	}
}

func TestControlledKubernetesPendingAttachmentPreservesRetirementGuards(t *testing.T) {
	old := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, old) })
	for _, mode := range []string{"terminating", "replaced-uid", "authority-revoked"} {
		t.Run(mode, func(t *testing.T) {
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			req := controlledPodRequest(types.Sync)
			req.AllowLaunch = false
			req.IndexRequired = true
			pod := controlledPod(t, k, req)
			pod.Status.Phase = corev1.PodPending
			if mode == "terminating" {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			}
			if err := client.Tracker().Add(pod); err != nil {
				t.Fatal(err)
			}
			if mode == "authority-revoked" {
				req.RecordResource = func(context.Context, string, string) error { return errors.New("execution admission sealed") }
			}
			if mode == "replaced-uid" {
				reads := 0
				client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					reads++
					current := pod.DeepCopy()
					if reads > 1 {
						current.UID = "replacement-uid"
						current.Status.Phase = corev1.PodSucceeded
					}
					return true, current, nil
				})
			}
			if _, err := k.Execute(context.Background(), req, t.TempDir()); err == nil {
				t.Fatal("invalid attachment was accepted")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" || action.GetResource().Resource != "pods" {
					t.Fatalf("invalid attachment mutated resources: %v", action)
				}
			}
		})
	}
}
