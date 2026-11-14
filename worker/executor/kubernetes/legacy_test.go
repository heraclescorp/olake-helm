package kubernetes

import (
	"context"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestLegacyKubernetesOrdinaryAsyncCommandsNeedNoRuntime(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		t.Run(string(command), func(t *testing.T) {
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			req := &types.ExecutionRequest{WorkflowID: "ordinary-mysql-operation", Command: command, ConnectorType: "mysql", Version: "v1", Timeout: time.Minute}
			client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
				if pod.Name != k.sanitizeName(req.WorkflowID) || pod.Annotations["olake.io/runtime-id"] != "" {
					t.Fatalf("legacy pod acquired runtime identity: %s, %v", pod.Name, pod.Annotations)
				}
				pod.UID, pod.Status.Phase = "legacy-uid", corev1.PodSucceeded
				return false, nil, nil
			})
			logs, err := k.Execute(context.Background(), req, t.TempDir())
			if err != nil || logs != "fake logs" {
				t.Fatalf("legacy launch: logs=%q error=%v", logs, err)
			}
			if _, err := client.CoreV1().Pods(k.namespace).Get(context.Background(), k.sanitizeName(req.WorkflowID), metav1.GetOptions{}); err != nil {
				t.Fatalf("legacy async pod was prematurely deleted: %v", err)
			}
			deleted := false
			client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				deleted = true
				del := action.(k8stesting.DeleteAction)
				if del.GetName() != k.sanitizeName(req.WorkflowID) || del.GetDeleteOptions().Preconditions != nil {
					t.Fatalf("legacy cleanup did not preserve name-only behavior: %v", del)
				}
				return false, nil, nil
			})
			if err := k.Cleanup(context.Background(), req); err != nil || !deleted {
				t.Fatalf("legacy cleanup: error=%v deleted=%v", err, deleted)
			}
			_, err = client.CoreV1().Pods(k.namespace).Get(context.Background(), k.sanitizeName(req.WorkflowID), metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				t.Fatalf("legacy pod not removed: %v", err)
			}
		})
	}
}

func TestLegacyKubernetesRetryAdoptsExistingPodWithoutControlledMetadata(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := &types.ExecutionRequest{WorkflowID: "ordinary-existing", Command: types.Sync, ConnectorType: "mysql", Version: "v1", Timeout: time.Minute}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: k.sanitizeName(req.WorkflowID), Namespace: k.namespace, UID: "legacy-existing-uid"}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
	if err := client.Tracker().Add(pod); err != nil {
		t.Fatal(err)
	}
	logs, err := k.Execute(context.Background(), req, t.TempDir())
	if err != nil || logs != "fake logs" {
		t.Fatalf("legacy adoption: logs=%q error=%v", logs, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "create" {
			t.Fatalf("legacy retry mutated existing pod: %v", action)
		}
	}
}

func TestLegacyKubernetesConfigIgnoresUncontrolledRuntimeField(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeS3)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	k := newIndicatorExecutor(fake.NewClientset())
	req := &types.ExecutionRequest{WorkflowID: "ordinary-stable", Command: types.Sync, ConnectorType: "mysql", Version: "v1", RuntimeID: "not-an-admission"}
	pod, err := k.CreatePodSpec(req, "/tmp/ordinary-stable-artifacts", "connector-image", nil)
	if err != nil {
		t.Fatal(err)
	}
	env := runtimeMemoryEnv(pod.Spec.Containers[0].Env)
	if pod.Name != k.sanitizeName(req.WorkflowID) || env[constants.EnvS3ConfigFolder] != utils.ConnectorConfigDir(req.Command, req.WorkflowID) || pod.Annotations["olake.io/runtime-id"] != "" {
		t.Fatalf("uncontrolled request acquired runtime identity: name=%s env=%v annotations=%v", pod.Name, env, pod.Annotations)
	}
}

func TestLegacyKubernetesResolveIsNoOpAndResyncIsRejected(t *testing.T) {
	k := &KubernetesExecutor{}
	if err := k.ResolveResource(context.Background(), &types.ExecutionRequest{Command: types.Sync}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Execute(context.Background(), &types.ExecutionRequest{Command: types.Resync}, ""); err == nil {
		t.Fatal("legacy resync accepted")
	}
}
