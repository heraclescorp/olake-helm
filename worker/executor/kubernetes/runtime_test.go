package kubernetes

import (
	"context"
	"errors"
	"reflect"
	"strings"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func controlledPodRequest(command types.Command) *types.ExecutionRequest {
	return &types.ExecutionRequest{
		WorkflowID: "stable-operation", RunID: "owner-run", LaunchID: "data-launch", RuntimeID: "runtime-a", JobID: 42,
		Command: command, ConnectorType: "postgres", Version: "v1", AllowLaunch: true, Controlled: true, Timeout: time.Minute,
		BeforeLaunch:   func(context.Context) error { return nil },
		RecordResource: func(context.Context, string, string) error { return nil },
	}
}

func controlledPod(t *testing.T, k *KubernetesExecutor, req *types.ExecutionRequest) *corev1.Pod {
	t.Helper()
	pod, err := k.CreatePodSpec(req, "/tmp/runtime-artifacts", "connector-image", nil)
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "immutable-uid"
	return pod
}

func TestControlledKubernetesRecordsUIDImmediatelyAfterCreate(t *testing.T) {
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := controlledPodRequest(types.Resync)
	spec := controlledPod(t, k, req)
	var events []string
	req.BeforeLaunch = func(context.Context) error { events = append(events, "authorize"); return nil }
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		events = append(events, "create")
		action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).UID = "created-uid"
		return false, nil, nil
	})
	req.RecordResource = func(_ context.Context, name, uid string) error {
		events = append(events, "record")
		if name != spec.Name || uid != "created-uid" || len(client.Actions()) != 1 {
			t.Fatalf("resource not captured immediately: %s/%s, actions=%v", name, uid, client.Actions())
		}
		return nil
	}
	pod, err := k.createControlledPod(context.Background(), req, spec)
	if err != nil || pod.UID != "created-uid" || req.ResourceUID != "created-uid" || req.ResourceID != spec.Name {
		t.Fatalf("created pod=%v error=%v request identity=%s/%s", pod, err, req.ResourceID, req.ResourceUID)
	}
	if !reflect.DeepEqual(events, []string{"authorize", "create", "record"}) {
		t.Fatalf("launch events = %v", events)
	}
}

func TestControlledKubernetesCannotCreateAfterAdmissionCloses(t *testing.T) {
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := controlledPodRequest(types.ClearDestination)
	closed := errors.New("admission closed")
	req.BeforeLaunch = func(context.Context) error { return closed }
	if _, err := k.createControlledPod(context.Background(), req, controlledPod(t, k, req)); !errors.Is(err, closed) {
		t.Fatalf("closed create error = %v", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("closed launch called provider: %v", client.Actions())
	}
}

func TestControlledKubernetesLateCreateOnlyRecordsCapturedUID(t *testing.T) {
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := controlledPodRequest(types.Resync)
	spec := controlledPod(t, k, req)
	creating, completeCreate := make(chan struct{}), make(chan struct{})
	closed := errors.New("owner closed while provider create was in flight")
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		close(creating)
		<-completeCreate
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		pod.UID = "late-created-uid"
		return false, nil, nil
	})
	req.RecordResource = func(_ context.Context, name, uid string) error {
		if name != spec.Name || uid != "late-created-uid" {
			t.Errorf("late resource identity = %s/%s", name, uid)
		}
		return closed
	}
	result := make(chan error, 1)
	go func() { _, err := k.createControlledPod(context.Background(), req, spec); result <- err }()
	<-creating
	// Model retirement while Create is dispatched. The common positive runtime
	// fence has closed; no retry may send another create for this admitted runtime.
	close(completeCreate)
	if err := <-result; !errors.Is(err, closed) {
		t.Fatalf("late create error = %v", err)
	}
	if actions := client.Actions(); len(actions) != 1 || actions[0].GetVerb() != "create" {
		t.Fatalf("provider calls occurred before identity recording: %v", actions)
	}
}

func TestControlledKubernetesRetriesAreAttachOnly(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	for _, command := range []types.Command{types.Sync, types.Resync, types.ClearDestination} {
		for _, state := range []string{"missing", "created", "running", "succeeded", "failed"} {
			t.Run(string(command)+"/"+state, func(t *testing.T) {
				client := fake.NewClientset()
				k := newIndicatorExecutor(client)
				req := controlledPodRequest(command)
				req.AllowLaunch = false
				req.IndexRequired = true // Attach must not prepare or allocate a PVC.
				pod := controlledPod(t, k, req)
				if state != "missing" {
					pod.Status.Phase = corev1.PodPending
					if state == "running" {
						pod.Status.Phase = corev1.PodRunning
						pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "connector", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
					}
					if state == "succeeded" || state == "failed" {
						pod.Status.Phase = corev1.PodSucceeded
						code := int32(0)
						if state == "failed" {
							pod.Status.Phase, code = corev1.PodFailed, 1
						}
						pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "connector", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code}}}}
					}
					if err := client.Tracker().Add(pod); err != nil {
						t.Fatal(err)
					}
				}
				if state == "running" {
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
				}
				logs, err := k.Execute(context.Background(), req, t.TempDir())
				wantSuccess := state == "running" || state == "succeeded"
				if (err == nil) != wantSuccess {
					t.Fatalf("attach logs=%q error=%v", logs, err)
				}
				if wantSuccess && logs != "fake logs" {
					t.Fatalf("attached logs = %q", logs)
				}
				for _, action := range client.Actions() {
					if action.GetVerb() != "get" || action.GetResource().Resource != "pods" {
						t.Fatalf("attach mutated provider or prepared PVC: %v", action)
					}
				}
			})
		}
	}
}

func TestControlledKubernetesValidatesOwnershipBeforeAttach(t *testing.T) {
	for _, mismatch := range []string{"runtime", "uid", "workflow", "restart policy"} {
		t.Run(mismatch, func(t *testing.T) {
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			req := controlledPodRequest(types.Resync)
			req.AllowLaunch = false
			pod := controlledPod(t, k, req)
			switch mismatch {
			case "runtime":
				pod.Annotations["olake.io/runtime-id"] = "another-runtime"
			case "uid":
				req.ResourceID, req.ResourceUID = pod.Name, "different-uid"
			case "workflow":
				pod.Annotations["olake.io/workflow-id"] = "another-workflow"
			case "restart policy":
				pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
			}
			if err := client.Tracker().Add(pod); err != nil {
				t.Fatal(err)
			}
			recorded := false
			req.RecordResource = func(context.Context, string, string) error { recorded = true; return nil }
			if _, err := k.Execute(context.Background(), req, t.TempDir()); err == nil || recorded {
				t.Fatalf("foreign pod attached: err=%v recorded=%v", err, recorded)
			}
		})
	}
}

func TestControlledKubernetesCleanupUsesUIDPrecondition(t *testing.T) {
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := controlledPodRequest(types.Resync)
	pod := controlledPod(t, k, req)
	req.ResourceID, req.ResourceUID = pod.Name, string(pod.UID)
	if err := client.Tracker().Add(pod); err != nil {
		t.Fatal(err)
	}
	deleted := false
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleted = true
		del := action.(k8stesting.DeleteAction)
		precondition := del.GetDeleteOptions().Preconditions
		if del.GetName() != req.ResourceID || precondition == nil || precondition.UID == nil || string(*precondition.UID) != req.ResourceUID {
			t.Fatalf("delete lacks immutable identity: %#v", del)
		}
		return false, nil, nil
	})
	if err := k.Cleanup(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("original pod was not deleted")
	}
}

func TestControlledKubernetesCleanupCannotDeleteReplacementUID(t *testing.T) {
	for _, replaceDuringDelete := range []bool{false, true} {
		name := "replacement already exists"
		if replaceDuringDelete {
			name = "replacement races delete"
		}
		t.Run(name, func(t *testing.T) {
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			req := controlledPodRequest(types.Resync)
			old := controlledPod(t, k, req)
			req.ResourceID, req.ResourceUID = old.Name, string(old.UID)
			replacement := old.DeepCopy()
			replacement.UID = "replacement-uid"
			if replaceDuringDelete {
				if err := client.Tracker().Add(old); err != nil {
					t.Fatal(err)
				}
				client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					uid := action.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions.UID
					if uid == nil || string(*uid) != req.ResourceUID {
						t.Fatal("delete has no old UID precondition")
					}
					gvr := corev1.SchemeGroupVersion.WithResource("pods")
					if err := client.Tracker().Delete(gvr, k.namespace, old.Name); err != nil {
						t.Fatal(err)
					}
					if err := client.Tracker().Add(replacement); err != nil {
						t.Fatal(err)
					}
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, old.Name, errors.New("UID precondition does not match"))
				})
			} else if err := client.Tracker().Add(replacement); err != nil {
				t.Fatal(err)
			}
			err := k.Cleanup(context.Background(), req)
			if (err != nil) != replaceDuringDelete {
				t.Fatalf("cleanup error = %v", err)
			}
			current, err := client.CoreV1().Pods(k.namespace).Get(context.Background(), old.Name, metav1.GetOptions{})
			if err != nil || current.UID != replacement.UID {
				t.Fatalf("replacement was deleted: %v, %v", current, err)
			}
		})
	}
}

func TestKubernetesRuntimeConfigAndNamesPreserveStableWorkflow(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeS3)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	k := newIndicatorExecutor(fake.NewClientset())
	req := controlledPodRequest(types.Resync)
	req.RuntimeID = strings.Repeat("shared-prefix-", 10) + "a"
	next := *req
	next.RuntimeID = strings.TrimSuffix(req.RuntimeID, "a") + "b"
	pod := controlledPod(t, k, req)
	if pod.Name == k.podResourceName(&next) || len(pod.Name) > 63 {
		t.Fatalf("runtime name collision or invalid length: %q", pod.Name)
	}
	env := runtimeMemoryEnv(pod.Spec.Containers[0].Env)
	if env[constants.EnvS3ConfigFolder] != utils.ConnectorConfigDir(req.Command, req.RuntimeID) || env["OLAKE_WORKFLOW_ID"] != req.WorkflowID {
		t.Fatalf("runtime/stable env = %v", env)
	}
	if pod.Annotations["olake.io/workflow-id"] != req.WorkflowID || pod.Annotations["olake.io/runtime-id"] != req.RuntimeID {
		t.Fatalf("runtime ownership = %v", pod.Annotations)
	}
}

func TestSynchronousKubernetesNeedsNoAdmissionCallbacks(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	req := &types.ExecutionRequest{WorkflowID: "metadata-check", Command: types.Check, ConnectorType: "postgres", Version: "v1", Timeout: time.Minute}
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		pod.UID, pod.Status.Phase = "sync-uid", corev1.PodSucceeded
		return false, nil, nil
	})
	logs, err := k.Execute(context.Background(), req, t.TempDir())
	if err != nil || logs != "fake logs" {
		t.Fatalf("unmanaged check logs=%q error=%v", logs, err)
	}
	_, err = client.CoreV1().Pods(k.namespace).Get(context.Background(), k.sanitizeName(req.WorkflowID), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("synchronous resource not cleaned up: %v", err)
	}
}

func TestControlledKubernetesResolvesLostCreateAcknowledgementWithoutCallbacks(t *testing.T) {
	for _, state := range []string{"late resource", "missing", "foreign resource"} {
		t.Run(state, func(t *testing.T) {
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			req := controlledPodRequest(types.Resync)
			req.RecordResource = func(context.Context, string, string) error {
				t.Fatal("resolution must not re-enter SQL recorder")
				return nil
			}
			pod := controlledPod(t, k, req)
			if state == "foreign resource" {
				pod.Annotations["olake.io/runtime-id"] = "different-runtime"
			}
			if state != "missing" {
				if err := client.Tracker().Add(pod); err != nil {
					t.Fatal(err)
				}
			}
			err := k.ResolveResource(context.Background(), req)
			if state == "late resource" {
				if err != nil || req.ResourceID != pod.Name || req.ResourceUID != string(pod.UID) {
					t.Fatalf("resolved identity = %s/%s, err=%v", req.ResourceID, req.ResourceUID, err)
				}
			} else if err == nil || req.ResourceID != "" || req.ResourceUID != "" {
				t.Fatalf("unresolved identity was accepted: %s/%s, err=%v", req.ResourceID, req.ResourceUID, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("resolution mutated provider: %v", action)
				}
			}
		})
	}
}

func TestControlledKubernetesCleanupResolvesLostCreateAcknowledgement(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "late resource"
		if missing {
			name = "late resource not materialized"
		}
		t.Run(name, func(t *testing.T) {
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			req := controlledPodRequest(types.ClearDestination)
			req.RecordResource = func(context.Context, string, string) error {
				t.Fatal("cleanup must not re-enter SQL recorder")
				return nil
			}
			pod := controlledPod(t, k, req)
			if !missing {
				if err := client.Tracker().Add(pod); err != nil {
					t.Fatal(err)
				}
			}
			deleted := false
			client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				deleted = true
				preconditions := action.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions
				if preconditions == nil || preconditions.UID == nil || *preconditions.UID != pod.UID {
					t.Fatal("resolved cleanup omitted immutable UID")
				}
				return false, nil, nil
			})
			err := k.Cleanup(context.Background(), req)
			if missing {
				if err == nil || deleted {
					t.Fatalf("missing unresolved pod cleanup: err=%v deleted=%v", err, deleted)
				}
			} else if err != nil || !deleted || req.ResourceID != pod.Name || req.ResourceUID != string(pod.UID) {
				t.Fatalf("late cleanup: err=%v deleted=%v captured=%s/%s", err, deleted, req.ResourceID, req.ResourceUID)
			}
		})
	}
}

func TestKnownKubernetesIdentityNeedsNoResolutionProviderRequest(t *testing.T) {
	k := &KubernetesExecutor{}
	req := controlledPodRequest(types.Resync)
	req.ResourceID, req.ResourceUID = "captured-name", "captured-uid"
	if err := k.ResolveResource(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestControlledKubernetesWithoutRuntimeFailsBeforeProviderCalls(t *testing.T) {
	k := &KubernetesExecutor{}
	req := &types.ExecutionRequest{WorkflowID: "controlled", Command: types.ClearDestination, Controlled: true}
	if _, err := k.Execute(context.Background(), req, t.TempDir()); err == nil {
		t.Fatal("unadmitted asynchronous execution accepted")
	}
	if err := k.Cleanup(context.Background(), req); err == nil {
		t.Fatal("unadmitted asynchronous cleanup accepted")
	}
}
