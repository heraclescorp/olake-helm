package kubernetes

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func retirementRequest(t *testing.T) *types.ExecutionRequest {
	t.Helper()
	req := controlledPodRequest(types.Sync)
	req.RuntimeID = strings.ReplaceAll(t.Name(), "/", "-")
	req.WorkflowID = req.RuntimeID + "-workflow"
	req.RecoverOnly, req.PriorRunTerminal = true, true
	_, dir := utils.GetExecutionDirAndSubDir(req)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	require.NoError(t, utils.CloseExecutionFence(context.Background(), req))
	return req
}

func TestRetirementReservationBlocksLateCreateAndCannotAttachOrCleanAsConnector(t *testing.T) {
	ctx := context.Background()
	req := retirementRequest(t)
	client := fake.NewClientset()
	recorded := 0
	req.RecordResource = func(context.Context, string, string) error { recorded++; return nil }
	k := newIndicatorExecutor(client)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).UID = "reservation-uid"
		return false, nil, nil
	})
	late := controlledPod(t, k, req)
	proof, err := k.ReserveExecution(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "reservation-uid", proof.UID)
	require.Empty(t, req.ResourceID)
	require.Empty(t, req.ResourceUID)
	barrier, err := client.CoreV1().Pods(k.namespace).Get(ctx, proof.Name, metav1.GetOptions{})
	require.NoError(t, err)
	legacyView := barrier.DeepCopy()
	delete(legacyView.Annotations, "olake.io/resource-kind")
	delete(legacyView.Labels, "olake.io/resource-kind")
	require.Error(t, validatePodOwnership(req, legacyView), "the ownership predicate predating reservation markers must reject the barrier")
	_, err = client.CoreV1().Pods(k.namespace).Create(ctx, late, metav1.CreateOptions{})
	require.True(t, apierrors.IsAlreadyExists(err))
	_, err = k.findControlledPod(ctx, req)
	require.ErrorContains(t, err, "nonexecuting retirement reservation")
	require.Error(t, k.ResolveResource(ctx, req))
	require.Error(t, k.cleanupControlledPod(ctx, req))
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb())
	}
	require.Zero(t, recorded, "no attempt may record the reservation UID as the original connector UID")
	require.NoError(t, utils.WriteExecutionRetirementReservation(ctx, req, proof))
	stored, err := utils.ReadExecutionRetirementReservation(ctx, req)
	require.NoError(t, err)
	require.Equal(t, proof, *stored)
	wrong := proof
	wrong.UID = "replacement"
	require.Error(t, utils.WriteExecutionRetirementReservation(ctx, req, wrong))
}

func TestRetirementReservationLostCreateAcknowledgementIsReconciledWithoutOriginalUID(t *testing.T) {
	ctx := context.Background()
	req := retirementRequest(t)
	client := fake.NewClientset()
	k := newIndicatorExecutor(client)
	lost := errors.New("reservation create acknowledgement lost")
	creates := 0
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		creates++
		if creates != 1 {
			return false, nil, nil
		}
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "durable-reservation-uid"
		require.NoError(t, client.Tracker().Add(pod))
		return true, nil, lost
	})
	_, err := k.ReserveExecution(ctx, req)
	require.ErrorIs(t, err, lost)
	proof, err := k.ReserveExecution(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "durable-reservation-uid", proof.UID)
	require.Empty(t, req.ResourceID)
	require.Empty(t, req.ResourceUID)
}

func TestRetirementReservationNeverReplacesExistingPodOrWeakensItsGate(t *testing.T) {
	for _, mode := range []string{"original", "foreign", "gate removed", "finalizer removed", "node assigned", "connector label"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			req := retirementRequest(t)
			client := fake.NewClientset()
			k := newIndicatorExecutor(client)
			if mode == "original" || mode == "foreign" {
				pod := controlledPod(t, k, req)
				if mode == "foreign" {
					pod.Annotations["olake.io/runtime-id"] = "foreign"
				}
				require.NoError(t, client.Tracker().Add(pod))
			} else {
				client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
					pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
					pod.UID = "reservation-uid"
					switch mode {
					case "gate removed":
						pod.Spec.SchedulingGates = nil
					case "finalizer removed":
						pod.Finalizers = nil
					case "node assigned":
						pod.Spec.NodeName = "worker"
					case "connector label":
						pod.Labels = map[string]string{"app": "olake"}
					}
					return false, nil, nil
				})
			}
			_, err := k.ReserveExecution(ctx, req)
			require.Error(t, err)
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb())
			}
			require.Empty(t, req.ResourceUID)
		})
	}
}
