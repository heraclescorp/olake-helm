package kubernetes

import (
	"context"
	"fmt"
	"strconv"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const retirementReservationKind = "retirement-reservation"
const retirementReservationGate = "olake.io/execution-retirement"

func (k *KubernetesExecutor) ReserveExecution(ctx context.Context, req *types.ExecutionRequest) (utils.ExecutionRetirementReservation, error) {
	proof := utils.ExecutionRetirementReservation{Version: 1, Kind: "kubernetes-name-reservation", WorkflowID: req.WorkflowID, RunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Namespace: k.namespace, Name: k.podResourceName(req)}
	if !req.PriorRunTerminal || !req.Controlled || !req.RecoverOnly || req.Command != types.Sync || req.RuntimeID == "" || req.RuntimeID == req.LaunchID || req.ResourceID != "" || req.ResourceUID != "" {
		return proof, fmt.Errorf("runtime reservation requires an unresolved ordinary recovery runtime")
	}
	if err := utils.RequireClosedExecutionFence(ctx, req); err != nil {
		return proof, err
	}
	annotations := map[string]string{
		"olake.io/resource-kind":          retirementReservationKind,
		"sidecar.istio.io/inject":         "false",
		"olake.io/retired-workflow-id":    req.WorkflowID,
		"olake.io/retired-owner-run-id":   req.RunID,
		"olake.io/retired-runtime-id":     req.RuntimeID,
		"olake.io/retired-launch-id":      req.LaunchID,
		"olake.io/retired-job-id":         strconv.Itoa(req.JobID),
		"olake.io/retired-operation-type": string(req.Command),
	}
	spec := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: proof.Name, Namespace: k.namespace, Annotations: annotations, Labels: map[string]string{"app": "olake-retirement-reservation", "olake.io/resource-kind": retirementReservationKind}, Finalizers: []string{retirementReservationGate}}, Spec: corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: new(false),
		SchedulingGates:              []corev1.PodSchedulingGate{{Name: retirementReservationGate}},
		Containers:                   []corev1.Container{{Name: "retirement-reservation", Image: "registry.k8s.io/pause:3.10"}},
	}}
	pod, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, spec, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		pod, err = k.client.CoreV1().Pods(k.namespace).Get(ctx, proof.Name, metav1.GetOptions{})
	}
	if err != nil {
		return proof, fmt.Errorf("reserve exact unresolved runtime name: %w", err)
	}
	if pod.UID == "" || pod.Name != proof.Name || pod.Namespace != k.namespace || pod.Spec.NodeName != "" || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != retirementReservationGate || pod.Spec.RestartPolicy != corev1.RestartPolicyNever || len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != "retirement-reservation" || pod.Spec.Containers[0].Image != "registry.k8s.io/pause:3.10" || len(pod.Spec.Volumes) != 0 || len(pod.OwnerReferences) != 0 || len(pod.Spec.InitContainers) != 0 || len(pod.Spec.EphemeralContainers) != 0 || len(pod.Spec.Containers[0].Env) != 0 || len(pod.Spec.Containers[0].EnvFrom) != 0 || len(pod.Spec.Containers[0].VolumeMounts) != 0 {
		return proof, fmt.Errorf("runtime name is not held by a nonexecuting retirement reservation")
	}
	retained := false
	for _, finalizer := range pod.Finalizers {
		retained = retained || finalizer == retirementReservationGate
	}
	if !retained || len(pod.Labels) != 2 || pod.Labels["app"] != "olake-retirement-reservation" || pod.Labels["olake.io/resource-kind"] != retirementReservationKind {
		return proof, fmt.Errorf("runtime reservation lacks isolated retained lifecycle")
	}
	for key, expected := range annotations {
		if pod.Annotations[key] != expected {
			return proof, fmt.Errorf("runtime reservation identity %s differs", key)
		}
	}
	proof.UID = string(pod.UID)
	return proof, nil
}
