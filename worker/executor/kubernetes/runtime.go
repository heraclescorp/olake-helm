package kubernetes

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

func (k *KubernetesExecutor) podResourceName(req *types.ExecutionRequest) string {
	if !req.Controlled || !utils.IsAsyncCommand(req.Command) || req.RuntimeID == "" {
		return k.sanitizeName(req.WorkflowID)
	}
	digest := sha256.Sum256([]byte(req.RuntimeID))
	// 224 digest bits fit the DNS label limit without truncating the identity.
	return fmt.Sprintf("olake-%x", digest[:28])
}

func validatePodOwnership(req *types.ExecutionRequest, pod *corev1.Pod) error {
	if pod.UID == "" || (req.ResourceID != "" && req.ResourceID != pod.Name) || (req.ResourceUID != "" && req.ResourceUID != string(pod.UID)) {
		return fmt.Errorf("execution outcome unknown: Kubernetes immutable resource identity does not match")
	}
	expected := map[string]string{
		"olake.io/workflow-id":    req.WorkflowID,
		"olake.io/runtime-id":     req.RuntimeID,
		"olake.io/launch-id":      req.LaunchID,
		"olake.io/job-id":         strconv.Itoa(req.JobID),
		"olake.io/operation-type": string(req.Command),
	}
	for key, value := range expected {
		if actual, present := pod.Annotations[key]; !present || actual != value {
			return fmt.Errorf("execution outcome unknown: Kubernetes ownership metadata %s does not match", key)
		}
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("execution outcome unknown: controlled Kubernetes resource permits container restarts")
	}
	return nil
}

func validatePodAttachment(pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil {
		return fmt.Errorf("execution outcome unknown: admitted Kubernetes resource is terminating")
	}
	// A pending pod with no evidence that the connector started is not proof of
	// an execution. Only the original Create caller may wait for its first start.
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "connector" && (status.State.Running != nil || status.State.Terminated != nil) {
			return nil
		}
	}
	return fmt.Errorf("execution outcome unknown: admitted Kubernetes resource has not completed a confirmed start")
}

func (k *KubernetesExecutor) findControlledPod(ctx context.Context, req *types.ExecutionRequest) (*corev1.Pod, error) {
	name := k.podResourceName(req)
	if req.ResourceID != "" {
		name = req.ResourceID
	}
	pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("execution outcome unknown: inspect Kubernetes resource: %w", err)
	}
	if err := validatePodOwnership(req, pod); err != nil {
		return nil, err
	}
	if req.RecordResource == nil {
		return nil, fmt.Errorf("execution outcome unknown: Kubernetes resource recorder is missing")
	}
	if err := req.RecordResource(ctx, pod.Name, string(pod.UID)); err != nil {
		return nil, err
	}
	req.ResourceID, req.ResourceUID = pod.Name, string(pod.UID)
	return pod, nil
}

func (k *KubernetesExecutor) createControlledPod(ctx context.Context, req *types.ExecutionRequest, spec *corev1.Pod) (*corev1.Pod, error) {
	if !req.AllowLaunch || req.BeforeLaunch == nil || req.RecordResource == nil {
		return nil, fmt.Errorf("execution outcome unknown: Kubernetes launch is not admitted")
	}
	if err := req.BeforeLaunch(ctx); err != nil {
		return nil, err
	}
	pod, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, spec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("execution outcome unknown: create admitted Kubernetes resource: %w", err)
	}
	// Persist the UID before logs, polling or any other provider request. A lost
	// acknowledgement is attach-only; the runtime's positive fence rejects late starts.
	if pod.UID == "" {
		return nil, fmt.Errorf("execution outcome unknown: Kubernetes create returned no UID")
	}
	if err := req.RecordResource(ctx, pod.Name, string(pod.UID)); err != nil {
		return nil, err
	}
	req.ResourceID, req.ResourceUID = pod.Name, string(pod.UID)
	return pod, nil
}

func (k *KubernetesExecutor) cleanupControlledPod(ctx context.Context, req *types.ExecutionRequest) error {
	name := req.ResourceID
	knownUID := req.ResourceUID != ""
	if name == "" {
		name = k.podResourceName(req)
	}
	pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if !knownUID {
			return fmt.Errorf("execution outcome unknown: admitted Kubernetes create may still materialize")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect Kubernetes cleanup resource: %w", err)
	}
	if knownUID && string(pod.UID) != req.ResourceUID {
		// The old UID is gone. Never delete the replacement, even at the same name.
		return nil
	}
	if err := validatePodOwnership(req, pod); err != nil {
		return err
	}
	// The caller holds the common SQL authority guard. Do not recursively call
	// RecordResource here; its owner persists this captured identity on closure.
	req.ResourceID, req.ResourceUID = pod.Name, string(pod.UID)
	uid := k8stypes.UID(req.ResourceUID)
	err = k.client.CoreV1().Pods(k.namespace).Delete(ctx, req.ResourceID, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete exact Kubernetes resource: %w", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Second*constants.ContainerCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, req.ResourceID, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return string(pod.UID) != req.ResourceUID, nil
	}); err != nil {
		return fmt.Errorf("Kubernetes resource termination is unconfirmed: %w", err)
	}
	return nil
}

// ResolveResource captures a lost Create acknowledgement without deleting or
// calling back into SQL. Its caller durably stores the UID before cleanup.
func (k *KubernetesExecutor) ResolveResource(ctx context.Context, req *types.ExecutionRequest) error {
	if !req.Controlled || !utils.IsAsyncCommand(req.Command) {
		return nil
	}
	if req.ResourceID != "" && req.ResourceUID != "" {
		return nil
	}
	if req.RuntimeID == "" {
		return fmt.Errorf("execution outcome unknown: Kubernetes resource resolution requires admitted runtime identity")
	}
	name := req.ResourceID
	if name == "" {
		name = k.podResourceName(req)
	}
	pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("execution outcome unknown: resolve admitted Kubernetes resource: %w", err)
	}
	if err := validatePodOwnership(req, pod); err != nil {
		return err
	}
	req.ResourceID, req.ResourceUID = pod.Name, string(pod.UID)
	return nil
}
