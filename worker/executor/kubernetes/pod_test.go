package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/failure"
)

func TestMemoryLimitKey(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		jobID     string
		want      string
	}{
		{"sync points at the job profile", "sync", "123", "global.jobProfiles.123.resources.limits.memory"},
		{"clear destination points at the job profile", "clear-destination", "123", "global.jobProfiles.123.resources.limits.memory"},
		{"discover points at the default profile", "discover", "123", "global.jobProfiles.0.resources.limits.memory"},
		{"check points at the default profile", "check", "0", "global.jobProfiles.0.resources.limits.memory"},
		{"missing job id points at the default profile", "sync", "", "global.jobProfiles.0.resources.limits.memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"olake.io/operation-type": tt.operation,
				"olake.io/job-id":         tt.jobID,
			}}}
			if got := memoryLimitKey(pod); got != tt.want {
				t.Errorf("memoryLimitKey() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestWaitForPodCompletionConnectorTermination(t *testing.T) {
	for _, sidecarFirst := range []bool{false, true} {
		order := "connector first"
		if sidecarFirst {
			order = "sidecar first"
		}
		for _, tt := range []struct {
			name       string
			exitCode   int32
			phase      corev1.PodPhase
			podReason  string
			podMessage string
			wantTip    string
		}{
			{name: "success with live sidecar", phase: corev1.PodRunning},
			{name: "failure with live sidecar", exitCode: 137, phase: corev1.PodRunning, wantTip: "memory"},
			{name: "memory eviction takes precedence", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "The node was low on resource: memory.", wantTip: "memory"},
			{name: "memory pressure eviction", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "The node had condition: [MemoryPressure].", wantTip: "memory"},
			{name: "ephemeral storage eviction overrides OOM termination", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "The node was low on resource: ephemeral-storage. Container connector was using more than its request.", wantTip: "storage"},
			{name: "local storage limit eviction", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "Pod ephemeral local storage usage exceeds the total limit of containers.", wantTip: "storage"},
			{name: "disk pressure overrides memory mention", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "The node had condition: [DiskPressure]. Memory usage was below its limit.", wantTip: "storage"},
			{name: "filesystem pressure eviction", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "The node was low on resource: nodefs.available.", wantTip: "storage"},
			{name: "unknown eviction overrides OOM termination", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", podMessage: "  Pod was evicted in response to imminent node shutdown.\n", wantTip: "neutral"},
			{name: "eviction without detail is neutral", exitCode: 137, phase: corev1.PodFailed, podReason: "Evicted", wantTip: "neutral"},
		} {
			t.Run(order+"/"+tt.name, func(t *testing.T) {
				connector := corev1.ContainerStatus{
					Name: "connector",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						ExitCode: tt.exitCode,
						Reason:   "OOMKilled",
						Message:  "connector exceeded its memory limit",
					}},
				}
				sidecar := corev1.ContainerStatus{
					Name:  "sidecar",
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}
				statuses := []corev1.ContainerStatus{connector, sidecar}
				if sidecarFirst {
					statuses = []corev1.ContainerStatus{sidecar, connector}
				}
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "operation", Namespace: "olake", UID: "original-uid"},
					Spec:       corev1.PodSpec{NodeName: "worker-node"},
					Status: corev1.PodStatus{
						Phase:             tt.phase,
						Reason:            tt.podReason,
						Message:           tt.podMessage,
						ContainerStatuses: statuses,
					},
				}
				k := newIndicatorExecutor(fake.NewClientset(pod))
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				err := k.waitForPodCompletion(ctx, pod.Name, string(pod.UID), time.Minute, nil)
				if tt.exitCode == 0 {
					if err != nil {
						t.Fatalf("successful connector did not complete: %v", err)
					}
					return
				}
				var executionFailure *failure.ExecutionFailure
				if !errors.As(err, &executionFailure) {
					t.Fatalf("connector failure = %v, want typed ExecutionFailure", err)
				}
				if !errors.Is(err, constants.ErrExecutionFailed) {
					t.Fatalf("connector failure does not unwrap to ErrExecutionFailed: %v", err)
				}
				if executionFailure.Kind != failure.KindPod || executionFailure.Name != pod.Name || executionFailure.Node != pod.Spec.NodeName {
					t.Errorf("wrong pod failure identity: %+v", executionFailure)
				}
				if executionFailure.ExitCode == nil || *executionFailure.ExitCode != int(tt.exitCode) {
					t.Errorf("exit code = %v, want connector exit code %d", executionFailure.ExitCode, tt.exitCode)
				}
				wantReason, wantMessage := connector.State.Terminated.Reason, connector.State.Terminated.Message
				if tt.podReason != "" {
					wantReason, wantMessage = tt.podReason, tt.podMessage
				}
				if executionFailure.Reason != wantReason || executionFailure.Message != wantMessage {
					t.Errorf("failure reason/message = %q/%q, want %q/%q", executionFailure.Reason, executionFailure.Message, wantReason, wantMessage)
				}
				message := failure.Describe(executionFailure)
				tip := strings.ToLower(message.Tip)
				switch tt.wantTip {
				case "memory":
					if !strings.Contains(tip, "memory") || strings.Contains(tip, "ephemeral-storage") {
						t.Errorf("memory failure recommendation = %q", message.Tip)
					}
				case "storage":
					if !strings.Contains(tip, "ephemeral-storage") || !strings.Contains(tip, "request") || !strings.Contains(tip, "disk capacity") || strings.Contains(tip, "memory") {
						t.Errorf("disk eviction recommendation = %q", message.Tip)
					}
				case "neutral":
					if tip == "" || strings.Contains(tip, "memory") || strings.Contains(tip, "ephemeral-storage") {
						t.Errorf("unknown eviction recommendation is not resource-neutral: %q", message.Tip)
					}
				}
				if tt.podReason == "Evicted" {
					if failure.TelemetryReason(executionFailure) != "evicted" || !strings.Contains(message.Headline, "evicted") {
						t.Errorf("eviction was reclassified using the container's OOM/137 status: %+v", message)
					}
					if !strings.Contains(message.Details, fmt.Sprintf("%q", tt.podMessage)) || !strings.Contains(err.Error(), tt.podMessage) {
						t.Errorf("eviction detail was lost: %+v; error: %v", message, err)
					}
				} else if failure.TelemetryReason(executionFailure) != "oom_killed" {
					t.Errorf("OOM termination telemetry = %q", failure.TelemetryReason(executionFailure))
				}
			})
		}
	}
}

func TestWaitForPodCompletionIgnoresOtherContainers(t *testing.T) {
	for _, tt := range []struct {
		name             string
		sidecarExitCode  int32
		connectorMissing bool
		phase            corev1.PodPhase
	}{
		{name: "live connector and successful sidecar", phase: corev1.PodRunning},
		{name: "live connector and failed sidecar", sidecarExitCode: 2, phase: corev1.PodRunning},
		{name: "missing connector status", connectorMissing: true, phase: corev1.PodRunning},
		{name: "pending connector", phase: corev1.PodPending},
	} {
		t.Run(tt.name, func(t *testing.T) {
			statuses := []corev1.ContainerStatus{{
				Name: "sidecar",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: tt.sidecarExitCode,
					Reason:   "sidecar stopped",
				}},
			}}
			if !tt.connectorMissing {
				state := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
				if tt.phase == corev1.PodPending {
					state = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
				}
				statuses = append(statuses, corev1.ContainerStatus{Name: "connector", State: state})
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "operation", Namespace: "olake", UID: "original-uid"},
				Status:     corev1.PodStatus{Phase: tt.phase, ContainerStatuses: statuses},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client := fake.NewClientset()
			// Cancel immediately after the status is read: no poll timers or sleeps.
			client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				cancel()
				return true, pod, nil
			})
			k := newIndicatorExecutor(client)

			if err := k.waitForPodCompletion(ctx, pod.Name, string(pod.UID), time.Minute, nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("uncompleted connector returned %v, want cancellation", err)
			}
		})
	}
}

func TestWaitForPodCompletionMissingPod(t *testing.T) {
	k := newIndicatorExecutor(fake.NewClientset())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := k.waitForPodCompletion(ctx, "operation", "original-uid", time.Minute, nil)
	var executionFailure *failure.ExecutionFailure
	if !errors.As(err, &executionFailure) || !executionFailure.NotFound {
		t.Fatalf("missing pod = %v, want typed not-found failure", err)
	}
	if executionFailure.Kind != failure.KindPod || executionFailure.Name != "operation" || executionFailure.ExitCode != nil {
		t.Errorf("wrong missing-pod failure: %+v", executionFailure)
	}
}

func TestWaitForPodCompletionRejectsReplacedPod(t *testing.T) {
	for _, uid := range []string{"replacement-uid", ""} {
		t.Run("UID="+uid, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "operation", Namespace: "olake"},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "connector",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
				}}},
			}
			pod.UID = types.UID(uid)
			k := newIndicatorExecutor(fake.NewClientset(pod))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := k.waitForPodCompletion(ctx, pod.Name, "original-uid", time.Minute, nil)
			if err == nil || !strings.Contains(err.Error(), "execution outcome unknown") || !strings.Contains(err.Error(), "UID changed") {
				t.Fatalf("replaced pod = %v, want unknown outcome for changed UID", err)
			}
			var executionFailure *failure.ExecutionFailure
			if errors.As(err, &executionFailure) {
				t.Fatalf("replacement pod was treated as the original connector's failure: %v", err)
			}
		})
	}
}
