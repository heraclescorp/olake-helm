package temporal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type resyncCleanupExecutor struct {
	cleanup func() error
}

func (e *resyncCleanupExecutor) Execute(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	panic("unexpected connector execution")
}

func (e *resyncCleanupExecutor) CleanupAndPersistState(context.Context, *types.ExecutionRequest) error {
	return e.cleanup()
}

type resyncScheduleHandle struct {
	client.ScheduleHandle
	mu                sync.Mutex
	schedule          client.Schedule
	pausedAfterUpdate []bool
	pausedBeforePatch []bool
	triggers          int
}

func (h *resyncScheduleHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	update, err := options.DoUpdate(client.ScheduleUpdateInput{
		Description: client.ScheduleDescription{Schedule: h.schedule},
	})
	if err != nil {
		return err
	}
	h.schedule = *update.Schedule
	h.pausedAfterUpdate = append(h.pausedAfterUpdate, h.schedule.State.Paused)
	return nil
}

func (h *resyncScheduleHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &client.ScheduleDescription{Schedule: h.schedule}, nil
}

func (h *resyncScheduleHandle) patch(_ context.Context, req *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pausedBeforePatch = append(h.pausedBeforePatch, h.schedule.State.Paused)
	if req.Patch.Unpause != "" {
		h.schedule.State.Paused = false
		h.schedule.State.Note = req.Patch.Unpause
	}
	if req.Patch.TriggerImmediately != nil {
		h.triggers++
	}
	return &workflowservice.PatchScheduleResponse{}, nil
}

type resyncScheduleClient struct {
	client.ScheduleClient
	handle client.ScheduleHandle
}

func (c *resyncScheduleClient) GetHandle(context.Context, string) client.ScheduleHandle {
	return c.handle
}

func TestClearWorkflowObservableScheduleSafety(t *testing.T) {
	clearFailed := temporal.NewNonRetryableApplicationError("clear failed", "clear", nil)
	cleanupFailed := temporal.NewNonRetryableApplicationError("state persistence failed", "cleanup", nil)
	for _, tc := range []struct {
		name                string
		resync              bool
		executeErr          error
		cleanupErr          error
		cancelDuringClear   bool
		cancelDuringCleanup bool
		wantPaused          bool
		wantTriggers        int
		wantError           bool
	}{
		{name: "resync success", resync: true, wantTriggers: 1},
		{name: "resync clear failure", resync: true, executeErr: clearFailed, wantPaused: true, wantError: true},
		{name: "resync clear cancellation", resync: true, cancelDuringClear: true, wantPaused: true, wantError: true},
		{name: "resync cleanup cancellation", resync: true, cancelDuringCleanup: true, wantPaused: true, wantError: true},
		{name: "resync cleanup failure", resync: true, cleanupErr: cleanupFailed, wantPaused: true, wantError: true},
		{name: "ordinary clear success"},
		{name: "ordinary clear failure", executeErr: clearFailed, wantError: true},
		{name: "ordinary clear cancellation", cancelDuringClear: true, wantError: true},
		{name: "ordinary clear cleanup failure", cleanupErr: cleanupFailed, wantPaused: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			spec := &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}}
			policy := &client.SchedulePolicies{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}
			handle := &resyncScheduleHandle{schedule: client.Schedule{
				Spec:   spec,
				Policy: policy,
				State:  &client.ScheduleState{Paused: true},
				Action: &client.ScheduleWorkflowAction{Args: []interface{}{&types.ExecutionRequest{Command: types.ClearDestination}}},
			}}
			activities := &Activity{
				executor: &resyncCleanupExecutor{cleanup: func() error {
					if tc.cancelDuringCleanup {
						env.CancelWorkflow()
					}
					return tc.cleanupErr
				}},
				tempClient: &resyncTemporalClient{
					service:   &resyncServiceClient{patch: handle.patch},
					schedules: &resyncScheduleClient{handle: handle},
				},
			}
			env.RegisterActivity(activities.ExecuteActivity)
			env.RegisterActivity(activities.PostClearActivity)
			env.RegisterActivity(activities.TriggerSyncAfterClearActivity)
			env.RegisterActivity(activities.SendWebhookNotificationActivity)
			clearCall := env.OnActivity(ExecuteActivity, mock.Anything, mock.Anything).
				Return(&types.ExecutorResponse{}, tc.executeErr).Once()
			if tc.cancelDuringClear {
				clearCall.After(time.Hour)
				env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
			}
			if tc.executeErr != nil {
				env.OnActivity(SendWebhookNotificationActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
			}

			env.ExecuteWorkflow(RunSyncWorkflow, resyncWorkflowArgs(tc.resync))
			require.True(t, env.IsWorkflowCompleted())
			if tc.wantError {
				require.Error(t, env.GetWorkflowError())
			} else {
				require.NoError(t, env.GetWorkflowError())
			}
			description, err := handle.Describe(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.wantPaused, description.Schedule.State.Paused)
			require.Equal(t, tc.wantTriggers, handle.triggers)
			require.Equal(t, spec, description.Schedule.Spec)
			require.Equal(t, policy, description.Schedule.Policy)
			if tc.cleanupErr != nil {
				require.Empty(t, handle.pausedAfterUpdate)
			} else {
				require.Equal(t, []bool{tc.resync}, handle.pausedAfterUpdate)
				action := description.Schedule.Action.(*client.ScheduleWorkflowAction)
				restored := action.Args[0].(*types.ExecutionRequest)
				require.Equal(t, types.Sync, restored.Command)
				require.False(t, restored.StartSyncAfterClear)
				if tc.resync {
					require.Empty(t, restored.TempPath)
				}
			}
			if tc.wantTriggers != 0 {
				require.Equal(t, []bool{true}, handle.pausedBeforePatch)
			} else {
				require.Empty(t, handle.pausedBeforePatch)
			}
			env.AssertExpectations(t)
		})
	}
}
