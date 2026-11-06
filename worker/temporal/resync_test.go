package temporal

import (
	"context"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc"
)

func resyncWorkflowArgs(startSync bool) map[string]interface{} {
	return map[string]interface{}{
		"command":                string(types.ClearDestination),
		"project_id":             "123",
		"job_id":                 42,
		"temp_path":              "table-resync/state.json",
		"start_sync_after_clear": startSync,
	}
}

func TestClearFollowupRequiresSuccessfulClearAndCleanup(t *testing.T) {
	for _, tc := range []struct {
		name                string
		startSync           bool
		executeErr          error
		cleanupErr          error
		cancelDuringClear   bool
		cancelDuringCleanup bool
		wantTrigger         bool
		wantError           bool
	}{
		{name: "successful chain", startSync: true, wantTrigger: true},
		{name: "ordinary clear"},
		{name: "failed clear", startSync: true, executeErr: temporal.NewNonRetryableApplicationError("clear failed", "clear", nil), wantError: true},
		{name: "cancelled clear", startSync: true, cancelDuringClear: true, wantError: true},
		{name: "cleanup or state persistence failed", startSync: true, cleanupErr: temporal.NewNonRetryableApplicationError("state persistence failed", "cleanup", nil), wantError: true},
		{name: "cancelled during cleanup", startSync: true, cancelDuringCleanup: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			activities := &Activity{}
			env.RegisterActivity(activities.ExecuteActivity)
			env.RegisterActivity(activities.PostClearActivity)
			env.RegisterActivity(activities.TriggerSyncAfterClearActivity)
			env.RegisterActivity(activities.SendWebhookNotificationActivity)

			clearCall := env.OnActivity(ExecuteActivity, mock.Anything, mock.Anything).
				Return(&types.ExecutorResponse{Response: "cleared"}, tc.executeErr).Once()
			if tc.cancelDuringClear {
				clearCall.After(time.Hour)
				env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
			}

			cleanupDone := false
			cleanupCall := env.OnActivity(PostClearActivity, mock.Anything, mock.Anything).
				Return(func(context.Context, *types.ExecutionRequest) error {
					cleanupDone = true
					return tc.cleanupErr
				}).Once()
			if tc.cancelDuringCleanup {
				cleanupCall.After(time.Hour)
				env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
			}

			if tc.executeErr != nil {
				env.OnActivity(SendWebhookNotificationActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
			}
			if tc.wantTrigger {
				env.OnActivity(TriggerSyncAfterClearActivity, mock.Anything, mock.Anything).
					Return(func(_ context.Context, req *types.ExecutionRequest) error {
						require.True(t, cleanupDone)
						require.Equal(t, types.ClearDestination, req.Command)
						require.True(t, req.StartSyncAfterClear)
						return nil
					}).Once()
			}

			env.ExecuteWorkflow(RunSyncWorkflow, resyncWorkflowArgs(tc.startSync))
			require.True(t, env.IsWorkflowCompleted())
			if tc.wantError {
				require.Error(t, env.GetWorkflowError())
			} else {
				require.NoError(t, env.GetWorkflowError())
			}
			if !tc.wantTrigger {
				env.AssertActivityNotCalled(t, TriggerSyncAfterClearActivity, mock.Anything, mock.Anything)
			}
			env.AssertExpectations(t)
		})
	}
}

func TestClearCleanupRetriesBeforeSingleFollowup(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	activities := &Activity{}
	env.RegisterActivity(activities.ExecuteActivity)
	env.RegisterActivity(activities.PostClearActivity)
	env.RegisterActivity(activities.TriggerSyncAfterClearActivity)
	env.OnActivity(ExecuteActivity, mock.Anything, mock.Anything).
		Return(&types.ExecutorResponse{}, nil).Once()
	env.OnActivity(PostClearActivity, mock.Anything, mock.Anything).
		Return(serviceerror.NewUnavailable("database unavailable")).Once()
	env.OnActivity(PostClearActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(TriggerSyncAfterClearActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(RunSyncWorkflow, resyncWorkflowArgs(true))
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

type resyncServiceClient struct {
	workflowservice.WorkflowServiceClient
	patch func(context.Context, *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error)
}

func (c *resyncServiceClient) PatchSchedule(ctx context.Context, req *workflowservice.PatchScheduleRequest, _ ...grpc.CallOption) (*workflowservice.PatchScheduleResponse, error) {
	return c.patch(ctx, req)
}

type resyncTemporalClient struct {
	client.Client
	service   workflowservice.WorkflowServiceClient
	schedules client.ScheduleClient
}

func (c *resyncTemporalClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.service
}

func (c *resyncTemporalClient) ScheduleClient() client.ScheduleClient {
	return c.schedules
}

func TestClearFollowupRetriesUseSameScheduleRequestID(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	requests := make([]*workflowservice.PatchScheduleRequest, 0, 2)
	accepted := make(map[string]bool)
	service := &resyncServiceClient{
		patch: func(_ context.Context, req *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error) {
			requests = append(requests, req)
			accepted[req.RequestId] = true
			if len(requests) == 1 {
				return nil, serviceerror.NewUnavailable("accepted but acknowledgement lost")
			}
			return &workflowservice.PatchScheduleResponse{}, nil
		},
	}
	activities := &Activity{tempClient: &resyncTemporalClient{service: service}}
	env.RegisterActivity(activities.ExecuteActivity)
	env.RegisterActivity(activities.PostClearActivity)
	env.RegisterActivity(activities.TriggerSyncAfterClearActivity)
	env.OnActivity(ExecuteActivity, mock.Anything, mock.Anything).
		Return(&types.ExecutorResponse{}, nil).Once()
	env.OnActivity(PostClearActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(RunSyncWorkflow, resyncWorkflowArgs(true))
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, requests, 2)
	require.Len(t, accepted, 1)
	require.NotEmpty(t, requests[0].RequestId)
	require.Equal(t, requests[0].RequestId, requests[1].RequestId)
	_, scheduleID := utils.SyncWorkflowAndScheduleID("123", 42)
	for _, req := range requests {
		require.Equal(t, scheduleID, req.ScheduleId)
		require.Equal(t, utils.GetTemporalNamespace(), req.Namespace)
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, req.Patch.TriggerImmediately.OverlapPolicy)
		require.Empty(t, req.Patch.Pause)
		require.NotEmpty(t, req.Patch.Unpause)
	}
	env.AssertExpectations(t)
}

func TestCancelledClearFollowupDoesNotSendPatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activities := &Activity{}
	err := activities.TriggerSyncAfterClearActivity(ctx, &types.ExecutionRequest{StartSyncAfterClear: true})
	require.ErrorIs(t, err, context.Canceled)
}
