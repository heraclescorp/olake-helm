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
	"go.temporal.io/api/serviceerror"
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

func (e *resyncCleanupExecutor) CleanupResync(context.Context, *types.ExecutionRequest, bool) error {
	return e.cleanup()
}

type resyncScheduleHandle struct {
	client.ScheduleHandle
	mu                sync.Mutex
	schedule          client.Schedule
	pausedAfterUpdate []bool
	pausedBeforePatch []bool
	triggers          int
	updateErr         error
	patchErr          error
	loseFirstAck      bool
	accepted          map[string]bool
	requestIDs        []string
	triggerOverlaps   []enumspb.ScheduleOverlapPolicy
}

func (h *resyncScheduleHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.updateErr != nil {
		return h.updateErr
	}
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
	h.requestIDs = append(h.requestIDs, req.RequestId)
	if h.patchErr != nil {
		return nil, h.patchErr
	}
	if h.accepted[req.RequestId] {
		return &workflowservice.PatchScheduleResponse{}, nil
	}
	if h.accepted == nil {
		h.accepted = make(map[string]bool)
	}
	h.accepted[req.RequestId] = true
	h.pausedBeforePatch = append(h.pausedBeforePatch, h.schedule.State.Paused)
	if req.Patch.Unpause != "" {
		h.schedule.State.Paused = false
		h.schedule.State.Note = req.Patch.Unpause
	}
	if req.Patch.TriggerImmediately != nil {
		h.triggers++
		h.triggerOverlaps = append(h.triggerOverlaps, req.Patch.TriggerImmediately.OverlapPolicy)
	}
	if h.loseFirstAck {
		h.loseFirstAck = false
		return nil, serviceerror.NewUnavailable("handoff accepted but acknowledgement lost")
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
	terminal := temporal.NewNonRetryableApplicationError("clear failed", "clear", nil)
	for _, tc := range []struct {
		name       string
		executeErr error
		cleanupErr error
		cancel     bool
	}{
		{name: "ordinary clear success"},
		{name: "ordinary clear failure", executeErr: terminal},
		{name: "ordinary clear cancellation", cancel: true},
		{name: "ordinary clear cleanup failure", cleanupErr: terminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			spec := &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}}
			policy := &client.SchedulePolicies{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}
			handle := &resyncScheduleHandle{schedule: client.Schedule{Spec: spec, Policy: policy, State: &client.ScheduleState{Paused: true}}}
			a := &Activity{db: &enrollmentActivityDB{}, executor: &resyncCleanupExecutor{cleanup: func() error { return tc.cleanupErr }}, tempClient: &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}}}
			env.RegisterActivity(a.ExecuteActivity)
			env.RegisterActivity(a.PostClearActivity)
			env.RegisterActivity(a.ReleaseJobExecutionActivity)
			env.RegisterActivity(a.SendWebhookNotificationActivity)
			call := env.OnActivity(ExecuteActivity, mock.Anything, mock.Anything).Return(&types.ExecutorResponse{}, tc.executeErr)
			if tc.cancel {
				call.After(time.Hour)
				env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
			}
			if tc.executeErr != nil {
				env.OnActivity(SendWebhookNotificationActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
			}
			env.ExecuteWorkflow(RunSyncWorkflow, clearWorkflowArgs())
			if tc.executeErr != nil || tc.cleanupErr != nil || tc.cancel {
				require.Error(t, env.GetWorkflowError())
			} else {
				require.NoError(t, env.GetWorkflowError())
			}
			desc, err := handle.Describe(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.cleanupErr != nil, desc.Schedule.State.Paused)
			require.Equal(t, spec, desc.Schedule.Spec)
			require.Equal(t, policy, desc.Schedule.Policy)
			require.Zero(t, handle.triggers)
			if tc.cleanupErr == nil {
				require.Equal(t, types.Sync, desc.Schedule.Action.(*client.ScheduleWorkflowAction).Args[0].(*types.ExecutionRequest).Command)
			}
		})
	}
}
