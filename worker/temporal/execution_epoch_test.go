package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestEpochAdoptionRequiresExactOldTemporalRunToBeTerminalBeforeCleanup(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		status                            enumspb.WorkflowExecutionStatus
		mismatch, unavailable, emptyEpoch bool
		retire                            bool
	}{
		{name: "running", status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		{name: "unknown", status: enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED},
		{name: "mismatched response", status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, mismatch: true},
		{name: "Temporal unavailable", unavailable: true},
		{name: "legacy empty epoch", emptyEpoch: true},
		{name: "unrecognized future status", status: enumspb.WorkflowExecutionStatus(99)},
		{name: "positively terminated", status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, retire: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, db := activityEnrollmentFixture(t)
			req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
			req.RunID = "new-run"
			oldRun := "old-run"
			if tc.emptyEpoch {
				oldRun = ""
			}
			db.owner, db.run = req.WorkflowID, oldRun
			db.records = map[string]*database.ExecutionRecord{req.WorkflowID: {Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: oldRun, RuntimeID: "old-runtime", LaunchID: "original-launch", ResourceID: "immutable-resource", ResourceUID: "immutable-uid", Phase: "admitted"}}
			described, retired := 0, 0
			temporalClient := &resyncTemporalClient{describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				described++
				require.Equal(t, req.WorkflowID, id)
				require.Equal(t, "old-run", run)
				if tc.unavailable {
					return nil, errors.New("Temporal unavailable")
				}
				actualRun := run
				if tc.mismatch {
					actualRun = "another-run"
				}
				return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: actualRun}, Status: tc.status}}, nil
			}}
			a := &Activity{db: db, tempClient: temporalClient, executor: &atomicActivityExecutor{retire: func(_ context.Context, old *types.ExecutionRequest) error {
				retired++
				require.Equal(t, "old-run", old.RunID)
				require.Equal(t, "old-runtime", old.RuntimeID)
				require.Equal(t, "original-launch", old.LaunchID)
				require.Equal(t, "immutable-resource", old.ResourceID)
				require.Equal(t, "immutable-uid", old.ResourceUID)
				return errors.New("exact resource termination is still unknown")
			}}}
			require.Error(t, a.acquireJobExecution(context.Background(), req))
			if tc.retire {
				require.Equal(t, 1, retired)
			} else {
				require.Zero(t, retired)
			}
			if tc.emptyEpoch {
				require.Zero(t, described)
			} else {
				require.Equal(t, 1, described)
			}
			require.Equal(t, oldRun, db.run, "unproven retirement must retain the old controlling epoch")
		})
	}
}

func TestOrdinaryResetSafelyRetiresKnownFailureWithoutAnyDataRelaunch(t *testing.T) {
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		for _, outcome := range []string{"not_started", "settled", "unknown"} {
			t.Run(string(command)+"/"+outcome, func(t *testing.T) {
				req, db := activityEnrollmentFixture(t)
				req.Command = command
				req.RunID = "retirement-run"
				db.owner, db.run = req.WorkflowID, "old-run"
				record := &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: "old-run", RuntimeID: "old-runtime", LaunchID: "old-runtime", ResourceID: "immutable-old-resource", Phase: "admitted"}
				db.records = map[string]*database.ExecutionRecord{req.WorkflowID: record}
				handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}}}
				temporalClient := &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}, describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
					return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}}, nil
				}}
				retirements := 0
				a := &Activity{db: db, tempClient: temporalClient, executor: &atomicActivityExecutor{
					execute: func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
						t.Fatal("retirement reset must never launch another data process")
						return nil, nil
					},
					retire: func(context.Context, *types.ExecutionRequest) error {
						t.Fatal("ordinary retirement must validate terminal receipt, not generic resource deletion")
						return nil
					},
					ordinaryCleanup: func(_ context.Context, current *types.ExecutionRequest) error {
						if current.RunID == "retirement-run" && record.Phase == "closed" && record.Outcome == "retired" {
							return nil
						}
						require.Equal(t, "old-run", current.RunID)
						require.Equal(t, "old-runtime", current.RuntimeID)
						require.Equal(t, "immutable-old-resource", current.ResourceID)
						retirements++
						if outcome == "unknown" {
							return errors.New("terminal source receipt is unknown")
						}
						record.Phase, record.Outcome = "closed", outcome
						return nil
					},
				}}
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestActivityEnvironment()
				var err error
				if command == types.Sync {
					env.RegisterActivity(a.SyncActivity)
					_, err = env.ExecuteActivity(a.SyncActivity, req)
				} else {
					env.RegisterActivity(a.ExecuteActivity)
					_, err = env.ExecuteActivity(a.ExecuteActivity, req)
				}
				require.Error(t, err)
				require.Equal(t, 1, retirements)
				if outcome == "unknown" {
					require.Equal(t, "old-run", db.run)
					require.Error(t, a.ReleaseJobExecutionActivity(context.Background(), req))
					return
				}
				require.Contains(t, err.Error(), "PriorExecutionRetired")
				require.Equal(t, "retirement-run", db.run)
				require.Equal(t, "closed", record.Phase)
				require.Equal(t, "retired", record.Outcome)
				if command == types.Sync {
					require.NoError(t, a.PostSyncActivity(context.Background(), req, syncStatusFailed))
				} else {
					require.NoError(t, a.PostClearActivity(context.Background(), req))
				}
				require.NoError(t, a.ReleaseJobExecutionActivity(context.Background(), req))
				require.Empty(t, db.owner)
				fresh := *req
				fresh.WorkflowID += "-fresh-resync"
				fresh.RunID = "fresh-run"
				fresh.Command = types.Resync
				require.NoError(t, db.AcquireJobExecution(context.Background(), &fresh), "safely retired TOAST/auth failures must permit a fresh table resync")
			})
		}
	}
}

func TestExitZeroUnknownCleanupPausesScheduleAndRetainsOwnerAndCheckpoint(t *testing.T) {
	req, db := activityEnrollmentFixture(t)
	req.Controlled = true
	saved := db.job.State
	db.owner, db.run = req.WorkflowID, req.RunID
	record := &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: "runtime", LaunchID: "runtime", Phase: "closing"}
	db.records = map[string]*database.ExecutionRecord{req.WorkflowID: record}
	handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: false}}}
	a := &Activity{db: db, tempClient: &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}}, executor: &atomicActivityExecutor{ordinaryCleanup: func(context.Context, *types.ExecutionRequest) error {
		return errors.New("execution-result outcome unknown despite backend exit zero")
	}}}
	require.Error(t, a.PostSyncActivity(context.Background(), req, syncStatusSuccess))
	require.True(t, handle.schedule.State.Paused)
	require.Equal(t, saved, db.job.State)
	require.Equal(t, req.WorkflowID, db.owner)
	require.Equal(t, "closing", record.Phase)
	require.Error(t, a.ReleaseJobExecutionActivity(context.Background(), req))
}
