package temporal

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
)

func retainedClearActivityFixture(t *testing.T) (*types.ExecutionRequest, *enrollmentActivityDB, *database.ExecutionRecord) {
	t.Helper()
	req, db := activityEnrollmentFixture(t)
	req.Command, req.RunID = types.ClearDestination, "fresh-clear-run"
	req.Args[0] = string(types.ClearDestination)
	req.Configs = []types.JobConfig{{Name: "state.json", Data: db.job.State}}
	db.owner, db.run = req.WorkflowID, "old-run"
	record := &database.ExecutionRecord{
		Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: "old-run",
		RuntimeID: req.WorkflowID + "-old-runtime", LaunchID: req.WorkflowID + "-old-launch",
		LauncherAttemptID: "old-run/activity/1", ResourceID: "old-resource", ResourceUID: "old-uid",
		Phase: "closed", Outcome: "settled",
	}
	db.records = map[string]*database.ExecutionRecord{req.WorkflowID: record}
	old := *req
	executionRequestFromRecord(&old, record)
	_, oldDir := utils.GetExecutionDirAndSubDir(&old)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(oldDir)) })
	return req, db, record
}

func TestExplicitClearAfterKnownOrdinaryRetirementLaunchesFreshClearWithoutRecovery(t *testing.T) {
	for _, priorCommand := range []types.Command{types.Sync, types.ClearDestination} {
		for _, outcome := range []string{"settled", "not_started", "retired", "not_launched"} {
			t.Run(string(priorCommand)+"/"+outcome, func(t *testing.T) {
				req, db, record := retainedClearActivityFixture(t)
				if outcome == "not_launched" {
					record.Phase, record.Outcome = "unadmitted", ""
					record.RuntimeID, record.LaunchID, record.LauncherAttemptID, record.ResourceID, record.ResourceUID = "", "", "", "", ""
				} else {
					record.Outcome = outcome
				}
				previous := *record
				savedCheckpoint := db.job.State
				retirements, launches := 0, 0
				var retiredRequest types.ExecutionRequest
				a := &Activity{db: db, tempClient: &resyncTemporalClient{describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
					require.Equal(t, req.WorkflowID, id)
					require.Equal(t, "old-run", run)
					return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}}, nil
				}}, executor: &atomicActivityExecutor{
					retireForClear: func(ctx context.Context, old *types.ExecutionRequest) error {
						retirements++
						require.Equal(t, "old-run", old.RunID)
						require.Equal(t, previous.RuntimeID, old.RuntimeID)
						require.Equal(t, previous.LaunchID, old.LaunchID)
						require.Equal(t, previous.LauncherAttemptID, old.LauncherAttemptID)
						require.Equal(t, previous.ResourceID, old.ResourceID)
						require.Equal(t, previous.ResourceUID, old.ResourceUID)
						require.Equal(t, types.ClearDestination, old.Command, "the executor, not the new clear command, proves the old command")
						if outcome != "not_launched" {
							// Model the executor binding the immutable ordinary receipt before cleanup.
							old.Command = priorCommand
							require.NoError(t, utils.CloseExecutionFence(ctx, old))
						}
						record.Phase, record.Outcome = "closed", outcome
						retiredRequest = *old
						return nil
					},
					ordinaryCleanup: func(context.Context, *types.ExecutionRequest) error {
						t.Error("new clear admission must not use ordinary reconciliation")
						return errors.New("unexpected ordinary reconciliation")
					},
					ordinaryRecovery: func(context.Context, *types.ExecutionRequest) error {
						t.Error("known retirement must not replay CDC or prepare ordinary recovery")
						return errors.New("unexpected ordinary recovery")
					},
					execute: func(ctx context.Context, current *types.ExecutionRequest) (*types.ExecutorResponse, error) {
						launches++
						require.Equal(t, types.ClearDestination, current.Command)
						require.Equal(t, "fresh-clear-run", current.RunID)
						require.True(t, current.Controlled)
						require.True(t, current.AllowLaunch)
						require.False(t, current.RecoverOnly)
						require.NotEmpty(t, current.RuntimeID)
						require.NotEmpty(t, current.LaunchID)
						require.NotEqual(t, previous.RuntimeID, current.RuntimeID)
						require.NotEqual(t, previous.LaunchID, current.LaunchID)
						require.NotEqual(t, previous.LauncherAttemptID, current.LauncherAttemptID)
						require.Empty(t, current.ResourceID)
						require.Empty(t, current.ResourceUID)
						require.NotContains(t, current.Args, "--ordinary-recovery-origin")
						require.NotContains(t, current.Args, "--ordinary-recovery-result")
						require.NoError(t, current.BeforeLaunch(ctx), "the fresh clear has a separate open launch fence")
						require.Equal(t, savedCheckpoint, db.job.State, "adoption cannot fabricate or reset the old checkpoint")
						for _, config := range current.Configs {
							if config.Name == "state.json" {
								require.Equal(t, savedCheckpoint, config.Data)
							}
						}
						_, dir := utils.GetExecutionDirAndSubDir(current)
						t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
						return &types.ExecutorResponse{Response: "fresh clear launched"}, nil
					},
				}}
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestActivityEnvironment()
				env.RegisterActivity(a.ExecuteActivity)
				_, err := env.ExecuteActivity(a.ExecuteActivity, req)
				require.NoError(t, err, "an explicit clear must launch, not return PriorExecutionRetired")
				require.Equal(t, 1, retirements)
				require.Equal(t, 1, launches)
				require.Equal(t, req.RunID, db.run)
				require.Equal(t, "admitted", record.Phase)
				require.Empty(t, record.Outcome)
				require.Equal(t, savedCheckpoint, db.job.State)
				require.Zero(t, db.commits, "clear must not enroll tables or synthesize a reconciled catalog")
				if outcome != "not_launched" {
					require.NoError(t, utils.RequireClosedExecutionFence(context.Background(), &retiredRequest), "the fresh launch cannot reopen the old fence")
				}
			})
		}
	}
}

func TestExplicitClearRetainsOldOwnerWhenRetirementOrHandoffIsUnproven(t *testing.T) {
	for _, mode := range []string{
		"active old Temporal", "unknown Temporal", "mismatched Temporal", "Temporal unavailable",
		"missing receipt", "mismatched receipt", "unknown receipt", "retirement failure", "recovery proof missing",
		"handoff phase", "handed off phase", "handoff token", "handoff request", "handoff run", "active handoff",
	} {
		t.Run(mode, func(t *testing.T) {
			req, db, record := retainedClearActivityFixture(t)
			savedCheckpoint := db.job.State
			switch mode {
			case "handoff phase":
				record.Phase = "handoff"
			case "handed off phase":
				record.Phase = "handed_off"
			case "handoff token":
				record.HandoffToken = "unresolved-token"
			case "handoff request":
				record.HandoffRequestID = "unresolved-request"
			case "handoff run":
				record.HandoffRunID = "unresolved-run"
			case "active handoff":
				record.HandoffActive = true
			case "recovery proof missing":
				record.Outcome = "recovery"
			}
			previous := *record
			retirements, launches, reconciliations, recoveries := 0, 0, 0, 0
			a := &Activity{db: db, tempClient: &resyncTemporalClient{describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				require.Equal(t, req.WorkflowID, id)
				require.Equal(t, "old-run", run)
				if mode == "Temporal unavailable" {
					return nil, errors.New("Temporal unavailable")
				}
				status := enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
				if mode == "active old Temporal" {
					status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
				} else if mode == "unknown Temporal" {
					status = enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
				} else if mode == "mismatched Temporal" {
					run = "another-run"
				}
				return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: status}}, nil
			}}, executor: &atomicActivityExecutor{
				retireForClear: func(ctx context.Context, old *types.ExecutionRequest) error {
					retirements++
					switch mode {
					case "missing receipt":
						return os.ErrNotExist
					case "mismatched receipt":
						return errors.New("receipt identity does not match the old runtime and launch")
					case "unknown receipt":
						return errors.New("ordinary receipt outcome is unknown")
					case "retirement failure":
						return errors.New("exact provider resource retirement is unproven")
					case "recovery proof missing":
						return errors.New("settled helper retirement lacks original recovery proof")
					}
					// Even a successful executor retirement cannot bypass a DB handoff fence.
					return utils.CloseExecutionFence(ctx, old)
				},
				ordinaryCleanup: func(context.Context, *types.ExecutionRequest) error {
					reconciliations++
					return errors.New("unexpected ordinary reconciliation")
				},
				ordinaryRecovery: func(context.Context, *types.ExecutionRequest) error {
					recoveries++
					return errors.New("unexpected ordinary recovery")
				},
				execute: func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
					launches++
					return &types.ExecutorResponse{}, nil
				},
			}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ExecuteActivity)
			_, err := env.ExecuteActivity(a.ExecuteActivity, req)
			require.Error(t, err)
			if mode == "active old Temporal" || mode == "unknown Temporal" || mode == "mismatched Temporal" || mode == "Temporal unavailable" {
				require.Zero(t, retirements, "old Temporal retirement must precede resource or receipt work")
			} else {
				require.Equal(t, 1, retirements)
			}
			require.Zero(t, launches)
			require.Zero(t, reconciliations)
			require.Zero(t, recoveries, "clear cannot authorize a CDC replay when retirement is blocked")
			require.Equal(t, "old-run", db.run)
			require.Equal(t, previous, *record)
			require.Equal(t, savedCheckpoint, db.job.State)
			require.Zero(t, db.commits)
			require.Error(t, a.ReleaseJobExecutionActivity(context.Background(), req))
		})
	}
}
