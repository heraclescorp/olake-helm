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

type ordinaryRecoveryActivityDB struct {
	*enrollmentActivityDB
	originAuthorized bool
	adoptions        int
}

func (db *ordinaryRecoveryActivityDB) AdoptExecution(ctx context.Context, old, newer *types.ExecutionRequest) error {
	if !db.originAuthorized {
		return db.enrollmentActivityDB.AdoptExecution(ctx, old, newer)
	}
	r := db.records[old.WorkflowID]
	if r.OwnerRunID != old.RunID || db.run != old.RunID || old.ResourceID != r.ResourceID || old.ResourceUID != r.ResourceUID {
		return database.ErrJobExecutionOwned
	}
	db.adoptions++
	r.OwnerRunID, r.Phase, r.Outcome = newer.RunID, "unadmitted", "recovery"
	r.RuntimeID, r.LauncherAttemptID, r.ResourceID, r.ResourceUID = "", "", "", ""
	db.run = newer.RunID
	return nil
}

func TestOrdinaryUnknownRecoveryAdoptsOnlyAfterTerminalEpochAndProofPreparation(t *testing.T) {
	for _, mode := range []string{"supported recovery", "active old epoch", "retirement unproven", "unsupported clear", "recovery connector failed"} {
		t.Run(mode, func(t *testing.T) {
			req, base := activityEnrollmentFixture(t)
			req.Command, req.RunID = types.Sync, "recovery-run"
			if mode == "unsupported clear" {
				req.Command = types.ClearDestination
			}
			db := &ordinaryRecoveryActivityDB{enrollmentActivityDB: base}
			db.owner, db.run = req.WorkflowID, "original-run"
			record := &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: "original-run", RuntimeID: "original-runtime", LaunchID: "original-runtime", ResourceID: "original-pod", ResourceUID: "original-uid", Phase: "closing"}
			db.records = map[string]*database.ExecutionRecord{req.WorkflowID: record}
			handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: false}}}
			status := enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
			if mode == "active old epoch" {
				status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			}
			tc := &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}, describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				require.Equal(t, "original-run", run)
				return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: status}}, nil
			}}
			prepared, launches := 0, 0
			a := &Activity{db: db, tempClient: tc, executor: &atomicActivityExecutor{
				ordinaryCleanup: func(_ context.Context, current *types.ExecutionRequest) error {
					if current.RunID == "original-run" {
						return errors.New("original mutation receipt remains unknown")
					}
					record.Phase, record.Outcome = "closed", "settled"
					return nil
				},
				ordinaryRecovery: func(_ context.Context, old *types.ExecutionRequest) error {
					prepared++
					require.Equal(t, "original-runtime", old.RuntimeID)
					require.Equal(t, "original-runtime", old.LaunchID)
					require.Equal(t, "original-pod", old.ResourceID)
					require.Equal(t, "original-uid", old.ResourceUID)
					if mode == "retirement unproven" {
						return errors.New("exact original UID retirement unproven")
					}
					db.originAuthorized = true
					return nil
				},
				execute: func(ctx context.Context, current *types.ExecutionRequest) (*types.ExecutorResponse, error) {
					launches++
					require.Equal(t, types.Sync, current.Command)
					require.True(t, current.Controlled)
					require.True(t, current.RecoverOnly)
					require.NotEqual(t, current.LaunchID, current.RuntimeID)
					require.Equal(t, "original-runtime", current.LaunchID)
					require.Contains(t, current.Args, "--ordinary-recovery-origin")
					require.Contains(t, current.Args, "--ordinary-recovery-result")
					require.True(t, handle.schedule.State.Paused, "pause is acknowledged before any recovery connector launch")
					require.Zero(t, db.commits, "recovery must never enroll new tables")
					if mode == "recovery connector failed" {
						return nil, errors.New("native table metadata is not provable")
					}
					return &types.ExecutorResponse{}, nil
				},
			}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ExecuteActivity)
			_, err := env.ExecuteActivity(a.ExecuteActivity, req)
			if mode == "supported recovery" {
				require.NoError(t, err)
				require.Equal(t, 1, prepared)
				require.Equal(t, 1, launches)
				require.Equal(t, 1, db.adoptions)
				require.Equal(t, "recovery", record.Outcome)
				require.NoError(t, a.PostSyncActivity(context.Background(), req, syncStatusSuccess))
				require.True(t, handle.schedule.State.Paused, "proof-backed retirement must not auto-resume ordinary schedule")
			} else {
				require.Error(t, err)
				if mode == "recovery connector failed" {
					require.Equal(t, 1, launches)
					require.Equal(t, "recovery-run", db.run)
				} else {
					require.Zero(t, launches)
					require.Zero(t, db.adoptions)
					require.Equal(t, "original-run", db.run)
				}
			}
		})
	}
}
