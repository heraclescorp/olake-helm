package temporal

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestCancelledResyncChecksOriginalUnpublishedProofBeforeRecoveryAdmission(t *testing.T) {
	for _, confirmed := range []bool{true, false} {
		t.Run(fmt.Sprint(confirmed), func(t *testing.T) {
			req, db := activityEnrollmentFixture(t)
			req.Command, req.Args, req.RunID = types.Resync, atomicArgs(req.WorkflowID), "recovery-run"
			db.owner, db.run = req.WorkflowID, "cancelled-run"
			record := &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: "cancelled-run", Phase: "closing", RuntimeID: "original-runtime", LaunchID: "original-launch", ResourceID: "exact-pod", ResourceUID: "exact-uid"}
			db.records = map[string]*database.ExecutionRecord{req.WorkflowID: record}
			handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}}}
			remote := &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}, describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED}}, nil
			}}
			proofChecks, launches := 0, 0
			a := &Activity{db: db, tempClient: remote, executor: &atomicActivityExecutor{
				cleanupRequest: func(_ context.Context, old *types.ExecutionRequest, succeeded bool) error {
					if record.Phase == "closed" && record.Outcome == "retired" {
						return nil
					}
					proofChecks++
					require.False(t, succeeded)
					require.Equal(t, "cancelled-run", old.RunID)
					require.Equal(t, "original-runtime", old.RuntimeID)
					require.Equal(t, "original-launch", old.LaunchID)
					require.Equal(t, "exact-uid", old.ResourceUID)
					if !confirmed {
						return fmt.Errorf("%w: original receipt unavailable", executor.ErrResyncUnconfirmed)
					}
					record.Phase, record.Outcome = "closed", "not_published"
					return nil
				},
				retire: func(context.Context, *types.ExecutionRequest) error { return nil },
				execute: func(_ context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
					launches++
					require.False(t, confirmed, "positively unpublished cancellation cannot run another connector")
					require.True(t, request.RecoverOnly, "unknown original publication permits read-only recovery only")
					require.Equal(t, "original-launch", request.LaunchID)
					return nil, fmt.Errorf("UNKNOWN: no matching durable publication checkpoint")
				},
			}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ResyncActivity)
			_, err := env.ExecuteActivity(a.ResyncActivity, req)
			require.Error(t, err)
			require.Equal(t, 1, proofChecks)
			if confirmed {
				require.Contains(t, err.Error(), "PriorExecutionRetired")
				require.Zero(t, launches)
				require.NoError(t, a.PostResyncActivity(context.Background(), req, syncStatusFailed))
				require.NoError(t, a.ReleaseJobExecutionActivity(context.Background(), req))
				require.Empty(t, db.owner)
				require.True(t, handle.schedule.State.Paused, "retirement must not implicitly resume ingestion")
			} else {
				require.Equal(t, 1, launches)
				require.Error(t, a.ReleaseJobExecutionActivity(context.Background(), req))
				require.Equal(t, req.WorkflowID, db.owner)
			}
		})
	}
}
