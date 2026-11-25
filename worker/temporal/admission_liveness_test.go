package temporal

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestOverlappingRecoveryAttemptsAcceptAlreadyAdoptedExactRun(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "same successor adopted", true: "canceled successor"}[canceled], func(t *testing.T) {
			req, db := activityEnrollmentFixture(t)
			req.RunID = "same-successor-run"
			db.owner, db.run = req.WorkflowID, "old-run"
			r := &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: "old-run", RuntimeID: "old-runtime", LaunchID: "original-launch", Outcome: "recovery", Phase: "closing"}
			db.records = map[string]*database.ExecutionRecord{req.WorkflowID: r}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			remote := &resyncTemporalClient{describe: func(_ context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED}}, nil
			}}
			preparations := 0
			a := &Activity{db: db, tempClient: remote, executor: &atomicActivityExecutor{ordinaryCleanup: func(context.Context, *types.ExecutionRequest) error {
				db.run, r.OwnerRunID, r.Phase = req.RunID, req.RunID, "unadmitted"
				r.RuntimeID = ""
				if canceled {
					cancel()
				}
				return database.ErrJobExecutionOwned
			}, ordinaryRecovery: func(context.Context, *types.ExecutionRequest) error {
				preparations++
				return database.ErrJobExecutionOwned
			}}}
			err := a.acquireJobExecution(ctx, req)
			if canceled {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, preparations)
			require.Equal(t, req.RunID, r.OwnerRunID)
			require.Equal(t, "recovery", r.Outcome)
			require.Empty(t, r.RuntimeID)
		})
	}
}
