package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
)

func (a *Activity) TriggerSyncAfterClearActivity(ctx context.Context, req *types.ExecutionRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	_, scheduleID := utils.SyncWorkflowAndScheduleID(req.ProjectID, req.JobID)
	_, err := a.tempClient.WorkflowService().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  utils.GetTemporalNamespace(),
		ScheduleId: scheduleID,
		RequestId:  activity.GetInfo(ctx).WorkflowExecution.RunID + "/table-resync-sync",
		Patch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			},
		},
	})
	return err
}
