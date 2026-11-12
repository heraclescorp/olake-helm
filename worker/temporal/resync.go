package temporal

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

func (a *Activity) TriggerSyncAfterResyncActivity(ctx context.Context, req *types.ExecutionRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := utils.ResyncTargetFromRequest(req); err != nil {
		return err
	}
	return a.db.ApplyHandoff(ctx, req, func(ctx context.Context, r *database.ExecutionRecord) error {
		restored := *req
		utils.RevertUpdatesInSchedule(&restored)
		restored.RunID, restored.RuntimeID, restored.LaunchID, restored.LauncherAttemptID = "", "", "", ""
		restored.ResourceID, restored.ResourceUID = "", ""
		restored.AllowLaunch = false
		restored.RecoverOnly = false
		restored.BeforeLaunch = nil
		restored.RecordResource = nil
		restored.AfterResyncOperationID = req.WorkflowID
		restored.AfterResyncRunID = r.HandoffRunID
		restored.AfterResyncTransferToken = r.HandoffToken
		restored.AfterResyncStateSHA256 = r.StateSHA256
		workflowID, scheduleID := utils.SyncWorkflowAndScheduleID(req.ProjectID, req.JobID)
		handle := a.tempClient.ScheduleClient().GetHandle(ctx, scheduleID)
		if err := handle.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Action = &client.ScheduleWorkflowAction{ID: workflowID, Workflow: RunSyncWorkflow, Args: []interface{}{&restored}, TaskQueue: utils.GetTemporalTaskQueue()}
			// Never re-pause here: a previous stable-ID patch may already be applied.
			return &client.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
		}}); err != nil {
			return fmt.Errorf("restore guarded sync action: %w", err)
		}
		// Inactive jobs stay paused. The durable binding is still completed so a
		// subsequent explicit user activation has ordinary ownership semantics.
		if !r.HandoffActive {
			return nil
		}
		_, err := a.tempClient.WorkflowService().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
			Namespace: utils.GetTemporalNamespace(), ScheduleId: scheduleID, RequestId: r.HandoffRequestID,
			Patch: &schedulepb.SchedulePatch{Unpause: "Atomic table resync publication and checkpoint confirmed", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}},
		})
		return err
	})
}
