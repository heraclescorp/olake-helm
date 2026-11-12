package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/telemetry"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func executionRequestFromRecord(req *types.ExecutionRequest, r *database.ExecutionRecord) {
	req.RuntimeID, req.LaunchID, req.LauncherAttemptID = r.RuntimeID, r.LaunchID, r.LauncherAttemptID
	req.ResourceID, req.ResourceUID = r.ResourceID, r.ResourceUID
	req.RecoverOnly = r.Outcome == "recovery"
	req.Controlled = r.Controlled
}

func (a *Activity) acquireJobExecution(ctx context.Context, req *types.ExecutionRequest) error {
	err := a.db.AcquireJobExecution(ctx, req)
	if err == nil {
		return nil
	}
	if errors.Is(err, database.ErrJobExecutionInactive) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "JobPaused", err)
	}
	if !errors.Is(err, database.ErrJobExecutionOwned) && !errors.Is(err, database.ErrExecutionClosed) {
		return err
	}
	r, readErr := a.db.GetExecution(ctx, req.JobID, req.WorkflowID)
	if readErr != nil || r.OwnerRunID == req.RunID {
		return temporal.NewNonRetryableApplicationError(err.Error(), "JobExecutionOwned", err)
	}
	if r.OwnerRunID == "" {
		return temporal.NewNonRetryableApplicationError("legacy or unresolved execution has no controlling epoch; drain/reconcile with the original worker", "UnresolvedExecutionEpoch", err)
	}
	// WorkflowID reuse/reset is not authority. Positively retire the old Temporal
	// run, seal its launcher, close its positive fence and delete its exact runtime.
	description, describeErr := a.tempClient.DescribeWorkflowExecution(ctx, req.WorkflowID, r.OwnerRunID)
	if describeErr != nil {
		return fmt.Errorf("cannot establish old controlling run retirement: %w", describeErr)
	}
	if description == nil || description.WorkflowExecutionInfo == nil || description.WorkflowExecutionInfo.Execution == nil || description.WorkflowExecutionInfo.Execution.WorkflowId != req.WorkflowID || description.WorkflowExecutionInfo.Execution.RunId != r.OwnerRunID {
		return temporal.NewNonRetryableApplicationError("previous controlling run identity is unknown", "ExecutionEpochActive", err)
	}
	switch description.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
	default:
		return temporal.NewNonRetryableApplicationError("previous controlling run is active or not positively terminal", "ExecutionEpochActive", err)
	}
	old := *req
	old.RunID = r.OwnerRunID
	executionRequestFromRecord(&old, r)
	if req.Command != types.Resync && (r.LaunchID != "" || !r.Controlled) {
		if err := a.executor.CleanupAndPersistState(ctx, &old); err != nil {
			return fmt.Errorf("ordinary execution retirement is not proven: %w", err)
		}
	} else if err := a.executor.RetireExecution(ctx, &old); err != nil {
		return fmt.Errorf("retire prior execution epoch: %w", err)
	}
	if err := a.db.AdoptExecution(ctx, &old, req); err != nil {
		return err
	}
	return a.db.AcquireJobExecution(ctx, req)
}

func (a *Activity) prepareExecution(ctx context.Context, req *types.ExecutionRequest) error {
	info := activity.GetInfo(ctx)
	attempt := fmt.Sprintf("%s/%s/%d", req.RunID, info.ActivityID, info.Attempt)
	r, admitted, err := a.db.AdmitExecution(ctx, req, attempt, uuid.NewString())
	if err != nil {
		return err
	}
	executionRequestFromRecord(req, r)
	req.AllowLaunch = admitted
	req.BeforeLaunch = nil
	req.RecordResource = nil
	if req.Controlled {
		req.BeforeLaunch = func(ctx context.Context) error { return a.db.CheckLaunch(ctx, req) }
		req.RecordResource = func(ctx context.Context, id, uid string) error {
			return a.db.RecordExecutionResource(ctx, req, id, uid)
		}
	}
	if admitted && req.Controlled {
		if err := a.db.GuardExecution(ctx, req, func(ctx context.Context, current *database.ExecutionRecord) error {
			if current.Phase != "admitted" || current.LauncherAttemptID != attempt {
				return database.ErrExecutionClosed
			}
			if err := utils.OpenExecutionFence(ctx, req); err != nil {
				return err
			}
			return utils.WriteExecutionPointer(ctx, req)
		}); err != nil {
			return err
		}
	}
	utils.SetExecutionArgs(req)
	return nil
}

func (a *Activity) ReleaseJobExecutionActivity(ctx context.Context, req *types.ExecutionRequest) error {
	return a.db.ReleaseJobExecution(ctx, req)
}

func (a *Activity) ResyncActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	if _, err := utils.ResyncTargetFromRequest(req); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidResyncRequest", err)
	}
	activity.RecordHeartbeat(ctx, "executing table resync")
	req.HeartbeatFunc = activity.RecordHeartbeat
	if err := a.acquireJobExecution(ctx, req); err != nil {
		return nil, err
	}
	current, err := a.db.GetExecution(ctx, req.JobID, req.WorkflowID)
	if err != nil {
		return nil, err
	}
	if !current.Controlled {
		return nil, temporal.NewNonRetryableApplicationError("atomic resync requires canonical PostgreSQL to REST-family Iceberg destination", "UnsupportedAtomicResyncDomain", nil)
	}
	if current.Phase == "handoff" {
		executionRequestFromRecord(req, current)
		return &types.ExecutorResponse{}, nil
	}
	job, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		return nil, err
	}
	indexRequired, err := job.IndexRequired()
	if err != nil {
		return nil, err
	}
	req.IndexRequired = indexRequired
	req.ConnectorType = job.Driver
	req.TempPath = ""
	req.OutputFile = "state.json"
	req.Configs = nil
	utils.UpdateConfigWithJobDetails(job, req)
	telemetry.WriteConfigs(req, telemetry.BasePayload(ctx, req))
	if err := utils.ValidateConnectorVersionForStorageMode(req.Version); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "UnsupportedConnectorVersion", err)
	}
	if err := a.prepareExecution(ctx, req); err != nil {
		return nil, err
	}
	result, err := a.executor.Execute(ctx, req)
	if ctx.Err() != nil {
		return nil, temporal.NewCanceledError("table resync cancelled")
	}
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "ResyncExecutionFailed", err)
	}
	return result, nil
}

func (a *Activity) PostResyncActivity(ctx context.Context, req *types.ExecutionRequest, status syncStatus) error {
	_, workDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	defer utils.ReleaseWorkerLogWriter(workDir)
	if err := a.executor.CleanupResync(ctx, req, status == syncStatusSuccess); err != nil {
		if errors.Is(err, executor.ErrResyncUnconfirmed) {
			return temporal.NewNonRetryableApplicationError(err.Error(), "ResyncCleanupFailed", err)
		}
		return err
	}
	if status == syncStatusSuccess {
		return a.db.BeginHandoff(ctx, req, uuid.NewString())
	}
	return a.db.GuardExecution(ctx, req, func(ctx context.Context, _ *database.ExecutionRecord) error {
		restored := *req
		utils.RevertUpdatesInSchedule(&restored)
		restored.RunID, restored.RuntimeID, restored.LaunchID, restored.LauncherAttemptID = "", "", "", ""
		restored.ResourceID, restored.ResourceUID = "", ""
		restored.BeforeLaunch = nil
		restored.RecordResource = nil
		workflowID, scheduleID := utils.SyncWorkflowAndScheduleID(req.ProjectID, req.JobID)
		return a.tempClient.ScheduleClient().GetHandle(ctx, scheduleID).Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Action = &client.ScheduleWorkflowAction{ID: workflowID, Workflow: RunSyncWorkflow, Args: []interface{}{&restored}, TaskQueue: utils.GetTemporalTaskQueue()}
			if input.Description.Schedule.State == nil {
				input.Description.Schedule.State = &client.ScheduleState{}
			}
			input.Description.Schedule.State.Paused = true
			input.Description.Schedule.State.Note = "Resync did not complete; explicit operator action required"
			return &client.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
		}})
	})
}
