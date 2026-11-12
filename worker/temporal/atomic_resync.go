package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/acarl005/stripansi"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/failure"
	"github.com/datazip-inc/olake-helm/worker/utils/telemetry"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func (a *Activity) acquireJobExecution(ctx context.Context, req *types.ExecutionRequest) error {
	if err := a.db.AcquireJobExecution(ctx, req.JobID, req.WorkflowID); err != nil {
		if errors.Is(err, database.ErrJobExecutionOwned) {
			return temporal.NewNonRetryableApplicationError(err.Error(), "JobExecutionOwned", err)
		}
		return err
	}
	return nil
}

func (a *Activity) ReleaseJobExecutionActivity(ctx context.Context, req *types.ExecutionRequest) error {
	return a.db.ReleaseJobExecution(ctx, req.JobID, req.WorkflowID)
}

func (a *Activity) ResyncActivity(ctx context.Context, req *types.ExecutionRequest) (result *types.ExecutorResponse, err error) {
	if _, err := utils.ResyncTargetFromRequest(req); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidResyncRequest", err)
	}
	activity.RecordHeartbeat(ctx, "executing table resync")
	req.HeartbeatFunc = activity.RecordHeartbeat
	if err := a.acquireJobExecution(ctx, req); err != nil {
		return nil, err
	}
	invoked, err := resyncExecutionWasInvoked(ctx, req)
	if err != nil {
		return nil, err
	}
	previousInvocation := invoked
	previouslyUnpublished := invoked && resyncPreviouslyUnpublished(ctx, req)
	defer func() {
		if err == nil || invoked {
			return
		}
		proofCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*constants.ContainerCleanupTimeout)
		defer cancel()
		if proofErr := writeResyncEvidence(proofCtx, req, constants.ResyncFailureFile, "not_published"); proofErr != nil {
			err = fmt.Errorf("resync failed before execution: %w; cannot record unpublished proof, retain ownership: %v", err, proofErr)
		}
	}()
	job, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		return nil, err
	}
	indexRequired, err := job.IndexRequired()
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidAdvanceSettings", err)
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
	if err := writeResyncEvidence(ctx, req, constants.ResyncExecutionFile, "execution_started"); err != nil {
		return nil, err
	}
	if err := writeResyncEvidence(ctx, req, constants.ResyncFailureFile, "in_progress"); err != nil {
		return nil, err
	}
	invoked = true
	result, err = a.executor.Execute(ctx, req)
	if err == nil {
		return result, nil
	}
	if ctx.Err() != nil {
		return nil, temporal.NewCanceledError("table resync cancelled")
	}
	var executionFailure *failure.ExecutionFailure
	if errors.As(err, &executionFailure) {
		if unsupportedResync(executionFailure) && (!previousInvocation || previouslyUnpublished) {
			if receiptErr := writeResyncEvidence(ctx, req, constants.ResyncFailureFile, "not_published"); receiptErr != nil {
				return nil, receiptErr
			}
		}
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "ResyncExecutionFailed", err)
	}
	return nil, err
}

func (a *Activity) PostResyncActivity(ctx context.Context, req *types.ExecutionRequest, status syncStatus) error {
	_, workDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	defer utils.ReleaseWorkerLogWriter(workDir)
	cleanupErr := a.executor.CleanupResync(ctx, req, status == syncStatusSuccess)
	// Always restore the normal action while paused, including cleanup failure.
	// The workflow releases ownership and unpauses only after this activity succeeds.
	restored := *req
	utils.RevertUpdatesInSchedule(&restored)
	workflowID, scheduleID := utils.SyncWorkflowAndScheduleID(req.ProjectID, req.JobID)
	handle := a.tempClient.ScheduleClient().GetHandle(ctx, scheduleID)
	err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Action = &client.ScheduleWorkflowAction{
				ID: workflowID, Workflow: RunSyncWorkflow, Args: []interface{}{&restored}, TaskQueue: utils.GetTemporalTaskQueue(),
			}
			if input.Description.Schedule.State == nil {
				input.Description.Schedule.State = &client.ScheduleState{}
			}
			input.Description.Schedule.State.Paused = true
			input.Description.Schedule.State.Note = "Table resync requires confirmed publication, persisted state and successful cleanup before resume"
			return &client.ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("restore paused sync schedule: %w; resync cleanup: %v", err, cleanupErr)
	}
	if cleanupErr != nil {
		if errors.Is(cleanupErr, executor.ErrResyncUnconfirmed) {
			return temporal.NewNonRetryableApplicationError(cleanupErr.Error(), "ResyncCleanupFailed", cleanupErr)
		}
		return cleanupErr
	}
	return nil
}

func unsupportedResync(f *failure.ExecutionFailure) bool {
	if f.ExitCode == nil || *f.ExitCode == 0 || f.NotFound {
		return false
	}
	const invalid = "'resync' is an invalid command. Use 'olake --help' to display usage guide"
	const cobra = `unknown command "resync" for "olake"`
	for _, line := range strings.Split(stripansi.Strip(f.LogTail), "\n") {
		line = strings.TrimSpace(line)
		var record struct {
			Message string `json:"message"`
		}
		if json.Unmarshal([]byte(line), &record) == nil && (record.Message == invalid || record.Message == cobra) {
			return true
		}
		if line == cobra || line == "Error: "+cobra || line == invalid || strings.HasSuffix(line, " FTL "+invalid) || strings.HasSuffix(line, " FATAL "+invalid) {
			return true
		}
	}
	return false
}

func writeResyncEvidence(ctx context.Context, req *types.ExecutionRequest, filename, publication string) error {
	target, err := utils.ResyncTargetFromRequest(req)
	if err != nil {
		return err
	}
	data, err := json.Marshal(map[string]interface{}{
		"version": 1, "resync_id": target.ID, "namespace": target.Namespace, "stream_name": target.Table, "publication": publication,
	})
	if err != nil {
		return err
	}
	_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	return storage.WriteFiles(ctx, dir, []types.JobConfig{{Name: filename, Data: string(data)}})
}

func resyncExecutionWasInvoked(ctx context.Context, req *types.ExecutionRequest) (bool, error) {
	target, err := utils.ResyncTargetFromRequest(req)
	if err != nil {
		return false, err
	}
	_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	data, err := storage.ReadFile(ctx, dir, constants.ResyncExecutionFile, false)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var marker struct {
		Version     int    `json:"version"`
		ID          string `json:"resync_id"`
		Namespace   string `json:"namespace"`
		Table       string `json:"stream_name"`
		Publication string `json:"publication"`
	}
	if err := json.Unmarshal([]byte(data), &marker); err != nil {
		return false, fmt.Errorf("invalid resync execution marker: %w", err)
	}
	if marker.Version != 1 || marker.ID != target.ID || marker.Namespace != target.Namespace || marker.Table != target.Table || marker.Publication != "execution_started" {
		return false, fmt.Errorf("resync execution marker does not match this operation; retain ownership")
	}
	return true, nil
}

func resyncPreviouslyUnpublished(ctx context.Context, req *types.ExecutionRequest) bool {
	target, err := utils.ResyncTargetFromRequest(req)
	if err != nil {
		return false
	}
	_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	data, err := storage.ReadFile(ctx, dir, constants.ResyncFailureFile, false)
	if err != nil {
		return false
	}
	var proof struct {
		Version     int    `json:"version"`
		ID          string `json:"resync_id"`
		Namespace   string `json:"namespace"`
		Table       string `json:"stream_name"`
		Publication string `json:"publication"`
	}
	return json.Unmarshal([]byte(data), &proof) == nil && proof.Version == 1 && proof.ID == target.ID && proof.Namespace == target.Namespace && proof.Table == target.Table && proof.Publication == "not_published"
}
