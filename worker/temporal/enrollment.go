package temporal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"go.temporal.io/sdk/activity"
)

const enrollmentPolicyFile = "enrollment-policy.json"
const enrollmentResultFile = "enrollment-result.json"

func enrollmentPreflightRequest(req *types.ExecutionRequest, job types.JobData, policy *types.EnrollmentPolicy, executionID string) (*types.ExecutionRequest, error) {
	encodedPolicy, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	// A distinct attempt directory cannot reuse a previous proposal or overwrite
	// a running ingestion's configs/logs in either NFS or S3 mode.
	hash := sha256.Sum256([]byte(req.WorkflowID + ":" + executionID))
	preflight := &types.ExecutionRequest{
		Command:       types.DiscoverNewTables,
		ConnectorType: job.Driver,
		Version:       job.Version,
		WorkflowID:    fmt.Sprintf("enroll-%x", hash[:24]),
		JobID:         req.JobID,
		ProjectID:     job.ProjectID,
		Timeout:       req.Timeout,
		OutputFile:    enrollmentResultFile,
		HeartbeatFunc: req.HeartbeatFunc,
		Args: []string{
			string(types.DiscoverNewTables),
			"--config", filepath.Join(constants.ContainerMountDir, "source.json"),
			"--destination", filepath.Join(constants.ContainerMountDir, "destination.json"),
			"--enrollment-policy", filepath.Join(constants.ContainerMountDir, enrollmentPolicyFile),
			"--enrollment-result", filepath.Join(constants.ContainerMountDir, enrollmentResultFile),
		},
		Configs: []types.JobConfig{
			{Name: "source.json", Data: job.Source},
			{Name: "destination.json", Data: job.Destination},
			{Name: enrollmentPolicyFile, Data: string(encodedPolicy)},
		},
	}
	if job.AvailableStreams != "" && job.SelectedStreams != "" {
		preflight.Args = append(preflight.Args,
			constants.AvailableStreamsFlag, filepath.Join(constants.ContainerMountDir, constants.AvailableStreamsFile),
			constants.SelectedStreamsFlag, filepath.Join(constants.ContainerMountDir, constants.SelectedStreamsFile))
		preflight.Configs = append(preflight.Configs,
			types.JobConfig{Name: constants.AvailableStreamsFile, Data: job.AvailableStreams},
			types.JobConfig{Name: constants.SelectedStreamsFile, Data: job.SelectedStreams})
	} else {
		preflight.Args = append(preflight.Args, constants.StreamsFlag, filepath.Join(constants.ContainerMountDir, constants.StreamsFile))
		preflight.Configs = append(preflight.Configs, types.JobConfig{Name: constants.StreamsFile, Data: job.Streams})
	}
	// DiscoverNewTables is deliberately not async: default scheduling, automatic
	// pod/container cleanup, and no per-job index volume or state persistence.
	return preflight, nil
}

func (a *Activity) enrollNewTables(ctx context.Context, req *types.ExecutionRequest, original types.JobData) (types.JobData, error) {
	policy, err := original.AutoTableEnrollment()
	if err != nil {
		return types.JobData{}, err
	}
	if policy == nil {
		return original, nil
	}
	if err := ctx.Err(); err != nil {
		return types.JobData{}, err
	}
	if err := utils.ValidateConnectorVersionForStorageMode(original.Version); err != nil {
		return types.JobData{}, err
	}
	info := activity.GetInfo(ctx)
	executionID := fmt.Sprintf("%s:%s:%d", info.WorkflowExecution.RunID, info.ActivityID, info.Attempt)
	preflight, err := enrollmentPreflightRequest(req, original, policy, executionID)
	if err != nil {
		return types.JobData{}, err
	}
	if _, err := a.executor.Execute(ctx, preflight); err != nil {
		return types.JobData{}, fmt.Errorf("automatic table enrollment discovery failed: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return types.JobData{}, err
	}
	_, workDir := utils.GetWorkflowDirAndSubDir(preflight.WorkflowID, preflight.Command)
	data, err := storage.ReadFile(ctx, workDir, enrollmentResultFile, true)
	if err != nil {
		return types.JobData{}, fmt.Errorf("failed to read automatic table enrollment result: %w", err)
	}
	var result types.EnrollmentResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return types.JobData{}, fmt.Errorf("invalid automatic table enrollment result JSON")
	}
	proposed, err := original.ValidateEnrollment(result)
	if err != nil {
		return types.JobData{}, err
	}
	if err := a.db.CommitCatalogEnrollment(ctx, req, original, result); err != nil {
		return types.JobData{}, fmt.Errorf("automatic table enrollment catalog was not committed: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return types.JobData{}, err
	}
	fresh, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		return types.JobData{}, fmt.Errorf("failed to re-read committed automatic table enrollment catalog: %w", err)
	}
	if err := database.SameEnrollmentContext(proposed, fresh); err != nil {
		return types.JobData{}, err
	}
	if err := ctx.Err(); err != nil {
		return types.JobData{}, err
	}
	log := logger.Log(ctx)
	for _, skipped := range result.Skipped {
		log.Info("automatic table enrollment skipped", "namespace", skipped.Namespace, "stream_name", skipped.StreamName, "reason", skipped.Reason)
	}
	for _, enrolled := range result.Enrolled {
		log.Info("automatic table enrollment committed", "namespace", enrolled.Namespace, "stream_name", enrolled.StreamName)
	}
	return fresh, nil
}
