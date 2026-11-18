package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

// PrepareOrdinaryRecovery is not settlement. It first proves exact process
// retirement and freezes the evidence needed to adopt the same operation. Only
// a separate newly fenced connector may subsequently produce settlement proof.
func (a *AbstractExecutor) PrepareOrdinaryRecovery(ctx context.Context, req *types.ExecutionRequest) error {
	if req.Command != types.Sync || !req.Controlled || req.LaunchID == "" {
		return fmt.Errorf("unknown execution is outside ordinary sync recovery")
	}
	if err := a.RetireExecution(ctx, req); err != nil {
		return err
	}
	return a.db.GuardExecution(ctx, req, func(ctx context.Context, record *database.ExecutionRecord) error {
		if !record.Controlled || record.Phase != "closing" || req.ResourceID == "" {
			return fmt.Errorf("ordinary recovery requires sealed admission and positively retired immutable resource")
		}
		reader, ok := a.db.(interface {
			GetJobData(context.Context, int) (types.JobData, error)
		})
		if !ok {
			return fmt.Errorf("ordinary recovery requires authoritative saved checkpoint")
		}
		job, err := reader.GetJobData(ctx, req.JobID)
		if err != nil {
			return err
		}
		files, err := utils.ReadOrdinaryFrozenConfigs(ctx, req)
		if err != nil {
			return err
		}
		original := *req
		original.RuntimeID = req.LaunchID
		_, originalDir := utils.GetExecutionDirAndSubDir(&original)
		receipt, err := storage.ReadFile(ctx, originalDir, executionResultFile, false)
		if err != nil {
			return err
		}
		var proof executionResult
		if err := json.Unmarshal([]byte(receipt), &proof); err != nil {
			return err
		}
		if proof.Version != 1 || proof.ExecutionID != req.LaunchID || proof.LaunchID != req.LaunchID || proof.Command != "sync" || proof.Outcome != "unknown" {
			return fmt.Errorf("ordinary recovery requires the original identity-bound unknown sync receipt")
		}
		_, stableDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
		// A crash between these two create-only publications is retried against
		// the same baseline, never silently replaced by a later DB checkpoint.
		if _, err := storage.CreateFileIfAbsent(ctx, stableDir, utils.OrdinaryRecoveryBaselineFile, job.State); err != nil {
			return err
		}
		baseline, err := storage.ReadFile(ctx, stableDir, utils.OrdinaryRecoveryBaselineFile, false)
		if err != nil {
			return err
		}
		baselineSHA, err := utils.StateSHA256(baseline)
		if err != nil {
			return err
		}
		if err := utils.ValidateOrdinaryRecoveryJob(job, files, baselineSHA); err != nil {
			return err
		}
		if _, _, err := utils.OrdinaryRecoverySelection(files, baseline); err != nil {
			return err
		}
		catalogSHA, err := utils.OrdinaryCatalogSHA256(files)
		if err != nil {
			return err
		}
		origin := utils.OrdinaryRecoveryOrigin{Version: 1, OriginalExecutionID: req.LaunchID, OriginalLaunchID: req.LaunchID, OriginalReceiptSHA256: utils.RawSHA256(receipt), SourceSHA256: utils.RawSHA256(files["source.json"]), DestinationSHA256: utils.RawSHA256(files["destination.json"]), CatalogSHA256: catalogSHA, BaselineStateSHA256: baselineSHA}
		data, err := json.Marshal(origin)
		if err != nil {
			return err
		}
		if _, err := storage.CreateFileIfAbsent(ctx, stableDir, utils.OrdinaryRecoveryOriginFile, string(data)); err != nil {
			return err
		}
		persisted, _, _, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
		if err != nil {
			return err
		}
		if persisted != origin {
			return fmt.Errorf("immutable ordinary recovery origin conflicts with original evidence")
		}
		if err := utils.PreserveOrdinaryRecoveryServer(ctx, req, origin); err != nil {
			return err
		}
		return nil
	})
}

func (a *AbstractExecutor) ordinaryRecoveryInputs(ctx context.Context, req *types.ExecutionRequest) error {
	if !req.Controlled || req.Command != types.Sync || req.RuntimeID == "" || req.RuntimeID == req.LaunchID {
		return fmt.Errorf("ordinary recovery requires a fresh controlled sync runtime")
	}
	origin, baseline, files, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
	if err != nil {
		return err
	}
	reader, ok := a.db.(interface {
		GetJobData(context.Context, int) (types.JobData, error)
	})
	if !ok {
		return fmt.Errorf("ordinary recovery requires authoritative saved checkpoint")
	}
	job, err := reader.GetJobData(ctx, req.JobID)
	if err != nil {
		return err
	}
	if err := utils.ValidateOrdinaryRecoveryJob(job, files, origin.BaselineStateSHA256); err != nil {
		return err
	}
	data, err := json.Marshal(origin)
	if err != nil {
		return err
	}
	req.TempPath, req.OutputFile = "", "state.json"
	req.Configs = []types.JobConfig{{Name: "state.json", Data: baseline}, {Name: utils.OrdinaryRecoveryOriginFile, Data: string(data)}, {Name: utils.OrdinaryRecoveryBaselineFile, Data: baseline}}
	_, stableDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	server, err := storage.ReadFile(ctx, stableDir, utils.OrdinaryRecoveryServerFile, false)
	if err == nil {
		if _, err := utils.ValidateOrdinaryRecoveryServer(server, origin); err != nil {
			return err
		}
		req.Configs = append(req.Configs, types.JobConfig{Name: utils.OrdinaryRecoveryServerFile, Data: server})
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, name := range []string{"source.json", "destination.json", constants.StreamsFile, constants.AvailableStreamsFile, constants.SelectedStreamsFile} {
		if content, exists := files[name]; exists {
			req.Configs = append(req.Configs, types.JobConfig{Name: name, Data: content})
		}
	}
	// No editable request flags can redirect the source, destination or saved
	// checkpoint, or turn this bounded recovery into ordinary ingestion.
	req.Args = []string{"sync", "--config", filepath.Join(constants.ContainerMountDir, "source.json"), "--destination", filepath.Join(constants.ContainerMountDir, "destination.json"), "--state", filepath.Join(constants.ContainerMountDir, "state.json")}
	_, split := files[constants.AvailableStreamsFile]
	req.Args = utils.SetCatalogArgs(req.Args, split)
	return nil
}

func confirmedOrdinaryRecovery(ctx context.Context, req *types.ExecutionRequest) (string, error) {
	origin, baseline, files, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
	if err != nil {
		return "", err
	}
	_, dir := utils.GetExecutionDirAndSubDir(req)
	copied, err := storage.ReadFile(ctx, dir, utils.OrdinaryRecoveryOriginFile, false)
	if err != nil {
		return "", err
	}
	var runtimeOrigin utils.OrdinaryRecoveryOrigin
	if err := json.Unmarshal([]byte(copied), &runtimeOrigin); err != nil {
		return "", err
	}
	if runtimeOrigin != origin {
		return "", fmt.Errorf("recovery runtime origin differs from immutable operation origin")
	}
	runtimeFiles, err := utils.ReadOrdinaryRuntimeConfigs(ctx, req)
	if err != nil {
		return "", err
	}
	runtimeCatalog, err := utils.OrdinaryCatalogSHA256(runtimeFiles)
	if err != nil {
		return "", err
	}
	if utils.RawSHA256(runtimeFiles["source.json"]) != origin.SourceSHA256 || utils.RawSHA256(runtimeFiles["destination.json"]) != origin.DestinationSHA256 || runtimeCatalog != origin.CatalogSHA256 {
		return "", fmt.Errorf("recovery runtime source, destination or catalog evidence changed")
	}
	runtimeBaseline, err := storage.ReadFile(ctx, dir, utils.OrdinaryRecoveryBaselineFile, false)
	if err != nil {
		return "", err
	}
	runtimeBaselineSHA, err := utils.StateSHA256(runtimeBaseline)
	if err != nil {
		return "", err
	}
	if runtimeBaselineSHA != origin.BaselineStateSHA256 {
		return "", fmt.Errorf("recovery runtime baseline evidence changed")
	}
	state, outcome, err := confirmedExecutionResult(ctx, req)
	if err != nil {
		return "", err
	}
	if outcome != "settled" || state == nil {
		return "", fmt.Errorf("ordinary recovery requires a new settled execution receipt")
	}
	data, err := storage.ReadFile(ctx, dir, utils.OrdinaryRecoveryResultFile, false)
	if err != nil {
		return "", err
	}
	var result utils.OrdinaryRecoveryResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return "", err
	}
	stateSHA, err := utils.StateSHA256(*state)
	if err != nil {
		return "", err
	}
	if result.OrdinaryRecoveryOrigin != origin || result.ExecutionID != req.RuntimeID || result.LaunchID != req.LaunchID || result.StateSHA256 != stateSHA {
		return "", fmt.Errorf("ordinary recovery result does not bind origin, runtime and final state")
	}
	serverData, err := storage.ReadFile(ctx, dir, utils.OrdinaryRecoveryServerFile, false)
	if err != nil {
		return "", err
	}
	server, err := utils.ValidateOrdinaryRecoveryServer(serverData, origin)
	if err != nil {
		return "", err
	}
	if server.ServerIdentity != result.ServerIdentity || server.SlotName != result.SlotName {
		return "", fmt.Errorf("ordinary recovery result does not bind pinned PostgreSQL server/slot")
	}
	_, stableDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	prior, err := storage.ReadFile(ctx, stableDir, utils.OrdinaryRecoveryServerFile, false)
	if err == nil {
		pinned, err := utils.ValidateOrdinaryRecoveryServer(prior, origin)
		if err != nil {
			return "", err
		}
		if pinned != server {
			return "", fmt.Errorf("recovery PostgreSQL server pin changed")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if result.ToastValueSemantics != "current_source_lookup" && result.ToastValueSemantics != "destination" {
		return "", fmt.Errorf("ordinary recovery result has unknown TOAST value semantics")
	}
	selected, lowerLSN, err := utils.OrdinaryRecoverySelection(files, baseline)
	if err != nil {
		return "", err
	}
	lower, err := utils.ParseRecoveryLSN(lowerLSN)
	if err != nil {
		return "", err
	}
	resultLower, err := utils.ParseRecoveryLSN(result.LowerLSN)
	if err != nil {
		return "", err
	}
	target, err := utils.ParseRecoveryLSN(result.TargetLSN)
	if err != nil {
		return "", err
	}
	if lower != resultLower || target < lower {
		return "", fmt.Errorf("recovery lower/target boundary is invalid")
	}
	_, finalLSN, err := utils.OrdinaryRecoverySelection(files, *state)
	if err != nil {
		return "", err
	}
	final, err := utils.ParseRecoveryLSN(finalLSN)
	if err != nil {
		return "", err
	}
	if final != target || len(result.Tables) != len(selected) {
		return "", fmt.Errorf("recovery final checkpoint/table coverage differs from target")
	}
	seen := make(map[string]bool)
	for _, table := range result.Tables {
		committed, err := utils.ParseRecoveryLSN(table.CommittedLSN)
		if err != nil || !selected[table.Stream] || seen[table.Stream] || table.TableUUID == "" || table.FenceSnapshotID <= 0 || committed != target {
			return "", fmt.Errorf("recovery table proof is incomplete, duplicated or not at common target")
		}
		seen[table.Stream] = true
	}
	return *state, nil
}
