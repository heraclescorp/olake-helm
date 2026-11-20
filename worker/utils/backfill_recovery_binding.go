package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

type BackfillRecoveryBinding struct {
	Version             int               `json:"version"`
	OriginalExecutionID string            `json:"original_execution_id"`
	OriginalLaunchID    string            `json:"original_launch_id"`
	SourceSHA256        string            `json:"source_sha256"`
	DestinationSHA256   string            `json:"destination_sha256"`
	CatalogSHA256       string            `json:"catalog_sha256"`
	ServerIdentity      string            `json:"server_identity"`
	SlotName            string            `json:"slot_name"`
	LowerLSN            string            `json:"lower_lsn"`
	Stream              string            `json:"stream"`
	TableUUID           string            `json:"table_uuid"`
	ShapeSHA256         string            `json:"shape_sha256"`
	Plan                []json.RawMessage `json:"plan"`
}

type backfillBindingAuthority struct {
	origin    OrdinaryRecoveryOrigin
	candidate BackfillRecoveryCheckpoint
	selected  map[string]bool
	original  map[string]backfillRecoveryStream
	lower     uint64
}

func BackfillRecoveryBindingFilename(stream string) string {
	return "backfill-recovery-binding-" + RawSHA256(stream) + ".json"
}

func newBackfillBindingAuthority(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string) (backfillBindingAuthority, error) {
	authority := backfillBindingAuthority{origin: origin}
	candidate, data, err := ReadBackfillRecoveryCheckpoint(ctx, req, files)
	if err != nil {
		return authority, err
	}
	if !origin.Backfill || RawSHA256(data) != origin.BackfillCheckpointSHA256 || candidate.StateSHA256 != origin.BaselineStateSHA256 {
		return authority, fmt.Errorf("backfill binding journal requires the immutable candidate-bound operation origin")
	}
	baselineSHA, err := StateSHA256(baseline)
	if err != nil || baselineSHA != origin.BaselineStateSHA256 {
		return authority, fmt.Errorf("backfill binding journal baseline differs from immutable origin")
	}
	selected, lower, err := OrdinaryBackfillRecoverySelection(files, baseline)
	if err != nil {
		return authority, err
	}
	boundary, err := ParseRecoveryLSN(lower)
	if err != nil {
		return authority, err
	}
	streams, err := backfillRecoveryStreams(baseline)
	if err != nil {
		return authority, err
	}
	authority.candidate, authority.selected, authority.lower = candidate, selected, boundary
	authority.original = make(map[string]backfillRecoveryStream, len(streams))
	for _, stream := range streams {
		authority.original[stream.Namespace+"."+stream.Stream] = stream
	}
	return authority, nil
}

func (authority backfillBindingAuthority) validate(data, stream string) (BackfillRecoveryBinding, error) {
	var binding BackfillRecoveryBinding
	if err := json.Unmarshal([]byte(data), &binding); err != nil {
		return binding, fmt.Errorf("invalid backfill recovery binding journal: %w", err)
	}
	lower, err := ParseRecoveryLSN(binding.LowerLSN)
	if err != nil || lower != authority.lower || binding.Version != 1 || binding.OriginalExecutionID != authority.origin.OriginalExecutionID || binding.OriginalLaunchID != authority.origin.OriginalLaunchID || binding.SourceSHA256 != authority.origin.SourceSHA256 || binding.DestinationSHA256 != authority.origin.DestinationSHA256 || binding.CatalogSHA256 != authority.origin.CatalogSHA256 || !authority.selected[stream] || binding.Stream != stream || binding.ServerIdentity != authority.candidate.ServerIdentity || binding.SlotName != authority.candidate.SlotName || binding.TableUUID == "" || !validRecoverySHA256(binding.ShapeSHA256) {
		return binding, fmt.Errorf("backfill binding journal differs from original operation, frozen source identity, stream or WAL boundary")
	}
	if len(binding.Plan) == 0 {
		return binding, fmt.Errorf("backfill binding journal requires a complete unique immutable chunk plan")
	}
	chunks := make(map[string]bool, len(binding.Plan))
	for _, chunk := range binding.Plan {
		sha, err := recoveryChunkSHA256(chunk)
		if err != nil || chunks[sha] {
			return binding, fmt.Errorf("backfill binding journal requires a complete unique immutable chunk plan")
		}
		chunks[sha] = true
	}
	if original, started := authority.original[stream]; started {
		if original.State.Table == nil || binding.TableUUID != original.State.Table.TableUUID || binding.ShapeSHA256 != original.State.Table.ShapeSHA256 || !sameBackfillRecoveryPlan(binding.Plan, original.State.BackfillPlan) {
			return binding, fmt.Errorf("backfill binding journal conflicts with original candidate table identity, shape or chunk plan")
		}
	}
	return binding, nil
}

func PreserveBackfillRecoveryBindings(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string) error {
	if !origin.Backfill {
		return nil
	}
	if err := RequireClosedExecutionFence(ctx, req); err != nil {
		return fmt.Errorf("backfill binding journal custody requires the positively retired runtime: %w", err)
	}
	authority, err := newBackfillBindingAuthority(ctx, req, origin, baseline, files)
	if err != nil {
		return err
	}
	_, runtimeDir := GetExecutionDirAndSubDir(req)
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	for stream := range authority.selected {
		name := BackfillRecoveryBindingFilename(stream)
		data, err := storage.ReadFile(ctx, runtimeDir, name, false)
		if err == nil {
			if _, err := authority.validate(data, stream); err != nil {
				return err
			}
			if _, err := storage.CreateFileIfAbsent(ctx, stableDir, name, data); err != nil {
				return err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		stored, storedErr := storage.ReadFile(ctx, stableDir, name, false)
		if errors.Is(storedErr, fs.ErrNotExist) {
			continue
		}
		if storedErr != nil {
			return storedErr
		}
		if _, err := authority.validate(stored, stream); err != nil {
			return err
		}
		if err == nil && stored != data {
			return fmt.Errorf("immutable backfill binding journal conflicts with retired runtime evidence")
		}
	}
	return nil
}

func BackfillRecoveryBindingConfigs(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string) ([]types.JobConfig, error) {
	if !origin.Backfill {
		return nil, nil
	}
	authority, err := newBackfillBindingAuthority(ctx, req, origin, baseline, files)
	if err != nil {
		return nil, err
	}
	_, dir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	var configs []types.JobConfig
	for stream := range authority.selected {
		name := BackfillRecoveryBindingFilename(stream)
		data, err := storage.ReadFile(ctx, dir, name, false)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if _, err := authority.validate(data, stream); err != nil {
			return nil, err
		}
		configs = append(configs, types.JobConfig{Name: name, Data: data})
	}
	return configs, nil
}

func ValidateBackfillRecoveryBindingProofs(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string, final string) error {
	if err := PreserveBackfillRecoveryBindings(ctx, req, origin, baseline, files); err != nil {
		return err
	}
	authority, err := newBackfillBindingAuthority(ctx, req, origin, baseline, files)
	if err != nil {
		return err
	}
	settled, err := backfillRecoveryStreams(final)
	if err != nil {
		return err
	}
	_, runtimeDir := GetExecutionDirAndSubDir(req)
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	for _, stream := range settled {
		id := stream.Namespace + "." + stream.Stream
		name := BackfillRecoveryBindingFilename(id)
		data, err := storage.ReadFile(ctx, runtimeDir, name, false)
		if errors.Is(err, fs.ErrNotExist) {
			if _, started := authority.original[id]; !started {
				return fmt.Errorf("recovery-created table has no durable original-operation binding journal")
			}
			if _, stableErr := storage.ReadFile(ctx, stableDir, name, false); !errors.Is(stableErr, fs.ErrNotExist) {
				return fmt.Errorf("recovery runtime lacks its inherited immutable binding journal")
			}
			continue
		}
		if err != nil {
			return err
		}
		binding, err := authority.validate(data, id)
		if err != nil {
			return err
		}
		if stream.State.Table == nil || binding.TableUUID != stream.State.Table.TableUUID || binding.ShapeSHA256 != stream.State.Table.ShapeSHA256 || !sameBackfillRecoveryPlan(binding.Plan, stream.State.BackfillPlan) {
			return fmt.Errorf("final backfill checkpoint conflicts with its durable table binding journal")
		}
	}
	return nil
}
