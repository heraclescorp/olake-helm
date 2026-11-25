package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

type BackfillRecoveryCheckpoint struct {
	Version           int             `json:"version"`
	ExecutionID       string          `json:"execution_id"`
	LaunchID          string          `json:"launch_id"`
	SourceSHA256      string          `json:"source_sha256"`
	DestinationSHA256 string          `json:"destination_sha256"`
	CatalogSHA256     string          `json:"catalog_sha256"`
	StateSHA256       string          `json:"state_sha256"`
	State             json.RawMessage `json:"state"`
	ServerIdentity    string          `json:"server_identity"`
	SlotName          string          `json:"slot_name"`
}

type backfillRecoveryTable struct {
	TableUUID   string `json:"table_uuid"`
	ShapeSHA256 string `json:"shape_sha256"`
}

type backfillRecoveryStream struct {
	Stream    string `json:"stream"`
	Namespace string `json:"namespace"`
	SyncMode  string `json:"sync_mode"`
	State     struct {
		BackfillPlan []json.RawMessage      `json:"backfill_plan"`
		ResumePlan   []json.RawMessage      `json:"resume_plan"`
		Chunks       []json.RawMessage      `json:"chunks"`
		Table        *backfillRecoveryTable `json:"backfill_table"`
	} `json:"state"`
}

func validRecoverySHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}

func ReadBackfillRecoveryCheckpoint(ctx context.Context, req *types.ExecutionRequest, files map[string]string) (BackfillRecoveryCheckpoint, string, error) {
	return readBackfillRecoveryCheckpoint(ctx, req, files, true)
}

func FreezeBackfillRecoveryCheckpoint(ctx context.Context, req *types.ExecutionRequest, files map[string]string, saved string) (BackfillRecoveryCheckpoint, string, error) {
	candidate, data, err := readBackfillRecoveryCheckpoint(ctx, req, files, false)
	if err != nil {
		return candidate, "", err
	}
	if err := ValidateBackfillRecoveryLowerBound(saved, string(candidate.State)); err != nil {
		return candidate, "", err
	}
	_, dir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	if _, err := storage.CreateFileIfAbsent(ctx, dir, BackfillRecoveryCheckpointFile, data); err != nil {
		return candidate, "", err
	}
	frozen, data, err := ReadBackfillRecoveryCheckpoint(ctx, req, files)
	if err != nil {
		return frozen, "", err
	}
	if err := ValidateBackfillRecoveryLowerBound(saved, string(frozen.State)); err != nil {
		return frozen, "", err
	}
	return frozen, data, nil
}

func readBackfillRecoveryCheckpoint(ctx context.Context, req *types.ExecutionRequest, files map[string]string, frozenOnly bool) (BackfillRecoveryCheckpoint, string, error) {
	var candidate BackfillRecoveryCheckpoint
	original := *req
	original.Controlled, original.RuntimeID = true, req.LaunchID
	if err := RequireClosedExecutionFence(ctx, &original); err != nil {
		return candidate, "", fmt.Errorf("backfill recovery requires the positively retired original runtime: %w", err)
	}
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	_, originalDir := GetExecutionDirAndSubDir(&original)
	if stableDir == originalDir {
		return candidate, "", fmt.Errorf("original runtime aliases immutable operation storage; backfill candidate cannot be frozen safely and job remains paused and owned")
	}
	data, err := storage.ReadFile(ctx, stableDir, BackfillRecoveryCheckpointFile, false)
	if errors.Is(err, fs.ErrNotExist) {
		if frozenOnly {
			return candidate, "", fmt.Errorf("unsupported historical initial-backfill recovery: immutable operation has no frozen %s; job remains paused and owned; restore the exact frozen evidence from backup if available, otherwise an explicitly planned new backfill is required; do not overwrite immutable baseline or origin", BackfillRecoveryCheckpointFile)
		}
		data, err = storage.ReadFile(ctx, originalDir, BackfillRecoveryCheckpointFile, false)
		if errors.Is(err, fs.ErrNotExist) {
			return candidate, "", fmt.Errorf("unsupported historical initial-backfill recovery: original runtime has no durable %s; job remains paused and owned; restore valid original-runtime evidence from backup if available, otherwise an explicitly planned new backfill is required; do not reset ownership or overwrite immutable recovery evidence", BackfillRecoveryCheckpointFile)
		}
	}
	if err != nil {
		return candidate, "", err
	}
	if err := json.Unmarshal([]byte(data), &candidate); err != nil {
		return candidate, "", fmt.Errorf("invalid original backfill recovery candidate: %w", err)
	}
	catalogSHA, err := OrdinaryCatalogSHA256(files)
	if err != nil {
		return candidate, "", err
	}
	stateSHA, err := StateSHA256(string(candidate.State))
	if err != nil {
		return candidate, "", fmt.Errorf("invalid backfill candidate checkpoint: %w", err)
	}
	if candidate.Version != 1 || req.LaunchID == "" || candidate.ExecutionID != req.LaunchID || candidate.LaunchID != req.LaunchID || candidate.SourceSHA256 != RawSHA256(files["source.json"]) || candidate.DestinationSHA256 != RawSHA256(files["destination.json"]) || candidate.CatalogSHA256 != catalogSHA || candidate.StateSHA256 != stateSHA {
		return candidate, "", fmt.Errorf("backfill candidate does not bind original runtime, frozen configurations and canonical checkpoint")
	}
	var source struct {
		UpdateMethod struct {
			ReplicationSlot string `json:"replication_slot"`
		} `json:"update_method"`
	}
	if err := json.Unmarshal([]byte(files["source.json"]), &source); err != nil {
		return candidate, "", err
	}
	if strings.TrimSpace(candidate.ServerIdentity) == "" || candidate.SlotName == "" || candidate.SlotName != source.UpdateMethod.ReplicationSlot {
		return candidate, "", fmt.Errorf("backfill candidate has no original PostgreSQL identity or differs from the frozen replication slot")
	}
	if _, lower, err := OrdinaryBackfillRecoverySelection(files, string(candidate.State)); err != nil {
		return candidate, "", err
	} else if boundary, err := ParseRecoveryLSN(lower); err != nil || boundary == 0 {
		return candidate, "", fmt.Errorf("backfill candidate requires a valid nonzero original GLOBAL LSN")
	}
	return candidate, data, nil
}

func backfillRecoveryStreams(state string) ([]backfillRecoveryStream, error) {
	var checkpoint struct {
		Streams []backfillRecoveryStream `json:"streams"`
	}
	if err := json.Unmarshal([]byte(state), &checkpoint); err != nil {
		return nil, err
	}
	return checkpoint.Streams, nil
}

func recoveryChunkSHA256(chunk json.RawMessage) (string, error) {
	var bounds map[string]json.RawMessage
	if err := json.Unmarshal(chunk, &bounds); err != nil {
		return "", fmt.Errorf("backfill resume plan has invalid chunk bounds")
	}
	if len(bounds) != 2 || bounds["min"] == nil || bounds["max"] == nil {
		return "", fmt.Errorf("backfill resume plan requires exact min/max chunk bounds")
	}
	return StateSHA256(string(chunk))
}

func ValidateBackfillRecoveryLowerBound(saved, candidate string) error {
	type checkpointState struct {
		Type   string `json:"type"`
		Global struct {
			State struct {
				LSN string `json:"lsn"`
			} `json:"state"`
		} `json:"global"`
	}
	var checkpoint checkpointState
	if err := json.Unmarshal([]byte(saved), &checkpoint); err != nil {
		return err
	}
	if checkpoint.Type != "GLOBAL" {
		return nil
	}
	lower, err := ParseRecoveryLSN(checkpoint.Global.State.LSN)
	if err != nil {
		return fmt.Errorf("saved GLOBAL checkpoint has no valid original WAL boundary")
	}
	checkpoint = checkpointState{}
	if err := json.Unmarshal([]byte(candidate), &checkpoint); err != nil {
		return err
	}
	if checkpoint.Type != "GLOBAL" {
		return fmt.Errorf("backfill candidate requires its original GLOBAL WAL boundary")
	}
	candidateLower, err := ParseRecoveryLSN(checkpoint.Global.State.LSN)
	if err != nil || lower != candidateLower {
		return fmt.Errorf("backfill candidate changed the authoritative original GLOBAL WAL boundary")
	}
	return nil
}

func validateBackfillRecoveryPlans(state string, selected, completed map[string]bool) error {
	streams, err := backfillRecoveryStreams(state)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, stream := range streams {
		id := stream.Namespace + "." + stream.Stream
		if !selected[id] || seen[id] || (stream.SyncMode != "cdc" && stream.SyncMode != "strict_cdc") {
			return fmt.Errorf("backfill candidate contains duplicate, unselected or non-CDC stream state")
		}
		seen[id] = true
		if stream.State.Table == nil || stream.State.Table.TableUUID == "" || !validRecoverySHA256(stream.State.Table.ShapeSHA256) {
			return fmt.Errorf("backfill candidate requires immutable table identity and shape for every started stream")
		}
		if len(stream.State.BackfillPlan) == 0 {
			return fmt.Errorf("every started backfill stream requires its retained full immutable chunk plan")
		}
		plan := make(map[string]bool)
		for _, chunk := range stream.State.BackfillPlan {
			sha, err := recoveryChunkSHA256(chunk)
			if err != nil {
				return err
			}
			if plan[sha] {
				return fmt.Errorf("backfill retained plan contains duplicate chunks")
			}
			plan[sha] = true
		}
		if completed[id] {
			if len(stream.State.ResumePlan) != 0 || len(stream.State.Chunks) != 0 {
				return fmt.Errorf("completed backfill stream retains unfinished chunk work")
			}
			continue
		}
		if stream.State.Chunks == nil || !sameBackfillRecoveryPlan(stream.State.ResumePlan, stream.State.BackfillPlan) {
			return fmt.Errorf("started incomplete backfill stream requires the full retained resume plan and pending chunk list")
		}
		pending := make(map[string]bool)
		for _, chunk := range stream.State.Chunks {
			sha, err := recoveryChunkSHA256(chunk)
			if err != nil {
				return err
			}
			if !plan[sha] || pending[sha] {
				return fmt.Errorf("backfill pending chunks are not a unique subset of the immutable resume plan")
			}
			pending[sha] = true
		}
	}
	for id := range completed {
		if !seen[id] {
			return fmt.Errorf("completed backfill membership has no immutable table identity")
		}
	}
	return nil
}

func sameBackfillRecoveryPlan(a, b []json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	chunks := make(map[string]bool, len(a))
	for _, chunk := range a {
		sha, err := recoveryChunkSHA256(chunk)
		if err != nil || chunks[sha] {
			return false
		}
		chunks[sha] = true
	}
	for _, chunk := range b {
		sha, err := recoveryChunkSHA256(chunk)
		if err != nil || !chunks[sha] {
			return false
		}
		delete(chunks, sha)
	}
	return true
}

func ValidateBackfillRecoveryFinalState(files map[string]string, baseline, final string, tables []OrdinaryRecoveryTableProof) error {
	selected, _, err := OrdinaryBackfillRecoverySelection(files, final)
	if err != nil {
		return err
	}
	original, err := backfillRecoveryStreams(baseline)
	if err != nil {
		return err
	}
	settled, err := backfillRecoveryStreams(final)
	if err != nil {
		return err
	}
	bindings := make(map[string]backfillRecoveryStream)
	for _, stream := range settled {
		if len(stream.State.ResumePlan) != 0 || len(stream.State.Chunks) != 0 || stream.State.Table == nil {
			return fmt.Errorf("final backfill recovery checkpoint retains unfinished work or lacks table identity")
		}
		bindings[stream.Namespace+"."+stream.Stream] = stream
	}
	if len(bindings) != len(selected) {
		return fmt.Errorf("final backfill recovery checkpoint lacks selected table identities")
	}
	for _, stream := range original {
		prior := bindings[stream.Namespace+"."+stream.Stream]
		if stream.State.Table == nil || prior.State.Table == nil || *prior.State.Table != *stream.State.Table || !sameBackfillRecoveryPlan(stream.State.BackfillPlan, prior.State.BackfillPlan) {
			return fmt.Errorf("backfill recovery replaced an original table identity, shape or full chunk plan")
		}
	}
	for _, table := range tables {
		binding := bindings[table.Stream]
		if binding.State.Table == nil || binding.State.Table.TableUUID != table.TableUUID {
			return fmt.Errorf("backfill recovery table proof differs from checkpoint table identity")
		}
	}
	return nil
}
