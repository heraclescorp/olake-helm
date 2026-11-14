package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

var ErrResyncUnconfirmed = errors.New("resync publication outcome is unconfirmed")

type resyncPublication struct {
	Version     int    `json:"version"`
	ResyncID    string `json:"resync_id"`
	ExecutionID string `json:"execution_id"`
	LaunchID    string `json:"launch_id"`
	Namespace   string `json:"namespace"`
	StreamName  string `json:"stream_name"`
	TableUUID   string `json:"table_uuid"`
	SnapshotID  int64  `json:"snapshot_id"`
	StateSHA256 string `json:"state_sha256"`
}

// CleanupResync never reads candidate state from a failed or cancelled command.
// A successful exit also needs the connector's publication receipt, bound to the
// final complete state. The receipt is emitted only after atomic publication.
func (a *AbstractExecutor) CleanupResync(ctx context.Context, req *types.ExecutionRequest, succeeded bool) error {
	if req.Command != types.Resync {
		return fmt.Errorf("resync cleanup requires resync command")
	}
	r, err := a.prepareRetirement(ctx, req)
	if err != nil {
		return err
	}
	if r.Phase == "handoff" || r.Phase == "handed_off" {
		if !succeeded {
			return fmt.Errorf("%w: published resync has a bound pending handoff; reconcile the same operation to finish its transfer", ErrResyncUnconfirmed)
		}
		return nil
	}
	if r.Phase == "closed" {
		if !succeeded && r.Outcome == "published" {
			return fmt.Errorf("%w: publication is confirmed but its handoff is unfinished; recover the same operation instead of releasing it as canceled", ErrResyncUnconfirmed)
		}
		return nil
	}
	err = a.db.GuardExecution(ctx, req, func(ctx context.Context, current *database.ExecutionRecord) error {
		applyRecord(req, current)
		if err := utils.CloseExecutionFence(ctx, req); err != nil {
			return err
		}
		if err := a.executor.Cleanup(ctx, req); err != nil {
			return err
		}
		if !succeeded {
			if err := confirmedUnpublishedResync(ctx, req); err != nil {
				return fmt.Errorf("%w: %v", ErrResyncUnconfirmed, err)
			}
			return nil
		}
		state, err := confirmedResyncState(ctx, req)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrResyncUnconfirmed, err)
		}
		return a.db.UpdateJobState(ctx, req.JobID, req.WorkflowID, req.RunID, state)
	})
	if err != nil {
		return err
	}
	outcome := "not_published"
	if succeeded {
		outcome = "published"
	}
	return a.db.FinishExecution(ctx, req, outcome)
}

func confirmedResyncState(ctx context.Context, req *types.ExecutionRequest) (string, error) {
	target, err := utils.ResyncTargetFromRequest(req)
	if err != nil {
		return "", err
	}
	_, dir := utils.GetExecutionDirAndSubDir(req)
	data, err := storage.ReadFile(ctx, dir, constants.ResyncResultFile, false)
	if err != nil {
		return "", fmt.Errorf("resync publication is unconfirmed: %w", err)
	}
	var receipt resyncPublication
	if err := json.Unmarshal([]byte(data), &receipt); err != nil {
		return "", fmt.Errorf("invalid resync publication receipt: %w", err)
	}
	if receipt.Version != 1 || receipt.ResyncID != target.ID || receipt.ExecutionID != req.RuntimeID || receipt.LaunchID != req.LaunchID || req.RuntimeID == "" || req.LaunchID == "" || receipt.Namespace != target.Namespace || receipt.StreamName != target.Table || strings.TrimSpace(receipt.TableUUID) == "" || receipt.SnapshotID <= 0 {
		return "", fmt.Errorf("resync publication receipt does not confirm this operation and target")
	}
	state, err := storage.ReadFile(ctx, dir, "state.json", false)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(strings.NewReader(state))
	decoder.UseNumber()
	var object map[string]interface{}
	if err := decoder.Decode(&object); err != nil || object == nil {
		return "", fmt.Errorf("resync state must be a JSON object")
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return "", fmt.Errorf("resync state has trailing JSON")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	if receipt.StateSHA256 != hex.EncodeToString(digest[:]) {
		return "", fmt.Errorf("resync publication receipt does not match final state")
	}
	return state, nil
}

func confirmedUnpublishedResync(ctx context.Context, req *types.ExecutionRequest) error {
	target, err := utils.ResyncTargetFromRequest(req)
	if err != nil {
		return err
	}
	_, dir := utils.GetExecutionDirAndSubDir(req)
	data, err := storage.ReadFile(ctx, dir, constants.ResyncFailureFile, false)
	if err != nil {
		return fmt.Errorf("resync publication outcome is unknown; job remains paused and owned by %s; reset/retry this same Temporal workflow ID to reconcile: %w", target.ID, err)
	}
	var receipt struct {
		Version     int    `json:"version"`
		ResyncID    string `json:"resync_id"`
		ExecutionID string `json:"execution_id"`
		LaunchID    string `json:"launch_id"`
		Namespace   string `json:"namespace"`
		StreamName  string `json:"stream_name"`
		Publication string `json:"publication"`
	}
	if err := json.Unmarshal([]byte(data), &receipt); err != nil {
		return fmt.Errorf("invalid resync failure receipt: %w", err)
	}
	if receipt.Version != 1 || receipt.ResyncID != target.ID || receipt.ExecutionID != req.RuntimeID || receipt.LaunchID != req.LaunchID || req.RuntimeID == "" || req.LaunchID == "" || req.RecoverOnly || receipt.Namespace != target.Namespace || receipt.StreamName != target.Table || receipt.Publication != "not_published" {
		return fmt.Errorf("resync failure receipt does not prove this operation unpublished; retain ownership and retry the same workflow ID %s", target.ID)
	}
	return nil
}
