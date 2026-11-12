package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/lib/pq"
)

func (db *DB) handoffBinding(ctx context.Context, tx *sql.Tx, jobID int) (stateSHA, catalogSHA string, active bool, err error) {
	var state, streams, available, selected string
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT state,active,COALESCE(streams_config,''),COALESCE(available_streams_config,''),COALESCE(selected_streams_config,'') FROM %s WHERE id=$1`, pq.QuoteIdentifier(db.tables["job"])), jobID).Scan(&state, &active, &streams, &available, &selected)
	if err != nil {
		return
	}
	stateSHA, err = utils.StateSHA256(state)
	if err != nil {
		return
	}
	data, _ := json.Marshal([]string{streams, available, selected})
	digest := sha256.Sum256(data)
	catalogSHA = hex.EncodeToString(digest[:])
	return
}

func (db *DB) BeginHandoff(ctx context.Context, req *types.ExecutionRequest, token string) error {
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		r, err := db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != req.RunID {
			return ErrJobExecutionOwned
		}
		if r.Phase == "handoff" || r.Phase == "handed_off" {
			return nil
		}
		if r.Phase != "closed" || r.Outcome != "published" {
			return ErrExecutionClosed
		}
		sha, catalogSHA, active, err := db.handoffBinding(ctx, tx, req.JobID)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET phase='handoff',state_sha256=$4,handoff_token=$5,handoff_active=$6,handoff_request_id=$7,catalog_sha256=$8,handoff_run_id=$3,updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND owner_run_id=$3`, db.executionTable()), req.JobID, req.WorkflowID, req.RunID, sha, token, active, req.WorkflowID+"/"+req.RunID+"/atomic-resync-sync", catalogSHA)
		return err
	})
}

// The external Temporal request is made while the shared catalog fence excludes
// API mutations. A lost acknowledgement leaves HANDOFF and ownership intact;
// the exact token-bearing trigger can accept the durable transfer instead.
func (db *DB) ApplyHandoff(ctx context.Context, req *types.ExecutionRequest, apply func(context.Context, *ExecutionRecord) error) error {
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		r, err := db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != req.RunID {
			return ErrJobExecutionOwned
		}
		if r.Phase == "handed_off" {
			return nil
		}
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		if r.Phase != "handoff" {
			return ErrExecutionClosed
		}
		sha, catalogSHA, active, err := db.handoffBinding(ctx, tx, req.JobID)
		if err != nil {
			return err
		}
		if sha != r.StateSHA256 || catalogSHA != r.CatalogSHA256 || active != r.HandoffActive {
			return fmt.Errorf("pending handoff no longer matches checkpoint, catalog or active intent")
		}
		if err := apply(ctx, r); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET phase='handed_off',updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND owner_run_id=$3 AND phase='handoff'`, db.executionTable()), req.JobID, req.WorkflowID, req.RunID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET execution_workflow_id=NULL,execution_run_id=NULL WHERE id=$1 AND execution_workflow_id=$2 AND execution_run_id=$3`, pq.QuoteIdentifier(db.tables["job"])), req.JobID, req.WorkflowID, req.RunID)
		return err
	})
}

func (db *DB) acceptHandoff(ctx context.Context, tx *sql.Tx, req *types.ExecutionRequest) error {
	r, err := db.readExecution(ctx, tx, req.JobID, req.AfterResyncOperationID)
	if err != nil {
		return err
	}
	if r.HandoffRunID != req.AfterResyncRunID || r.HandoffToken == "" || r.HandoffToken != req.AfterResyncTransferToken || r.StateSHA256 != req.AfterResyncStateSHA256 {
		return fmt.Errorf("invalid internal resync transfer binding")
	}
	if r.Phase == "handed_off" {
		return nil
	}
	if r.Phase != "handoff" || !r.HandoffActive {
		return fmt.Errorf("resync transfer is not pending and active")
	}
	var matches bool
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id=$1 AND execution_workflow_id=$2 AND execution_run_id=$3)`, pq.QuoteIdentifier(db.tables["job"])), req.JobID, r.WorkflowID, r.OwnerRunID).Scan(&matches); err != nil {
		return err
	}
	if !matches {
		return ErrJobExecutionOwned
	}
	sha, catalogSHA, active, err := db.handoffBinding(ctx, tx, req.JobID)
	if err != nil {
		return err
	}
	if !active || sha != r.StateSHA256 || catalogSHA != r.CatalogSHA256 {
		return fmt.Errorf("resync transfer checkpoint, catalog or active intent changed")
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET phase='handed_off',updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND phase='handoff'`, db.executionTable()), req.JobID, r.WorkflowID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET execution_workflow_id=$2,execution_run_id=$3 WHERE id=$1 AND execution_workflow_id=$4 AND execution_run_id=$5`, pq.QuoteIdentifier(db.tables["job"])), req.JobID, req.WorkflowID, req.RunID, r.WorkflowID, r.OwnerRunID)
	return err
}
