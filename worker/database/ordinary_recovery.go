package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/lib/pq"
)

func (db *DB) ordinaryRecoveryBinding(ctx context.Context, tx *sql.Tx, req *types.ExecutionRequest) error {
	origin, _, files, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
	if err != nil {
		return err
	}
	query, err := db.jobDataQuery(ctx)
	if err != nil {
		return err
	}
	job, err := scanJobData(tx.QueryRowContext(ctx, query+" FOR SHARE OF j, s, d", req.JobID))
	if err != nil {
		return err
	}
	return utils.ValidateOrdinaryRecoveryJob(job, files, origin.SavedCheckpointSHA256())
}

// CompleteOrdinaryRecovery has one DB commit point: a lost acknowledgement can
// never leave a new checkpoint with an unresolved old owner. Proof is read while
// holding the same catalog/owner authority as the checkpoint and phase update.
func (db *DB) CompleteOrdinaryRecovery(ctx context.Context, req *types.ExecutionRequest, verify func(context.Context) (string, error)) error {
	if req.Command != types.Sync || !req.Controlled || !req.RecoverOnly || req.RuntimeID == "" || req.RuntimeID == req.LaunchID || req.ResourceID == "" || verify == nil {
		return fmt.Errorf("ordinary recovery completion requires a fresh controlled sync runtime")
	}
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		r, err := db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != req.RunID || r.RuntimeID != req.RuntimeID || r.LaunchID != req.LaunchID || r.ResourceID != req.ResourceID || r.ResourceUID != req.ResourceUID || !r.Controlled || r.Phase != "closing" || r.Outcome != "recovery" {
			return ErrExecutionClosed
		}
		if err := utils.RequireClosedExecutionFence(ctx, req); err != nil {
			return err
		}
		if err := db.ordinaryRecoveryBinding(ctx, tx, req); err != nil {
			return err
		}
		state, err := verify(ctx)
		if err != nil {
			return err
		}
		if _, err := utils.StateSHA256(state); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET state=$4,updated_at=now() WHERE id=$1 AND execution_workflow_id=$2 AND execution_run_id=$3`, pq.QuoteIdentifier(db.tables["job"])), req.JobID, req.WorkflowID, req.RunID, state); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET phase='closed',outcome='settled',updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND owner_run_id=$3`, db.executionTable()), req.JobID, req.WorkflowID, req.RunID)
		return err
	})
}
