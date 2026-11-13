package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/lib/pq"
)

var ErrJobExecutionOwned = errors.New("job execution belongs to another controlling run")
var ErrExecutionClosed = errors.New("execution admission is sealed")
var ErrJobExecutionInactive = errors.New("job is paused; queued execution is no longer authorized")

type ExecutionRecord struct {
	JobID                                                                 int
	WorkflowID, OwnerRunID, LaunchID, LauncherAttemptID, Phase, RuntimeID string
	ResourceID, ResourceUID, Outcome, StateSHA256, CatalogSHA256          string
	HandoffToken, HandoffRequestID, HandoffRunID                          string
	HandoffActive                                                         bool
	Controlled                                                            bool
}

func (db *DB) executionTable() string {
	return pq.QuoteIdentifier(db.tables["job"] + "_execution_records")
}

func (db *DB) executionTx(ctx context.Context, jobID int, fn func(*sql.Tx) error) error {
	tx, err := db.client.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, catalogEditFenceQuery, fmt.Sprintf("olake:catalog-edit:%d", jobID)); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) readExecution(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}, jobID int, workflowID string) (*ExecutionRecord, error) {
	r := &ExecutionRecord{JobID: jobID, WorkflowID: workflowID}
	err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT owner_run_id,launch_id,launcher_attempt_id,phase,runtime_id,resource_id,resource_uid,outcome,state_sha256,catalog_sha256,handoff_token,handoff_request_id,handoff_run_id,handoff_active,controlled FROM %s WHERE job_id=$1 AND workflow_id=$2`, db.executionTable()), jobID, workflowID).Scan(&r.OwnerRunID, &r.LaunchID, &r.LauncherAttemptID, &r.Phase, &r.RuntimeID, &r.ResourceID, &r.ResourceUID, &r.Outcome, &r.StateSHA256, &r.CatalogSHA256, &r.HandoffToken, &r.HandoffRequestID, &r.HandoffRunID, &r.HandoffActive, &r.Controlled)
	return r, err
}

func (db *DB) GetExecution(ctx context.Context, jobID int, workflowID string) (*ExecutionRecord, error) {
	return db.readExecution(ctx, db.client, jobID, workflowID)
}

func (db *DB) checkOwner(ctx context.Context, tx *sql.Tx, req *types.ExecutionRequest) error {
	var owned bool
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id=$1 AND execution_workflow_id=$2 AND execution_run_id=$3)`, pq.QuoteIdentifier(db.tables["job"])), req.JobID, req.WorkflowID, req.RunID).Scan(&owned)
	if err != nil {
		return err
	}
	if !owned {
		return ErrJobExecutionOwned
	}
	return nil
}

func (db *DB) AcquireJobExecution(ctx context.Context, req *types.ExecutionRequest) error {
	if strings.TrimSpace(req.WorkflowID) == "" || strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("execution requires workflow and controlling run IDs")
	}
	query, err := db.jobDataQuery(ctx)
	if err != nil {
		return err
	}
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		canonical, err := scanJobData(tx.QueryRowContext(ctx, query+" FOR SHARE OF j, s, d", req.JobID))
		if err != nil {
			return err
		}
		controlled, err := canonical.SupportsAtomicResync()
		if err != nil {
			return err
		}
		if req.Command == types.Sync || req.Command == types.Resync {
			var active bool
			if err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT active FROM %s WHERE id=$1`, pq.QuoteIdentifier(db.tables["job"])), req.JobID).Scan(&active); err != nil {
				return err
			}
			if !active {
				return ErrJobExecutionInactive
			}
		}
		// A trigger may atomically take the pending transfer. No public request can
		// manufacture this token: it is generated only in the durable handoff row.
		if req.AfterResyncOperationID != "" {
			if err := db.acceptHandoff(ctx, tx, req); err != nil {
				return err
			}
		}
		r, err := db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if r.Controlled != controlled {
				return fmt.Errorf("canonical execution domain changed; retained operation must be reconciled without downgrade")
			}
			if r.OwnerRunID != req.RunID || r.Phase == "handed_off" {
				return ErrExecutionClosed
			}
			if r.Phase == "closed" {
				if r.Outcome != "retired" {
					return ErrExecutionClosed
				}
				if err := db.checkOwner(ctx, tx, req); err != nil {
					return ErrExecutionClosed
				}
			}
		}
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET execution_workflow_id=$1,execution_run_id=$2 WHERE id=$3 AND ((execution_workflow_id IS NULL AND execution_run_id IS NULL) OR (execution_workflow_id=$1 AND execution_run_id=$2)) AND NOT EXISTS(SELECT 1 FROM %s pending WHERE pending.job_id=$3 AND pending.phase='handoff' AND pending.workflow_id<>$1)`, pq.QuoteIdentifier(db.tables["job"]), db.executionTable()), req.WorkflowID, req.RunID, req.JobID)
		if err != nil {
			return fmt.Errorf("acquire execution (upgrade UI database schema first): %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrJobExecutionOwned
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s(job_id,workflow_id,owner_run_id,phase,controlled) VALUES($1,$2,$3,'unadmitted',$4) ON CONFLICT(job_id,workflow_id) DO NOTHING`, db.executionTable()), req.JobID, req.WorkflowID, req.RunID, controlled)
		return err
	})
}

// Admission and prelaunch closure are one SQL decision. The admitted attempt is
// immutable; retries only attach, even if their predecessor has no resource yet.
func (db *DB) AdmitExecution(ctx context.Context, req *types.ExecutionRequest, attemptID, runtimeID string) (*ExecutionRecord, bool, error) {
	var r *ExecutionRecord
	admitted := false
	err := db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		var err error
		r, err = db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != req.RunID {
			return ErrJobExecutionOwned
		}
		if !r.Controlled {
			if req.Command == types.Resync {
				return fmt.Errorf("atomic resync requires canonical PostgreSQL to REST-family Iceberg destination")
			}
			runtimeID = ""
		}
		if r.Phase != "unadmitted" {
			if r.Phase != "admitted" {
				return ErrExecutionClosed
			}
			return nil
		}
		launchID := r.LaunchID
		if launchID == "" {
			launchID = runtimeID
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET launch_id=$3,runtime_id=$4,launcher_attempt_id=$5,phase='admitted',updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND phase='unadmitted' AND owner_run_id=$6`, db.executionTable()), req.JobID, req.WorkflowID, launchID, runtimeID, attemptID, req.RunID)
		if err != nil {
			return err
		}
		r.LaunchID = launchID
		r.RuntimeID = runtimeID
		r.LauncherAttemptID = attemptID
		r.Phase = "admitted"
		admitted = true
		return nil
	})
	return r, admitted, err
}

func (db *DB) GuardExecution(ctx context.Context, req *types.ExecutionRequest, fn func(context.Context, *ExecutionRecord) error) error {
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		r, err := db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != req.RunID || (req.RuntimeID != "" && r.RuntimeID != req.RuntimeID) || (req.LaunchID != "" && r.LaunchID != req.LaunchID) || (req.ResourceID != "" && r.ResourceID != req.ResourceID) || (req.ResourceUID != "" && r.ResourceUID != req.ResourceUID) {
			return ErrJobExecutionOwned
		}
		return fn(ctx, r)
	})
}

func (db *DB) CheckLaunch(ctx context.Context, req *types.ExecutionRequest) error {
	return db.GuardExecution(ctx, req, func(ctx context.Context, r *ExecutionRecord) error {
		if !req.AllowLaunch || r.Phase != "admitted" || r.LauncherAttemptID != req.LauncherAttemptID {
			return ErrExecutionClosed
		}
		return utils.ReadExecutionFence(ctx, req)
	})
}

func (db *DB) RecordExecutionResource(ctx context.Context, req *types.ExecutionRequest, id, uid string) error {
	if id == "" {
		return fmt.Errorf("immutable provider resource ID is required")
	}
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET resource_id=$4,resource_uid=$5,updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND owner_run_id=$3 AND runtime_id=$6 AND launch_id=$7 AND phase IN ('admitted','closing') AND (resource_id='' OR (resource_id=$4 AND resource_uid=$5))`, db.executionTable()), req.JobID, req.WorkflowID, req.RunID, id, uid, req.RuntimeID, req.LaunchID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrExecutionClosed
		}
		return nil
	})
}

func (db *DB) SealExecution(ctx context.Context, req *types.ExecutionRequest) (*ExecutionRecord, error) {
	var r *ExecutionRecord
	err := db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		var err error
		r, err = db.readExecution(ctx, tx, req.JobID, req.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != req.RunID {
			return ErrJobExecutionOwned
		}
		phase := "closing"
		outcome := r.Outcome
		if r.Phase == "unadmitted" {
			phase = "closed"
			outcome = "not_launched"
		}
		if r.Phase == "closed" || r.Phase == "handoff" || r.Phase == "handed_off" {
			return nil
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET phase=$3,outcome=$4,updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND owner_run_id=$5`, db.executionTable()), req.JobID, req.WorkflowID, phase, outcome, req.RunID)
		r.Phase = phase
		r.Outcome = outcome
		return err
	})
	return r, err
}

func (db *DB) FinishExecution(ctx context.Context, req *types.ExecutionRequest, outcome string) error {
	return db.executionTx(ctx, req.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, req); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET phase='closed',outcome=$4,resource_id=$5,resource_uid=$6,updated_at=now() WHERE job_id=$1 AND workflow_id=$2 AND owner_run_id=$3 AND runtime_id=$7 AND launch_id=$8 AND phase IN ('closing','closed') AND (resource_id='' OR resource_id=$5) AND (resource_uid='' OR resource_uid=$6)`, db.executionTable()), req.JobID, req.WorkflowID, req.RunID, outcome, req.ResourceID, req.ResourceUID, req.RuntimeID, req.LaunchID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrExecutionClosed
		}
		return nil
	})
}

// Caller has positively retired the old runtime while holding GuardExecution.
// A fresh data launch is never admitted after adoption: only resync recovery.
func (db *DB) AdoptExecution(ctx context.Context, oldReq, newReq *types.ExecutionRequest) error {
	if oldReq.RunID == "" || newReq.RunID == "" || oldReq.RunID == newReq.RunID || oldReq.WorkflowID != newReq.WorkflowID || oldReq.JobID != newReq.JobID {
		return fmt.Errorf("epoch adoption requires distinct nonempty runs of the same operation")
	}
	return db.executionTx(ctx, oldReq.JobID, func(tx *sql.Tx) error {
		if err := db.checkOwner(ctx, tx, oldReq); err != nil {
			return err
		}
		r, err := db.readExecution(ctx, tx, oldReq.JobID, oldReq.WorkflowID)
		if err != nil {
			return err
		}
		if r.OwnerRunID != oldReq.RunID || (r.Phase != "closing" && r.Phase != "closed" && r.Phase != "handoff") {
			return ErrExecutionClosed
		}
		retiredWithoutPublication := r.Phase == "closed" && (r.Outcome == "not_launched" || r.Outcome == "retired" || (newReq.Command == types.Resync && r.Outcome == "not_published"))
		if retiredWithoutPublication || (r.LaunchID != "" && newReq.Command != types.Resync) {
			if !retiredWithoutPublication && (r.Phase != "closed" || (r.Outcome != "settled" && r.Outcome != "not_started" && r.Outcome != "retired")) {
				return fmt.Errorf("ordinary execution cannot retire without positive settled outcome proof")
			}
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET owner_run_id=$3,outcome='retired',updated_at=now() WHERE job_id=$1 AND workflow_id=$2`, db.executionTable()), oldReq.JobID, oldReq.WorkflowID, newReq.RunID)
		} else if r.Phase == "handoff" {
			sha, catalogSHA, active, bindingErr := db.handoffBinding(ctx, tx, oldReq.JobID)
			if bindingErr != nil {
				return bindingErr
			}
			if sha != r.StateSHA256 || catalogSHA != r.CatalogSHA256 || active != r.HandoffActive || r.HandoffToken == "" || r.HandoffRunID == "" || r.HandoffRequestID == "" {
				return fmt.Errorf("cannot adopt a handoff whose transfer binding changed")
			}
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET owner_run_id=$3,updated_at=now() WHERE job_id=$1 AND workflow_id=$2`, db.executionTable()), oldReq.JobID, oldReq.WorkflowID, newReq.RunID)
		} else {
			outcome := ""
			if r.LaunchID != "" {
				outcome = "recovery"
			}
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET owner_run_id=$3,runtime_id='',launcher_attempt_id='',resource_id='',resource_uid='',phase='unadmitted',outcome=$4,updated_at=now() WHERE job_id=$1 AND workflow_id=$2`, db.executionTable()), oldReq.JobID, oldReq.WorkflowID, newReq.RunID, outcome)
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET execution_run_id=$3 WHERE id=$1 AND execution_workflow_id=$2 AND execution_run_id=$4`, pq.QuoteIdentifier(db.tables["job"])), oldReq.JobID, oldReq.WorkflowID, newReq.RunID, oldReq.RunID)
		return err
	})
}

func (db *DB) ReleaseJobExecution(ctx context.Context, req *types.ExecutionRequest) error {
	// Release need not wait for the catalog advisory fence: one owner-and-phase
	// conditional SQL statement is its linearization point, after retirement.
	result, err := db.client.ExecContext(ctx, fmt.Sprintf(`UPDATE %s j SET execution_workflow_id=NULL,execution_run_id=NULL FROM %s r WHERE j.id=$1 AND j.execution_workflow_id=$2 AND j.execution_run_id=$3 AND r.job_id=j.id AND r.workflow_id=$2 AND r.owner_run_id=$3 AND r.phase IN ('closed','handed_off')`, pq.QuoteIdentifier(db.tables["job"]), db.executionTable()), req.JobID, req.WorkflowID, req.RunID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	var alreadyReleased bool
	err = db.client.QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s j JOIN %s r ON r.job_id=j.id WHERE j.id=$1 AND j.execution_workflow_id IS NULL AND j.execution_run_id IS NULL AND r.workflow_id=$2 AND r.owner_run_id=$3 AND r.phase IN ('closed','handed_off'))`, pq.QuoteIdentifier(db.tables["job"]), db.executionTable()), req.JobID, req.WorkflowID, req.RunID).Scan(&alreadyReleased)
	if err != nil {
		return err
	}
	if alreadyReleased {
		return nil
	}
	return ErrJobExecutionOwned
}
