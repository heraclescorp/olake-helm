package database

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

var ErrJobExecutionOwned = errors.New("job execution is owned by another workflow")

// Ownership has no lease: a connector can outlive its worker. Only the same
// workflow may adopt it, and cleanup may release only its own claim.
func (db *DB) AcquireJobExecution(ctx context.Context, jobID int, workflowID string) error {
	if strings.TrimSpace(workflowID) == "" {
		return fmt.Errorf("job execution requires a stable workflow id")
	}
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	tx, err := db.client.BeginTx(cctx, nil)
	if err != nil {
		return err
	}
	// UI clear dispatch returns after triggering, not after worker completion.
	// Claims can therefore wait for its catalog fence; state/owner cleanup does
	// not take this fence, so cancellation of the previous sync can finish.
	defer tx.Rollback()
	if _, err := tx.ExecContext(cctx, catalogEditFenceQuery, fmt.Sprintf("olake:catalog-edit:%d", jobID)); err != nil {
		return fmt.Errorf("failed to acquire execution/catalog fence: %w", err)
	}
	query := fmt.Sprintf(`UPDATE %s SET execution_workflow_id = $1
		WHERE id = $2 AND (execution_workflow_id IS NULL OR execution_workflow_id = $1)`, pq.QuoteIdentifier(db.tables["job"]))
	result, err := tx.ExecContext(cctx, query, workflowID, jobID)
	if err != nil {
		return fmt.Errorf("failed to acquire job execution; upgrade the UI database schema first: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrJobExecutionOwned
	}
	return tx.Commit()
}

func (db *DB) ReleaseJobExecution(ctx context.Context, jobID int, workflowID string) error {
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	query := fmt.Sprintf(`UPDATE %s SET execution_workflow_id = NULL
		WHERE id = $1 AND execution_workflow_id = $2`, pq.QuoteIdentifier(db.tables["job"]))
	_, err := db.client.ExecContext(cctx, query, jobID, workflowID)
	return err
}
