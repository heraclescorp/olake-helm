package database

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/lib/pq"
)

var ErrEnrollmentConflict = errors.New("automatic table enrollment conflicted with a concurrent job, source or destination edit; retry the sync")

const catalogEditFenceQuery = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

// CommitCatalogEnrollment first takes the UI's catalog-edit fence, then locks
// the job and both config rows before comparing preflight's snapshot. The UI
// holds that fence across cancellation/clear without blocking ordinary state
// persistence. This UPDATE never touches state/global LSN, frequency or version.
func (db *DB) CommitCatalogEnrollment(ctx context.Context, req *types.ExecutionRequest, original types.JobData, result types.EnrollmentResult) error {
	jobID := req.JobID
	proposed, err := original.ValidateEnrollment(result)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	query, err := db.jobDataQuery(cctx)
	if err != nil {
		return err
	}
	tx, err := db.client.BeginTx(cctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(cctx, catalogEditFenceQuery, fmt.Sprintf("olake:catalog-edit:%d", jobID)); err != nil {
		return fmt.Errorf("failed to acquire catalog edit fence: %w", err)
	}
	if err := db.checkOwner(cctx, tx, req); err != nil {
		return err
	}
	execution, err := db.readExecution(cctx, tx, jobID, req.WorkflowID)
	if err != nil {
		return err
	}
	if execution.OwnerRunID != req.RunID || execution.Phase != "unadmitted" {
		return ErrExecutionClosed
	}
	current, err := scanJobData(tx.QueryRowContext(cctx, query+" FOR UPDATE OF j, s, d", jobID))
	if err != nil {
		return err
	}
	if err := SameEnrollmentContext(original, current); err != nil {
		return err
	}
	if err := cctx.Err(); err != nil {
		return err
	}
	if len(result.Enrolled) != 0 {
		var update string
		var args []any
		if original.AvailableStreams != "" && original.SelectedStreams != "" {
			update = fmt.Sprintf(`UPDATE %s SET available_streams_config = $1, selected_streams_config = $2,
				advanced_settings = $3::jsonb, updated_at = NOW() WHERE id = $4`, pq.QuoteIdentifier(db.tables["job"]))
			args = []any{proposed.AvailableStreams, proposed.SelectedStreams, proposed.AdvancedSettings, jobID}
		} else {
			update = fmt.Sprintf(`UPDATE %s SET streams_config = $1, advanced_settings = $2::jsonb,
				updated_at = NOW() WHERE id = $3`, pq.QuoteIdentifier(db.tables["job"]))
			args = []any{proposed.Streams, proposed.AdvancedSettings, jobID}
		}
		changed, err := tx.ExecContext(cctx, update, args...)
		if err != nil {
			return err
		}
		count, err := changed.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return ErrEnrollmentConflict
		}
	}
	if err := cctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

// SameEnrollmentContext also guards the post-commit read before ingestion.
func SameEnrollmentContext(original, current types.JobData) error {
	before, err := original.CatalogRevision()
	if err != nil {
		return err
	}
	after, err := current.CatalogRevision()
	if err != nil {
		return err
	}
	if before != after || original.SourceID != current.SourceID || original.DestinationID != current.DestinationID ||
		original.ProjectID != current.ProjectID || original.Driver != current.Driver ||
		original.DestinationDriver != current.DestinationDriver || original.Version != current.Version ||
		original.DestinationVersion != current.DestinationVersion {
		return ErrEnrollmentConflict
	}
	for _, pair := range [][2]string{{original.Source, current.Source}, {original.Destination, current.Destination}} {
		before, err := types.CanonicalJSON(pair[0])
		if err != nil {
			return fmt.Errorf("invalid original enrollment connection config")
		}
		after, err := types.CanonicalJSON(pair[1])
		if err != nil || !reflect.DeepEqual(before, after) {
			return ErrEnrollmentConflict
		}
	}
	return nil
}
