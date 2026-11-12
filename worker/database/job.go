package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/lib/pq"
)

const queryTimeout = 5 * time.Second

const columnExistsQuery = `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.columns
	WHERE table_schema = current_schema()
	  AND table_name = $1
	  AND column_name = $2
)`

func columnExists(ctx context.Context, db *DB, table, column string) (bool, error) {
	var exists bool
	if err := db.client.QueryRowContext(ctx, columnExistsQuery, table, column).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check column %s on %s: %w", column, table, err)
	}
	return exists, nil
}

// optionalColumn returns the select expression for a job column that may not exist yet.
func optionalColumn(ctx context.Context, db *DB, table, column string) (string, error) {
	exists, err := columnExists(ctx, db, table, column)
	if err != nil {
		return "", err
	}
	if !exists {
		return "CAST(NULL AS TEXT)", nil
	}
	return "j." + column, nil
}

// decryptJobData decrypts the Source and Destination config fields of a JobData.
// If OLAKE_SECRET_KEY is not configured, Decrypt returns the value unchanged.
func decryptJobData(jobData *types.JobData) error {
	decryptedSource, err := utils.Decrypt(jobData.Source)
	if err != nil {
		return fmt.Errorf("failed to decrypt source config: %s", err)
	}
	jobData.Source = decryptedSource

	decryptedDest, err := utils.Decrypt(jobData.Destination)
	if err != nil {
		return fmt.Errorf("failed to decrypt destination config: %s", err)
	}
	jobData.Destination = decryptedDest

	return nil
}

func (db *DB) GetJobData(ctx context.Context, jobId int) (types.JobData, error) {
	log := logger.Log(ctx)
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	query, err := db.jobDataQuery(cctx)
	if err != nil {
		return types.JobData{}, err
	}
	jobData, err := scanJobData(db.client.QueryRowContext(cctx, query, jobId))
	if err != nil {
		log.Error("failed to get job data from database", "jobID", jobId, "error", err)
	}
	return jobData, err
}

// The same joined projection is used by preflight's locked comparison and by
// ordinary reads, including optional split-catalog columns on older databases.
func (db *DB) jobDataQuery(ctx context.Context) (string, error) {
	jobTable := db.tables["job"]
	selectedStreamsExpr, err := optionalColumn(ctx, db, jobTable, "selected_streams_config")
	if err != nil {
		return "", err
	}
	availableStreamsExpr, err := optionalColumn(ctx, db, jobTable, "available_streams_config")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
		SELECT j.name, j.streams_config, %s, %s, j.state, j.project_id,
			s.config, d.config, s.version, s.type, COALESCE(j.advanced_settings::text, ''),
			j.frequency, j.created_at, d.version, s.name, d.name, j.source_id, j.dest_id, d.dest_type
		FROM %q j
		JOIN %q s ON j.source_id = s.id
		JOIN %q d ON j.dest_id = d.id
		WHERE j.id = $1`,
		selectedStreamsExpr, availableStreamsExpr, jobTable, db.tables["source"], db.tables["dest"]), nil
}

func scanJobData(row *sql.Row) (types.JobData, error) {
	var jobData types.JobData
	var streams, selectedStreams, availableStreams sql.NullString
	if err := row.Scan(&jobData.JobName, &streams, &selectedStreams, &availableStreams,
		&jobData.State, &jobData.ProjectID, &jobData.Source, &jobData.Destination, &jobData.Version,
		&jobData.Driver, &jobData.AdvancedSettings, &jobData.Frequency, &jobData.CreatedAt,
		&jobData.DestinationVersion, &jobData.SourceName, &jobData.DestinationName,
		&jobData.SourceID, &jobData.DestinationID, &jobData.DestinationDriver); err != nil {
		return types.JobData{}, fmt.Errorf("failed to scan job data: %w", err)
	}
	jobData.Streams, jobData.SelectedStreams, jobData.AvailableStreams = streams.String, selectedStreams.String, availableStreams.String
	if err := decryptJobData(&jobData); err != nil {
		return types.JobData{}, fmt.Errorf("failed to decrypt job data: %w", err)
	}
	return jobData, nil
}

func (db *DB) UpdateJobState(ctx context.Context, jobId int, workflowID, state string) error {
	log := logger.Log(ctx)

	log.Info("updating job state", "jobID", jobId, "state", state)

	tableName := pq.QuoteIdentifier(db.tables["job"])
	query := fmt.Sprintf(`
			UPDATE %s
			SET state = $1, updated_at = NOW() 
			WHERE id = $2 AND execution_workflow_id = $3`,
		tableName)

	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	result, err := db.client.ExecContext(cctx, query, state, jobId, workflowID)
	if err != nil {
		log.Error("failed to update job state", "jobID", jobId, "error", err)
		return fmt.Errorf("failed to update job state: %s", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrJobExecutionOwned
	}

	log.Info("successfully updated job state", "jobID", jobId, "state", state)

	return nil
}
