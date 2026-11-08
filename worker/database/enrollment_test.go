package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

const dbEnrollmentSettings = `{"index_required":false,"unrelated":{"keep":9007199254740993},"auto_table_enrollment":{"enabled":true,"schemas":[{"namespace":"public","destination_database":"lake"}],"defaults":{"normalization":true,"append_mode":false,"update_type":"pos","sync_new_columns":false}}}`
const dbOldCatalog = `{"streams":[{"stream":{"namespace":"public","name":"old","destination_database":"lake"}}],"selected_streams":{"public":[{"stream_name":"old","normalization":false,"append_mode":true}]}}`
const dbNewCatalog = `{"streams":[{"stream":{"namespace":"public","name":"old","destination_database":"lake"}},{"stream":{"namespace":"public","name":"new","destination_database":"lake","destination_table":"new","sync_mode":"cdc"}}],"selected_streams":{"public":[{"stream_name":"old","normalization":false,"append_mode":true},{"stream_name":"new","normalization":true,"append_mode":false,"update_type":"pos","selected_columns":{"columns":["id"],"sync_new_columns":false}}]}}`

func dbEnrollmentResult() types.EnrollmentResult {
	return types.EnrollmentResult{Enrolled: []types.EnrollmentID{{Namespace: "public", StreamName: "new"}}, Streams: dbNewCatalog}
}

func TestEnrollmentContextRejectsConcurrentConfigEditsButIgnoresState(t *testing.T) {
	original := types.JobData{Streams: dbOldCatalog, AdvancedSettings: dbEnrollmentSettings, SourceID: 1, DestinationID: 2,
		Driver: "postgres", DestinationDriver: "iceberg", Version: "v1", DestinationVersion: "v2", ProjectID: "project",
		Source: `{"host":"source","port":5432}`, Destination: `{"type":"ICEBERG","writer":{}}`, State: `{"global":{"lsn":"old"}}`}
	for _, field := range []string{"catalog", "policy", "source config", "destination config", "source id", "destination id", "source version", "destination version", "source type", "destination type", "project"} {
		t.Run(field, func(t *testing.T) {
			current := original
			switch field {
			case "catalog":
				current.Streams = strings.Replace(current.Streams, `"normalization":false`, `"normalization":true`, 1)
			case "policy":
				current.AdvancedSettings = strings.Replace(current.AdvancedSettings, `"enabled":true`, `"enabled":false`, 1)
			case "source config":
				current.Source = `{"host":"changed"}`
			case "destination config":
				current.Destination = `{"type":"ICEBERG","writer":{"database":"changed"}}`
			case "source id":
				current.SourceID++
			case "destination id":
				current.DestinationID++
			case "source version":
				current.Version = "changed"
			case "destination version":
				current.DestinationVersion = "changed"
			case "source type":
				current.Driver = "mysql"
			case "destination type":
				current.DestinationDriver = "parquet"
			case "project":
				current.ProjectID = "other"
			}
			require.ErrorIs(t, SameEnrollmentContext(original, current), ErrEnrollmentConflict)
		})
	}
	fresh := original
	fresh.State = `{"global":{"lsn":"newer"}}`
	fresh.Frequency = "new-frequency"
	fresh.Source = ` {"port":5432,"host":"source"} `
	require.NoError(t, SameEnrollmentContext(original, fresh))
}

// Opt-in smoke test against an isolated schema on a local PostgreSQL instance.
// No Docker containers or remote databases are created/contacted by default.
func TestCatalogEnrollmentLocalPostgres(t *testing.T) {
	dsn := os.Getenv("OLAKE_ENROLLMENT_TEST_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_ENROLLMENT_TEST_DSN to a localhost PostgreSQL URL")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, u.Scheme)
	require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, u.Hostname(), "smoke test only accepts a local database")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := fmt.Sprintf("olake_enrollment_test_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
		require.NoError(t, err)
	})
	query := u.Query()
	query.Set("search_path", schema)
	query.Set("application_name", schema)
	u.RawQuery = query.Encode()
	client, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	defer client.Close()
	db := &DB{client: client, tables: map[string]string{"job": "enrollment_job", "source": "enrollment_source", "dest": "enrollment_destination"}}
	_, err = client.ExecContext(ctx, `
		CREATE TABLE enrollment_source(id integer PRIMARY KEY, config text, version text, type text, name text);
		CREATE TABLE enrollment_destination(id integer PRIMARY KEY, config text, version text, type text, name text);
		CREATE TABLE enrollment_job(id integer PRIMARY KEY, name text, streams_config text, available_streams_config text,
			selected_streams_config text, state text, project_id text, source_id integer, dest_id integer,
			advanced_settings jsonb, frequency text, created_at timestamptz, updated_at timestamptz);
		INSERT INTO enrollment_source VALUES(1,'{"host":"source"}','v-test','postgres','source');
		INSERT INTO enrollment_destination VALUES(2,'{"type":"ICEBERG","writer":{}}','v-dest','iceberg','destination');`)
	require.NoError(t, err)
	state := `{"global":{"lsn":"unmodified-global-lsn"},"streams":{"public.old":{"cursor":"123"}}}`
	reset := func(t *testing.T) types.JobData {
		_, err := client.ExecContext(ctx, `TRUNCATE enrollment_job;
			UPDATE enrollment_source SET config='{"host":"source"}';
			UPDATE enrollment_destination SET config='{"type":"ICEBERG","writer":{}}';`)
		require.NoError(t, err)
		_, err = client.ExecContext(ctx, `INSERT INTO enrollment_job(id,name,streams_config,state,project_id,source_id,dest_id,advanced_settings,frequency,created_at)
			VALUES(42,'test',$1,$2,'project',1,2,$3::jsonb,'15m',NOW())`, dbOldCatalog, state, dbEnrollmentSettings)
		require.NoError(t, err)
		job, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		return job
	}
	t.Run("durable commit preserves global state and retry does not duplicate", func(t *testing.T) {
		original := reset(t)
		// State may advance independently while discovery is running.
		newState := `{"global":{"lsn":"newer-lsn"},"streams":{"public.old":{"cursor":"456"}}}`
		_, err := client.ExecContext(ctx, `UPDATE enrollment_job SET state=$1 WHERE id=42`, newState)
		require.NoError(t, err)
		require.NoError(t, db.CommitCatalogEnrollment(ctx, 42, original, dbEnrollmentResult()))
		fresh, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, newState, fresh.State)
		require.Equal(t, "15m", fresh.Frequency)
		require.Equal(t, "v-test", fresh.Version)
		require.Contains(t, fresh.AdvancedSettings, "9007199254740993")
		index, err := fresh.IndexRequired()
		require.NoError(t, err)
		require.True(t, index)
		// A lost commit acknowledgement cannot make the stale proposal repeat.
		require.ErrorIs(t, db.CommitCatalogEnrollment(ctx, 42, original, dbEnrollmentResult()), ErrEnrollmentConflict)
		require.NoError(t, db.CommitCatalogEnrollment(ctx, 42, fresh, types.EnrollmentResult{Streams: fresh.Streams}))
	})
	for _, mutation := range []string{
		`UPDATE enrollment_job SET streams_config='{"streams":[]}' WHERE id=42`,
		`UPDATE enrollment_job SET advanced_settings=jsonb_set(advanced_settings,'{auto_table_enrollment,enabled}','false') WHERE id=42`,
		`UPDATE enrollment_source SET config='{"host":"changed"}' WHERE id=1`,
		`UPDATE enrollment_destination SET config='{"type":"ICEBERG","writer":{"database":"changed"}}' WHERE id=2`,
	} {
		t.Run("conflict/"+strings.Fields(mutation)[1], func(t *testing.T) {
			original := reset(t)
			uiTx, err := client.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer uiTx.Rollback()
			_, err = uiTx.ExecContext(ctx, `SELECT id FROM enrollment_job WHERE id=42 FOR UPDATE`)
			require.NoError(t, err)
			_, err = uiTx.ExecContext(ctx, mutation)
			require.NoError(t, err)
			finished := make(chan error, 1)
			go func() { finished <- db.CommitCatalogEnrollment(ctx, 42, original, dbEnrollmentResult()) }()
			require.NoError(t, uiTx.Commit())
			require.ErrorIs(t, <-finished, ErrEnrollmentConflict)
			var actualState string
			require.NoError(t, client.QueryRowContext(ctx, `SELECT state FROM enrollment_job WHERE id=42`).Scan(&actualState))
			require.Equal(t, state, actualState)
		})
	}
	for _, completion := range []string{"cancel", "release"} {
		t.Run("catalog edit fence/"+completion, func(t *testing.T) {
			original := reset(t)
			fence, err := client.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer fence.Rollback()
			// Match the BFF's wire protocol literally, independently of the
			// worker's query constant and key formatting.
			_, err = fence.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "olake:catalog-edit:42")
			require.NoError(t, err)
			workerCtx, stopWorker := context.WithCancel(ctx)
			defer stopWorker()
			finished := make(chan error, 1)
			go func() {
				finished <- db.CommitCatalogEnrollment(workerCtx, 42, original, dbEnrollmentResult())
			}()
			// Observe the actual server lock wait rather than inferring blocking
			// from a sleep or goroutine scheduling order.
			require.Eventually(t, func() bool {
				var waiting bool
				err := client.QueryRowContext(ctx, `SELECT EXISTS (
					SELECT 1 FROM pg_stat_activity
					WHERE application_name = $1 AND wait_event_type = 'Lock' AND wait_event = 'advisory'
				)`, schema).Scan(&waiting)
				return err == nil && waiting
			}, 2*time.Second, 10*time.Millisecond, "worker must wait on the BFF catalog edit fence")
			select {
			case err := <-finished:
				t.Fatalf("catalog commit finished while the edit fence was held: %v", err)
			default:
			}
			// PostSync cancellation cleanup must remain able to persist state
			// while the BFF owns the fence and the worker waits for it. A short
			// deadline also proves the worker has not taken row locks first.
			newState := fmt.Sprintf(`{"global":{"lsn":"state-during-%s-fence"}}`, completion)
			stateCtx, cancelState := context.WithTimeout(ctx, time.Second)
			err = db.UpdateJobState(stateCtx, 42, newState)
			cancelState()
			require.NoError(t, err, "ordinary state persistence must not acquire or wait behind the catalog fence")
			if completion == "cancel" {
				stopWorker()
			} else {
				require.NoError(t, fence.Rollback())
			}
			select {
			case err := <-finished:
				if completion == "cancel" {
					require.Error(t, err)
					require.ErrorIs(t, workerCtx.Err(), context.Canceled)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("catalog commit did not finish after fence cancellation/release")
			}
			fresh, err := db.GetJobData(ctx, 42)
			require.NoError(t, err)
			require.Equal(t, newState, fresh.State)
			if completion == "cancel" {
				require.Equal(t, original.Streams, fresh.Streams)
				require.Equal(t, original.AdvancedSettings, fresh.AdvancedSettings)
			} else {
				require.Equal(t, dbNewCatalog, fresh.Streams)
			}
		})
	}
	t.Run("cancelled proposal is not committed", func(t *testing.T) {
		original := reset(t)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.Error(t, db.CommitCatalogEnrollment(cancelled, 42, original, dbEnrollmentResult()))
		fresh, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, original.Streams, fresh.Streams)
		require.Equal(t, original.State, fresh.State)
	})
}
