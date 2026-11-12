package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func createExecutionTestTable(t *testing.T, ctx context.Context, db *DB) {
	t.Helper()
	_, err := db.client.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s(
 job_id integer NOT NULL,workflow_id text NOT NULL,owner_run_id text NOT NULL DEFAULT '',
 launch_id text NOT NULL DEFAULT '',launcher_attempt_id text NOT NULL DEFAULT '',phase text NOT NULL DEFAULT '',
 runtime_id text NOT NULL DEFAULT '',resource_id text NOT NULL DEFAULT '',resource_uid text NOT NULL DEFAULT '',
 outcome text NOT NULL DEFAULT '',state_sha256 text NOT NULL DEFAULT '',catalog_sha256 text NOT NULL DEFAULT '',
 handoff_token text NOT NULL DEFAULT '',handoff_request_id text NOT NULL DEFAULT '',handoff_run_id text NOT NULL DEFAULT '',
 controlled boolean NOT NULL DEFAULT false,handoff_active boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(job_id,workflow_id));CREATE INDEX ON %s(job_id,phase)`, db.executionTable(), db.executionTable()))
	require.NoError(t, err)
}

func executionTestRequest(id, run string, command types.Command) *types.ExecutionRequest {
	return &types.ExecutionRequest{JobID: 42, WorkflowID: id, RunID: run, Command: command}
}
func admittedTestRequest(t *testing.T, ctx context.Context, db *DB, req *types.ExecutionRequest, runtime string) *ExecutionRecord {
	t.Helper()
	require.NoError(t, db.AcquireJobExecution(ctx, req))
	r, admitted, err := db.AdmitExecution(ctx, req, req.RunID+"/activity/1", runtime)
	require.NoError(t, err)
	require.True(t, admitted)
	req.RuntimeID, req.LaunchID, req.LauncherAttemptID = r.RuntimeID, r.LaunchID, r.LauncherAttemptID
	req.AllowLaunch = true
	req.Controlled = r.Controlled
	return r
}
func waitForExecutionAdvisoryWait(t *testing.T, ctx context.Context, db *DB) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting bool
		err := db.client.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND wait_event='advisory')`).Scan(&waiting)
		return err == nil && waiting
	}, 2*time.Second, 5*time.Millisecond)
}

func runExecutionConcurrencyTests(t *testing.T, ctx context.Context, db *DB, reset func(*testing.T) types.JobData, saved string) {
	for _, catalog := range []string{"rest", "lakekeeper", "nessie", "s3tables", "unity", "polaris", "biglake", "horizon", "glue", ""} {
		t.Run("canonical domain/"+catalog, func(t *testing.T) {
			reset(t)
			config := fmt.Sprintf(`{"type":"ICEBERG","writer":{"catalog_type":%q}}`, catalog)
			_, err := db.client.ExecContext(ctx, `UPDATE enrollment_destination SET config=$1 WHERE id=2`, config)
			require.NoError(t, err)
			req := executionTestRequest("canonical-domain", "run", types.Sync)
			req.Controlled = catalog == "glue" || catalog == "" // caller cannot choose either policy
			require.NoError(t, db.AcquireJobExecution(ctx, req))
			r, err := db.GetExecution(ctx, 42, req.WorkflowID)
			require.NoError(t, err)
			expected := catalog != "glue" && catalog != ""
			require.Equal(t, expected, r.Controlled)
			admitted, won, err := db.AdmitExecution(ctx, req, "attempt", "runtime")
			require.NoError(t, err)
			require.True(t, won)
			if expected {
				require.Equal(t, "runtime", admitted.RuntimeID)
			} else {
				require.Empty(t, admitted.RuntimeID)
				require.Empty(t, admitted.LaunchID)
			}
		})
	}
	t.Run("unsupported resync cannot launch and protected domain cannot downgrade", func(t *testing.T) {
		reset(t)
		_, err := db.client.ExecContext(ctx, `UPDATE enrollment_destination SET config='{"type":"ICEBERG","writer":{"catalog_type":"glue"}}' WHERE id=2`)
		require.NoError(t, err)
		req := executionTestRequest("unsupported-resync", "run", types.Resync)
		req.Controlled = true
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		_, _, err = db.AdmitExecution(ctx, req, "attempt", "runtime")
		require.Error(t, err)
		reset(t)
		req = executionTestRequest("protected-domain", "run", types.Sync)
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		_, err = db.client.ExecContext(ctx, `UPDATE enrollment_destination SET config='{"type":"ICEBERG","writer":{"catalog_type":"glue"}}' WHERE id=2`)
		require.NoError(t, err)
		req.Controlled = false
		require.Error(t, db.AcquireJobExecution(ctx, req))
		r, err := db.GetExecution(ctx, 42, req.WorkflowID)
		require.NoError(t, err)
		require.True(t, r.Controlled)
	})
	t.Run("stale preflight cannot claim operation unpublished after another admission", func(t *testing.T) {
		reset(t)
		req := executionTestRequest("operation", "run", types.Resync)
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		stalled, resume := make(chan struct{}), make(chan struct{})
		sealed := make(chan *ExecutionRecord, 1)
		failed := make(chan error, 1)
		go func() {
			close(stalled)
			<-resume
			r, err := db.SealExecution(ctx, req)
			if err != nil {
				failed <- err
				return
			}
			sealed <- r
		}()
		<-stalled
		admitted, won, err := db.AdmitExecution(ctx, req, "run/B/2", "runtime-B")
		require.NoError(t, err)
		require.True(t, won)
		req.RuntimeID, req.LaunchID, req.LauncherAttemptID = admitted.RuntimeID, admitted.LaunchID, admitted.LauncherAttemptID
		req.AllowLaunch = true
		close(resume)
		select {
		case r := <-sealed:
			require.Equal(t, "closing", r.Phase)
			require.NotEqual(t, "not_launched", r.Outcome)
		case err := <-failed:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.Error(t, db.ReleaseJobExecution(ctx, req), "sealing is not publication/termination proof")
		require.ErrorIs(t, db.CheckLaunch(ctx, req), ErrExecutionClosed)
		_, _, err = db.AdmitExecution(ctx, req, "run/A/1", "runtime-A")
		require.ErrorIs(t, err, ErrExecutionClosed)
	})
	t.Run("close-before-launch wins forever including after later owner release", func(t *testing.T) {
		reset(t)
		req := executionTestRequest("closed-operation", "run-A", types.Resync)
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		r, err := db.SealExecution(ctx, req)
		require.NoError(t, err)
		require.Equal(t, "not_launched", r.Outcome)
		require.Equal(t, "closed", r.Phase)
		_, won, err := db.AdmitExecution(ctx, req, "late-attempt", "must-not-launch")
		require.ErrorIs(t, err, ErrExecutionClosed)
		require.False(t, won)
		require.NoError(t, db.ReleaseJobExecution(ctx, req))
		require.NoError(t, db.ReleaseJobExecution(ctx, req), "lost release acknowledgement is idempotent")
		newer := executionTestRequest("later-operation", "run-B", types.Sync)
		require.NoError(t, db.AcquireJobExecution(ctx, newer))
		_, err = db.SealExecution(ctx, newer)
		require.NoError(t, err)
		require.NoError(t, db.ReleaseJobExecution(ctx, newer))
		require.ErrorIs(t, db.AcquireJobExecution(ctx, req), ErrExecutionClosed)
		r, err = db.GetExecution(ctx, 42, req.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "closed", r.Phase)
	})
	t.Run("exactly one durable launcher among overlapping attempts", func(t *testing.T) {
		reset(t)
		req := executionTestRequest("admission", "run", types.Resync)
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		start := make(chan struct{})
		var wg sync.WaitGroup
		var winners atomic.Int32
		results := make(chan *ExecutionRecord, 8)
		failures := make(chan error, 8)
		for i := range 8 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				r, won, err := db.AdmitExecution(ctx, req, fmt.Sprintf("run/activity/%d", i), fmt.Sprintf("runtime-%d", i))
				if err != nil {
					failures <- err
					return
				}
				if won {
					winners.Add(1)
				}
				results <- r
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		close(failures)
		for err := range failures {
			require.NoError(t, err)
		}
		require.Equal(t, int32(1), winners.Load())
		identity := ""
		for r := range results {
			if identity == "" {
				identity = r.RuntimeID
			}
			require.Equal(t, identity, r.RuntimeID)
			require.Equal(t, identity, r.LaunchID)
			require.Equal(t, "admitted", r.Phase)
		}
	})
	t.Run("new run adoption serializes cleanup and fences every old mutation", func(t *testing.T) {
		reset(t)
		old := executionTestRequest("same-operation", "old-run", types.Resync)
		admittedTestRequest(t, ctx, db, old, "old-runtime")
		require.NoError(t, db.RecordExecutionResource(ctx, old, "immutable-old-resource", "old-uid"))
		old.ResourceID, old.ResourceUID = "immutable-old-resource", "old-uid"
		replacement := executionTestRequest(old.WorkflowID, "new-run", types.Resync)
		require.ErrorIs(t, db.AcquireJobExecution(ctx, replacement), ErrExecutionClosed, "same WorkflowID is not epoch authority")
		_, err := db.SealExecution(ctx, old)
		require.NoError(t, err)
		entered, finishCleanup := make(chan struct{}), make(chan struct{})
		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- db.GuardExecution(ctx, old, func(context.Context, *ExecutionRecord) error { close(entered); <-finishCleanup; return nil })
		}()
		<-entered
		adoptDone := make(chan error, 1)
		go func() { adoptDone <- db.AdoptExecution(ctx, old, replacement) }()
		waitForExecutionAdvisoryWait(t, ctx, db)
		select {
		case err := <-adoptDone:
			t.Fatalf("adoption escaped cleanup authority: %v", err)
		default:
		}
		close(finishCleanup)
		require.NoError(t, <-cleanupDone)
		require.NoError(t, <-adoptDone)
		called := false
		require.ErrorIs(t, db.GuardExecution(ctx, old, func(context.Context, *ExecutionRecord) error { called = true; return nil }), ErrJobExecutionOwned)
		require.False(t, called)
		require.ErrorIs(t, db.UpdateJobState(ctx, 42, old.WorkflowID, old.RunID, `{"stale":true}`), ErrJobExecutionOwned)
		require.Error(t, db.ReleaseJobExecution(ctx, old))
		require.Error(t, db.RecordExecutionResource(ctx, old, "other", "other"))
		_, err = db.SealExecution(ctx, old)
		require.ErrorIs(t, err, ErrJobExecutionOwned)
		require.NoError(t, db.AcquireJobExecution(ctx, replacement))
		recovered, won, err := db.AdmitExecution(ctx, replacement, "new-run/recovery/1", "recovery-runtime")
		require.NoError(t, err)
		require.True(t, won)
		require.Equal(t, "old-runtime", recovered.LaunchID)
		require.Equal(t, "recovery-runtime", recovered.RuntimeID)
		require.Equal(t, "recovery", recovered.Outcome)
		require.Empty(t, recovered.ResourceID)
		var currentState string
		require.NoError(t, db.client.QueryRowContext(ctx, fmt.Sprintf(`SELECT state FROM %s WHERE id=42`, pq.QuoteIdentifier(db.tables["job"]))).Scan(&currentState))
		require.Equal(t, saved, currentState)
	})
	t.Run("late enrollment cannot mutate catalog after runtime admission", func(t *testing.T) {
		original := reset(t)
		req := executionTestRequest("sync-enrollment", "run", types.Sync)
		admittedTestRequest(t, ctx, db, req, "sync-runtime")
		require.ErrorIs(t, db.CommitCatalogEnrollment(ctx, req, original, dbEnrollmentResult()), ErrExecutionClosed)
		current, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, original.Streams, current.Streams)
	})
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		for _, outcome := range []string{"settled", "not_started", "unknown"} {
			t.Run("ordinary reset retirement/"+string(command)+"/"+outcome, func(t *testing.T) {
				reset(t)
				old := executionTestRequest("failed-ordinary", "old-run", command)
				admittedTestRequest(t, ctx, db, old, "ordinary-runtime")
				_, err := db.SealExecution(ctx, old)
				require.NoError(t, err)
				newer := executionTestRequest(old.WorkflowID, "retirement-run", command)
				if outcome == "unknown" {
					require.Error(t, db.AdoptExecution(ctx, old, newer))
					require.Error(t, db.ReleaseJobExecution(ctx, old))
					return
				}
				require.NoError(t, db.FinishExecution(ctx, old, outcome))
				require.NoError(t, db.AdoptExecution(ctx, old, newer))
				require.NoError(t, db.AcquireJobExecution(ctx, newer))
				r, err := db.GetExecution(ctx, 42, old.WorkflowID)
				require.NoError(t, err)
				require.Equal(t, "closed", r.Phase)
				require.Equal(t, "retired", r.Outcome)
				require.Equal(t, "ordinary-runtime", r.RuntimeID)
				_, won, err := db.AdmitExecution(ctx, newer, "must-not-relaunch", "new-data-runtime")
				require.ErrorIs(t, err, ErrExecutionClosed)
				require.False(t, won)
				require.Error(t, db.ReleaseJobExecution(ctx, old))
				require.NoError(t, db.ReleaseJobExecution(ctx, newer))
				require.ErrorIs(t, db.AcquireJobExecution(ctx, newer), ErrExecutionClosed, "retired tombstone may not reacquire after release")
				require.NoError(t, db.AcquireJobExecution(ctx, executionTestRequest("fresh-atomic-resync", "new-operation-run", types.Resync)), "a safely retired TOAST/auth failure must permit a fresh table resync")
			})
		}
	}
	t.Run("legacy workflow owner without run epoch is never free admission", func(t *testing.T) {
		reset(t)
		_, err := db.client.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET execution_workflow_id='legacy-workflow',execution_run_id=NULL WHERE id=42`, pq.QuoteIdentifier(db.tables["job"])))
		require.NoError(t, err)
		req := executionTestRequest("legacy-workflow", "new-run", types.Resync)
		require.ErrorIs(t, db.AcquireJobExecution(ctx, req), ErrJobExecutionOwned)
		_, err = db.SealExecution(ctx, req)
		require.ErrorIs(t, err, ErrJobExecutionOwned)
		require.Error(t, db.ReleaseJobExecution(ctx, req))
		require.ErrorIs(t, db.UpdateJobState(ctx, 42, req.WorkflowID, req.RunID, `{"wrong":true}`), ErrJobExecutionOwned)
		var owner string
		var hasRun bool
		require.NoError(t, db.client.QueryRowContext(ctx, fmt.Sprintf(`SELECT execution_workflow_id,execution_run_id IS NOT NULL FROM %s WHERE id=42`, pq.QuoteIdentifier(db.tables["job"]))).Scan(&owner, &hasRun))
		require.Equal(t, "legacy-workflow", owner)
		require.False(t, hasRun)
	})
	t.Run("queued paused jobs cannot admit sync or resync but explicit clear remains supported", func(t *testing.T) {
		reset(t)
		_, err := db.client.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET active=false WHERE id=42`, pq.QuoteIdentifier(db.tables["job"])))
		require.NoError(t, err)
		for _, command := range []types.Command{types.Sync, types.Resync} {
			require.Error(t, db.AcquireJobExecution(ctx, executionTestRequest(string(command), "run", command)))
		}
		require.NoError(t, db.AcquireJobExecution(ctx, executionTestRequest("clear", "run", types.ClearDestination)))
	})
	t.Run("claim waits for API fence and rechecks committed pause intent", func(t *testing.T) {
		reset(t)
		fence, err := db.client.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer fence.Rollback()
		_, err = fence.ExecContext(ctx, catalogEditFenceQuery, "olake:catalog-edit:42")
		require.NoError(t, err)
		finished := make(chan error, 1)
		go func() { finished <- db.AcquireJobExecution(ctx, executionTestRequest("queued", "run", types.Sync)) }()
		waitForExecutionAdvisoryWait(t, ctx, db)
		_, err = fence.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET active=false WHERE id=42`, pq.QuoteIdentifier(db.tables["job"])))
		require.NoError(t, err)
		require.NoError(t, fence.Commit())
		require.Error(t, <-finished)
	})
	t.Run("handoff retains exclusion and lost acknowledgement permits only bound transfer", func(t *testing.T) {
		reset(t)
		req := executionTestRequest("published", "resync-run", types.Resync)
		admittedTestRequest(t, ctx, db, req, "data-runtime")
		_, err := db.SealExecution(ctx, req)
		require.NoError(t, err)
		require.NoError(t, db.FinishExecution(ctx, req, "published"))
		require.NoError(t, db.BeginHandoff(ctx, req, "internal-secret-token"))
		r, err := db.GetExecution(ctx, 42, req.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "handoff", r.Phase)
		require.NotEmpty(t, r.CatalogSHA256)
		require.Equal(t, req.RunID, r.HandoffRunID)
		require.Error(t, db.ReleaseJobExecution(ctx, req))
		require.Error(t, db.AcquireJobExecution(ctx, executionTestRequest("ordinary", "other", types.Sync)))
		requestID := ""
		require.Error(t, db.ApplyHandoff(ctx, req, func(_ context.Context, current *ExecutionRecord) error {
			requestID = current.HandoffRequestID
			return errors.New("PatchSchedule accepted but acknowledgement lost")
		}))
		transfer := executionTestRequest("triggered-sync", "sync-run", types.Sync)
		transfer.AfterResyncOperationID = req.WorkflowID
		transfer.AfterResyncRunID = req.RunID
		transfer.AfterResyncTransferToken = "wrong-token"
		transfer.AfterResyncStateSHA256 = r.StateSHA256
		require.Error(t, db.AcquireJobExecution(ctx, transfer))
		transfer.AfterResyncTransferToken = r.HandoffToken
		require.NoError(t, db.AcquireJobExecution(ctx, transfer))
		applied := false
		require.NoError(t, db.ApplyHandoff(ctx, req, func(context.Context, *ExecutionRecord) error { applied = true; return nil }))
		require.False(t, applied, "old retry must not unpause/trigger after transfer")
		after, err := db.GetExecution(ctx, 42, req.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "handed_off", after.Phase)
		require.Equal(t, requestID, after.HandoffRequestID)
	})
	t.Run("handoff reset preserves dispatched transfer epoch but rejects stale controller", func(t *testing.T) {
		reset(t)
		old := executionTestRequest("handoff-reset", "old-run", types.Resync)
		admittedTestRequest(t, ctx, db, old, "runtime")
		_, err := db.SealExecution(ctx, old)
		require.NoError(t, err)
		require.NoError(t, db.FinishExecution(ctx, old, "published"))
		require.NoError(t, db.BeginHandoff(ctx, old, "transfer-token"))
		before, err := db.GetExecution(ctx, 42, old.WorkflowID)
		require.NoError(t, err)
		newer := executionTestRequest(old.WorkflowID, "new-run", types.Resync)
		require.NoError(t, db.AdoptExecution(ctx, old, newer))
		require.NoError(t, db.AcquireJobExecution(ctx, newer))
		after, err := db.GetExecution(ctx, 42, old.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "new-run", after.OwnerRunID)
		require.Equal(t, "old-run", after.HandoffRunID)
		require.Equal(t, before.HandoffRequestID, after.HandoffRequestID)
		require.ErrorIs(t, db.ApplyHandoff(ctx, old, func(context.Context, *ExecutionRecord) error { t.Fatal("old controller mutated schedule"); return nil }), ErrJobExecutionOwned)
		transfer := executionTestRequest("late-dispatched-sync", "sync-run", types.Sync)
		transfer.AfterResyncOperationID = old.WorkflowID
		transfer.AfterResyncRunID = "old-run"
		transfer.AfterResyncTransferToken = after.HandoffToken
		transfer.AfterResyncStateSHA256 = after.StateSHA256
		require.NoError(t, db.AcquireJobExecution(ctx, transfer))
	})
	for _, change := range []string{"checkpoint", "catalog", "pause"} {
		t.Run("handoff rejects changed "+change, func(t *testing.T) {
			reset(t)
			req := executionTestRequest("guarded-handoff", "run", types.Resync)
			admittedTestRequest(t, ctx, db, req, "runtime")
			_, err := db.SealExecution(ctx, req)
			require.NoError(t, err)
			require.NoError(t, db.FinishExecution(ctx, req, "published"))
			require.NoError(t, db.BeginHandoff(ctx, req, "token"))
			r, err := db.GetExecution(ctx, 42, req.WorkflowID)
			require.NoError(t, err)
			mutation := `state='{"changed":true}'`
			if change == "catalog" {
				mutation = `streams_config='{"streams":[]}'`
			}
			if change == "pause" {
				mutation = `active=false`
			}
			_, err = db.client.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET %s WHERE id=42`, pq.QuoteIdentifier(db.tables["job"]), mutation))
			require.NoError(t, err)
			called := false
			require.Error(t, db.ApplyHandoff(ctx, req, func(context.Context, *ExecutionRecord) error { called = true; return nil }))
			require.False(t, called)
			transfer := executionTestRequest("trigger", "new", types.Sync)
			transfer.AfterResyncOperationID = req.WorkflowID
			transfer.AfterResyncRunID = req.RunID
			transfer.AfterResyncTransferToken = r.HandoffToken
			transfer.AfterResyncStateSHA256 = r.StateSHA256
			require.Error(t, db.AcquireJobExecution(ctx, transfer))
			require.Error(t, db.AdoptExecution(ctx, req, executionTestRequest(req.WorkflowID, "replacement-run", types.Resync)), "changed transfer binding must also reject new controlling-run adoption")
		})
	}
	t.Run("closed tombstone survives SQL job deletion", func(t *testing.T) {
		reset(t)
		req := executionTestRequest("deleted-job-operation", "run", types.ClearDestination)
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		_, err := db.SealExecution(ctx, req)
		require.NoError(t, err)
		require.NoError(t, db.ReleaseJobExecution(ctx, req))
		_, err = db.client.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id=42`, pq.QuoteIdentifier(db.tables["job"])))
		require.NoError(t, err)
		r, err := db.GetExecution(ctx, 42, req.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "closed", r.Phase)
		require.Error(t, db.AcquireJobExecution(ctx, req))
	})
}
