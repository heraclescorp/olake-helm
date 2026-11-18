package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const recoveryDBCatalog = `{"streams":[{"stream":{"namespace":"public","name":"old","sync_mode":"cdc","source_defined_primary_key":["id"]}}],"selected_streams":{"public":[{"stream_name":"old"}]}}`
const recoveryDBBaseline = `{"type":"GLOBAL","global":{"state":{"lsn":"0/100"},"streams":["public.old"]},"streams":[]}`
const recoveryDBFinal = `{"type":"GLOBAL","global":{"state":{"lsn":"0/200"},"streams":["public.old"]},"streams":[]}`

// Runs inside TestCatalogEnrollmentLocalPostgres's isolated real PostgreSQL
// schema. No sqlmock: owner races, rollback and advisory locks use real sessions.
func runOrdinaryRecoveryDBTests(t *testing.T, ctx context.Context, db *DB, reset func(*testing.T) types.JobData) {
	mode, kube := viper.Get(constants.EnvStorageMode), viper.Get(constants.EnvKubernetesServiceHost)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	viper.Set(constants.EnvKubernetesServiceHost, "")
	defer func() { viper.Set(constants.EnvStorageMode, mode); viper.Set(constants.EnvKubernetesServiceHost, kube) }()
	fixture := func(t *testing.T) (*types.ExecutionRequest, *types.ExecutionRequest) {
		t.Helper()
		reset(t)
		_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job SET streams_config=$1,state=$2 WHERE id=42`, recoveryDBCatalog, recoveryDBBaseline)
		require.NoError(t, err)
		job, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		id := fmt.Sprintf("ordinary-recovery-%d", time.Now().UnixNano())
		old := executionTestRequest(id, "original-run", types.Sync)
		admittedTestRequest(t, ctx, db, old, id+"-original")
		old.ResourceID, old.ResourceUID = "original-pod", "original-uid"
		require.NoError(t, db.RecordExecutionResource(ctx, old, old.ResourceID, old.ResourceUID))
		_, originalDir := utils.GetExecutionDirAndSubDir(old)
		_, stableDir := utils.GetWorkflowDirAndSubDir(old.WorkflowID, types.Sync)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(originalDir)); require.NoError(t, os.RemoveAll(stableDir)) })
		receipt := fmt.Sprintf(`{"version":1,"execution_id":%q,"launch_id":%q,"command":"sync","outcome":"unknown"}`, old.RuntimeID, old.LaunchID)
		files := map[string]string{"source.json": job.Source, "destination.json": job.Destination, constants.StreamsFile: job.Streams}
		for name, data := range files {
			require.NoError(t, storage.WriteFileAtomic(ctx, originalDir, name, data))
		}
		require.NoError(t, storage.WriteFileAtomic(ctx, originalDir, "execution-result.json", receipt))
		require.NoError(t, storage.WriteFileAtomic(ctx, stableDir, utils.OrdinaryRecoveryBaselineFile, recoveryDBBaseline))
		catalogSHA, err := utils.OrdinaryCatalogSHA256(files)
		require.NoError(t, err)
		stateSHA, err := utils.StateSHA256(recoveryDBBaseline)
		require.NoError(t, err)
		origin := utils.OrdinaryRecoveryOrigin{Version: 1, OriginalExecutionID: old.RuntimeID, OriginalLaunchID: old.LaunchID, OriginalReceiptSHA256: utils.RawSHA256(receipt), SourceSHA256: utils.RawSHA256(job.Source), DestinationSHA256: utils.RawSHA256(job.Destination), CatalogSHA256: catalogSHA, BaselineStateSHA256: stateSHA}
		data, err := json.Marshal(origin)
		require.NoError(t, err)
		require.NoError(t, storage.WriteFileAtomic(ctx, stableDir, utils.OrdinaryRecoveryOriginFile, string(data)))
		_, err = db.SealExecution(ctx, old)
		require.NoError(t, err)
		require.NoError(t, utils.CloseExecutionFence(ctx, old))
		return old, executionTestRequest(old.WorkflowID, "recovery-run", types.Sync)
	}
	adopt := func(t *testing.T, old, newer *types.ExecutionRequest) {
		t.Helper()
		require.NoError(t, db.AdoptExecution(ctx, old, newer))
		require.NoError(t, db.AcquireJobExecution(ctx, newer))
		r, won, err := db.AdmitExecution(ctx, newer, "recovery-attempt", "fresh-"+old.LaunchID)
		require.NoError(t, err)
		require.True(t, won)
		newer.Controlled, newer.RecoverOnly = r.Controlled, r.Outcome == "recovery"
		newer.RuntimeID, newer.LaunchID, newer.LauncherAttemptID = r.RuntimeID, r.LaunchID, r.LauncherAttemptID
		newer.ResourceID, newer.ResourceUID = "recovery-pod", "recovery-uid"
		require.NoError(t, db.RecordExecutionResource(ctx, newer, newer.ResourceID, newer.ResourceUID))
		_, err = db.SealExecution(ctx, newer)
		require.NoError(t, err)
		require.NoError(t, utils.CloseExecutionFence(ctx, newer))
		_, runtimeDir := utils.GetExecutionDirAndSubDir(newer)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	}
	assertLockedBaseline := func(t *testing.T, req *types.ExecutionRequest) {
		t.Helper()
		job, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, recoveryDBBaseline, job.State)
		r, err := db.GetExecution(ctx, 42, req.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "closing", r.Phase)
		require.Error(t, db.ReleaseJobExecution(ctx, req))
	}
	t.Run("ordinary recovery concurrent adoption and one durable launcher", func(t *testing.T) {
		old, newer := fixture(t)
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for _, run := range []string{"recovery-A", "recovery-B"} {
			wg.Add(1)
			go func(run string) {
				defer wg.Done()
				candidate := *newer
				candidate.RunID = run
				results <- db.AdoptExecution(ctx, old, &candidate)
			}(run)
		}
		wg.Wait()
		close(results)
		wins := 0
		for err := range results {
			if err == nil {
				wins++
			}
		}
		require.Equal(t, 1, wins)
		r, err := db.GetExecution(ctx, 42, old.WorkflowID)
		require.NoError(t, err)
		newer.RunID = r.OwnerRunID
		require.Equal(t, "recovery", r.Outcome)
		require.Equal(t, old.LaunchID, r.LaunchID)
		require.Empty(t, r.RuntimeID)
		require.ErrorIs(t, db.UpdateJobState(ctx, 42, old.WorkflowID, old.RunID, recoveryDBFinal), ErrJobExecutionOwned)
		require.Error(t, db.ReleaseJobExecution(ctx, old))
		launches := 0
		for i := range 3 {
			r, won, err := db.AdmitExecution(ctx, newer, fmt.Sprintf("attempt-%d", i), fmt.Sprintf("runtime-%d", i))
			require.NoError(t, err)
			if won {
				launches++
			}
			require.Equal(t, "runtime-0", r.RuntimeID)
			require.Equal(t, old.LaunchID, r.LaunchID)
		}
		require.Equal(t, 1, launches)
	})
	t.Run("ordinary recovery serializes against old cleanup authority", func(t *testing.T) {
		old, newer := fixture(t)
		entered, resume := make(chan struct{}), make(chan struct{})
		guardDone := make(chan error, 1)
		go func() {
			guardDone <- db.GuardExecution(ctx, old, func(context.Context, *ExecutionRecord) error { close(entered); <-resume; return nil })
		}()
		<-entered
		adoptDone := make(chan error, 1)
		go func() { adoptDone <- db.AdoptExecution(ctx, old, newer) }()
		waitForExecutionAdvisoryWait(t, ctx, db)
		select {
		case err := <-adoptDone:
			t.Fatalf("adoption escaped exact cleanup guard: %v", err)
		default:
		}
		close(resume)
		require.NoError(t, <-guardDone)
		require.NoError(t, <-adoptDone)
	})
	t.Run("ordinary adoption requires a positively closed original fence", func(t *testing.T) {
		old, newer := fixture(t)
		_, dir := utils.GetExecutionDirAndSubDir(old)
		require.NoError(t, os.Remove(dir+"/"+utils.ExecutionFenceFile))
		require.Error(t, db.AdoptExecution(ctx, old, newer), "artifact absence cannot prove closed launch admission")
		assertLockedBaseline(t, old)
	})
	t.Run("ordinary recovery cannot reuse the original runtime fence", func(t *testing.T) {
		old, newer := fixture(t)
		require.NoError(t, db.AdoptExecution(ctx, old, newer))
		_, won, err := db.AdmitExecution(ctx, newer, "must-not-reopen", old.LaunchID)
		require.Error(t, err)
		require.False(t, won)
		r, won, err := db.AdmitExecution(ctx, newer, "fresh-attempt", "fresh-runtime")
		require.NoError(t, err)
		require.True(t, won)
		require.Equal(t, old.LaunchID, r.LaunchID)
		require.NotEqual(t, r.LaunchID, r.RuntimeID)
	})
	for _, change := range []string{"saved source", "saved destination", "saved catalog", "saved checkpoint", "origin hash", "original receipt", "resource UID", "unsupported clear"} {
		t.Run("ordinary adoption rejects changed evidence/"+change, func(t *testing.T) {
			old, newer := fixture(t)
			switch change {
			case "saved source":
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_source SET config='{"host":"changed"}' WHERE id=1`)
				require.NoError(t, err)
			case "saved destination":
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_destination SET config='{"type":"ICEBERG","writer":{"catalog_type":"rest","database":"changed"}}' WHERE id=2`)
				require.NoError(t, err)
			case "saved catalog":
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job SET streams_config='{"streams":[]}' WHERE id=42`)
				require.NoError(t, err)
			case "saved checkpoint":
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job SET state=$1 WHERE id=42`, recoveryDBFinal)
				require.NoError(t, err)
			case "origin hash":
				_, dir := utils.GetWorkflowDirAndSubDir(old.WorkflowID, types.Sync)
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryOriginFile, `{"version":1}`))
			case "original receipt":
				_, dir := utils.GetExecutionDirAndSubDir(old)
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "execution-result.json", `{"outcome":"settled"}`))
			case "resource UID":
				old.ResourceUID = "foreign"
			case "unsupported clear":
				newer.Command = types.ClearDestination
			}
			require.Error(t, db.AdoptExecution(ctx, old, newer))
			r, err := db.GetExecution(ctx, 42, old.WorkflowID)
			require.NoError(t, err)
			require.Equal(t, old.RunID, r.OwnerRunID)
			require.Equal(t, old.RuntimeID, r.RuntimeID)
			require.Error(t, db.ReleaseJobExecution(ctx, old))
		})
	}
	t.Run("paused retained sync allows recovery but never fresh ingestion", func(t *testing.T) {
		old, newer := fixture(t)
		_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job SET active=false WHERE id=42`)
		require.NoError(t, err)
		require.ErrorIs(t, db.AcquireJobExecution(ctx, newer), ErrExecutionClosed)
		require.NoError(t, db.AdoptExecution(ctx, old, newer))
		require.NoError(t, db.AcquireJobExecution(ctx, newer))
		require.ErrorIs(t, db.AcquireJobExecution(ctx, executionTestRequest("fresh-operation", "run", types.Sync)), ErrJobExecutionInactive)
	})
	for _, failure := range []string{"proof unavailable", "evidence changed during verification", "stale runtime", "stale UID", "saved checkpoint changed"} {
		t.Run("ordinary recovery completion stays locked/"+failure, func(t *testing.T) {
			old, newer := fixture(t)
			adopt(t, old, newer)
			if failure == "stale runtime" {
				newer.RuntimeID = "foreign"
			}
			if failure == "stale UID" {
				newer.ResourceUID = "foreign"
			}
			if failure == "saved checkpoint changed" {
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job SET state=$1 WHERE id=42`, recoveryDBFinal)
				require.NoError(t, err)
			}
			err := db.CompleteOrdinaryRecovery(ctx, newer, func(context.Context) (string, error) {
				if failure == "evidence changed during verification" {
					_, dir := utils.GetExecutionDirAndSubDir(old)
					if err := storage.WriteFileAtomic(ctx, dir, "execution-result.json", `{"outcome":"changed"}`); err != nil {
						return "", err
					}
					_, _, _, err := utils.ReadOrdinaryRecoveryOrigin(ctx, newer)
					return "", err
				}
				return "", errors.New("settled result bindings unavailable or changed")
			})
			require.Error(t, err)
			r, readErr := db.GetExecution(ctx, 42, newer.WorkflowID)
			require.NoError(t, readErr)
			require.Equal(t, "closing", r.Phase)
			require.Equal(t, "recovery", r.Outcome)
			require.Equal(t, newer.RunID, r.OwnerRunID)
			require.Error(t, db.ReleaseJobExecution(ctx, newer))
			if failure != "saved checkpoint changed" {
				assertLockedBaseline(t, newer)
			}
		})
	}
	t.Run("checkpoint and settlement rollback together after partial SQL handoff", func(t *testing.T) {
		old, newer := fixture(t)
		adopt(t, old, newer)
		_, err := db.client.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION fail_ordinary_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='closed' THEN RAISE EXCEPTION 'injected after checkpoint update'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_ordinary_completion BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION fail_ordinary_completion()`, db.executionTable()))
		require.NoError(t, err)
		err = db.CompleteOrdinaryRecovery(ctx, newer, func(context.Context) (string, error) { return recoveryDBFinal, nil })
		require.Error(t, err)
		assertLockedBaseline(t, newer)
		_, err = db.client.ExecContext(ctx, fmt.Sprintf(`DROP TRIGGER fail_ordinary_completion ON %s; DROP FUNCTION fail_ordinary_completion()`, db.executionTable()))
		require.NoError(t, err)
		require.NoError(t, db.CompleteOrdinaryRecovery(ctx, newer, func(context.Context) (string, error) { return recoveryDBFinal, nil }))
		job, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, recoveryDBFinal, job.State)
		r, err := db.GetExecution(ctx, 42, newer.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "closed", r.Phase)
		require.Equal(t, "settled", r.Outcome)
		require.Equal(t, "recovery-pod", r.ResourceID)
		require.Equal(t, "recovery-uid", r.ResourceUID)
		require.NoError(t, db.ReleaseJobExecution(ctx, newer))
		var active bool
		require.NoError(t, db.client.QueryRowContext(ctx, fmt.Sprintf(`SELECT active FROM %s WHERE id=42`, pq.QuoteIdentifier(db.tables["job"]))).Scan(&active))
		require.True(t, active, "completion does not mutate saved schedule intent")
		require.ErrorIs(t, db.AcquireJobExecution(ctx, newer), ErrExecutionClosed, "settled tombstone cannot reacquire or relaunch")
	})
	t.Run("reset of interrupted recovery retains original launch and rejects late writes", func(t *testing.T) {
		old, newer := fixture(t)
		adopt(t, old, newer)
		third := executionTestRequest(old.WorkflowID, "third-run", types.Sync)
		require.NoError(t, db.AdoptExecution(ctx, newer, third))
		r, won, err := db.AdmitExecution(ctx, third, "third-attempt", "third-runtime")
		require.NoError(t, err)
		require.True(t, won)
		require.Equal(t, old.LaunchID, r.LaunchID)
		require.Equal(t, "recovery", r.Outcome)
		require.Equal(t, "third-runtime", r.RuntimeID)
		require.Error(t, db.RecordExecutionResource(ctx, newer, "late-pod", "late-uid"))
		require.Error(t, db.CompleteOrdinaryRecovery(ctx, newer, func(context.Context) (string, error) { return recoveryDBFinal, nil }))
		job, err := db.GetJobData(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, recoveryDBBaseline, job.State)
	})
}
