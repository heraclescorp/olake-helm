package database

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// Uses TestCatalogEnrollmentLocalPostgres's isolated real PostgreSQL schema.
func runFreshClearDBTests(t *testing.T, ctx context.Context, db *DB, reset func(*testing.T) types.JobData) {
	mode, kube := viper.Get(constants.EnvStorageMode), viper.Get(constants.EnvKubernetesServiceHost)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	viper.Set(constants.EnvKubernetesServiceHost, "")
	defer func() { viper.Set(constants.EnvStorageMode, mode); viper.Set(constants.EnvKubernetesServiceHost, kube) }()
	fixture := func(t *testing.T, command types.Command, outcome string) (*types.ExecutionRequest, *types.ExecutionRequest, string) {
		t.Helper()
		job := reset(t)
		id := fmt.Sprintf("fresh-clear-%d", time.Now().UnixNano())
		old := executionTestRequest(id, "old-run", command)
		if outcome == "not_launched" {
			require.NoError(t, db.AcquireJobExecution(ctx, old))
			r, err := db.SealExecution(ctx, old)
			require.NoError(t, err)
			old.Controlled = r.Controlled
		} else {
			admittedTestRequest(t, ctx, db, old, id+"-original")
			old.ResourceID, old.ResourceUID = "original-pod", "original-uid"
			require.NoError(t, db.RecordExecutionResource(ctx, old, old.ResourceID, old.ResourceUID))
			_, dir := utils.GetExecutionDirAndSubDir(old)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
			require.NoError(t, utils.OpenExecutionFence(ctx, old))
			_, err := db.SealExecution(ctx, old)
			require.NoError(t, err)
			require.NoError(t, utils.CloseExecutionFence(ctx, old))
			require.NoError(t, db.FinishExecution(ctx, old, outcome))
		}
		return old, executionTestRequest(id, "clear-run", types.ClearDestination), job.State
	}
	assertOwnerAndState := func(t *testing.T, workflow, run, state string) {
		t.Helper()
		var actualWorkflow, actualRun, actualState string
		require.NoError(t, db.client.QueryRowContext(ctx, `SELECT execution_workflow_id,execution_run_id,state FROM enrollment_job WHERE id=42`).Scan(&actualWorkflow, &actualRun, &actualState))
		require.Equal(t, workflow, actualWorkflow)
		require.Equal(t, run, actualRun)
		require.Equal(t, state, actualState)
	}
	for _, attempt := range []string{"", "legacy-run/activity/1"} {
		t.Run("legacy completed clear adoption/"+attempt, func(t *testing.T) {
			job := reset(t)
			_, err := db.client.ExecContext(ctx, `UPDATE enrollment_destination SET config='{"type":"ICEBERG","writer":{"catalog_type":"glue"}}' WHERE id=2`)
			require.NoError(t, err)
			old := executionTestRequest("legacy-clear", "legacy-run", types.ClearDestination)
			require.NoError(t, db.AcquireJobExecution(ctx, old))
			r, won, err := db.AdmitExecution(ctx, old, attempt, "must-not-be-controlled")
			require.NoError(t, err)
			require.True(t, won)
			require.False(t, r.Controlled)
			require.Empty(t, r.RuntimeID)
			require.Empty(t, r.LaunchID)
			require.Empty(t, r.ResourceID)
			require.Empty(t, r.ResourceUID)
			old.LauncherAttemptID = r.LauncherAttemptID
			_, err = db.SealExecution(ctx, old)
			require.NoError(t, err)
			require.NoError(t, db.FinishExecution(ctx, old, "legacy_completed"))
			newer := executionTestRequest(old.WorkflowID, "new-legacy-clear-run", types.ClearDestination)
			require.NoError(t, db.AdoptExecution(ctx, old, newer), "legacy clear must retain its existing unfenced adoption policy")
			require.NoError(t, db.AcquireJobExecution(ctx, newer))
			r, err = db.GetExecution(ctx, old.JobID, old.WorkflowID)
			require.NoError(t, err)
			require.False(t, r.Controlled)
			require.Equal(t, newer.RunID, r.OwnerRunID)
			require.Equal(t, "unadmitted", r.Phase)
			require.Empty(t, r.Outcome)
			require.Empty(t, r.LaunchID)
			require.Empty(t, r.RuntimeID)
			require.Empty(t, r.LauncherAttemptID)
			require.Empty(t, r.ResourceID)
			require.Empty(t, r.ResourceUID)
			assertOwnerAndState(t, newer.WorkflowID, newer.RunID, job.State)
			require.ErrorIs(t, db.GuardExecution(ctx, old, func(context.Context, *ExecutionRecord) error { t.Fatal("stale legacy cleanup entered"); return nil }), ErrJobExecutionOwned)
			r, won, err = db.AdmitExecution(ctx, newer, "new-legacy-clear-run/activity/1", "still-must-not-be-controlled")
			require.NoError(t, err)
			require.True(t, won)
			require.False(t, r.Controlled)
			require.Empty(t, r.RuntimeID)
			require.Empty(t, r.LaunchID)
			assertOwnerAndState(t, newer.WorkflowID, newer.RunID, job.State)
		})
	}
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		for _, outcome := range []string{"settled", "not_started", "not_launched", "retired"} {
			t.Run("fresh clear epoch/"+string(command)+"/"+outcome, func(t *testing.T) {
				old, newer, saved := fixture(t, command, outcome)
				require.NoError(t, db.AdoptExecution(ctx, old, newer))
				require.NoError(t, db.AcquireJobExecution(ctx, newer))
				r, err := db.GetExecution(ctx, old.JobID, old.WorkflowID)
				require.NoError(t, err)
				require.Equal(t, newer.RunID, r.OwnerRunID)
				require.True(t, r.Controlled)
				require.Equal(t, "unadmitted", r.Phase)
				require.Empty(t, r.Outcome)
				require.Empty(t, r.LaunchID)
				require.Empty(t, r.RuntimeID)
				require.Empty(t, r.LauncherAttemptID)
				require.Empty(t, r.ResourceID)
				require.Empty(t, r.ResourceUID)
				assertOwnerAndState(t, newer.WorkflowID, newer.RunID, saved)
				require.ErrorIs(t, db.GuardExecution(ctx, old, func(context.Context, *ExecutionRecord) error { t.Fatal("stale cleanup entered"); return nil }), ErrJobExecutionOwned)
				require.Error(t, db.CheckLaunch(ctx, old))
				require.Error(t, db.ReleaseJobExecution(ctx, old))
				require.Error(t, db.UpdateJobState(ctx, old.JobID, old.WorkflowID, old.RunID, `{"stale":true}`))
				require.Error(t, db.AdoptExecution(ctx, old, executionTestRequest(old.WorkflowID, "racing-clear", types.ClearDestination)))
				freshRuntime := old.WorkflowID + "-fresh-clear"
				r, won, err := db.AdmitExecution(ctx, newer, "clear-run/activity/1", freshRuntime)
				require.NoError(t, err)
				require.True(t, won)
				require.Equal(t, freshRuntime, r.RuntimeID)
				require.Equal(t, freshRuntime, r.LaunchID, "new clear must not inherit the old launch fence")
				require.NotEqual(t, old.RuntimeID, r.RuntimeID)
				require.NotEqual(t, old.LaunchID, r.LaunchID)
				require.Empty(t, r.Outcome, "clear must not be a recovery runtime")
				newer.Controlled, newer.AllowLaunch = true, true
				newer.RuntimeID, newer.LaunchID, newer.LauncherAttemptID = r.RuntimeID, r.LaunchID, r.LauncherAttemptID
				_, dir := utils.GetExecutionDirAndSubDir(newer)
				t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
				require.NoError(t, utils.OpenExecutionFence(ctx, newer))
				require.NoError(t, db.CheckLaunch(ctx, newer))
				if old.RuntimeID != "" {
					require.NoError(t, utils.RequireClosedExecutionFence(ctx, old))
				}
				assertOwnerAndState(t, newer.WorkflowID, newer.RunID, saved)
			})
		}
	}
	for _, mode := range []string{"unknown", "closing", "recovery", "handoff", "handoff token", "handoff request", "handoff run", "handoff active", "uncontrolled", "wrong owner", "wrong runtime", "wrong launch", "wrong launcher", "wrong resource", "wrong UID", "resync command", "missing fence", "open fence", "foreign fence", "launched not launched"} {
		t.Run("fresh clear rejects/"+mode, func(t *testing.T) {
			old, newer, saved := fixture(t, types.Sync, "settled")
			switch mode {
			case "unknown":
				require.NoError(t, db.FinishExecution(ctx, old, "unknown"))
			case "closing", "recovery":
				outcome := ""
				if mode == "recovery" {
					outcome = "recovery"
				}
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job_execution_records SET phase='closing',outcome=$1 WHERE job_id=42`, outcome)
				require.NoError(t, err)
			case "handoff":
				require.NoError(t, db.FinishExecution(ctx, old, "published"))
				require.NoError(t, db.BeginHandoff(ctx, old, "retained-transfer"))
			case "handoff token", "handoff request", "handoff run", "handoff active", "uncontrolled":
				updates := map[string]string{"handoff token": "handoff_token='retained'", "handoff request": "handoff_request_id='retained'", "handoff run": "handoff_run_id='retained'", "handoff active": "handoff_active=true", "uncontrolled": "controlled=false"}
				_, err := db.client.ExecContext(ctx, `UPDATE enrollment_job_execution_records SET `+updates[mode]+` WHERE job_id=42`)
				require.NoError(t, err)
			case "wrong owner":
				old.RunID = "foreign-run"
			case "wrong runtime":
				old.RuntimeID = "foreign-runtime"
			case "wrong launch":
				old.LaunchID = "foreign-launch"
			case "wrong launcher":
				old.LauncherAttemptID = "foreign-launcher"
			case "wrong resource":
				old.ResourceID = "foreign-resource"
			case "wrong UID":
				old.ResourceUID = "foreign-uid"
			case "resync command":
				old.Command = types.Resync
			case "missing fence":
				_, dir := utils.GetExecutionDirAndSubDir(old)
				require.NoError(t, os.Remove(dir+"/"+utils.ExecutionFenceFile))
			case "open fence", "foreign fence":
				_, dir := utils.GetExecutionDirAndSubDir(old)
				state, runtime := "open", old.RuntimeID
				if mode == "foreign fence" {
					state, runtime = "closed", "foreign"
				}
				data := fmt.Sprintf(`{"version":1,"execution_id":%q,"state":%q}`, runtime, state)
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.ExecutionFenceFile, data))
			case "launched not launched":
				require.NoError(t, db.FinishExecution(ctx, old, "not_launched"))
			}
			before, err := db.GetExecution(ctx, old.JobID, old.WorkflowID)
			require.NoError(t, err)
			require.Error(t, db.AdoptExecution(ctx, old, newer))
			after, err := db.GetExecution(ctx, old.JobID, old.WorkflowID)
			require.NoError(t, err)
			require.Equal(t, before, after)
			assertOwnerAndState(t, old.WorkflowID, "old-run", saved)
		})
	}
	t.Run("concurrent clear adoption has one atomic owner", func(t *testing.T) {
		old, first, saved := fixture(t, types.Sync, "settled")
		second := executionTestRequest(old.WorkflowID, "other-clear-run", types.ClearDestination)
		start := make(chan struct{})
		results := make(chan *types.ExecutionRequest, 2)
		failures := make(chan error, 2)
		var wg sync.WaitGroup
		for _, newer := range []*types.ExecutionRequest{first, second} {
			wg.Add(1)
			go func(newer *types.ExecutionRequest) {
				defer wg.Done()
				<-start
				if err := db.AdoptExecution(ctx, old, newer); err != nil {
					failures <- err
				} else {
					results <- newer
				}
			}(newer)
		}
		close(start)
		wg.Wait()
		require.Len(t, results, 1)
		require.Len(t, failures, 1)
		winner := <-results
		r, err := db.GetExecution(ctx, old.JobID, old.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, winner.RunID, r.OwnerRunID)
		require.Equal(t, "unadmitted", r.Phase)
		require.Empty(t, r.LaunchID)
		assertOwnerAndState(t, winner.WorkflowID, winner.RunID, saved)
	})
	t.Run("concurrent owner release rolls back clear record adoption", func(t *testing.T) {
		old, newer, saved := fixture(t, types.Sync, "settled")
		before, err := db.GetExecution(ctx, old.JobID, old.WorkflowID)
		require.NoError(t, err)
		blocker, err := db.client.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer blocker.Rollback()
		var id int
		require.NoError(t, blocker.QueryRowContext(ctx, `SELECT id FROM enrollment_job WHERE id=42 FOR UPDATE`).Scan(&id))
		done := make(chan error, 1)
		go func() { done <- db.AdoptExecution(ctx, old, newer) }()
		require.Eventually(t, func() bool {
			var waiting bool
			err := db.client.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND query LIKE 'UPDATE "enrollment_job" SET execution_run_id=%')`).Scan(&waiting)
			return err == nil && waiting
		}, 2*time.Second, 5*time.Millisecond)
		// Release does not take the advisory lock; this is its owner-row mutation.
		_, err = blocker.ExecContext(ctx, `UPDATE enrollment_job SET execution_workflow_id=NULL,execution_run_id=NULL WHERE id=42 AND execution_workflow_id=$1 AND execution_run_id=$2`, old.WorkflowID, old.RunID)
		require.NoError(t, err)
		require.NoError(t, blocker.Commit())
		require.ErrorIs(t, <-done, ErrJobExecutionOwned)
		after, err := db.GetExecution(ctx, old.JobID, old.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, before, after, "zero-row owner transfer must roll back epoch reset")
		var state string
		var unowned bool
		require.NoError(t, db.client.QueryRowContext(ctx, `SELECT state,execution_workflow_id IS NULL AND execution_run_id IS NULL FROM enrollment_job WHERE id=42`).Scan(&state, &unowned))
		require.Equal(t, saved, state)
		require.True(t, unowned)
	})
}
