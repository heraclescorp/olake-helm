package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

const activityEnrollmentSettings = `{"index_required":false,"target_query_engines":["snowflake"],"auto_table_enrollment":{"enabled":true,"schemas":[{"namespace":"public","destination_database":"lake"}],"defaults":{"normalization":true,"append_mode":false,"update_type":"pos","sync_new_columns":false}}}`
const activityOldCatalog = `{"streams":[{"stream":{"namespace":"public","name":"old"}}],"selected_streams":{"public":[]}}`
const activityNewCatalog = `{"streams":[{"stream":{"namespace":"public","name":"old"}},{"stream":{"namespace":"public","name":"new","destination_database":"lake","destination_table":"new","sync_mode":"cdc"}}],"selected_streams":{"public":[{"stream_name":"new","normalization":true,"append_mode":false,"update_type":"pos","selected_columns":{"columns":["id"],"sync_new_columns":false}}]}}`

type enrollmentActivityDB struct {
	job             types.JobData
	commits         int
	reads           int
	commitErr       error
	commitThenError bool
	failFreshRead   bool
}

func (db *enrollmentActivityDB) GetJobData(context.Context, int) (types.JobData, error) {
	db.reads++
	if db.failFreshRead && db.commits > 0 {
		return types.JobData{}, errors.New("database unavailable after commit")
	}
	return db.job, nil
}
func (db *enrollmentActivityDB) GetProjectSettingsByProjectID(context.Context, string) (*types.ProjectSettings, error) {
	return &types.ProjectSettings{}, nil
}
func (db *enrollmentActivityDB) CommitCatalogEnrollment(ctx context.Context, _ int, original types.JobData, result types.EnrollmentResult) error {
	db.commits++
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.commitErr != nil && !db.commitThenError {
		return db.commitErr
	}
	if err := database.SameEnrollmentContext(original, db.job); err != nil {
		return err
	}
	proposed, err := original.ValidateEnrollment(result)
	if err != nil {
		return err
	}
	proposed.State = db.job.State
	db.job = proposed
	return db.commitErr
}

type enrollmentActivityExecutor struct {
	execute func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error)
}

func (e *enrollmentActivityExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	return e.execute(ctx, req)
}
func (e *enrollmentActivityExecutor) CleanupAndPersistState(context.Context, *types.ExecutionRequest) error {
	return nil
}

func activityEnrollmentFixture(t *testing.T) (*types.ExecutionRequest, *enrollmentActivityDB) {
	t.Helper()
	oldMode, oldKube := viper.Get(constants.EnvStorageMode), viper.Get(constants.EnvKubernetesServiceHost)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	viper.Set(constants.EnvKubernetesServiceHost, "")
	t.Cleanup(func() {
		viper.Set(constants.EnvStorageMode, oldMode)
		viper.Set(constants.EnvKubernetesServiceHost, oldKube)
	})
	req := &types.ExecutionRequest{Command: types.Sync, ConnectorType: "stale", Version: "stale", JobID: int(time.Now().UnixNano()),
		WorkflowID: fmt.Sprintf("enrollment-test-%s-%d", t.Name(), time.Now().UnixNano()), ProjectID: "project", Timeout: time.Minute,
		Args: []string{"sync", "--config", "/mnt/config/source.json", "--destination", "/mnt/config/destination.json", "--state", "/mnt/config/state.json"}}
	_, workDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(workDir)) })
	// Mark the synthetic job ineligible for telemetry run counting, so tests never
	// need a Temporal server or emit a telemetry callback for this current CLI.
	counter := filepath.Join("telemetry", "job-counters", fmt.Sprint(req.JobID))
	require.NoError(t, storage.WriteFiles(context.Background(), storage.ConfigDir(), []types.JobConfig{{Name: counter, Data: `{"ineligible":true}`}}))
	t.Cleanup(func() { require.NoError(t, os.Remove(filepath.Join(storage.ConfigDir(), counter))) })
	return req, &enrollmentActivityDB{job: types.JobData{Driver: "postgres", DestinationDriver: "iceberg", Version: "v0.14.0", ProjectID: "project",
		Source: `{"host":"source"}`, Destination: `{"type":"ICEBERG","writer":{}}`, Streams: activityOldCatalog, AdvancedSettings: activityEnrollmentSettings,
		State: `{"global":{"lsn":"exact-original-lsn"},"streams":{"public.old":{"cursor":"keep"}}}`}}
}

func writeActivityEnrollmentResult(t *testing.T, ctx context.Context, req *types.ExecutionRequest, result types.EnrollmentResult) {
	t.Helper()
	_, workDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(workDir)) })
	data, err := json.Marshal(result)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFiles(ctx, workDir, []types.JobConfig{{Name: enrollmentResultFile, Data: string(data)}}))
}

func TestPreflightRequestUsesSourceImageIsolatedMetadataAndDefaultScheduling(t *testing.T) {
	req, db := activityEnrollmentFixture(t)
	for _, split := range []bool{false, true} {
		job := db.job
		if split {
			job.AvailableStreams = `{"streams":[{"stream":{"namespace":"public","name":"old"}}]}`
			job.SelectedStreams = `{"selected_streams":{"public":[]}}`
			job.Streams = ""
		}
		policy, err := job.AutoTableEnrollment()
		require.NoError(t, err)
		preflight, err := enrollmentPreflightRequest(req, job, policy, "attempt-1")
		require.NoError(t, err)
		require.Equal(t, types.DiscoverNewTables, preflight.Command)
		require.Equal(t, utils.GetDockerImageName(job.Driver, job.Version), utils.GetDockerImageName(preflight.ConnectorType, preflight.Version))
		require.False(t, preflight.IndexRequired)
		require.False(t, utils.IsAsyncCommand(preflight.Command))
		require.Equal(t, enrollmentResultFile, preflight.OutputFile)
		require.NotContains(t, preflight.Args, constants.StateFlag)
		require.NotEqual(t, req.WorkflowID, preflight.WorkflowID)
		_, ordinaryDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
		_, preflightDir := utils.GetWorkflowDirAndSubDir(preflight.WorkflowID, preflight.Command)
		require.NotEqual(t, ordinaryDir, preflightDir)
		for _, config := range preflight.Configs {
			require.NotEqual(t, "state.json", config.Name)
		}
		if split {
			require.Contains(t, preflight.Args, constants.AvailableStreamsFlag)
			require.Contains(t, preflight.Args, constants.SelectedStreamsFlag)
			require.NotContains(t, preflight.Args, constants.StreamsFlag)
		} else {
			require.Contains(t, preflight.Args, constants.StreamsFlag)
		}
		second, err := enrollmentPreflightRequest(req, job, policy, "attempt-2")
		require.NoError(t, err)
		require.NotEqual(t, preflight.WorkflowID, second.WorkflowID)
	}
}

func TestSyncCannotIngestUntilEnrollmentIsDurableAndFresh(t *testing.T) {
	for _, mode := range []string{"disabled", "enroll", "no new", "skips", "discovery failure", "invalid proposal", "commit conflict", "postcommit read failure", "cancel during discovery"} {
		t.Run(mode, func(t *testing.T) {
			req, db := activityEnrollmentFixture(t)
			if mode == "disabled" {
				db.job.AdvancedSettings = `{"auto_table_enrollment":{"enabled":false}}`
			}
			if mode == "commit conflict" {
				db.commitErr = database.ErrEnrollmentConflict
			}
			if mode == "postcommit read failure" {
				db.failFreshRead = true
			}
			originalState := db.job.State
			var commands []types.Command
			var cancel context.CancelFunc
			exec := &enrollmentActivityExecutor{execute: func(ctx context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				commands = append(commands, request.Command)
				if request.Command == types.DiscoverNewTables {
					require.Equal(t, 0, db.commits)
					if mode == "discovery failure" {
						return nil, errors.New("discovery unavailable")
					}
					result := types.EnrollmentResult{Enrolled: []types.EnrollmentID{{Namespace: "public", StreamName: "new"}}, Streams: activityNewCatalog}
					if mode == "no new" || mode == "skips" {
						result.Enrolled = nil
						result.Streams = activityOldCatalog
					}
					if mode == "skips" {
						result.Skipped = []types.EnrollmentSkip{{EnrollmentID: types.EnrollmentID{Namespace: "public", StreamName: "no_pk"}, Reason: "stable primary key required"}}
					}
					if mode == "invalid proposal" {
						result.Streams = strings.Replace(result.Streams, `"name":"old"`, `"name":"edited"`, 1)
					}
					writeActivityEnrollmentResult(t, ctx, request, result)
					if mode == "cancel during discovery" {
						cancel()
					}
					return &types.ExecutorResponse{}, nil
				}
				if mode != "disabled" {
					require.Equal(t, 1, db.commits, "ordinary ingestion must follow a durable transaction")
					require.GreaterOrEqual(t, db.reads, 2, "ordinary ingestion must use a post-commit read")
				}
				require.Equal(t, "postgres", request.ConnectorType)
				require.Equal(t, "v0.14.0", request.Version)
				configs := make(map[string]string)
				for _, config := range request.Configs {
					configs[config.Name] = config.Data
				}
				require.Equal(t, db.job.Streams, configs[constants.StreamsFile])
				require.Equal(t, originalState, configs["state.json"])
				if mode == "enroll" {
					require.True(t, request.IndexRequired)
				}
				return &types.ExecutorResponse{Response: "ingested"}, nil
			}}
			a := &Activity{db: db, executor: exec}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			run := func(ctx context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				child, stop := context.WithCancel(ctx)
				defer stop()
				cancel = stop
				return a.SyncActivity(child, request)
			}
			env.RegisterActivity(run)
			_, err := env.ExecuteActivity(run, req)
			if strings.Contains(mode, "failure") || mode == "invalid proposal" || mode == "commit conflict" || mode == "cancel during discovery" {
				require.Error(t, err)
				require.Equal(t, []types.Command{types.DiscoverNewTables}, commands)
			} else {
				require.NoError(t, err)
				if mode == "disabled" {
					require.Equal(t, []types.Command{types.Sync}, commands)
					require.Zero(t, db.commits)
				} else {
					require.Equal(t, []types.Command{types.DiscoverNewTables, types.Sync}, commands)
				}
			}
			require.Equal(t, originalState, db.job.State)
		})
	}
}

func TestRetryAfterDurableCommitDoesNotDuplicateOrIngestUnacknowledgedProposal(t *testing.T) {
	req, db := activityEnrollmentFixture(t)
	db.commitThenError, db.commitErr = true, errors.New("commit acknowledgement lost")
	var commands []types.Command
	exec := &enrollmentActivityExecutor{execute: func(ctx context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
		commands = append(commands, request.Command)
		if request.Command == types.DiscoverNewTables {
			result := types.EnrollmentResult{Streams: db.job.Streams}
			if db.job.Streams == activityOldCatalog {
				result.Streams = activityNewCatalog
				result.Enrolled = []types.EnrollmentID{{Namespace: "public", StreamName: "new"}}
			}
			writeActivityEnrollmentResult(t, ctx, request, result)
		}
		return &types.ExecutorResponse{}, nil
	}}
	a := &Activity{db: db, executor: exec}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.SyncActivity)
	_, err := env.ExecuteActivity(a.SyncActivity, req)
	require.Error(t, err)
	require.Equal(t, []types.Command{types.DiscoverNewTables}, commands)
	require.Equal(t, activityNewCatalog, db.job.Streams, "the simulated lost acknowledgement happened after the durable commit")
	db.commitErr = nil
	commands = nil
	_, err = env.ExecuteActivity(a.SyncActivity, req)
	require.NoError(t, err)
	require.Equal(t, []types.Command{types.DiscoverNewTables, types.Sync}, commands)
	require.Equal(t, 1, strings.Count(db.job.Streams, `"name":"new"`))
	require.Equal(t, 1, strings.Count(db.job.Streams, `"stream_name":"new"`))
}

func TestSyncRetryEnrollmentUsesActualOrdinaryLaunchBoundary(t *testing.T) {
	for _, launched := range []bool{false, true} {
		t.Run(fmt.Sprintf("launched=%t", launched), func(t *testing.T) {
			req, db := activityEnrollmentFixture(t)
			originalState := db.job.State
			lateCatalog := strings.Replace(activityNewCatalog, `"name":"new"`, `"name":"late"`, 1)
			lateCatalog = strings.Replace(lateCatalog, `"destination_table":"new"`, `"destination_table":"late"`, 1)
			lateCatalog = strings.Replace(lateCatalog, `"stream_name":"new"`, `"stream_name":"late"`, 1)
			// Keep the previously enrolled table and its settings unchanged.
			lateCatalog = strings.Replace(lateCatalog, `"streams":[`, `"streams":[{"stream":{"namespace":"public","name":"new","destination_database":"lake","destination_table":"new","sync_mode":"cdc"}},`, 1)
			lateCatalog = strings.Replace(lateCatalog, `"public":[`, `"public":[{"stream_name":"new","normalization":true,"append_mode":false,"update_type":"pos","selected_columns":{"columns":["id"],"sync_new_columns":false}},`, 1)
			lateTableExists := false
			providerFailure := true
			exec := &enrollmentActivityExecutor{execute: func(ctx context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				if request.Command == types.DiscoverNewTables {
					result := types.EnrollmentResult{Streams: activityNewCatalog}
					if db.job.Streams == activityOldCatalog {
						result.Enrolled = []types.EnrollmentID{{Namespace: "public", StreamName: "new"}}
					}
					if lateTableExists {
						result.Streams = lateCatalog
						result.Enrolled = append(result.Enrolled, types.EnrollmentID{Namespace: "public", StreamName: "late"})
					}
					writeActivityEnrollmentResult(t, ctx, request, result)
					return &types.ExecutorResponse{}, nil
				}
				// Ordinary retries retain files once connector logs establish launch.
				_, dir := utils.GetWorkflowDirAndSubDir(request.WorkflowID, request.Command)
				alreadyLaunched, err := utils.WorkflowAlreadyLaunched(ctx, dir)
				require.NoError(t, err)
				if !alreadyLaunched {
					require.NoError(t, storage.WriteFiles(ctx, dir, request.Configs))
				}
				require.NoError(t, utils.MarkEnrollmentIngestionStarted(ctx, request))
				if launched && !alreadyLaunched {
					require.NoError(t, storage.WriteFiles(ctx, dir, []types.JobConfig{
						{Name: filepath.Join("logs", "run", "olake.log"), Data: "connector started"},
						{Name: "state.json", Data: `{"global":{"lsn":"running-lsn"}}`},
					}))
				}
				if providerFailure {
					return nil, errors.New("ordinary provider execution failed")
				}
				return &types.ExecutorResponse{}, nil
			}}
			a := &Activity{db: db, executor: exec}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.SyncActivity)
			_, err := env.ExecuteActivity(a.SyncActivity, req)
			require.Error(t, err)
			require.JSONEq(t, activityNewCatalog, db.job.Streams)
			pending, err := utils.EnrollmentPreflightPending(context.Background(), req)
			require.NoError(t, err)
			require.False(t, pending, "both provider outcomes have the pre-launch gate set")

			lateTableExists, providerFailure = true, false
			_, err = env.ExecuteActivity(a.SyncActivity, req)
			require.NoError(t, err)
			wantCatalog, wantState := lateCatalog, originalState
			if launched {
				wantCatalog = activityNewCatalog
				wantState = `{"global":{"lsn":"running-lsn"}}`
			}
			require.JSONEq(t, wantCatalog, db.job.Streams, "a launched retry cannot persist newly discovered selections")
			_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
			runningCatalog, err := storage.ReadFile(context.Background(), dir, constants.StreamsFile, false)
			require.NoError(t, err)
			require.JSONEq(t, wantCatalog, runningCatalog)
			runningState, err := storage.ReadFile(context.Background(), dir, "state.json", false)
			require.NoError(t, err)
			require.Equal(t, wantState, runningState)
			require.Equal(t, originalState, db.job.State, "enrollment never changes the saved CDC state")

			if launched {
				// A different scheduled workflow has no ordinary launch marker and
				// must still enroll the table that appeared during the previous run.
				freshReq := *req
				freshReq.WorkflowID += "-next"
				_, freshDir := utils.GetWorkflowDirAndSubDir(freshReq.WorkflowID, freshReq.Command)
				t.Cleanup(func() { require.NoError(t, os.RemoveAll(freshDir)) })
				_, err = env.ExecuteActivity(a.SyncActivity, &freshReq)
				require.NoError(t, err)
				require.JSONEq(t, lateCatalog, db.job.Streams)
				freshCatalog, err := storage.ReadFile(context.Background(), freshDir, constants.StreamsFile, false)
				require.NoError(t, err)
				require.JSONEq(t, lateCatalog, freshCatalog)
				previousCatalog, err := storage.ReadFile(context.Background(), dir, constants.StreamsFile, false)
				require.NoError(t, err)
				require.JSONEq(t, activityNewCatalog, previousCatalog)
				require.Equal(t, originalState, db.job.State)
			}
		})
	}
}

func TestResyncFollowupReloadsFullCatalogAndRunsEnrollment(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprintf("split=%t", split), func(t *testing.T) {
			req, db := activityEnrollmentFixture(t)
			req.Command = types.ClearDestination
			req.StartSyncAfterClear = true
			req.TempPath = "table-resync/state.json"
			req.Configs = []types.JobConfig{
				{Name: constants.StreamsFile, Data: `{"streams":[{"stream":{"namespace":"public","name":"scoped-only"}}]}`},
				{Name: "source.json", Data: `{"host":"stale-source"}`},
				{Name: "state.json", Data: `{"global":{"lsn":"stale-lsn"}}`},
			}
			db.job.Version = "v0.15.0"
			db.job.State = `{"global":{"lsn":"preserved-lsn"},"streams":{"public.old":{"cursor":"after-clear"},"public.unrelated":{"cursor":"keep"}}}`
			state := db.job.State
			result := types.EnrollmentResult{Enrolled: []types.EnrollmentID{{Namespace: "public", StreamName: "new"}}, Streams: activityNewCatalog}
			if split {
				parts := func(catalog string) (string, string) {
					var fields map[string]json.RawMessage
					require.NoError(t, json.Unmarshal([]byte(catalog), &fields))
					return `{"streams":` + string(fields["streams"]) + `}`, `{"selected_streams":` + string(fields["selected_streams"]) + `}`
				}
				db.job.AvailableStreams, db.job.SelectedStreams = parts(db.job.Streams)
				db.job.Streams = ""
				result.AvailableStreams, result.SelectedStreams = parts(result.Streams)
				result.Streams = ""
				req.Args = utils.SetCatalogArgs(req.Args, true)
			}
			utils.RevertUpdatesInSchedule(req)
			require.Empty(t, req.Configs)
			require.Empty(t, req.TempPath)
			var ingested bool
			exec := &enrollmentActivityExecutor{execute: func(ctx context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				configs := make(map[string]string)
				for _, config := range request.Configs {
					configs[config.Name] = config.Data
				}
				require.Equal(t, db.job.Version, request.Version)
				require.Equal(t, db.job.Source, configs["source.json"])
				if request.Command == types.DiscoverNewTables {
					if split {
						require.JSONEq(t, db.job.AvailableStreams, configs[constants.AvailableStreamsFile])
						require.JSONEq(t, db.job.SelectedStreams, configs[constants.SelectedStreamsFile])
					} else {
						require.JSONEq(t, activityOldCatalog, configs[constants.StreamsFile])
					}
					require.NotContains(t, configs, "state.json")
					writeActivityEnrollmentResult(t, ctx, request, result)
					return &types.ExecutorResponse{}, nil
				}
				require.Equal(t, types.Sync, request.Command)
				require.Equal(t, state, configs["state.json"])
				if split {
					require.JSONEq(t, result.AvailableStreams, configs[constants.AvailableStreamsFile])
					require.JSONEq(t, result.SelectedStreams, configs[constants.SelectedStreamsFile])
					require.NotContains(t, request.Args, constants.CatalogFlag)
				} else {
					require.JSONEq(t, activityNewCatalog, configs[constants.StreamsFile])
					require.NotContains(t, request.Args, constants.AvailableStreamsFlag)
				}
				ingested = true
				return &types.ExecutorResponse{Response: "full-job-ingested"}, nil
			}}
			a := &Activity{db: db, executor: exec}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.SyncActivity)
			_, err := env.ExecuteActivity(a.SyncActivity, req)
			require.NoError(t, err)
			require.True(t, ingested)
			require.Equal(t, state, db.job.State)
			if split {
				require.JSONEq(t, result.AvailableStreams, db.job.AvailableStreams)
				require.JSONEq(t, result.SelectedStreams, db.job.SelectedStreams)
			} else {
				require.JSONEq(t, activityNewCatalog, db.job.Streams)
			}
		})
	}
}
