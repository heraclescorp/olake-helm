package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/failure"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type atomicActivityDB struct {
	*enrollmentActivityDB
	owner    string
	releases int
	readErr  error
}

func (db *atomicActivityDB) AcquireJobExecution(_ context.Context, _ int, id string) error {
	if db.owner != "" && db.owner != id {
		return database.ErrJobExecutionOwned
	}
	db.owner = id
	return nil
}
func (db *atomicActivityDB) ReleaseJobExecution(_ context.Context, _ int, id string) error {
	if db.owner == id {
		db.owner = ""
		db.releases++
	}
	return nil
}

func (db *atomicActivityDB) GetJobData(ctx context.Context, id int) (types.JobData, error) {
	if db.readErr != nil {
		return types.JobData{}, db.readErr
	}
	return db.enrollmentActivityDB.GetJobData(ctx, id)
}

type atomicActivityExecutor struct {
	execute func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error)
	cleanup func(bool) error
}

func (e *atomicActivityExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	return e.execute(ctx, req)
}
func (e *atomicActivityExecutor) CleanupAndPersistState(context.Context, *types.ExecutionRequest) error {
	panic("resync must not use generic cleanup")
}
func (e *atomicActivityExecutor) CleanupResync(_ context.Context, _ *types.ExecutionRequest, succeeded bool) error {
	return e.cleanup(succeeded)
}

func atomicArgs(id string) []string {
	return []string{"resync", "--config", "/mnt/config/source.json", "--destination", "/mnt/config/destination.json", "--state", "/mnt/config/state.json", constants.ResyncNamespaceFlag, "public", constants.ResyncTableFlag, "target", constants.ResyncIDFlag, id}
}

func TestResyncLoadsFullSavedInputsWithoutAutomaticEnrollment(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			req, originalDB := activityEnrollmentFixture(t)
			req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
			req.Configs = []types.JobConfig{{Name: constants.StreamsFile, Data: `{"scoped":"must not ingest"}`}, {Name: "state.json", Data: `{"partial":true}`}}
			db := &atomicActivityDB{enrollmentActivityDB: originalDB}
			if split {
				db.job.AvailableStreams = `{"streams":[{"stream":{"namespace":"public","name":"target"}},{"stream":{"namespace":"public","name":"other"}}]}`
				db.job.SelectedStreams = `{"selected_streams":{"public":[{"stream_name":"target"},{"stream_name":"other"}]}}`
				db.job.Streams = ""
			}
			calls := 0
			a := &Activity{db: db, executor: &atomicActivityExecutor{execute: func(_ context.Context, request *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				calls++
				require.Equal(t, req.WorkflowID, db.owner)
				require.Equal(t, types.Resync, request.Command)
				require.Equal(t, db.job.Driver, request.ConnectorType)
				require.Equal(t, db.job.Version, request.Version)
				require.Empty(t, request.TempPath)
				configs := make(map[string]string)
				for _, config := range request.Configs {
					configs[config.Name] = config.Data
				}
				require.Equal(t, db.job.Source, configs["source.json"])
				require.Equal(t, db.job.Destination, configs["destination.json"])
				require.Equal(t, db.job.State, configs["state.json"])
				if split {
					require.Equal(t, db.job.AvailableStreams, configs[constants.AvailableStreamsFile])
					require.Equal(t, db.job.SelectedStreams, configs[constants.SelectedStreamsFile])
					require.NotContains(t, configs, constants.StreamsFile)
				} else {
					require.Equal(t, db.job.Streams, configs[constants.StreamsFile])
				}
				target, err := utils.ResyncTargetFromRequest(request)
				require.NoError(t, err)
				require.Equal(t, req.WorkflowID, target.ID)
				return &types.ExecutorResponse{}, nil
			}}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ResyncActivity)
			_, err := env.ExecuteActivity(a.ResyncActivity, req)
			require.NoError(t, err)
			require.Equal(t, 1, calls, "no discovery/enrollment command may run during resync")
			require.Zero(t, db.commits)
		})
	}
}

func TestDelayedSyncCannotLoadInputsAfterResyncAcquiresOwnership(t *testing.T) {
	req, original := activityEnrollmentFixture(t)
	db := &atomicActivityDB{enrollmentActivityDB: original, owner: "active-resync-operation"}
	a := &Activity{db: db, executor: &atomicActivityExecutor{}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.SyncActivity)
	_, err := env.ExecuteActivity(a.SyncActivity, req)
	require.Error(t, err)
	require.Zero(t, db.reads)
	require.Equal(t, "active-resync-operation", db.owner)
}

func TestAtomicResyncWorkflowScheduleAndStateSafety(t *testing.T) {
	terminal := temporal.NewNonRetryableApplicationError("terminal failure", "test", nil)
	for _, tc := range []struct {
		name             string
		executeErr       error
		cancel           bool
		cleanupErr       error
		transientCleanup bool
		unknown          bool
		restoreErr       error
		patchErr         error
		loseAck          bool
	}{
		{name: "success"},
		{name: "retry persistence", transientCleanup: true},
		{name: "lost handoff acknowledgement", loseAck: true},
		{name: "failed unpublished", executeErr: terminal},
		{name: "cancelled unpublished", cancel: true},
		{name: "unknown publication retains owner", executeErr: terminal, unknown: true},
		{name: "cleanup failed", cleanupErr: terminal},
		{name: "restore failed", restoreErr: terminal},
		{name: "handoff failed", patchErr: terminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id = "sync-project-42-resync-operation"
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: id})
			db := &atomicActivityDB{enrollmentActivityDB: &enrollmentActivityDB{}, owner: id}
			handle := &resyncScheduleHandle{updateErr: tc.restoreErr, patchErr: tc.patchErr, loseFirstAck: tc.loseAck, schedule: client.Schedule{
				Spec:   &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}},
				Policy: &client.SchedulePolicies{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
				State:  &client.ScheduleState{Paused: true},
				Action: &client.ScheduleWorkflowAction{Args: []interface{}{&types.ExecutionRequest{Command: types.Sync}}},
			}}
			attempts, persisted := 0, false
			a := &Activity{db: db, executor: &atomicActivityExecutor{cleanup: func(succeeded bool) error {
				attempts++
				require.Equal(t, tc.executeErr == nil && !tc.cancel, succeeded)
				if tc.transientCleanup && attempts == 1 {
					return errors.New("temporary persistence failure")
				}
				if tc.cleanupErr != nil {
					return tc.cleanupErr
				}
				if tc.unknown {
					return executor.ErrResyncUnconfirmed
				}
				persisted = succeeded
				return nil
			}}, tempClient: &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}, service: &resyncServiceClient{patch: func(ctx context.Context, req *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error) {
				require.True(t, persisted)
				require.Empty(t, db.owner)
				return handle.patch(ctx, req)
			}}}}
			env.RegisterActivity(a.ResyncActivity)
			env.RegisterActivity(a.PostResyncActivity)
			env.RegisterActivity(a.ReleaseJobExecutionActivity)
			env.RegisterActivity(a.TriggerSyncAfterResyncActivity)
			env.RegisterActivity(a.SendWebhookNotificationActivity)
			call := env.OnActivity(ResyncActivity, mock.Anything, mock.Anything).Return(&types.ExecutorResponse{}, tc.executeErr).Once()
			if tc.cancel {
				call.After(time.Hour)
				env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
			}
			if tc.executeErr != nil {
				env.OnActivity(SendWebhookNotificationActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
			}
			env.ExecuteWorkflow(RunSyncWorkflow, map[string]interface{}{"command": "resync", "project_id": "project", "job_id": 42, "args": atomicArgs(id), "configs": []types.JobConfig{{Name: "state.json", Data: `{"stale":true}`}}})
			require.True(t, env.IsWorkflowCompleted())
			wantSuccess := tc.executeErr == nil && !tc.cancel && tc.cleanupErr == nil && tc.restoreErr == nil && tc.patchErr == nil
			if wantSuccess {
				require.NoError(t, env.GetWorkflowError())
			} else {
				require.Error(t, env.GetWorkflowError())
			}
			desc, err := handle.Describe(context.Background())
			require.NoError(t, err)
			require.Equal(t, !wantSuccess, desc.Schedule.State.Paused)
			require.Equal(t, persisted, tc.executeErr == nil && !tc.cancel && tc.cleanupErr == nil)
			if tc.unknown || tc.cleanupErr != nil || tc.restoreErr != nil {
				require.Equal(t, id, db.owner)
			} else {
				require.Empty(t, db.owner)
			}
			if tc.restoreErr == nil {
				restored := desc.Schedule.Action.(*client.ScheduleWorkflowAction).Args[0].(*types.ExecutionRequest)
				require.Equal(t, types.Sync, restored.Command)
				require.Empty(t, restored.Configs)
				require.NotContains(t, restored.Args, constants.ResyncIDFlag)
				for _, paused := range handle.pausedAfterUpdate {
					require.True(t, paused)
				}
			}
			if wantSuccess {
				require.Equal(t, 1, handle.triggers)
				for _, requestID := range handle.requestIDs {
					require.Equal(t, id+"/atomic-resync-sync", requestID)
				}
			} else {
				require.Zero(t, handle.triggers)
			}
		})
	}
}

func TestUnsupportedResyncClassificationIsExact(t *testing.T) {
	code := 1
	for _, tc := range []struct {
		log         string
		unsupported bool
	}{
		{`Error: unknown command "resync" for "olake"`, true},
		{`{"level":"fatal","message":"'resync' is an invalid command. Use 'olake --help' to display usage guide"}`, true},
		{`publish failed: unknown command`, false},
		{`{"message":"unknown command resync"}`, false},
	} {
		require.Equal(t, tc.unsupported, unsupportedResync(&failure.ExecutionFailure{ExitCode: &code, LogTail: tc.log}))
	}
}

func readResyncFailureEvidence(t *testing.T, req *types.ExecutionRequest) map[string]interface{} {
	t.Helper()
	_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	data, err := storage.ReadFile(context.Background(), dir, constants.ResyncFailureFile, false)
	require.NoError(t, err)
	var proof map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(data), &proof))
	require.Equal(t, float64(1), proof["version"])
	require.Equal(t, req.WorkflowID, proof["resync_id"])
	require.Equal(t, "public", proof["namespace"])
	require.Equal(t, "target", proof["stream_name"])
	return proof
}

func TestResyncPrelaunchFailuresReleaseOnlyWithDurableUnpublishedProof(t *testing.T) {
	for _, mode := range []string{"database read", "invalid settings", "cancelled preflight", "proof write failure", "prior invocation unknown"} {
		t.Run(mode, func(t *testing.T) {
			req, original := activityEnrollmentFixture(t)
			req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
			db := &atomicActivityDB{enrollmentActivityDB: original}
			_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
			const saved = `{"global":{"lsn":"keep-original"},"streams":{"other":{"cursor":"unchanged"}}}`
			require.NoError(t, storage.WriteFiles(context.Background(), dir, []types.JobConfig{{Name: "state.json", Data: saved}}))
			switch mode {
			case "invalid settings":
				db.job.AdvancedSettings = "invalid JSON"
			case "cancelled preflight":
				db.readErr = context.Canceled
			default:
				db.readErr = errors.New("database unavailable before execution")
			}
			if mode == "proof write failure" {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, constants.ResyncFailureFile), 0755))
			}
			if mode == "prior invocation unknown" {
				require.NoError(t, writeResyncEvidence(context.Background(), req, constants.ResyncExecutionFile, "execution_started"))
				require.NoError(t, writeResyncEvidence(context.Background(), req, constants.ResyncFailureFile, "in_progress"))
			}
			handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}}}
			a := &Activity{db: db, executor: &atomicActivityExecutor{execute: func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				t.Fatal("preflight failure must not invoke executor")
				return nil, nil
			}, cleanup: func(succeeded bool) error {
				require.False(t, succeeded)
				if mode == "proof write failure" || mode == "prior invocation unknown" {
					return executor.ErrResyncUnconfirmed
				}
				require.Equal(t, "not_published", readResyncFailureEvidence(t, req)["publication"])
				return nil
			}}, tempClient: &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ResyncActivity)
			_, err := env.ExecuteActivity(a.ResyncActivity, req)
			require.Error(t, err)
			require.Equal(t, req.WorkflowID, db.owner)
			postErr := a.PostResyncActivity(context.Background(), req, syncStatusFailed)
			if mode == "proof write failure" || mode == "prior invocation unknown" {
				require.Error(t, postErr)
				require.Equal(t, req.WorkflowID, db.owner)
				if mode == "prior invocation unknown" {
					require.Equal(t, "in_progress", readResyncFailureEvidence(t, req)["publication"])
				}
			} else {
				require.NoError(t, postErr)
				require.NoError(t, a.ReleaseJobExecutionActivity(context.Background(), req))
				require.Empty(t, db.owner)
			}
			state, err := storage.ReadFile(context.Background(), dir, "state.json", false)
			require.NoError(t, err)
			require.Equal(t, saved, state)
			require.True(t, handle.schedule.State.Paused)
		})
	}
}

func TestUnsupportedResyncCannotErasePriorUnknownPublication(t *testing.T) {
	for _, mode := range []string{"first invocation", "prior unknown", "prior confirmed unpublished"} {
		t.Run(mode, func(t *testing.T) {
			req, original := activityEnrollmentFixture(t)
			req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
			db := &atomicActivityDB{enrollmentActivityDB: original}
			if mode != "first invocation" {
				require.NoError(t, writeResyncEvidence(context.Background(), req, constants.ResyncExecutionFile, "execution_started"))
				publication := "in_progress"
				if mode == "prior confirmed unpublished" {
					publication = "not_published"
				}
				require.NoError(t, writeResyncEvidence(context.Background(), req, constants.ResyncFailureFile, publication))
			}
			code := 1
			handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}}}
			a := &Activity{db: db, executor: &atomicActivityExecutor{execute: func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				return nil, &failure.ExecutionFailure{ExitCode: &code, LogTail: `Error: unknown command "resync" for "olake"`}
			}, cleanup: func(succeeded bool) error {
				require.False(t, succeeded)
				if readResyncFailureEvidence(t, req)["publication"] != "not_published" {
					return executor.ErrResyncUnconfirmed
				}
				return nil
			}}, tempClient: &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ResyncActivity)
			_, err := env.ExecuteActivity(a.ResyncActivity, req)
			require.Error(t, err)
			proof := readResyncFailureEvidence(t, req)
			if mode == "prior unknown" {
				require.Equal(t, "in_progress", proof["publication"])
			} else {
				require.Equal(t, "not_published", proof["publication"])
			}
			postErr := a.PostResyncActivity(context.Background(), req, syncStatusFailed)
			if mode == "prior unknown" {
				require.Error(t, postErr)
				require.Equal(t, req.WorkflowID, db.owner)
				require.Zero(t, db.releases)
			} else {
				require.NoError(t, postErr)
				require.NoError(t, a.ReleaseJobExecutionActivity(context.Background(), req))
				require.Empty(t, db.owner)
			}
		})
	}
}

type ordinaryCleanupExecutor struct {
	cleanup func(context.Context, *types.ExecutionRequest) error
}

func (e *ordinaryCleanupExecutor) Execute(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	panic("unexpected execution")
}
func (e *ordinaryCleanupExecutor) CleanupAndPersistState(ctx context.Context, req *types.ExecutionRequest) error {
	return e.cleanup(ctx, req)
}
func (e *ordinaryCleanupExecutor) CleanupResync(context.Context, *types.ExecutionRequest, bool) error {
	panic("unexpected resync cleanup")
}

func TestOrdinaryCleanupResponseLossRetainsOwnerUntilRecordedRelease(t *testing.T) {
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		t.Run(string(command), func(t *testing.T) {
			req, original := activityEnrollmentFixture(t)
			req.Command = command
			db := &atomicActivityDB{enrollmentActivityDB: original, owner: req.WorkflowID}
			cleanupCalls, postCalls := 0, 0
			handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}}}
			a := &Activity{db: db, executor: &ordinaryCleanupExecutor{cleanup: func(ctx context.Context, request *types.ExecutionRequest) error {
				cleanupCalls++
				require.Equal(t, req.WorkflowID, db.owner)
				require.ErrorIs(t, db.AcquireJobExecution(ctx, req.JobID, "newer-workflow"), database.ErrJobExecutionOwned)
				return nil
			}}, tempClient: &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: req.WorkflowID})
			env.RegisterActivity(a.ReleaseJobExecutionActivity)
			if command == types.Sync {
				env.RegisterActivity(a.SyncActivity)
				env.RegisterActivity(a.PostSyncActivity)
				env.OnActivity(SyncActivity, mock.Anything, mock.Anything).Return(&types.ExecutorResponse{}, nil).Once()
				env.OnActivity(PostSyncActivity, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *types.ExecutionRequest, status syncStatus) error {
					if err := a.PostSyncActivity(ctx, request, status); err != nil {
						return err
					}
					postCalls++
					require.Equal(t, req.WorkflowID, db.owner)
					if postCalls == 1 {
						return errors.New("cleanup completed but response lost")
					}
					return nil
				}).Times(2)
			} else {
				env.RegisterActivity(a.ExecuteActivity)
				env.RegisterActivity(a.PostClearActivity)
				env.OnActivity(ExecuteActivity, mock.Anything, mock.Anything).Return(&types.ExecutorResponse{}, nil).Once()
				env.OnActivity(PostClearActivity, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *types.ExecutionRequest) error {
					if err := a.PostClearActivity(ctx, request); err != nil {
						return err
					}
					postCalls++
					require.Equal(t, req.WorkflowID, db.owner)
					if postCalls == 1 {
						return errors.New("cleanup completed but response lost")
					}
					return nil
				}).Times(2)
			}
			env.ExecuteWorkflow(RunSyncWorkflow, map[string]interface{}{"command": string(command), "job_id": req.JobID, "project_id": req.ProjectID, "version": original.job.Version, "connector_type": original.job.Driver})
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, 2, cleanupCalls)
			require.Equal(t, 2, postCalls)
			require.Empty(t, db.owner)
			require.Equal(t, 1, db.releases)
		})
	}
}
