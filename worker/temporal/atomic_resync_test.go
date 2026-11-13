package temporal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type atomicActivityDB struct {
	*enrollmentActivityDB
	readErr error
}

func (db *atomicActivityDB) GetJobData(ctx context.Context, id int) (types.JobData, error) {
	if db.readErr != nil {
		return types.JobData{}, db.readErr
	}
	return db.enrollmentActivityDB.GetJobData(ctx, id)
}

type atomicActivityExecutor struct {
	execute         func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error)
	cleanup         func(bool) error
	cleanupRequest  func(context.Context, *types.ExecutionRequest, bool) error
	retire          func(context.Context, *types.ExecutionRequest) error
	ordinaryCleanup func(context.Context, *types.ExecutionRequest) error
}

func (e *atomicActivityExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	return e.execute(ctx, req)
}
func (e *atomicActivityExecutor) CleanupAndPersistState(ctx context.Context, req *types.ExecutionRequest) error {
	if e.ordinaryCleanup == nil {
		panic("resync cannot use ordinary cleanup")
	}
	return e.ordinaryCleanup(ctx, req)
}
func (e *atomicActivityExecutor) CleanupResync(ctx context.Context, req *types.ExecutionRequest, success bool) error {
	if e.cleanupRequest != nil {
		return e.cleanupRequest(ctx, req, success)
	}
	if e.cleanup == nil {
		return nil
	}
	return e.cleanup(success)
}
func (e *atomicActivityExecutor) RetireExecution(ctx context.Context, req *types.ExecutionRequest) error {
	if e.retire == nil {
		return nil
	}
	return e.retire(ctx, req)
}

func atomicArgs(id string) []string {
	return []string{"resync", "--config", "/mnt/config/source.json", "--destination", "/mnt/config/destination.json", "--state", "/mnt/config/state.json", constants.ResyncNamespaceFlag, "public", constants.ResyncTableFlag, "target", constants.ResyncIDFlag, id}
}

func TestResyncLoadsFullSavedInputsWithoutAutomaticEnrollment(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			req, base := activityEnrollmentFixture(t)
			req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
			req.Configs = []types.JobConfig{{Name: "state.json", Data: `{"partial":true}`}}
			db := &atomicActivityDB{enrollmentActivityDB: base}
			if split {
				db.job.AvailableStreams = `{"streams":[{"stream":{"namespace":"public","name":"target"}},{"stream":{"namespace":"public","name":"other"}}]}`
				db.job.SelectedStreams = `{"selected_streams":{"public":[{"stream_name":"target"},{"stream_name":"other"}]}}`
				db.job.Streams = ""
			}
			calls := 0
			a := &Activity{db: db, executor: &atomicActivityExecutor{execute: func(ctx context.Context, current *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				calls++
				require.Equal(t, req.WorkflowID, db.owner)
				require.Equal(t, req.RunID, db.run)
				require.True(t, current.AllowLaunch)
				require.NotEmpty(t, current.RuntimeID)
				require.NotEmpty(t, current.LaunchID)
				require.NoError(t, current.BeforeLaunch(ctx))
				require.Contains(t, current.Args, "--execution-fence")
				require.Contains(t, current.Args, "--resync-launch-id")
				require.Equal(t, types.Resync, current.Command)
				require.Equal(t, db.job.Version, current.Version)
				require.Empty(t, current.TempPath)
				configs := map[string]string{}
				for _, config := range current.Configs {
					configs[config.Name] = config.Data
				}
				require.Equal(t, db.job.Source, configs["source.json"])
				require.Equal(t, db.job.Destination, configs["destination.json"])
				require.Equal(t, db.job.State, configs["state.json"])
				if split {
					require.Equal(t, db.job.AvailableStreams, configs[constants.AvailableStreamsFile])
					require.Equal(t, db.job.SelectedStreams, configs[constants.SelectedStreamsFile])
				} else {
					require.Equal(t, db.job.Streams, configs[constants.StreamsFile])
				}
				target, err := utils.ResyncTargetFromRequest(current)
				require.NoError(t, err)
				require.Equal(t, req.WorkflowID, target.ID)
				return &types.ExecutorResponse{}, nil
			}}}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.ResyncActivity)
			_, err := env.ExecuteActivity(a.ResyncActivity, req)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Zero(t, db.commits)
		})
	}
}

func TestPreflightFailureCannotWriteOperationWideUnpublishedProof(t *testing.T) {
	req, base := activityEnrollmentFixture(t)
	req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
	db := &atomicActivityDB{enrollmentActivityDB: base, readErr: errors.New("preflight database unavailable")}
	a := &Activity{db: db, executor: &atomicActivityExecutor{execute: func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
		t.Fatal("preflight must not launch")
		return nil, nil
	}}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.ResyncActivity)
	_, err := env.ExecuteActivity(a.ResyncActivity, req)
	require.Error(t, err)
	r, err := db.GetExecution(context.Background(), req.JobID, req.WorkflowID)
	require.NoError(t, err)
	require.Equal(t, "unadmitted", r.Phase)
	_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	_, err = storage.ReadFile(context.Background(), dir, constants.ResyncFailureFile, false)
	require.Error(t, err, "only source launch-bound proof or SQL close-before-launch is authority")
}

func TestRetryAfterAdmissionCanOnlyAttach(t *testing.T) {
	req, db := activityEnrollmentFixture(t)
	req.Command, req.Args = types.Resync, atomicArgs(req.WorkflowID)
	modes := []bool{}
	a := &Activity{db: db, executor: &atomicActivityExecutor{execute: func(_ context.Context, current *types.ExecutionRequest) (*types.ExecutorResponse, error) {
		modes = append(modes, current.AllowLaunch)
		return nil, fmt.Errorf("provider outcome unknown")
	}}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.ResyncActivity)
	_, err := env.ExecuteActivity(a.ResyncActivity, req)
	require.Error(t, err)
	_, err = env.ExecuteActivity(a.ResyncActivity, req)
	require.Error(t, err)
	require.Equal(t, []bool{true, false}, modes)
}

func TestDelayedSyncCannotReadInputsOwnedByAnotherRun(t *testing.T) {
	req, db := activityEnrollmentFixture(t)
	db.owner, db.run = "resync-owner", "other-run"
	a := &Activity{db: db, executor: &atomicActivityExecutor{}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.SyncActivity)
	_, err := env.ExecuteActivity(a.SyncActivity, req)
	require.Error(t, err)
	require.Zero(t, db.reads)
	require.Equal(t, "resync-owner", db.owner)
}

func TestResyncWorkflowRetainsAuthorityUntilDurableHandoff(t *testing.T) {
	terminal := temporal.NewNonRetryableApplicationError("failed", "terminal", nil)
	for _, tc := range []struct {
		name                   string
		executeErr, cleanupErr error
		cancel, loseAck        bool
	}{
		{name: "published"}, {name: "lost patch acknowledgement", loseAck: true}, {name: "unpublished failure", executeErr: terminal}, {name: "unpublished cancellation", cancel: true}, {name: "unknown publication", executeErr: terminal, cleanupErr: terminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "stable-resync-operation"})
			a := &Activity{}
			env.RegisterActivity(a.ResyncActivity)
			env.RegisterActivity(a.PostResyncActivity)
			env.RegisterActivity(a.ReleaseJobExecutionActivity)
			env.RegisterActivity(a.TriggerSyncAfterResyncActivity)
			env.RegisterActivity(a.SendWebhookNotificationActivity)
			events := []string{}
			patches := 0
			call := env.OnActivity(ResyncActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, current *types.ExecutionRequest) (*types.ExecutorResponse, error) {
				events = append(events, "execute")
				require.Equal(t, "stable-resync-operation", current.WorkflowID)
				require.NotEmpty(t, current.RunID)
				require.Empty(t, current.RuntimeID)
				return &types.ExecutorResponse{}, tc.executeErr
			}).Once()
			if tc.cancel {
				call.After(time.Hour)
				env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
			}
			env.OnActivity(PostResyncActivity, mock.Anything, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *types.ExecutionRequest, status syncStatus) error {
				events = append(events, "cleanup")
				if tc.cancel {
					require.Equal(t, syncStatusCancelled, status)
				}
				return tc.cleanupErr
			}).Once()
			env.OnActivity(ReleaseJobExecutionActivity, mock.Anything, mock.Anything).Return(func(context.Context, *types.ExecutionRequest) error { events = append(events, "release"); return nil }).Maybe()
			env.OnActivity(TriggerSyncAfterResyncActivity, mock.Anything, mock.Anything).Return(func(context.Context, *types.ExecutionRequest) error {
				events = append(events, "handoff")
				patches++
				if tc.loseAck && patches == 1 {
					return errors.New("patch accepted but response lost")
				}
				return nil
			}).Maybe()
			env.OnActivity(SendWebhookNotificationActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
			req := &types.ExecutionRequest{Command: types.Resync, JobID: 42, ProjectID: "project", WorkflowID: "untrusted", RunID: "untrusted", RuntimeID: "untrusted", Args: atomicArgs("stable-resync-operation")}
			env.ExecuteWorkflow(RunSyncWorkflow, req)
			success := tc.executeErr == nil && !tc.cancel && tc.cleanupErr == nil
			if success {
				require.NoError(t, env.GetWorkflowError())
				require.NotContains(t, events, "release")
				require.Contains(t, events, "handoff")
				if tc.loseAck {
					require.Equal(t, 2, patches)
				}
			} else {
				require.Error(t, env.GetWorkflowError())
				require.NotContains(t, events, "handoff")
				if tc.cleanupErr == nil {
					require.Contains(t, events, "release")
				} else {
					require.NotContains(t, events, "release")
				}
			}
			env.AssertExpectations(t)
		})
	}
}

var _ activityDatabase = (*database.DB)(nil)
