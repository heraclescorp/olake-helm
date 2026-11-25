package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type enrollmentStateStore struct {
	executionTestAuthority
	state string
}

func (db *enrollmentStateStore) UpdateJobState(_ context.Context, _ int, _, _ string, state string) error {
	db.state = state
	return nil
}

type enrollmentInnerExecutor struct {
	execute    func(context.Context, *types.ExecutionRequest, string) (string, error)
	cleanupErr error
}

func (e *enrollmentInnerExecutor) Execute(ctx context.Context, req *types.ExecutionRequest, dir string) (string, error) {
	return e.execute(ctx, req, dir)
}
func (e *enrollmentInnerExecutor) Cleanup(context.Context, *types.ExecutionRequest) error {
	return e.cleanupErr
}
func (e *enrollmentInnerExecutor) Close() error { return nil }
func (e *enrollmentInnerExecutor) ResolveResource(context.Context, *types.ExecutionRequest) error {
	return nil
}

func executorEnrollmentFixture(t *testing.T) (*types.ExecutionRequest, string) {
	t.Helper()
	mode, kube := viper.Get(constants.EnvStorageMode), viper.Get(constants.EnvKubernetesServiceHost)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	viper.Set(constants.EnvKubernetesServiceHost, "")
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, mode); viper.Set(constants.EnvKubernetesServiceHost, kube) })
	id := fmt.Sprintf("executor-test-%s-%d", t.Name(), time.Now().UnixNano())
	req := &types.ExecutionRequest{Controlled: true, Command: types.Sync, WorkflowID: id, RunID: "test-run", RuntimeID: id, LaunchID: id, JobID: 42, AllowLaunch: true, BeforeLaunch: func(context.Context) error { return nil }}
	_, dir := utils.GetExecutionDirAndSubDir(req)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	return req, dir
}

func TestPreflightOnlyCleanupPreservesExactSavedStateWithoutBackendOrProof(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	ctx := context.Background()
	saved := ` {"global":{"lsn":"exact-preserved-lsn"},"streams":{"other":{"cursor":"keep"}}} `
	store := &enrollmentStateStore{state: saved, executionTestAuthority: executionTestAuthority{prelaunch: true}}
	inner := &enrollmentInnerExecutor{cleanupErr: errors.New("no backend was admitted")}
	a := &AbstractExecutor{executor: inner, db: store}
	_, err := storage.ReadFile(ctx, dir, "state.json", false)
	require.Error(t, err)
	require.NoError(t, a.CleanupAndPersistState(ctx, req))
	require.Equal(t, saved, store.state)
	require.Equal(t, "not_launched", store.record.Outcome)
}

func TestAttachRetryNeverRewritesConfigOrCheckpointEvenWithoutConnectorLogs(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	ctx := context.Background()
	saved := `{"global":{"lsn":"saved"}}`
	running := `{"global":{"lsn":"running"}}`
	req.Configs = []types.JobConfig{{Name: "state.json", Data: saved}, {Name: constants.StreamsFile, Data: `{"streams":[]}`}}
	calls := 0
	inner := &enrollmentInnerExecutor{execute: func(ctx context.Context, current *types.ExecutionRequest, workdir string) (string, error) {
		calls++
		require.Equal(t, dir, workdir)
		state, err := storage.ReadFile(ctx, workdir, "state.json", false)
		require.NoError(t, err)
		if calls == 1 {
			require.True(t, current.AllowLaunch)
			require.Equal(t, saved, state)
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", running))
			writeOrdinaryExecutionProof(t, ctx, current, "settled", running)
		} else {
			require.False(t, current.AllowLaunch)
			require.Equal(t, running, state)
		}
		return `{}`, nil
	}}
	store := &enrollmentStateStore{state: saved}
	a := &AbstractExecutor{executor: inner, db: store}
	_, err := a.Execute(ctx, req)
	require.NoError(t, err)
	req.AllowLaunch = false
	req.Configs = []types.JobConfig{{Name: "state.json", Data: `{"stale":true}`}}
	_, err = a.Execute(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.NoError(t, a.CleanupAndPersistState(ctx, req))
	require.Equal(t, running, store.state)
}
