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
	state string
}

func (db *enrollmentStateStore) UpdateJobState(_ context.Context, _ int, _ string, state string) error {
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

func executorEnrollmentFixture(t *testing.T) (*types.ExecutionRequest, string) {
	t.Helper()
	mode, kube := viper.Get(constants.EnvStorageMode), viper.Get(constants.EnvKubernetesServiceHost)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	viper.Set(constants.EnvKubernetesServiceHost, "")
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, mode); viper.Set(constants.EnvKubernetesServiceHost, kube) })
	req := &types.ExecutionRequest{Command: types.Sync, WorkflowID: fmt.Sprintf("gate-test-%s-%d", t.Name(), time.Now().UnixNano()), JobID: 42}
	_, dir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	return req, dir
}

func TestPreflightOnlyCleanupPreservesExactSavedState(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	ctx := context.Background()
	state := ` {"global":{"lsn":"exact-preserved-lsn"},"streams":{"public.old":{"cursor":"keep"}}} `
	store := &enrollmentStateStore{state: state}
	inner := &enrollmentInnerExecutor{cleanupErr: errors.New("ordinary Docker container does not exist")}
	exec := &AbstractExecutor{executor: inner, db: store}
	require.NoError(t, utils.BeginEnrollmentPreflight(ctx, req))
	_, err := storage.ReadFile(ctx, dir, "state.json", false)
	require.Error(t, err, "preflight must not create or rewrite the ingestion state")
	require.NoError(t, exec.CleanupAndPersistState(ctx, req))
	require.Equal(t, state, store.state)
	pending, err := utils.EnrollmentPreflightPending(ctx, req)
	require.NoError(t, err)
	require.True(t, pending)
}

func TestStartedAndOrdinarySyncStillFailWhenStateIsMissing(t *testing.T) {
	for _, withGate := range []bool{false, true} {
		t.Run(fmt.Sprint(withGate), func(t *testing.T) {
			req, _ := executorEnrollmentFixture(t)
			ctx := context.Background()
			if withGate {
				require.NoError(t, utils.BeginEnrollmentPreflight(ctx, req))
				require.NoError(t, utils.MarkEnrollmentIngestionStarted(ctx, req))
			}
			store := &enrollmentStateStore{state: `{"global":{"lsn":"saved"}}`}
			exec := &AbstractExecutor{executor: &enrollmentInnerExecutor{}, db: store}
			require.Error(t, exec.CleanupAndPersistState(ctx, req))
		})
	}
}

func TestSyncTransitionsGateAfterConfigWriteEvenWhenPolicyWasDisabledOnRetry(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	ctx := context.Background()
	state := `{"global":{"lsn":"saved"},"streams":{"public.old":{"cursor":"keep"}}}`
	store := &enrollmentStateStore{state: `{"global":{"lsn":"previously-persisted"}}`}
	require.NoError(t, utils.BeginEnrollmentPreflight(ctx, req))
	// This request need not carry an enrollment policy: disabling it after a
	// previous failed preflight must not make real ingestion look preflight-only.
	req.Configs = []types.JobConfig{{Name: "state.json", Data: state}, {Name: constants.StreamsFile, Data: `{"streams":[{"stream":{"name":"new","namespace":"public"}}]}`}}
	inner := &enrollmentInnerExecutor{execute: func(ctx context.Context, request *types.ExecutionRequest, workDir string) (string, error) {
		require.Equal(t, dir, workDir)
		pending, err := utils.EnrollmentPreflightPending(ctx, request)
		require.NoError(t, err)
		require.False(t, pending, "gate must be durable before the connector launches")
		written, err := storage.ReadFile(ctx, workDir, "state.json", false)
		require.NoError(t, err)
		require.Equal(t, state, written, "ordinary configs must exist before the gate flips")
		return `{}`, nil
	}}
	exec := &AbstractExecutor{executor: inner, db: store}
	_, err := exec.Execute(ctx, req)
	require.NoError(t, err)
	// A retry after an ordinary launch must never rewrite this gate false.
	require.NoError(t, utils.BeginEnrollmentPreflight(ctx, req))
	pending, err := utils.EnrollmentPreflightPending(ctx, req)
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, exec.CleanupAndPersistState(ctx, req))
	require.Equal(t, state, store.state)
}
