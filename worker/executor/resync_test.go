package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
)

type resyncStateStore struct {
	executionTestAuthority
	state string
	err   error
}

func (db *resyncStateStore) UpdateJobState(_ context.Context, _ int, _, _ string, state string) error {
	if db.err != nil {
		return db.err
	}
	db.state = state
	return nil
}

func TestScopedClearPersistsCompleteStateOnlyAfterCleanup(t *testing.T) {
	for _, mode := range []string{"success", "cleanup failure", "persistence failure", "missing state"} {
		t.Run(mode, func(t *testing.T) {
			req, dir := executorEnrollmentFixture(t)
			req.Command = types.ClearDestination
			ctx := context.Background()
			saved := `{"global":{"lsn":"keep-global-lsn"},"streams":{"public.target":{"cursor":"old"},"public.other":{"cursor":"keep-other"}}}`
			cleared := `{"global":{"lsn":"keep-global-lsn"},"streams":{"public.target":{"cursor":"cleared"},"public.other":{"cursor":"keep-other"}}}`
			store := &resyncStateStore{state: saved}
			req.Configs = []types.JobConfig{{Name: "state.json", Data: saved}}
			inner := &enrollmentInnerExecutor{execute: func(ctx context.Context, _ *types.ExecutionRequest, workDir string) (string, error) {
				initial, err := storage.ReadFile(ctx, workDir, "state.json", false)
				require.NoError(t, err)
				require.Equal(t, saved, initial)
				require.NoError(t, storage.WriteFiles(ctx, workDir, []types.JobConfig{{Name: "state.json", Data: cleared}}))
				writeOrdinaryExecutionProof(t, ctx, req, "settled", cleared)
				return `{}`, nil
			}}
			exec := &AbstractExecutor{executor: inner, db: store}
			_, err := exec.Execute(ctx, req)
			require.NoError(t, err)
			require.Equal(t, saved, store.state)
			switch mode {
			case "cleanup failure":
				inner.cleanupErr = errors.New("connector cleanup failed")
			case "persistence failure":
				store.err = errors.New("database unavailable")
			case "missing state":
				require.NoError(t, os.Remove(filepath.Join(dir, "state.json")))
			}
			err = exec.CleanupAndPersistState(ctx, req)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, cleared, store.state)
				return
			}
			require.Error(t, err)
			require.Equal(t, saved, store.state)
			if mode != "missing state" {
				inner.cleanupErr, store.err = nil, nil
				require.NoError(t, exec.CleanupAndPersistState(ctx, req))
				require.Equal(t, cleared, store.state)
			}
		})
	}
}
