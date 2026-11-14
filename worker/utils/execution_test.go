package utils

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func executionFenceFixture(t *testing.T) *types.ExecutionRequest {
	t.Helper()
	mode, kube := viper.Get(constants.EnvStorageMode), viper.Get(constants.EnvKubernetesServiceHost)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	viper.Set(constants.EnvKubernetesServiceHost, "")
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, mode); viper.Set(constants.EnvKubernetesServiceHost, kube) })
	id := t.Name() + time.Now().Format("150405.000000000")
	req := &types.ExecutionRequest{Controlled: true, Command: types.Resync, WorkflowID: "operation-" + id, RunID: "controlling-run", LaunchID: "launch-" + id, RuntimeID: "runtime-" + id}
	_, dir := GetExecutionDirAndSubDir(req)
	_, stable := GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)); require.NoError(t, os.RemoveAll(stable)) })
	return req
}
func TestPositiveExecutionFenceNeverInfersPermissionFromAbsence(t *testing.T) {
	for _, state := range []string{"missing", "malformed", "wrong version", "wrong runtime", "closed", "open"} {
		t.Run(state, func(t *testing.T) {
			req := executionFenceFixture(t)
			ctx := context.Background()
			_, dir := GetExecutionDirAndSubDir(req)
			fence := ExecutionFence{Version: 1, ExecutionID: req.RuntimeID, State: "open"}
			if state == "wrong version" {
				fence.Version = 2
			}
			if state == "wrong runtime" {
				fence.ExecutionID = "another-runtime"
			}
			if state == "closed" {
				fence.State = "closed"
			}
			if state != "missing" {
				data, _ := json.Marshal(fence)
				if state == "malformed" {
					data = []byte(`{"version":`)
				}
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, ExecutionFenceFile, string(data)))
			}
			err := ReadExecutionFence(ctx, req)
			if state == "open" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
func TestRuntimeFenceClosureCannotBeReopenedByAnyOldAttempt(t *testing.T) {
	req := executionFenceFixture(t)
	ctx := context.Background()
	require.NoError(t, CloseExecutionFence(ctx, req))
	require.Error(t, OpenExecutionFence(ctx, req))
	require.Error(t, ReadExecutionFence(ctx, req))
	require.NoError(t, CloseExecutionFence(ctx, req))
	require.Error(t, OpenExecutionFence(ctx, req))
	successor := *req
	successor.RuntimeID += "-recovery"
	_, dir := GetExecutionDirAndSubDir(&successor)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	require.NoError(t, OpenExecutionFence(ctx, &successor))
	require.NoError(t, ReadExecutionFence(ctx, &successor))
	require.Error(t, ReadExecutionFence(ctx, req))
}
func TestWorkerCanonicalizesFencedRecoveryFlagsAndRetainsStableOperationID(t *testing.T) {
	req := executionFenceFixture(t)
	req.RecoverOnly = true
	req.Args = []string{"resync", "--resync-id", req.WorkflowID, "--execution-id=untrusted", "--execution-fence", "untrusted", "--resync-launch-id=untrusted", "--resync-recover-only=false"}
	SetExecutionArgs(req)
	SetExecutionArgs(req)
	joined := strings.Join(req.Args, " ")
	require.NotContains(t, joined, "untrusted")
	require.Contains(t, joined, "--resync-id "+req.WorkflowID)
	require.Contains(t, joined, "--execution-id "+req.RuntimeID)
	require.Contains(t, joined, "--resync-launch-id "+req.LaunchID)
	require.Equal(t, 1, strings.Count(joined, "--resync-recover-only"))
	require.Equal(t, 1, strings.Count(joined, "--execution-fence"))
	req.RecoverOnly = false
	SetExecutionArgs(req)
	require.NotContains(t, strings.Join(req.Args, " "), "--resync-recover-only")
}
func TestRuntimeArtifactsAndLogPointersSeparateEpochs(t *testing.T) {
	req := executionFenceFixture(t)
	ctx := context.Background()
	require.NoError(t, WriteExecutionPointer(ctx, req))
	_, stable := GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	_, first := GetExecutionDirAndSubDir(req)
	successor := *req
	successor.RunID = "new-run"
	successor.RuntimeID += "-recovery"
	_, second := GetExecutionDirAndSubDir(&successor)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(second)) })
	require.NotEqual(t, first, second)
	require.NotEqual(t, stable, first)
	require.NoError(t, WriteExecutionPointer(ctx, &successor))
	old, err := storage.ReadFile(ctx, stable, "execution-runtime-"+req.RuntimeID+".json", false)
	require.NoError(t, err)
	require.Contains(t, old, req.RuntimeID)
	current, err := storage.ReadFile(ctx, stable, ExecutionPointerFile, false)
	require.NoError(t, err)
	require.Contains(t, current, successor.RuntimeID)
	require.Equal(t, filepath.Dir(first), filepath.Dir(stable), "runtime artifact roots are siblings of stable worker logs")
}
func TestLogRetentionNeverDeletesRuntimeFenceOrProof(t *testing.T) {
	root := t.TempDir()
	runtime := filepath.Join(root, "runtime")
	require.NoError(t, os.MkdirAll(runtime, 0755))
	closed := `{"version":1,"execution_id":"runtime","state":"closed"}`
	require.NoError(t, storage.WriteFileAtomic(context.Background(), runtime, ExecutionFenceFile, closed))
	require.NoError(t, storage.WriteFileAtomic(context.Background(), runtime, "resync-result.json", `{"proof":"retained"}`))
	require.NoError(t, storage.WriteFileAtomic(context.Background(), runtime, "olake.log", "old log"))
	old := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(runtime, "olake.log"), old, old))
	cleanNFSOldLogs(root, 1)
	data, err := os.ReadFile(filepath.Join(runtime, ExecutionFenceFile))
	require.NoError(t, err)
	require.Equal(t, closed, string(data))
	_, err = os.Stat(filepath.Join(runtime, "resync-result.json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(runtime, "olake.log"))
	require.True(t, os.IsNotExist(err))
}
func TestCheckpointDigestRejectsTrailingJSONAndPreservesLargeNumbers(t *testing.T) {
	a, err := StateSHA256(` {"lsn":9007199254740993,"streams":{}} `)
	require.NoError(t, err)
	b, err := StateSHA256(`{"streams":{},"lsn":9007199254740993}`)
	require.NoError(t, err)
	require.Equal(t, a, b)
	for _, state := range []string{"null", "[]", `{} {}`, `{"partial":`} {
		_, err := StateSHA256(state)
		require.Error(t, err)
	}
}
