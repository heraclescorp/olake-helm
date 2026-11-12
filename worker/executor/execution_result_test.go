package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

func writeOrdinaryExecutionProof(t *testing.T, ctx context.Context, req *types.ExecutionRequest, outcome, state string) {
	t.Helper()
	proof := executionResult{Version: 1, ExecutionID: req.RuntimeID, LaunchID: req.LaunchID, Command: string(req.Command), Outcome: outcome}
	if outcome == "settled" {
		var err error
		proof.StateSHA256, err = utils.StateSHA256(state)
		require.NoError(t, err)
	}
	data, err := json.Marshal(proof)
	require.NoError(t, err)
	_, dir := utils.GetExecutionDirAndSubDir(req)
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, executionResultFile, string(data)))
}

func TestOrdinaryFailureRetiresOnlyWithLaunchBoundSettledOrNotStartedEvidence(t *testing.T) {
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		for _, mode := range []string{"bad password not started", "TOAST after acknowledged writes", "unknown mutation", "missing proof", "wrong runtime", "wrong launch", "wrong command", "wrong version", "checkpoint mismatch", "missing checkpoint", "cleanup failure"} {
			t.Run(string(command)+"/"+mode, func(t *testing.T) {
				req, dir := executorEnrollmentFixture(t)
				req.Command = command
				ctx := context.Background()
				saved := `{"global":{"lsn":"saved"},"streams":{"other":{"cursor":"keep"}}}`
				final := ` {"global":{"lsn":9007199254740993},"streams":{"other":{"cursor":"keep"},"target":{"cursor":"partial-but-acknowledged"}}} `
				store := &resyncStateStore{state: saved}
				inner := &enrollmentInnerExecutor{}
				a := &AbstractExecutor{executor: inner, db: store}
				outcome := "settled"
				if mode == "bad password not started" {
					outcome = "not_started"
				}
				if mode == "unknown mutation" {
					outcome = "unknown"
				}
				proof := executionResult{Version: 1, ExecutionID: req.RuntimeID, LaunchID: req.LaunchID, Command: string(command), Outcome: outcome}
				var err error
				proof.StateSHA256, err = utils.StateSHA256(final)
				require.NoError(t, err)
				switch mode {
				case "wrong runtime":
					proof.ExecutionID = "old-runtime"
				case "wrong launch":
					proof.LaunchID = "other-launch"
				case "wrong command":
					proof.Command = "resync"
				case "wrong version":
					proof.Version = 2
				case "checkpoint mismatch":
					proof.StateSHA256 = "wrong"
				case "cleanup failure":
					inner.cleanupErr = errors.New("provider termination unconfirmed")
				}
				if mode != "missing checkpoint" {
					state := final
					if outcome == "not_started" {
						state = "invalid candidate must never be read"
					}
					require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", state))
				}
				if mode != "missing proof" {
					data, err := json.Marshal(proof)
					require.NoError(t, err)
					require.NoError(t, storage.WriteFileAtomic(ctx, dir, executionResultFile, string(data)))
				}
				err = a.CleanupAndPersistState(ctx, req)
				switch mode {
				case "bad password not started":
					require.NoError(t, err)
					require.Equal(t, saved, store.state)
					require.Equal(t, "not_started", store.record.Outcome)
					require.Equal(t, "closed", store.record.Phase)
				case "TOAST after acknowledged writes":
					require.NoError(t, err)
					require.Equal(t, final, store.state)
					require.Equal(t, "settled", store.record.Outcome)
					require.Equal(t, "closed", store.record.Phase)
				default:
					require.Error(t, err)
					require.Equal(t, saved, store.state)
					require.Equal(t, "closing", store.record.Phase, "unknown evidence may not authorize release")
				}
			})
		}
	}
}

func TestSuccessfulBackendExitCannotOverrideStickyUnknownReceipt(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	ctx := context.Background()
	saved := `{"global":{"lsn":"saved"}}`
	candidate := `{"global":{"lsn":"candidate-after-503"}}`
	store := &resyncStateStore{state: saved}
	inner := &enrollmentInnerExecutor{execute: func(ctx context.Context, current *types.ExecutionRequest, _ string) (string, error) {
		require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", candidate))
		writeOrdinaryExecutionProof(t, ctx, current, "unknown", "")
		return `{}`, nil
	}}
	a := &AbstractExecutor{executor: inner, db: store}
	_, err := a.Execute(ctx, req)
	require.NoError(t, err, "a real backend can exit zero after a previously ambiguous dispatch")
	require.Error(t, a.CleanupAndPersistState(ctx, req))
	require.Equal(t, saved, store.state)
	require.Equal(t, "closing", store.record.Phase)
	require.NotEqual(t, "settled", store.record.Outcome)
}

func TestLegacyOrdinaryDomainKeepsNormalLifecycleWithoutFabricatedProof(t *testing.T) {
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		t.Run(string(command), func(t *testing.T) {
			req, dir := executorEnrollmentFixture(t)
			req.Command = command
			req.Controlled = false
			req.RuntimeID, req.LaunchID = "", ""
			req.BeforeLaunch = nil
			req.Args = []string{string(command), "--execution-id", "caller-cannot-force-domain", "--execution-fence", "caller-path"}
			req.Configs = []types.JobConfig{{Name: "state.json", Data: `{"global":{"lsn":"saved"}}`}}
			ctx := context.Background()
			final := `{"global":{"lsn":"legacy-next"}}`
			store := &resyncStateStore{state: `{"global":{"lsn":"saved"}}`}
			inner := &enrollmentInnerExecutor{execute: func(ctx context.Context, current *types.ExecutionRequest, _ string) (string, error) {
				require.NotContains(t, current.Args, "--execution-id")
				require.NotContains(t, current.Args, "--execution-fence")
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", final))
				return `{}`, nil
			}}
			a := &AbstractExecutor{executor: inner, db: store}
			_, err := a.Execute(ctx, req)
			require.NoError(t, err)
			require.NoError(t, a.CleanupAndPersistState(ctx, req))
			require.Equal(t, final, store.state)
			require.Equal(t, "legacy_completed", store.record.Outcome)
			_, err = storage.ReadFile(ctx, dir, executionResultFile, false)
			require.Error(t, err, "legacy policy must not fabricate settled evidence")
			_, err = storage.ReadFile(ctx, dir, utils.ExecutionFenceFile, false)
			require.Error(t, err)
		})
	}
}
