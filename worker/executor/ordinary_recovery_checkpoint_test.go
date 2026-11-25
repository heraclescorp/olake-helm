package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

func recoveryCheckpointFixture(t *testing.T, result utils.OrdinaryRecoveryResult, target, final string, previous string) utils.OrdinaryRecoveryCheckpoint {
	t.Helper()
	result.Tables = append([]utils.OrdinaryRecoveryTableProof(nil), result.Tables...)
	result.TargetLSN = target
	state := strings.Replace(ordinaryFinal, "0/200", target, 1)
	sha, err := utils.StateSHA256(state)
	require.NoError(t, err)
	result.StateSHA256 = sha
	for i := range result.Tables {
		result.Tables[i].CommittedLSN = target
	}
	checkpoint := utils.OrdinaryRecoveryCheckpoint{OrdinaryRecoveryResult: result, FinalTargetLSN: final, State: json.RawMessage(state)}
	if previous != "" {
		var prior utils.OrdinaryRecoveryCheckpoint
		require.NoError(t, json.Unmarshal([]byte(previous), &prior))
		checkpoint.PreviousCheckpointSHA256 = utils.RawSHA256(previous)
		checkpoint.PreviousExecutionID = prior.ExecutionID
	}
	return checkpoint
}

func writeRecoveryCheckpoint(t *testing.T, dir string, checkpoint utils.OrdinaryRecoveryCheckpoint) string {
	t.Helper()
	data, err := json.Marshal(checkpoint)
	require.NoError(t, err)
	name := recoveryCheckpointArtifactName(t, checkpoint)
	require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, name, string(data)))
	return string(data)
}

func successorRecoveryRuntime(t *testing.T, a *AbstractExecutor, store *ordinaryRecoveryStore, req *types.ExecutionRequest) string {
	t.Helper()
	req.RunID += "-next"
	req.RuntimeID += "-next"
	req.ResourceID += "-next"
	req.ResourceUID += "-next"
	store.record = &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, ResourceID: req.ResourceID, ResourceUID: req.ResourceUID, Outcome: "recovery", Phase: "admitted"}
	require.NoError(t, a.ordinaryRecoveryInputs(context.Background(), req))
	_, dir := utils.GetExecutionDirAndSubDir(req)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	require.NoError(t, storage.WriteFiles(context.Background(), dir, req.Configs))
	require.NoError(t, utils.OpenExecutionFence(context.Background(), req))
	return dir
}

func recoveryConfig(t *testing.T, req *types.ExecutionRequest, name string) string {
	t.Helper()
	for _, config := range req.Configs {
		if config.Name == name {
			return config.Data
		}
	}
	t.Fatalf("missing recovery config %s", name)
	return ""
}

func TestOrdinaryRecoveryCheckpointsSurviveEveryRetiredRuntime(t *testing.T) {
	for _, missingOriginalReceipt := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown original receipt", true: "missing original receipt"}[missingOriginalReceipt], func(t *testing.T) {
			ctx := context.Background()
			a, store, req, originalDir := ordinaryRecoveryFixture(t)
			if missingOriginalReceipt {
				require.NoError(t, os.Remove(path.Join(originalDir, executionResultFile)))
			}
			a, store, req, dir := prepareRecoveryRuntimeFromOriginal(t, a, store, req)
			result := recoveryResultFixture(t, req, dir)
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			origin, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
			require.NoError(t, err)
			first := recoveryCheckpointFixture(t, result, "0/150", "0/300", "")
			first.Tables[0].CommittedLSN = "0/300"
			firstRaw := writeRecoveryCheckpoint(t, dir, first)
			second := recoveryCheckpointFixture(t, result, "0/200", "0/300", firstRaw)
			second.Tables[0].CommittedLSN = "0/300"
			secondRaw := writeRecoveryCheckpoint(t, dir, second)
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", strings.Replace(ordinaryFinal, "0/200", "0/FFFF", 1)))
			require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
			require.Equal(t, ordinaryBaseline, store.job.State)
			require.Zero(t, store.completions)
			for _, checkpoint := range []utils.OrdinaryRecoveryCheckpoint{first, second} {
				name := recoveryCheckpointArtifactName(t, checkpoint)
				persisted, err := storage.ReadFile(ctx, stable, name, false)
				require.NoError(t, err)
				runtime, err := storage.ReadFile(ctx, dir, name, false)
				require.NoError(t, err)
				require.Equal(t, runtime, persisted)
			}
			dir = successorRecoveryRuntime(t, a, store, req)
			require.Equal(t, secondRaw, recoveryConfig(t, req, utils.OrdinaryRecoveryCheckpointFile))
			require.Equal(t, ordinaryBaseline, recoveryConfig(t, req, "state.json"))
			result = recoveryResultFixture(t, req, dir)
			third := recoveryCheckpointFixture(t, result, "0/250", "0/300", secondRaw)
			third.Tables[0].CommittedLSN = "0/300"
			thirdRaw := writeRecoveryCheckpoint(t, dir, third)
			require.Error(t, a.CleanupAndPersistState(ctx, req), "intermediate checkpoints cannot replace a final receipt")
			require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
			dir = successorRecoveryRuntime(t, a, store, req)
			require.Equal(t, thirdRaw, recoveryConfig(t, req, utils.OrdinaryRecoveryCheckpointFile))
			require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req), "no new completed batch preserves inherited input")
			dir = successorRecoveryRuntime(t, a, store, req)
			require.Equal(t, thirdRaw, recoveryConfig(t, req, utils.OrdinaryRecoveryCheckpointFile))
			finalResult := recoveryResultFixture(t, req, dir)
			finalCheckpoint := recoveryCheckpointFixture(t, finalResult, "0/300", "0/300", thirdRaw)
			writeRecoveryCheckpoint(t, dir, finalCheckpoint)
			finalResult = finalCheckpoint.OrdinaryRecoveryResult
			data, err := json.Marshal(finalResult)
			require.NoError(t, err)
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", string(finalCheckpoint.State)))
			writeOrdinaryExecutionProof(t, ctx, req, "settled", string(finalCheckpoint.State))
			require.NoError(t, a.CleanupAndPersistState(ctx, req))
			require.JSONEq(t, string(finalCheckpoint.State), store.job.State)
			unchanged, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
			require.NoError(t, err)
			require.Equal(t, origin, unchanged)
			unchanged, err = storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryBaselineFile, false)
			require.NoError(t, err)
			require.Equal(t, ordinaryBaseline, unchanged)
		})
	}
}

func TestOrdinaryRecoveryRejectsUnprovenCheckpointArtifacts(t *testing.T) {
	for _, mode := range []string{"corrupt JSON", "foreign origin", "foreign execution", "foreign launch", "foreign source", "foreign slot", "foreign server", "wrong digest", "wrong global LSN", "changed lower", "unbounded batch", "partial tables", "duplicate table", "foreign table", "missing UUID", "duplicate UUID", "missing fence", "table behind", "table past final", "foreign membership", "duplicate membership", "wrong filename", "missing predecessor", "incomplete predecessor", "retirement failed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			a, store, req, dir := prepareRecoveryRuntime(t)
			checkpoint := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/150", "0/300", "")
			switch mode {
			case "foreign origin":
				checkpoint.OriginalExecutionID = "foreign"
			case "foreign execution":
				checkpoint.ExecutionID = "foreign"
			case "foreign launch":
				checkpoint.LaunchID = "foreign"
			case "foreign source":
				checkpoint.SourceSHA256 = strings.Repeat("a", 64)
			case "foreign slot":
				checkpoint.SlotName = "foreign"
			case "foreign server":
				checkpoint.ServerIdentity = "foreign"
			case "wrong digest":
				checkpoint.StateSHA256 = strings.Repeat("a", 64)
			case "wrong global LSN":
				checkpoint.State = json.RawMessage(ordinaryFinal)
			case "changed lower":
				checkpoint.LowerLSN = "0/99"
			case "unbounded batch":
				checkpoint.FinalTargetLSN = "0/140"
			case "partial tables":
				checkpoint.Tables = checkpoint.Tables[:1]
			case "duplicate table":
				checkpoint.Tables[1] = checkpoint.Tables[0]
			case "foreign table":
				checkpoint.Tables[0].Stream = "public.foreign"
			case "missing UUID":
				checkpoint.Tables[0].TableUUID = ""
			case "duplicate UUID":
				checkpoint.Tables[1].TableUUID = checkpoint.Tables[0].TableUUID
			case "missing fence":
				checkpoint.Tables[0].FenceSnapshotID = 0
			case "table behind":
				checkpoint.Tables[1].CommittedLSN = "0/140"
			case "table past final":
				checkpoint.Tables[0].CommittedLSN = "0/400"
			case "foreign membership":
				checkpoint.State = json.RawMessage(strings.Replace(string(checkpoint.State), "public.two", "public.foreign", 1))
			case "duplicate membership":
				checkpoint.State = json.RawMessage(strings.Replace(string(checkpoint.State), "public.two", "public.one", 1))
			case "missing predecessor":
				checkpoint.PreviousCheckpointSHA256, checkpoint.PreviousExecutionID = strings.Repeat("a", 64), req.RuntimeID
			case "incomplete predecessor":
				checkpoint.PreviousExecutionID = req.RuntimeID
			case "retirement failed":
				a.executor.(*enrollmentInnerExecutor).cleanupErr = errors.New("exact runtime retirement unavailable")
			}
			if mode == "foreign membership" || mode == "duplicate membership" || mode == "wrong global LSN" {
				var err error
				checkpoint.StateSHA256, err = utils.StateSHA256(string(checkpoint.State))
				require.NoError(t, err)
			}
			writeRecoveryCheckpoint(t, dir, checkpoint)
			name := recoveryCheckpointArtifactName(t, checkpoint)
			if mode == "corrupt JSON" {
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, name, `{"version":`))
			}
			if mode == "wrong filename" {
				require.NoError(t, os.Rename(path.Join(dir, name), path.Join(dir, utils.OrdinaryRecoveryCheckpointsDir, "foreign.json")))
			}
			require.Error(t, a.PrepareOrdinaryRecovery(ctx, req))
			require.Equal(t, ordinaryBaseline, store.job.State)
			require.NotEqual(t, "settled", store.record.Outcome)
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			names, err := storage.ListFiles(ctx, stable, utils.OrdinaryRecoveryCheckpointsDir)
			require.NoError(t, err)
			require.Empty(t, names)
		})
	}
}

func TestOrdinaryRecoveryRejectsChangedInputAndCheckpointChains(t *testing.T) {
	for _, mode := range []string{"changed original bytes", "missing original input", "foreign input", "stale boundary", "regressed boundary", "widened target", "regressed table", "replaced table", "wrong predecessor execution", "unlinked root", "fork", "corrupt saved manifest", "immutable conflict", "runtime source changed", "runtime baseline changed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			a, store, req, dir := prepareRecoveryRuntime(t)
			first := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/200", "0/300", "")
			first.Tables[0].CommittedLSN = "0/300"
			firstRaw := writeRecoveryCheckpoint(t, dir, first)
			require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			if mode == "immutable conflict" {
				name := recoveryCheckpointArtifactName(t, first)
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, name, firstRaw+"\n"))
				require.Error(t, a.PrepareOrdinaryRecovery(ctx, req))
				stored, err := storage.ReadFile(ctx, stable, name, false)
				require.NoError(t, err)
				require.Equal(t, firstRaw, stored)
				return
			}
			dir = successorRecoveryRuntime(t, a, store, req)
			second := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/250", "0/300", firstRaw)
			second.Tables[0].CommittedLSN = "0/300"
			switch mode {
			case "changed original bytes":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryCheckpointFile, firstRaw+"\n"))
			case "missing original input":
				require.NoError(t, os.Remove(path.Join(dir, utils.OrdinaryRecoveryCheckpointFile)))
			case "foreign input":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryCheckpointFile, `{}`))
			case "stale boundary":
				second.TargetLSN = "0/200"
			case "regressed boundary":
				second.TargetLSN = "0/150"
			case "widened target":
				second.FinalTargetLSN = "0/400"
			case "regressed table":
				second.Tables[0].CommittedLSN = "0/250"
			case "replaced table":
				second.Tables[0].TableUUID = "replacement"
			case "wrong predecessor execution":
				second.PreviousExecutionID = "foreign"
			case "unlinked root":
				second.PreviousCheckpointSHA256, second.PreviousExecutionID = "", ""
			case "runtime source changed":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "source.json", `{}`))
			case "runtime baseline changed":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryBaselineFile, ordinaryFinal))
			case "corrupt saved manifest":
				name := recoveryCheckpointArtifactName(t, first)
				require.NoError(t, storage.WriteFileAtomic(ctx, stable, name, `{}`))
			}
			if mode == "stale boundary" || mode == "regressed boundary" {
				second.State = json.RawMessage(strings.Replace(ordinaryFinal, "0/200", second.TargetLSN, 1))
				var err error
				second.StateSHA256, err = utils.StateSHA256(string(second.State))
				require.NoError(t, err)
			}
			writeRecoveryCheckpoint(t, dir, second)
			if mode == "fork" {
				fork := recoveryCheckpointFixture(t, second.OrdinaryRecoveryResult, "0/260", "0/300", firstRaw)
				fork.Tables[0].CommittedLSN = "0/300"
				writeRecoveryCheckpoint(t, dir, fork)
			}
			require.Error(t, a.PrepareOrdinaryRecovery(ctx, req))
			require.Equal(t, ordinaryBaseline, store.job.State)
			require.Zero(t, store.completions)
			names, err := storage.ListFiles(ctx, stable, utils.OrdinaryRecoveryCheckpointsDir)
			require.NoError(t, err)
			require.Len(t, names, 1)
		})
	}
}

func TestOrdinaryRecoveryFinalCheckpointCanBeReconfirmedAfterPublishedBeforeACK(t *testing.T) {
	ctx := context.Background()
	a, store, req, dir := prepareRecoveryRuntime(t)
	first := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/200", "0/200", "")
	firstRaw := writeRecoveryCheckpoint(t, dir, first)
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	dir = successorRecoveryRuntime(t, a, store, req)
	second := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/200", "0/200", firstRaw)
	writeRecoveryCheckpoint(t, dir, second)
	data, err := json.Marshal(second.OrdinaryRecoveryResult)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", string(second.State)))
	writeOrdinaryExecutionProof(t, ctx, req, "settled", string(second.State))
	require.NoError(t, a.CleanupAndPersistState(ctx, req))
	require.JSONEq(t, ordinaryFinal, store.job.State)
}

func TestBackfillRecoveryCheckpointsPreserveCandidateAndTableBindings(t *testing.T) {
	for _, mode := range []string{"success", "missing journal", "unfinished table", "replaced table", "changed plan"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			a, store, req, dir, binding := backfillBindingFixture(t, true)
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			candidate, err := storage.ReadFile(ctx, stable, utils.BackfillRecoveryCheckpointFile, false)
			require.NoError(t, err)
			origin, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
			require.NoError(t, err)
			journal := ""
			if mode != "missing journal" {
				journal = writeBackfillBinding(t, dir, binding)
			}
			checkpoint := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/150", "0/300", "")
			checkpoint.State = json.RawMessage(strings.Replace(backfillFinal, "0/200", "0/150", 1))
			switch mode {
			case "unfinished table":
				checkpoint.State = json.RawMessage(strings.Replace(backfillPartial, "0/100", "0/150", 1))
			case "replaced table":
				checkpoint.State = json.RawMessage(strings.Replace(string(checkpoint.State), "table-two", "replacement", 1))
				checkpoint.Tables[1].TableUUID = "replacement"
			case "changed plan":
				checkpoint.State = json.RawMessage(strings.Replace(string(checkpoint.State), `[{"min":null,"max":10},{"min":10,"max":null}]`, `[{"min":null,"max":null}]`, 1))
			}
			checkpoint.StateSHA256, err = utils.StateSHA256(string(checkpoint.State))
			require.NoError(t, err)
			raw := writeRecoveryCheckpoint(t, dir, checkpoint)
			err = a.PrepareOrdinaryRecovery(ctx, req)
			if mode == "success" {
				require.NoError(t, err)
				successorRecoveryRuntime(t, a, store, req)
				require.Equal(t, raw, recoveryConfig(t, req, utils.OrdinaryRecoveryCheckpointFile))
				require.Equal(t, candidate, recoveryConfig(t, req, utils.BackfillRecoveryCheckpointFile))
				require.Equal(t, journal, recoveryConfig(t, req, utils.BackfillRecoveryBindingFilename(binding.Stream)))
			} else {
				require.Error(t, err)
				names, err := storage.ListFiles(ctx, stable, utils.OrdinaryRecoveryCheckpointsDir)
				require.NoError(t, err)
				require.Empty(t, names)
			}
			require.Equal(t, `{}`, store.job.State)
			unchanged, err := storage.ReadFile(ctx, stable, utils.BackfillRecoveryCheckpointFile, false)
			require.NoError(t, err)
			require.Equal(t, candidate, unchanged)
			unchanged, err = storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
			require.NoError(t, err)
			require.Equal(t, origin, unchanged)
		})
	}
}

func TestOrdinaryRecoveryCheckpointPreservesUnselectedBaselineState(t *testing.T) {
	for _, mode := range []string{"unchanged", "changed", "removed", "invented"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			a, store, req, _ := ordinaryRecoveryFixture(t)
			unselected := `{"stream":"unrelated","namespace":"public","sync_mode":"cdc","state":{"last_seen":9007199254740993}}`
			baseline := strings.Replace(ordinaryBaseline, `"public.two"]`, `"public.two","public.unrelated"]`, 1)
			baseline = strings.Replace(baseline, `"streams":[]`, `"streams":[`+unselected+`]`, 1)
			store.job.State = baseline
			a, store, req, dir := prepareRecoveryRuntimeFromOriginal(t, a, store, req)
			checkpoint := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/150", "0/300", "")
			state := strings.Replace(baseline, "0/100", "0/150", 1)
			switch mode {
			case "changed":
				state = strings.Replace(state, "9007199254740993", "9007199254740992", 1)
			case "removed":
				state = strings.Replace(state, unselected, "", 1)
			case "invented":
				state = strings.Replace(state, `"public.unrelated"]`, `"public.unrelated","public.invented"]`, 1)
			}
			checkpoint.State = json.RawMessage(state)
			var err error
			checkpoint.StateSHA256, err = utils.StateSHA256(state)
			require.NoError(t, err)
			writeRecoveryCheckpoint(t, dir, checkpoint)
			err = a.PrepareOrdinaryRecovery(ctx, req)
			if mode == "unchanged" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, baseline, store.job.State)
		})
	}
}

func TestOrdinaryRecoverySettlementCannotWidenCheckpointTarget(t *testing.T) {
	ctx := context.Background()
	a, store, req, dir := prepareRecoveryRuntime(t)
	checkpoint := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/150", "0/200", "")
	writeRecoveryCheckpoint(t, dir, checkpoint)
	result := recoveryCheckpointFixture(t, checkpoint.OrdinaryRecoveryResult, "0/300", "0/300", "")
	data, err := json.Marshal(result.OrdinaryRecoveryResult)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", string(result.State)))
	writeOrdinaryExecutionProof(t, ctx, req, "settled", string(result.State))
	require.Error(t, a.CleanupAndPersistState(ctx, req))
	require.Equal(t, ordinaryBaseline, store.job.State)
	require.NotEqual(t, "settled", store.record.Outcome)
}

func TestOrdinaryRecoveryDirectRetirementPreservesCheckpointInput(t *testing.T) {
	ctx := context.Background()
	a, store, req, dir := prepareRecoveryRuntime(t)
	checkpoint := recoveryCheckpointFixture(t, recoveryResultFixture(t, req, dir), "0/150", "0/300", "")
	raw := writeRecoveryCheckpoint(t, dir, checkpoint)
	require.NoError(t, a.RetireExecution(ctx, req))
	successorRecoveryRuntime(t, a, store, req)
	require.Equal(t, raw, recoveryConfig(t, req, utils.OrdinaryRecoveryCheckpointFile))
	require.Equal(t, ordinaryBaseline, store.job.State)
	require.Zero(t, store.completions)
}

func TestOrdinaryRecoveryCheckpointPromotionRetriesPartialPublication(t *testing.T) {
	ctx := context.Background()
	a, store, req, dir := prepareRecoveryRuntime(t)
	result := recoveryResultFixture(t, req, dir)
	first := recoveryCheckpointFixture(t, result, "0/150", "0/300", "")
	firstRaw := writeRecoveryCheckpoint(t, dir, first)
	second := recoveryCheckpointFixture(t, result, "0/200", "0/300", firstRaw)
	secondRaw := writeRecoveryCheckpoint(t, dir, second)
	_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	server, err := storage.ReadFile(ctx, dir, utils.OrdinaryRecoveryServerFile, false)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(ctx, stable, utils.OrdinaryRecoveryServerFile, server))
	name := recoveryCheckpointArtifactName(t, first)
	require.NoError(t, storage.WriteFileAtomic(ctx, stable, name, firstRaw))
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	successorRecoveryRuntime(t, a, store, req)
	require.Equal(t, secondRaw, recoveryConfig(t, req, utils.OrdinaryRecoveryCheckpointFile))
	require.Equal(t, ordinaryBaseline, store.job.State)
}

func recoveryCheckpointArtifactName(t *testing.T, checkpoint utils.OrdinaryRecoveryCheckpoint) string {
	t.Helper()
	name, err := utils.OrdinaryRecoveryCheckpointFilename(checkpoint.ExecutionID, checkpoint.TargetLSN)
	require.NoError(t, err)
	return path.Join(utils.OrdinaryRecoveryCheckpointsDir, name)
}
