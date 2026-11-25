package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

func backfillBindingFixture(t *testing.T, unstarted bool) (*AbstractExecutor, *ordinaryRecoveryStore, *types.ExecutionRequest, string, utils.BackfillRecoveryBinding) {
	t.Helper()
	a, store, req, originalDir, candidate := backfillRecoveryFixture(t)
	if unstarted {
		var state map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(candidate.State, &state))
		var streams []json.RawMessage
		require.NoError(t, json.Unmarshal(state["streams"], &streams))
		var err error
		state["streams"], err = json.Marshal(streams[:1])
		require.NoError(t, err)
		candidate.State, err = json.Marshal(state)
		require.NoError(t, err)
		candidate.StateSHA256, err = utils.StateSHA256(string(candidate.State))
		require.NoError(t, err)
		writeBackfillCandidate(t, originalDir, candidate)
	}
	a, store, req, dir := prepareRecoveryRuntimeFromOriginal(t, a, store, req)
	origin, _, _, err := utils.ReadOrdinaryRecoveryOrigin(context.Background(), req)
	require.NoError(t, err)
	binding := utils.BackfillRecoveryBinding{Version: 1, OriginalExecutionID: origin.OriginalExecutionID, OriginalLaunchID: origin.OriginalLaunchID, SourceSHA256: origin.SourceSHA256, DestinationSHA256: origin.DestinationSHA256, CatalogSHA256: origin.CatalogSHA256, ServerIdentity: backfillIdentity, SlotName: "slot", LowerLSN: "0/100", Stream: "public.two", TableUUID: "table-two", ShapeSHA256: backfillShape, Plan: []json.RawMessage{json.RawMessage(`{"min":null,"max":10}`), json.RawMessage(`{"min":10,"max":null}`)}}
	return a, store, req, dir, binding
}

func writeBackfillBinding(t *testing.T, dir string, binding utils.BackfillRecoveryBinding) string {
	t.Helper()
	data, err := json.Marshal(binding)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, utils.BackfillRecoveryBindingFilename(binding.Stream), string(data)))
	return string(data)
}

func TestBackfillBindingJournalSurvivesRetiredEpochAndCopiesExactBytes(t *testing.T) {
	a, store, req, dir, binding := backfillBindingFixture(t, true)
	ctx := context.Background()
	name := utils.BackfillRecoveryBindingFilename(binding.Stream)
	raw := writeBackfillBinding(t, dir, binding) + "\n"
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, name, raw))
	_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	origin, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
	require.NoError(t, err)
	baseline, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryBaselineFile, false)
	require.NoError(t, err)
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	require.NoError(t, utils.RequireClosedExecutionFence(ctx, req))
	stored, err := storage.ReadFile(ctx, stable, name, false)
	require.NoError(t, err)
	require.Equal(t, raw, stored)
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	req.RuntimeID += "-next"
	require.NoError(t, a.ordinaryRecoveryInputs(ctx, req))
	var inherited string
	for _, config := range req.Configs {
		if config.Name == name {
			inherited = config.Data
		}
	}
	require.Equal(t, raw, inherited)
	require.Equal(t, `{}`, store.job.State)
	unchanged, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
	require.NoError(t, err)
	require.Equal(t, origin, unchanged)
	unchanged, err = storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryBaselineFile, false)
	require.NoError(t, err)
	require.Equal(t, baseline, unchanged)
}

func TestBackfillBindingJournalOriginalPublicationSurvivesCandidateAdoption(t *testing.T) {
	a, store, req, dir, candidate := backfillRecoveryFixture(t)
	ctx := context.Background()
	binding := utils.BackfillRecoveryBinding{Version: 1, OriginalExecutionID: candidate.ExecutionID, OriginalLaunchID: candidate.LaunchID, SourceSHA256: candidate.SourceSHA256, DestinationSHA256: candidate.DestinationSHA256, CatalogSHA256: candidate.CatalogSHA256, ServerIdentity: candidate.ServerIdentity, SlotName: candidate.SlotName, LowerLSN: "0/100", Stream: "public.two", TableUUID: "table-two", ShapeSHA256: backfillShape, Plan: []json.RawMessage{json.RawMessage(`{"min":10,"max":null}`), json.RawMessage(`{"min":null,"max":10}`)}}
	raw := writeBackfillBinding(t, dir, binding)
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	name := utils.BackfillRecoveryBindingFilename(binding.Stream)
	stored, err := storage.ReadFile(ctx, stable, name, false)
	require.NoError(t, err)
	require.Equal(t, raw, stored)
	req.RuntimeID += "-recovery"
	require.NoError(t, a.ordinaryRecoveryInputs(ctx, req))
	var copied string
	for _, config := range req.Configs {
		if config.Name == name {
			copied = config.Data
		}
	}
	require.Equal(t, raw, copied)
	require.Equal(t, `{}`, store.job.State)
}

func TestBackfillBindingJournalRejectsInvalidAuthorityBeforePromotion(t *testing.T) {
	for _, mode := range []string{"malformed", "version", "original execution", "original launch", "source", "destination", "catalog", "server", "slot", "lower", "foreign stream", "UUID", "shape", "plan missing", "plan duplicated", "plan changed", "chunk bound missing"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir, binding := backfillBindingFixture(t, false)
			switch mode {
			case "version":
				binding.Version = 2
			case "original execution":
				binding.OriginalExecutionID = "foreign"
			case "original launch":
				binding.OriginalLaunchID = "foreign"
			case "source":
				binding.SourceSHA256 = "foreign"
			case "destination":
				binding.DestinationSHA256 = "foreign"
			case "catalog":
				binding.CatalogSHA256 = "foreign"
			case "server":
				binding.ServerIdentity = "foreign"
			case "slot":
				binding.SlotName = "foreign"
			case "lower":
				binding.LowerLSN = "0/101"
			case "foreign stream":
				binding.Stream = "public.foreign"
			case "UUID":
				binding.TableUUID = "replacement"
			case "shape":
				binding.ShapeSHA256 = strings.Repeat("b", 64)
			case "plan missing":
				binding.Plan = nil
			case "plan duplicated":
				binding.Plan = append(binding.Plan, binding.Plan[0])
			case "plan changed":
				binding.Plan = []json.RawMessage{json.RawMessage(`{"min":null,"max":null}`)}
			case "chunk bound missing":
				binding.Plan[0] = json.RawMessage(`{"max":10}`)
			}
			data, err := json.Marshal(binding)
			require.NoError(t, err)
			if mode == "malformed" {
				data = []byte("{")
			}
			name := utils.BackfillRecoveryBindingFilename("public.two")
			require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, name, string(data)))
			require.Error(t, a.PrepareOrdinaryRecovery(context.Background(), req))
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			_, err = storage.ReadFile(context.Background(), stable, name, false)
			require.ErrorIs(t, err, os.ErrNotExist)
			require.Equal(t, `{}`, store.job.State)
			require.Equal(t, "closing", store.record.Phase)
			require.Equal(t, "recovery", store.record.Outcome)
		})
	}
}

func TestBackfillBindingJournalConflictsAndRetirementFailuresRemainOwned(t *testing.T) {
	for _, mode := range []string{"different bytes", "replacement UUID", "retirement failed", "corrupt stable evidence"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir, binding := backfillBindingFixture(t, true)
			ctx := context.Background()
			name := utils.BackfillRecoveryBindingFilename(binding.Stream)
			raw := writeBackfillBinding(t, dir, binding)
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			if mode != "retirement failed" {
				require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
			}
			switch mode {
			case "different bytes":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, name, raw+"\n"))
			case "replacement UUID":
				binding.TableUUID = "replacement"
				writeBackfillBinding(t, dir, binding)
			case "retirement failed":
				a.executor.(*enrollmentInnerExecutor).cleanupErr = errors.New("exact runtime retirement unconfirmed")
			case "corrupt stable evidence":
				require.NoError(t, storage.WriteFileAtomic(ctx, stable, name, "{"))
				require.NoError(t, os.Remove(dir+"/"+name))
			}
			require.Error(t, a.PrepareOrdinaryRecovery(ctx, req))
			if mode == "retirement failed" {
				_, err := storage.ReadFile(ctx, stable, name, false)
				require.ErrorIs(t, err, os.ErrNotExist)
			} else if mode != "corrupt stable evidence" {
				stored, err := storage.ReadFile(ctx, stable, name, false)
				require.NoError(t, err)
				require.Equal(t, raw, stored)
			}
			if mode == "corrupt stable evidence" {
				req.RuntimeID += "-next"
				require.Error(t, a.ordinaryRecoveryInputs(ctx, req))
			}
			require.Equal(t, `{}`, store.job.State)
			require.Equal(t, "closing", store.record.Phase)
			require.Equal(t, "recovery", store.record.Outcome)
		})
	}
}

func TestBackfillBindingJournalFinalSettlementRequiresRecoveryCreatedTableEvidence(t *testing.T) {
	for _, mode := range []string{"success", "missing journal", "UUID mismatch", "shape mismatch", "plan mismatch", "lost inherited journal", "DB failure"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir, binding := backfillBindingFixture(t, true)
			ctx := context.Background()
			if mode != "missing journal" {
				writeBackfillBinding(t, dir, binding)
			}
			if mode == "lost inherited journal" {
				require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
				require.NoError(t, os.Remove(dir+"/"+utils.BackfillRecoveryBindingFilename(binding.Stream)))
			}
			final := backfillFinal
			result := recoveryResultFixture(t, req, dir)
			switch mode {
			case "UUID mismatch":
				final = strings.Replace(final, `"table-two"`, `"replacement"`, 1)
				result.Tables[1].TableUUID = "replacement"
			case "shape mismatch":
				final = strings.Replace(final, `"table_uuid":"table-two","shape_sha256":"`+backfillShape+`"`, `"table_uuid":"table-two","shape_sha256":"`+strings.Repeat("b", 64)+`"`, 1)
			case "plan mismatch":
				final = strings.Replace(final, `"backfill_plan":[{"min":null,"max":10},{"min":10,"max":null}]`, `"backfill_plan":[{"min":null,"max":null}]`, 1)
			case "DB failure":
				store.completeErr = errors.New("atomic completion failed")
			}
			var err error
			result.StateSHA256, err = utils.StateSHA256(final)
			require.NoError(t, err)
			data, err := json.Marshal(result)
			require.NoError(t, err)
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", final))
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
			writeOrdinaryExecutionProof(t, ctx, req, "settled", final)
			err = a.CleanupAndPersistState(ctx, req)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, final, store.job.State)
				require.Equal(t, "settled", store.record.Outcome)
			} else {
				require.Error(t, err)
				require.Equal(t, `{}`, store.job.State)
				require.Equal(t, "closing", store.record.Phase)
				require.Equal(t, "recovery", store.record.Outcome)
			}
		})
	}
}
