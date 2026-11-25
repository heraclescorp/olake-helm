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

const backfillIdentity = `{"system_id":"123","timeline":1,"database":"db","slot_name":"slot","plugin":"pgoutput"}`
const backfillShape = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const backfillPartial = `{"type":"GLOBAL","global":{"state":{"lsn":"0/100"},"streams":["public.one"]},"streams":[{"stream":"one","namespace":"public","sync_mode":"cdc","state":{"backfill_table":{"table_uuid":"table-one","shape_sha256":"` + backfillShape + `"},"backfill_plan":[{"min":null,"max":null}],"chunks":[]}},{"stream":"two","namespace":"public","sync_mode":"strict_cdc","state":{"backfill_table":{"table_uuid":"table-two","shape_sha256":"` + backfillShape + `"},"backfill_plan":[{"min":null,"max":10},{"min":10,"max":null}],"resume_plan":[{"min":null,"max":10},{"min":10,"max":null}],"chunks":[{"min":10,"max":null}]}}]}`
const backfillFinal = `{"type":"GLOBAL","global":{"state":{"lsn":"0/200"},"streams":["public.one","public.two"]},"streams":[{"stream":"one","namespace":"public","sync_mode":"cdc","state":{"backfill_table":{"table_uuid":"table-one","shape_sha256":"` + backfillShape + `"},"backfill_plan":[{"min":null,"max":null}],"chunks":[]}},{"stream":"two","namespace":"public","sync_mode":"strict_cdc","state":{"backfill_table":{"table_uuid":"table-two","shape_sha256":"` + backfillShape + `"},"backfill_plan":[{"min":null,"max":10},{"min":10,"max":null}],"chunks":[]}}]}`

func backfillRecoveryFixture(t *testing.T) (*AbstractExecutor, *ordinaryRecoveryStore, *types.ExecutionRequest, string, utils.BackfillRecoveryCheckpoint) {
	t.Helper()
	a, store, req, dir := ordinaryRecoveryFixture(t)
	store.job.State = `{}`
	store.job.Source = `{"host":"original","update_method":{"replication_slot":"slot"}}`
	require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, "source.json", store.job.Source))
	files, err := utils.ReadOrdinaryFrozenConfigs(context.Background(), req)
	require.NoError(t, err)
	catalogSHA, err := utils.OrdinaryCatalogSHA256(files)
	require.NoError(t, err)
	stateSHA, err := utils.StateSHA256(backfillPartial)
	require.NoError(t, err)
	candidate := utils.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: req.RuntimeID, LaunchID: req.LaunchID, SourceSHA256: utils.RawSHA256(store.job.Source), DestinationSHA256: utils.RawSHA256(store.job.Destination), CatalogSHA256: catalogSHA, StateSHA256: stateSHA, State: json.RawMessage(backfillPartial), ServerIdentity: backfillIdentity, SlotName: "slot"}
	writeBackfillCandidate(t, dir, candidate)
	return a, store, req, dir, candidate
}

func writeBackfillCandidate(t *testing.T, dir string, candidate utils.BackfillRecoveryCheckpoint) string {
	t.Helper()
	data, err := json.Marshal(candidate)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, utils.BackfillRecoveryCheckpointFile, string(data)))
	return string(data)
}

func TestBackfillRecoveryAdoptsCandidateWithoutAuthoritativeCheckpointOrReceiptMutation(t *testing.T) {
	a, store, req, originalDir, _ := backfillRecoveryFixture(t)
	ctx := context.Background()
	candidate, err := storage.ReadFile(ctx, originalDir, utils.BackfillRecoveryCheckpointFile, false)
	require.NoError(t, err)
	receipt, err := storage.ReadFile(ctx, originalDir, executionResultFile, false)
	require.NoError(t, err)
	a, store, req, dir := prepareRecoveryRuntimeFromOriginal(t, a, store, req)
	origin, baseline, _, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
	require.NoError(t, err)
	require.True(t, origin.Backfill)
	require.JSONEq(t, backfillPartial, baseline)
	require.Equal(t, utils.RawSHA256(candidate), origin.BackfillCheckpointSHA256)
	savedSHA, err := utils.StateSHA256(`{}`)
	require.NoError(t, err)
	require.Equal(t, savedSHA, origin.SavedStateSHA256)
	require.Equal(t, `{}`, store.job.State)
	copied, err := storage.ReadFile(ctx, dir, utils.BackfillRecoveryCheckpointFile, false)
	require.NoError(t, err)
	require.Equal(t, candidate, copied)
	result := recoveryResultFixture(t, req, dir)
	result.StateSHA256, err = utils.StateSHA256(backfillFinal)
	require.NoError(t, err)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", backfillFinal))
	writeOrdinaryExecutionProof(t, ctx, req, "settled", backfillFinal)
	require.NoError(t, a.CleanupAndPersistState(ctx, req))
	require.Equal(t, backfillFinal, store.job.State)
	require.Equal(t, "settled", store.record.Outcome)
	unchanged, err := storage.ReadFile(ctx, originalDir, executionResultFile, false)
	require.NoError(t, err)
	require.Equal(t, receipt, unchanged)
	unchanged, err = storage.ReadFile(ctx, originalDir, utils.BackfillRecoveryCheckpointFile, false)
	require.NoError(t, err)
	require.Equal(t, candidate, unchanged)
}

func TestBackfillRecoveryMissingOriginalReceiptNeverFabricatesSettlement(t *testing.T) {
	a, store, req, dir, _ := backfillRecoveryFixture(t)
	ctx := context.Background()
	require.NoError(t, os.Remove(dir+"/"+executionResultFile))
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	origin, _, _, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
	require.NoError(t, err)
	require.True(t, origin.Backfill)
	require.Empty(t, origin.OriginalReceiptSHA256)
	require.Equal(t, `{}`, store.job.State)
	require.Equal(t, "closing", store.record.Phase)
	_, err = storage.ReadFile(ctx, dir, executionResultFile, false)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestBackfillRecoveryCandidateValidationFailsClosed(t *testing.T) {
	for _, mode := range []string{"missing", "malformed", "version", "execution", "launch", "source", "destination", "catalog", "digest", "identity", "slot", "no GLOBAL", "invalid LSN", "zero LSN", "missing plan", "missing retained plan", "retained plan differs", "missing pending", "foreign pending", "duplicate pending", "duplicate plan", "missing table", "invalid shape", "duplicate state", "foreign membership", "missing completed table", "completed pending"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir, candidate := backfillRecoveryFixture(t)
			state := string(candidate.State)
			switch mode {
			case "version":
				candidate.Version = 2
			case "execution":
				candidate.ExecutionID = "foreign"
			case "launch":
				candidate.LaunchID = "foreign"
			case "source":
				candidate.SourceSHA256 = strings.Repeat("b", 64)
			case "destination":
				candidate.DestinationSHA256 = strings.Repeat("b", 64)
			case "catalog":
				candidate.CatalogSHA256 = strings.Repeat("b", 64)
			case "identity":
				candidate.ServerIdentity = " "
			case "slot":
				candidate.SlotName = "foreign"
			case "no GLOBAL":
				state = strings.Replace(state, `"GLOBAL"`, `"STREAM"`, 1)
			case "invalid LSN":
				state = strings.Replace(state, `"0/100"`, `"bad"`, 1)
			case "zero LSN":
				state = strings.Replace(state, `"0/100"`, `"0/0"`, 1)
			case "missing plan":
				state = strings.Replace(state, `"resume_plan":[{"min":null,"max":10},{"min":10,"max":null}],`, "", 1)
			case "missing retained plan":
				state = strings.Replace(state, `"backfill_plan":[{"min":null,"max":null}],`, "", 1)
			case "retained plan differs":
				state = strings.Replace(state, `"backfill_plan":[{"min":null,"max":10},{"min":10,"max":null}]`, `"backfill_plan":[{"min":null,"max":11},{"min":11,"max":null}]`, 1)
			case "missing pending":
				state = strings.Replace(state, `,"chunks":[{"min":10,"max":null}]`, "", 1)
			case "foreign pending":
				state = strings.Replace(state, `"chunks":[{"min":10,"max":null}]`, `"chunks":[{"min":11,"max":null}]`, 1)
			case "duplicate pending":
				state = strings.Replace(state, `"chunks":[{"min":10,"max":null}]`, `"chunks":[{"min":10,"max":null},{"min":10,"max":null}]`, 1)
			case "duplicate plan":
				state = strings.Replace(state, `"resume_plan":[{"min":null,"max":10},{"min":10,"max":null}]`, `"resume_plan":[{"min":10,"max":null},{"min":10,"max":null}]`, 1)
			case "missing table":
				state = strings.Replace(state, `"backfill_table":{"table_uuid":"table-two","shape_sha256":"`+backfillShape+`"},`, "", 1)
			case "invalid shape":
				state = strings.Replace(state, backfillShape, "invalid", 1)
			case "duplicate state":
				state = strings.Replace(state, `"stream":"two"`, `"stream":"one"`, 1)
			case "foreign membership":
				state = strings.Replace(state, `"streams":["public.one"]`, `"streams":["public.foreign"]`, 1)
			case "missing completed table":
				state = strings.Replace(state, `{"stream":"one","namespace":"public","sync_mode":"cdc","state":{"backfill_table":{"table_uuid":"table-one","shape_sha256":"`+backfillShape+`"},"backfill_plan":[{"min":null,"max":null}],"chunks":[]}},`, "", 1)
			case "completed pending":
				state = strings.Replace(state, `"chunks":[]`, `"chunks":[{"min":null,"max":10}]`, 1)
			}
			candidate.State = json.RawMessage(state)
			var err error
			candidate.StateSHA256, err = utils.StateSHA256(state)
			require.NoError(t, err)
			if mode == "digest" {
				candidate.StateSHA256 = strings.Repeat("b", 64)
			}
			writeBackfillCandidate(t, dir, candidate)
			if mode == "missing" {
				require.NoError(t, os.Remove(dir+"/"+utils.BackfillRecoveryCheckpointFile))
			} else if mode == "malformed" {
				require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, utils.BackfillRecoveryCheckpointFile, "{"))
			}
			require.Error(t, a.PrepareOrdinaryRecovery(context.Background(), req))
			require.Equal(t, `{}`, store.job.State)
			require.Equal(t, "closing", store.record.Phase)
			require.NotEqual(t, "settled", store.record.Outcome)
			require.NoError(t, utils.RequireClosedExecutionFence(context.Background(), req))
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			_, err = storage.ReadFile(context.Background(), stable, utils.OrdinaryRecoveryOriginFile, false)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestBackfillRecoveryHistoricalAndImmutableEvidenceRemainOwned(t *testing.T) {
	for _, mode := range []string{"historical missing candidate", "historical immutable origin", "immutable empty baseline", "partial baseline publication", "retirement failed", "saved checkpoint changed", "candidate bytes changed"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir, _ := backfillRecoveryFixture(t)
			ctx := context.Background()
			_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
			var immutableOrigin string
			switch mode {
			case "historical missing candidate", "historical immutable origin", "immutable empty baseline":
				require.NoError(t, storage.WriteFileAtomic(ctx, stable, utils.OrdinaryRecoveryBaselineFile, `{}`))
				if mode != "immutable empty baseline" {
					require.NoError(t, os.Remove(dir+"/"+utils.BackfillRecoveryCheckpointFile))
				}
				if mode == "historical immutable origin" {
					files, err := utils.ReadOrdinaryFrozenConfigs(ctx, req)
					require.NoError(t, err)
					catalogSHA, err := utils.OrdinaryCatalogSHA256(files)
					require.NoError(t, err)
					baselineSHA, err := utils.StateSHA256(`{}`)
					require.NoError(t, err)
					origin := utils.OrdinaryRecoveryOrigin{Version: 1, OriginalExecutionID: req.LaunchID, OriginalLaunchID: req.LaunchID, SourceSHA256: utils.RawSHA256(store.job.Source), DestinationSHA256: utils.RawSHA256(store.job.Destination), CatalogSHA256: catalogSHA, BaselineStateSHA256: baselineSHA}
					data, err := json.Marshal(origin)
					require.NoError(t, err)
					immutableOrigin = string(data)
					require.NoError(t, storage.WriteFileAtomic(ctx, stable, utils.OrdinaryRecoveryOriginFile, immutableOrigin))
				}
			case "partial baseline publication":
				require.NoError(t, storage.WriteFileAtomic(ctx, stable, utils.OrdinaryRecoveryBaselineFile, backfillPartial))
			case "retirement failed":
				a.executor.(*enrollmentInnerExecutor).cleanupErr = errors.New("exact original runtime retirement unconfirmed")
			case "saved checkpoint changed", "candidate bytes changed":
				require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
				if mode == "saved checkpoint changed" {
					store.job.State = ordinaryBaseline
				} else {
					candidate, err := storage.ReadFile(ctx, dir, utils.BackfillRecoveryCheckpointFile, false)
					require.NoError(t, err)
					require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.BackfillRecoveryCheckpointFile, candidate+"\n"))
				}
			}
			err := a.PrepareOrdinaryRecovery(ctx, req)
			if mode == "partial baseline publication" || mode == "candidate bytes changed" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				if mode == "historical missing candidate" || mode == "historical immutable origin" {
					require.Contains(t, err.Error(), "unsupported historical initial-backfill recovery")
					require.Contains(t, err.Error(), "job remains paused and owned")
				}
			}
			require.Equal(t, "closing", store.record.Phase)
			require.NotEqual(t, "settled", store.record.Outcome)
			if mode == "historical missing candidate" || mode == "historical immutable origin" || mode == "immutable empty baseline" {
				baseline, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryBaselineFile, false)
				require.NoError(t, err)
				require.Equal(t, `{}`, baseline)
			}
			if immutableOrigin != "" {
				origin, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
				require.NoError(t, err)
				require.Equal(t, immutableOrigin, origin)
			}
			if mode == "retirement failed" {
				_, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryBaselineFile, false)
				require.ErrorIs(t, err, os.ErrNotExist)
				_, err = storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryOriginFile, false)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			if mode != "saved checkpoint changed" {
				require.Equal(t, `{}`, store.job.State)
			}
		})
	}
}

func TestBackfillRecoveryRequiresCompleteFinalProof(t *testing.T) {
	for _, mode := range []string{"missing runtime candidate", "changed runtime candidate", "missing table proof", "partial final membership", "pending final chunks", "changed table UUID", "changed table shape", "changed retained plan", "missing final stream", "unknown receipt", "DB failure"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, _, _ := backfillRecoveryFixture(t)
			a, store, req, dir := prepareRecoveryRuntimeFromOriginal(t, a, store, req)
			ctx := context.Background()
			result := recoveryResultFixture(t, req, dir)
			final, outcome := backfillFinal, "settled"
			switch mode {
			case "missing runtime candidate":
				require.NoError(t, os.Remove(dir+"/"+utils.BackfillRecoveryCheckpointFile))
			case "changed runtime candidate":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.BackfillRecoveryCheckpointFile, `{}`))
			case "missing table proof":
				result.Tables = result.Tables[:1]
			case "partial final membership":
				final = strings.Replace(final, `"streams":["public.one","public.two"]`, `"streams":["public.one"]`, 1)
			case "pending final chunks":
				final = strings.Replace(final, `"chunks":[]`, `"chunks":[{"min":null,"max":10}]`, 1)
			case "changed table UUID":
				final = strings.Replace(final, `"table-two"`, `"replacement"`, 1)
				result.Tables[1].TableUUID = "replacement"
			case "changed table shape":
				final = strings.Replace(final, backfillShape, strings.Repeat("b", 64), 1)
			case "changed retained plan":
				final = strings.Replace(final, `"backfill_plan":[{"min":null,"max":10},{"min":10,"max":null}]`, `"backfill_plan":[{"min":null,"max":null}]`, 1)
			case "missing final stream":
				final = strings.Replace(final, `,{"stream":"two","namespace":"public","sync_mode":"strict_cdc","state":{"backfill_table":{"table_uuid":"table-two","shape_sha256":"`+backfillShape+`"},"backfill_plan":[{"min":null,"max":10},{"min":10,"max":null}],"chunks":[]}}`, "", 1)
			case "unknown receipt":
				outcome = "unknown"
			case "DB failure":
				store.completeErr = errors.New("atomic persistence unavailable")
			}
			var err error
			result.StateSHA256, err = utils.StateSHA256(final)
			require.NoError(t, err)
			data, err := json.Marshal(result)
			require.NoError(t, err)
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", final))
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
			writeOrdinaryExecutionProof(t, ctx, req, outcome, final)
			require.Error(t, a.CleanupAndPersistState(ctx, req))
			require.Equal(t, `{}`, store.job.State)
			require.Equal(t, "closing", store.record.Phase)
			require.Equal(t, "recovery", store.record.Outcome)
		})
	}
}

func TestBackfillRecoveryInterruptedEpochReusesOriginalCandidateAndBaseline(t *testing.T) {
	a, store, req, originalDir, _ := backfillRecoveryFixture(t)
	a, store, req, dir := prepareRecoveryRuntimeFromOriginal(t, a, store, req)
	ctx := context.Background()
	originalCandidate, err := storage.ReadFile(ctx, originalDir, utils.BackfillRecoveryCheckpointFile, false)
	require.NoError(t, err)
	writeOrdinaryExecutionProof(t, ctx, req, "unknown", "")
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", backfillFinal))
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	require.Equal(t, `{}`, store.job.State)
	req.RuntimeID += "-next"
	require.NoError(t, a.ordinaryRecoveryInputs(ctx, req))
	copied := make(map[string]string)
	for _, config := range req.Configs {
		copied[config.Name] = config.Data
	}
	require.Equal(t, originalCandidate, copied[utils.BackfillRecoveryCheckpointFile])
	require.JSONEq(t, backfillPartial, copied["state.json"])
	require.JSONEq(t, backfillPartial, copied[utils.OrdinaryRecoveryBaselineFile])
}
