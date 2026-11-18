package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

const ordinaryBaseline = `{"type":"GLOBAL","global":{"state":{"lsn":"0/100"},"streams":["public.one","public.two"]},"streams":[]}`
const ordinaryFinal = `{"type":"GLOBAL","global":{"state":{"lsn":"0/200"},"streams":["public.one","public.two"]},"streams":[]}`
const ordinaryCatalog = `{"streams":[{"stream":{"namespace":"public","name":"one","sync_mode":"cdc","source_defined_primary_key":["id"]}},{"stream":{"namespace":"public","name":"two","sync_mode":"strict_cdc","source_defined_primary_key":["id"]}}],"selected_streams":{"public":[{"stream_name":"one"},{"stream_name":"two"}]}}`

type ordinaryRecoveryStore struct {
	executionTestAuthority
	job         types.JobData
	completeErr error
	completions int
}

func (db *ordinaryRecoveryStore) GetJobData(context.Context, int) (types.JobData, error) {
	return db.job, nil
}
func (db *ordinaryRecoveryStore) UpdateJobState(context.Context, int, string, string, string) error {
	return errors.New("recovery must not use non-atomic ordinary checkpoint persistence")
}
func (db *ordinaryRecoveryStore) CompleteOrdinaryRecovery(ctx context.Context, req *types.ExecutionRequest, verify func(context.Context) (string, error)) error {
	db.completions++
	state, err := verify(ctx)
	if err != nil {
		return err
	}
	if db.completeErr != nil {
		return db.completeErr
	}
	db.job.State = state
	db.record.Phase, db.record.Outcome = "closed", "settled"
	return nil
}

func ordinaryRecoveryFixture(t *testing.T) (*AbstractExecutor, *ordinaryRecoveryStore, *types.ExecutionRequest, string) {
	t.Helper()
	req, dir := executorEnrollmentFixture(t)
	req.ResourceID, req.ResourceUID = "original-resource", "original-uid"
	job := types.JobData{Driver: "postgres", DestinationDriver: "iceberg", Source: `{"host":"original","replication_slot":"slot"}`, Destination: `{"type":"ICEBERG","writer":{"catalog_type":"rest"}}`, Streams: ordinaryCatalog, State: ordinaryBaseline}
	store := &ordinaryRecoveryStore{job: job}
	inner := &enrollmentInnerExecutor{}
	a := &AbstractExecutor{db: store, executor: inner}
	require.NoError(t, storage.WriteFiles(context.Background(), dir, []types.JobConfig{{Name: "source.json", Data: job.Source}, {Name: "destination.json", Data: job.Destination}, {Name: constants.StreamsFile, Data: job.Streams}}))
	writeOrdinaryExecutionProof(t, context.Background(), req, "unknown", "")
	require.NoError(t, utils.OpenExecutionFence(context.Background(), req))
	return a, store, req, dir
}

func prepareRecoveryRuntime(t *testing.T) (*AbstractExecutor, *ordinaryRecoveryStore, *types.ExecutionRequest, string) {
	t.Helper()
	a, store, req, _ := ordinaryRecoveryFixture(t)
	require.NoError(t, a.PrepareOrdinaryRecovery(context.Background(), req))
	req.RunID, req.RuntimeID = "recovery-run", req.RuntimeID+"-recovery"
	req.RecoverOnly, req.AllowLaunch = true, true
	req.ResourceID, req.ResourceUID = "recovery-resource", "recovery-uid"
	store.record = &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, ResourceID: req.ResourceID, ResourceUID: req.ResourceUID, Outcome: "recovery", Phase: "admitted"}
	_, dir := utils.GetExecutionDirAndSubDir(req)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	require.NoError(t, a.ordinaryRecoveryInputs(context.Background(), req))
	require.NoError(t, storage.WriteFiles(context.Background(), dir, req.Configs))
	require.NoError(t, utils.OpenExecutionFence(context.Background(), req))
	return a, store, req, dir
}

func recoveryResultFixture(t *testing.T, req *types.ExecutionRequest, dir string) utils.OrdinaryRecoveryResult {
	t.Helper()
	origin, _, _, err := utils.ReadOrdinaryRecoveryOrigin(context.Background(), req)
	require.NoError(t, err)
	server := utils.OrdinaryRecoveryServer{Version: 1, OriginalLaunchID: req.LaunchID, SourceSHA256: origin.SourceSHA256, ServerIdentity: `{"system_id":"123","timeline":1,"database":"db","slot_name":"slot","plugin":"pgoutput"}`, SlotName: "slot"}
	data, err := json.Marshal(server)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, utils.OrdinaryRecoveryServerFile, string(data)))
	stateSHA, err := utils.StateSHA256(ordinaryFinal)
	require.NoError(t, err)
	return utils.OrdinaryRecoveryResult{OrdinaryRecoveryOrigin: origin, ExecutionID: req.RuntimeID, LaunchID: req.LaunchID, StateSHA256: stateSHA, LowerLSN: "0/100", TargetLSN: "0/200", ServerIdentity: server.ServerIdentity, SlotName: server.SlotName, ToastValueSemantics: "current_source_lookup", Tables: []utils.OrdinaryRecoveryTableProof{{Stream: "public.one", TableUUID: "table-one", FenceSnapshotID: 101, CommittedLSN: "0/200"}, {Stream: "public.two", TableUUID: "table-two", FenceSnapshotID: 202, CommittedLSN: "0/200"}}}
}

func TestOrdinaryRecoveryFreezesUnknownOriginWithoutRelabelingOriginal(t *testing.T) {
	a, store, req, dir := ordinaryRecoveryFixture(t)
	ctx := context.Background()
	original, err := storage.ReadFile(ctx, dir, executionResultFile, false)
	require.NoError(t, err)
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	require.Equal(t, "closing", store.record.Phase)
	require.Empty(t, store.record.Outcome)
	require.Equal(t, ordinaryBaseline, store.job.State)
	require.Error(t, utils.ReadExecutionFence(ctx, req))
	origin, baseline, _, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
	require.NoError(t, err)
	require.Equal(t, utils.RawSHA256(original), origin.OriginalReceiptSHA256)
	require.Equal(t, ordinaryBaseline, baseline)
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req), "create-only origin is retry safe")
	unchanged, err := storage.ReadFile(ctx, dir, executionResultFile, false)
	require.NoError(t, err)
	require.Equal(t, original, unchanged)
}

func TestOrdinaryRecoveryPreparationFailsClosed(t *testing.T) {
	for _, mode := range []string{"clear", "uncontrolled", "missing resource", "delete failed", "wrong original receipt", "changed original source", "missing PK", "full refresh", "missing membership", "not GLOBAL", "checkpoint changed after partial origin"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir := ordinaryRecoveryFixture(t)
			saved := store.job.State
			switch mode {
			case "clear":
				req.Command = types.ClearDestination
			case "uncontrolled":
				req.Controlled = false
			case "missing resource":
				req.ResourceID, req.ResourceUID = "", ""
			case "delete failed":
				a.executor.(*enrollmentInnerExecutor).cleanupErr = errors.New("exact UID deletion unconfirmed")
			case "wrong original receipt":
				writeOrdinaryExecutionProof(t, context.Background(), req, "settled", ordinaryBaseline)
			case "changed original source":
				require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, "source.json", `{"host":"changed"}`))
			case "missing PK", "full refresh":
				catalog := strings.Replace(ordinaryCatalog, `"source_defined_primary_key":["id"]`, `"source_defined_primary_key":[]`, 1)
				if mode == "full refresh" {
					catalog = strings.Replace(ordinaryCatalog, `"cdc"`, `"full_refresh"`, 1)
				}
				store.job.Streams = catalog
				require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, constants.StreamsFile, catalog))
			case "missing membership":
				store.job.State = strings.Replace(ordinaryBaseline, `"public.one","public.two"`, `"public.one"`, 1)
			case "not GLOBAL":
				store.job.State = strings.Replace(ordinaryBaseline, `"GLOBAL"`, `"STREAM"`, 1)
			case "checkpoint changed after partial origin":
				_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
				require.NoError(t, storage.WriteFileAtomic(context.Background(), stable, utils.OrdinaryRecoveryBaselineFile, ordinaryFinal))
			}
			require.Error(t, a.PrepareOrdinaryRecovery(context.Background(), req))
			if mode != "missing membership" && mode != "not GLOBAL" {
				require.Equal(t, saved, store.job.State)
			}
			if store.record != nil {
				require.NotEqual(t, "settled", store.record.Outcome)
			}
		})
	}
}

func TestOrdinaryRecoveryConsumesOnlyCompleteFreshProofAndAtomicCheckpoint(t *testing.T) {
	for _, mode := range []string{"success", "missing result", "wrong execution", "wrong launch", "wrong origin receipt", "wrong baseline", "wrong source", "wrong destination", "wrong catalog", "wrong final digest", "lower changed", "target behind", "checkpoint behind", "missing table", "duplicate table", "foreign table", "missing UUID", "missing fence snapshot", "table behind", "table ahead", "new receipt unknown", "new receipt not started", "original receipt changed", "frozen config changed", "runtime source changed", "runtime destination changed", "runtime catalog changed", "runtime baseline changed", "missing server pin", "server changed", "slot changed", "unknown toast semantics", "DB persistence failed", "cleanup failed"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir := prepareRecoveryRuntime(t)
			ctx := context.Background()
			result := recoveryResultFixture(t, req, dir)
			final := ordinaryFinal
			outcome := "settled"
			switch mode {
			case "wrong execution":
				result.ExecutionID = "foreign"
			case "wrong launch":
				result.LaunchID = "foreign"
			case "wrong origin receipt":
				result.OriginalReceiptSHA256 = "foreign"
			case "wrong baseline":
				result.BaselineStateSHA256 = "foreign"
			case "wrong source":
				result.SourceSHA256 = "foreign"
			case "wrong destination":
				result.DestinationSHA256 = "foreign"
			case "wrong catalog":
				result.CatalogSHA256 = "foreign"
			case "wrong final digest":
				result.StateSHA256 = "foreign"
			case "lower changed":
				result.LowerLSN = "0/101"
			case "target behind":
				result.TargetLSN = "0/FF"
			case "checkpoint behind":
				final = ordinaryBaseline
			case "missing table":
				result.Tables = result.Tables[:1]
			case "duplicate table":
				result.Tables[1].Stream = result.Tables[0].Stream
			case "foreign table":
				result.Tables[1].Stream = "public.foreign"
			case "missing UUID":
				result.Tables[0].TableUUID = ""
			case "missing fence snapshot":
				result.Tables[0].FenceSnapshotID = 0
			case "table behind":
				result.Tables[0].CommittedLSN = "0/100"
			case "table ahead":
				result.Tables[0].CommittedLSN = "0/300"
			case "new receipt unknown":
				outcome = "unknown"
			case "new receipt not started":
				outcome = "not_started"
			case "original receipt changed":
				original := *req
				original.RuntimeID = req.LaunchID
				writeOrdinaryExecutionProof(t, ctx, &original, "not_started", "")
			case "frozen config changed":
				original := *req
				original.RuntimeID = req.LaunchID
				_, originalDir := utils.GetExecutionDirAndSubDir(&original)
				require.NoError(t, storage.WriteFileAtomic(ctx, originalDir, "source.json", `{"host":"edited"}`))
			case "runtime source changed":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "source.json", `{"host":"edited"}`))
			case "runtime destination changed":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "destination.json", `{"type":"edited"}`))
			case "runtime catalog changed":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, constants.StreamsFile, `{"streams":[]}`))
			case "runtime baseline changed":
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryBaselineFile, ordinaryFinal))
			case "missing server pin":
				require.NoError(t, os.Remove(dir+"/"+utils.OrdinaryRecoveryServerFile))
			case "server changed":
				result.ServerIdentity = "foreign"
			case "slot changed":
				result.SlotName = "foreign"
			case "unknown toast semantics":
				result.ToastValueSemantics = "event_time"
			case "DB persistence failed":
				store.completeErr = errors.New("transaction failed")
			case "cleanup failed":
				a.executor.(*enrollmentInnerExecutor).cleanupErr = errors.New("retirement unconfirmed")
			}
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", final))
			writeOrdinaryExecutionProof(t, ctx, req, outcome, final)
			if mode != "missing result" {
				data, err := json.Marshal(result)
				require.NoError(t, err)
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
			}
			err := a.CleanupAndPersistState(ctx, req)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, ordinaryFinal, store.job.State)
				require.Equal(t, "settled", store.record.Outcome)
				require.Equal(t, "closed", store.record.Phase)
				require.Equal(t, 1, store.completions)
				original := *req
				original.RuntimeID = req.LaunchID
				_, originalDir := utils.GetExecutionDirAndSubDir(&original)
				originalReceipt, err := storage.ReadFile(ctx, originalDir, executionResultFile, false)
				require.NoError(t, err)
				require.Equal(t, result.OriginalReceiptSHA256, utils.RawSHA256(originalReceipt), "recovery cannot rewrite or relabel the old unknown receipt")
				require.NoError(t, a.CleanupAndPersistState(ctx, req), "lost completion acknowledgement does not persist or launch again")
				require.Equal(t, 1, store.completions)
			} else {
				require.Error(t, err)
				require.Equal(t, ordinaryBaseline, store.job.State)
				require.Equal(t, "closing", store.record.Phase)
				require.Equal(t, "recovery", store.record.Outcome)
			}
		})
	}
}

func TestOrdinaryRecoveryRetriesAttachWithoutRewritingAndPinServerAcrossEpochs(t *testing.T) {
	a, store, req, dir := prepareRecoveryRuntime(t)
	ctx := context.Background()
	result := recoveryResultFixture(t, req, dir)
	calls := 0
	a.executor = &enrollmentInnerExecutor{execute: func(_ context.Context, current *types.ExecutionRequest, _ string) (string, error) {
		calls++
		require.False(t, current.AllowLaunch)
		require.Equal(t, req.LaunchID, current.LaunchID)
		require.Contains(t, current.Args, "--ordinary-recovery-origin")
		require.Contains(t, current.Args, "--ordinary-recovery-result")
		require.Contains(t, current.Args, "--execution-launch-id")
		require.NotContains(t, current.Args, "--clear-destination")
		return "", nil
	}}
	req.AllowLaunch = false
	req.Args = []string{"sync", "--clear-destination", "--ordinary-recovery-origin", "caller-path"}
	req.Configs = []types.JobConfig{{Name: "state.json", Data: `{"corrupt":true}`}}
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", ordinaryFinal))
	_, err := a.Execute(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	state, err := storage.ReadFile(ctx, dir, "state.json", false)
	require.NoError(t, err)
	require.Equal(t, ordinaryFinal, state, "attach retry cannot rewrite running checkpoint")
	writeOrdinaryExecutionProof(t, ctx, req, "unknown", "")
	require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
	_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	pin, err := storage.ReadFile(ctx, stable, utils.OrdinaryRecoveryServerFile, false)
	require.NoError(t, err)
	require.Contains(t, pin, `"slot_name":"slot"`)
	store.record.Phase, store.record.Outcome = "admitted", "recovery"
	req.RuntimeID += "-next"
	require.NoError(t, a.ordinaryRecoveryInputs(ctx, req))
	var copied string
	for _, config := range req.Configs {
		if config.Name == utils.OrdinaryRecoveryServerFile {
			copied = config.Data
		}
	}
	require.Equal(t, pin, copied)
	server, err := utils.ValidateOrdinaryRecoveryServer(copied, result.OrdinaryRecoveryOrigin)
	require.NoError(t, err)
	require.Equal(t, result.ServerIdentity, server.ServerIdentity)
}

func TestOrdinaryRecoveryInterruptedEpochRequiresPinnedServerOrPositiveNoStart(t *testing.T) {
	for _, outcome := range []string{"unknown", "not_started"} {
		t.Run(outcome, func(t *testing.T) {
			a, store, req, _ := prepareRecoveryRuntime(t)
			writeOrdinaryExecutionProof(t, context.Background(), req, outcome, "")
			err := a.PrepareOrdinaryRecovery(context.Background(), req)
			if outcome == "not_started" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, ordinaryBaseline, store.job.State)
			require.Equal(t, "closing", store.record.Phase)
			require.Equal(t, "recovery", store.record.Outcome, "no-start successor is never settlement of unknown original")
		})
	}
	a, _, req, dir := prepareRecoveryRuntime(t)
	result := recoveryResultFixture(t, req, dir)
	writeOrdinaryExecutionProof(t, context.Background(), req, "unknown", "")
	require.NoError(t, a.PrepareOrdinaryRecovery(context.Background(), req))
	server := utils.OrdinaryRecoveryServer{Version: 1, OriginalLaunchID: req.LaunchID, SourceSHA256: result.SourceSHA256, ServerIdentity: "different-cluster", SlotName: "slot"}
	data, err := json.Marshal(server)
	require.NoError(t, err)
	require.NoError(t, storage.WriteFileAtomic(context.Background(), dir, utils.OrdinaryRecoveryServerFile, string(data)))
	require.Error(t, a.PrepareOrdinaryRecovery(context.Background(), req), "an interrupted epoch cannot replace the durable server identity")
}
