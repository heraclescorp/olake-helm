package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
)

func atomicExecutorFixture(t *testing.T) (*types.ExecutionRequest, string) {
	t.Helper()
	req, dir := executorEnrollmentFixture(t)
	req.Command = types.Resync
	req.Args = []string{"resync", constants.ResyncNamespaceFlag, "public", constants.ResyncTableFlag, "target", constants.ResyncIDFlag, req.WorkflowID}
	return req, dir
}

func publicationForState(t *testing.T, req *types.ExecutionRequest, state string) resyncPublication {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(state))
	decoder.UseNumber()
	var object map[string]interface{}
	require.NoError(t, decoder.Decode(&object))
	canonical, err := json.Marshal(object)
	require.NoError(t, err)
	digest := sha256.Sum256(canonical)
	return resyncPublication{Version: 1, ResyncID: req.WorkflowID, ExecutionID: req.RuntimeID, LaunchID: req.LaunchID, Namespace: "public", StreamName: "target", TableUUID: "original-table-uuid", SnapshotID: 123, StateSHA256: hex.EncodeToString(digest[:])}
}

func TestAtomicResyncRequiresSuccessCleanupAndMatchingPublication(t *testing.T) {
	for _, mode := range []string{"success", "cleanup failure", "persistence failure", "missing receipt", "wrong id", "wrong runtime", "wrong launch", "wrong namespace", "wrong target", "wrong version", "missing uuid", "missing snapshot", "state mismatch", "null state", "trailing state"} {
		t.Run(mode, func(t *testing.T) {
			req, dir := atomicExecutorFixture(t)
			ctx := context.Background()
			const saved = `{"global":{"lsn":"original"},"streams":{"public.other":{"cursor":"preserved"}}}`
			const candidate = ` { "streams": {"public.other":{"cursor":"preserved"}, "public.target":{"cursor":9007199254740993}}, "global":{"lsn":9007199254740995} } `
			store := &resyncStateStore{state: saved}
			inner := &enrollmentInnerExecutor{}
			exec := &AbstractExecutor{executor: inner, db: store}
			receipt := publicationForState(t, req, candidate)
			state := candidate
			switch mode {
			case "cleanup failure":
				inner.cleanupErr = errors.New("resource still running")
			case "persistence failure":
				store.err = errors.New("database unavailable")
			case "wrong id":
				receipt.ResyncID = "other-operation"
			case "wrong runtime":
				receipt.ExecutionID = "old-runtime"
			case "wrong launch":
				receipt.LaunchID = "another-admitted-launch"
			case "wrong namespace":
				receipt.Namespace = "other"
			case "wrong target":
				receipt.StreamName = "other"
			case "wrong version":
				receipt.Version = 2
			case "missing uuid":
				receipt.TableUUID = " "
			case "missing snapshot":
				receipt.SnapshotID = 0
			case "state mismatch":
				state = `{"partial":true}`
			case "null state":
				state = `null`
			case "trailing state":
				state = candidate + ` {"extra":true}`
			}
			require.NoError(t, storage.WriteFiles(ctx, dir, []types.JobConfig{{Name: "state.json", Data: state}}))
			if mode != "missing receipt" {
				data, err := json.Marshal(receipt)
				require.NoError(t, err)
				require.NoError(t, storage.WriteFiles(ctx, dir, []types.JobConfig{{Name: constants.ResyncResultFile, Data: string(data)}}))
			}
			err := exec.CleanupResync(ctx, req, true)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, candidate, store.state, "full checkpoint and large integers must survive exactly")
			} else {
				require.Error(t, err)
				require.Equal(t, saved, store.state)
			}
			if mode == "cleanup failure" || mode == "persistence failure" {
				inner.cleanupErr, store.err = nil, nil
				require.NoError(t, exec.CleanupResync(ctx, req, true), "retry persistence without rerunning the connector")
				require.Equal(t, candidate, store.state)
			}
			require.Error(t, exec.CleanupAndPersistState(ctx, req), "generic cleanup must never bypass publication checks")
		})
	}
}

func TestFailedAndCancelledResyncNeverReadOrPersistCandidateState(t *testing.T) {
	for _, mode := range []string{"failed confirmed unpublished", "cancelled confirmed unpublished", "unknown publication", "in progress", "stale identity", "stale runtime", "stale launch", "recovery cannot abort", "cleanup failed"} {
		t.Run(mode, func(t *testing.T) {
			req, dir := atomicExecutorFixture(t)
			ctx := context.Background()
			const saved = `{"global":{"lsn":"original"},"streams":{"other":{"cursor":"unchanged"}}}`
			store := &resyncStateStore{state: saved}
			inner := &enrollmentInnerExecutor{}
			exec := &AbstractExecutor{executor: inner, db: store}
			proof := map[string]interface{}{"version": 1, "resync_id": req.WorkflowID, "execution_id": req.RuntimeID, "launch_id": req.LaunchID, "namespace": "public", "stream_name": "target", "publication": "not_published"}
			if mode == "in progress" {
				proof["publication"] = "in_progress"
			}
			if mode == "stale identity" {
				proof["resync_id"] = "different-operation"
			}
			if mode == "stale runtime" {
				proof["execution_id"] = "prior-runtime"
			}
			if mode == "stale launch" {
				proof["launch_id"] = "different-launch"
			}
			if mode == "recovery cannot abort" {
				req.RecoverOnly = true
				store.record = &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Phase: "admitted", Outcome: "recovery"}
			}
			if mode == "cleanup failed" {
				inner.cleanupErr = errors.New("cannot stop connector")
			}
			// Invalid JSON proves that failed/cancelled cleanup never reads state.
			require.NoError(t, storage.WriteFiles(ctx, dir, []types.JobConfig{{Name: "state.json", Data: "partial invalid checkpoint"}}))
			if mode != "unknown publication" {
				data, err := json.Marshal(proof)
				require.NoError(t, err)
				require.NoError(t, storage.WriteFiles(ctx, dir, []types.JobConfig{{Name: constants.ResyncFailureFile, Data: string(data)}}))
			}
			err := exec.CleanupResync(ctx, req, false)
			if strings.Contains(mode, "confirmed unpublished") {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, saved, store.state)
		})
	}
}
