package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

type clearRetirementExecutor struct {
	authorityCleanupExecutor
	resolve func(context.Context, *types.ExecutionRequest) error
}

func (e *clearRetirementExecutor) ResolveResource(ctx context.Context, req *types.ExecutionRequest) error {
	return e.resolve(ctx, req)
}

func TestRetireForClearAuthenticatesOldCommandBeforeProviderLookup(t *testing.T) {
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		for _, outcome := range []string{"settled", "not_started"} {
			t.Run(string(command)+"/"+outcome, func(t *testing.T) {
				req, dir := executorEnrollmentFixture(t)
				ctx := context.Background()
				req.Command = command
				const saved = ` {"global":{"lsn":"saved"},"streams":{"other":{"cursor":"keep"}}} `
				const final = ` {"global":{"lsn":9007199254740993},"streams":{"other":{"cursor":"keep"}}} `
				candidate := final
				if outcome == "not_started" {
					candidate = "invalid checkpoint must not be read"
				}
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", candidate))
				writeOrdinaryExecutionProof(t, ctx, req, outcome, final)
				originalReceipt, err := storage.ReadFile(ctx, dir, executionResultFile, false)
				require.NoError(t, err)
				require.NoError(t, utils.OpenExecutionFence(ctx, req))
				store := &enrollmentStateStore{state: saved}
				resolved, cleaned := 0, 0
				inner := &clearRetirementExecutor{
					resolve: func(ctx context.Context, current *types.ExecutionRequest) error {
						resolved++
						require.Equal(t, command, current.Command, "lookup must not use the new clear intent")
						require.NoError(t, utils.RequireClosedExecutionFence(ctx, current))
						current.ResourceID, current.ResourceUID = "original-resource", "original-uid"
						return nil
					},
					authorityCleanupExecutor: authorityCleanupExecutor{cleanup: func(ctx context.Context, current *types.ExecutionRequest) error {
						cleaned++
						require.Equal(t, command, current.Command)
						require.Equal(t, "original-resource", current.ResourceID)
						require.Equal(t, "original-uid", store.record.ResourceUID, "persist discovered identity before deletion")
						require.NoError(t, utils.RequireClosedExecutionFence(ctx, current))
						return nil
					}},
				}
				a := &AbstractExecutor{executor: inner, db: store}
				req.Command = types.ClearDestination
				require.NoError(t, a.RetireForClear(ctx, req))
				require.Equal(t, command, req.Command)
				require.Equal(t, 1, resolved)
				require.Equal(t, 1, cleaned)
				require.Equal(t, "closed", store.record.Phase)
				require.Equal(t, outcome, store.record.Outcome)
				if outcome == "settled" {
					require.Equal(t, final, store.state)
				} else {
					require.Equal(t, saved, store.state)
				}
				unchanged, err := storage.ReadFile(ctx, dir, executionResultFile, false)
				require.NoError(t, err)
				require.Equal(t, originalReceipt, unchanged)
			})
		}
	}
}

func TestRetireForClearBlocksUnprovenOrUnsafeOrdinaryRetirement(t *testing.T) {
	for _, mode := range []string{"unknown", "missing", "foreign runtime", "foreign launch", "resync command", "wrong version", "malformed", "mismatched hash", "missing state", "unsafe lookup", "unsafe cleanup", "changed owner", "handoff", "uncontrolled"} {
		t.Run(mode, func(t *testing.T) {
			req, dir := executorEnrollmentFixture(t)
			ctx := context.Background()
			const saved = `{"global":{"lsn":"saved"}}`
			const final = `{"global":{"lsn":"next"}}`
			store := &enrollmentStateStore{state: saved}
			proof := executionResult{Version: 1, ExecutionID: req.RuntimeID, LaunchID: req.LaunchID, Command: string(types.Sync), Outcome: "settled"}
			var err error
			proof.StateSHA256, err = utils.StateSHA256(final)
			require.NoError(t, err)
			switch mode {
			case "unknown":
				proof.Outcome = "unknown"
			case "foreign runtime":
				proof.ExecutionID = "foreign"
			case "foreign launch":
				proof.LaunchID = "foreign"
			case "resync command":
				proof.Command = string(types.Resync)
			case "wrong version":
				proof.Version = 2
			case "mismatched hash":
				proof.StateSHA256 = "foreign"
			case "changed owner":
				store.authorityErr = database.ErrJobExecutionOwned
			case "handoff":
				store.record = &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Phase: "handoff", HandoffToken: "unresolved"}
			case "uncontrolled":
				req.Controlled = false
			}
			if mode != "missing state" {
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", final))
			}
			data, err := json.Marshal(proof)
			require.NoError(t, err)
			if mode == "malformed" {
				data = []byte("{")
			}
			if mode != "missing" {
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, executionResultFile, string(data)))
			}
			lookups, cleanups := 0, 0
			inner := &clearRetirementExecutor{
				resolve: func(context.Context, *types.ExecutionRequest) error {
					lookups++
					if mode == "unsafe lookup" {
						return errors.New("provider ownership metadata does not match")
					}
					return nil
				},
				authorityCleanupExecutor: authorityCleanupExecutor{cleanup: func(context.Context, *types.ExecutionRequest) error {
					cleanups++
					if mode == "unsafe cleanup" {
						return errors.New("immutable resource termination unconfirmed")
					}
					return nil
				}},
			}
			a := &AbstractExecutor{executor: inner, db: store}
			req.Command = types.ClearDestination
			require.Error(t, a.RetireForClear(ctx, req))
			require.Equal(t, saved, store.state)
			if store.record != nil {
				require.NotEqual(t, "closed", store.record.Phase)
			}
			switch mode {
			case "missing", "foreign runtime", "foreign launch", "resync command", "wrong version", "malformed", "changed owner", "handoff", "uncontrolled":
				require.Zero(t, lookups, "invalid identity cannot authorize provider lookup")
				require.Zero(t, cleanups)
				require.Equal(t, types.ClearDestination, req.Command)
			case "unsafe lookup":
				require.Zero(t, cleanups)
			}
			if mode != "missing" {
				unchanged, err := storage.ReadFile(ctx, dir, executionResultFile, false)
				require.NoError(t, err)
				require.Equal(t, string(data), unchanged)
			}
		})
	}
}

func TestRetireForClearSealsUnadmittedOwnerWithoutReceipt(t *testing.T) {
	req, _ := executorEnrollmentFixture(t)
	req.Command = types.ClearDestination
	req.RuntimeID, req.LaunchID = "", ""
	store := &enrollmentStateStore{state: ` {"keep":true} `, executionTestAuthority: executionTestAuthority{prelaunch: true}}
	a := &AbstractExecutor{executor: &authorityCleanupExecutor{cleanup: func(context.Context, *types.ExecutionRequest) error {
		t.Fatal("unadmitted execution must not contact provider")
		return nil
	}}, db: store}
	require.NoError(t, a.RetireForClear(context.Background(), req))
	require.Equal(t, "closed", store.record.Phase)
	require.Equal(t, "not_launched", store.record.Outcome)
	require.Equal(t, ` {"keep":true} `, store.state)
}

func TestRetireForClearRecoveryStillRequiresSeparateRecoveryProof(t *testing.T) {
	for _, mode := range []string{"complete recovery", "missing recovery proof", "not started recovery", "clear receipt", "closed missing recovery proof"} {
		t.Run(mode, func(t *testing.T) {
			a, store, req, dir := prepareRecoveryRuntime(t)
			ctx := context.Background()
			result := recoveryResultFixture(t, req, dir)
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", ordinaryFinal))
			outcome := "settled"
			if mode == "not started recovery" {
				outcome = "not_started"
			}
			if mode == "clear receipt" {
				req.Command = types.ClearDestination
			}
			writeOrdinaryExecutionProof(t, ctx, req, outcome, ordinaryFinal)
			if mode == "complete recovery" {
				data, err := json.Marshal(result)
				require.NoError(t, err)
				require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryResultFile, string(data)))
			}
			if mode == "closed missing recovery proof" {
				store.record.Phase, store.record.Outcome = "closed", "settled"
				require.NoError(t, utils.CloseExecutionFence(ctx, req))
			}
			req.Command = types.ClearDestination
			err := a.RetireForClear(ctx, req)
			if mode == "complete recovery" {
				require.NoError(t, err)
				require.Equal(t, ordinaryFinal, store.job.State)
				require.Equal(t, "closed", store.record.Phase)
			} else {
				require.Error(t, err, "settled retirement alone must not erase an unresolved recovery")
				require.Equal(t, ordinaryBaseline, store.job.State)
			}
		})
	}
}

func TestRetireForClearDoesNotTreatClosedOutcomeAsReceiptAuthority(t *testing.T) {
	for _, mode := range []string{"settled", "not_started", "unknown", "missing", "foreign", "mismatched hash"} {
		t.Run(mode, func(t *testing.T) {
			req, dir := executorEnrollmentFixture(t)
			ctx := context.Background()
			const saved = `{"saved":true}`
			const final = `{"final":9007199254740993}`
			store := &enrollmentStateStore{state: saved, executionTestAuthority: executionTestAuthority{record: &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Phase: "closed", Outcome: "settled"}}}
			outcome := "settled"
			if mode == "not_started" || mode == "unknown" {
				outcome = mode
			}
			if mode != "missing" {
				original := *req
				if mode == "foreign" {
					original.LaunchID = "foreign"
				}
				writeOrdinaryExecutionProof(t, ctx, &original, outcome, final)
			}
			candidate := final
			if mode == "mismatched hash" {
				candidate = saved
			}
			require.NoError(t, storage.WriteFileAtomic(ctx, dir, "state.json", candidate))
			require.NoError(t, utils.CloseExecutionFence(ctx, req))
			a := &AbstractExecutor{db: store, executor: &authorityCleanupExecutor{cleanup: func(context.Context, *types.ExecutionRequest) error {
				t.Fatal("already finalized runtime must not be deleted again")
				return nil
			}}}
			req.Command = types.ClearDestination
			err := a.RetireForClear(ctx, req)
			if mode == "settled" || mode == "not_started" {
				require.NoError(t, err)
				require.Equal(t, types.Sync, req.Command)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, saved, store.state, "closed retry must not reapply a checkpoint")
		})
	}
}
