package executor

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

type reservationRecoveryStore struct {
	*ordinaryRecoveryStore
	loseAcknowledgement bool
}

func (db *reservationRecoveryStore) RecordExecutionReservation(ctx context.Context, req *types.ExecutionRequest) error {
	if _, err := utils.ReadExecutionRetirementReservation(ctx, req); err != nil {
		return err
	}
	db.record.Phase = "reserved"
	if db.loseAcknowledgement {
		db.loseAcknowledgement = false
		return errors.New("reservation SQL acknowledgement lost")
	}
	return nil
}

type reservationInner struct {
	*enrollmentInnerExecutor
	reservations, cleanups, resolutions int
}

func (e *reservationInner) ResolveResource(context.Context, *types.ExecutionRequest) error {
	e.resolutions++
	return fs.ErrNotExist
}
func (e *reservationInner) Cleanup(context.Context, *types.ExecutionRequest) error {
	e.cleanups++
	return errors.New("reservation must never be cleaned as a connector")
}
func (e *reservationInner) ReserveExecution(ctx context.Context, req *types.ExecutionRequest) (utils.ExecutionRetirementReservation, error) {
	e.reservations++
	if err := utils.RequireClosedExecutionFence(ctx, req); err != nil {
		return utils.ExecutionRetirementReservation{}, err
	}
	return utils.ExecutionRetirementReservation{Version: 1, Kind: "kubernetes-name-reservation", WorkflowID: req.WorkflowID, RunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Namespace: "test", Name: "old-runtime-name", UID: "reservation-uid-not-original-uid"}, nil
}

func TestCanceledPreparationRetiresWithoutProviderOrConnectorReceipt(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	store := &enrollmentStateStore{state: ordinaryBaseline, executionTestAuthority: executionTestAuthority{record: &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Phase: "preparing"}}}
	inner := &reservationInner{enrollmentInnerExecutor: &enrollmentInnerExecutor{}}
	a := &AbstractExecutor{db: store, executor: inner}
	require.NoError(t, a.CleanupAndPersistState(context.Background(), req))
	require.Equal(t, "closed", store.record.Phase)
	require.Equal(t, "not_dispatched", store.record.Outcome)
	require.Equal(t, ordinaryBaseline, store.state)
	require.Zero(t, inner.reservations)
	require.Zero(t, inner.resolutions)
	require.Zero(t, inner.cleanups)
	require.NoError(t, utils.RequireClosedExecutionFence(context.Background(), req))
	_, err := storage.ReadFile(context.Background(), dir, executionResultFile, false)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestLegacyMissingUIDRecoveryReservesNameWithoutClaimingSettlement(t *testing.T) {
	for _, mode := range []string{"valid", "lost SQL acknowledgement", "old run not terminal", "source changed", "destination changed", "origin missing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			a, base, req, _ := ordinaryRecoveryFixture(t)
			require.NoError(t, a.PrepareOrdinaryRecovery(ctx, req))
			req.RunID, req.RuntimeID = "canceled-recovery-run", req.RuntimeID+"-never-acknowledged"
			req.RecoverOnly, req.PriorRunTerminal = true, true
			req.ResourceID, req.ResourceUID = "", ""
			_, dir := utils.GetExecutionDirAndSubDir(req)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
			require.NoError(t, utils.OpenExecutionFence(ctx, req))
			base.record = &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, Outcome: "recovery", Phase: "admitted"}
			store := &reservationRecoveryStore{ordinaryRecoveryStore: base, loseAcknowledgement: mode == "lost SQL acknowledgement"}
			inner := &reservationInner{enrollmentInnerExecutor: &enrollmentInnerExecutor{}}
			a.db, a.executor = store, inner
			switch mode {
			case "old run not terminal":
				req.PriorRunTerminal = false
			case "source changed":
				store.job.Source = `{"host":"different"}`
			case "destination changed":
				store.job.Destination = `{"type":"ICEBERG","writer":{"catalog_type":"rest","database":"different"}}`
			case "origin missing":
				_, stable := utils.GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
				require.NoError(t, os.Remove(stable+"/"+utils.OrdinaryRecoveryOriginFile))
			}
			err := a.PrepareOrdinaryRecovery(ctx, req)
			if mode == "lost SQL acknowledgement" {
				require.Error(t, err)
				err = a.PrepareOrdinaryRecovery(ctx, req)
			}
			if mode != "valid" && mode != "lost SQL acknowledgement" {
				require.Error(t, err)
				require.Zero(t, inner.reservations)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "reserved", store.record.Phase)
			require.Equal(t, "recovery", store.record.Outcome)
			require.Empty(t, req.ResourceID)
			require.Empty(t, req.ResourceUID)
			require.Empty(t, store.record.ResourceID)
			require.Empty(t, store.record.ResourceUID)
			require.Equal(t, ordinaryBaseline, store.job.State)
			require.Zero(t, store.completions)
			require.Zero(t, inner.cleanups)
			_, err = storage.ReadFile(ctx, dir, executionResultFile, false)
			require.ErrorIs(t, err, fs.ErrNotExist)
			require.Error(t, a.CleanupAndPersistState(ctx, req), "reservation is not settlement")
			clear := *req
			clear.Command = types.ClearDestination
			require.Error(t, a.RetireForClear(ctx, &clear), "reservation never authorizes clear")
			require.Equal(t, "reserved", store.record.Phase)
		})
	}
}
