package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

type executionTestAuthority struct {
	record       *database.ExecutionRecord
	prelaunch    bool
	authorityErr error
}

func (db *executionTestAuthority) SealExecution(_ context.Context, req *types.ExecutionRequest) (*database.ExecutionRecord, error) {
	if db.authorityErr != nil {
		return nil, db.authorityErr
	}
	if db.record == nil {
		db.record = &database.ExecutionRecord{Controlled: req.Controlled, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, RuntimeID: req.RuntimeID, LaunchID: req.LaunchID, ResourceID: req.ResourceID, ResourceUID: req.ResourceUID, Phase: "admitted"}
	}
	if db.record.OwnerRunID != req.RunID {
		return nil, database.ErrJobExecutionOwned
	}
	if db.prelaunch {
		db.record.Phase = "closed"
		db.record.Outcome = "not_launched"
	} else if db.record.Phase == "admitted" {
		db.record.Phase = "closing"
	}
	return db.record, nil
}
func (db *executionTestAuthority) GuardExecution(ctx context.Context, req *types.ExecutionRequest, fn func(context.Context, *database.ExecutionRecord) error) error {
	if db.authorityErr != nil {
		return db.authorityErr
	}
	if db.record.OwnerRunID != req.RunID {
		return database.ErrJobExecutionOwned
	}
	return fn(ctx, db.record)
}
func (db *executionTestAuthority) FinishExecution(_ context.Context, _ *types.ExecutionRequest, outcome string) error {
	db.record.Phase = "closed"
	db.record.Outcome = outcome
	return nil
}
func (db *executionTestAuthority) RecordExecutionResource(_ context.Context, _ *types.ExecutionRequest, id, uid string) error {
	db.record.ResourceID = id
	db.record.ResourceUID = uid
	return nil
}

func TestEpochAuthorityPrecedesAnyCleanupOrCheckpointRead(t *testing.T) {
	req, dir := atomicExecutorFixture(t)
	store := &resyncStateStore{state: `{"saved":true}`, executionTestAuthority: executionTestAuthority{authorityErr: database.ErrJobExecutionOwned}}
	calls := 0
	inner := &authorityCleanupExecutor{cleanup: func(context.Context, *types.ExecutionRequest) error { calls++; return nil }}
	a := &AbstractExecutor{executor: inner, db: store}
	require.ErrorIs(t, a.CleanupResync(context.Background(), req, true), database.ErrJobExecutionOwned)
	require.Zero(t, calls)
	_, err := storage.ReadFile(context.Background(), dir, utils.ExecutionFenceFile, false)
	require.Error(t, err)
	require.Equal(t, `{"saved":true}`, store.state)
}

type authorityCleanupExecutor struct {
	cleanup func(context.Context, *types.ExecutionRequest) error
}

func (e *authorityCleanupExecutor) Execute(context.Context, *types.ExecutionRequest, string) (string, error) {
	return "", fmt.Errorf("unexpected launch")
}
func (e *authorityCleanupExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	return e.cleanup(ctx, req)
}
func (e *authorityCleanupExecutor) Close() error { return nil }
func (e *authorityCleanupExecutor) ResolveResource(context.Context, *types.ExecutionRequest) error {
	return nil
}

func TestClosedFenceAcknowledgedBeforeExactResourceDeletion(t *testing.T) {
	req, _ := atomicExecutorFixture(t)
	req.ResourceID = "immutable-container-id"
	require.NoError(t, utils.OpenExecutionFence(context.Background(), req))
	store := &resyncStateStore{state: `{"saved":true}`}
	calls := 0
	inner := &authorityCleanupExecutor{cleanup: func(ctx context.Context, current *types.ExecutionRequest) error {
		calls++
		require.Equal(t, "immutable-container-id", current.ResourceID)
		require.Error(t, utils.ReadExecutionFence(ctx, current), "cleanup must see a positively CLOSED fence")
		return fmt.Errorf("delete acknowledgement unknown")
	}}
	a := &AbstractExecutor{executor: inner, db: store}
	require.Error(t, a.CleanupResync(context.Background(), req, false))
	require.Equal(t, 1, calls)
	require.Equal(t, "closing", store.record.Phase)
	require.Error(t, utils.OpenExecutionFence(context.Background(), req), "a delayed OPEN cannot reopen CLOSED")
}
