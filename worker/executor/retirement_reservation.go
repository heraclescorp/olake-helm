package executor

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

func (a *AbstractExecutor) reserveOrdinaryRecovery(ctx context.Context, req *types.ExecutionRequest) error {
	provider, ok := a.executor.(interface {
		ReserveExecution(context.Context, *types.ExecutionRequest) (utils.ExecutionRetirementReservation, error)
	})
	if !ok {
		return fmt.Errorf("provider cannot positively reserve an unresolved runtime")
	}
	store, ok := a.db.(interface {
		GetJobData(context.Context, int) (types.JobData, error)
		RecordExecutionReservation(context.Context, *types.ExecutionRequest) error
	})
	if !ok {
		return fmt.Errorf("runtime reservation requires durable recovery authority")
	}
	if err := a.db.GuardExecution(ctx, req, func(ctx context.Context, record *database.ExecutionRecord) error {
		if !req.PriorRunTerminal || !record.Controlled || record.Outcome != "recovery" || (record.Phase != "closing" && record.Phase != "reserved") || record.RuntimeID == record.LaunchID || record.ResourceID != "" || record.ResourceUID != "" || req.ResourceID != "" || req.ResourceUID != "" {
			return fmt.Errorf("runtime reservation requires sealed unresolved recovery and a positively terminal old run")
		}
		if err := utils.RequireClosedExecutionFence(ctx, req); err != nil {
			return err
		}
		origin, _, files, err := utils.ReadOrdinaryRecoveryOrigin(ctx, req)
		if err != nil {
			return err
		}
		job, err := store.GetJobData(ctx, req.JobID)
		if err != nil {
			return err
		}
		if err := utils.ValidateOrdinaryRecoveryJob(job, files, origin.SavedCheckpointSHA256()); err != nil {
			return err
		}
		reservation, err := provider.ReserveExecution(ctx, req)
		if err != nil {
			return err
		}
		return utils.WriteExecutionRetirementReservation(ctx, req, reservation)
	}); err != nil {
		return err
	}
	return store.RecordExecutionReservation(ctx, req)
}
