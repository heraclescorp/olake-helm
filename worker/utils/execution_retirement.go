package utils

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

const ExecutionRetirementReservationFile = "execution-retirement-reservation.json"

type ExecutionRetirementReservation struct {
	Version    int    `json:"version"`
	Kind       string `json:"kind"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	RuntimeID  string `json:"runtime_id"`
	LaunchID   string `json:"launch_id"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

func ReadExecutionRetirementReservation(ctx context.Context, req *types.ExecutionRequest) (*ExecutionRetirementReservation, error) {
	_, dir := GetExecutionDirAndSubDir(req)
	data, err := storage.ReadFile(ctx, dir, ExecutionRetirementReservationFile, false)
	if err != nil {
		return nil, err
	}
	var reservation ExecutionRetirementReservation
	if err := json.Unmarshal([]byte(data), &reservation); err != nil {
		return nil, err
	}
	if reservation.Version != 1 || reservation.Kind != "kubernetes-name-reservation" || reservation.WorkflowID != req.WorkflowID || reservation.RunID != req.RunID || reservation.RuntimeID != req.RuntimeID || reservation.LaunchID != req.LaunchID || reservation.Namespace == "" || reservation.Name == "" || reservation.UID == "" || req.RuntimeID == req.LaunchID || req.ResourceID != "" || req.ResourceUID != "" {
		return nil, fmt.Errorf("retirement reservation does not bind exact unresolved recovery runtime")
	}
	return &reservation, nil
}

func WriteExecutionRetirementReservation(ctx context.Context, req *types.ExecutionRequest, reservation ExecutionRetirementReservation) error {
	data, err := json.Marshal(reservation)
	if err != nil {
		return err
	}
	_, dir := GetExecutionDirAndSubDir(req)
	if _, err := storage.CreateFileIfAbsent(ctx, dir, ExecutionRetirementReservationFile, string(data)); err != nil {
		return err
	}
	stored, err := ReadExecutionRetirementReservation(ctx, req)
	if err != nil {
		return err
	}
	if *stored != reservation {
		return fmt.Errorf("immutable retirement reservation conflicts with provider identity")
	}
	return nil
}
