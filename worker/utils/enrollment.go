package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

func readEnrollmentGate(ctx context.Context, req *types.ExecutionRequest) (*bool, error) {
	_, workDir := GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	data, err := storage.ReadFile(ctx, workDir, constants.EnrollmentGateFile, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var gate struct {
		IngestionStarted *bool `json:"ingestion_started"`
	}
	if err := json.Unmarshal([]byte(data), &gate); err != nil || gate.IngestionStarted == nil {
		return nil, fmt.Errorf("invalid automatic table enrollment execution gate")
	}
	return gate.IngestionStarted, nil
}

// BeginEnrollmentPreflight must not turn a previously launched retry back into
// a preflight-only execution. The marker lives in ordinary sync storage, never
// the isolated discovery directory, and works identically with NFS and S3.
func BeginEnrollmentPreflight(ctx context.Context, req *types.ExecutionRequest) error {
	gate, err := readEnrollmentGate(ctx, req)
	if err != nil || gate != nil {
		return err
	}
	_, workDir := GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	launched, err := WorkflowAlreadyLaunched(ctx, workDir)
	if err != nil || launched {
		return err
	}
	return storage.WriteFiles(ctx, workDir, []types.JobConfig{{
		Name: constants.EnrollmentGateFile, Data: `{"ingestion_started":false}`,
	}})
}

// MarkEnrollmentIngestionStarted is called only after sync configs/state are
// written, before invoking the ordinary executor. Jobs without a gate are unchanged.
func MarkEnrollmentIngestionStarted(ctx context.Context, req *types.ExecutionRequest) error {
	gate, err := readEnrollmentGate(ctx, req)
	if err != nil || gate == nil || *gate {
		return err
	}
	_, workDir := GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	return storage.WriteFiles(ctx, workDir, []types.JobConfig{{
		Name: constants.EnrollmentGateFile, Data: `{"ingestion_started":true}`,
	}})
}

func EnrollmentPreflightPending(ctx context.Context, req *types.ExecutionRequest) (bool, error) {
	gate, err := readEnrollmentGate(ctx, req)
	return gate != nil && !*gate, err
}
