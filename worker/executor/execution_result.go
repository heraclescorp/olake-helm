package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

const executionResultFile = "execution-result.json"

type executionResult struct {
	Version     int    `json:"version"`
	ExecutionID string `json:"execution_id"`
	LaunchID    string `json:"launch_id"`
	Command     string `json:"command"`
	Outcome     string `json:"outcome"`
	StateSHA256 string `json:"state_sha256"`
}

// Process exit/absence is not evidence that remotely dispatched mutations have
// settled. Only the terminal source/JVM receipt can authorize ordinary release.
func confirmedExecutionResult(ctx context.Context, req *types.ExecutionRequest) (state *string, outcome string, err error) {
	if req.Command != types.Sync && req.Command != types.ClearDestination {
		return nil, "", fmt.Errorf("ordinary retirement requires sync or clear-destination")
	}
	_, dir := utils.GetExecutionDirAndSubDir(req)
	data, err := storage.ReadFile(ctx, dir, executionResultFile, false)
	if err != nil {
		return nil, "", fmt.Errorf("execution retirement is unknown; retain job ownership: %w", err)
	}
	var proof executionResult
	if err := json.Unmarshal([]byte(data), &proof); err != nil {
		return nil, "", fmt.Errorf("invalid execution retirement receipt: %w", err)
	}
	if proof.Version != 1 || req.RuntimeID == "" || req.LaunchID == "" || proof.ExecutionID != req.RuntimeID || proof.LaunchID != req.LaunchID || proof.Command != string(req.Command) {
		return nil, "", fmt.Errorf("execution retirement receipt does not bind admitted runtime, launch and command")
	}
	switch proof.Outcome {
	case "not_started":
		return nil, proof.Outcome, nil
	case "settled":
		finalState, err := storage.ReadFile(ctx, dir, "state.json", false)
		if err != nil {
			return nil, "", err
		}
		digest, err := utils.StateSHA256(finalState)
		if err != nil {
			return nil, "", err
		}
		if proof.StateSHA256 == "" || proof.StateSHA256 != digest {
			return nil, "", fmt.Errorf("settled execution receipt does not bind the complete final checkpoint")
		}
		return &finalState, proof.Outcome, nil
	default:
		return nil, "", fmt.Errorf("execution mutation outcome is not positively settled; retain job ownership")
	}
}
