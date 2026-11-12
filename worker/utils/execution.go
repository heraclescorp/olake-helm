package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

const ExecutionFenceFile = "execution-fence.json"
const ExecutionPointerFile = "execution-runtime.json"

type ExecutionFence struct {
	Version     int    `json:"version"`
	ExecutionID string `json:"execution_id"`
	State       string `json:"state"`
}

func ExecutionIdentity(req *types.ExecutionRequest) string {
	if req.Controlled && req.RuntimeID != "" {
		return req.RuntimeID
	}
	return req.WorkflowID
}

func GetExecutionDirAndSubDir(req *types.ExecutionRequest) (string, string) {
	return GetWorkflowDirAndSubDir(ExecutionIdentity(req), req.Command)
}

func StateSHA256(state string) (string, error) {
	var object map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(state))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || object == nil {
		return "", fmt.Errorf("checkpoint must be a JSON object")
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return "", fmt.Errorf("checkpoint has trailing JSON")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func ReadExecutionFence(ctx context.Context, req *types.ExecutionRequest) error {
	if !req.Controlled || req.RuntimeID == "" {
		return fmt.Errorf("positive execution fence requires controlled runtime identity")
	}
	_, dir := GetExecutionDirAndSubDir(req)
	data, err := storage.ReadFile(ctx, dir, ExecutionFenceFile, false)
	if err != nil {
		return fmt.Errorf("execution fence unavailable; refusing mutation: %w", err)
	}
	var fence ExecutionFence
	if err := json.Unmarshal([]byte(data), &fence); err != nil {
		return fmt.Errorf("invalid execution fence: %w", err)
	}
	if fence.Version != 1 || fence.ExecutionID != req.RuntimeID || fence.State != "open" {
		return fmt.Errorf("execution fence is not positively open for runtime %s", req.RuntimeID)
	}
	return nil
}

func OpenExecutionFence(ctx context.Context, req *types.ExecutionRequest) error {
	if !req.Controlled || req.RuntimeID == "" {
		return fmt.Errorf("execution fence requires controlled runtime identity")
	}
	_, dir := GetExecutionDirAndSubDir(req)
	data, _ := json.Marshal(ExecutionFence{Version: 1, ExecutionID: req.RuntimeID, State: "open"})
	if _, err := storage.CreateFileIfAbsent(ctx, dir, ExecutionFenceFile, string(data)); err != nil {
		return err
	}
	return ReadExecutionFence(ctx, req)
}

func CloseExecutionFence(ctx context.Context, req *types.ExecutionRequest) error {
	if req.RuntimeID == "" {
		return fmt.Errorf("cannot close unidentified execution")
	}
	_, dir := GetExecutionDirAndSubDir(req)
	data, _ := json.Marshal(ExecutionFence{Version: 1, ExecutionID: req.RuntimeID, State: "closed"})
	return storage.WriteFileAtomic(ctx, dir, ExecutionFenceFile, string(data))
}

func SetExecutionArgs(req *types.ExecutionRequest) {
	filtered := req.Args[:0]
	for i := 0; i < len(req.Args); i++ {
		flag, _, inline := strings.Cut(req.Args[i], "=")
		switch flag {
		case "--execution-id", "--execution-fence", "--resync-launch-id":
			if !inline && i+1 < len(req.Args) {
				i++
			}
		case "--resync-recover-only":
		default:
			filtered = append(filtered, req.Args[i])
		}
	}
	req.Args = filtered
	if !req.Controlled || req.RuntimeID == "" {
		return
	}
	_, dir := GetExecutionDirAndSubDir(req)
	req.Args = append(req.Args, "--execution-id", req.RuntimeID, "--execution-fence", storage.ExecutionFenceLocation(dir, ExecutionFenceFile))
	if req.Command == types.Resync {
		req.Args = append(req.Args, "--resync-launch-id", req.LaunchID)
		if req.RecoverOnly {
			req.Args = append(req.Args, "--resync-recover-only")
		}
	}
}

// The stable workflow directory retains worker logs. Readers follow this pointer
// for connector logs and artifacts; each runtime writes only its own directory.
func WriteExecutionPointer(ctx context.Context, req *types.ExecutionRequest) error {
	_, dir := GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	subdir, _ := GetExecutionDirAndSubDir(req)
	data, err := json.Marshal(map[string]interface{}{"version": 1, "workflow_id": req.WorkflowID, "run_id": req.RunID, "runtime_id": req.RuntimeID, "launch_id": req.LaunchID, "subdir": subdir})
	if err != nil {
		return err
	}
	if _, err := storage.CreateFileIfAbsent(ctx, dir, "execution-runtime-"+req.RuntimeID+".json", string(data)); err != nil {
		return err
	}
	return storage.WriteFileAtomic(ctx, dir, ExecutionPointerFile, string(data))
}
