package executor

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/acarl005/stripansi"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor/docker"
	"github.com/datazip-inc/olake-helm/worker/executor/kubernetes"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// Executor interface for k8s and docker executor
type Executor interface {
	Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error)
	Cleanup(ctx context.Context, req *types.ExecutionRequest) error
	ResolveResource(context.Context, *types.ExecutionRequest) error
	Close() error
}

// FailureIndicator spawns or deletes GitOps failure-indicator pods/containers.
type FailureIndicator interface {
	Indicator(ctx context.Context, req *types.IndicatorRequest) error
}

type jobStateStore interface {
	UpdateJobState(context.Context, int, string, string, string) error
	SealExecution(context.Context, *types.ExecutionRequest) (*database.ExecutionRecord, error)
	GuardExecution(context.Context, *types.ExecutionRequest, func(context.Context, *database.ExecutionRecord) error) error
	FinishExecution(context.Context, *types.ExecutionRequest, string) error
	RecordExecutionResource(context.Context, *types.ExecutionRequest, string, string) error
}

type AbstractExecutor struct {
	executor  Executor
	indicator FailureIndicator
	db        jobStateStore
}

// NewExecutor creates and returns the executor client based on the executor environment
func NewExecutor(ctx context.Context, db *database.DB) (*AbstractExecutor, error) {
	executorEnv := utils.GetExecutorEnvironment()
	switch executorEnv {
	case string(types.Docker):
		d, err := docker.NewDockerExecutor()
		if err != nil {
			return nil, err
		}
		return &AbstractExecutor{executor: d, indicator: d, db: db}, nil
	case string(types.Kubernetes):
		k, err := kubernetes.NewKubernetesExecutor(ctx)
		if err != nil {
			return nil, err
		}
		return &AbstractExecutor{executor: k, indicator: k, db: db}, nil
	default:
		return nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}
}

func (a *AbstractExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	if req.Command == types.Resync && !req.Controlled {
		return nil, fmt.Errorf("atomic resync is outside the canonical supported domain")
	}
	if utils.IsAsyncCommand(req.Command) && req.Controlled && req.RuntimeID == "" {
		return nil, fmt.Errorf("controlled asynchronous execution requires durable runtime identity")
	}
	utils.SetExecutionArgs(req)
	log := logger.Log(ctx)
	subdir, workdir := utils.GetExecutionDirAndSubDir(req)

	// Only the admitted launcher initializes its immutable runtime namespace.
	// Attach retries never rewrite state/config/proof inputs of a live runtime.
	if req.Controlled {
		if req.AllowLaunch {
			if req.BeforeLaunch == nil {
				return nil, fmt.Errorf("admitted launcher requires authoritative launch guard")
			}
			if err := req.BeforeLaunch(ctx); err != nil {
				return nil, err
			}
			if err := storage.WriteFiles(ctx, workdir, req.Configs); err != nil {
				return nil, err
			}
		}
	} else {
		launched, err := utils.WorkflowAlreadyLaunched(ctx, workdir)
		if err != nil {
			return nil, err
		}
		if !launched && req.Configs != nil {
			if err := storage.WriteFiles(ctx, workdir, req.Configs); err != nil {
				return nil, err
			}
		}
	}

	output, err := a.executor.Execute(ctx, req, workdir)
	if err != nil {
		// SyncActivity explains sync failures itself; log only the other commands
		if req.Command != types.Sync {
			log.Error("executor failed", "command", req.Command, "error", err)
		}
		return nil, err
	}
	if req.Command != types.Sync && req.Command != types.DiscoverNewTables {
		log.Info("executor output", "environment", utils.GetExecutorEnvironment(), "output", stripansi.Strip(output))
	}

	// generated file as response
	if req.OutputFile != "" {
		filePath := filepath.Join(subdir, req.OutputFile)
		return &types.ExecutorResponse{Response: filePath}, nil
	}

	outputJSON, err := utils.ExtractJSONAndMarshal(output)
	if err != nil {
		log.Error("failed to extract JSON from output", "error", err)
		return nil, err
	}

	outputFile := []types.JobConfig{{Name: constants.OutputFileName, Data: string(outputJSON)}}
	if err := storage.WriteFiles(ctx, workdir, outputFile); err != nil {
		log.Error("failed to write output file", "workdir", workdir, "filename", constants.OutputFileName, "error", err)
		return nil, err
	}

	// logs as response
	return &types.ExecutorResponse{Response: filepath.Join(subdir, constants.OutputFileName)}, nil
}

// Retirement is serialized against epoch adoption and API mutations. SQL seals
// admission before CLOSED is acknowledged, then cleanup addresses only the
// provider identity persisted by that admitted runtime.
func (a *AbstractExecutor) RetireExecution(ctx context.Context, req *types.ExecutionRequest) error {
	r, err := a.prepareRetirement(ctx, req)
	if err != nil {
		return err
	}
	if r.Phase == "closed" || r.Phase == "handoff" || r.Phase == "handed_off" {
		return nil
	}
	return a.db.GuardExecution(ctx, req, func(ctx context.Context, current *database.ExecutionRecord) error {
		return a.executor.Cleanup(ctx, req)
	})
}

func (a *AbstractExecutor) prepareRetirement(ctx context.Context, req *types.ExecutionRequest) (*database.ExecutionRecord, error) {
	r, err := a.db.SealExecution(ctx, req)
	if err != nil {
		return nil, err
	}
	applyRecord(req, r)
	if r.Phase == "closed" || r.Phase == "handoff" || r.Phase == "handed_off" {
		return r, nil
	}
	if !r.Controlled {
		return r, nil
	}
	err = a.db.GuardExecution(ctx, req, func(ctx context.Context, current *database.ExecutionRecord) error {
		if err := utils.CloseExecutionFence(ctx, req); err != nil {
			return err
		}
		return a.executor.ResolveResource(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	// Persist discovered immutable identity before deletion, so lost deletion or
	// final SQL acknowledgements never require a name-only cleanup retry.
	if req.ResourceID != "" && (r.ResourceID != req.ResourceID || r.ResourceUID != req.ResourceUID) {
		if err := a.db.RecordExecutionResource(ctx, req, req.ResourceID, req.ResourceUID); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func applyRecord(req *types.ExecutionRequest, r *database.ExecutionRecord) {
	req.RuntimeID, req.LaunchID = r.RuntimeID, r.LaunchID
	req.ResourceID, req.ResourceUID = r.ResourceID, r.ResourceUID
	req.LauncherAttemptID = r.LauncherAttemptID
	req.RecoverOnly = r.Outcome == "recovery"
	req.Controlled = r.Controlled
}

func (a *AbstractExecutor) CleanupAndPersistState(ctx context.Context, req *types.ExecutionRequest) error {
	if req.Command == types.Resync {
		return fmt.Errorf("resync requires confirmed publication")
	}
	r, err := a.prepareRetirement(ctx, req)
	if err != nil {
		return err
	}
	if r.Phase == "closed" {
		return nil
	}
	outcome := ""
	err = a.db.GuardExecution(ctx, req, func(ctx context.Context, current *database.ExecutionRecord) error {
		applyRecord(req, current)
		if req.Controlled {
			if err := utils.CloseExecutionFence(ctx, req); err != nil {
				return err
			}
		}
		if err := a.executor.Cleanup(ctx, req); err != nil {
			return err
		}
		if !req.Controlled {
			_, dir := utils.GetExecutionDirAndSubDir(req)
			state, err := storage.ReadFile(ctx, dir, "state.json", true)
			if err != nil {
				return err
			}
			outcome = "legacy_completed"
			return a.db.UpdateJobState(ctx, req.JobID, req.WorkflowID, req.RunID, state)
		}
		state, confirmedOutcome, err := confirmedExecutionResult(ctx, req)
		if err != nil {
			return err
		}
		outcome = confirmedOutcome
		if state == nil {
			return nil
		}
		return a.db.UpdateJobState(ctx, req.JobID, req.WorkflowID, req.RunID, *state)
	})
	if err != nil {
		return err
	}
	return a.db.FinishExecution(ctx, req, outcome)
}

func (a *AbstractExecutor) FailureIndicator() FailureIndicator {
	return a.indicator
}

func (a *AbstractExecutor) Close() {
	a.executor.Close()
}

// RecoverWorkerLogs uploads worker logs from the previous container/pod on startup (S3 mode).
func (a *AbstractExecutor) RecoverWorkerLogs(ctx context.Context) error {
	if storage.Mode() != constants.StorageModeS3 {
		return nil
	}
	switch e := a.executor.(type) {
	case *docker.DockerExecutor:
		return docker.RecoverWorkerLogs(ctx, e)
	case *kubernetes.KubernetesExecutor:
		return kubernetes.RecoverWorkerLogs(ctx, e)
	default:
		return nil
	}
}
