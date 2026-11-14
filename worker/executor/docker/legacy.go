package docker

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/moby/moby/client"
)

// shouldStartOperation preserves the ordinary asynchronous lifecycle outside the
// canonical controlled domain. Resync never reaches this legacy path.
func (d *DockerExecutor) shouldStartOperation(ctx context.Context, req *types.ExecutionRequest, containerName, workDir string) (*types.Result, error) {
	log := logger.Log(ctx)
	state := d.getContainerState(ctx, containerName, req.WorkflowID)

	// Adopt running legacy resources without sending a second ContainerStart.
	if state.Exists && state.Running {
		log.Info("adopting running container", "workflowID", req.WorkflowID, "containerName", containerName)
		if storage.Mode() == constants.StorageModeS3 {
			err := utils.AcquireConnectorLogCollector(ctx, workDir, func() (*utils.ConnectorLogCollector, error) {
				return NewContainerLogCollector(ctx, d, containerName, workDir)
			})
			if err != nil {
				return nil, fmt.Errorf("failed to start connector log collector: %s", err)
			}
		}
		if err := d.waitForContainerCompletion(ctx, containerName, req.HeartbeatFunc); err != nil {
			return nil, err
		}
		state = d.getContainerState(ctx, containerName, req.WorkflowID)
	}

	if state.Exists && !state.Running {
		if err := d.flushExitedConnectorLogs(ctx, workDir, containerName); err != nil {
			return nil, err
		}
	}

	if state.Exists && !state.Running && state.ExitCode != nil {
		log.Info("container exited", "workflowID", req.WorkflowID, "containerName", containerName, "exitCode", *state.ExitCode)
		if *state.ExitCode == 0 {
			return &types.Result{OK: false, Message: "sync status: completed"}, nil
		}
		if req.Command == types.ClearDestination {
			log.Info("removing old container for clear-destination", "workflowID", req.WorkflowID, "containerName", containerName)
			if _, err := d.client.ContainerRemove(ctx, containerName, client.ContainerRemoveOptions{Force: true}); err != nil {
				log.Error("failed to remove old container", "containerName", containerName, "error", err)
				return nil, fmt.Errorf("failed to remove old container: %w", err)
			}
			return &types.Result{OK: true}, nil
		}
		return nil, d.containerFailure(ctx, containerName, *state.ExitCode)
	}

	alreadyLaunched, err := utils.WorkflowAlreadyLaunched(ctx, workDir)
	if err != nil {
		return nil, err
	}
	if !alreadyLaunched {
		return &types.Result{OK: true}, nil
	}
	log.Info("container already handled, skipping launch", "workflowID", req.WorkflowID, "containerName", containerName)
	return &types.Result{OK: false, Message: "sync status: skipped"}, nil
}
