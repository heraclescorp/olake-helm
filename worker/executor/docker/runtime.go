package docker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Runtime names never reuse the stable workflow's name across admitted launches.
func dockerResourceName(req *types.ExecutionRequest) string {
	if !req.Controlled || !utils.IsAsyncCommand(req.Command) || req.RuntimeID == "" {
		return utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	}
	return fmt.Sprintf("olake-%x", sha256.Sum256([]byte(req.RuntimeID)))
}

func dockerOwnership(req *types.ExecutionRequest) map[string]string {
	return map[string]string{
		"olake.io/workflow-id":    req.WorkflowID,
		"olake.io/runtime-id":     req.RuntimeID,
		"olake.io/launch-id":      req.LaunchID,
		"olake.io/job-id":         strconv.Itoa(req.JobID),
		"olake.io/operation-type": string(req.Command),
	}
}

func validateDockerOwnership(req *types.ExecutionRequest, id string, labels map[string]string) error {
	if id == "" || (req.ResourceID != "" && req.ResourceID != id) {
		return fmt.Errorf("execution outcome unknown: Docker immutable resource identity does not match")
	}
	for key, value := range dockerOwnership(req) {
		if actual, present := labels[key]; !present || actual != value {
			return fmt.Errorf("execution outcome unknown: Docker ownership metadata %s does not match", key)
		}
	}
	return nil
}

func (d *DockerExecutor) findControlledContainer(ctx context.Context, req *types.ExecutionRequest, name string) (string, bool, error) {
	if req.ResourceID != "" {
		name = req.ResourceID
	}
	inspect, err := d.client.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("execution outcome unknown: inspect Docker resource: %w", err)
	}
	if inspect.Container.Config == nil {
		return "", false, fmt.Errorf("execution outcome unknown: Docker ownership metadata is missing")
	}
	id := inspect.Container.ID
	if err := validateDockerOwnership(req, id, inspect.Container.Config.Labels); err != nil {
		return "", false, err
	}
	if req.RecordResource == nil {
		return "", false, fmt.Errorf("execution outcome unknown: Docker resource recorder is missing")
	}
	if err := req.RecordResource(ctx, id, ""); err != nil {
		return "", false, err
	}
	req.ResourceID = id
	return id, true, nil
}

func (d *DockerExecutor) createControlledContainer(ctx context.Context, req *types.ExecutionRequest, config *container.Config, host *container.HostConfig, name string) (string, error) {
	if !req.AllowLaunch || req.BeforeLaunch == nil || req.RecordResource == nil {
		return "", fmt.Errorf("execution outcome unknown: Docker launch is not admitted")
	}
	if err := req.BeforeLaunch(ctx); err != nil {
		return "", err
	}
	created, err := d.client.ContainerCreate(ctx, client.ContainerCreateOptions{Config: config, HostConfig: host, Name: name})
	if err != nil {
		// A conflict or lost acknowledgement cannot authorize starting an existing
		// container. A later attempt may only inspect and attach to its identity.
		return "", fmt.Errorf("execution outcome unknown: create admitted Docker resource: %w", err)
	}
	if created.ID == "" {
		return "", fmt.Errorf("execution outcome unknown: Docker create returned no immutable ID")
	}
	if err := req.RecordResource(ctx, created.ID, ""); err != nil {
		return "", err
	}
	req.ResourceID = created.ID
	if err := req.BeforeLaunch(ctx); err != nil {
		return "", err
	}
	if err := d.startContainer(ctx, created.ID); err != nil {
		return "", err
	}
	return created.ID, nil
}

func (d *DockerExecutor) attachControlledContainer(ctx context.Context, req *types.ExecutionRequest, id, workdir string) (string, error) {
	inspect, err := d.client.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return "", fmt.Errorf("execution outcome unknown: inspect admitted Docker resource: %w", err)
	}
	if inspect.Container.Config == nil {
		return "", fmt.Errorf("execution outcome unknown: Docker ownership metadata is missing")
	}
	if err := validateDockerOwnership(req, inspect.Container.ID, inspect.Container.Config.Labels); err != nil {
		return "", err
	}
	state := inspect.Container.State
	if state == nil || state.Paused || state.Restarting || (!state.Running && state.Status != "exited" && state.Status != "dead") {
		return "", fmt.Errorf("execution outcome unknown: admitted Docker resource has not completed a confirmed start")
	}
	if storage.Mode() == constants.StorageModeS3 {
		if err := utils.AcquireConnectorLogCollector(ctx, workdir, func() (*utils.ConnectorLogCollector, error) {
			return NewContainerLogCollector(ctx, d, id, workdir)
		}); err != nil {
			return "", err
		}
	}
	if state.Running {
		if err := d.waitForContainerCompletion(ctx, id, req.HeartbeatFunc); err != nil {
			return "", err
		}
	} else {
		if err := d.flushExitedConnectorLogs(ctx, workdir, id); err != nil {
			return "", err
		}
		if state.ExitCode != 0 {
			return "", d.containerFailure(ctx, id, state.ExitCode)
		}
	}
	logs, err := d.getContainerLogs(ctx, id, "")
	return string(logs), err
}

func (d *DockerExecutor) cleanupControlledContainer(ctx context.Context, req *types.ExecutionRequest) error {
	resource := req.ResourceID
	knownID := resource != ""
	if !knownID {
		resource = dockerResourceName(req)
	}
	inspect, err := d.client.ContainerInspect(ctx, resource, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		if !knownID {
			return fmt.Errorf("execution outcome unknown: admitted Docker create may still materialize")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect Docker cleanup resource: %w", err)
	}
	if inspect.Container.Config == nil {
		return fmt.Errorf("execution outcome unknown: Docker cleanup ownership metadata is missing")
	}
	if err := validateDockerOwnership(req, inspect.Container.ID, inspect.Container.Config.Labels); err != nil {
		return err
	}
	// Cleanup runs inside the common SQL authority guard: capture locally, never
	// re-enter that guard through RecordResource.
	req.ResourceID = inspect.Container.ID
	return d.stopContainerResource(ctx, req.WorkflowID, inspect.Container.ID)
}

// ResolveResource captures a lost Create acknowledgement without changing the
// provider. The common executor persists this identity before guarded deletion.
func (d *DockerExecutor) ResolveResource(ctx context.Context, req *types.ExecutionRequest) error {
	if !req.Controlled || !utils.IsAsyncCommand(req.Command) {
		return nil
	}
	if req.ResourceID != "" {
		return nil
	}
	if req.RuntimeID == "" {
		return fmt.Errorf("execution outcome unknown: Docker resource resolution requires admitted runtime identity")
	}
	inspect, err := d.client.ContainerInspect(ctx, dockerResourceName(req), client.ContainerInspectOptions{})
	if err != nil {
		return fmt.Errorf("execution outcome unknown: resolve admitted Docker resource: %w", err)
	}
	if inspect.Container.Config == nil {
		return fmt.Errorf("execution outcome unknown: Docker resource ownership metadata is missing")
	}
	if err := validateDockerOwnership(req, inspect.Container.ID, inspect.Container.Config.Labels); err != nil {
		return err
	}
	req.ResourceID = inspect.Container.ID
	return nil
}
