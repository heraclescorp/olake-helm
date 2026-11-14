package docker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/failure"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/registry"
	"github.com/moby/moby/client"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	DockerPullTimeout       = 2 * time.Minute
	DockerHeartbeatInterval = 5 * time.Second
)

type ContainerState struct {
	Exists   bool
	Running  bool
	ExitCode *int
}

func (d *DockerExecutor) PullImage(ctx context.Context, imageName, version string, heartbeatFunc func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	_, err := d.client.ImageInspect(ctx, imageName)
	if err != nil {
		pullCtx, cancel := context.WithTimeout(ctx, DockerPullTimeout)
		defer cancel()

		// Image doesn't exist, pull it
		log.Info(fmt.Sprintf("Pulling image %s", imageName))
		pullStart := time.Now()

		done := make(chan struct{})
		defer close(done)

		if heartbeatFunc != nil {
			go func() {
				ticker := time.NewTicker(DockerHeartbeatInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						heartbeatFunc(ctx, fmt.Sprintf("pulling image %s", imageName))
					case <-done:
						return
					}
				}
			}()
		}

		reader, err := d.client.ImagePull(pullCtx, imageName, client.ImagePullOptions{RegistryAuth: registryAuth()})
		if err != nil {
			if errors.Is(pullCtx.Err(), context.DeadlineExceeded) {
				log.Error("image pull timed out", "image", imageName)
				return fmt.Errorf("image pull for %s timed out", imageName)
			}
			log.Error("image pull failed", "image", imageName, "error", err)
			return fmt.Errorf("image pull %s: %s", imageName, err)
		}
		defer reader.Close()

		if _, err = io.Copy(io.Discard, reader); err != nil {
			if errors.Is(pullCtx.Err(), context.DeadlineExceeded) {
				log.Error("image pull timed out", "image", imageName)
				return fmt.Errorf("image pull for %s timed out", imageName)
			}
			log.Warn("failed to read image pull output", "image", imageName, "error", err)
		}
		log.Info(fmt.Sprintf("Pulled image %s in %s", imageName, time.Since(pullStart).Round(time.Second)))
		return nil
	}

	log.Info(fmt.Sprintf("Using image %s (found in local cache)", imageName))
	return nil
}

// getOrCreateContainer creates a container or returns the ID of an existing one
func (d *DockerExecutor) getOrCreateContainer(ctx context.Context, containerConfig *container.Config, hostConfig *container.HostConfig, containerName string) (string, error) {
	log := logger.Log(ctx)
	resp, err := d.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     containerConfig,
		HostConfig: hostConfig,
		Name:       containerName,
	})
	if err != nil {
		if errdefs.IsAlreadyExists(err) || errdefs.IsConflict(err) {
			log.Info("container already exists, resuming", "containerName", containerName)
			return containerName, nil
		}

		log.Error("failed to create container", "containerName", containerName, "error", err)
		return "", fmt.Errorf("failed to create container: %s", err)
	}

	log.Debug("created container", "containerName", containerName, "containerID", resp.ID)
	return resp.ID, nil
}

// getContainerLogs retrieves and properly parses logs from a container using stdcopy;
// tail is the number of last lines to return, "" means all of them.
func (d *DockerExecutor) getContainerLogs(ctx context.Context, containerID, tail string) ([]byte, error) {
	reader, err := d.client.ContainerLogs(ctx, containerID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       tail,
	})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdoutBuf, &stderrBuf, reader); err != nil {
		return nil, err
	}

	// Prefer stdout, but include stderr if present
	if stderrBuf.Len() > 0 && stdoutBuf.Len() == 0 {
		return stderrBuf.Bytes(), nil
	}
	if stderrBuf.Len() > 0 {
		return append(stdoutBuf.Bytes(), []byte("\n"+stderrBuf.String())...), nil
	}
	return stdoutBuf.Bytes(), nil
}

// containerFailure describes a container that exited with a non-zero code.
func (d *DockerExecutor) containerFailure(ctx context.Context, containerID string, exitCode int) *failure.ExecutionFailure {
	containerFailure := &failure.ExecutionFailure{
		Kind:     failure.KindContainer,
		Name:     containerID,
		ExitCode: &exitCode,
	}
	if tail, err := d.getContainerLogs(ctx, containerID, strconv.Itoa(failure.LogTailLines)); err == nil {
		containerFailure.LogTail = string(tail)
	}

	inspect, err := d.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil {
		return containerFailure
	}
	// report the container name (as in the other worker lines), not its ID
	if name := strings.TrimPrefix(inspect.Container.Name, "/"); name != "" {
		containerFailure.Name = name
	}
	if state := inspect.Container.State; state != nil {
		// report docker's OOM flag the way kubernetes does
		if state.OOMKilled {
			containerFailure.Reason = failure.ReasonOOMKilled
		}
		containerFailure.Message = state.Error
	}
	if hostConfig := inspect.Container.HostConfig; hostConfig != nil && hostConfig.Memory > 0 {
		containerFailure.MemoryLimit = resource.NewQuantity(hostConfig.Memory, resource.BinarySI).String()
	}
	return containerFailure
}

// getContainerState inspects a container and returns its state
func (d *DockerExecutor) getContainerState(ctx context.Context, name, workflowID string) ContainerState {
	log := logger.Log(ctx)
	inspect, err := d.client.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil || inspect.Container.State == nil {
		log.Debug("container inspect failed or state missing", "workflowID", workflowID, "containerName", name, "error", err)
		return ContainerState{Exists: false}
	}

	running := inspect.Container.State.Running
	var ec *int
	if !running && (inspect.Container.State.ExitCode != 0 || inspect.Container.State.Status == "exited") {
		code := inspect.Container.State.ExitCode
		ec = &code
	}
	return ContainerState{Exists: true, Running: running, ExitCode: ec}
}

// StopContainer preserves name-based cleanup for unmanaged executions.
func (d *DockerExecutor) StopContainer(ctx context.Context, workflowID string) error {
	return d.stopContainerResource(ctx, workflowID, utils.WorkflowHash(workflowID))
}

// stopContainerResource addresses an immutable ID for controlled executions.
func (d *DockerExecutor) stopContainerResource(ctx context.Context, workflowID, containerName string) error {
	log := logger.Log(ctx)
	log.Info("stop request received for container", "workflowID", workflowID, "containerName", containerName)

	if strings.TrimSpace(containerName) == "" {
		log.Warn("empty container name", "workflowID", workflowID)
		return fmt.Errorf("empty container name")
	}

	// Graceful stop with timeout
	timeout := constants.ContainerStopTimeout
	if _, err := d.client.ContainerStop(ctx, containerName, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		log.Warn("docker stop failed, attempting kill", "workflowID", workflowID, "containerName", containerName, "error", err)
		if _, kerr := d.client.ContainerKill(ctx, containerName, client.ContainerKillOptions{Signal: "SIGKILL"}); kerr != nil {
			if errdefs.IsNotFound(kerr) {
				return nil
			}
			log.Error("docker kill failed", "workflowID", workflowID, "containerName", containerName, "error", kerr)
			return fmt.Errorf("docker kill failed: %s", kerr)
		}
	}

	// Remove container
	if _, err := d.client.ContainerRemove(ctx, containerName, client.ContainerRemoveOptions{Force: true}); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		log.Error("docker rm failed", "workflowID", workflowID, "containerName", containerName, "error", err)
		return fmt.Errorf("workflowID %s: docker rm failed for %s: %s", workflowID, containerName, err)
	}

	log.Info("container removed successfully", "workflowID", workflowID, "containerName", containerName)
	return nil
}

// registryAuth returns the base64url-encoded registry credentials for image pulls,
// built from CONTAINER_REGISTRY_USERNAME/PASSWORD. When unset it returns "", so the
// Docker daemon falls back to its own credential store (host `docker login` / IAM) -
// preserving existing behavior for Docker Hub, ECR, and GCR.
func registryAuth() string {
	username := strings.TrimSpace(viper.GetString(constants.EnvRegistryUsername))
	password := strings.TrimSpace(viper.GetString(constants.EnvRegistryPassword))
	if username == "" && password == "" {
		return ""
	}

	authConfig := registry.AuthConfig{
		Username:      username,
		Password:      password,
		ServerAddress: strings.TrimSpace(viper.GetString(constants.ContainerRegistryBase)),
	}
	encoded, err := json.Marshal(authConfig)
	if err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(encoded)
}

func (d *DockerExecutor) startContainer(ctx context.Context, containerID string) error {
	log := logger.Log(ctx)
	_, err := d.client.ContainerStart(ctx, containerID, client.ContainerStartOptions{})
	if err != nil && !errdefs.IsAlreadyExists(err) {
		log.Error("failed to start container", "containerID", containerID, "error", err)
		return fmt.Errorf("failed to start container %s: %s", containerID, err)
	}
	log.Debug("container started", "containerID", containerID)
	return nil
}

func (d *DockerExecutor) waitForContainerCompletion(ctx context.Context, containerID string, heartbeatFunc func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	waitResult := d.client.ContainerWait(ctx, containerID, client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	statusCh, errCh := waitResult.Result, waitResult.Error

	for {
		if heartbeatFunc != nil {
			heartbeatFunc(ctx, fmt.Sprintf("waiting for container %s", containerID))
		}

		select {
		case <-ctx.Done():
			log.Debug("context cancelled while waiting for container", "containerID", containerID)
			return ctx.Err()

		case status := <-statusCh:
			if status.StatusCode != 0 {
				return d.containerFailure(ctx, containerID, int(status.StatusCode))
			}
			return nil

		case err := <-errCh:
			if err != nil {
				// CRITICAL: Check if error is because context was cancelled
				if ctx.Err() != nil {
					log.Info("container wait failed due to context cancellation", "containerID", containerID, "dockerError", err)
					return ctx.Err() // Return cancellation error, not docker error
				}
				log.Error("error waiting for container", "containerID", containerID, "error", err)
				return fmt.Errorf("error waiting for container %s: %s", containerID, err)
			}
			return nil

		case <-time.After(5 * time.Second):
			// continue
		}
	}
}
