package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/moby/moby/api/types/container"
	"github.com/spf13/viper"
)

func TestLegacyDockerOrdinaryAsyncCommandsNeedNoRuntime(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	for _, command := range []types.Command{types.Sync, types.ClearDestination} {
		t.Run(string(command), func(t *testing.T) {
			req := &types.ExecutionRequest{WorkflowID: "ordinary-mysql-operation", Command: command, ConnectorType: "mysql", Version: "v1", Args: []string{string(command)}}
			starts, removes := 0, 0
			d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
				path := strings.TrimPrefix(r.URL.Path, "/v1.54")
				switch {
				case strings.HasPrefix(path, "/images/"):
					return runtimeResponse(http.StatusOK, map[string]string{"Id": "image"}), nil
				case r.Method == http.MethodGet && strings.HasSuffix(path, "/json"):
					if path != "/containers/"+utils.GetWorkflowDirectory(command, req.WorkflowID)+"/json" {
						t.Errorf("legacy lookup name = %s", path)
					}
					return runtimeResponse(http.StatusNotFound, map[string]string{"message": "not found"}), nil
				case path == "/containers/create":
					if r.URL.Query().Get("name") != utils.GetWorkflowDirectory(command, req.WorkflowID) {
						t.Errorf("legacy create name = %q", r.URL.Query().Get("name"))
					}
					var config container.Config
					if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
						t.Fatal(err)
					}
					if config.Labels["olake.io/runtime-id"] != "" {
						t.Fatal("legacy container acquired controlled ownership metadata")
					}
					return runtimeResponse(http.StatusCreated, map[string]string{"Id": "legacy-id"}), nil
				case strings.HasSuffix(path, "/start"):
					starts++
					return runtimeResponse(http.StatusNoContent, nil), nil
				case strings.HasSuffix(path, "/wait"):
					return runtimeResponse(http.StatusOK, map[string]int{"StatusCode": 0}), nil
				case strings.HasSuffix(path, "/logs"):
					return runtimeLogResponse(), nil
				case strings.HasSuffix(path, "/stop"):
					if path != "/containers/"+utils.WorkflowHash(req.WorkflowID)+"/stop" {
						t.Errorf("legacy stop name = %s", path)
					}
					return runtimeResponse(http.StatusNoContent, nil), nil
				case r.Method == http.MethodDelete:
					removes++
					if path != "/containers/"+utils.WorkflowHash(req.WorkflowID) {
						t.Errorf("legacy cleanup name = %s", path)
					}
					return runtimeResponse(http.StatusNoContent, nil), nil
				default:
					return nil, errors.New("unexpected legacy request: " + path)
				}
			})
			logs, err := d.Execute(context.Background(), req, t.TempDir())
			if err != nil || starts != 1 || removes != 0 || logs != "original runtime logs" {
				t.Fatalf("legacy launch: logs=%q err=%v starts=%d removes=%d", logs, err, starts, removes)
			}
			if err := d.Cleanup(context.Background(), req); err != nil || removes != 1 {
				t.Fatalf("legacy cleanup: err=%v removes=%d", err, removes)
			}
		})
	}
}

func TestLegacyDockerCompletedRetryKeepsOriginalSkipBehavior(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	req := &types.ExecutionRequest{WorkflowID: "ordinary-completed", Command: types.Sync, ConnectorType: "mysql", Version: "v1"}
	d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/json") {
			t.Errorf("completed legacy retry mutated provider: %s %s", r.Method, r.URL.Path)
		}
		return runtimeResponse(http.StatusOK, map[string]interface{}{"State": map[string]interface{}{"Status": "exited", "Running": false, "ExitCode": 0}}), nil
	})
	logs, err := d.Execute(context.Background(), req, t.TempDir())
	if err != nil || logs != "sync status: completed" {
		t.Fatalf("legacy retry: %q, %v", logs, err)
	}
}

func TestLegacyDockerNameIgnoresUncontrolledRuntimeField(t *testing.T) {
	req := &types.ExecutionRequest{WorkflowID: "ordinary-stable", Command: types.Sync, ConnectorType: "mysql", Version: "v1", RuntimeID: "not-an-admission"}
	if dockerResourceName(req) != utils.GetWorkflowDirectory(req.Command, req.WorkflowID) {
		t.Fatal("uncontrolled runtime changed provider identity")
	}
}

func TestLegacyDockerResolveIsNoOpAndResyncIsRejected(t *testing.T) {
	d := &DockerExecutor{}
	if err := d.ResolveResource(context.Background(), &types.ExecutionRequest{Command: types.Sync}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Execute(context.Background(), &types.ExecutionRequest{Command: types.Resync}, ""); err == nil {
		t.Fatal("legacy resync accepted")
	}
}
