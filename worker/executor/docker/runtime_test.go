package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/spf13/viper"
)

type runtimeRoundTripper func(*http.Request) (*http.Response, error)

func (f runtimeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func runtimeResponse(code int, value interface{}) *http.Response {
	body, _ := json.Marshal(value)
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}
}

func runtimeLogResponse() *http.Response {
	message := []byte("original runtime logs")
	body := make([]byte, 8+len(message))
	body[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(body[4:8], uint32(len(message)))
	copy(body[8:], message)
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}
}

func newRuntimeDocker(t *testing.T, handle runtimeRoundTripper) *DockerExecutor {
	t.Helper()
	c, err := client.New(client.WithHost("http://docker.test"), client.WithAPIVersion("1.54"), client.WithHTTPClient(&http.Client{Transport: handle}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &DockerExecutor{client: c}
}

func runtimeDockerRequest(command types.Command) *types.ExecutionRequest {
	return &types.ExecutionRequest{
		WorkflowID: "stable-operation", RunID: "owner-run", LaunchID: "data-launch", RuntimeID: "runtime-a", JobID: 42,
		Command: command, ConnectorType: "postgres", Version: "v1", AllowLaunch: true, Controlled: true,
		BeforeLaunch:   func(context.Context) error { return nil },
		RecordResource: func(context.Context, string, string) error { return nil },
	}
}

func TestControlledDockerRecordsIdentityBeforeStart(t *testing.T) {
	req := runtimeDockerRequest(types.Resync)
	var events []string
	req.BeforeLaunch = func(context.Context) error { events = append(events, "authorize"); return nil }
	req.RecordResource = func(_ context.Context, id, uid string) error {
		events = append(events, "record")
		if id != "immutable-id" || uid != "" {
			t.Fatalf("recorded identity = %q, %q", id, uid)
		}
		return nil
	}
	d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
		switch strings.TrimPrefix(r.URL.Path, "/v1.54") {
		case "/containers/create":
			events = append(events, "create")
			return runtimeResponse(http.StatusCreated, map[string]string{"Id": "immutable-id"}), nil
		case "/containers/immutable-id/start":
			events = append(events, "start")
			return runtimeResponse(http.StatusNoContent, nil), nil
		default:
			t.Fatalf("unexpected provider request: %s %s", r.Method, r.URL.Path)
			return nil, errors.New("unexpected request")
		}
	})
	id, err := d.createControlledContainer(context.Background(), req, &container.Config{Image: "olakego/source-postgres:v1"}, &container.HostConfig{}, dockerResourceName(req))
	if err != nil || id != "immutable-id" || req.ResourceID != id {
		t.Fatalf("launch = %q, %v; captured ID %q", id, err, req.ResourceID)
	}
	if want := []string{"authorize", "create", "record", "authorize", "start"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestControlledDockerFencesCreateAndStart(t *testing.T) {
	for _, stopAt := range []string{"before create", "record", "before start", "create conflict"} {
		t.Run(stopAt, func(t *testing.T) {
			req := runtimeDockerRequest(types.ClearDestination)
			closed := errors.New("execution closed")
			authorizations, creates, starts := 0, 0, 0
			req.BeforeLaunch = func(context.Context) error {
				authorizations++
				if stopAt == "before create" || (stopAt == "before start" && authorizations == 2) {
					return closed
				}
				return nil
			}
			req.RecordResource = func(context.Context, string, string) error {
				if stopAt == "record" {
					return closed
				}
				return nil
			}
			d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/create") {
					creates++
					if stopAt == "create conflict" {
						return runtimeResponse(http.StatusConflict, map[string]string{"message": "already exists"}), nil
					}
					return runtimeResponse(http.StatusCreated, map[string]string{"Id": "immutable-id"}), nil
				}
				starts++
				return runtimeResponse(http.StatusNoContent, nil), nil
			})
			_, err := d.createControlledContainer(context.Background(), req, &container.Config{Image: "olakego/source-postgres:v1"}, &container.HostConfig{}, dockerResourceName(req))
			if err == nil || starts != 0 || (stopAt == "before create" && creates != 0) {
				t.Fatalf("closed/conflicting launch: err=%v creates=%d starts=%d", err, creates, starts)
			}
		})
	}
}

func TestControlledDockerRetriesNeverCreateOrRestart(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	for _, command := range []types.Command{types.Sync, types.Resync, types.ClearDestination} {
		for _, state := range []string{"missing", "created", "running", "exited", "failed"} {
			t.Run(string(command)+"/"+state, func(t *testing.T) {
				req := runtimeDockerRequest(command)
				req.AllowLaunch = false
				mutations, records := 0, 0
				req.RecordResource = func(context.Context, string, string) error { records++; return nil }
				d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
					path := strings.TrimPrefix(r.URL.Path, "/v1.54")
					if r.Method != http.MethodGet && !strings.HasSuffix(path, "/wait") {
						mutations++
						return runtimeResponse(http.StatusInternalServerError, nil), nil
					}
					if strings.HasSuffix(path, "/logs") {
						return runtimeLogResponse(), nil
					}
					if strings.HasSuffix(path, "/wait") {
						return runtimeResponse(http.StatusOK, map[string]int{"StatusCode": 0}), nil
					}
					if state == "missing" {
						return runtimeResponse(http.StatusNotFound, map[string]string{"message": "not found"}), nil
					}
					exitCode, status := 0, state
					if state == "failed" {
						exitCode, status = 1, "exited"
					}
					return runtimeResponse(http.StatusOK, map[string]interface{}{
						"Id": "immutable-id", "Config": map[string]interface{}{"Labels": dockerOwnership(req)},
						"State": map[string]interface{}{"Status": status, "Running": state == "running", "ExitCode": exitCode},
					}), nil
				})
				logs, err := d.Execute(context.Background(), req, t.TempDir())
				wantSuccess := state == "running" || state == "exited"
				if (err == nil) != wantSuccess || mutations != 0 {
					t.Fatalf("attach: logs=%q err=%v mutations=%d", logs, err, mutations)
				}
				if wantSuccess && logs != "original runtime logs" {
					t.Fatalf("logs = %q", logs)
				}
				if state != "missing" && records != 1 {
					t.Fatalf("identity records = %d", records)
				}
			})
		}
	}
}

func TestControlledDockerValidatesOwnershipBeforeAttach(t *testing.T) {
	req := runtimeDockerRequest(types.Resync)
	req.AllowLaunch = false
	records, mutations := 0, 0
	req.RecordResource = func(context.Context, string, string) error { records++; return nil }
	labels := dockerOwnership(req)
	labels["olake.io/runtime-id"] = "another-runtime"
	d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			mutations++
		}
		return runtimeResponse(http.StatusOK, map[string]interface{}{"Id": "foreign-id", "Config": map[string]interface{}{"Labels": labels}}), nil
	})
	if _, err := d.Execute(context.Background(), req, t.TempDir()); err == nil || records != 0 || mutations != 0 {
		t.Fatalf("foreign resource attach: err=%v records=%d mutations=%d", err, records, mutations)
	}
}

func TestControlledDockerCleanupUsesOnlyImmutableID(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(strconvBool(missing), func(t *testing.T) {
			req := runtimeDockerRequest(types.Resync)
			req.ResourceID = "old-immutable-id"
			var requests []string
			d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
				path := strings.TrimPrefix(r.URL.Path, "/v1.54")
				requests = append(requests, r.Method+" "+path)
				if !strings.HasPrefix(path, "/containers/old-immutable-id") {
					t.Fatalf("cleanup addressed reusable name: %s", path)
				}
				if r.Method == http.MethodGet {
					if missing {
						return runtimeResponse(http.StatusNotFound, map[string]string{"message": "old resource gone; successor exists by name"}), nil
					}
					return runtimeResponse(http.StatusOK, map[string]interface{}{"Id": req.ResourceID, "Config": map[string]interface{}{"Labels": dockerOwnership(req)}}), nil
				}
				return runtimeResponse(http.StatusNoContent, nil), nil
			})
			if err := d.Cleanup(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			want := []string{"GET /containers/old-immutable-id/json"}
			if !missing {
				want = append(want, "POST /containers/old-immutable-id/stop", "DELETE /containers/old-immutable-id")
			}
			if !reflect.DeepEqual(requests, want) {
				t.Fatalf("cleanup requests = %v, want %v", requests, want)
			}
		})
	}
}

func strconvBool(value bool) string {
	if value {
		return "missing old resource"
	}
	return "existing old resource"
}

func TestDockerRuntimeConfigAndNamesDoNotUseStableWorkflow(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeS3)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	req := runtimeDockerRequest(types.Resync)
	req.RuntimeID = strings.Repeat("shared-prefix-", 10) + "a"
	next := *req
	next.RuntimeID = strings.TrimSuffix(req.RuntimeID, "a") + "b"
	if dockerResourceName(req) == dockerResourceName(&next) {
		t.Fatal("runtime names collide after a shared prefix")
	}
	closed, beforeCalls := errors.New("closed before start"), 0
	req.BeforeLaunch = func(context.Context) error {
		beforeCalls++
		if beforeCalls == 2 {
			return closed
		}
		return nil
	}
	var config container.Config
	d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
		path := strings.TrimPrefix(r.URL.Path, "/v1.54")
		switch {
		case strings.HasPrefix(path, "/images/"):
			return runtimeResponse(http.StatusOK, map[string]string{"Id": "image"}), nil
		case strings.HasSuffix(path, "/json"):
			return runtimeResponse(http.StatusNotFound, map[string]string{"message": "not found"}), nil
		case path == "/containers/create":
			if r.URL.Query().Get("name") != dockerResourceName(req) {
				t.Errorf("container name = %q", r.URL.Query().Get("name"))
			}
			if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
				t.Fatal(err)
			}
			return runtimeResponse(http.StatusCreated, map[string]string{"Id": "immutable-id"}), nil
		default:
			t.Fatalf("unexpected request after closure: %s", path)
			return nil, closed
		}
	})
	if _, err := d.Execute(context.Background(), req, ""); !errors.Is(err, closed) {
		t.Fatalf("execute error = %v", err)
	}
	wanted := constants.EnvS3ConfigFolder + "=" + utils.ConnectorConfigDir(req.Command, req.RuntimeID)
	found := false
	for _, env := range config.Env {
		if env == wanted {
			found = true
		}
	}
	if !found || config.Labels["olake.io/workflow-id"] != req.WorkflowID {
		t.Fatalf("runtime environment/ownership = %v / %v", config.Env, config.Labels)
	}
}

func TestSynchronousDockerNeedsNoAdmissionCallbacks(t *testing.T) {
	oldMode := viper.Get(constants.EnvStorageMode)
	viper.Set(constants.EnvStorageMode, constants.StorageModeNFS)
	t.Cleanup(func() { viper.Set(constants.EnvStorageMode, oldMode) })
	req := &types.ExecutionRequest{WorkflowID: "metadata-check", Command: types.Check, ConnectorType: "postgres", Version: "v1"}
	starts, removes := 0, 0
	d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
		path := strings.TrimPrefix(r.URL.Path, "/v1.54")
		switch {
		case strings.HasPrefix(path, "/images/"):
			return runtimeResponse(http.StatusOK, map[string]string{"Id": "image"}), nil
		case path == "/containers/create":
			return runtimeResponse(http.StatusCreated, map[string]string{"Id": "sync-id"}), nil
		case strings.HasSuffix(path, "/start"):
			starts++
			return runtimeResponse(http.StatusNoContent, nil), nil
		case strings.HasSuffix(path, "/wait"):
			return runtimeResponse(http.StatusOK, map[string]int{"StatusCode": 0}), nil
		case strings.HasSuffix(path, "/logs"):
			return runtimeLogResponse(), nil
		case r.Method == http.MethodDelete:
			removes++
			return runtimeResponse(http.StatusNoContent, nil), nil
		default:
			return nil, errors.New("unexpected synchronous request: " + path)
		}
	})
	logs, err := d.Execute(context.Background(), req, t.TempDir())
	if err != nil || starts != 1 || removes != 1 || logs != "original runtime logs" {
		t.Fatalf("unmanaged check: logs=%q err=%v starts=%d removes=%d", logs, err, starts, removes)
	}
}

func TestControlledDockerLateCreateCannotStartAfterRetirement(t *testing.T) {
	req := runtimeDockerRequest(types.Resync)
	creating, finishCreate := make(chan struct{}), make(chan struct{})
	closed := errors.New("admission closed while Docker create was in flight")
	d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/create") {
			t.Errorf("late create attempted another mutation: %s %s", r.Method, r.URL.Path)
			return nil, closed
		}
		close(creating)
		<-finishCreate
		return runtimeResponse(http.StatusCreated, map[string]string{"Id": "late-immutable-id"}), nil
	})
	req.RecordResource = func(_ context.Context, id, uid string) error {
		if id != "late-immutable-id" || uid != "" {
			t.Errorf("late resource identity = %s/%s", id, uid)
		}
		return closed
	}
	result := make(chan error, 1)
	go func() {
		_, err := d.createControlledContainer(context.Background(), req, &container.Config{Image: "olakego/source-postgres:v1"}, &container.HostConfig{}, dockerResourceName(req))
		result <- err
	}()
	select {
	case <-creating:
	case err := <-result:
		t.Fatalf("execution returned before Docker create: %v", err)
	}
	close(finishCreate)
	if err := <-result; !errors.Is(err, closed) {
		t.Fatalf("late create error = %v", err)
	}
}

func TestControlledDockerCleanupResolvesLostCreateAcknowledgement(t *testing.T) {
	for _, state := range []string{"late resource", "missing", "foreign resource"} {
		t.Run(state, func(t *testing.T) {
			req := runtimeDockerRequest(types.Resync)
			req.RecordResource = func(context.Context, string, string) error {
				t.Fatal("cleanup must not re-enter SQL recorder")
				return nil
			}
			mutations := 0
			d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
				path := strings.TrimPrefix(r.URL.Path, "/v1.54")
				if r.Method == http.MethodGet {
					if path != "/containers/"+dockerResourceName(req)+"/json" {
						t.Errorf("cleanup resolution path = %s", path)
					}
					if state == "missing" {
						return runtimeResponse(http.StatusNotFound, map[string]string{"message": "not found"}), nil
					}
					labels := dockerOwnership(req)
					if state == "foreign resource" {
						labels["olake.io/runtime-id"] = "different-runtime"
					}
					return runtimeResponse(http.StatusOK, map[string]interface{}{"Id": "late-id", "Config": map[string]interface{}{"Labels": labels}}), nil
				}
				mutations++
				if !strings.HasPrefix(path, "/containers/late-id") {
					t.Errorf("cleanup mutated reusable name: %s", path)
				}
				return runtimeResponse(http.StatusNoContent, nil), nil
			})
			err := d.Cleanup(context.Background(), req)
			if state == "late resource" {
				if err != nil || req.ResourceID != "late-id" || mutations != 2 {
					t.Fatalf("late cleanup: err=%v id=%s mutations=%d", err, req.ResourceID, mutations)
				}
			} else if err == nil || mutations != 0 {
				t.Fatalf("unresolved cleanup: err=%v mutations=%d", err, mutations)
			}
		})
	}
}

func TestControlledDockerWithoutRuntimeFailsBeforeProviderCalls(t *testing.T) {
	d := &DockerExecutor{}
	req := &types.ExecutionRequest{WorkflowID: "controlled", Command: types.ClearDestination, Controlled: true}
	if _, err := d.Execute(context.Background(), req, t.TempDir()); err == nil {
		t.Fatal("unadmitted asynchronous execution accepted")
	}
	if err := d.Cleanup(context.Background(), req); err == nil {
		t.Fatal("unadmitted asynchronous cleanup accepted")
	}
}

func TestControlledDockerResolvesLostCreateAcknowledgementWithoutCallbacks(t *testing.T) {
	for _, state := range []string{"late resource", "missing", "foreign resource"} {
		t.Run(state, func(t *testing.T) {
			req := runtimeDockerRequest(types.Resync)
			req.RecordResource = func(context.Context, string, string) error {
				t.Fatal("resolution must not re-enter SQL recorder")
				return nil
			}
			d := newRuntimeDocker(t, func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet || strings.TrimPrefix(r.URL.Path, "/v1.54") != "/containers/"+dockerResourceName(req)+"/json" {
					t.Errorf("resolution request = %s %s", r.Method, r.URL.Path)
				}
				if state == "missing" {
					return runtimeResponse(http.StatusNotFound, map[string]string{"message": "not found"}), nil
				}
				labels := dockerOwnership(req)
				if state == "foreign resource" {
					labels["olake.io/workflow-id"] = "different-operation"
				}
				return runtimeResponse(http.StatusOK, map[string]interface{}{"Id": "late-id", "Config": map[string]interface{}{"Labels": labels}}), nil
			})
			err := d.ResolveResource(context.Background(), req)
			if state == "late resource" {
				if err != nil || req.ResourceID != "late-id" {
					t.Fatalf("resolved identity = %s, err=%v", req.ResourceID, err)
				}
			} else if err == nil || req.ResourceID != "" {
				t.Fatalf("unresolved identity was accepted: %s, err=%v", req.ResourceID, err)
			}
		})
	}
}

func TestKnownDockerIdentityNeedsNoResolutionProviderRequest(t *testing.T) {
	d := &DockerExecutor{}
	req := runtimeDockerRequest(types.Resync)
	req.ResourceID = "captured-id"
	if err := d.ResolveResource(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}
