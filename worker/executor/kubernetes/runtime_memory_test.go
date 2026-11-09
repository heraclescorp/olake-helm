package kubernetes

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
)

func runtimeMemoryExecutor(raw string) *KubernetesExecutor {
	profiles, err := LoadJobProfiles(raw)
	return &KubernetesExecutor{
		namespace: "jobs",
		config:    &KubernetesConfig{PVCName: "jobs-config", SecretKey: "not-a-metric-attribute"},
		configWatcher: &ConfigMapWatcher{
			jobProfiles:    profiles,
			jobProfilesErr: err,
		},
	}
}

func runtimeMemoryEnv(env []corev1.EnvVar) map[string]string {
	values := make(map[string]string, len(env))
	for _, item := range env {
		values[item.Name] = item.Value
	}
	return values
}

func TestRuntimeMemoryResolvesContainerLimitBudgets(t *testing.T) {
	tests := []struct {
		name     string
		limit    string
		policy   RuntimeMemoryConfig
		wantGo   string
		wantJava string
	}{
		{"percentage uses limit not request", "10Gi", RuntimeMemoryConfig{GoPercent: 10, JavaPercent: 50}, "1073741824", "50"},
		{"absolute Kubernetes quantity", "64Gi", RuntimeMemoryConfig{GoLimit: "16Gi", JavaPercent: 50}, "17179869184", "50"},
		{"exactly eighty percent", "1000", RuntimeMemoryConfig{GoPercent: 30, JavaPercent: 50}, "300", "50"},
		{"fractional Kubernetes bytes round up", "1000", RuntimeMemoryConfig{GoLimit: "1500m", JavaPercent: 50}, "2", "50"},
		{"percentage rounds down", "1001", RuntimeMemoryConfig{GoPercent: 30, JavaPercent: 50}, "300", "50"},
		{"large limit does not overflow", "9223372036854775807", RuntimeMemoryConfig{GoPercent: 30, JavaPercent: 50}, "2767011611056432742", "50"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resources := corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(tt.limit)},
			}
			env, err := tt.policy.resolve(resources)
			if err != nil {
				t.Fatal(err)
			}
			got := runtimeMemoryEnv(env)
			if got["GOMEMLIMIT"] != tt.wantGo || got["OLAKE_JAVA_HEAP_PERCENT"] != tt.wantJava {
				t.Fatalf("runtime env = %v, want Go %s and Java %s", got, tt.wantGo, tt.wantJava)
			}
		})
	}
	if got := memoryPercent(math.MaxInt64, 80); got != 7378697629483820645 {
		t.Fatalf("overflow-safe 80%% of MaxInt64 = %d", got)
	}
}

func TestRuntimeMemoryRejectsUnsafeResolvedBudgets(t *testing.T) {
	tests := []struct {
		name   string
		limit  string
		policy RuntimeMemoryConfig
	}{
		{"missing limit", "", RuntimeMemoryConfig{GoPercent: 10, JavaPercent: 50}},
		{"zero limit", "0", RuntimeMemoryConfig{GoPercent: 10, JavaPercent: 50}},
		{"negative limit", "-1Gi", RuntimeMemoryConfig{GoPercent: 10, JavaPercent: 50}},
		{"overflowing limit", "1e30", RuntimeMemoryConfig{GoPercent: 10, JavaPercent: 50}},
		{"absolute budget exceeds headroom", "10Gi", RuntimeMemoryConfig{GoLimit: "4Gi", JavaPercent: 50}},
		{"no room for Go", "10Gi", RuntimeMemoryConfig{GoLimit: "1", JavaPercent: 80}},
		{"zero resolved Go budget", "1", RuntimeMemoryConfig{GoPercent: 10, JavaPercent: 50}},
		{"fractional byte headroom is not rounded up", "101", RuntimeMemoryConfig{GoLimit: "31", JavaPercent: 50}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resources := corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Gi")}}
			if tt.limit != "" {
				resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(tt.limit)}
			}
			if env, err := tt.policy.resolve(resources); err == nil || len(env) != 0 {
				t.Fatalf("unsafe policy returned env %v and error %v", env, err)
			}
		})
	}
}

func TestRuntimeMemoryInvalidPoliciesFailClosed(t *testing.T) {
	policies := []string{
		`null`, `true`, `"not-an-object"`, `[]`, `{}`,
		`{"goPercent":10}`,
		`{"javaPercent":50}`,
		`{"goLimit":"1Gi","goPercent":10,"javaPercent":50}`,
		`{"goLimit":"","goPercent":10,"javaPercent":50}`,
		`{"goPercent":0,"javaPercent":50}`,
		`{"goPercent":-1,"javaPercent":50}`,
		`{"goPercent":10.5,"javaPercent":50}`,
		`{"goPercent":"10","javaPercent":50}`,
		`{"goPercent":10,"javaPercent":0}`,
		`{"goPercent":10,"javaPercent":-1}`,
		`{"goPercent":10,"javaPercent":50.5}`,
		`{"goPercent":10,"javaPercent":81}`,
		`{"goPercent":31,"javaPercent":50}`,
		`{"goPercent":10,"javaPercent":50,"typo":1}`,
		`{"goLimit":"2GB","javaPercent":50}`,
		`{"goLimit":"-1Gi","javaPercent":50}`,
		`{"goLimit":"0","javaPercent":50}`,
		`{"goLimit":"1e30","javaPercent":50}`,
		`{"goLimit":10,"javaPercent":50}`,
		`{"goLimit":null,"javaPercent":50}`,
		`{"goLimit":"1Gi","goPercent":null,"javaPercent":50}`,
		`{"goPercent":10,"javaPercent":null}`,
	}
	for _, policy := range policies {
		t.Run(policy, func(t *testing.T) {
			raw := fmt.Sprintf(`{
				"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}},
				"42":{"runtimeMemory":%s}
			}`, policy)
			exec := runtimeMemoryExecutor(raw)
			if _, exists := exec.configWatcher.jobProfiles[42]; !exists {
				t.Fatal("invalid policy was discarded, allowing a default fallback")
			}
			pod, err := exec.CreatePodSpec(&types.ExecutionRequest{JobID: 42, Command: types.Sync}, "/tmp/job", "connector:test", nil)
			if err == nil || pod != nil {
				t.Fatalf("invalid selected policy launched pod %v with error %v", pod, err)
			}
			if _, _, err := exec.resourcesAndRuntimeEnvForJob(99, types.Sync); err != nil {
				t.Fatalf("invalid neighbour blocked default policy: %v", err)
			}
			if _, _, err := exec.resourcesAndRuntimeEnvForJob(42, types.Discover); err != nil {
				t.Fatalf("discover selected an invalid job-specific policy: %v", err)
			}
		})
	}
}

func TestRuntimeMemoryPolicyAndResourcesPrecedence(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		op      types.Command
		wantGo  string
		wantErr bool
	}{
		{"own policy uses fallback resources", `{"0":{"resources":{"limits":{"memory":"10Gi"}}},"42":{"runtimeMemory":{"goPercent":20,"javaPercent":50}}}`, types.Sync, "2147483648", false},
		{"fallback policy uses actual own resources", `{"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}},"42":{"resources":{"limits":{"memory":"20Gi"}}}}`, types.Sync, "2147483648", false},
		{"clear destination uses own policy", `{"0":{"resources":{"limits":{"memory":"10Gi"}}},"42":{"runtimeMemory":{"goPercent":20,"javaPercent":50}}}`, types.ClearDestination, "2147483648", false},
		{"discover uses default", `{"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}},"42":{"resources":{"limits":{"memory":"20Gi"}},"runtimeMemory":{"goPercent":20,"javaPercent":50}}}`, types.Discover, "1073741824", false},
		{"enrollment discover uses default", `{"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}},"42":{"runtimeMemory":null}}`, types.DiscoverNewTables, "1073741824", false},
		{"own valid policy bypasses invalid default policy", `{"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":null},"42":{"runtimeMemory":{"goPercent":20,"javaPercent":50}}}`, types.Sync, "2147483648", false},
		{"selected invalid default policy blocks", `{"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":null},"42":{"nodeSelector":{"pool":"heavy"}}}`, types.Sync, "", true},
		{"no job policy and no default policy", `{"42":{"resources":{"limits":{"memory":"10Gi"}}}}`, types.Sync, "", false},
		{"unlimited discovery ignores own budget", `{"42":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`, types.Discover, "", false},
		{"own resources are not merged with default limit", `{"0":{"resources":{"limits":{"memory":"10Gi"}}},"42":{"resources":{"requests":{"memory":"1Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`, types.Sync, "", true},
		{"invalid resource quantity with policy blocks", `{"0":{"resources":{"limits":{"memory":"10Gi"}}},"42":{"resources":{"limits":{"memory":"2GB"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`, types.Sync, "", true},
		{"invalid resources with inherited policy block", `{"0":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}},"42":{"resources":{"limits":{"memory":"2GB"}}}}`, types.Sync, "", true},
		{"request above limit with policy blocks", `{"42":{"resources":{"requests":{"memory":"20Gi"},"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`, types.Sync, "", true},
		{"invalid scheduling cannot discard policy", `{"42":{"nodeSelector":"not-a-map","runtimeMemory":{"goPercent":10,"javaPercent":50}}}`, types.Sync, "", true},
		{"invalid default resources do not affect independent job", `{"0":{"resources":{"limits":{"memory":"2GB"}},"runtimeMemory":null},"42":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`, types.Sync, "1073741824", false},
		{"legacy bad resources without policy still fall back", `{"0":{"resources":{"limits":{"memory":"10Gi"}}},"42":{"resources":{"limits":{"memory":"2GB"}}}}`, types.Sync, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := runtimeMemoryExecutor(tt.raw)
			_, env, err := exec.resourcesAndRuntimeEnvForJob(42, tt.op)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolve error = %v, want error %v", err, tt.wantErr)
			}
			if got := runtimeMemoryEnv(env)["GOMEMLIMIT"]; got != tt.wantGo {
				t.Fatalf("GOMEMLIMIT = %q, want %q", got, tt.wantGo)
			}
		})
	}
}

func TestRuntimeMemoryPodEnvironmentPrecedenceAndIdentity(t *testing.T) {
	exec := runtimeMemoryExecutor(`{"42":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`)
	req := &types.ExecutionRequest{JobID: 42, Command: types.Sync, WorkflowID: "workflow-42", ConnectorType: "postgres"}
	pod, err := exec.CreatePodSpec(req, "/tmp/job", "connector:test", nil)
	if err != nil {
		t.Fatal(err)
	}
	container := pod.Spec.Containers[0]
	values := runtimeMemoryEnv(container.Env)
	want := map[string]string{
		"GOMEMLIMIT":              "1073741824",
		"OLAKE_JAVA_HEAP_PERCENT": "50",
		"OLAKE_JOB_ID":            "42",
		"OLAKE_OPERATION_TYPE":    "sync",
		"OLAKE_WORKFLOW_ID":       "workflow-42",
	}
	for name, value := range want {
		if values[name] != value {
			t.Errorf("explicit %s = %q, want %q", name, values[name], value)
		}
	}
	// Kubernetes gives explicit Env priority over EnvFrom, including global budget values.
	if len(container.EnvFrom) == 0 || container.EnvFrom[0].ConfigMapRef.Name != constants.GlobalEnvConfigMap {
		t.Fatalf("global EnvFrom was lost: %v", container.EnvFrom)
	}
	if _, exists := values["OTEL_RESOURCE_ATTRIBUTES"]; exists {
		t.Fatal("identity must not replace inherited OTEL resource attributes")
	}
}

func TestRuntimeMemoryNoPolicyPreservesInheritedBudgets(t *testing.T) {
	exec := runtimeMemoryExecutor(`{}`)
	req := &types.ExecutionRequest{JobID: 42, Command: types.Discover, WorkflowID: "discover-42"}
	pod, err := exec.CreatePodSpec(req, "/tmp/job", "connector:test", nil)
	if err != nil {
		t.Fatal(err)
	}
	container := pod.Spec.Containers[0]
	values := runtimeMemoryEnv(container.Env)
	for _, name := range []string{"GOMEMLIMIT", "OLAKE_JAVA_HEAP_PERCENT", "_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"} {
		if _, exists := values[name]; exists {
			t.Errorf("no-policy pod unexpectedly overrides %s", name)
		}
	}
	if !reflect.DeepEqual(container.Resources, defaultJobResources()) {
		t.Fatalf("no-policy resources changed: %v", container.Resources)
	}
	if values["OLAKE_JOB_ID"] != "42" || values["OLAKE_OPERATION_TYPE"] != "discover" || values["OLAKE_WORKFLOW_ID"] != "discover-42" {
		t.Fatalf("no-policy identity = %v", values)
	}
	wantFields := map[string]string{"POD_NAME": "metadata.name", "POD_NAMESPACE": "metadata.namespace"}
	for _, variable := range container.Env {
		if field, exists := wantFields[variable.Name]; exists {
			if variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil || variable.ValueFrom.FieldRef.APIVersion != "v1" || variable.ValueFrom.FieldRef.FieldPath != field {
				t.Errorf("%s does not use downward API field %s: %v", variable.Name, field, variable.ValueFrom)
			}
			delete(wantFields, variable.Name)
		}
	}
	if len(wantFields) != 0 {
		t.Errorf("missing downward API identity: %v", wantFields)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("connector metadata must not require a service account token")
	}
}

func TestRuntimeMemoryMalformedConfigBlocksUntilCorrected(t *testing.T) {
	exec := runtimeMemoryExecutor(`{}`)
	cm := &corev1.ConfigMap{Data: map[string]string{"OLAKE_JOB_PROFILES": `{"42":{"runtimeMemory":`}}
	exec.configWatcher.updateJobMapping(cm)
	if _, _, err := exec.resourcesAndRuntimeEnvForJob(42, types.Sync); err == nil {
		t.Fatal("malformed JSON was silently replaced by default configuration")
	}
	cm.Data["OLAKE_JOB_PROFILES"] = `{"42":{"resources":{"limits":{"memory":"10Gi"}},"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`
	exec.configWatcher.updateJobMapping(cm)
	if _, env, err := exec.resourcesAndRuntimeEnvForJob(42, types.Sync); err != nil || runtimeMemoryEnv(env)["GOMEMLIMIT"] != "1073741824" {
		t.Fatalf("corrected policy = %v, %v", env, err)
	}
	delete(cm.Data, "OLAKE_JOB_PROFILES")
	exec.configWatcher.updateJobMapping(cm)
	if _, env, err := exec.resourcesAndRuntimeEnvForJob(42, types.Sync); err != nil || len(env) != 0 {
		t.Fatalf("removed policy = %v, %v", env, err)
	}
}

func TestRuntimeMemoryExecuteRejectsPolicyBeforeCreatingPod(t *testing.T) {
	exec := runtimeMemoryExecutor(`{"42":{"runtimeMemory":{"goPercent":10,"javaPercent":50}}}`)
	client := fake.NewSimpleClientset()
	exec.client = client
	_, err := exec.Execute(context.Background(), &types.ExecutionRequest{JobID: 42, Command: types.Sync, ConnectorType: "postgres", WorkflowID: "sync-42"}, "/tmp/job")
	if err == nil || !strings.Contains(err.Error(), "resources.limits.memory") {
		t.Fatalf("Execute error = %v, want missing container limit", err)
	}
	if actions := client.Actions(); len(actions) != 0 {
		t.Fatalf("invalid policy contacted Kubernetes: %v", actions)
	}
}
