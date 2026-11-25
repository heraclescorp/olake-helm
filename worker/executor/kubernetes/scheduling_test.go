package kubernetes

import (
	"fmt"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/types"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestLoadJobProfilesResources(t *testing.T) {
	raw := `{
		"0":   {"resources": {"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"memory": "2Gi"}}},
		"123": {"nodeSelector": {"node-type": "big"}, "resources": {"requests": {"cpu": 2}}},
		"456": {"nodeSelector": {"node-type": "big"}, "resources": {"requests": {"memory": "4Gi"}, "limits": {"memory": "1Gi"}}}
	}`

	profiles, err := LoadJobProfiles(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 3 {
		t.Fatalf("expected 3 profiles, got %d", len(profiles))
	}

	def := profiles[0].Resources
	if def == nil {
		t.Fatal("default profile resources not parsed")
	}
	if mem := def.Limits[corev1.ResourceMemory]; mem.String() != "2Gi" {
		t.Errorf("default memory limit = %s, want 2Gi", mem.String())
	}
	if cpu := def.Requests[corev1.ResourceCPU]; cpu.String() != "500m" {
		t.Errorf("default cpu request = %s, want 500m", cpu.String())
	}

	// A plain JSON number is a valid quantity (YAML `cpu: 2`)
	if cpu := profiles[123].Resources.Requests[corev1.ResourceCPU]; cpu.String() != "2" {
		t.Errorf("job 123 cpu request = %s, want 2", cpu.String())
	}

	// Request above limit: resources dropped, scheduling kept
	if profiles[456].Resources != nil {
		t.Errorf("job 456 resources should be dropped when request exceeds limit")
	}
	if profiles[456].NodeSelector["node-type"] != "big" {
		t.Errorf("job 456 nodeSelector should be kept when resources are dropped")
	}
}

func TestLoadJobProfilesRejectsNegativeResources(t *testing.T) {
	raw := `{
		"1": {"nodeSelector": {"node-type": "big"}, "resources": {"requests": {"cpu": -1}}},
		"2": {"resources": {"limits": {"memory": "-1Gi"}}}
	}`

	profiles, err := LoadJobProfiles(raw)
	if err != nil {
		t.Fatal(err)
	}
	for jobID, profile := range profiles {
		if profile.Resources != nil {
			t.Errorf("job %d: negative resources should be dropped", jobID)
		}
	}
	if profiles[1].NodeSelector["node-type"] != "big" {
		t.Errorf("job 1 nodeSelector should be kept when resources are dropped")
	}
}

func TestLoadJobProfilesIsolatesBadProfiles(t *testing.T) {
	raw := `{
		"0":   {"nodeSelector": {"node-type": "standard"}, "resources": {"requests": {"memory": "1Gi"}}},
		"123": {"nodeSelector": {"node-type": "big"}, "resources": {"limits": {"memory": "2GB"}}},
		"456": {"nodeSelector": "not-a-map"}
	}`

	profiles, err := LoadJobProfiles(raw)
	if err != nil {
		t.Fatal(err)
	}

	// A good profile survives a bad neighbour
	if mem := profiles[0].Resources.Requests[corev1.ResourceMemory]; mem.String() != "1Gi" {
		t.Errorf("job 0 memory request = %s, want 1Gi", mem.String())
	}

	// An unparsable quantity drops only that profile's resources
	if profiles[123].Resources != nil {
		t.Errorf("job 123 resources should be dropped when a quantity cannot be parsed")
	}
	if profiles[123].NodeSelector["node-type"] != "big" {
		t.Errorf("job 123 nodeSelector should be kept when only resources are invalid")
	}

	// A profile broken outside resources is skipped entirely
	if _, exists := profiles[456]; exists {
		t.Errorf("job 456 should be skipped when its scheduling fields are invalid")
	}
}

func TestLoadJobProfilesWithoutResources(t *testing.T) {
	profiles, err := LoadJobProfiles(`{"0": {"nodeSelector": {"node-type": "standard"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if profiles[0].Resources != nil {
		t.Errorf("resources should stay nil when not configured")
	}
}

func TestLoadJobProfilesEphemeralStoragePodResources(t *testing.T) {
	for _, tt := range []struct {
		name    string
		request string
		limit   string
		valid   bool
	}{
		{"binary quantities", `"3Gi"`, `"6Gi"`, true},
		{"decimal quantities", `"1.5G"`, `"2G"`, true},
		{"numeric byte quantities", `1073741824`, `2147483648`, true},
		{"unitless string quantities", `"1024"`, `"2048"`, true},
		{"equivalent request and limit", `"3072Mi"`, `"3Gi"`, true},
		{"request only", `"3Gi"`, "", true},
		{"limit only", "", `"6Gi"`, true},
		{"boolean request", `true`, `"6Gi"`, false},
		{"array limit", `"3Gi"`, `[6]`, false},
		{"object request", `{"value":"3Gi"}`, `"6Gi"`, false},
		{"null request", `null`, `"6Gi"`, false},
		{"invalid request suffix", `"3GB"`, `"6Gi"`, false},
		{"invalid limit quantity", `"3Gi"`, `"not-a-quantity"`, false},
		{"zero numeric request", `0`, `"6Gi"`, false},
		{"zero string limit", `"3Gi"`, `"0Gi"`, false},
		{"zero request and limit", `"0Mi"`, `0`, false},
		{"negative request", `"-3Gi"`, `"6Gi"`, false},
		{"negative limit", `"3Gi"`, `"-6Gi"`, false},
		{"request one Mi above limit", `"2049Mi"`, `"2Gi"`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests, limit := "", ""
			if tt.request != "" {
				requests = fmt.Sprintf(`"requests":{"ephemeral-storage":%s},`, tt.request)
			}
			if tt.limit != "" {
				limit = fmt.Sprintf(`,"ephemeral-storage":%s`, tt.limit)
			}
			raw := fmt.Sprintf(`{
				"0":{"resources":{"requests":{"cpu":"500m","memory":"1Gi"},"limits":{"memory":"2Gi"}}},
				"123":{"nodeSelector":{"pool":"disk"},"resources":{%s"limits":{"memory":"4Gi"%s}}}
			}`, requests, limit)
			exec := runtimeMemoryExecutor(raw)
			if exec.configWatcher.jobProfilesErr != nil {
				t.Fatal(exec.configWatcher.jobProfilesErr)
			}
			profile, exists := exec.configWatcher.jobProfiles[123]
			if !exists || profile.NodeSelector["pool"] != "disk" {
				t.Fatalf("resource validation discarded scheduling: %+v", profile)
			}
			if tt.valid {
				if profile.Resources == nil || profile.resourcesErr != nil {
					t.Fatalf("valid resource quantities rejected: %+v", profile)
				}
			} else if profile.Resources != nil || profile.resourcesErr == nil {
				t.Fatalf("invalid quantities did not drop only resources: %+v", profile)
			}
			pod, err := exec.CreatePodSpec(&types.ExecutionRequest{JobID: 123, Command: types.Sync}, "/tmp/job", "connector:test", nil)
			if err != nil {
				t.Fatal(err)
			}
			want := corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
			}
			if tt.valid {
				want = corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}}
				if tt.request != "" {
					want.Requests = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse(strings.Trim(tt.request, `"`))}
				}
				if tt.limit != "" {
					want.Limits[corev1.ResourceEphemeralStorage] = resource.MustParse(strings.Trim(tt.limit, `"`))
				}
			}
			if got := pod.Spec.Containers[0].Resources; !apiequality.Semantic.DeepEqual(got, want) {
				t.Errorf("effective pod resources = %+v, want %+v", got, want)
			}
			if pod.Spec.NodeSelector["pool"] != "disk" {
				t.Errorf("effective pod scheduling was lost: %v", pod.Spec.NodeSelector)
			}
		})
	}
}

func TestLoadJobProfilesEphemeralStorageFallbackPodResources(t *testing.T) {
	for _, tt := range []struct {
		name    string
		profile string
		op      types.Command
		want    string
	}{
		{"sync uses own resources", `"resources":{"requests":{"ephemeral-storage":"4Gi"},"limits":{"ephemeral-storage":"8Gi"}}`, types.Sync, "own"},
		{"clear destination uses own resources", `"resources":{"requests":{"ephemeral-storage":"4Gi"},"limits":{"ephemeral-storage":"8Gi"}}`, types.ClearDestination, "own"},
		{"discover ignores own resources", `"resources":{"requests":{"ephemeral-storage":"4Gi"},"limits":{"ephemeral-storage":"8Gi"}}`, types.Discover, "default"},
		{"missing resources fall back", `"nodeSelector":{"pool":"disk"}`, types.Sync, "default"},
		{"empty resources fall back", `"resources":{}`, types.Sync, "default"},
		{"invalid resources fall back", `"resources":{"limits":{"ephemeral-storage":"0Gi"}}`, types.Sync, "default"},
		{"CPU and memory only profile remains valid", `"resources":{"requests":{"cpu":"250m","memory":"256Mi"},"limits":{"memory":"1Gi"}}`, types.Sync, "legacy"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			exec := runtimeMemoryExecutor(fmt.Sprintf(`{
				"0":{"resources":{"requests":{"cpu":"500m","memory":"1Gi","ephemeral-storage":"1Gi"},"limits":{"memory":"2Gi","ephemeral-storage":"2Gi"}}},
				"123":{%s}
			}`, tt.profile))
			pod, err := exec.CreatePodSpec(&types.ExecutionRequest{JobID: 123, Command: tt.op}, "/tmp/job", "connector:test", nil)
			if err != nil {
				t.Fatal(err)
			}
			want := corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("2Gi")},
			}
			switch tt.want {
			case "own":
				want = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("4Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("8Gi")},
				}
			case "legacy":
				want = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
				}
			}
			if got := pod.Spec.Containers[0].Resources; !apiequality.Semantic.DeepEqual(got, want) {
				t.Errorf("effective pod resources = %+v, want %+v", got, want)
			}
		})
	}
	t.Run("invalid default and job resources use built-in defaults", func(t *testing.T) {
		exec := runtimeMemoryExecutor(`{"0":{"resources":{"limits":{"ephemeral-storage":0}}},"123":{"resources":{"requests":{"ephemeral-storage":"3Gi"},"limits":{"ephemeral-storage":"2Gi"}}}}`)
		pod, err := exec.CreatePodSpec(&types.ExecutionRequest{JobID: 123, Command: types.Sync}, "/tmp/job", "connector:test", nil)
		if err != nil {
			t.Fatal(err)
		}
		want := corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
		}
		if got := pod.Spec.Containers[0].Resources; !apiequality.Semantic.DeepEqual(got, want) {
			t.Errorf("effective built-in pod resources = %+v, want %+v", got, want)
		}
	})
}
