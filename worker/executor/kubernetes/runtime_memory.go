package kubernetes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var maxRuntimeMemoryQuantity = *resource.NewQuantity(math.MaxInt64, resource.DecimalSI)

// RuntimeMemoryConfig reserves at least 20% of the container limit for native memory and headroom.
// GoLimit and GoPercent are mutually exclusive; JavaPercent is always required.
type RuntimeMemoryConfig struct {
	GoLimit     string `json:"goLimit,omitempty"`
	GoPercent   int    `json:"goPercent,omitempty"`
	JavaPercent int    `json:"javaPercent"`
}

func parseRuntimeMemory(raw json.RawMessage) (*RuntimeMemoryConfig, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("runtimeMemory must be an object")
	}
	_, hasLimit := fields["goLimit"]
	_, hasPercent := fields["goPercent"]
	if hasLimit == hasPercent {
		return nil, fmt.Errorf("runtimeMemory requires exactly one of goLimit or goPercent")
	}
	for name, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("runtimeMemory.%s must not be null", name)
		}
	}
	var policy RuntimeMemoryConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return nil, fmt.Errorf("invalid runtimeMemory: %w", err)
	}
	if _, err := policy.validate(); err != nil {
		return nil, err
	}
	return &policy, nil
}

// validate returns the absolute Go budget, or zero when Go uses a percentage.
func (p RuntimeMemoryConfig) validate() (int64, error) {
	if (p.GoLimit != "") == (p.GoPercent != 0) {
		return 0, fmt.Errorf("runtimeMemory requires exactly one of goLimit or goPercent")
	}
	if p.JavaPercent < 1 || p.JavaPercent > 80 {
		return 0, fmt.Errorf("runtimeMemory.javaPercent must be an integer in 1..80")
	}
	if p.GoLimit != "" {
		quantity, err := resource.ParseQuantity(p.GoLimit)
		if err != nil {
			return 0, fmt.Errorf("invalid runtimeMemory.goLimit: %w", err)
		}
		goBytes, err := positiveMemoryBytes(quantity)
		if err != nil {
			return 0, fmt.Errorf("invalid runtimeMemory.goLimit: %w", err)
		}
		return goBytes, nil
	}
	if p.GoPercent < 1 || p.GoPercent > 80 || p.GoPercent+p.JavaPercent > 80 {
		return 0, fmt.Errorf("runtimeMemory percentages must be positive and total at most 80")
	}
	return 0, nil
}

func positiveMemoryBytes(quantity resource.Quantity) (int64, error) {
	if quantity.Sign() <= 0 || quantity.Cmp(maxRuntimeMemoryQuantity) > 0 {
		return 0, fmt.Errorf("memory quantity must resolve to positive int64 bytes")
	}
	return quantity.Value(), nil
}

// Divide first to avoid overflowing on large container limits. The result rounds down.
func memoryPercent(limit int64, percent int) int64 {
	return (limit/100)*int64(percent) + (limit%100)*int64(percent)/100
}

func (p RuntimeMemoryConfig) resolve(resources corev1.ResourceRequirements) ([]corev1.EnvVar, error) {
	goBytes, err := p.validate()
	if err != nil {
		return nil, err
	}
	limit, exists := resources.Limits[corev1.ResourceMemory]
	if !exists {
		return nil, fmt.Errorf("runtimeMemory requires a finite container resources.limits.memory")
	}
	limitBytes, err := positiveMemoryBytes(limit)
	if err != nil {
		return nil, fmt.Errorf("runtimeMemory requires a finite positive container memory limit: %w", err)
	}
	if p.GoLimit == "" {
		goBytes = memoryPercent(limitBytes, p.GoPercent)
	}
	// This comparison includes the JVM's fractional-byte percentage without rounding it down.
	if goBytes <= 0 || goBytes > memoryPercent(limitBytes, 80-p.JavaPercent) {
		return nil, fmt.Errorf("runtimeMemory Go and Java budgets must be positive and total at most 80%% of the container memory limit")
	}
	return []corev1.EnvVar{
		{Name: "GOMEMLIMIT", Value: strconv.FormatInt(goBytes, 10)},
		{Name: "OLAKE_JAVA_HEAP_PERCENT", Value: strconv.Itoa(p.JavaPercent)},
	}, nil
}

// Resolve resources and policy from the same ConfigMap snapshot. A missing policy can
// fall back, but an invalid policy or invalid resources used by a policy never can.
func (k *KubernetesExecutor) resourcesAndRuntimeEnvForJob(jobID int, operation types.Command) (corev1.ResourceRequirements, []corev1.EnvVar, error) {
	w := k.configWatcher
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.jobProfilesErr != nil {
		return corev1.ResourceRequirements{}, nil, w.jobProfilesErr
	}
	profiles := []int{0}
	if utils.IsAsyncCommand(operation) && jobID != 0 {
		profiles = []int{jobID, 0}
	}
	var policy *RuntimeMemoryConfig
	for _, id := range profiles {
		profile := w.jobProfiles[id]
		if profile.runtimeMemoryErr != nil {
			return corev1.ResourceRequirements{}, nil, fmt.Errorf("job profile %d: %w", id, profile.runtimeMemoryErr)
		}
		if profile.RuntimeMemory != nil {
			policy = profile.RuntimeMemory
			break
		}
	}
	resources := defaultJobResources()
	for _, id := range profiles {
		profile := w.jobProfiles[id]
		if policy != nil && profile.resourcesErr != nil {
			return corev1.ResourceRequirements{}, nil, fmt.Errorf("job profile %d resources used by runtimeMemory: %w", id, profile.resourcesErr)
		}
		if hasResources(profile.Resources) {
			resources = *profile.Resources.DeepCopy()
			break
		}
	}
	if policy == nil {
		return resources, nil, nil
	}
	if err := validateResources(&resources); err != nil {
		return corev1.ResourceRequirements{}, nil, fmt.Errorf("invalid resources used by runtimeMemory: %w", err)
	}
	env, err := policy.resolve(resources)
	return resources, env, err
}
