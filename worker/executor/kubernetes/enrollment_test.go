package kubernetes

import (
	"context"
	"reflect"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestEnrollmentPreflightUsesDefaultSchedulingAndNeverAnIndexPVC(t *testing.T) {
	base := &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}}
	heavy := &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Gi")}}
	defaultTolerations := []corev1.Toleration{{Key: "metadata", Operator: corev1.TolerationOpExists}}
	exec := newTestExecutor(map[int]JobSchedulingConfig{
		0:  {NodeSelector: map[string]string{"pool": "default"}, Resources: base, Tolerations: defaultTolerations},
		42: {NodeSelector: map[string]string{"pool": "heavy"}, Resources: heavy, Tolerations: []corev1.Toleration{{Key: "ingestion", Operator: corev1.TolerationOpExists}}},
	})
	if got := exec.GetNodeSelectorForJob(42, types.DiscoverNewTables); !reflect.DeepEqual(got, map[string]string{"pool": "default"}) {
		t.Fatalf("preflight selector = %v", got)
	}
	if got := exec.GetResourcesForJob(42, types.DiscoverNewTables); !reflect.DeepEqual(got, *base) {
		t.Fatalf("preflight resources = %v", got)
	}
	if got := exec.GetTolerationsForJob(42, types.DiscoverNewTables); !reflect.DeepEqual(got, defaultTolerations) {
		t.Fatalf("preflight tolerations = %v", got)
	}
	// No Kubernetes client is installed: any attempt to resolve/provision a PVC
	// would fail. Even a stale true flag cannot provision metadata-only storage.
	volume, err := exec.ensureIndexVolume(context.Background(), 42, types.DiscoverNewTables, true, nil)
	if err != nil || volume != nil {
		t.Fatalf("metadata preflight index volume = %v, %v", volume, err)
	}
}
