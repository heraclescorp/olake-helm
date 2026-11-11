package docker

import (
	"testing"

	"github.com/datazip-inc/olake-helm/worker/types"
)

func TestEnrollmentPreflightNeverCreatesAnIndexMount(t *testing.T) {
	exec := &DockerExecutor{}
	// DiscoverNewTables is metadata-only even when a caller carries a stale true
	// index flag. This must return before touching any persistence-root directory.
	index, err := exec.ensureIndexMount(42, types.DiscoverNewTables, true)
	if err != nil || index != nil {
		t.Fatalf("metadata preflight index mount = %v, %v", index, err)
	}
}
