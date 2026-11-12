package storage

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
)

// TODO: add support for GCS and Azure Blob storage modes.
// Mode returns OLAKE_STORAGE_MODE from the environment, defaulting to nfs.
func Mode() string {
	mode := strings.ToLower(strings.TrimSpace(viper.GetString(constants.EnvStorageMode)))
	if mode == "" {
		return constants.StorageModeNFS
	}
	return mode
}

// ConfigDir is the directory shared with the connector: the NFS mount, or the root that maps to
// the S3 prefix.
func ConfigDir() string {
	if viper.GetString(constants.EnvKubernetesServiceHost) != "" {
		return constants.K8sPersistentDir
	}
	return constants.DockerPersistentDir
}

// ExecutionFenceLocation returns the connector-visible location of a file in
// its runtime directory. Local providers mount dir itself at /mnt/config;
// S3 uses exactly the same object key mapping as ReadFile and WriteFileAtomic.
// An unresolvable S3 directory or missing bucket returns an empty location and
// must not be used to launch a controlled execution without its positive fence.
func ExecutionFenceLocation(dir, filename string) string {
	if Mode() != constants.StorageModeS3 {
		return filepath.Join(constants.ContainerMountDir, filename)
	}
	key, err := S3Key(dir, filename, false)
	if err != nil {
		return ""
	}
	bucket := viper.GetString(constants.EnvS3Bucket)
	if bucket == "" {
		return ""
	}
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}
