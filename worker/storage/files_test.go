package storage

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	openFence   = `{"version":1,"execution_id":"runtime-1","state":"open"}`
	closedFence = `{"version":1,"execution_id":"runtime-1","state":"closed"}`
)

// Storage configuration and the S3 client are process globals. These tests must
// not run in parallel with one another.
func setStorageSetting(t *testing.T, key string, value interface{}) {
	t.Helper()
	previous := viper.Get(key)
	viper.Set(key, value)
	t.Cleanup(func() { viper.Set(key, previous) })
}

func TestCreateFileIfAbsentPreservesExistingCompleteFile(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	ctx := context.Background()
	dir := t.TempDir()
	filename := filepath.Join("nested", "execution-fence.json")

	created, err := CreateFileIfAbsent(ctx, dir, filename, openFence)
	require.NoError(t, err)
	require.True(t, created)
	created, err = CreateFileIfAbsent(ctx, dir, filename, closedFence)
	require.NoError(t, err)
	require.False(t, created)
	data, err := ReadFile(ctx, dir, filename, true)
	require.NoError(t, err)
	require.Equal(t, openFence, data)
	info, err := os.Stat(filepath.Join(dir, filename))
	require.NoError(t, err)
	require.Equal(t, fs.FileMode(constants.DefaultFilePermissions), info.Mode().Perm())
}

func TestLocalExecutionFenceOpenClosedOrdering(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	ctx := context.Background()
	for _, openFirst := range []bool{true, false} {
		name := "closed before open"
		if openFirst {
			name = "open before closed"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if openFirst {
				created, err := CreateFileIfAbsent(ctx, dir, "execution-fence.json", openFence)
				require.NoError(t, err)
				require.True(t, created)
			}
			require.NoError(t, WriteFileAtomic(ctx, dir, "execution-fence.json", closedFence))
			created, err := CreateFileIfAbsent(ctx, dir, "execution-fence.json", openFence)
			require.NoError(t, err)
			require.False(t, created)
			data, err := ReadFile(ctx, dir, "execution-fence.json", true)
			require.NoError(t, err)
			require.Equal(t, closedFence, data)
		})
	}
}

func TestDelayedPreparedLocalOpenCannotReplaceClosed(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "execution-fence.json")
	prepared := make(chan error, 1)
	publish := make(chan struct{})
	type publication struct {
		created bool
		err     error
	}
	result := make(chan publication, 1)
	go func() {
		tempPath, err := prepareLocalFile(ctx, filePath, openFence)
		prepared <- err
		if err != nil {
			return
		}
		<-publish
		created, err := publishLocalFile(ctx, tempPath, filePath, true)
		_ = os.Remove(tempPath)
		result <- publication{created: created, err: err}
	}()
	require.NoError(t, <-prepared)
	// The old launcher has completely prepared OPEN but has not linked it.
	// Cleanup publishes and acknowledges CLOSED before that launcher resumes.
	closeErr := WriteFileAtomic(ctx, dir, "execution-fence.json", closedFence)
	close(publish)
	outcome := <-result
	require.NoError(t, closeErr)
	require.NoError(t, outcome.err)
	require.False(t, outcome.created)
	data, err := ReadFile(ctx, dir, "execution-fence.json", true)
	require.NoError(t, err)
	require.Equal(t, closedFence, data)
}

func TestLocalAtomicPublicationReadersSeeCompleteFiles(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	ctx := context.Background()
	dir := t.TempDir()
	filename := "execution-fence.json"
	filePath := filepath.Join(dir, filename)
	oldData := `{"payload":"` + strings.Repeat("old", 65536) + `"}`
	newData := `{"payload":"` + strings.Repeat("new", 32768) + `"}`

	tempPath, err := prepareLocalFile(ctx, filePath, oldData)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(tempPath) })
	_, err = ReadFile(ctx, dir, filename, false)
	require.ErrorIs(t, err, fs.ErrNotExist, "a prepared file is not yet visible")
	created, err := publishLocalFile(ctx, tempPath, filePath, true)
	require.NoError(t, err)
	require.True(t, created)
	data, err := ReadFile(ctx, dir, filename, true)
	require.NoError(t, err)
	require.Equal(t, oldData, data, "the first reader must see the complete linked file")

	reader, err := os.Open(filePath)
	require.NoError(t, err)
	defer reader.Close()
	prefix := make([]byte, 17)
	_, err = io.ReadFull(reader, prefix)
	require.NoError(t, err)
	require.NoError(t, WriteFileAtomic(ctx, dir, filename, newData))
	remainder, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, oldData, string(prefix)+string(remainder), "replacement never mutates an inode held by a reader")
	data, err = ReadFile(ctx, dir, filename, true)
	require.NoError(t, err)
	require.Equal(t, newData, data, "new readers must see the complete replacement")
}

func TestCancelledAtomicFilePublicationDoesNotMutateDestination(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	created, err := CreateFileIfAbsent(ctx, dir, "execution-fence.json", openFence)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, created)
	require.ErrorIs(t, WriteFileAtomic(ctx, dir, "execution-fence.json", closedFence), context.Canceled)
	_, err = os.Stat(filepath.Join(dir, "execution-fence.json"))
	require.ErrorIs(t, err, fs.ErrNotExist)

	require.NoError(t, WriteFileAtomic(context.Background(), dir, "execution-fence.json", closedFence))
	tempPath, err := prepareLocalFile(context.Background(), filepath.Join(dir, "execution-fence.json"), openFence)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(tempPath) })
	created, err = publishLocalFile(ctx, tempPath, filepath.Join(dir, "execution-fence.json"), false)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, created)
	data, err := ReadFile(context.Background(), dir, "execution-fence.json", true)
	require.NoError(t, err)
	require.Equal(t, closedFence, data)
}

func TestExecutionFenceLocation(t *testing.T) {
	for _, kubernetes := range []bool{false, true} {
		name := "docker"
		if kubernetes {
			name = "kubernetes"
		}
		t.Run(name, func(t *testing.T) {
			host := ""
			if kubernetes {
				host = "kubernetes.default.svc"
			}
			setStorageSetting(t, constants.EnvKubernetesServiceHost, host)
			dir := filepath.Join(ConfigDir(), "sync-runtime-1")
			filename := filepath.Join("fences", "execution-fence.json")
			setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
			require.Equal(t, "/mnt/config/fences/execution-fence.json", ExecutionFenceLocation(dir, filename))

			setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeS3)
			setStorageSetting(t, constants.EnvS3Bucket, "fence-bucket")
			setStorageSetting(t, constants.EnvS3Prefix, "/root/prefix/")
			require.Equal(t, "s3://fence-bucket/root/prefix/sync-runtime-1/fences/execution-fence.json", ExecutionFenceLocation(dir, filename))
			setStorageSetting(t, constants.EnvS3Prefix, "")
			require.Equal(t, "s3://fence-bucket/sync-runtime-1/fences/execution-fence.json", ExecutionFenceLocation(dir, filename))
			setStorageSetting(t, constants.EnvS3Bucket, "")
			require.Empty(t, ExecutionFenceLocation(dir, filename))
			setStorageSetting(t, constants.EnvS3Bucket, "fence-bucket")
			require.Empty(t, ExecutionFenceLocation("relative/runtime", filename))
		})
	}
}

func TestWriteFilesPreservesReadersOfPreviousConfig(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	ctx := context.Background()
	dir := t.TempDir()
	configs := []types.JobConfig{{Name: "state.json", Data: openFence}}
	require.NoError(t, WriteFiles(ctx, dir, configs))
	reader, err := os.Open(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	defer reader.Close()
	configs[0].Data = closedFence
	require.NoError(t, WriteFiles(ctx, dir, configs))
	oldData, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, openFence, string(oldData))
	newData, err := ReadFile(ctx, dir, "state.json", true)
	require.NoError(t, err)
	require.Equal(t, closedFence, newData)
}
