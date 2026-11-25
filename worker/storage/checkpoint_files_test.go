package storage

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/stretchr/testify/require"
)

func TestListCheckpointFilesLocal(t *testing.T) {
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeNFS)
	dir := t.TempDir()
	names, err := ListFiles(context.Background(), dir, "ordinary-recovery-checkpoints")
	require.NoError(t, err)
	require.Empty(t, names)
	for _, name := range []string{"runtime-0-200.json", "runtime-0-150.json"} {
		require.NoError(t, WriteFileAtomic(context.Background(), dir, filepath.Join("ordinary-recovery-checkpoints", name), `{}`))
	}
	names, err = ListFiles(context.Background(), dir, "ordinary-recovery-checkpoints")
	require.NoError(t, err)
	require.Equal(t, []string{"runtime-0-150.json", "runtime-0-200.json"}, names)
	require.NoError(t, os.Symlink(filepath.Join(dir, "ordinary-recovery-checkpoints", names[0]), filepath.Join(dir, "ordinary-recovery-checkpoints", "foreign.json")))
	_, err = ListFiles(context.Background(), dir, "ordinary-recovery-checkpoints")
	require.Error(t, err)
}

func TestListCheckpointFilesS3PaginationAndNamespace(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprint(nested), func(t *testing.T) {
			requests := 0
			var expectedPrefix string
			dir := s3FenceFixture(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				require.Equal(t, http.MethodGet, request.Method)
				require.Equal(t, "2", request.URL.Query().Get("list-type"))
				require.Equal(t, expectedPrefix, request.URL.Query().Get("prefix"))
				requests++
				w.Header().Set("Content-Type", "application/xml")
				if requests == 1 {
					_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page-two</NextContinuationToken><Contents><Key>%sruntime-0-150.json</Key></Contents></ListBucketResult>`, expectedPrefix)
					return
				}
				require.Equal(t, "page-two", request.URL.Query().Get("continuation-token"))
				name := "runtime-0-200.json"
				if nested {
					name = "foreign/runtime-0-200.json"
				}
				_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%s%s</Key></Contents></ListBucketResult>`, expectedPrefix, name)
			}))
			var err error
			expectedPrefix, err = S3Key(dir, "ordinary-recovery-checkpoints", true)
			require.NoError(t, err)
			names, err := ListFiles(context.Background(), dir, "ordinary-recovery-checkpoints")
			if nested {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"runtime-0-150.json", "runtime-0-200.json"}, names)
			}
			require.Equal(t, 2, requests)
		})
	}
}
