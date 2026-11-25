package utils

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestNormalizePodLogLinePreservesRuntimeMessages(t *testing.T) {
	const timestamp = "2026-11-24T23:52:09.123456789Z"
	for _, message := range []string{
		"panic: concurrent map writes", "fatal error: out of memory", "goroutine 206 [running]:",
		"\truntime.throw({0x1, 0x2})", "    /usr/local/go/src/runtime/panic.go:1067 +0x48  ",
		"", "\t  ", "\x1b[31mpanic: colored\x1b[0m", "trailing carriage return\r",
	} {
		t.Run(message, func(t *testing.T) {
			line, ts, ok := NormalizePodLogLine(timestamp + " " + message + "\n")
			require.True(t, ok)
			require.Equal(t, timestamp, ts.Format(time.RFC3339Nano))
			var entry map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			require.Len(t, entry, 2)
			var actual string
			require.NoError(t, json.Unmarshal(entry["message"], &actual))
			require.Equal(t, message, actual)
			require.NotContains(t, entry, "level")
			require.NotContains(t, entry, "seq")
		})
	}
	const existing = `{"level":"info","seq":37,"message":"existing JSON","nested":{"value":true}}`
	line, _, ok := NormalizePodLogLine(timestamp + " " + existing + "\n")
	require.True(t, ok)
	require.Equal(t, existing+"\n", line)
}

func TestStreamCheckpointRetainsSequenceFreeLinesWithoutChangingJSONFence(t *testing.T) {
	ts := time.Date(2026, 11, 24, 23, 52, 9, 0, time.UTC)
	checkpoint := newStreamCheckpoint(37, ts)
	require.False(t, checkpoint.accept(podLogLineEntry{Seq: 36, PodLogTimestamp: ts}))
	require.True(t, checkpoint.accept(podLogLineEntry{PodLogTimestamp: ts}))
	require.False(t, checkpoint.accept(podLogLineEntry{Seq: 37, PodLogTimestamp: ts}))
	require.True(t, checkpoint.accept(podLogLineEntry{Seq: 35, PodLogTimestamp: ts}))
}
func TestConnectorLogCollectorPersistsPanicAcrossReconnectCatchUpAndResume(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	objects := make(map[string]string)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/logs-bucket/")
		switch {
		case r.Method == http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			objects[key] = string(body)
			w.Header().Set("ETag", `"complete"`)
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			listing := struct {
				XMLName  xml.Name `xml:"ListBucketResult"`
				Contents []struct {
					Key string `xml:"Key"`
				} `xml:"Contents"`
			}{}
			for key := range objects {
				if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
					listing.Contents = append(listing.Contents, struct {
						Key string `xml:"Key"`
					}{Key: key})
				}
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(listing)
		case r.Method == http.MethodGet:
			body, ok := objects[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = io.WriteString(w, body)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	for key, value := range map[string]string{
		constants.EnvStorageMode:           constants.StorageModeS3,
		constants.EnvKubernetesServiceHost: "", constants.EnvS3Bucket: "logs-bucket",
		constants.EnvS3Endpoint: server.URL, constants.EnvS3Prefix: "runtime-logs",
		constants.EnvS3Region: "us-east-1", constants.EnvS3AccessKeyID: "test-key",
		constants.EnvS3SecretAccessKey: "test-secret",
	} {
		previous := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, previous) })
	}
	require.NoError(t, storage.Init(ctx))
	workDir := filepath.Join(storage.ConfigDir(), filepath.Base(t.TempDir()))
	const firstTS = "2026-11-24T23:52:09.100000001Z"
	const lastTS = "2026-11-24T23:52:09.900000001Z"
	const existing = `{"level":"info","seq":37,"message":"finalizing Parquet"}`
	jsonLine := firstTS + " " + existing + "\n"
	panicLine := firstTS + " panic: concurrent map writes\n"
	stack := lastTS + " \n" + lastTS + " goroutine 206 [running]:\n" +
		lastTS + " \tparquet.(*GenericWriter).Close()\n" + lastTS + "     /app/writer.go:42 +0x17  \n"
	unprefixed := "\tcreated by main.run\n\n"
	calls := 0
	stream := func(_ context.Context, since time.Time, follow bool) (io.Reader, error) {
		calls++
		switch calls {
		case 1:
			require.True(t, follow)
			require.True(t, since.IsZero())
			return strings.NewReader(jsonLine), nil
		case 2:
			require.True(t, follow)
			require.Equal(t, firstTS, since.Format(time.RFC3339Nano))
			return strings.NewReader(panicLine), nil
		case 3:
			require.False(t, follow)
			require.Equal(t, firstTS, since.Format(time.RFC3339Nano))
			return strings.NewReader(jsonLine + panicLine + stack + unprefixed), nil
		case 4:
			require.False(t, follow)
			require.Equal(t, lastTS, since.Format(time.RFC3339Nano))
			return strings.NewReader(jsonLine + panicLine + stack + lastTS + " fatal error: terminal tail\n"), nil
		default:
			t.Fatalf("unexpected stream call %d", calls)
			return nil, nil
		}
	}
	collector, err := NewConnectorLogCollector(ctx, workDir, stream, nil)
	require.NoError(t, err)
	require.NoError(t, collector.runStream(ctx, true))
	require.NoError(t, collector.buffer.Flush(ctx))
	require.NoError(t, collector.runStream(ctx, true))
	require.Zero(t, collector.lastPodLogSeq.Load())
	require.NoError(t, collector.catchUpAndFlush())
	restarted, err := NewConnectorLogCollector(ctx, workDir, stream, nil)
	require.NoError(t, err)
	require.Zero(t, restarted.lastPodLogSeq.Load())
	require.NoError(t, restarted.catchUpAndFlush())

	prefix, err := storage.S3Key(workDir, "logs", true)
	require.NoError(t, err)
	listed, err := storage.ListS3Objects(ctx, prefix)
	require.NoError(t, err)
	require.Len(t, listed, 3)
	sort.Slice(listed, func(i, j int) bool { return listed[i].Key < listed[j].Key })
	var messages []string
	for _, object := range listed {
		relative := strings.TrimPrefix(object.Key, strings.TrimSuffix(prefix, "logs/"))
		data, err := storage.ReadFile(ctx, workDir, relative, false)
		require.NoError(t, err)
		for _, line := range strings.Split(strings.TrimSuffix(data, "\n"), "\n") {
			var entry map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			var message string
			require.NoError(t, json.Unmarshal(entry["message"], &message))
			messages = append(messages, message)
			if message == "finalizing Parquet" {
				require.Equal(t, existing, line)
			} else {
				require.NotContains(t, entry, "level")
				require.NotContains(t, entry, "seq")
			}
		}
	}
	require.Equal(t, []string{
		"finalizing Parquet",
		"panic: concurrent map writes", "finalizing Parquet", "panic: concurrent map writes", "",
		"goroutine 206 [running]:", "\tparquet.(*GenericWriter).Close()", "    /app/writer.go:42 +0x17  ",
		"\tcreated by main.run", "",
		"finalizing Parquet", "panic: concurrent map writes", "", "goroutine 206 [running]:",
		"\tparquet.(*GenericWriter).Close()", "    /app/writer.go:42 +0x17  ", "fatal error: terminal tail",
	}, messages)
	require.Equal(t, 4, calls)
}
