package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/stretchr/testify/require"
)

type fenceS3Request struct {
	path        string
	body        string
	ifNoneMatch string
}

type fenceS3Store struct {
	mu               sync.Mutex
	exists           bool
	data             string
	puts             []fenceS3Request
	beforeOpen       func()
	beforeCloseReply func()
}

func writeS3TestError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test response</Message></Error>", code)
}

func (s *fenceS3Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeS3TestError(w, http.StatusBadRequest, "InvalidRequest")
			return
		}
		conditional := r.Header.Get("If-None-Match")
		if conditional == "*" && s.beforeOpen != nil {
			s.beforeOpen()
		}
		s.mu.Lock()
		s.puts = append(s.puts, fenceS3Request{path: r.URL.Path, body: string(body), ifNoneMatch: conditional})
		if conditional == "*" && s.exists {
			s.mu.Unlock()
			writeS3TestError(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		s.exists = true
		s.data = string(body)
		s.mu.Unlock()
		if conditional == "" && s.beforeCloseReply != nil {
			s.beforeCloseReply()
		}
		w.Header().Set("ETag", `"complete-object"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		s.mu.Lock()
		data, exists := s.data, s.exists
		s.mu.Unlock()
		if !exists {
			writeS3TestError(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, data)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func s3FenceFixture(t *testing.T, handler http.Handler) string {
	t.Helper()
	setStorageSetting(t, constants.EnvStorageMode, constants.StorageModeS3)
	setStorageSetting(t, constants.EnvKubernetesServiceHost, "")
	setStorageSetting(t, constants.EnvS3Bucket, "fence-bucket")
	setStorageSetting(t, constants.EnvS3Prefix, "/root/prefix/")
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	previousClient, previousBucket := s3Client, s3Bucket
	s3Client = s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	s3Bucket = "fence-bucket"
	t.Cleanup(func() { s3Client, s3Bucket = previousClient, previousBucket })
	return filepath.Join(ConfigDir(), "sync-runtime-1")
}

func TestS3ExecutionFenceOpenClosedOrdering(t *testing.T) {
	for _, openFirst := range []bool{true, false} {
		name := "closed before open"
		if openFirst {
			name = "open before closed"
		}
		t.Run(name, func(t *testing.T) {
			store := &fenceS3Store{}
			dir := s3FenceFixture(t, store)
			ctx := context.Background()
			_, err := ReadFile(ctx, dir, "execution-fence.json", false)
			require.ErrorIs(t, err, fs.ErrNotExist)
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
			store.mu.Lock()
			puts := append([]fenceS3Request(nil), store.puts...)
			store.mu.Unlock()
			for _, put := range puts {
				require.Equal(t, "/fence-bucket/root/prefix/sync-runtime-1/execution-fence.json", put.path)
				if put.body == openFence {
					require.Equal(t, "*", put.ifNoneMatch, "every OPEN must use server-side conditional creation")
				} else {
					require.Equal(t, closedFence, put.body)
					require.Empty(t, put.ifNoneMatch, "CLOSED must replace even an existing OPEN")
				}
			}
		})
	}
}

func TestDelayedS3OpenCannotReplaceAcknowledgedClosed(t *testing.T) {
	arrived := make(chan struct{})
	resume := make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	store := &fenceS3Store{beforeOpen: func() {
		close(arrived)
		<-resume
	}}
	dir := s3FenceFixture(t, store)
	t.Cleanup(unblock)
	ctx := context.Background()
	type publication struct {
		created bool
		err     error
	}
	result := make(chan publication, 1)
	go func() {
		created, err := CreateFileIfAbsent(ctx, dir, "execution-fence.json", openFence)
		result <- publication{created: created, err: err}
	}()
	<-arrived
	// OPEN has reached S3, but has not evaluated its condition. A completed
	// CLOSED replacement must win even when OPEN's request resumes afterwards.
	closeErr := WriteFileAtomic(ctx, dir, "execution-fence.json", closedFence)
	unblock()
	outcome := <-result
	require.NoError(t, closeErr)
	require.NoError(t, outcome.err)
	require.False(t, outcome.created)
	data, err := ReadFile(ctx, dir, "execution-fence.json", true)
	require.NoError(t, err)
	require.Equal(t, closedFence, data)
}

func TestS3AtomicReplacementWaitsForAcknowledgementAndReadersSeeCompleteFiles(t *testing.T) {
	committed := make(chan struct{})
	acknowledge := make(chan struct{})
	var acknowledgeOnce sync.Once
	unblock := func() { acknowledgeOnce.Do(func() { close(acknowledge) }) }
	store := &fenceS3Store{beforeCloseReply: func() {
		close(committed)
		<-acknowledge
	}}
	dir := s3FenceFixture(t, store)
	t.Cleanup(unblock)
	ctx := context.Background()
	oldData := `{"state":"open","payload":"` + strings.Repeat("old", 65536) + `"}`
	newData := `{"state":"closed","payload":"` + strings.Repeat("new", 32768) + `"}`
	created, err := CreateFileIfAbsent(ctx, dir, "execution-fence.json", oldData)
	require.NoError(t, err)
	require.True(t, created)
	data, err := ReadFile(ctx, dir, "execution-fence.json", true)
	require.NoError(t, err)
	require.Equal(t, oldData, data)

	result := make(chan error, 1)
	go func() { result <- WriteFileAtomic(ctx, dir, "execution-fence.json", newData) }()
	<-committed
	select {
	case err := <-result:
		unblock()
		t.Fatalf("replacement returned before PutObject acknowledgement: %v", err)
	default:
	}
	data, readErr := ReadFile(ctx, dir, "execution-fence.json", true)
	unblock()
	require.NoError(t, <-result)
	require.NoError(t, readErr)
	require.Equal(t, newData, data)
	store.mu.Lock()
	puts := append([]fenceS3Request(nil), store.puts...)
	store.mu.Unlock()
	require.Len(t, puts, 2, "each publication is one complete PutObject")
	require.Equal(t, oldData, puts[0].body)
	require.Equal(t, "*", puts[0].ifNoneMatch)
	require.Equal(t, newData, puts[1].body)
	require.Empty(t, puts[1].ifNoneMatch)
}

func TestS3AtomicPublicationDoesNotTreatAmbiguousErrorsAsExisting(t *testing.T) {
	for _, response := range []struct {
		name   string
		status int
		code   string
	}{
		{name: "conditional conflict", status: http.StatusConflict, code: "ConditionalRequestConflict"},
		{name: "server error", status: http.StatusInternalServerError, code: "InternalError"},
		{name: "access denied", status: http.StatusForbidden, code: "AccessDenied"},
	} {
		t.Run(response.name, func(t *testing.T) {
			dir := s3FenceFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeS3TestError(w, response.status, response.code)
			}))
			created, err := CreateFileIfAbsent(context.Background(), dir, "execution-fence.json", openFence)
			require.Error(t, err)
			require.False(t, created)
			require.Error(t, WriteFileAtomic(context.Background(), dir, "execution-fence.json", closedFence))
		})
	}
}

func TestS3AtomicReplacementDoesNotSwallowPreconditionFailure(t *testing.T) {
	dir := s3FenceFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeS3TestError(w, http.StatusPreconditionFailed, "PreconditionFailed")
	}))
	created, err := CreateFileIfAbsent(context.Background(), dir, "execution-fence.json", openFence)
	require.NoError(t, err)
	require.False(t, created)
	require.Error(t, WriteFileAtomic(context.Background(), dir, "execution-fence.json", closedFence))
}
