package artifact

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalArtifactStorePutOpenRoundTrip(t *testing.T) {
	store, err := NewLocalArtifactStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	const data = "large payload lives outside job metadata"
	uri, err := store.Put(ctx, "runs/run-1/input.json", strings.NewReader(data), int64(len(data)), "application/json")
	if err != nil {
		t.Fatalf("Put returned an error: %v", err)
	}
	if uri != "local://runs/run-1/input.json" {
		t.Fatalf("Put URI = %q, want local://runs/run-1/input.json", uri)
	}
	reader, err := store.Open(ctx, uri)
	if err != nil {
		t.Fatalf("Open returned an error: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading artifact returned an error: %v", err)
	}
	if string(got) != data {
		t.Fatalf("artifact = %q, want %q", got, data)
	}
}

func TestLocalArtifactStoreRejectsTraversalAndOversize(t *testing.T) {
	store, err := NewLocalArtifactStore(t.TempDir(), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if _, err := store.Open(context.Background(), "local://objects/%2e%2e/secret"); err == nil {
		t.Fatal("Open accepted a traversal URI")
	}
	if _, err := store.Put(context.Background(), "runs/run-1/output", strings.NewReader("12345"), 5, "text/plain"); !errors.Is(err, errObjectTooLarge) {
		t.Fatalf("Put oversize error = %v, want errObjectTooLarge", err)
	}
	if _, err := store.Put(context.Background(), "runs/run-1/output", strings.NewReader("short"), 4, "text/plain"); err == nil {
		t.Fatal("Put accepted a body whose size differs from the declared size")
	}
}

func TestLocalArtifactStoreRootDoesNotFollowSymlinkOutside(t *testing.T) {
	rootPath := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "escape")); err != nil {
		t.Skipf("symlink creation is unavailable: %v", err)
	}
	store, err := NewLocalArtifactStore(rootPath, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.Open(context.Background(), "local://escape/secret"); err == nil {
		t.Fatal("Open followed a symlink outside the artifact root")
	}
}

func TestS3ArtifactStoreRequiresBucketAndInitializesLazily(t *testing.T) {
	if _, err := NewS3ArtifactStore(S3Config{}, 1024); err == nil {
		t.Fatal("NewS3ArtifactStore accepted an empty bucket")
	}
	store, err := NewS3ArtifactStore(S3Config{Bucket: "atlas-artifacts", Endpoint: "http://minio:9000"}, 1024)
	if err != nil {
		t.Fatalf("NewS3ArtifactStore returned an error: %v", err)
	}
	if store.client != nil {
		t.Fatal("S3 client was initialized before the first object operation")
	}
}

func TestS3ArtifactStoreUsesConfiguredPathStyleEndpoint(t *testing.T) {
	var stored []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/atlas-artifacts/runs/run-1/output.json" {
			t.Errorf("S3 request path = %q, want path-style bucket and key", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodPut:
			var err error
			stored, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read S3 upload: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write(stored)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	objects, err := NewS3ArtifactStore(S3Config{
		Bucket: "atlas-artifacts", Region: "us-east-1", Endpoint: server.URL,
		AccessKey: "test-access", SecretKey: "test-secret",
	}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	const data = `{"prediction":"positive"}`
	uri, err := objects.Put(context.Background(), "runs/run-1/output.json", strings.NewReader(data), int64(len(data)), "application/json")
	if err != nil {
		t.Fatalf("Put through S3 endpoint: %v", err)
	}
	if uri != "s3://atlas-artifacts/runs/run-1/output.json" || string(stored) != data {
		t.Fatalf("S3 URI/upload = %q/%q, want expected URI and bytes", uri, stored)
	}
	reader, err := objects.Open(context.Background(), uri)
	if err != nil {
		t.Fatalf("Open through S3 endpoint: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != data {
		t.Fatalf("S3 read = %q, %v; want original bytes", got, err)
	}
}

func TestStageS3BodyChecksDeclaredSizeBeforeUpload(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
		size int64
	}{
		{name: "short", body: "abc", size: 4},
		{name: "long", body: "abcde", size: 4},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			f, cleanup, err := stageS3Body(context.Background(), strings.NewReader(testCase.body), testCase.size, 8)
			if err == nil {
				if cleanup != nil {
					cleanup()
				}
				t.Fatal("stageS3Body accepted a source with the wrong declared size")
			}
			if f != nil || cleanup != nil {
				t.Fatal("stageS3Body returned a staging file after rejecting the source")
			}
		})
	}
}
