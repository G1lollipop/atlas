package artifact

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LocalArtifactStore stores objects beneath a process-local directory. os.Root
// keeps every path operation beneath the configured root, including when
// untrusted artifact URIs contain traversal segments or symlinks are present.
type LocalArtifactStore struct {
	root    *os.Root
	maxSize int64
}

func NewLocalArtifactStore(rootPath string, maxSize int64) (*LocalArtifactStore, error) {
	if strings.TrimSpace(rootPath) == "" {
		return nil, fmt.Errorf("local artifact root is required")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxObjectBytes
	}
	absRoot, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, fmt.Errorf("resolve local artifact root: %w", err)
	}
	if err := os.MkdirAll(absRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create local artifact root: %w", err)
	}
	root, err := os.OpenRoot(absRoot)
	if err != nil {
		return nil, fmt.Errorf("open local artifact root: %w", err)
	}
	return &LocalArtifactStore{root: root, maxSize: maxSize}, nil
}

func (s *LocalArtifactStore) Close() error {
	if s == nil || s.root == nil {
		return nil
	}
	return s.root.Close()
}

func (s *LocalArtifactStore) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := parseLocalURI(uri)
	if err != nil {
		return nil, err
	}
	f, err := s.root.Open(filepath.FromSlash(key))
	if err != nil {
		return nil, fmt.Errorf("open local artifact: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat local artifact: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("local artifact is not a regular file")
	}
	if info.Size() > s.maxSize {
		_ = f.Close()
		return nil, errObjectTooLarge
	}
	return &maxReadCloser{reader: contextReader{ctx: ctx, r: f}, closer: f, left: s.maxSize}, nil
}

func (s *LocalArtifactStore) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateKey(key); err != nil {
		return "", err
	}
	if body == nil {
		return "", fmt.Errorf("artifact body is required")
	}
	if size < 0 || size > s.maxSize {
		return "", errObjectTooLarge
	}
	cleanKey := filepath.FromSlash(key)
	parent := filepath.Dir(cleanKey)
	if parent != "." {
		if err := s.root.MkdirAll(parent, 0o750); err != nil {
			return "", fmt.Errorf("create artifact directory: %w", err)
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("create temporary artifact name: %w", err)
	}
	tmpPath := filepath.Join(parent, "."+filepath.Base(cleanKey)+"."+hex.EncodeToString(nonce[:])+".tmp")
	f, err := s.root.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create temporary artifact: %w", err)
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = s.root.Remove(tmpPath)
		}
	}()
	written, copyErr := io.Copy(f, io.LimitReader(contextReader{ctx: ctx, r: body}, s.maxSize+1))
	if copyErr != nil {
		return "", fmt.Errorf("write artifact: %w", copyErr)
	}
	if written > s.maxSize {
		return "", errObjectTooLarge
	}
	if written != size {
		return "", fmt.Errorf("artifact size mismatch: declared %d bytes, read %d", size, written)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("sync artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close artifact: %w", err)
	}
	if err := s.root.Rename(tmpPath, cleanKey); err != nil {
		return "", fmt.Errorf("publish artifact: %w", err)
	}
	ok = true
	return localURI(key), nil
}
