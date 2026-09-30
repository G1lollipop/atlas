package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
)

var errObjectTooLarge = errors.New("artifact exceeds configured size limit")

// validateKey limits names to portable path segments, so the same artifact URI
// has the same meaning on Windows, Linux, local storage, and S3.
func validateKey(key string) error {
	if key == "" || len(key) > 1024 || strings.HasPrefix(key, "/") || strings.ContainsAny(key, `\:`) {
		return fmt.Errorf("invalid artifact key")
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid artifact key")
		}
		for _, r := range part {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune("-_.~", r) {
				return fmt.Errorf("invalid artifact key")
			}
		}
	}
	return nil
}

func parseLocalURI(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "local" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("invalid local artifact URI")
	}
	path, err := url.PathUnescape(u.EscapedPath())
	if err != nil {
		return "", fmt.Errorf("invalid local artifact URI: %w", err)
	}
	key := u.Host + strings.TrimPrefix(path, "/")
	if path != "" {
		key = u.Host + "/" + strings.TrimPrefix(path, "/")
	}
	if err := validateKey(key); err != nil {
		return "", fmt.Errorf("invalid local artifact URI: %w", err)
	}
	return key, nil
}

func localURI(key string) string {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 1 {
		return "local://" + parts[0]
	}
	return "local://" + parts[0] + "/" + parts[1]
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// maxReadCloser reports an error instead of silently truncating an object that
// exceeds max. The extra byte is probed only when the caller reaches the limit.
type maxReadCloser struct {
	reader io.Reader
	closer io.Closer
	left   int64
	probed bool
}

func (r *maxReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.left > 0 {
		if int64(len(p)) > r.left {
			p = p[:r.left]
		}
		n, err := r.reader.Read(p)
		r.left -= int64(n)
		return n, err
	}
	if r.probed {
		return 0, io.EOF
	}
	r.probed = true
	var extra [1]byte
	n, err := io.ReadFull(r.reader, extra[:])
	if n > 0 {
		return 0, errObjectTooLarge
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return 0, io.EOF
	}
	return 0, err
}

func (r *maxReadCloser) Close() error { return r.closer.Close() }
