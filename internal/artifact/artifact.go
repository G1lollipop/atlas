// Package artifact stores potentially large job inputs and outputs outside the
// job metadata database.
package artifact

import (
	"context"
	"io"
)

// ArtifactStore reads and writes objects addressed by URI. Put requires the
// caller to provide the exact content size so backends can enforce object
// limits before accepting the artifact.
type ArtifactStore interface {
	Open(ctx context.Context, uri string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) (uri string, err error)
}

const DefaultMaxObjectBytes int64 = 16 << 20
