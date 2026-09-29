package artifact

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Config struct {
	Bucket       string
	Region       string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

// S3ArtifactStore supports Amazon S3 and S3-compatible endpoints such as
// MinIO. It validates configuration eagerly but loads SDK credentials and
// constructs the client only on the first object operation.
type S3ArtifactStore struct {
	config  S3Config
	maxSize int64

	mu     sync.Mutex
	client *s3.Client
}

func NewS3ArtifactStore(cfg S3Config, maxSize int64) (*S3ArtifactStore, error) {
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	if cfg.Bucket == "" || strings.ContainsAny(cfg.Bucket, "/:@") {
		return nil, fmt.Errorf("S3 artifact bucket is required and must be a bucket name")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("S3 endpoint must be an http(s) URL without credentials, query, or fragment")
		}
		cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")
	}
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, fmt.Errorf("S3 access key and secret key must be configured together")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxObjectBytes
	}
	return &S3ArtifactStore{config: cfg, maxSize: maxSize}, nil
}

func (s *S3ArtifactStore) clientFor(ctx context.Context) (*s3.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return s.client, nil
	}
	options := []func(*config.LoadOptions) error{config.WithRegion(s.config.Region)}
	if s.config.AccessKey != "" {
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s.config.AccessKey, s.config.SecretKey, s.config.SessionToken)))
	}
	sdkConfig, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load S3 client configuration: %w", err)
	}
	s.client = s3.NewFromConfig(sdkConfig, func(options *s3.Options) {
		if s.config.Endpoint != "" {
			options.BaseEndpoint = aws.String(s.config.Endpoint)
			// MinIO and most self-hosted S3-compatible services expect
			// http(s)://endpoint/bucket/key path-style addressing.
			options.UsePathStyle = true
		}
	})
	return s.client, nil
}

func (s *S3ArtifactStore) Open(ctx context.Context, rawURI string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bucket, key, err := parseS3URI(rawURI)
	if err != nil {
		return nil, err
	}
	if bucket != s.config.Bucket {
		return nil, fmt.Errorf("S3 artifact URI bucket does not match configured bucket")
	}
	client, err := s.clientFor(ctx)
	if err != nil {
		return nil, err
	}
	result, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("get S3 artifact: %w", err)
	}
	if result.ContentLength != nil && *result.ContentLength > s.maxSize {
		_ = result.Body.Close()
		return nil, errObjectTooLarge
	}
	return &maxReadCloser{reader: contextReader{ctx: ctx, r: result.Body}, closer: result.Body, left: s.maxSize}, nil
}

func (s *S3ArtifactStore) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) (string, error) {
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
	staged, cleanup, err := stageS3Body(ctx, body, size, s.maxSize)
	if err != nil {
		return "", err
	}
	defer cleanup()
	client, err := s.clientFor(ctx)
	if err != nil {
		return "", err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.config.Bucket),
		Key:           aws.String(key),
		Body:          staged,
		ContentLength: aws.Int64(size),
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	if _, err := client.PutObject(ctx, input); err != nil {
		return "", fmt.Errorf("put S3 artifact: %w", err)
	}
	return "s3://" + s.config.Bucket + "/" + key, nil
}

// stageS3Body verifies the caller's byte count before sending a PutObject. The
// temporary file bounds memory use while detecting both short and long sources.
func stageS3Body(ctx context.Context, body io.Reader, size, maxSize int64) (*os.File, func(), error) {
	f, err := os.CreateTemp("", "atlas-artifact-upload-*.tmp")
	if err != nil {
		return nil, nil, fmt.Errorf("create S3 upload staging file: %w", err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	written, err := io.Copy(f, io.LimitReader(contextReader{ctx: ctx, r: body}, maxSize+1))
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("stage S3 artifact: %w", err)
	}
	if written > maxSize {
		cleanup()
		return nil, nil, errObjectTooLarge
	}
	if written != size {
		cleanup()
		return nil, nil, fmt.Errorf("artifact size mismatch: declared %d bytes, read %d", size, written)
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("rewind S3 artifact staging file: %w", err)
	}
	return f, cleanup, nil
}

func parseS3URI(raw string) (bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", "", fmt.Errorf("invalid S3 artifact URI")
	}
	key, err = url.PathUnescape(strings.TrimPrefix(u.EscapedPath(), "/"))
	if err != nil || validateKey(key) != nil {
		return "", "", fmt.Errorf("invalid S3 artifact URI key")
	}
	return u.Host, key, nil
}
