package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode"

	"github.com/G1lollipop/atlas/internal/artifact"
	"github.com/G1lollipop/atlas/internal/model"
)

const (
	embeddingDimensions = 32
	maxEmbeddingBatch   = 4096
	maxInferenceBatch   = 4096
	maxTransformRecords = 10000
	maxTransformRounds  = 64
	maxTransformWork    = 256 << 20
)

// NewEmbeddingHandler registers a deterministic, simulated embedding workload.
// Its input artifact is JSON with a "texts" array; its result artifact contains
// normalized feature-hash vectors and the job result stores only the output URI
// plus compact batch metadata.
func NewEmbeddingHandler(objects artifact.ArtifactStore, maxInputBytes int64) Handler {
	limit := normalizeHandlerLimit(maxInputBytes)
	return func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		input, err := loadAIInput(ctx, objects, job, limit, "embedding")
		if err != nil {
			return nil, err
		}
		var request textBatch
		if err := json.Unmarshal(input, &request); err != nil {
			return nil, fmt.Errorf("embedding: decode input JSON: %w", err)
		}
		if len(request.Texts) == 0 || len(request.Texts) > maxEmbeddingBatch {
			return nil, fmt.Errorf("embedding: texts must contain between 1 and %d items", maxEmbeddingBatch)
		}
		response := struct {
			Model      string          `json:"model"`
			Dimensions int             `json:"dimensions"`
			Embeddings []embeddingItem `json:"embeddings"`
		}{
			Model:      "simulated-feature-hash-v1",
			Dimensions: embeddingDimensions,
			Embeddings: make([]embeddingItem, 0, len(request.Texts)),
		}
		for i, text := range request.Texts {
			if i%64 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			response.Embeddings = append(response.Embeddings, embeddingItem{Index: i, Values: featureHashEmbedding(text)})
		}
		output, err := marshalAIOutput(response, limit, "embedding")
		if err != nil {
			return nil, err
		}
		uri, err := putAIOutput(ctx, objects, run, "embedding", output, "application/json")
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"output_uri": uri,
			"items":      len(request.Texts),
			"dimensions": embeddingDimensions,
			"model":      response.Model,
		}, nil
	}
}

// NewInferenceHandler registers a lightweight simulated inference workload.
// The example classifier is intentionally local and deterministic so it can
// demonstrate worker placement without external model downloads or API calls.
func NewInferenceHandler(objects artifact.ArtifactStore, maxInputBytes int64) Handler {
	limit := normalizeHandlerLimit(maxInputBytes)
	return func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		input, err := loadAIInput(ctx, objects, job, limit, "inference")
		if err != nil {
			return nil, err
		}
		var request textBatch
		if err := json.Unmarshal(input, &request); err != nil {
			return nil, fmt.Errorf("inference: decode input JSON: %w", err)
		}
		if len(request.Texts) == 0 || len(request.Texts) > maxInferenceBatch {
			return nil, fmt.Errorf("inference: texts must contain between 1 and %d items", maxInferenceBatch)
		}
		predictions := make([]inferenceItem, 0, len(request.Texts))
		for i, text := range request.Texts {
			if i%64 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			predictions = append(predictions, classifyText(i, text))
		}
		response := struct {
			Model       string          `json:"model"`
			Predictions []inferenceItem `json:"predictions"`
		}{Model: "simulated-lexicon-inference-v1", Predictions: predictions}
		output, err := marshalAIOutput(response, limit, "inference")
		if err != nil {
			return nil, err
		}
		uri, err := putAIOutput(ctx, objects, run, "inference", output, "application/json")
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"output_uri": uri,
			"items":      len(predictions),
			"model":      response.Model,
		}, nil
	}
}

// NewBatchTransformHandler registers a bounded CPU workload that normalizes
// JSONL records and computes a configurable number of chained SHA-256 rounds.
// Input lines are copied into a transformed JSONL artifact; they are never
// returned in the Postgres-backed job result.
func NewBatchTransformHandler(objects artifact.ArtifactStore, maxInputBytes int64) Handler {
	limit := normalizeHandlerLimit(maxInputBytes)
	return func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		input, err := loadAIInput(ctx, objects, job, limit, "batch_transform")
		if err != nil {
			return nil, err
		}
		rounds, err := transformRounds(job.Payload)
		if err != nil {
			return nil, err
		}
		if int64(len(input))*int64(rounds) > maxTransformWork {
			return nil, fmt.Errorf("batch_transform: input and rounds exceed the %d-byte work budget", maxTransformWork)
		}
		output, records, err := transformJSONL(ctx, input, rounds, limit)
		if err != nil {
			return nil, err
		}
		uri, err := putAIOutput(ctx, objects, run, "batch_transform", output, "application/x-ndjson")
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"output_uri": uri,
			"records":    records,
			"rounds":     rounds,
		}, nil
	}
}

type textBatch struct {
	Texts []string `json:"texts"`
}

type embeddingItem struct {
	Index  int       `json:"index"`
	Values []float64 `json:"values"`
}

type inferenceItem struct {
	Index      int     `json:"index"`
	Label      string  `json:"label"`
	Confidence float64 `json:"confidence"`
}

func normalizeHandlerLimit(limit int64) int64 {
	if limit <= 0 {
		return artifact.DefaultMaxObjectBytes
	}
	return limit
}

func loadAIInput(ctx context.Context, objects artifact.ArtifactStore, job *model.Job, maxBytes int64, handler string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if objects == nil {
		return nil, fmt.Errorf("%s: artifact store is nil", handler)
	}
	if job == nil {
		return nil, fmt.Errorf("%s: job is nil", handler)
	}
	rawURI, ok := job.Payload["input_uri"]
	if !ok {
		return nil, fmt.Errorf("%s: payload missing required \"input_uri\"", handler)
	}
	uri, ok := rawURI.(string)
	if !ok || strings.TrimSpace(uri) == "" {
		return nil, fmt.Errorf("%s: payload \"input_uri\" must be a non-empty string", handler)
	}
	reader, err := objects.Open(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("%s: open input artifact: %w", handler, err)
	}
	defer func() { _ = reader.Close() }()
	input, err := io.ReadAll(io.LimitReader(contextInputReader{ctx: ctx, reader: reader}, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s: read input artifact: %w", handler, err)
	}
	if int64(len(input)) > maxBytes {
		return nil, fmt.Errorf("%s: input artifact exceeds %d-byte limit", handler, maxBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return input, nil
}

type contextInputReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextInputReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func marshalAIOutput(value any, maxBytes int64, handler string) ([]byte, error) {
	output, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%s: encode output artifact: %w", handler, err)
	}
	if int64(len(output)) > maxBytes {
		return nil, fmt.Errorf("%s: output artifact exceeds %d-byte limit", handler, maxBytes)
	}
	return output, nil
}

func putAIOutput(ctx context.Context, objects artifact.ArtifactStore, run *model.JobRun, workload string, output []byte, contentType string) (string, error) {
	if objects == nil {
		return "", fmt.Errorf("%s: artifact store is nil", workload)
	}
	if run == nil || !safeRunID(run.ID) {
		return "", fmt.Errorf("%s: run ID is missing or invalid", workload)
	}
	key := "runs/" + run.ID + "/" + workload + ".json"
	if workload == "batch_transform" {
		key = "runs/" + run.ID + "/" + workload + ".jsonl"
	}
	uri, err := objects.Put(ctx, key, bytes.NewReader(output), int64(len(output)), contentType)
	if err != nil {
		return "", fmt.Errorf("%s: write output artifact: %w", workload, err)
	}
	return uri, nil
}

func safeRunID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func featureHashEmbedding(text string) []float64 {
	vector := make([]float64, embeddingDimensions)
	for _, token := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		digest := sha256.Sum256([]byte(token))
		index := int(binary.BigEndian.Uint32(digest[:4]) % embeddingDimensions)
		sign := 1.0
		if digest[4]&1 != 0 {
			sign = -1
		}
		vector[index] += sign
	}
	var norm float64
	for _, value := range vector {
		norm += value * value
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vector {
			vector[i] /= norm
		}
	}
	return vector
}

func classifyText(index int, text string) inferenceItem {
	positive := 0
	negative := 0
	for _, token := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		switch token {
		case "good", "great", "excellent", "love", "happy", "helpful":
			positive++
		case "bad", "awful", "hate", "sad", "broken", "slow":
			negative++
		}
	}
	label := "neutral"
	confidence := 0.5
	if positive > negative {
		label = "positive"
		confidence = 0.5 + 0.5*float64(positive-negative)/float64(positive+negative+1)
	} else if negative > positive {
		label = "negative"
		confidence = 0.5 + 0.5*float64(negative-positive)/float64(positive+negative+1)
	}
	return inferenceItem{Index: index, Label: label, Confidence: confidence}
}

func transformRounds(payload map[string]any) (int, error) {
	if payload == nil {
		return 16, nil
	}
	value, ok := payload["rounds"]
	if !ok {
		return 16, nil
	}
	number, ok := value.(float64)
	if !ok || math.Trunc(number) != number || number < 1 || number > maxTransformRounds {
		return 0, fmt.Errorf("batch_transform: rounds must be an integer from 1 to %d", maxTransformRounds)
	}
	return int(number), nil
}

func transformJSONL(ctx context.Context, input []byte, rounds int, maxBytes int64) ([]byte, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	var output bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(input))
	scanner.Buffer(make([]byte, 64*1024), len(input)+1)
	records := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		records++
		if records > maxTransformRecords {
			return nil, 0, fmt.Errorf("batch_transform: JSONL input exceeds %d records", maxTransformRecords)
		}
		lineBytes := []byte(line)
		state := sha256.Sum256(lineBytes)
		for i := 0; i < rounds; i++ {
			if i%8 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, 0, err
				}
			}
			h := sha256.New()
			_, _ = h.Write(state[:])
			_, _ = h.Write(lineBytes)
			copy(state[:], h.Sum(nil))
		}
		row, err := json.Marshal(struct {
			Input      string `json:"input"`
			Normalized string `json:"normalized"`
			Digest     string `json:"digest"`
		}{Input: line, Normalized: strings.ToLower(line), Digest: fmt.Sprintf("%x", state[:])})
		if err != nil {
			return nil, 0, fmt.Errorf("batch_transform: encode output record: %w", err)
		}
		if int64(output.Len()+len(row)+1) > maxBytes {
			return nil, 0, fmt.Errorf("batch_transform: output artifact exceeds %d-byte limit", maxBytes)
		}
		output.Write(row)
		output.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("batch_transform: scan input: %w", err)
	}
	return output.Bytes(), records, nil
}
