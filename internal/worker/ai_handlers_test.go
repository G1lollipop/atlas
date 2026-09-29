package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/G1lollipop/atlas/internal/artifact"
	"github.com/G1lollipop/atlas/internal/model"
)

func TestEmbeddingHandlerStoresVectorsAsArtifact(t *testing.T) {
	objects := newTestArtifactStore(t)
	inputURI := putTestInput(t, objects, "inputs/embedding.json", `{"texts":["worker schedules jobs", "GPU inference"]}`)
	job := &model.Job{Name: "embedding", Payload: map[string]any{"input_uri": inputURI}}
	run := &model.JobRun{ID: "run-embedding-1"}

	result, err := NewEmbeddingHandler(objects, 1024)(context.Background(), job, run)
	if err != nil {
		t.Fatalf("embedding handler returned an error: %v", err)
	}
	if result["output_uri"] != "local://runs/run-embedding-1/embedding.json" || result["items"] != 2 || result["dimensions"] != embeddingDimensions {
		t.Fatalf("unexpected embedding result metadata: %#v", result)
	}
	if _, exists := result["embeddings"]; exists {
		t.Fatal("embedding vectors were copied into the job result instead of the artifact")
	}
	output := readTestArtifact(t, objects, result["output_uri"].(string))
	var decoded struct {
		Embeddings []embeddingItem `json:"embeddings"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode embedding output: %v", err)
	}
	if len(decoded.Embeddings) != 2 || len(decoded.Embeddings[0].Values) != embeddingDimensions {
		t.Fatalf("unexpected embedding artifact: %#v", decoded)
	}
}

func TestInferenceHandlerProducesBoundedPredictionArtifact(t *testing.T) {
	objects := newTestArtifactStore(t)
	inputURI := putTestInput(t, objects, "inputs/inference.json", `{"texts":["this worker is helpful and great", "the deployment is broken"]}`)
	job := &model.Job{Name: "inference", Payload: map[string]any{"input_uri": inputURI}}
	run := &model.JobRun{ID: "run-inference-1"}

	result, err := NewInferenceHandler(objects, 1024)(context.Background(), job, run)
	if err != nil {
		t.Fatalf("inference handler returned an error: %v", err)
	}
	if result["items"] != 2 || result["model"] != "simulated-lexicon-inference-v1" {
		t.Fatalf("unexpected inference result metadata: %#v", result)
	}
	var decoded struct {
		Predictions []inferenceItem `json:"predictions"`
	}
	if err := json.Unmarshal(readTestArtifact(t, objects, result["output_uri"].(string)), &decoded); err != nil {
		t.Fatalf("decode inference artifact: %v", err)
	}
	if len(decoded.Predictions) != 2 || decoded.Predictions[0].Label != "positive" || decoded.Predictions[1].Label != "negative" {
		t.Fatalf("unexpected simulated predictions: %#v", decoded.Predictions)
	}
}

func TestBatchTransformHandlerWritesJSONLArtifactAndHonorsCancellation(t *testing.T) {
	objects := newTestArtifactStore(t)
	inputURI := putTestInput(t, objects, "inputs/batch.jsonl", "HELLO Worker\nGPU Jobs\n")
	job := &model.Job{Name: "batch_transform", Payload: map[string]any{"input_uri": inputURI, "rounds": float64(2)}}
	run := &model.JobRun{ID: "run-batch-1"}

	result, err := NewBatchTransformHandler(objects, 1024)(context.Background(), job, run)
	if err != nil {
		t.Fatalf("batch transform handler returned an error: %v", err)
	}
	if result["records"] != 2 || result["rounds"] != 2 {
		t.Fatalf("unexpected transform result metadata: %#v", result)
	}
	output := string(readTestArtifact(t, objects, result["output_uri"].(string)))
	if !strings.Contains(output, `"normalized":"hello worker"`) {
		t.Fatalf("batch transform output lacks normalized text: %s", output)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewBatchTransformHandler(objects, 1024)(cancelled, job, run); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled handler error = %v, want context.Canceled", err)
	}
}

func newTestArtifactStore(t *testing.T) *artifact.LocalArtifactStore {
	t.Helper()
	store, err := artifact.NewLocalArtifactStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func putTestInput(t *testing.T, objects artifact.ArtifactStore, key, input string) string {
	t.Helper()
	uri, err := objects.Put(context.Background(), key, strings.NewReader(input), int64(len(input)), "application/json")
	if err != nil {
		t.Fatal(err)
	}
	return uri
}

func readTestArtifact(t *testing.T, objects artifact.ArtifactStore, uri string) []byte {
	t.Helper()
	reader, err := objects.Open(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestAIHandlersRejectInputAboveConfiguredLimit(t *testing.T) {
	objects := newTestArtifactStore(t)
	inputURI := putTestInput(t, objects, "inputs/too-large.json", `{"texts":["this text is over the configured limit"]}`)
	job := &model.Job{Name: "embedding", Payload: map[string]any{"input_uri": inputURI}}
	if _, err := NewEmbeddingHandler(objects, 8)(context.Background(), job, &model.JobRun{ID: "run-size-1"}); err == nil {
		t.Fatal("embedding handler accepted an input above its configured limit")
	}
}
