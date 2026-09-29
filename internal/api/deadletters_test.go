package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

func newDeadLetterTestRouter(t *testing.T) (http.Handler, string, *fakeStore) {
	t.Helper()
	st := newFakeStore()
	log := slogForTest()
	router := NewRouter(st, log, testSecret, 10000, 10000)
	token, err := MintToken(testSecret, "dead-letter-test", time.Hour)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}
	return router, token, st
}

func addFakeDeadLetter(st *fakeStore, id string, createdAt time.Time) *model.DeadLetter {
	st.mu.Lock()
	defer st.mu.Unlock()

	run := &model.JobRun{
		ID:          "run-" + id,
		JobID:       "job-" + id,
		Status:      model.RunStatusDead,
		Attempt:     5,
		ScheduledAt: createdAt.Add(-time.Hour),
		CreatedAt:   createdAt.Add(-2 * time.Hour),
	}
	st.runs[run.JobID] = []*model.JobRun{run}
	letter := &model.DeadLetter{
		ID:        id,
		JobRunID:  run.ID,
		Reason:    "handler failed",
		Payload:   map[string]any{"error": "failed"},
		CreatedAt: createdAt,
	}
	st.deadLetters[id] = letter
	return letter
}

func TestListDeadLettersPaginationAndValidation(t *testing.T) {
	router, token, st := newDeadLetterTestRouter(t)
	now := time.Now()
	addFakeDeadLetter(st, "older", now.Add(-time.Hour))
	addFakeDeadLetter(st, "newer", now)

	rec := doRequest(t, router, http.MethodGet, "/v1/dead-letters?limit=1&offset=1", token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var letters []*model.DeadLetter
	if err := json.Unmarshal(rec.Body.Bytes(), &letters); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(letters) != 1 || letters[0].ID != "older" {
		t.Fatalf("paginated letters = %+v, want only older letter", letters)
	}

	for _, path := range []string{
		"/v1/dead-letters?limit=bad",
		"/v1/dead-letters?offset=-1",
	} {
		rec := doRequest(t, router, http.MethodGet, path, token, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s: status = %d, want 400", path, rec.Code)
		}
	}
}

func TestRetryDeadLetterResetsRun(t *testing.T) {
	router, token, st := newDeadLetterTestRouter(t)
	letter := addFakeDeadLetter(st, "retry-me", time.Now())
	run := st.runs["job-retry-me"][0]
	leasedBy := "worker"
	assignedWorker := "worker"
	startedAt := time.Now().Add(-time.Minute)
	finishedAt := time.Now()
	run.LeasedBy = &leasedBy
	run.LeasedAt = &startedAt
	run.LeaseExpiresAt = &finishedAt
	run.AssignedWorkerID = &assignedWorker
	run.AssignedAt = &startedAt
	run.AssignmentExpiresAt = &finishedAt
	run.StartedAt = &startedAt
	run.FinishedAt = &finishedAt
	run.Result = map[string]any{"partial": true}
	errMsg := "failed"
	run.Error = &errMsg

	rec := doRequest(t, router, http.MethodPost, "/v1/dead-letters/"+letter.ID+"/retry", token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("retry: status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var retried model.JobRun
	if err := json.Unmarshal(rec.Body.Bytes(), &retried); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if retried.Status != model.RunStatusQueued || retried.Attempt != 0 {
		t.Errorf("retried status/attempt = %q/%d, want queued/0", retried.Status, retried.Attempt)
	}
	if retried.ScheduledAt.IsZero() {
		t.Error("retried run should have scheduled_at set")
	}
	if retried.LeasedBy != nil || retried.LeasedAt != nil || retried.LeaseExpiresAt != nil ||
		retried.AssignedWorkerID != nil || retried.AssignedAt != nil || retried.AssignmentExpiresAt != nil ||
		retried.StartedAt != nil || retried.FinishedAt != nil || retried.Result != nil || retried.Error != nil {
		t.Errorf("retry left stale execution state: %+v", retried)
	}
	if _, ok := st.deadLetters[letter.ID]; ok {
		t.Error("retry should remove the dead-letter entry")
	}

	rec = doRequest(t, router, http.MethodPost, "/v1/dead-letters/missing/retry", token, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown retry: status = %d, want 404", rec.Code)
	}
}

func TestDeleteDeadLetterLeavesRunDead(t *testing.T) {
	router, token, st := newDeadLetterTestRouter(t)
	letter := addFakeDeadLetter(st, "discard-me", time.Now())

	rec := doRequest(t, router, http.MethodDelete, "/v1/dead-letters/"+letter.ID, token, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: status = %d, want 204 (body: %s)", rec.Code, rec.Body.String())
	}
	if _, ok := st.deadLetters[letter.ID]; ok {
		t.Error("delete should remove the dead-letter entry")
	}
	run, err := st.GetRun(t.Context(), letter.JobRunID)
	if err != nil || run.Status != model.RunStatusDead {
		t.Errorf("discard should leave run dead: run=%+v error=%v", run, err)
	}

	rec = doRequest(t, router, http.MethodDelete, "/v1/dead-letters/missing", token, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown delete: status = %d, want 404", rec.Code)
	}
}

func TestRetryAndDeleteDeadLetterOnlyOneWins(t *testing.T) {
	router, token, st := newDeadLetterTestRouter(t)
	letter := addFakeDeadLetter(st, "race-me", time.Now())

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/dead-letters/" + letter.ID + "/retry"},
		{http.MethodDelete, "/v1/dead-letters/" + letter.ID},
	}
	statuses := make([]int, len(paths))
	var wg sync.WaitGroup
	for i, item := range paths {
		wg.Add(1)
		go func(i int, item struct{ method, path string }) {
			defer wg.Done()
			rec := doRequest(t, router, item.method, item.path, token, nil)
			statuses[i] = rec.Code
		}(i, item)
	}
	wg.Wait()

	successes := 0
	for _, status := range statuses {
		if status == http.StatusOK || status == http.StatusNoContent {
			successes++
		} else if status != http.StatusNotFound {
			t.Errorf("racing operation status = %d, want one success and one 404", status)
		}
	}
	if successes != 1 {
		t.Fatalf("successful racing operations = %d, want 1 (statuses %v)", successes, statuses)
	}
	if _, ok := st.deadLetters[letter.ID]; ok {
		t.Error("winning operation should remove the dead-letter entry")
	}
}

// Keep this helper local to this focused test file so test setup does not depend on
// handler test internals beyond their shared API test secret.
func slogForTest() *slog.Logger {
	return slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
}
