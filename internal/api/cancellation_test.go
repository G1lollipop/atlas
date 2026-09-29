package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

func TestCancelJob_IsIdempotentAndStopsFutureResume(t *testing.T) {
	st := newFakeStore()
	job, err := st.CreateJob(t.Context(), model.NewJobInput{Name: "long-job"})
	if err != nil {
		t.Fatalf("create fake job: %v", err)
	}
	assignedWorker := "worker-1"
	assignedAt := time.Now()
	assignmentExpiry := assignedAt.Add(time.Minute)
	st.runs[job.ID] = []*model.JobRun{
		{ID: "queued", JobID: job.ID, Status: model.RunStatusQueued},
		{ID: "assigned", JobID: job.ID, Status: model.RunStatusAssigned,
			AssignedWorkerID: &assignedWorker, AssignedAt: &assignedAt, AssignmentExpiresAt: &assignmentExpiry},
		{ID: "running", JobID: job.ID, Status: model.RunStatusRunning},
	}

	log := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	router := NewRouter(st, log, testSecret, 10000, 10000)
	token, err := MintToken(testSecret, "handler-test", time.Hour)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}

	path := "/v1/jobs/" + job.ID + "/cancel"
	rec := doRequest(t, router, http.MethodPost, path, token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel: status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var canceled model.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &canceled); err != nil {
		t.Fatalf("unmarshal cancellation response: %v", err)
	}
	if canceled.Status != model.JobStatusCanceled {
		t.Fatalf("job status = %q, want canceled", canceled.Status)
	}

	rec = doRequest(t, router, http.MethodPost, path, token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("repeat cancel: status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	st.mu.Lock()
	func() {
		defer st.mu.Unlock()
		queued, assigned, running := st.runs[job.ID][0], st.runs[job.ID][1], st.runs[job.ID][2]
		if queued.Status != model.RunStatusCanceled || assigned.Status != model.RunStatusCanceled {
			t.Fatalf("unstarted run statuses = %q and %q, want canceled", queued.Status, assigned.Status)
		}
		if assigned.AssignedWorkerID != nil || assigned.AssignedAt != nil || assigned.AssignmentExpiresAt != nil {
			t.Fatal("canceling assigned run did not clear its worker reservation")
		}
		if running.Status != model.RunStatusRunning || running.CancelRequestedAt == nil {
			t.Fatalf("running run = status %q, cancel_requested_at %v; want running with durable request", running.Status, running.CancelRequestedAt)
		}
	}()
	rec = doRequest(t, router, http.MethodPost, "/v1/jobs/"+job.ID+"/resume", token, nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("resume canceled job: status = %d, want 409 (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestCancelJob_RequiresAuthAndReturnsNotFound(t *testing.T) {
	router, token := newTestRouter(t)
	path := "/v1/jobs/missing/cancel"
	if rec := doRequest(t, router, http.MethodPost, path, "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cancel: status = %d, want 401", rec.Code)
	}
	if rec := doRequest(t, router, http.MethodPost, path, token, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("missing job cancel: status = %d, want 404 (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestListJobs_AcceptsCanceledStatus(t *testing.T) {
	router, token := newTestRouter(t)
	rec := doRequest(t, router, http.MethodGet, "/v1/jobs?status=canceled", token, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list canceled jobs: status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
}
