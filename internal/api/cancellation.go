package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/G1lollipop/atlas/internal/store"
)

func (h *handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.store.CancelJob(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		h.serverError(w, err, "CancelJob failed")
		return
	}

	// Read replicas may lag behind the committed cancellation. Return the
	// acknowledged state directly instead of reading a potentially stale job.
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "canceled"})
}
