package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/G1lollipop/atlas/internal/store"
)

func (h *handler) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	limit, ok := parseLimit(r.URL.Query().Get("limit"), w)
	if !ok {
		return
	}
	offset, ok := parseOffset(r.URL.Query().Get("offset"), w)
	if !ok {
		return
	}

	letters, err := h.store.ListDeadLetters(r.Context(), limit, offset)
	if err != nil {
		h.serverError(w, err, "ListDeadLetters failed")
		return
	}
	writeJSON(w, http.StatusOK, letters)
}

func (h *handler) retryDeadLetter(w http.ResponseWriter, r *http.Request) {
	run, err := h.store.RetryDeadLetter(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "dead letter not found")
			return
		}
		h.serverError(w, err, "RetryDeadLetter failed")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *handler) deleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	if err := h.store.DeleteDeadLetter(r.Context(), chi.URLParam(r, "id")); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "dead letter not found")
			return
		}
		h.serverError(w, err, "DeleteDeadLetter failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
