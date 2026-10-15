package runtime

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

type ContinuationPreview = runstate.ContinuationPreview
type ContinuationBasis = runstate.ContinuationBasis

func (s *Server) continuationRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/runs/{id}/continuation", wrap(func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Cache-Control", "no-store")
		if _, err := callQuery(r); err != nil {
			return stateError(400, "continuation preview takes no query parameters")
		}
		if len(r.PathValue("id")) > 512 {
			return stateError(400, "run identity exceeds preview capacity")
		}
		v, err := s.Store.FailedContinuationPreview(r.Context(), r.PathValue("id"))
		if errors.Is(err, runstate.ErrContinuationRunNotFound) {
			return stateError(404, "run not found")
		}
		if err != nil {
			return stateError(503, "continuation preview unavailable")
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/continuation/request-context", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if err := continuationActionAccess(w, r); err != nil {
			return err
		}
		v, err := s.Store.FailedContinuationContext(r.Context(), r.PathValue("id"))
		if err != nil {
			return continuationActionError(err)
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("POST /v1/runs/{id}/continuation", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if err := continuationActionAccess(w, r); err != nil {
			return err
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		if err != nil {
			return stateError(400, "invalid continuation request")
		}
		in, err := decodeContinuationRequest(raw)
		if err != nil {
			return continuationActionError(err)
		}
		v, err := s.Store.RequestFailedContinuation(r.Context(), r.PathValue("id"), in)
		if err != nil {
			return continuationActionError(err)
		}
		jsonResponse(w, v.HTTPStatus, v.Body)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/continuation/requests/{request_id}", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if err := continuationActionAccess(w, r); err != nil {
			return err
		}
		v, err := s.Store.ContinuationReceipt(r.Context(), r.PathValue("id"), r.PathValue("request_id"))
		if err != nil {
			return continuationActionError(err)
		}
		// The body's outcome is historical; GET does not replay its POST status.
		jsonResponse(w, 200, v.Body)
		return nil
	}))
}

// New action endpoints require a named control identity even when a deployment
// otherwise permits the legacy single-token middleware configuration.
func continuationActionAccess(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Cache-Control", "no-store")
	if servicehttp.Caller(r.Context()) != "control" || (r.Method != "GET" && r.Method != "POST") {
		return stateError(403, "continuation action requires control identity")
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || !runstate.ValidContinuationRunID(r.PathValue("id")) {
		return stateError(400, "invalid continuation request")
	}
	return nil
}

func decodeContinuationRequest(raw []byte) (runstate.ContinuationRequest, error) {
	var in runstate.ContinuationRequest
	if len(raw) > 1024 || !utf8.Valid(raw) || uniqueWorkingJSON(raw) != nil {
		return in, runstate.ErrContinuationInput
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 {
		return in, runstate.ErrContinuationInput
	}
	// Exact keys avoid encoding/json's case-insensitive struct-field aliases.
	if json.Unmarshal(fields["request_id"], &in.RequestID) != nil ||
		json.Unmarshal(fields["expected_failure_version"], &in.ExpectedFailureVersion) != nil ||
		!runstate.ValidContinuationRequestID(in.RequestID) || !runstate.ValidContinuationFailureVersion(in.ExpectedFailureVersion) {
		return runstate.ContinuationRequest{}, runstate.ErrContinuationInput
	}
	return in, nil
}

func continuationActionError(err error) error {
	switch {
	case errors.Is(err, runstate.ErrContinuationInput):
		return stateError(400, "invalid continuation request")
	case errors.Is(err, runstate.ErrContinuationRunNotFound):
		return stateError(404, "run not found")
	case errors.Is(err, runstate.ErrContinuationReceiptMissing):
		return stateError(404, "continuation receipt not found")
	case errors.Is(err, runstate.ErrContinuationRequestConflict):
		return stateError(409, "request_id_conflict")
	default:
		return stateError(503, "continuation action unavailable")
	}
}
