package runtime

import (
	"errors"
	"net/http"

	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
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
}
