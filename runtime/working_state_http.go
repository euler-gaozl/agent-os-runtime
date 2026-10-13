package runtime

import (
	"net/http"
	"strconv"
)

func (s *Server) workingStateRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/source-state", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var inv Invocation
		if err := decode(w, r, &inv); err != nil {
			return err
		}
		response, err := s.Store.sourceState(r.Context(), inv)
		if err != nil {
			return err
		}
		jsonResponse(w, 200, response)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/working-state", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if operation := r.URL.Query().Get("operation_id"); operation != "" {
			if len(operation) > 240 || r.URL.Query().Has("revision") {
				return stateError(400, "select a bounded operation_id or revision, not both")
			}
			receipt, err := s.Store.WorkingReceipt(r.Context(), r.PathValue("id"), operation)
			if err != nil {
				return err
			}
			jsonResponse(w, 200, receipt)
			return nil
		}
		var revision int64
		if value := r.URL.Query().Get("revision"); value != "" {
			var err error
			revision, err = strconv.ParseInt(value, 10, 64)
			if err != nil || revision < 0 {
				return stateError(400, "invalid working-state revision")
			}
		}
		state, err := s.Store.WorkingState(r.Context(), r.PathValue("id"), revision)
		if err != nil {
			return err
		}
		jsonResponse(w, 200, state)
		return nil
	}))
}
