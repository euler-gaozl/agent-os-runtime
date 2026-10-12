package runtime

import (
	"errors"
	"net/http"
	"strings"
)

func (s *Server) sandboxAuthorizationRoutes(m *http.ServeMux) {
	m.HandleFunc("POST /v1/sandbox/authorize", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var inv Invocation
		if e := decode(w, r, &inv); e != nil {
			return e
		}
		if inv.Name != "sandbox_exec" || inv.RunID == "" || !strings.HasPrefix(inv.OperationID, inv.RunID+"/") {
			return errors.New("invalid sandbox invocation")
		}
		tx, e := s.Store.DB.Begin(r.Context())
		if e != nil {
			return e
		}
		defer tx.Rollback(r.Context())
		run, e := checkInvocation(r.Context(), tx, inv)
		if e != nil {
			return e
		}
		if e = authorize(run, inv); e != nil {
			return e
		}
		// Authorization is only for a persisted tool intent, not an arbitrary
		// call inferred from possession of a run ID or an old snapshot.
		_, status, e := existingCall(r.Context(), tx, inv)
		if e != nil {
			return e
		}
		if status != "inflight" && status != "completed" {
			return errors.New("sandbox requires a durable runtime tool intent")
		}
		jsonResponse(w, http.StatusOK, map[string]bool{"authorized": true})
		return nil
	}))
}
