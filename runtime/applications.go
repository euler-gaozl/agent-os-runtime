package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
)

func (s *Server) applicationRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/apps/{app}/runs", wrap(func(w http.ResponseWriter, r *http.Request) error {
		app := r.PathValue("app")
		var in struct {
			Agent    Ref             `json:"agent"`
			Task     string          `json:"task"`
			Metadata json.RawMessage `json:"metadata"`
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		key := r.Header.Get("Idempotency-Key")
		if !safeName.MatchString(app) || key == "" || len(key) > 180 || len(in.Task) == 0 || len(in.Task) > 40000 || len(in.Metadata) > 4000 {
			return errors.New("bounded app submission and idempotency key required")
		}
		if s.Store.Remote == nil {
			return errors.New("registry client required")
		}
		var published Resource
		if e := s.Store.Remote.Request(r.Context(), "GET", s.RegistryURL+"/v1/published/"+url.PathEscape(app)+"/"+url.PathEscape(in.Agent.ID), nil, &published); e != nil {
			return e
		}
		if published.Revision != in.Agent.Revision {
			return errors.New("published agent version changed")
		}
		snap, e := s.Store.Snapshot(r.Context(), in.Agent, 0)
		if e != nil {
			return e
		}
		if _, e = ToolDefinitions(snap); e != nil {
			return e
		}
		tx, e := s.Store.DB.Begin(r.Context())
		if e != nil {
			return e
		}
		defer tx.Rollback(r.Context())
		id, e := insertRun(r.Context(), tx, app+":"+key, in.Task, snap, nil, nil)
		if e != nil {
			return e
		}
		if len(in.Metadata) == 0 {
			in.Metadata = JSON(map[string]any{})
		}
		var previous []byte
		if e = tx.QueryRow(r.Context(), "INSERT INTO app_runs(run_id,app,metadata) VALUES($1,$2,$3) ON CONFLICT(run_id) DO UPDATE SET run_id=excluded.run_id RETURNING metadata", id, app, in.Metadata).Scan(&previous); e != nil {
			return e
		}
		if canonicalHash(previous) != canonicalHash(in.Metadata) {
			return errors.New("submission key reused with changed app metadata")
		}
		if e = tx.Commit(r.Context()); e != nil {
			return e
		}
		jsonResponse(w, 201, map[string]string{"id": id})
		return nil
	}))
	mux.HandleFunc("GET /v1/apps/{app}/runs", wrap(func(w http.ResponseWriter, r *http.Request) error {
		rows, e := s.Store.DB.Query(r.Context(), "SELECT r.id,r.status,r.created_at,a.metadata FROM app_runs a JOIN runs r ON r.id=a.run_id WHERE a.app=$1 ORDER BY a.created_at DESC LIMIT 100", r.PathValue("app"))
		if e != nil {
			return e
		}
		defer rows.Close()
		out := []any{}
		for rows.Next() {
			var id, status string
			var at any
			var metadata json.RawMessage
			if e = rows.Scan(&id, &status, &at, &metadata); e != nil {
				return e
			}
			out = append(out, map[string]any{"id": id, "status": status, "created_at": at, "metadata": metadata})
		}
		if e = rows.Err(); e != nil {
			return e
		}
		jsonResponse(w, 200, out)
		return nil
	}))
	mux.HandleFunc("GET /v1/apps/{app}/runs/{id}", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var metadata json.RawMessage
		if e := s.Store.DB.QueryRow(r.Context(), "SELECT metadata FROM app_runs WHERE app=$1 AND run_id=$2", r.PathValue("app"), r.PathValue("id")).Scan(&metadata); e != nil {
			return e
		}
		run, e := s.Store.Run(r.Context(), r.PathValue("id"))
		if e != nil {
			return e
		}
		jsonResponse(w, 200, map[string]any{"id": run.ID, "status": run.Status, "step": run.Step, "result": run.State.Result, "error": run.State.Error, "created_at": run.CreatedAt, "metadata": metadata})
		return nil
	}))
	mux.HandleFunc("POST /v1/apps/{app}/runs/{id}/cancel", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var ok bool
		if e := s.Store.DB.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM app_runs WHERE app=$1 AND run_id=$2)", r.PathValue("app"), r.PathValue("id")).Scan(&ok); e != nil {
			return e
		}
		if !ok {
			return errors.New("app task not found")
		}
		if e := s.Store.Cancel(r.Context(), r.PathValue("id")); e != nil {
			return e
		}
		jsonResponse(w, 202, map[string]bool{"cancel_requested": true})
		return nil
	}))
}
