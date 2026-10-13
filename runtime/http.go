package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	Store                                        *Store
	Token, RegistryURL, KnowledgeURL, SandboxURL string
}
type handler func(http.ResponseWriter, *http.Request) error

func (s *Server) Handler(c servicehttp.Config) http.Handler {
	return servicehttp.New(c, s.Store.DB.Ping, func(m *http.ServeMux) {
		s.runtimeRoutes(m)
		s.workingStateRoutes(m)
		s.callReadRoutes(m)
		s.documentRoutes(m, "/v1")
		s.toolRoutes(m)
		s.applicationRoutes(m)
		s.appEvidenceRoutes(m)
		s.sandboxAuthorizationRoutes(m)
	})
}
func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("expected exactly one JSON value")
	}
	return nil
}
func wrap(h handler) http.HandlerFunc {
	return servicehttp.Wrap(h)
}
func (s *Server) toolRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /invoke", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var inv Invocation
		if e := decode(w, r, &inv); e != nil {
			return e
		}
		if inv.OperationID == "" || !strings.HasPrefix(inv.OperationID, inv.RunID+"/") {
			return errors.New("operation id must be scoped to run")
		}
		// Recheck governance even for direct Runtime calls. Never trust the
		// worker's saved snapshot as authorization for a withdrawn asset.
		if s.Store.Remote == nil {
			return errors.New("registry/catalog client required for tool governance")
		}
		if s.Store.Remote != nil {
			run, err := s.Store.Run(r.Context(), inv.RunID)
			if err != nil {
				return err
			}
			for _, ref := range run.Snapshot.Agent.Skills {
				var v Skill
				if err = s.Store.Resource(r.Context(), "skill", ref, &v); err != nil {
					return err
				}
			}
			for _, ref := range run.Snapshot.Agent.Connectors {
				var v Connector
				if err = s.Store.Resource(r.Context(), "connector", ref, &v); err != nil {
					return err
				}
			}
		}
		remoteDocument := false
		if inv.Name == "document_read" && s.Store.Remote != nil {
			var args struct{ ID string }
			if e := json.Unmarshal(inv.Arguments, &args); e != nil {
				return e
			}
			var local bool
			if e := s.Store.DB.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM documents d JOIN runs r ON r.id=$2 WHERE d.id=$1 AND (d.scope='' OR d.scope=r.snapshot->'agent_ref'->>'id'))", args.ID, inv.RunID).Scan(&local); e != nil {
				return e
			}
			remoteDocument = !local
		}
		var result []byte
		var e error
		if strings.HasPrefix(inv.Name, "mcp_") || inv.Name == "sandbox_exec" || inv.Name == "kb_search" || inv.Name == "skill_file_read" || remoteDocument {
			result, e = s.Store.ExternalTool(r.Context(), inv, func(ctx context.Context, run Run) ([]byte, error) {
				if inv.Name == "skill_file_read" {
					var args map[string]any
					if e := json.Unmarshal(inv.Arguments, &args); e != nil {
						return nil, e
					}
					args["run_id"] = run.ID
					return internalRequest(ctx, s.Store.CatalogURL+"/v1/skill-file", s.Token, args)
				}
				if remoteDocument {
					var args map[string]any
					if e := json.Unmarshal(inv.Arguments, &args); e != nil {
						return nil, e
					}
					args["run_id"] = run.ID
					return internalRequest(ctx, s.KnowledgeURL+"/v1/document", s.Token, args)
				}
				if inv.Name == "kb_search" {
					var args map[string]any
					if e := json.Unmarshal(inv.Arguments, &args); e != nil {
						return nil, e
					}
					args["run_id"] = run.ID
					return internalRequest(ctx, s.KnowledgeURL+"/v1/retrieve", s.Token, args)
				}
				if inv.Name == "sandbox_exec" {
					return internalRequest(ctx, s.SandboxURL+"/execute", s.Token, inv)
				}
				if c, name, found := connectorForTool(run.Snapshot, inv.Name); found {
					var e error
					c.AuthToken, e = s.Store.Secret(ctx, c.KeyEnv)
					if e != nil {
						return nil, e
					}
					return executeCheckedMCP(ctx, c, name, inv.Arguments)
				}
				return nil, errors.New("connector missing")
			})
		} else {
			result, e = s.Store.LocalTool(r.Context(), inv)
		}
		if e != nil {
			return e
		}
		w.Header().Set("Content-Type", "application/json")
		_, e = w.Write(result)
		return e
	}))
}
func (s *Server) documentRoutes(mux *http.ServeMux, prefix string) {
	s.noteReadRoutes(mux, prefix)
	for _, kind := range []string{"revise", "retire"} {
		mux.HandleFunc("POST "+prefix+"/notes/"+kind, wrap(func(w http.ResponseWriter, r *http.Request) error {
			// Decode the concrete shape to reject unrecognized actor/scope fields.
			var requestID string
			var input []byte
			if kind == "revise" {
				var v struct {
					RequestID string `json:"request_id"`
					NoteRevision
				}
				if e := decode(w, r, &v); e != nil {
					return e
				}
				requestID = v.RequestID
				input = JSON(v.NoteRevision)
			} else {
				var v struct {
					RequestID string `json:"request_id"`
					NoteRetirement
				}
				if e := decode(w, r, &v); e != nil {
					return e
				}
				requestID = v.RequestID
				input = JSON(v.NoteRetirement)
			}
			actor := servicehttp.Caller(r.Context())
			if actor == "" {
				actor = "authenticated-admin"
			}
			result, e := s.Store.AdminNoteMutation(r.Context(), requestID, kind, actor, input)
			if e != nil {
				var rejected *noteRejection
				if errors.As(e, &rejected) {
					jsonResponse(w, rejected.status, map[string]string{"error": rejected.Error(), "note_rejected_request_id": requestID})
					return nil
				}
				return e
			}
			jsonResponse(w, 200, json.RawMessage(result))
			return nil
		}))
	}
	mux.HandleFunc("GET "+prefix+"/documents/{id}", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var d Document
		var metadata []byte
		e := s.Store.DB.QueryRow(r.Context(), "SELECT id,kind,title,content,source,metadata,supersedes,scope,valid_until FROM documents WHERE id=$1", r.PathValue("id")).Scan(&d.ID, &d.Kind, &d.Title, &d.Content, &d.Source, &metadata, &d.Supersedes, &d.Scope, &d.ValidUntil)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(metadata, &d.Metadata); e != nil {
			return e
		}
		state, e := noteLifecycle(r.Context(), s.Store.DB, d.ID)
		if e != nil {
			return e
		}
		jsonResponse(w, 200, struct {
			Document
			Lifecycle NoteLifecycle `json:"lifecycle"`
		}{d, state})
		return nil
	}))
	mux.HandleFunc("GET "+prefix+"/documents", wrap(func(w http.ResponseWriter, r *http.Request) error {
		v, e := s.Store.Search(r.Context(), r.URL.Query().Get("q"), r.URL.Query().Get("kind"))
		if e != nil {
			return e
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("POST "+prefix+"/documents", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var d Document
		if e := decode(w, r, &d); e != nil {
			return e
		}
		actor := servicehttp.Caller(r.Context())
		if actor == "" {
			actor = "authenticated-admin"
		}
		v, e := s.Store.putAdminDocument(r.Context(), d, actor)
		if e != nil {
			return e
		}
		jsonResponse(w, 201, v)
		return nil
	}))
}
func internalRequest(ctx context.Context, endpoint, token string, v any) ([]byte, error) {
	req, e := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(JSON(v)))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, e := externalHTTP.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
	if e != nil {
		return nil, e
	}
	if len(body) > 4<<20 {
		return nil, errors.New("internal response too large")
	}
	if resp.StatusCode == 409 {
		return nil, ErrUnknown
	}
	if resp.StatusCode == 412 {
		return nil, ErrFence
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("service HTTP %d: %s", resp.StatusCode, truncateUTF8(string(body), 500))
	}
	return body, nil
}
func (s *Server) runtimeRoutes(mux *http.ServeMux) {
	s.telemetryDeliveryRoutes(mux)
	mux.HandleFunc("GET /v1/status", wrap(func(w http.ResponseWriter, r *http.Request) error {
		rows, e := s.Store.DB.Query(r.Context(), "SELECT id,role,hostname,pid,heartbeat_at,heartbeat_at>now()-interval '15 seconds' FROM instances ORDER BY role,id")
		if e != nil {
			return e
		}
		defer rows.Close()
		out := []any{}
		for rows.Next() {
			var id, role, host string
			var pid int
			var at time.Time
			var active bool
			if e = rows.Scan(&id, &role, &host, &pid, &at, &active); e != nil {
				return e
			}
			out = append(out, map[string]any{"id": id, "role": role, "hostname": host, "pid": pid, "heartbeat_at": at, "active": active})
		}
		if e = rows.Err(); e != nil {
			return e
		}
		jsonResponse(w, 200, map[string]any{"instances": out, "default_run_engine": s.Store.defaultRunEngine()})
		return nil
	}))
	mux.HandleFunc("POST /v1/instances", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			ID, Role, Hostname string
			PID                int
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		if !safeName.MatchString(in.ID) || !safeName.MatchString(in.Role) || len(in.Hostname) > 255 {
			return errors.New("invalid instance identity")
		}
		if caller := servicehttp.Caller(r.Context()); caller != "" && caller != in.Role {
			return &servicehttp.StatusError{Status: 403, Message: "instance role must match service identity"}
		}
		tag, e := s.Store.DB.Exec(r.Context(), "INSERT INTO instances(id,role,hostname,pid) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET heartbeat_at=now(),pid=$4 WHERE instances.role=excluded.role AND instances.hostname=excluded.hostname", in.ID, in.Role, in.Hostname, in.PID)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return errors.New("instance identity already belongs to another service or host")
		}
		jsonResponse(w, 200, map[string]bool{"registered": true})
		return nil
	}))
	mux.HandleFunc("GET /v1/runs", wrap(func(w http.ResponseWriter, r *http.Request) error {
		v, e := s.Store.Runs(r.Context())
		if e != nil {
			return e
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("POST /v1/runs", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Agent       Ref      `json:"agent"`
			Task        string   `json:"task"`
			Constraints []string `json:"constraints"`
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 200 || strings.TrimSpace(in.Task) == "" || len(in.Task) > 40000 || len(JSON(in.Constraints)) > 16000 {
			return errors.New("idempotency key and bounded task required")
		}
		snap, e := s.Store.Snapshot(r.Context(), in.Agent, 0)
		if e != nil {
			return e
		}
		if _, e = ToolDefinitions(snap); e != nil {
			return e
		}
		id, e := s.Store.Submit(r.Context(), key, in.Task, snap, in.Constraints)
		if e != nil {
			return e
		}
		jsonResponse(w, 201, map[string]string{"id": id})
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}", wrap(func(w http.ResponseWriter, r *http.Request) error {
		v, e := s.Store.Run(r.Context(), r.PathValue("id"))
		if e != nil {
			return e
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/events", wrap(func(w http.ResponseWriter, r *http.Request) error {
		after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		v, e := s.Store.Events(r.Context(), r.PathValue("id"), after)
		if e != nil {
			return e
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/calls", wrap(func(w http.ResponseWriter, r *http.Request) error {
		v, e := s.Store.Calls(r.Context(), r.PathValue("id"))
		if e != nil {
			return e
		}
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/usage", wrap(func(w http.ResponseWriter, r *http.Request) error {
		usage, e := s.Store.Usage(r.Context(), r.PathValue("id"))
		if e != nil {
			return e
		}
		jsonResponse(w, 200, usage)
		return nil
	}))
	mux.HandleFunc("POST /v1/runs/{id}/cancel", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if e := s.Store.Cancel(r.Context(), r.PathValue("id")); e != nil {
			return e
		}
		jsonResponse(w, 202, map[string]bool{"cancel_requested": true})
		return nil
	}))
	mux.HandleFunc("POST /v1/runs/{id}/constraints", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Values []string `json:"values"`
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		if e := s.Store.Constraints(r.Context(), r.PathValue("id"), in.Values); e != nil {
			return e
		}
		jsonResponse(w, 200, map[string]bool{"updated": true})
		return nil
	}))
	mux.HandleFunc("POST /v1/runs/{id}/resume", wrap(func(w http.ResponseWriter, r *http.Request) error {
		tag, e := s.Store.DB.Exec(r.Context(), "UPDATE runs SET status='queued',owner='',lease_until=NULL,updated_at=now() WHERE id=$1 AND status='failed' AND NOT cancel_requested", r.PathValue("id"))
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return errors.New("only failed, non-cancelled runs may resume; unknown calls require reconciliation")
		}
		jsonResponse(w, 202, map[string]bool{"queued": true})
		return nil
	}))
	mux.HandleFunc("POST /v1/runs/{id}/reconcile", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			CallID   string          `json:"call_id"`
			Decision string          `json:"decision"`
			Evidence string          `json:"evidence"`
			Result   json.RawMessage `json:"result"`
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		if e := s.Store.Resolve(r.Context(), r.PathValue("id"), in.CallID, in.Decision, in.Evidence, in.Result); e != nil {
			return e
		}
		jsonResponse(w, 200, map[string]bool{"reconciled": true})
		return nil
	}))

}
