package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// Management-only bounded live views. Cursor order is by opaque id/sequence,
// not a claim of chronological completeness. Original bodies use /documents/id.
func (s *Server) noteReadRoutes(mux *http.ServeMux, prefix string) {
	mux.HandleFunc("GET "+prefix+"/notes", wrap(func(w http.ResponseWriter, r *http.Request) error {
		query, after, state := r.URL.Query().Get("q"), r.URL.Query().Get("after"), r.URL.Query().Get("state")
		if len(query) > 1200 || len(after) > 200 || !Has([]string{"", "active", "expired", "retired", "conflict"}, state) {
			return errors.New("invalid note filter")
		}
		pattern := "%" + strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(query) + "%"
		rows, e := s.Store.DB.Query(r.Context(), `SELECT jsonb_build_object('id',d.id,'root_id',n.root_id,'title',d.title,'kind',d.kind,'scope',d.scope,'source',d.source,'content',left(d.content,240),'valid_until',d.valid_until,
 'state',CASE WHEN n.state='active' AND d.valid_until<=now() THEN 'expired' ELSE n.state END)
 FROM note_roots n JOIN documents d ON d.id=COALESCE(n.current_id,n.root_id)
 WHERE n.root_id COLLATE "C">$1 AND d.scope NOT LIKE 'knowledge:%' AND (d.title ILIKE $2 OR d.content ILIKE $2 OR d.scope ILIKE $2)
 AND ($3='' OR $3=CASE WHEN n.state='active' AND d.valid_until<=now() THEN 'expired' ELSE n.state END)
 ORDER BY n.root_id COLLATE "C" LIMIT 26`, after, pattern, state)
		if e != nil {
			return e
		}
		defer rows.Close()
		items := []json.RawMessage{}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				return e
			}
			items = append(items, b)
		}
		if e = rows.Err(); e != nil {
			return e
		}
		next := ""
		if len(items) > 25 {
			items = items[:25]
			var last struct {
				RootID string `json:"root_id"`
			}
			if e = json.Unmarshal(items[24], &last); e != nil {
				return e
			}
			next = last.RootID
		}
		jsonResponse(w, 200, map[string]any{"items": items, "next_cursor": next})
		return nil
	}))
	mux.HandleFunc("GET "+prefix+"/notes/{id}/history", wrap(func(w http.ResponseWriter, r *http.Request) error {
		after := r.URL.Query().Get("after")
		if len(after) > 200 {
			return errors.New("invalid cursor")
		}
		state, e := noteLifecycle(r.Context(), s.Store.DB, r.PathValue("id"))
		if e != nil {
			return e
		}
		rows, e := s.Store.DB.Query(r.Context(), `SELECT jsonb_build_object('id',d.id,'title',d.title,'source',d.source,'supersedes',d.supersedes,'valid_until',d.valid_until,'created_at',d.created_at)
 FROM documents d JOIN note_versions v ON v.document_id=d.id WHERE v.root_id=$1 AND d.id COLLATE "C">$2 ORDER BY d.id COLLATE "C" LIMIT 26`, state.RootID, after)
		if e != nil {
			return e
		}
		defer rows.Close()
		items := []json.RawMessage{}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				return e
			}
			items = append(items, b)
		}
		if e = rows.Err(); e != nil {
			return e
		}
		next := ""
		if len(items) > 25 {
			items = items[:25]
			var last struct{ ID string }
			if e = json.Unmarshal(items[24], &last); e != nil {
				return e
			}
			next = last.ID
		}
		jsonResponse(w, 200, map[string]any{"lifecycle": state, "items": items, "next_cursor": next})
		return nil
	}))
	mux.HandleFunc("GET "+prefix+"/notes/{id}/audit", wrap(func(w http.ResponseWriter, r *http.Request) error {
		after := int64(0)
		var e error
		if v := r.URL.Query().Get("after"); v != "" {
			after, e = strconv.ParseInt(v, 10, 64)
			if e != nil || after < 0 {
				return errors.New("invalid audit cursor")
			}
		}
		state, e := noteLifecycle(r.Context(), s.Store.DB, r.PathValue("id"))
		if e != nil {
			return e
		}
		rows, e := s.Store.DB.Query(r.Context(), `SELECT to_jsonb(a) FROM note_audits a WHERE root_id=$1 AND seq>$2 ORDER BY seq LIMIT 26`, state.RootID, after)
		if e != nil {
			return e
		}
		defer rows.Close()
		items := []json.RawMessage{}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				return e
			}
			items = append(items, b)
		}
		if e = rows.Err(); e != nil {
			return e
		}
		next := ""
		if len(items) > 25 {
			items = items[:25]
			var last struct{ Seq int64 }
			if e = json.Unmarshal(items[24], &last); e != nil {
				return e
			}
			next = strconv.FormatInt(last.Seq, 10)
		}
		jsonResponse(w, 200, map[string]any{"items": items, "next_cursor": next})
		return nil
	}))
}
