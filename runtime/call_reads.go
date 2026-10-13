package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Management projections deliberately do not use Calls: that historical
// execution API returns complete request/response bodies for replay.
type callSummary struct {
	ID           string    `json:"id"`
	Kind         string    `json:"kind"`
	Status       string    `json:"status"`
	ToolName     string    `json:"tool_name,omitempty"`
	ErrorPreview string    `json:"error_preview,omitempty"`
	HasResponse  bool      `json:"has_response"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type callPage struct {
	RunID      string        `json:"run_id"`
	Filter     string        `json:"filter"`
	Items      []callSummary `json:"items"`
	NextCursor string        `json:"next_cursor"`
}

type callCursor struct {
	RunID  string    `json:"run_id"`
	Filter string    `json:"filter"`
	At     time.Time `json:"at"`
	ID     string    `json:"id"`
}

type callText struct {
	RunID       string `json:"run_id"`
	OperationID string `json:"operation_id"`
	Kind        string `json:"kind"`
	Status      string `json:"status"`
	Field       string `json:"field"`
	Text        string `json:"text"`
	ValueNull   bool   `json:"value_null"`
	Offset      int    `json:"offset"`
	NextOffset  *int   `json:"next_offset"`
	Version     string `json:"version"`
}

func callQuery(r *http.Request, allowed ...string) (url.Values, error) {
	if len(r.URL.RawQuery) > 4096 {
		return nil, stateError(400, "receipt query too long")
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, stateError(400, "invalid receipt query")
	}
	for key, values := range q {
		found := false
		for _, a := range allowed {
			found = found || key == a
		}
		if !found || len(values) != 1 || values[0] == "" {
			return nil, stateError(400, "unknown, duplicate or empty receipt parameter")
		}
	}
	return q, nil
}

func callNumber(q url.Values, key string, def, min, max int) (int, error) {
	if !q.Has(key) {
		return def, nil
	}
	n, err := strconv.ParseInt(q.Get(key), 10, 32)
	if err != nil || n < int64(min) || n > int64(max) {
		return 0, stateError(400, "invalid receipt "+key)
	}
	return int(n), nil
}

func parseCallCursor(encoded, run, filter string) (*callCursor, error) {
	if encoded == "" {
		return nil, nil
	}
	if len(encoded) > 2048 {
		return nil, stateError(400, "invalid receipt cursor")
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	var c callCursor
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err != nil || d.Decode(&c) != nil || c.RunID != run || c.Filter != filter || c.At.IsZero() || c.ID == "" || len(c.ID) > 512 {
		return nil, stateError(400, "invalid receipt cursor")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, stateError(400, "invalid receipt cursor")
	}
	return &c, nil
}

func (s *Store) readCallPage(ctx context.Context, run, filter, after string, limit int) (callPage, error) {
	out := callPage{RunID: run, Filter: filter, Items: []callSummary{}}
	if limit < 1 || limit > 50 || (filter != "all" && filter != "unresolved") {
		return out, stateError(400, "invalid receipt page")
	}
	c, err := parseCallCursor(after, run, filter)
	if err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var exists bool
	if err = s.DB.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM runs WHERE id=$1)", run).Scan(&exists); err != nil {
		return out, err
	}
	if !exists {
		return out, stateError(404, "run not found")
	}
	query := `SELECT id,kind,status,CASE WHEN kind='tool' THEN left(coalesce(request->>'name',''),100) ELSE '' END,
	 left(error,160),response IS NOT NULL,created_at,updated_at FROM calls WHERE run_id=$1`
	args := []any{run, limit + 1}
	if filter == "unresolved" {
		query += " AND status IN ('unknown','inflight')"
	}
	if c != nil {
		query += " AND (created_at,id)<($3,$4)"
		args = append(args, c.At, c.ID)
	}
	query += " ORDER BY created_at DESC,id DESC LIMIT $2"
	rows, err := s.DB.Query(ctx, query, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var item callSummary
		if err = rows.Scan(&item.ID, &item.Kind, &item.Status, &item.ToolName, &item.ErrorPreview, &item.HasResponse, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return out, err
		}
		out.Items = append(out.Items, item)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Items) > limit {
		out.Items = out.Items[:limit]
		last := out.Items[limit-1]
		b, _ := json.Marshal(callCursor{run, filter, last.CreatedAt, last.ID})
		out.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	return out, nil
}

func (s *Store) readCallText(ctx context.Context, run, operation, field string, offset, limit int, version string) (callText, error) {
	out := callText{RunID: run, OperationID: operation, Field: field, Offset: offset}
	if operation == "" || len(operation) > 512 || offset < 0 || offset > 2147483646 || limit < 1 || limit > 16384 ||
		(version != "" && (len(version) != 64 || strings.Trim(version, "0123456789abcdef") != "")) || (offset > 0 && version == "") {
		return out, stateError(400, "invalid receipt segment or missing version")
	}
	// Only these fixed expressions can be inserted into SQL.
	column := map[string]string{"request": "request::text", "response": "response::text", "error": "error"}[field]
	if column == "" {
		return out, stateError(400, "invalid receipt field")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Version pins the persisted PostgreSQL text representation, including SQL
	// NULL. It is not the canonical JSON response hash stored in working state.
	// DB conversion/hash cost is proportional to one field; only a bounded
	// substring is transferred/decoded by Go. No whole-history body is loaded.
	query := `SELECT kind,status,body IS NULL,
	 encode(sha256(convert_to(CASE WHEN body IS NULL THEN 'N' ELSE 'V'||body END,'UTF8')),'hex'),
	 coalesce(substring(body FROM $3::integer FOR $4::integer),'')
	 FROM (SELECT kind,status,` + column + ` AS body FROM calls WHERE run_id=$1 AND id=$2) AS receipt`
	err := s.DB.QueryRow(ctx, query, run, operation, offset+1, limit+1).Scan(&out.Kind, &out.Status, &out.ValueNull, &out.Version, &out.Text)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, stateError(404, "receipt not found in this run")
	}
	if err != nil {
		return out, err
	}
	if version != "" && version != out.Version {
		return out, stateError(409, "receipt changed; reload from the first segment")
	}
	if offset > 0 && out.Text == "" {
		return out, stateError(416, "receipt offset outside stored text")
	}
	runes := []rune(out.Text)
	if len(runes) > limit {
		next := offset + limit
		out.NextOffset = &next
		out.Text = string(runes[:limit])
	}
	return out, nil
}

func (s *Server) callReadRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/runs/{id}/calls/page", wrap(func(w http.ResponseWriter, r *http.Request) error {
		q, err := callQuery(r, "limit", "after", "filter")
		if err != nil {
			return err
		}
		limit, err := callNumber(q, "limit", 25, 1, 50)
		if err != nil {
			return err
		}
		filter := q.Get("filter")
		if filter == "" {
			filter = "all"
		}
		page, err := s.Store.readCallPage(r.Context(), r.PathValue("id"), filter, q.Get("after"), limit)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, page)
		return nil
	}))
	mux.HandleFunc("GET /v1/runs/{id}/calls/record", wrap(func(w http.ResponseWriter, r *http.Request) error {
		q, err := callQuery(r, "operation_id", "field", "offset", "limit", "version")
		if err != nil {
			return err
		}
		limit, err := callNumber(q, "limit", 8192, 1, 16384)
		if err != nil {
			return err
		}
		offset, err := callNumber(q, "offset", 0, 0, 2147483646)
		if err != nil {
			return err
		}
		field := q.Get("field")
		if field == "" {
			field = "response"
		}
		part, err := s.Store.readCallText(r.Context(), r.PathValue("id"), q.Get("operation_id"), field, offset, limit, q.Get("version"))
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, part)
		return nil
	}))
}
