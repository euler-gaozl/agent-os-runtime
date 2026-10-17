package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// App readers see only successful document_read material already retained by
// their task tree. No live Knowledge lookup, model messages, arguments, notes
// search results or arbitrary tool response can be read through this API.
type appEvidence struct {
	OperationID      string    `json:"operation_id"`
	RunID            string    `json:"run_id"`
	DocumentID       string    `json:"document_id"`
	OriginalHash     string    `json:"original_hash,omitempty"`
	Title            string    `json:"title"`
	Source           string    `json:"source"`
	Kind             string    `json:"kind"`
	Preview          string    `json:"preview"`
	PreviewTruncated bool      `json:"preview_truncated"`
	SourceTruncated  bool      `json:"source_truncated"`
	ReadOffset       int       `json:"read_offset"`
	ReadEnd          int       `json:"read_end"`
	HasMore          bool      `json:"has_more"`
	RecordedAt       time.Time `json:"recorded_at"`
}

type appEvidencePage struct {
	Items      []appEvidence `json:"items"`
	NextCursor string        `json:"next_cursor"`
}

type appEvidenceText struct {
	appEvidence
	Text       string `json:"text"`
	Offset     int    `json:"offset"`
	NextOffset *int   `json:"next_offset"`
	Version    string `json:"version"`
}

const documentReceipt = `kind='tool' AND status='completed' AND request->>'name'='document_read'
 AND jsonb_typeof(response->'document')='object'
 AND jsonb_typeof(response#>'{document,id}')='string'
 AND length(response#>>'{document,id}') BETWEEN 1 AND 120
 AND response#>>'{document,id}'=request#>>'{arguments,id}'
 AND jsonb_typeof(response#>'{document,content}')='string'`

const evidenceColumns = `id,run_id,response#>>'{document,id}',
 CASE WHEN jsonb_typeof(response#>'{document,metadata,original_hash}')='string'
 AND response#>>'{document,metadata,original_hash}' ~ '^[a-f0-9]{64}$'
 THEN response#>>'{document,metadata,original_hash}' ELSE '' END,
 left(coalesce(response#>>'{document,title}',''),200),left(coalesce(response#>>'{document,source}',''),2048),
 left(coalesce(response#>>'{document,kind}',''),40),left(response#>>'{document,content}',512),
 length(response#>>'{document,content}')>512,length(coalesce(response#>>'{document,source}',''))>2048,
 coalesce(request#>>'{arguments,offset}','0'),coalesce(response->>'next_offset','0'),
 coalesce(response->>'has_more','false')='true',created_at`

func scanAppEvidence(row pgx.Row, item *appEvidence, extra ...any) error {
	var start, end string
	args := []any{&item.OperationID, &item.RunID, &item.DocumentID, &item.OriginalHash, &item.Title, &item.Source, &item.Kind,
		&item.Preview, &item.PreviewTruncated, &item.SourceTruncated, &start, &end, &item.HasMore, &item.RecordedAt}
	if err := row.Scan(append(args, extra...)...); err != nil {
		return err
	}
	var err error
	if item.ReadOffset, err = strconv.Atoi(start); err != nil || item.ReadOffset < 0 {
		return stateError(409, "invalid retained document offset")
	}
	if item.ReadEnd, err = strconv.Atoi(end); err != nil || item.ReadEnd < item.ReadOffset {
		return stateError(409, "invalid retained document end")
	}
	return nil
}

func (s *Store) appEvidenceRuns(ctx context.Context, app, root string) ([]string, error) {
	rows, err := s.DB.Query(ctx, `WITH RECURSIVE visible AS (
 SELECT run_id AS id FROM app_runs WHERE app=$1 AND run_id=$2
 UNION SELECT child.id FROM runs child JOIN visible parent ON child.parent_id=parent.id)
 SELECT id FROM visible LIMIT 129`, app, root)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, stateError(404, "app task not found")
	}
	if len(ids) > 128 {
		return nil, stateError(409, "evidence task tree exceeds read limit")
	}
	return ids, nil
}

func (s *Store) readAppEvidence(ctx context.Context, app, root, document, after string, limit int) (appEvidencePage, error) {
	out := appEvidencePage{Items: []appEvidence{}}
	if limit < 1 || limit > 30 || len(document) > 120 {
		return out, stateError(400, "invalid evidence page")
	}
	filter := app + ":document:" + document
	cursor, err := parseCallCursor(after, root, filter)
	if err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ids, err := s.appEvidenceRuns(ctx, app, root)
	if err != nil {
		return out, err
	}
	query := `SELECT ` + evidenceColumns + ` FROM calls WHERE run_id=ANY($1) AND ` + documentReceipt
	args := []any{ids, limit + 1, document}
	query += ` AND ($3='' OR response#>>'{document,id}'=$3)`
	if cursor != nil {
		query += ` AND (created_at,id)<($4,$5)`
		args = append(args, cursor.At, cursor.ID)
	}
	query += ` ORDER BY created_at DESC,id DESC LIMIT $2`
	rows, err := s.DB.Query(ctx, query, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var item appEvidence
		if err = scanAppEvidence(rows, &item); err != nil {
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
		b, _ := json.Marshal(callCursor{root, filter, last.RecordedAt, last.OperationID})
		out.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	return out, nil
}

func (s *Store) readAppEvidenceText(ctx context.Context, app, root, operation string, offset, limit int, version string) (appEvidenceText, error) {
	out := appEvidenceText{Offset: offset}
	if operation == "" || len(operation) > 512 || offset < 0 || offset > 2147483646 || limit < 1 || limit > 8192 ||
		(version != "" && (len(version) != 64 || strings.Trim(version, "0123456789abcdef") != "")) || (offset > 0 && version == "") {
		return out, stateError(400, "invalid evidence segment or missing version")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ids, err := s.appEvidenceRuns(ctx, app, root)
	if err != nil {
		return out, err
	}
	row := s.DB.QueryRow(ctx, `SELECT `+evidenceColumns+`,
 substring(response#>>'{document,content}' FROM $3::integer FOR $4::integer),
 encode(sha256(convert_to(response::text,'UTF8')),'hex')
 FROM calls WHERE run_id=ANY($1) AND id=$2 AND `+documentReceipt, ids, operation, offset+1, limit+1)
	if err = scanAppEvidence(row, &out.appEvidence, &out.Text, &out.Version); errors.Is(err, pgx.ErrNoRows) {
		return out, stateError(404, "document receipt not found in this task")
	} else if err != nil {
		return out, err
	}
	if version != "" && version != out.Version {
		return out, stateError(409, "retained document changed; reload first segment")
	}
	if offset > 0 && out.Text == "" {
		return out, stateError(416, "offset outside retained document segment")
	}
	chars := []rune(out.Text)
	if len(chars) > limit {
		next := offset + limit
		out.NextOffset = &next
		out.Text = string(chars[:limit])
	}
	return out, nil
}

func (s *Server) appEvidenceRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/apps/{app}/runs/{id}/evidence", wrap(func(w http.ResponseWriter, r *http.Request) error {
		q, err := callQuery(r, "document_id", "after", "limit")
		if err != nil {
			return err
		}
		limit, err := callNumber(q, "limit", 20, 1, 30)
		if err != nil {
			return err
		}
		page, err := s.Store.readAppEvidence(r.Context(), r.PathValue("app"), r.PathValue("id"), q.Get("document_id"), q.Get("after"), limit)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, page)
		return nil
	}))
	mux.HandleFunc("GET /v1/apps/{app}/runs/{id}/evidence/record", wrap(func(w http.ResponseWriter, r *http.Request) error {
		q, err := callQuery(r, "operation_id", "offset", "limit", "version")
		if err != nil {
			return err
		}
		offset, err := callNumber(q, "offset", 0, 0, 2147483646)
		if err != nil {
			return err
		}
		limit, err := callNumber(q, "limit", 4096, 1, 8192)
		if err != nil {
			return err
		}
		part, err := s.Store.readAppEvidenceText(r.Context(), r.PathValue("app"), r.PathValue("id"), q.Get("operation_id"), offset, limit, q.Get("version"))
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, part)
		return nil
	}))
}
