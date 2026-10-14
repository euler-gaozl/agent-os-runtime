package runtime

// Deployment-owned projections expose selected strings from committed receipts.
// Clients cannot select tools, JSON paths or arbitrary request/response fields.
import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type ReceiptExport struct {
	App        string   `json:"app"`
	ID         string   `json:"id"`
	Tool       string   `json:"tool"`
	InputPath  []string `json:"input_path"`
	OutputPath []string `json:"output_path"`
}

var exportName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,120}$`)

func (s *Server) ConfigureReceiptExports(raw string) error {
	rules, err := ParseReceiptExports(raw)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(raw))
	s.ReceiptExports, s.receiptExportHash = rules, hex.EncodeToString(sum[:])
	return nil
}

func ParseReceiptExports(raw string) ([]ReceiptExport, error) {
	if raw == "" {
		return nil, nil
	}
	fail := errors.New("invalid APP_RECEIPT_EXPORTS: expected up to 16 unique app/id projections with explicit tool and string paths")
	if len(raw) > 32768 {
		return nil, fail
	}
	var rules []ReceiptExport
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&rules); err != nil || len(rules) > 16 {
		return nil, fail
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, fail
	}
	seen := map[string]bool{}
	for _, rule := range rules {
		key := rule.App + "/" + rule.ID
		if !exportName.MatchString(rule.App) || !exportName.MatchString(rule.ID) || !exportName.MatchString(rule.Tool) || seen[key] {
			return nil, fail
		}
		seen[key] = true
		for _, path := range [][]string{rule.InputPath, rule.OutputPath} {
			if len(path) == 0 || len(path) > 8 {
				return nil, fail
			}
			for _, part := range path {
				if !exportName.MatchString(part) {
					return nil, fail
				}
			}
		}
	}
	return rules, nil
}

func (rule ReceiptExport) version() string {
	b, _ := json.Marshal(rule)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type appReceiptItem struct {
	OperationID  string    `json:"operation_id"`
	RunID        string    `json:"run_id"`
	RecordedAt   time.Time `json:"recorded_at"`
	InputSHA256  string    `json:"input_sha256"`
	OutputSHA256 string    `json:"output_sha256"`
	InputChars   int       `json:"input_chars"`
	OutputChars  int       `json:"output_chars"`
	Version      string    `json:"version"`
}

type appReceiptPage struct {
	Items      []appReceiptItem `json:"items"`
	NextCursor string           `json:"next_cursor"`
}

type appReceiptPart struct {
	appReceiptItem
	Field      string `json:"field"`
	Text       string `json:"text"`
	Offset     int    `json:"offset"`
	NextOffset *int   `json:"next_offset"`
}

const exportedReceiptPredicate = `kind='tool' AND status='completed' AND request->>'name'=$2
 AND jsonb_typeof(request #> $3::text[])='string' AND jsonb_typeof(response #> $4::text[])='string'`
const exportedReceiptColumns = `id,run_id,created_at,
 encode(sha256(convert_to(request #>> $3::text[],'UTF8')),'hex'),
 encode(sha256(convert_to(response #>> $4::text[],'UTF8')),'hex'),
 length(request #>> $3::text[]),length(response #>> $4::text[]),
 encode(sha256(convert_to(jsonb_build_array($5::text,request #> $3::text[],response #> $4::text[])::text,'UTF8')),'hex')`

func scanAppReceipt(row pgx.Row, item *appReceiptItem, extra ...any) error {
	args := []any{&item.OperationID, &item.RunID, &item.RecordedAt, &item.InputSHA256, &item.OutputSHA256, &item.InputChars, &item.OutputChars, &item.Version}
	return row.Scan(append(args, extra...)...)
}

func (s *Store) readAppReceiptPage(ctx context.Context, root, after string, rule ReceiptExport, limit int) (appReceiptPage, error) {
	out := appReceiptPage{Items: []appReceiptItem{}}
	if limit < 1 || limit > 20 {
		return out, stateError(400, "invalid receipt page")
	}
	filter := "export:" + rule.version()
	cursor, err := parseCallCursor(after, root, filter)
	if err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ids, err := s.appEvidenceRuns(ctx, rule.App, root)
	if err != nil {
		return out, err
	}
	args := []any{ids, rule.Tool, rule.InputPath, rule.OutputPath, rule.version(), limit + 1}
	q := `SELECT ` + exportedReceiptColumns + ` FROM calls WHERE run_id=ANY($1) AND ` + exportedReceiptPredicate
	if cursor != nil {
		q += ` AND (created_at,id)<($7,$8)`
		args = append(args, cursor.At, cursor.ID)
	}
	q += ` ORDER BY created_at DESC,id DESC LIMIT $6`
	rows, err := s.DB.Query(ctx, q, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var item appReceiptItem
		if err = scanAppReceipt(rows, &item); err != nil {
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

func (s *Store) readAppReceiptPart(ctx context.Context, root, operation, field, version string, rule ReceiptExport, offset, limit int) (appReceiptPart, error) {
	out := appReceiptPart{Field: field, Offset: offset}
	if operation == "" || len(operation) > 512 || (field != "input" && field != "output") || offset < 0 || offset > 2147483646 || limit < 1 || limit > 16384 ||
		(version != "" && (len(version) != 64 || strings.Trim(version, "0123456789abcdef") != "")) || (offset > 0 && version == "") {
		return out, stateError(400, "invalid projected receipt segment or missing version")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ids, err := s.appEvidenceRuns(ctx, rule.App, root)
	if err != nil {
		return out, err
	}
	column := `request #>> $3::text[]`
	if field == "output" {
		column = `response #>> $4::text[]`
	}
	row := s.DB.QueryRow(ctx, `SELECT `+exportedReceiptColumns+`,substring((`+column+`) FROM $7::integer FOR $8::integer)
 FROM calls WHERE run_id=ANY($1) AND id=$6 AND `+exportedReceiptPredicate, ids, rule.Tool, rule.InputPath, rule.OutputPath, rule.version(), operation, offset+1, limit+1)
	if err = scanAppReceipt(row, &out.appReceiptItem, &out.Text); errors.Is(err, pgx.ErrNoRows) {
		return out, stateError(404, "projected receipt not found in this task")
	}
	if err != nil {
		return out, err
	}
	if version != "" && version != out.Version {
		return out, stateError(409, "receipt projection changed; reload first segment")
	}
	if offset > 0 && out.Text == "" {
		return out, stateError(416, "offset outside projected receipt")
	}
	chars := []rune(out.Text)
	if len(chars) > limit {
		next := offset + limit
		out.NextOffset = &next
		out.Text = string(chars[:limit])
	}
	return out, nil
}

func (s *Server) appReceiptRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/receipt-projections", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.RawQuery != "" {
			return stateError(400, "unexpected receipt projection status query")
		}
		if s.receiptExportHash == "" {
			return stateError(503, "receipt projection configuration not initialized")
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, map[string]any{"protocol": "app-receipt-projection-v1", "configuration_sha256": s.receiptExportHash, "profiles": len(s.ReceiptExports)})
		return nil
	}))
	ruleFor := func(r *http.Request) (ReceiptExport, error) {
		for _, rule := range s.ReceiptExports {
			if rule.App == r.PathValue("app") && rule.ID == r.PathValue("profile") {
				return rule, nil
			}
		}
		return ReceiptExport{}, stateError(404, "receipt projection is not configured")
	}
	mux.HandleFunc("GET /v1/apps/{app}/runs/{id}/receipts/{profile}", wrap(func(w http.ResponseWriter, r *http.Request) error {
		rule, err := ruleFor(r)
		if err != nil {
			return err
		}
		q, err := callQuery(r, "after", "limit")
		if err != nil {
			return err
		}
		limit, err := callNumber(q, "limit", 10, 1, 20)
		if err != nil {
			return err
		}
		page, err := s.Store.readAppReceiptPage(r.Context(), r.PathValue("id"), q.Get("after"), rule, limit)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, page)
		return nil
	}))
	mux.HandleFunc("GET /v1/apps/{app}/runs/{id}/receipts/{profile}/record", wrap(func(w http.ResponseWriter, r *http.Request) error {
		rule, err := ruleFor(r)
		if err != nil {
			return err
		}
		q, err := callQuery(r, "operation_id", "field", "version", "offset", "limit")
		if err != nil {
			return err
		}
		offset, err := callNumber(q, "offset", 0, 0, 2147483646)
		if err != nil {
			return err
		}
		limit, err := callNumber(q, "limit", 8192, 1, 16384)
		if err != nil {
			return err
		}
		part, err := s.Store.readAppReceiptPart(r.Context(), r.PathValue("id"), q.Get("operation_id"), q.Get("field"), q.Get("version"), rule, offset, limit)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, part)
		return nil
	}))
}
