package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func insertCallReadFixture(t *testing.T, s *Store, run, id, status string, body any, at time.Time) {
	t.Helper()
	_, err := s.DB.Exec(context.Background(), `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence,response,created_at,error)
	 VALUES($1,$2,'tool','fixture',$3,$4,'fixture',1,$5,$6,$7)`, id, run,
		JSON(map[string]any{"name": strings.Repeat("工具", 130), "arguments": strings.Repeat("REQUEST_SECRET_FIXTURE", 2000)}), status, body, at, strings.Repeat("错", 220))
	if err != nil {
		t.Fatal(err)
	}
}

func TestCallReadPaginationScopesTiesAndUnresolved(t *testing.T) {
	s, r, original := workingFixture(t)
	ctx := context.Background()
	when := time.Date(2020, 1, 2, 3, 4, 5, 123456000, time.UTC)
	for i := 0; i < 63; i++ {
		status := "completed"
		if i == 0 {
			status = "unknown"
		}
		if i == 1 {
			status = "inflight"
		}
		insertCallReadFixture(t, s, r.ID, fmt.Sprintf("%s/fixture/%03d", r.ID, i), status, JSON(map[string]any{"body": strings.Repeat("BODY_SECRET_FIXTURE", 1600)}), when)
	}
	seen := map[string]bool{}
	after := ""
	var firstCursor string
	for {
		page, err := s.readCallPage(ctx, r.ID, "all", after, 7)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(page)
		if len(page.Items) > 7 || len(body) > 19000 || strings.Contains(string(body), "SECRET_FIXTURE") || strings.Contains(string(body), `"request"`) || strings.Contains(string(body), `"response"`) {
			t.Fatal("unbounded/full receipt in summary", len(body))
		}
		for _, item := range page.Items {
			if seen[item.ID] {
				t.Fatal("duplicate across timestamp ties", item.ID)
			}
			seen[item.ID] = true
			if utf8.RuneCountInString(item.ToolName) > 100 || utf8.RuneCountInString(item.ErrorPreview) > 160 {
				t.Fatal("unbounded preview")
			}
		}
		if firstCursor == "" {
			firstCursor = page.NextCursor
		}
		if page.NextCursor == "" {
			break
		}
		after = page.NextCursor
	}
	if len(seen) != 64 || !seen[original.OperationID] {
		t.Fatal("missing retained receipts", len(seen))
	}
	page, err := s.readCallPage(ctx, r.ID, "unresolved", "", 1)
	if err != nil || len(page.Items) != 1 || !strings.HasSuffix(page.Items[0].ID, "/001") || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	page, err = s.readCallPage(ctx, r.ID, "unresolved", page.NextCursor, 1)
	if err != nil || len(page.Items) != 1 || !strings.HasSuffix(page.Items[0].ID, "/000") || page.NextCursor != "" {
		t.Fatal(page, err)
	}
	_, err = s.readCallPage(ctx, r.ID, "unresolved", firstCursor, 7)
	requireStateStatus(t, err, 400)
	_, err = s.readCallPage(ctx, "another-run", "all", firstCursor, 7)
	requireStateStatus(t, err, 400)
	_, err = s.readCallPage(ctx, "missing-run", "all", "", 7)
	requireStateStatus(t, err, 404)
	for _, invalid := range []string{"!", strings.Repeat("a", 2049), base64.RawURLEncoding.EncodeToString([]byte(`{} {}`))} {
		_, err = s.readCallPage(ctx, r.ID, "all", invalid, 7)
		requireStateStatus(t, err, 400)
	}
	// Migration must supply both seek paths. ANALYZE/plan costs depend on corpus;
	// test index eligibility explicitly, not a planner choice for only 64 rows.
	conn, err := s.DB.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, "SET enable_seqscan=off; SET enable_bitmapscan=off"); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, "RESET enable_seqscan; RESET enable_bitmapscan")
	for _, tc := range []struct{ where, index string }{{"", "calls_run_page"}, {" AND status IN ('unknown','inflight')", "calls_unresolved_page"}} {
		var plan []byte
		err = conn.QueryRow(ctx, "EXPLAIN (FORMAT JSON) SELECT id FROM calls WHERE run_id=$1"+tc.where+" ORDER BY created_at DESC,id DESC LIMIT 8", r.ID).Scan(&plan)
		if err != nil || !strings.Contains(string(plan), tc.index) {
			t.Fatal("seek index unavailable", string(plan), err)
		}
	}
}

func TestCallReadSegmentsUnicodeVersionNullAndScope(t *testing.T) {
	s, r, _ := workingFixture(t)
	ctx := context.Background()
	id := r.ID + "/large/工具"
	insertCallReadFixture(t, s, r.ID, id, "completed", JSON(map[string]any{"text": strings.Repeat("甲醇🙂\n<raw>", 20000)}), time.Now())
	var persisted string
	if err := s.DB.QueryRow(ctx, "SELECT response::text FROM calls WHERE id=$1", id).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	var full strings.Builder
	offset, version := 0, ""
	for {
		part, err := s.readCallText(ctx, r.ID, id, "response", offset, 8192, version)
		if err != nil {
			t.Fatal(err)
		}
		if part.ValueNull || !utf8.ValidString(part.Text) || utf8.RuneCountInString(part.Text) > 8192 {
			t.Fatal("invalid bounded unicode segment")
		}
		if version != "" && version != part.Version {
			t.Fatal("version changed across immutable read")
		}
		version = part.Version
		full.WriteString(part.Text)
		if part.NextOffset == nil {
			break
		}
		if *part.NextOffset != offset+utf8.RuneCountInString(part.Text) {
			t.Fatal("offset counted UTF-8 bytes or UTF-16 units")
		}
		offset = *part.NextOffset
	}
	hash := sha256.Sum256([]byte("V" + persisted))
	if full.String() != persisted || version != hex.EncodeToString(hash[:]) {
		t.Fatal("lost/duplicated text or wrong version")
	}
	if _, err := s.DB.Exec(ctx, "UPDATE calls SET response='{}' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	_, err := s.readCallText(ctx, r.ID, id, "response", 8192, 8192, version)
	requireStateStatus(t, err, 409)
	_, err = s.readCallText(ctx, r.ID, id, "response", 1, 8192, "")
	requireStateStatus(t, err, 400)
	_, err = s.readCallText(ctx, "another-run", id, "response", 0, 8192, "")
	requireStateStatus(t, err, 404)
	first, err := s.readCallText(ctx, r.ID, id, "response", 0, 8192, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.readCallText(ctx, r.ID, id, "response", 900, 8192, first.Version)
	requireStateStatus(t, err, 416)
	for _, field := range []string{"request", "error"} {
		p, err := s.readCallText(ctx, r.ID, id, field, 0, 13, "")
		if err != nil || utf8.RuneCountInString(p.Text) != 13 || p.NextOffset == nil {
			t.Fatal(field, p, err)
		}
	}
	if _, err = s.DB.Exec(ctx, "UPDATE calls SET response=NULL WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	null, err := s.readCallText(ctx, r.ID, id, "response", 0, 8192, "")
	if err != nil || !null.ValueNull || null.Text != "" || null.NextOffset != nil {
		t.Fatal(null, err)
	}
	if _, err = s.DB.Exec(ctx, "UPDATE calls SET response='null'::jsonb WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	value, err := s.readCallText(ctx, r.ID, id, "response", 0, 8192, "")
	if err != nil || value.ValueNull || value.Text != "null" || value.Version == null.Version {
		t.Fatal(value, err)
	}
}

func TestCallReadHTTPValidationAndControlBoundary(t *testing.T) {
	s, run, source := workingFixture(t)
	peers := map[string]string{}
	for _, role := range []string{"control", "worker", "toolhub", "futures", "knowledge"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	h := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
	base := "/v1/runs/" + run.ID + "/calls/"
	request := func(role, suffix string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", base+suffix, nil)
		r.Header.Set("Authorization", "Bearer "+testToken+role)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for _, suffix := range []string{"page", "record?operation_id=" + url.QueryEscape(source.OperationID)} {
		if w := request("control", suffix); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, role := range []string{"worker", "toolhub", "futures", "knowledge"} {
			if w := request(role, suffix); w.Code != 403 {
				t.Fatal("receipt exposed to", role, w.Code)
			}
		}
	}
	for _, suffix := range []string{"page?limit=51", "page?limit=0", "page?limit=1&limit=2", "page?after=", "page?filter=unknown", "page?surprise=x", "page?bad=%zz", "record", "record?operation_id=x&field=request;DROP", "record?operation_id=x&limit=16385", "record?operation_id=x&offset=2147483647", "record?operation_id=x&offset=-1", "record?operation_id=x&offset=1", "record?operation_id=x&version=invalid"} {
		if w := request("control", suffix); w.Code != 400 {
			t.Fatal(suffix, w.Code, w.Body.String())
		}
	}
}
