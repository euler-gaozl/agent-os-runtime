package runtime

import (
	"context"
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

func appEvidenceFixture(t *testing.T, s *Store, run, operation, document, status, kind, tool, text string, at time.Time) {
	t.Helper()
	_, err := s.DB.Exec(context.Background(), `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence,response,created_at,error)
 VALUES($1,$2,$3,'fixture',$4,$5,'fixture',1,$6,$7,'PRIVATE_ERROR_FIXTURE')`, operation, run, kind,
		JSON(map[string]any{"name": tool, "arguments": map[string]any{"id": document}, "private": "PRIVATE_REQUEST_FIXTURE"}), status,
		JSON(map[string]any{"document": map[string]any{"id": document, "title": strings.Repeat("资料", 150), "source": "https://source.invalid/original", "kind": "evidence", "content": text, "metadata": map[string]string{"hidden": "PRIVATE_METADATA_FIXTURE"}}, "next_offset": utf8.RuneCountInString(text), "has_more": false, "hidden": "PRIVATE_RESPONSE_FIXTURE"}), at)
	if err != nil {
		t.Fatal(err)
	}
}

func bindEvidenceApp(t *testing.T, s *Store, run, app string) {
	t.Helper()
	if _, err := s.DB.Exec(context.Background(), `INSERT INTO app_runs(run_id,app,metadata) VALUES($1,$2,'{}')`, run, app); err != nil {
		t.Fatal(err)
	}
}

func TestAppEvidenceScopesPaginationAndProjection(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	bindEvidenceApp(t, s, root.ID, "futures")
	for _, child := range []struct {
		id     string
		parent any
	}{{"fixture-child", root.ID}, {"fixture-foreign", nil}} {
		if _, err := s.DB.Exec(ctx, `INSERT INTO runs(id,submission_key,input_hash,parent_id,snapshot,task) SELECT $1,$1,'fixture',$2,snapshot,'fixture' FROM runs WHERE id=$3`, child.id, child.parent, root.ID); err != nil {
			t.Fatal(err)
		}
	}
	bindEvidenceApp(t, s, "fixture-foreign", "other")
	when := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 35; i++ {
		run := root.ID
		if i%2 == 0 {
			run = "fixture-child"
		}
		appEvidenceFixture(t, s, run, fmt.Sprintf("%s/source/%03d", run, i), fmt.Sprintf("document-%02d", i), "completed", "tool", "document_read", strings.Repeat("证据🙂", 5000), when)
	}
	for i, tc := range []struct{ run, status, kind, tool string }{{"fixture-foreign", "completed", "tool", "document_read"}, {root.ID, "unknown", "tool", "document_read"}, {root.ID, "inflight", "tool", "document_read"}, {root.ID, "completed", "model", "document_read"}, {root.ID, "completed", "tool", "knowledge_search"}, {root.ID, "completed", "tool", "mcp_document_read"}} {
		appEvidenceFixture(t, s, tc.run, fmt.Sprintf("%s/excluded/%d", tc.run, i), "excluded", tc.status, tc.kind, tc.tool, "PRIVATE_NOT_EVIDENCE_FIXTURE", when)
	}
	seen := map[string]bool{}
	after := ""
	var firstCursor string
	for {
		page, err := s.readAppEvidence(ctx, "futures", root.ID, "", after, 6)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(page)
		if len(page.Items) > 6 || len(b) > 24000 || strings.Contains(string(b), "PRIVATE_") {
			t.Fatal("unbounded/private projection", len(b))
		}
		for _, item := range page.Items {
			if seen[item.OperationID] || utf8.RuneCountInString(item.Preview) > 512 || utf8.RuneCountInString(item.Title) > 200 || !item.PreviewTruncated || item.SourceTruncated {
				t.Fatal("invalid projection/page")
			}
			seen[item.OperationID] = true
		}
		if firstCursor == "" {
			firstCursor = page.NextCursor
		}
		if page.NextCursor == "" {
			break
		}
		after = page.NextCursor
	}
	if len(seen) != 35 {
		t.Fatal("missing or leaked source", len(seen))
	}
	page, err := s.readAppEvidence(ctx, "futures", root.ID, "document-02", "", 20)
	if err != nil || len(page.Items) != 1 || page.Items[0].RunID != "fixture-child" {
		t.Fatal(page, err)
	}
	_, err = s.readAppEvidence(ctx, "futures", root.ID, "document-02", firstCursor, 20)
	requireStateStatus(t, err, 400)
	_, err = s.readAppEvidence(ctx, "other", root.ID, "", "", 20)
	requireStateStatus(t, err, 404)
	_, err = s.readAppEvidence(ctx, "futures", "fixture-foreign", "", "", 20)
	requireStateStatus(t, err, 404)
	_, err = s.readAppEvidenceText(ctx, "futures", root.ID, "fixture-foreign/excluded/0", 0, 100, "")
	requireStateStatus(t, err, 404)
}

func TestAppEvidenceRetainedUnicodeAndChangedVersion(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	bindEvidenceApp(t, s, root.ID, "futures")
	text := strings.Repeat("甲醇🙂\n<raw>", 700)
	operation := root.ID + "/read"
	appEvidenceFixture(t, s, root.ID, operation, "fixture-document", "completed", "tool", "document_read", text, time.Now())
	var full strings.Builder
	offset, version := 0, ""
	for {
		part, err := s.readAppEvidenceText(ctx, "futures", root.ID, operation, offset, 511, version)
		if err != nil || !utf8.ValidString(part.Text) || utf8.RuneCountInString(part.Text) > 511 {
			t.Fatal(part, err)
		}
		b, _ := json.Marshal(part)
		if strings.Contains(string(b), "PRIVATE_") {
			t.Fatal("raw receipt details leaked")
		}
		full.WriteString(part.Text)
		version = part.Version
		if part.NextOffset == nil {
			break
		}
		offset = *part.NextOffset
	}
	if full.String() != text {
		t.Fatal("retained material changed")
	}
	_, err := s.readAppEvidenceText(ctx, "futures", root.ID, operation, 1, 20, "")
	requireStateStatus(t, err, 400)
	_, err = s.readAppEvidenceText(ctx, "futures", root.ID, operation, 999999, 20, version)
	requireStateStatus(t, err, 416)
	if _, err = s.DB.Exec(ctx, `UPDATE calls SET response=jsonb_set(response,'{document,content}','"changed"') WHERE id=$1`, operation); err != nil {
		t.Fatal(err)
	}
	_, err = s.readAppEvidenceText(ctx, "futures", root.ID, operation, 1, 20, version)
	requireStateStatus(t, err, 409)
	if _, err = s.DB.Exec(ctx, `UPDATE calls SET response=jsonb_set(response,'{document,id}','"mismatched"') WHERE id=$1`, operation); err != nil {
		t.Fatal(err)
	}
	_, err = s.readAppEvidenceText(ctx, "futures", root.ID, operation, 0, 20, "")
	requireStateStatus(t, err, 404)
}

func TestAppEvidenceTruncationFlags(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	bindEvidenceApp(t, s, root.ID, "futures")
	for _, size := range []int{512, 513} {
		operation := fmt.Sprintf("%s/read/%d", root.ID, size)
		document := fmt.Sprintf("document-%d", size)
		appEvidenceFixture(t, s, root.ID, operation, document, "completed", "tool", "document_read", strings.Repeat("证", size), time.Now())
		// Exactly on the boundary remains complete; one Unicode character over
		// must be marked so clients cannot treat a partial URL as provenance.
		source := "https://source.invalid/" + strings.Repeat("源", 2048-len("https://source.invalid/")+size-512)
		if _, err := s.DB.Exec(ctx, `UPDATE calls SET response=jsonb_set(response,'{document,source}',to_jsonb($2::text)) WHERE id=$1`, operation, source); err != nil {
			t.Fatal(err)
		}
		page, err := s.readAppEvidence(ctx, "futures", root.ID, document, "", 20)
		if err != nil || len(page.Items) != 1 {
			t.Fatal(page, err)
		}
		part, err := s.readAppEvidenceText(ctx, "futures", root.ID, operation, 0, 8192, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range []appEvidence{page.Items[0], part.appEvidence} {
			if item.PreviewTruncated != (size > 512) || item.SourceTruncated != (size > 512) || utf8.RuneCountInString(item.Source) != 2048 || utf8.RuneCountInString(item.Preview) != 512 {
				t.Fatalf("incorrect truncation flags or Unicode bounds at %d", size)
			}
		}
	}
}

func TestAppEvidenceHTTPBoundary(t *testing.T) {
	s, root, _ := workingFixture(t)
	bindEvidenceApp(t, s, root.ID, "futures")
	operation := root.ID + "/read"
	appEvidenceFixture(t, s, root.ID, operation, "fixture-document", "completed", "tool", "document_read", "actual archived fixture", time.Now())
	peers := map[string]string{}
	for _, role := range []string{"futures", "control", "worker", "toolhub", "knowledge", "telemetry"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	h := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
	base := "/v1/apps/futures/runs/" + root.ID + "/evidence"
	request := func(role, path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+testToken+role)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for _, path := range []string{base, base + "/record?operation_id=" + url.QueryEscape(operation)} {
		if w := request("futures", path); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, role := range []string{"control", "worker", "toolhub", "knowledge", "telemetry"} {
			if w := request(role, path); w.Code != 403 {
				t.Fatal("unauthorized", role, w.Code)
			}
		}
	}
	for _, suffix := range []string{"?limit=31", "?document_id=", "?after=", "?limit=1&limit=2", "?field=request", "/record", "/record?operation_id=x&offset=1", "/record?operation_id=x&field=request", "/record?operation_id=x&limit=8193"} {
		if w := request("futures", base+suffix); w.Code != 400 {
			t.Fatal(suffix, w.Code, w.Body.String())
		}
	}
	if w := request("futures", strings.Replace(base, "/futures/", "/other/", 1)); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := request("futures", "/v1/runs/"+root.ID+"/calls/record?operation_id="+url.QueryEscape(operation)); w.Code != 403 {
		t.Fatal("raw receipt route became public")
	}
}
