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

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func diagApp(t *testing.T, s *Store, run, app string, metadata map[string]any, at time.Time) {
	t.Helper()
	if _, err := s.DB.Exec(context.Background(), `INSERT INTO app_runs(run_id,app,metadata,created_at) VALUES($1,$2,$3,$4)`, run, app, JSON(metadata), at); err != nil {
		t.Fatal(err)
	}
}

// diagClone copies the immutable snapshot of a fixture run and applies the
// terminal state and creation time that the projection reads.
func diagClone(t *testing.T, s *Store, base, id, status string, at time.Time) {
	t.Helper()
	_, err := s.DB.Exec(context.Background(), `INSERT INTO runs(id,submission_key,input_hash,snapshot,task,status,created_at,updated_at)
 SELECT $1,$1,'fixture',snapshot,'fixture',$3,$4,$4 FROM runs WHERE id=$2`, id, base, status, at)
	if err != nil {
		t.Fatal(err)
	}
}

func diagEvent(t *testing.T, s *Store, run, kind string, payload any) int64 {
	t.Helper()
	var seq int64
	if err := s.DB.QueryRow(context.Background(), `INSERT INTO events(run_id,kind,payload) VALUES($1,$2,$3) RETURNING seq`, run, kind, JSON(payload)).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

func diagCall(t *testing.T, s *Store, run, id, kind, status, tool, arguments, errText string) {
	t.Helper()
	request := JSON(map[string]any{"name": tool, "arguments": json.RawMessage(arguments)})
	if _, err := s.DB.Exec(context.Background(), `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence,error) VALUES($1,$2,$3,'fixture',$4,$5,'fixture',1,$6)`, id, run, kind, request, status, errText); err != nil {
		t.Fatal(err)
	}
}

func diagModelUsage(t *testing.T, s *Store, run, callID, attempt string, prompt, completion int64) {
	t.Helper()
	if _, err := s.DB.Exec(context.Background(), `INSERT INTO model_attempts(id,call_id,status,request,response) VALUES($1,$2,'completed','{"max_tokens":4096}',$3)`,
		attempt, callID, JSON(map[string]any{"usage": map[string]int64{"prompt_tokens": prompt, "completion_tokens": completion, "prompt_cache_hit_tokens": 10}})); err != nil {
		t.Fatal(err)
	}
}

// diagToolFailureFixture reproduces the recorded shape of dd80cadf: two
// successful model requests, two completed reads, then a rejected query and a
// masked terminal checkpoint.
func diagToolFailureFixture(t *testing.T, s *Store, base string, at time.Time) string {
	t.Helper()
	run := "diag-tool-failure"
	diagClone(t, s, base, run, "failed", at)
	diagApp(t, s, run, "futures", map[string]any{"commodity": "豆粕", "period": "2026-07-20 至 2026-10-20"}, at)
	diagEvent(t, s, run, "checkpoint", map[string]any{"step": 0, "status": "running", "result": "", "error": ""})
	for _, m := range []struct{ call, attempt string }{{run + "/dsh/4", "attempt-a"}, {run + "/dsh/12", "attempt-b"}} {
		diagCall(t, s, run, m.call, "model", "completed", "", "{}", "")
		diagModelUsage(t, s, run, m.call, m.attempt, 20000, 100)
		diagEvent(t, s, run, "model_started", map[string]any{"call_id": m.call, "attempt": m.attempt})
		diagEvent(t, s, run, "model_completed", map[string]any{"call_id": m.call, "attempt": m.attempt})
	}
	for _, tool := range []struct{ op, name, args string }{
		{run + "/dsh/6", "kb_search", `{"kb_id":"futures-library","query":"豆粕"}`},
		{run + "/dsh/8", "knowledge_search", `{"query":"豆粕 供给 库存"}`},
	} {
		diagCall(t, s, run, tool.op, "tool", "completed", tool.name, tool.args, "")
		diagEvent(t, s, run, "tool_completed", map[string]any{"name": tool.name, "operation_id": tool.op})
	}
	rejected := run + "/dsh/14"
	diagCall(t, s, run, rejected, "tool", "retryable", "kb_search",
		`{"kb_id":"futures-library","query":"豆"}`,
		`service HTTP 400: {"error":"query requires at least two adjacent Han characters or a word/number"} PRIVATE_SECRET https://provider.invalid/v1`)
	diagEvent(t, s, run, "tool_started", map[string]any{"name": "kb_search", "operation_id": rejected})
	diagEvent(t, s, run, "tool_retryable", map[string]any{"operation_id": rejected, "error": "PRIVATE_SECRET https://provider.invalid/v1"})
	diagEvent(t, s, run, "checkpoint", map[string]any{"step": 2, "status": "failed", "result": "", "error": "service HTTP 400: {\"error\":\"upstream request rejected\"}"})
	return run
}

func TestAppRunDiagnosticsLinkRejectedToolWithoutLeakingReceipts(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	run := diagToolFailureFixture(t, s, root.ID, time.Now().Add(-time.Hour))
	got, err := s.readRunDiagnostics(ctx, "futures", run)
	if err != nil {
		t.Fatal(err)
	}
	if got.Protocol != diagnosticsProtocol || got.RunID != run || got.Status != "failed" {
		t.Fatal("identity", got)
	}
	if got.Stage != "tool" || got.Code != "parameter_rejected" || !got.CauseConfirmed {
		t.Fatal("classification", got)
	}
	if got.OperationID != run+"/dsh/14" || got.RelatedRole != "tool" || got.Tool != "kb_search" {
		t.Fatal("operation projection", got)
	}
	if got.SafeParameter == nil || got.SafeParameter.Name != "query" || got.SafeParameter.Value != "豆" {
		t.Fatal("safe parameter", got.SafeParameter)
	}
	if len(got.RelatedEventIDs) != 2 || got.RelatedEventIDs[0] >= got.RelatedEventIDs[1] {
		t.Fatal("event linkage", got.RelatedEventIDs)
	}
	if got.Usage.Attempts != 2 || got.Usage.UnknownUsageAttempts != 0 || got.Usage.PromptTokens != 40000 || got.Usage.CompletionTokens != 200 {
		t.Fatal("usage projection", got.Usage)
	}
	if got.RecommendedAction != "correct_input" || got.RetryPolicy != "manual_after_correction" {
		t.Fatal("action projection", got)
	}
	if len(got.FailureVersion) != len("fvd1:")+64 || !strings.HasPrefix(got.FailureVersion, "fvd1:") {
		t.Fatal("failure version", got.FailureVersion)
	}
	body, _ := json.Marshal(got)
	text := string(body)
	for _, forbidden := range []string{"two adjacent", "upstream request rejected", "PRIVATE", "https://", "provider.invalid", "error\":\""} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("diagnostic projection leaked %q: %s", forbidden, text)
		}
	}
	// A late duplicate read does not change the projection.
	again, err := s.readRunDiagnostics(ctx, "futures", run)
	if err != nil || again.FailureVersion != got.FailureVersion {
		t.Fatal("repeat read is not stable", err)
	}
}

func TestAppRunDiagnosticsSeparatesRecoveredErrorsAndTerminalStates(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	at := time.Now().Add(-time.Hour)
	recovered := "diag-recovered"
	diagClone(t, s, root.ID, recovered, "failed", at)
	diagApp(t, s, recovered, "futures", map[string]any{"commodity": "甲醇"}, at)
	diagCall(t, s, recovered, recovered+"/tool", "tool", "retryable", "kb_search", `{"query":"库存"}`, "service HTTP 503: busy")
	diagEvent(t, s, recovered, "tool_retryable", map[string]any{"operation_id": recovered + "/tool", "error": "service HTTP 503: busy"})
	diagEvent(t, s, recovered, "tool_completed", map[string]any{"operation_id": recovered + "/tool", "name": "kb_search"})
	diagCall(t, s, recovered, recovered+"/model", "model", "failed", "", "{}", "model token budget exhausted")
	diagEvent(t, s, recovered, "model_failed", map[string]any{"call_id": recovered + "/model"})
	diagEvent(t, s, recovered, "checkpoint", map[string]any{"step": 4, "status": "failed", "error": "model token budget exhausted"})
	got, err := s.readRunDiagnostics(ctx, "futures", recovered)
	if err != nil {
		t.Fatal(err)
	}
	if got.Stage != "model" || got.Code != "budget_exhausted" || !got.CauseConfirmed || got.OperationID != recovered+"/model" {
		t.Fatal("recovered tool error was treated as the cause", got)
	}

	cancelled := "diag-cancelled"
	diagClone(t, s, root.ID, cancelled, "cancelled", at)
	diagApp(t, s, cancelled, "futures", map[string]any{"commodity": "豆粕"}, at)
	diagEvent(t, s, cancelled, "checkpoint", map[string]any{"step": 1, "status": "cancelled", "error": ""})
	got, err = s.readRunDiagnostics(ctx, "futures", cancelled)
	if err != nil || got.Stage != "cancel" || got.Code != "cancelled" || !got.CauseConfirmed || got.RetryPolicy != "do_not_retry" {
		t.Fatal("cancelled", got, err)
	}

	unknownEffect := "diag-unknown-effect"
	diagClone(t, s, root.ID, unknownEffect, "waiting_unknown", at)
	diagApp(t, s, unknownEffect, "futures", map[string]any{"commodity": "豆粕"}, at)
	diagCall(t, s, unknownEffect, unknownEffect+"/write", "tool", "unknown", "note_save", `{"text":"x"}`, "service HTTP 409: conflict")
	diagEvent(t, s, unknownEffect, "tool_unknown", map[string]any{"operation_id": unknownEffect + "/write", "error": "PRIVATE_EFFECT"})
	diagEvent(t, s, unknownEffect, "checkpoint", map[string]any{"step": 2, "status": "waiting_unknown", "error": "reconcile required"})
	got, err = s.readRunDiagnostics(ctx, "futures", unknownEffect)
	if err != nil || got.Code != "effect_unknown" || got.Stage != "tool" || got.RetryPolicy != "operator_review" {
		t.Fatal("unknown effect", got, err)
	}

	unconfirmed := "diag-unconfirmed"
	diagClone(t, s, root.ID, unconfirmed, "failed", at)
	diagApp(t, s, unconfirmed, "futures", map[string]any{"commodity": "豆粕"}, at)
	diagEvent(t, s, unconfirmed, "checkpoint", map[string]any{"step": 3, "status": "failed", "error": "persistence unavailable"})
	got, err = s.readRunDiagnostics(ctx, "futures", unconfirmed)
	if err != nil || got.Code != "unknown" || got.CauseConfirmed || got.Stage != "unknown" {
		t.Fatal("unconfirmed cause must stay unconfirmed", got, err)
	}
}

func TestAppRunDiagnosticsHTTPIsolationAndBounds(t *testing.T) {
	s, root, _ := workingFixture(t)
	run := diagToolFailureFixture(t, s, root.ID, time.Now().Add(-time.Hour))
	peers := map[string]string{}
	for _, role := range []string{"futures", "control", "worker", "toolhub", "knowledge", "telemetry"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	h := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
	request := func(method, role, path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("Authorization", "Bearer "+testToken+role)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	base := "/v1/apps/futures/runs/" + run + "/diagnostics"
	if w := request("GET", "futures", base); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("GET", "control", base); w.Code != 200 {
		t.Fatal("control projection", w.Code)
	}
	for _, role := range []string{"worker", "toolhub", "knowledge", "telemetry"} {
		if w := request("GET", role, base); w.Code != 403 {
			t.Fatal("role gained diagnostics", role, w.Code)
		}
	}
	if w := request("GET", "futures", strings.Replace(base, "/futures/", "/other/", 1)); w.Code != 403 {
		t.Fatal("cross-app diagnostics", w.Code)
	}
	if w := request("GET", "futures", "/v1/apps/futures/runs/missing/diagnostics"); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("POST", "futures", base); w.Code != 403 {
		t.Fatal("diagnostics accepted a write method", w.Code)
	}
	if w := request("GET", "futures", base+"?app=other"); w.Code != 400 {
		t.Fatal("diagnostics accepted a query", w.Code)
	}
}

func TestAppRunFailuresPaginationFiltersAndCoverage(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	// Newer successful tasks must not push old failures out of the list, and
	// they must not be counted as failures.
	for i := 0; i < 120; i++ {
		id := fmt.Sprintf("diag-success-%03d", i)
		diagClone(t, s, root.ID, id, "completed", base.Add(time.Duration(i)*time.Minute))
		diagApp(t, s, id, "futures", map[string]any{"commodity": "甲醇"}, base.Add(time.Duration(i)*time.Minute))
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("diag-failure-%d", i)
		at := base.Add(time.Duration(i) * time.Hour)
		diagClone(t, s, root.ID, id, "failed", at)
		diagApp(t, s, id, "futures", map[string]any{"commodity": []string{"豆粕", "甲醇"}[i%2]}, at)
		rejected := id + "/tool"
		diagCall(t, s, id, rejected, "tool", "retryable", "kb_search", `{"query":"豆"}`, "service HTTP 400: invalid query")
		diagEvent(t, s, id, "tool_retryable", map[string]any{"operation_id": rejected, "error": "service HTTP 400: invalid query"})
		diagEvent(t, s, id, "checkpoint", map[string]any{"step": 1, "status": "failed", "error": "upstream request rejected"})
	}
	foreign := "diag-foreign"
	diagClone(t, s, root.ID, foreign, "failed", base.Add(30*24*time.Hour))
	diagApp(t, s, foreign, "other", map[string]any{"commodity": "豆粕"}, base.Add(30*24*time.Hour))
	diagEvent(t, s, foreign, "checkpoint", map[string]any{"step": 1, "status": "failed", "error": "x"})

	since := base.Add(-24 * time.Hour)
	until := base.Add(60 * 24 * time.Hour)
	f := failureFilter{since: since, until: until}
	seen := map[string]bool{}
	after := ""
	pages := 0
	for {
		page, err := s.readAppRunFailures(ctx, "futures", f, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if len(page.Items) > 2 {
			t.Fatal("page exceeded limit")
		}
		for _, item := range page.Items {
			if seen[item.RunID] || item.Code != "parameter_rejected" || !item.CauseConfirmed {
				t.Fatal("duplicate, foreign or misclassified row", item)
			}
			seen[item.RunID] = true
		}
		if !page.Coverage.Complete && page.NextCursor == "" {
			t.Fatal("incomplete page without a cursor")
		}
		if page.NextCursor == "" {
			break
		}
		if pages > 10 {
			t.Fatal("pagination did not finish")
		}
		after = page.NextCursor
	}
	if len(seen) != 5 || pages != 3 {
		t.Fatal("pagination lost or duplicated rows", seen, pages)
	}
	// Old failures remain visible beyond the 100-row app task list; the
	// newest success is outside this window but the failures are found.
	page, err := s.readAppRunFailures(ctx, "futures", failureFilter{since: since, until: base.Add(10 * time.Minute)}, "", 50)
	if err != nil || len(page.Items) != 1 || page.Coverage.Complete != true {
		t.Fatal("window boundary", page, err)
	}
	// Category, stage, commodity and status filters run before pagination.
	for _, tc := range []struct {
		f    failureFilter
		want int
	}{
		{failureFilter{since: since, until: until, commodity: "豆粕"}, 3},
		{failureFilter{since: since, until: until, commodity: "甲醇"}, 2},
		{failureFilter{since: since, until: until, commodity: "豆粕", stage: "tool"}, 3},
		{failureFilter{since: since, until: until, category: "downstream_unavailable"}, 0},
		{failureFilter{since: since, until: until, stage: "model"}, 0},
	} {
		tc.f.status = "failed"
		page, err := s.readAppRunFailures(ctx, "futures", tc.f, "", 50)
		if err != nil || len(page.Items) != tc.want {
			t.Fatal("filter", tc.f, len(page.Items), err)
		}
	}
	if _, err := s.readAppRunFailures(ctx, "futures", failureFilter{since: since, until: since}, "", 10); err == nil {
		t.Fatal("empty window accepted")
	}
	if _, err := s.readAppRunFailures(ctx, "futures", failureFilter{since: until.Add(-failureWindowMaximum - time.Hour), until: until}, "", 10); err == nil {
		t.Fatal("oversized window accepted")
	}
	// A cursor is bound to app, window and filters.
	first, err := s.readAppRunFailures(ctx, "futures", failureFilter{since: since, until: until, status: "failed"}, "", 1)
	if err != nil || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	if _, err := s.readAppRunFailures(ctx, "futures", failureFilter{since: since, until: until, status: "cancelled"}, first.NextCursor, 1); err == nil {
		t.Fatal("cursor reused across filters")
	}
	if page, err := s.readAppRunFailures(ctx, "other", failureFilter{since: since, until: until}, "", 10); err != nil || len(page.Items) != 1 || page.Items[0].RunID != foreign {
		t.Fatal("foreign app list is isolated", page, err)
	}
}

func TestAppRunFailuresHTTPReadOnlyAndInjection(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	run := diagToolFailureFixture(t, s, root.ID, now.Add(-time.Hour))
	var beforeStatus string
	var beforeEvents int
	var beforeUpdated time.Time
	if err := s.DB.QueryRow(ctx, `SELECT status,updated_at,(SELECT count(*) FROM events WHERE run_id=$1) FROM runs WHERE id=$1`, run).Scan(&beforeStatus, &beforeUpdated, &beforeEvents); err != nil {
		t.Fatal(err)
	}
	peers := map[string]string{}
	for _, role := range []string{"futures", "control", "knowledge"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	h := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
	request := func(method, role, path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("Authorization", "Bearer "+testToken+role)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	listBase := "/v1/apps/futures/run-failures?since=" + url.QueryEscape(now.Add(-48*time.Hour).Format(time.RFC3339)) + "&until=" + url.QueryEscape(now.Add(time.Hour).Format(time.RFC3339))
	if w := request("GET", "futures", listBase); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	} else if !strings.Contains(w.Body.String(), run) || !strings.Contains(w.Body.String(), "parameter_rejected") {
		t.Fatal("failure missing from list", w.Body.String())
	}
	if w := request("GET", "knowledge", listBase); w.Code != 403 {
		t.Fatal("knowledge read failures", w.Code)
	}
	if w := request("GET", "futures", strings.Replace(listBase, "/futures/", "/other/", 1)); w.Code != 403 {
		t.Fatal("cross-app list", w.Code)
	}
	for _, suffix := range []string{"&limit=51", "&limit=0", "&category=bogus", "&stage=bogus", "&status=bogus", "&since=not-a-date", "&unknown=1", "&after=not-a-cursor"} {
		if w := request("GET", "futures", listBase+suffix); w.Code != 400 {
			t.Fatal(suffix, w.Code, w.Body.String())
		}
	}
	if _, err := s.readRunDiagnostics(ctx, "futures", run); err != nil {
		t.Fatal(err)
	}
	var afterStatus string
	var afterEvents int
	var afterUpdated time.Time
	if err := s.DB.QueryRow(ctx, `SELECT status,updated_at,(SELECT count(*) FROM events WHERE run_id=$1) FROM runs WHERE id=$1`, run).Scan(&afterStatus, &afterUpdated, &afterEvents); err != nil {
		t.Fatal(err)
	}
	if afterStatus != beforeStatus || beforeEvents != afterEvents || !afterUpdated.Equal(beforeUpdated) {
		t.Fatal("a read changed execution facts")
	}
}

func TestSafeParameterRejectsStatefulOrSecretArguments(t *testing.T) {
	cases := []struct {
		args string
		want bool
	}{
		{`{"query":"豆"}`, true},
		{`{"query":"豆粕 供给"}`, true},
		{`{"query":"https://leak.invalid/x"}`, false},
		{`{"query":"Bearer abcdef"}`, false},
		{`{"query":"api_key local"}`, false},
		{`{"query":"` + strings.Repeat("豆", 81) + `"}`, false},
		{`{"query":""}`, false},
		{`{"query":42}`, false},
		{`{"note":"` + strings.Repeat("x", 9000) + `"}`, false},
	}
	for _, tc := range cases {
		if got := safeToolParameter(json.RawMessage(tc.args)); (got != nil) != tc.want {
			t.Fatalf("safeToolParameter(%q)=%v", tc.args, got)
		}
	}
}
