package runtime

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func receiptRule() ReceiptExport {
	return ReceiptExport{App: "futures", ID: "fixture-v1", Tool: "mcp_fixture_check", InputPath: []string{"arguments", "draft"}, OutputPath: []string{"content", "0", "text"}}
}

func projectedReceiptFixture(t *testing.T, s *Store, run, op, tool, status, input, output string) {
	t.Helper()
	_, err := s.DB.Exec(context.Background(), `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence,response)
 VALUES($1,$2,'tool','fixture',$3,$4,'fixture',1,$5)`, op, run,
		JSON(map[string]any{"name": tool, "arguments": map[string]string{"draft": input, "secret": "PRIVATE_ARGUMENT"}, "owner": "PRIVATE_OWNER"}), status,
		JSON(map[string]any{"content": []map[string]string{{"type": "text", "text": output}, {"type": "text", "text": "PRIVATE_OTHER_CONTENT"}}, "private": "PRIVATE_RESPONSE"}))
	if err != nil {
		t.Fatal(err)
	}
}

func TestReceiptExportConfiguration(t *testing.T) {
	rule := receiptRule()
	raw, _ := json.Marshal([]ReceiptExport{rule})
	if parsed, err := ParseReceiptExports(string(raw)); err != nil || len(parsed) != 1 {
		t.Fatal(parsed, err)
	}
	for _, raw := range []string{`{}`, `[] {}`, `[{"app":"futures","id":"x","tool":"t","input_path":[],"output_path":["text"]}]`, strings.Replace(string(raw), `"tool":`, `"unknown":`, 1), strings.Repeat(" ", 32769), "[" + string(JSON(rule)) + "," + string(JSON(rule)) + "]"} {
		if _, err := ParseReceiptExports(raw); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	if rules, err := ParseReceiptExports(""); err != nil || len(rules) != 0 {
		t.Fatal("optional config must default closed")
	}
}

func TestProjectedReceiptScopePairingAndUnicode(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	rule := receiptRule()
	bindEvidenceApp(t, s, root.ID, rule.App)
	for _, child := range []struct {
		id     string
		parent any
	}{{"draft-child", root.ID}, {"draft-foreign", nil}} {
		if _, err := s.DB.Exec(ctx, `INSERT INTO runs(id,submission_key,input_hash,parent_id,snapshot,task) SELECT $1,$1,'fixture',$2,snapshot,'fixture' FROM runs WHERE id=$3`, child.id, child.parent, root.ID); err != nil {
			t.Fatal(err)
		}
	}
	bindEvidenceApp(t, s, "draft-foreign", "other")
	input := strings.Repeat("豆粕🙂\n", 3000)
	output := `{"validator":"fixture-check-v1","phase":"draft_only"}`
	projectedReceiptFixture(t, s, root.ID, root.ID+"/old", rule.Tool, "completed", "older", output)
	projectedReceiptFixture(t, s, "draft-child", "draft-child/new", rule.Tool, "completed", input, output)
	projectedReceiptFixture(t, s, "draft-foreign", "draft-foreign/read", rule.Tool, "completed", "PRIVATE_FOREIGN", output)
	for _, tc := range []struct{ suffix, tool, status string }{{"unknown", rule.Tool, "unknown"}, {"inflight", rule.Tool, "inflight"}, {"other", "other_tool", "completed"}} {
		projectedReceiptFixture(t, s, root.ID, root.ID+"/"+tc.suffix, tc.tool, tc.status, "PRIVATE_EXCLUDED", output)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE runs SET status='failed',state='{"result":"","error":"fixture budget failure"}' WHERE id=$1`, root.ID); err != nil {
		t.Fatal(err)
	}
	page, err := s.readAppReceiptPage(ctx, root.ID, "", rule, 1)
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	second, err := s.readAppReceiptPage(ctx, root.ID, page.NextCursor, rule, 1)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Items[0].OperationID == page.Items[0].OperationID {
		t.Fatal(second, err)
	}
	for _, p := range []appReceiptPage{page, second} {
		b, _ := json.Marshal(p)
		if strings.Contains(string(b), "PRIVATE_") || strings.Contains(string(b), "older") {
			t.Fatal("raw receipt exposed")
		}
	}
	var full strings.Builder
	offset, version := 0, ""
	for {
		part, e := s.readAppReceiptPart(ctx, root.ID, "draft-child/new", "input", version, rule, offset, 511)
		if e != nil || !utf8.ValidString(part.Text) {
			t.Fatal(part, e)
		}
		full.WriteString(part.Text)
		version = part.Version
		if part.NextOffset == nil {
			break
		}
		offset = *part.NextOffset
	}
	if full.String() != input {
		t.Fatal("Unicode data changed")
	}
	part, err := s.readAppReceiptPart(ctx, root.ID, "draft-child/new", "output", version, rule, 0, 8192)
	if err != nil || part.Text != output {
		t.Fatal(part, err)
	}
	_, err = s.readAppReceiptPart(ctx, root.ID, "draft-foreign/read", "input", "", rule, 0, 100)
	requireStateStatus(t, err, 404)
	_, err = s.readAppReceiptPart(ctx, root.ID, root.ID+"/unknown", "input", "", rule, 0, 100)
	requireStateStatus(t, err, 404)
	_, err = s.readAppReceiptPart(ctx, root.ID, "draft-child/new", "input", "", rule, 1, 100)
	requireStateStatus(t, err, 400)
	_, err = s.readAppReceiptPart(ctx, root.ID, "draft-child/new", "input", version, rule, 100000, 100)
	requireStateStatus(t, err, 416)
	foreign := rule
	foreign.App = "other"
	_, err = s.readAppReceiptPage(ctx, root.ID, "", foreign, 10)
	requireStateStatus(t, err, 404)
	changed := rule
	changed.InputPath = []string{"arguments", "secret"}
	_, err = s.readAppReceiptPage(ctx, root.ID, page.NextCursor, changed, 1)
	requireStateStatus(t, err, 400)
	_, err = s.readAppReceiptPart(ctx, root.ID, "draft-child/new", "input", version, changed, 0, 100)
	requireStateStatus(t, err, 409)
	if _, err = s.DB.Exec(ctx, `UPDATE calls SET response=jsonb_set(response,'{content,0,text}','"changed feedback"') WHERE id='draft-child/new'`); err != nil {
		t.Fatal(err)
	}
	_, err = s.readAppReceiptPart(ctx, root.ID, "draft-child/new", "input", version, rule, 0, 100)
	requireStateStatus(t, err, 409)
	var status, result string
	if err = s.DB.QueryRow(ctx, `SELECT status,state->>'result' FROM runs WHERE id=$1`, root.ID).Scan(&status, &result); err != nil || status != "failed" || result != "" {
		t.Fatal("read changed failed task", status, result, err)
	}
}

func TestProjectedReceiptHTTPBoundary(t *testing.T) {
	s, root, _ := workingFixture(t)
	rule := receiptRule()
	bindEvidenceApp(t, s, root.ID, "futures")
	op := root.ID + "/draft"
	projectedReceiptFixture(t, s, root.ID, op, rule.Tool, "completed", "draft", "feedback")
	peers := map[string]string{}
	for _, role := range []string{"futures", "control", "worker", "toolhub", "knowledge", "runtime"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	server := &Server{Store: s}
	raw := string(JSON([]ReceiptExport{rule}))
	if err := server.ConfigureReceiptExports(raw); err != nil {
		t.Fatal(err)
	}
	h := server.Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
	base := "/v1/apps/futures/runs/" + root.ID + "/receipts/" + rule.ID
	request := func(role, path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+testToken+role)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for _, role := range []string{"runtime", "control"} {
		w := request(role, "/v1/receipt-projections")
		var got struct{ Protocol, ConfigurationSHA256 string }
		var response map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		got.Protocol, _ = response["protocol"].(string)
		got.ConfigurationSHA256, _ = response["configuration_sha256"].(string)
		if w.Code != 200 || got.Protocol != "app-receipt-projection-v1" || got.ConfigurationSHA256 != server.receiptExportHash || response["profiles"] != float64(1) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, role := range []string{"futures", "worker", "toolhub", "knowledge"} {
		if w := request(role, "/v1/receipt-projections"); w.Code != 403 {
			t.Fatal("configuration status exposed", role, w.Code)
		}
	}
	for _, path := range []string{base, base + "/record?operation_id=" + url.QueryEscape(op) + "&field=input"} {
		if w := request("futures", path); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "PRIVATE_") {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, role := range []string{"control", "worker", "toolhub", "knowledge"} {
			if w := request(role, path); w.Code != 403 {
				t.Fatal("unexpected grant", role, w.Code)
			}
		}
	}
	for _, suffix := range []string{"?tool=other", "?limit=21", "?after=", "?limit=1&limit=2", "/record?operation_id=x&field=request", "/record?operation_id=x&field=input&input_path=secret", "/record?operation_id=x&field=input&limit=16385"} {
		if w := request("futures", base+suffix); w.Code != 400 {
			t.Fatal(suffix, w.Code, w.Body.String())
		}
	}
	if w := request("futures", strings.Replace(base, "/futures/", "/other/", 1)); w.Code != 403 {
		t.Fatal(w.Code)
	}
	if w := request("futures", strings.Replace(base, rule.ID, "unconfigured", 1)); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := request("futures", "/v1/runs/"+root.ID+"/calls/record?operation_id="+url.QueryEscape(op)); w.Code != 403 {
		t.Fatal("raw API exposed")
	}
}
