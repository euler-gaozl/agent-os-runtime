package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// Independent Worker executable + fixed Node/DSH + Runtime HTTP + actual owner
// and worker PostgreSQL roles. TLS supplier, checker and archive HTTP are fixtures.
// These checks prove byte identity/recovery, not research content correctness.
func TestIndependentFinalWorkerOriginalAndRecovery(t *testing.T) {
	for _, mode := range []string{"original", "selection_kill", "result_kill", "select_sql", "finish_sql", "cancel", "partial", "model503"} {
		t.Run(mode, func(t *testing.T) { testFinalWorker(t, mode) })
	}
}

func testFinalWorker(t *testing.T, mode string) {
	binary, _, node := requireEvidenceWorker(t)
	runner := os.Getenv("TEST_FINAL_DSH_RUNNER")
	if runner == "" {
		t.Fatal("fixed final-v5 runner required")
	}
	db, role, workerRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if e := Migrate(ctx, db, role, workerRole); e != nil {
		t.Fatal(e)
	}
	token := testToken + "final-worker"
	s := NewStore(rolePool(t, db, role), &serviceclient.Client{Token: token}, "")
	if e := s.ConfigureSubmissionEngine(EngineDSHFinal); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer((&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"worker": servicehttp.TokenHash(token), "control": servicehttp.TokenHash(token + "control")}}))
	defer server.Close()
	draft := "中文🙂e\u0301\r\n" + strings.Repeat("checked actual17 / plan20\n", 12000) + "END  \n"
	var generations, checks atomic.Int64
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var inv Invocation
		if req.Header.Get("Authorization") != "Bearer "+token || json.NewDecoder(req.Body).Decode(&inv) != nil || inv.Name != "mcp_fixture_check" {
			w.WriteHeader(400)
			return
		}
		var input map[string]string
		if json.Unmarshal(inv.Arguments, &input) != nil || input["draft"] != draft {
			t.Error("checker original changed")
			w.WriteHeader(400)
			return
		}
		tx, e := s.DB.Begin(req.Context())
		if e != nil {
			t.Error(e)
			w.WriteHeader(500)
			return
		}
		defer tx.Rollback(req.Context())
		if _, e = checkInvocation(req.Context(), tx, inv); e != nil {
			t.Error(e)
			w.WriteHeader(409)
			return
		}
		raw := JSON(map[string]any{"structuredContent": map[string]any{"ready": true}})
		if e = insertCall(req.Context(), tx, inv, "completed", raw); e == nil {
			e = tx.Commit(req.Context())
		}
		if e != nil {
			t.Error(e)
			w.WriteHeader(500)
			return
		}
		checks.Add(1)
		_, _ = w.Write(raw)
	}))
	defer hub.Close()
	model := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
			Max      int       `json:"max_tokens"`
		}
		if json.NewDecoder(req.Body).Decode(&body) != nil || body.Max != 393216 {
			t.Error("frozen native limits lost")
			w.WriteHeader(400)
			return
		}
		n := generations.Add(1)
		if mode == "model503" {
			if n == 2 {
				jsonResponse(w, 503, map[string]any{"error": map[string]string{"message": "synthetic temporary failure after check"}})
				return
			}
			if n > 2 {
				n-- // Explicit retry of the second logical generation.
			}
		}
		if mode == "partial" {
			_, _ = w.Write([]byte(`{"choices":[`))
			return
		}
		name, args := "mcp_fixture_check", map[string]string{"draft": draft}
		if n == 2 {
			// The supplier only uses the authoritative receipt it was shown;
			// do not obtain the operation ID/hash through fixture SQL.
			var receipt struct {
				Receipt struct {
					Operation string `json:"operation_id"`
					Final     struct {
						SHA   string `json:"sha256"`
						Ready bool   `json:"ready"`
					} `json:"final_output"`
				} `json:"receipt"`
			}
			found := false
			for _, m := range body.Messages {
				if m.Role == "tool" && json.Unmarshal([]byte(m.Content), &receipt) == nil && receipt.Receipt.Final.Ready {
					found = true
					break
				}
			}
			if !found || receipt.Receipt.Final.SHA != Hash([]byte(draft)) {
				t.Error("model did not see checked receipt")
				w.WriteHeader(500)
				return
			}
			name, args = contracts.FinalOutputToolName, map[string]string{"operation_id": receipt.Receipt.Operation, "sha256": receipt.Receipt.Final.SHA}
		} else if n != 1 {
			t.Error("unexpected additional supplier request", n)
			w.WriteHeader(500)
			return
		}
		m := Message{Role: "assistant", ToolCalls: []ToolCall{{ID: fmt.Sprintf("fixture-%d", n), Type: "function", Function: FunctionCall{Name: name, Arguments: string(JSON(args))}}}}
		jsonResponse(w, 200, map[string]any{"choices": []any{map[string]any{"message": m, "finish_reason": "tool_calls"}}, "usage": map[string]int{"prompt_tokens": 200, "completion_tokens": 50, "total_tokens": 250}})
	}))
	defer model.Close()
	ca := filepath.Join(t.TempDir(), "fixture-ca.pem")
	if e := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: model.Certificate().Raw}), 0600); e != nil {
		t.Fatal(e)
	}
	snap := nativeSnapshot()
	snap.Agent.Model.BaseURL = model.URL
	snap.Agent.Model.Limits.BaseURL = model.URL
	snap.Agent.Model.Limits.Source = model.URL + "/models"
	snap.Agent.FinalOutput = &contracts.FinalOutputPolicy{Protocol: contracts.FinalOutputProtocol, Tool: "mcp_fixture_check", InputField: "draft", ReadyPath: []string{"structuredContent", "ready"}}
	snap.Connectors = []Connector{{Name: "fixture", Tools: []Tool{{Name: "check", InputSchema: JSON(map[string]any{"type": "object", "properties": map[string]any{"draft": map[string]string{"type": "string"}}, "required": []string{"draft"}})}}}}
	id, e := s.Submit(ctx, ID(), "Synthetic supplied actual17 and plan20 only; check once and select its receipt.", snap, nil)
	if e != nil {
		t.Fatal(e)
	}
	if strings.HasSuffix(mode, "_sql") {
		method := "final_output_select"
		if mode == "finish_sql" {
			method = "finish_ref"
		}
		_, e = db.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION final_process_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.method='%s' AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'fixture final write interruption'; END IF; RETURN NEW; END $$; CREATE TRIGGER final_process_fault BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION final_process_fault()`, method))
		if e != nil {
			t.Fatal(e)
		}
	}
	point := make(chan struct{})
	release := make(chan struct{})
	var interrupted atomic.Bool
	archive := newArchiveHTTPFixtureToken(t, id, func(req *http.Request) bool {
		if (mode != "selection_kill" && mode != "result_kill" && mode != "cancel") || interrupted.Load() {
			return false
		}
		var selection, checkpoint []byte
		e := db.QueryRow(req.Context(), `SELECT COALESCE((SELECT response FROM harness_exchanges WHERE run_id=$1 AND method='final_output_select' AND response->>'selected'='true' ORDER BY seq DESC LIMIT 1),'null'::jsonb),COALESCE((SELECT response FROM harness_exchanges WHERE run_id=$1 AND method='checkpoint' AND response IS NOT NULL ORDER BY seq DESC LIMIT 1),'null'::jsonb)`, id).Scan(&selection, &checkpoint)
		if e != nil {
			t.Error(e)
			return false
		}
		var chosen struct {
			Selected bool   `json:"selected"`
			CallID   string `json:"call_id"`
		}
		var saved struct {
			Events []struct {
				Type    string `json:"type"`
				Turn    int    `json:"turn"`
				CallID  string `json:"call_id"`
				Success bool   `json:"success"`
				Hash    string `json:"receipt_sha256"`
			} `json:"finalization_events"`
		}
		if json.Unmarshal(selection, &chosen) != nil || json.Unmarshal(checkpoint, &saved) != nil {
			t.Error("invalid frozen checkpoint fixture")
			return false
		}
		turn, ended := 0, false
		for _, event := range saved.Events {
			if event.Type == "tool/result" && event.CallID == chosen.CallID && event.Success && event.Hash == canonicalHash(selection) {
				turn = event.Turn
			}
			if event.Type == "turn/end" && event.Success && event.Turn == turn && turn > 0 {
				ended = true
			}
		}
		if chosen.Selected && (mode != "result_kill" || ended) && interrupted.CompareAndSwap(false, true) {
			close(point)
			select {
			case <-req.Context().Done():
			case <-release:
			}
			return true
		}
		return false
	}, token)
	defer close(release)
	s.StorageURL = archive.server.URL
	config := map[string]string{"DATABASE_URL": evidenceWorkerDSN(t, db, workerRole), "SERVICE_TOKEN": token, "SERVICE_PEERS": string(JSON(map[string]string{"worker": servicehttp.TokenHash(token)})), "RUNTIME_URL": server.URL, "REGISTRY_URL": server.URL, "TOOLHUB_URL": hub.URL, "STORAGE_URL": archive.server.URL, "AGENTOS_NODE": node, "DSH_FINAL_RUNNER": runner, "SSL_CERT_FILE": ca, "GODEBUG": "x509sslcertoverrideplatform=1", "LISTEN": "127.0.0.1:0", "INSTANCE_ID": "final-first", "DB_MAX_CONNS": "3"}
	first, done := startEvidenceWorker(t, ctx, binary, config)
	var result Run
	var failedAttempt []byte
	resumed := false
	for {
		result, e = s.Run(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		if !resumed && (strings.HasSuffix(mode, "_kill") || mode == "cancel") && interrupted.Load() {
			<-point
			if mode == "cancel" {
				if e = s.Cancel(ctx, id); e != nil {
					t.Fatal(e)
				}
			} else {
				if e = first.Process.Kill(); e != nil {
					t.Fatal(e)
				}
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("kill timeout")
				}
				if _, e = db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", id); e != nil {
					t.Fatal(e)
				}
				config["INSTANCE_ID"] = "final-replacement"
				_, done = startEvidenceWorker(t, ctx, binary, config)
			}
			resumed = true
		}
		if !resumed && strings.HasSuffix(mode, "_sql") && result.Status == "waiting_unknown" {
			preview, err := readSourceRecovery(ctx, s.DB, id)
			if err != nil || !preview.Allowed {
				t.Fatal("actual Worker failure not recoverable", preview, err)
			}
			if _, e = db.Exec(ctx, "DROP TRIGGER final_process_fault ON harness_exchanges"); e != nil {
				t.Fatal(e)
			}
			if e = s.resumeSourceState(ctx, id); e != nil {
				t.Fatal(e)
			}
			resumed = true
			continue
		}
		if !resumed && mode == "model503" && result.Status == "failed" {
			// Stop the first actual process before queueing, so the management
			// phase can be checked without racing a Worker claim/model attempt.
			if e = first.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("failed Worker stop timeout")
			}
			if e = db.QueryRow(ctx, "SELECT to_jsonb(a) FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 AND a.status='failed'", id).Scan(&failedAttempt); e != nil {
				t.Fatal(e)
			}
			frozen := canonicalHash(JSON(result.Snapshot))
			version := actionContext(t, ctx, server.Client(), server.URL, token+"control", id)
			path := "/v1/runs/" + id + "/continuation"
			request := actionBody("final-worker-retry", version)
			accepted := actionCheck(t, actionHTTP(ctx, server.Client(), server.URL, token+"control", "POST", path, request), 202)
			if !bytes.Equal(accepted, actionCheck(t, actionHTTP(ctx, server.Client(), server.URL, token+"control", "POST", path, request), 202)) ||
				!bytes.Equal(accepted, actionCheck(t, actionHTTP(ctx, server.Client(), server.URL, token+"control", "GET", path+"/requests/final-worker-retry", nil), 200)) {
				t.Fatal("explicit retry did not retain one stable receipt")
			}
			queued, err := s.Run(ctx, id)
			if err != nil || queued.Status != "queued" || canonicalHash(JSON(queued.Snapshot)) != frozen || generations.Load() != 2 || checks.Load() != 1 {
				t.Fatal("management changed policy or repeated an external effect", err)
			}
			config["INSTANCE_ID"] = "final-after-503"
			_, done = startEvidenceWorker(t, ctx, binary, config)
			resumed = true
			continue
		}
		if result.Status == "completed" || result.Status == "failed" || result.Status == "waiting_unknown" || result.Status == "cancelled" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("final Worker timeout", mode, result.Status)
		case <-done:
			t.Fatal("Worker exited")
		case <-time.After(25 * time.Millisecond):
		}
	}
	u, e := s.Usage(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if mode == "partial" {
		if result.Status != "failed" || u.UnknownUsageAttempts != 1 || generations.Load() != 1 {
			t.Fatal("partial response misclassified", result.Status, u)
		}
		path := "/v1/runs/" + id + "/continuation"
		body := actionCheck(t, actionHTTP(ctx, server.Client(), server.URL, token+"control", "GET", path+"/request-context", nil), 200)
		var preview struct {
			Assessment string `json:"assessment"`
			Reason     string `json:"reason"`
		}
		if json.Unmarshal(body, &preview) != nil || preview.Assessment != "manual_review" || preview.Reason != "failure_not_assessed" {
			t.Fatal("partial response became eligible for continuation")
		}
		rejected := actionCheck(t, actionHTTP(ctx, server.Client(), server.URL, token+"control", "POST", path, actionBody("partial-must-not-retry", strings.Repeat("0", 64))), 409)
		actionReceipt(t, rejected, "partial-must-not-retry", id, strings.Repeat("0", 64), "rejected", "failure_not_assessed")
		return
	}
	if mode == "cancel" {
		if !resumed || result.Status != "cancelled" || result.State.Result != "" {
			t.Fatal("cancel did not converge", result.Status, result.State.Error)
		}
		return
	}
	if result.Status != "completed" || result.State.Result != draft {
		t.Fatal("checked original not delivered", result.Status, result.State.Error)
	}
	wantRequests, wantUnknown := int64(2), int64(0)
	if mode == "model503" {
		wantRequests, wantUnknown = 3, 1
		var retained []byte
		if e = db.QueryRow(ctx, "SELECT to_jsonb(a) FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 AND a.status='failed'", id).Scan(&retained); e != nil || !bytes.Equal(retained, failedAttempt) {
			t.Fatal("old 503 attempt was overwritten", e)
		}
		var continuationReceipts, queueEvents int
		if e = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM run_continuation_receipts WHERE run_id=$1),(SELECT count(*) FROM events WHERE run_id=$1 AND kind='continuation_queued')", id).Scan(&continuationReceipts, &queueEvents); e != nil || continuationReceipts != 1 || queueEvents != 1 {
			t.Fatal("duplicate continuation effects", e, continuationReceipts, queueEvents)
		}
	}
	if generations.Load() != wantRequests || checks.Load() != 1 || int64(u.Attempts) != wantRequests || int64(u.UnknownUsageAttempts) != wantUnknown {
		t.Fatal("recovery repeated effects", generations.Load(), checks.Load(), u)
	}
	if mode != "original" && (!resumed || result.Fence != 2) {
		t.Fatal("expected recovery not exercised", resumed, result.Fence)
	}
	var receipts, pending int
	if e = db.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id=$1 AND kind='final_output_completed'", id).Scan(&receipts); e != nil {
		t.Fatal(e)
	}
	if e = db.QueryRow(ctx, "SELECT count(*) FROM harness_archives WHERE run_id=$1 AND (manifest_hash='' OR batch IS NOT NULL)", id).Scan(&pending); e != nil {
		t.Fatal(e)
	}
	if receipts != 1 || pending != 0 {
		t.Fatal("incomplete/duplicate durable completion", receipts, pending)
	}
	t.Logf("mode=%s bytes=%d sha256=%s fence=%d provider=%d checker=1 unknown_usage=%d; actual Worker/Node/Runtime HTTP/PG, fixture provider/checker/archive", mode, len(draft), Hash([]byte(draft)), result.Fence, generations.Load(), u.UnknownUsageAttempts)
}
