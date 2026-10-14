package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// This drives an immutable independent Worker package, its real Node/DSH,
// restricted PostgreSQL and Runtime HTTP. Provider and immutable Storage are
// explicit protocol fixtures. It does not spend money or modify a live service.
func TestIndependentBudgetWorkerFinalRequestAndUsageRecovery(t *testing.T) {
	for _, mode := range []string{"serialized_overflow", "rejection_kill", "compaction_kill", "usage_pressure", "usage_control", "irreducible", "summary_overflow"} {
		t.Run(mode, func(t *testing.T) { testBudgetProcess(t, mode) })
	}
}

func testBudgetProcess(t *testing.T, mode string) {
	binary, _, node := requireEvidenceWorker(t)
	runner := os.Getenv("TEST_BUDGET_DSH_RUNNER")
	if runner == "" {
		t.Fatal("pinned budget engine package required")
	}
	db, role, workerRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	if err := Migrate(ctx, db, role, workerRole); err != nil {
		t.Fatal(err)
	}
	token := testToken + "budget-worker"
	s := NewStore(rolePool(t, db, role), &serviceclient.Client{Token: token}, "")
	if err := s.ConfigureSubmissionEngine(EngineDSHBudget); err != nil {
		t.Fatal(err)
	}
	api := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"toolhub": servicehttp.TokenHash(testToken), "worker": servicehttp.TokenHash(token)}})
	runtimeServer := httptest.NewServer(api)
	defer runtimeServer.Close()
	proxy := serviceclient.Client{RuntimeURL: runtimeServer.URL, Token: testToken}
	var tools, generations, summaries, promptTotal, outputTotal, cacheTotal atomic.Int64
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var inv Invocation
		if r.Header.Get("Authorization") != "Bearer "+token || json.NewDecoder(r.Body).Decode(&inv) != nil {
			w.WriteHeader(400)
			return
		}
		tools.Add(1)
		reply, err := proxy.Invoke(r.Context(), inv)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write(reply)
	}))
	defer hub.Close()
	final := "2026-10-01 实际库存17单位，计划20单位另列。" + strings.Repeat("answer ", 5000)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages  []Message `json:"messages"`
			MaxTokens int       `json:"max_tokens"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Messages) == 0 {
			w.WriteHeader(400)
			return
		}
		message := Message{Role: "assistant"}
		prompt, output, cached := int64(1800), int64(100), int64(900)
		if strings.Contains(body.Messages[len(body.Messages)-1].Content, "Create a concise continuity checkpoint") {
			summaries.Add(1)
			if body.MaxTokens != 2048 {
				t.Error("summary output cap changed", body.MaxTokens)
			}
			message.Content = "Synthetic continuity: saved evidence is in Runtime; original dates remain authoritative."
		} else {
			n := generations.Add(1)
			if body.MaxTokens != 6144 {
				t.Error("generation output cap changed", body.MaxTokens)
			}
			if n <= 12 {
				message.Content = strings.Repeat("synthetic history ", 180)
				if n == 12 {
					if mode == "usage_pressure" {
						prompt = 28000
					}
					if mode == "serialized_overflow" || mode == "summary_overflow" || strings.HasSuffix(mode, "_kill") {
						// Go's final provider JSON escapes '<'; DSH's text estimate
						// cannot see this transport expansion. Earlier history leaves
						// a genuinely reducible region outside the retained tail.
						message.Content = strings.Repeat("<", 10000)
						if mode == "summary_overflow" {
							message.Content = strings.Repeat("<", 12000)
						}
					}
				}
				message.ToolCalls = []ToolCall{{ID: fmt.Sprintf("budget-note-%d", n), Type: "function", Function: FunctionCall{Name: "note_save", Arguments: string(JSON(map[string]string{"kind": "fact", "title": "fixture", "content": "2026-10-01 actual 17 units; plan 20 units separate", "source": "fixture://budget"}))}}}
			} else if n == 13 {
				message.Content = final
				output = 5000
			} else {
				t.Error("provider generation repeated", n)
				w.WriteHeader(500)
				return
			}
		}
		promptTotal.Add(prompt)
		outputTotal.Add(output)
		cacheTotal.Add(cached)
		finish := "stop"
		if len(message.ToolCalls) > 0 {
			finish = "tool_calls"
		}
		jsonResponse(w, 200, map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}, "usage": map[string]int64{"prompt_tokens": prompt, "completion_tokens": output, "prompt_cache_hit_tokens": cached, "total_tokens": prompt + output}})
	}))
	defer model.Close()
	snap := basicSnapshot(model.URL)
	snap.Agent.ContextTokens = 32000
	snap.Agent.Model.MaxTokens = 6144
	snap.Agent.MaxSteps = 40
	snap.Agent.MaxModelAttempts = 20
	snap.Agent.ModelTokenBudget = 1000000
	id := submit(t, s, snap)
	if mode == "irreducible" {
		if _, err := db.Exec(ctx, "UPDATE runs SET constraints=$2 WHERE id=$1", id, JSON([]string{strings.Repeat("不可压缩的当前约束<>", 10000)})); err != nil {
			t.Fatal(err)
		}
	}
	killPoint := make(chan struct{})
	var interrupted atomic.Bool
	var archives *archiveHTTPFixture
	archives = newArchiveHTTPFixtureToken(t, id, func(req *http.Request) bool {
		if !strings.HasSuffix(mode, "_kill") || interrupted.Load() {
			return false
		}
		var rejected int
		if err := db.QueryRow(req.Context(), "SELECT count(*) FROM harness_exchanges WHERE run_id=$1 AND response->>'kind'='context_rejected'", id).Scan(&rejected); err != nil {
			t.Error(err)
			return false
		}
		match := rejected > 0 && mode == "rejection_kill" && summaries.Load() == 0
		if mode == "compaction_kill" {
			archives.mu.Lock()
			body := archives.bodies[strings.TrimPrefix(req.URL.Path, "/v1/artifacts/")]
			match = strings.Contains(string(body), `"type":"compaction/end"`)
			archives.mu.Unlock()
		}
		if match && interrupted.CompareAndSwap(false, true) {
			// Previous model/rejection is committed; this next archive is not
			// acknowledged. Kill the real process while its HTTP reply is held.
			close(killPoint)
			<-req.Context().Done()
			return true
		}
		return false
	}, token)
	s.StorageURL = archives.server.URL
	config := map[string]string{"DATABASE_URL": evidenceWorkerDSN(t, db, workerRole), "SERVICE_TOKEN": token, "SERVICE_PEERS": string(JSON(map[string]string{"worker": servicehttp.TokenHash(token)})), "RUNTIME_URL": runtimeServer.URL, "REGISTRY_URL": runtimeServer.URL, "TOOLHUB_URL": hub.URL, "STORAGE_URL": archives.server.URL, "AGENTOS_NODE": node, "DSH_BUDGET_RUNNER": runner, "LISTEN": "127.0.0.1:0", "INSTANCE_ID": "budget-first", "DB_MAX_CONNS": "3"}
	first, done := startEvidenceWorker(t, ctx, binary, config)
	if strings.HasSuffix(mode, "_kill") {
		for !interrupted.Load() {
			r, err := s.Run(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status == "failed" || r.Status == "completed" || r.Status == "waiting_unknown" {
				t.Fatal("interruption not reached", r.Status, r.State.Error)
			}
			select {
			case <-killPoint:
			case <-done:
				t.Fatal("Worker exited")
			case <-ctx.Done():
				t.Fatal("kill point timeout")
			case <-time.After(20 * time.Millisecond):
			}
		}
		if err := first.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Worker not killed")
			}
		case <-ctx.Done():
			t.Fatal("kill timeout")
		}
		if _, err := db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", id); err != nil {
			t.Fatal(err)
		}
		config["INSTANCE_ID"] = "budget-replacement"
		_, done = startEvidenceWorker(t, ctx, binary, config)
	}
	var result Run
	for {
		var err error
		result, err = s.Run(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status == "completed" || result.Status == "failed" || result.Status == "waiting_unknown" {
			break
		}
		select {
		case <-done:
			t.Fatal("Worker exited")
		case <-ctx.Done():
			t.Fatal("run timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}
	var rejected, rejectedWithCalls, duplicates, pending int
	for _, check := range []struct {
		sql   string
		count *int
	}{
		{"SELECT count(*) FROM harness_exchanges WHERE run_id=$1 AND response->>'kind'='context_rejected'", &rejected},
		{"SELECT count(*) FROM harness_exchanges e JOIN calls c ON c.id=e.run_id||'/dsh/'||e.seq WHERE e.run_id=$1 AND e.response->>'kind'='context_rejected'", &rejectedWithCalls},
		{"SELECT count(*) FROM (SELECT a.call_id FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 GROUP BY a.call_id HAVING count(*)>1) x", &duplicates},
		{"SELECT count(*) FROM harness_archives WHERE run_id=$1 AND (manifest_hash='' OR batch IS NOT NULL)", &pending},
	} {
		if err := db.QueryRow(ctx, check.sql, id).Scan(check.count); err != nil {
			t.Fatal(err)
		}
	}
	if rejectedWithCalls != 0 || duplicates != 0 || pending != 0 {
		t.Fatal("effect or archive invariant failed", rejectedWithCalls, duplicates, pending)
	}
	u, err := s.Usage(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var outcomes []byte
	if err = db.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('seq',seq,'purpose',request->>'purpose',
 'outcome',response->>'kind','estimate',response->'estimated_input_tokens','output',request->'max_tokens') ORDER BY seq),'[]')
 FROM harness_exchanges WHERE run_id=$1 AND method='model' AND (request->>'purpose'='compaction' OR response->>'kind'='context_rejected')`, id).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	t.Logf("durable model outcomes: %s", outcomes)
	if u.Attempts != int(generations.Load()+summaries.Load()) || u.UnknownUsageAttempts != 0 || u.PromptTokens != promptTotal.Load() || u.CompletionTokens != outputTotal.Load() || u.CachedPromptTokens != cacheTotal.Load() || u.AccountedTokens != promptTotal.Load()+outputTotal.Load() {
		t.Fatal("provider usage did not match original receipts", u)
	}
	if mode == "irreducible" {
		if result.Status != "failed" || u.Attempts != 0 || rejected < 1 || rejected > 3 {
			t.Fatal("irreducible context did not fail bounded without provider", result.Status, u, rejected)
		}
	} else if mode == "summary_overflow" {
		// The summary request also exceeds the final JSON window. It must
		// terminate without a paid summary or recursive overflow recovery.
		var summaryRejections int
		if err := db.QueryRow(ctx, "SELECT count(*) FROM harness_exchanges WHERE run_id=$1 AND request->>'purpose'='compaction' AND response->>'kind'='context_rejected'", id).Scan(&summaryRejections); err != nil {
			t.Fatal(err)
		}
		if result.Status != "failed" || generations.Load() != 12 || tools.Load() != 12 || rejected != 2 || summaryRejections != 1 || summaries.Load() != 0 {
			t.Fatal("oversized summary did not fail within recovery bounds", result.Status, generations.Load(), summaries.Load(), rejected)
		}
	} else {
		if result.Status != "completed" || result.State.Result != final || generations.Load() != 13 || tools.Load() != 12 {
			t.Fatal("budget process failed or repeated effects", result.Status, result.State.Error, generations.Load(), tools.Load())
		}
		if mode == "usage_control" && (summaries.Load() != 0 || rejected != 0) {
			t.Fatal("control unexpectedly compacted", summaries.Load(), rejected)
		}
		if mode == "usage_pressure" && (summaries.Load() == 0 || rejected != 0) {
			t.Fatal("measured usage did not cause proactive compaction", summaries.Load(), rejected)
		}
		if mode == "serialized_overflow" || strings.HasSuffix(mode, "_kill") {
			if rejected < 1 || summaries.Load() == 0 {
				t.Fatal("final JSON rejection or recovery not exercised", rejected, summaries.Load())
			}
		}
		if strings.HasSuffix(mode, "_kill") && (result.Fence != 2 || result.Owner != "budget-replacement") {
			t.Fatal("new Worker did not take over original task", result.Fence, result.Owner)
		}
	}
	t.Logf("mode=%s status=%s fence=%d provider=%d summaries=%d not-sent=%d tokens=%d; real Worker/Node/PG, synthetic model and Storage", mode, result.Status, result.Fence, u.Attempts, summaries.Load(), rejected, u.AccountedTokens)
}
