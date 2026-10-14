package runtime

import (
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

	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// Real packaged Worker.exe, Node, Runtime HTTP and restricted PostgreSQL.
// Provider is a TLS fixture trusted only by this child process's SSL_CERT_FILE
// (Go 1.27); no system trust-store changes or production TLS bypass.
func TestIndependentNativeWorkerDurableProviderAndLongText(t *testing.T) {
	for _, mode := range []string{"large", "completion_kill", "rejection_kill", "compaction_kill", "cancel", "partial"} {
		t.Run(mode, func(t *testing.T) { testNativeProcess(t, mode) })
	}
}

func testNativeProcess(t *testing.T, mode string) {
	binary, _, node := requireEvidenceWorker(t)
	runner := os.Getenv("TEST_NATIVE_DSH_RUNNER")
	if runner == "" {
		t.Fatal("native packaged runner required")
	}
	db, role, workerRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if e := Migrate(ctx, db, role, workerRole); e != nil {
		t.Fatal(e)
	}
	token := testToken + "native-worker"
	s := NewStore(rolePool(t, db, role), &serviceclient.Client{Token: token}, "")
	if e := s.ConfigureSubmissionEngine(EngineDSHNative); e != nil {
		t.Fatal(e)
	}
	api := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"worker": servicehttp.TokenHash(token), "toolhub": servicehttp.TokenHash(testToken)}})
	runtimeServer := httptest.NewServer(api)
	defer runtimeServer.Close()
	proxy := serviceclient.Client{RuntimeURL: runtimeServer.URL, Token: testToken}
	var generations, summaries, tools atomic.Int64
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var inv Invocation
		if r.Header.Get("Authorization") != "Bearer "+token || json.NewDecoder(r.Body).Decode(&inv) != nil {
			w.WriteHeader(400)
			return
		}
		tools.Add(1)
		raw, e := proxy.Invoke(r.Context(), inv)
		if e != nil {
			t.Error(e)
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write(raw)
	}))
	defer hub.Close()
	final := "2026-10-01实际库存17单位，计划20单位另列。"
	if mode == "large" || mode == "completion_kill" {
		final += strings.Repeat("事实。", 1000000)
	}
	entered := make(chan struct{}, 1)
	model := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
			Max      int       `json:"max_tokens"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Messages) == 0 || body.Max != 393216 {
			t.Error("provider input or frozen native cap changed")
			w.WriteHeader(400)
			return
		}
		m := Message{Role: "assistant"}
		if strings.Contains(body.Messages[len(body.Messages)-1].Content, "Create a concise continuity checkpoint") {
			summaries.Add(1)
			m.Content = "Synthetic checkpoint: actual17, plan20, evidence retained in Runtime."
		} else {
			n := generations.Add(1)
			if mode == "cancel" {
				entered <- struct{}{}
				<-r.Context().Done()
				return
			}
			if mode == "partial" {
				_, _ = w.Write([]byte(`{"choices":[`))
				return
			}
			if mode == "rejection_kill" || mode == "compaction_kill" {
				if n <= 6 {
					m.Content = strings.Repeat("synthetic history ", 180)
					m.ToolCalls = []ToolCall{{ID: fmt.Sprintf("native-note-%d", n), Type: "function", Function: FunctionCall{Name: "note_save", Arguments: string(JSON(map[string]string{"kind": "fact", "title": "fixture", "content": "actual17;plan20", "source": "fixture://native"}))}}}
				}
				if n == 7 {
					w.WriteHeader(400)
					_, _ = w.Write([]byte(`{"error":{"code":"context_length_exceeded","message":"synthetic provider window refusal"}}`))
					return
				}
				if n >= 8 {
					m.Content = final
				}
			} else {
				m.Content = final
			}
		}
		finish := "stop"
		if len(m.ToolCalls) > 0 {
			finish = "tool_calls"
		}
		jsonResponse(w, 200, map[string]any{"choices": []any{map[string]any{"message": m, "finish_reason": finish}}, "usage": map[string]int{"prompt_tokens": 1800, "completion_tokens": 100, "prompt_cache_hit_tokens": 900, "total_tokens": 1900}})
	}))
	defer model.Close()
	caFile := filepath.Join(t.TempDir(), "fixture-ca.pem")
	if e := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: model.Certificate().Raw}), 0600); e != nil {
		t.Fatal(e)
	}
	snap := nativeSnapshot()
	snap.Agent.Model.BaseURL = model.URL
	snap.Agent.Model.Limits.BaseURL = model.URL
	snap.Agent.Model.Limits.Source = model.URL + "/models"
	snap.Agent.MaxSteps = 40
	snap.Agent.MaxModelAttempts = 20
	task := "Synthetic supplied facts only: actual17 plan20"
	if mode == "large" || mode == "completion_kill" {
		task += strings.Repeat("资料。", 500000)
	}
	id, e := s.Submit(ctx, ID(), task, snap, nil)
	if e != nil {
		t.Fatal(e)
	}
	var interrupted atomic.Bool
	killPoint := make(chan struct{})
	var archives *archiveHTTPFixture
	archives = newArchiveHTTPFixtureToken(t, id, func(req *http.Request) bool {
		if !strings.HasSuffix(mode, "_kill") || interrupted.Load() {
			return false
		}
		match := false
		if mode == "completion_kill" || mode == "rejection_kill" {
			kind := "completion"
			if mode == "rejection_kill" {
				kind = "provider_context_rejected"
			}
			var n int
			if err := db.QueryRow(req.Context(), "SELECT count(*) FROM harness_exchanges WHERE run_id=$1 AND response->>'kind'=$2", id, kind).Scan(&n); err != nil {
				t.Error(err)
				return false
			}
			match = n > 0
		} else {
			archives.mu.Lock()
			body := archives.bodies[strings.TrimPrefix(req.URL.Path, "/v1/artifacts/")]
			match = strings.Contains(string(body), `"type":"compaction/end"`)
			archives.mu.Unlock()
		}
		if match && interrupted.CompareAndSwap(false, true) {
			close(killPoint)
			<-req.Context().Done()
			return true
		}
		return false
	}, token)
	s.StorageURL = archives.server.URL
	config := map[string]string{"DATABASE_URL": evidenceWorkerDSN(t, db, workerRole), "SERVICE_TOKEN": token, "SERVICE_PEERS": string(JSON(map[string]string{"worker": servicehttp.TokenHash(token)})), "RUNTIME_URL": runtimeServer.URL, "REGISTRY_URL": runtimeServer.URL, "TOOLHUB_URL": hub.URL, "STORAGE_URL": archives.server.URL, "AGENTOS_NODE": node, "DSH_NATIVE_RUNNER": runner, "SSL_CERT_FILE": caFile, "GODEBUG": "x509sslcertoverrideplatform=1", "LISTEN": "127.0.0.1:0", "INSTANCE_ID": "native-first", "DB_MAX_CONNS": "3"}
	first, done := startEvidenceWorker(t, ctx, binary, config)
	if strings.HasSuffix(mode, "_kill") || mode == "cancel" {
		point := killPoint
		if mode == "cancel" {
			point = entered
		}
		for {
			select {
			case <-point:
				goto reached
			case <-done:
				t.Fatal("Worker exited before fault")
			case <-ctx.Done():
				t.Fatal("fault not reached")
			case <-time.After(25 * time.Millisecond):
			}
			r, err := s.Run(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status == "failed" || r.Status == "completed" || r.Status == "waiting_unknown" {
				t.Fatal("fault not reached", r.Status, r.State.Error)
			}
		}
	reached:
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
			config["INSTANCE_ID"] = "native-replacement"
			_, done = startEvidenceWorker(t, ctx, binary, config)
		}
	}
	var result Run
	for {
		result, e = s.Run(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		if result.Status == "completed" || result.Status == "failed" || result.Status == "waiting_unknown" || result.Status == "cancelled" {
			break
		}
		select {
		case <-done:
			t.Fatal("Worker exited")
		case <-ctx.Done():
			t.Fatal("run timeout")
		case <-time.After(25 * time.Millisecond):
		}
	}
	u, e := s.Usage(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if mode == "cancel" || mode == "partial" {
		want := "failed"
		if mode == "cancel" {
			want = "cancelled"
		}
		if result.Status != want || u.Attempts != 1 || u.UnknownUsageAttempts != 1 || summaries.Load() != 0 {
			t.Fatal("interrupted request lost uncertainty", result.Status, result.State.Error, u)
		}
		return
	}
	if result.Status != "completed" || result.State.Result != final {
		t.Fatal("native task failed or truncated", result.Status, result.State.Error)
	}
	var duplicated, pending, rejected int
	for _, check := range []struct {
		sql   string
		count *int
	}{
		{"SELECT count(*) FROM (SELECT c.id FROM calls c JOIN model_attempts a ON c.id=a.call_id WHERE c.run_id=$1 GROUP BY c.id HAVING count(*)>1) x", &duplicated},
		{"SELECT count(*) FROM harness_archives WHERE run_id=$1 AND (manifest_hash='' OR batch IS NOT NULL)", &pending},
		{"SELECT count(*) FROM harness_exchanges WHERE run_id=$1 AND response->>'kind'='provider_context_rejected' AND response->>'not_sent'='false'", &rejected},
	} {
		if e = db.QueryRow(ctx, check.sql, id).Scan(check.count); e != nil {
			t.Fatal(e)
		}
	}
	if duplicated != 0 || pending != 0 || u.Attempts != int(generations.Load()+summaries.Load()) || u.TokenBudget != 0 {
		t.Fatal("receipt/archive invariant", duplicated, pending, u)
	}
	if mode == "rejection_kill" || mode == "compaction_kill" {
		if rejected != 1 || u.UnknownUsageAttempts != 1 || generations.Load() != 8 || summaries.Load() == 0 || tools.Load() != 6 {
			t.Fatal("refusal/summary replay drifted", rejected, u, generations.Load(), summaries.Load(), tools.Load())
		}
	} else if generations.Load() != 1 || summaries.Load() != 0 || u.UnknownUsageAttempts != 0 {
		t.Fatal("completion replay repeated", u)
	}
	if strings.HasSuffix(mode, "_kill") && (result.Fence != 2 || result.Owner != "native-replacement") {
		t.Fatal("wrong takeover", result.Fence, result.Owner)
	}
	// Verify actual archived bytes, including batches larger than the historical
	// response cap, without silently treating the fixture as durable Storage.
	archives.mu.Lock()
	defer archives.mu.Unlock()
	largest := 0
	count := 0
	for hash, raw := range archives.bodies {
		var manifest runstate.HarnessArchive
		if json.Unmarshal(raw, &manifest) != nil || manifest.Version != 1 || manifest.RunID != id {
			continue
		}
		if Hash(raw) != hash || len(manifest.Parts) == 0 || len(manifest.Parts) > 66 {
			t.Fatal("invalid native archive manifest")
		}
		var all []byte
		for _, part := range manifest.Parts {
			chunk := archives.bodies[part.Hash]
			if len(chunk) != part.Bytes || Hash(chunk) != part.Hash || part.Offset != len(all) {
				t.Fatal("native archive part changed")
			}
			all = append(all, chunk...)
		}
		if len(all) != manifest.Bytes || Hash(all) != manifest.SHA256 || !json.Valid(all) {
			t.Fatal("native archive bytes changed")
		}
		if manifest.Bytes > largest {
			largest = manifest.Bytes
		}
		count++
	}
	if (mode == "large" || mode == "completion_kill") && largest <= 16<<20 {
		t.Fatal("large archive was not exercised", largest)
	}
	t.Logf("mode=%s fence=%d attempts=%d summaries=%d rejected=%d archives=%d largest_batch=%d; real packaged Worker/Node/Runtime/PG, TLS provider and Storage fixtures", mode, result.Fence, u.Attempts, summaries.Load(), rejected, count, largest)
}
