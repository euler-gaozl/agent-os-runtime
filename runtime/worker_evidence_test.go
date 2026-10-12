package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIndependentWorkerEvidencePastFourMiBRecallsOmittedSourceAfterProcessInterruption(t *testing.T) {
	binary, runner, node := requireEvidenceWorker(t)
	db, role, workerRole := fixtureDatabase(t)
	if err := Migrate(context.Background(), db, role, workerRole); err != nil {
		t.Fatal(err)
	}
	s := NewStore(rolePool(t, db, role), &serviceclient.Client{Token: testToken}, "")
	workerDSN := evidenceWorkerDSN(t, db, workerRole)
	runtimeServer := httptest.NewServer((&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"toolhub": servicehttp.TokenHash(testToken)}}))
	defer runtimeServer.Close()
	proxy := serviceclient.Client{RuntimeURL: runtimeServer.URL, Token: testToken}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	original, e := s.PutDocument(ctx, Document{Kind: "evidence", Title: "库存修订", Content: "受控验收：初值12吨已撤回，修订实绩17吨；下月20吨仅为计划。", Source: "fixture://OMITTED-SOURCE-492"})
	if e != nil {
		t.Fatal(e)
	}
	var ordinary, summaries, models, recallInvocations atomic.Int32
	var sourceMissing atomic.Bool
	tool := func(name string, args any) Message {
		return Message{Role: "assistant", ToolCalls: []ToolCall{{ID: fmt.Sprintf("fixture-%d", ordinary.Load()), Type: "function", Function: FunctionCall{Name: name, Arguments: string(JSON(args))}}}}
	}
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if json.NewDecoder(req.Body).Decode(&body) != nil {
			w.WriteHeader(400)
			return
		}
		models.Add(1)
		if strings.Contains(body.Messages[len(body.Messages)-1].Content, "Create a concise continuity checkpoint") {
			summaries.Add(1)
			binaryFixtureCompletion(w, Message{Role: "assistant", Content: "继续核对库存修订，原始来源标识和数值未保存在本摘要中。"})
			return
		}
		n := ordinary.Add(1)
		lastTool := func() string {
			for i := len(body.Messages) - 1; i >= 0; i-- {
				if body.Messages[i].Role == "tool" {
					return body.Messages[i].Content
				}
			}
			return ""
		}
		switch {
		case n == 1:
			binaryFixtureCompletion(w, tool("knowledge_search", map[string]string{"query": "库存修订"}))
		case n >= 2 && n <= 8:
			m := tool("knowledge_search", map[string]string{"query": "filler-not-present"})
			m.Content = strings.Repeat("受控长记录abcdefghij；", 32000)
			binaryFixtureCompletion(w, m)
		case n == 9:
			text := string(JSON(body.Messages))
			sourceMissing.Store(!strings.Contains(text, original.ID) && !strings.Contains(text, "OMITTED-SOURCE-492"))
			binaryFixtureCompletion(w, tool("run_recall", map[string]string{"query": "库存修订"}))
		case n == 10:
			var list struct {
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}
			if json.Unmarshal([]byte(lastTool()), &list) != nil || len(list.Items) != 1 {
				t.Error("source receipt not rediscovered", lastTool())
				w.WriteHeader(500)
				return
			}
			binaryFixtureCompletion(w, tool("run_recall", map[string]string{"operation_id": list.Items[0].ID}))
		case n == 11:
			var page struct {
				Content string `json:"content"`
			}
			var docs []Document
			if json.Unmarshal([]byte(lastTool()), &page) != nil || json.Unmarshal([]byte(page.Content), &docs) != nil || len(docs) != 1 || docs[0].ID != original.ID {
				t.Error("lost document ID not recovered")
				w.WriteHeader(500)
				return
			}
			binaryFixtureCompletion(w, tool("document_read", map[string]string{"id": docs[0].ID}))
		case n == 12:
			if !strings.Contains(lastTool(), "修订实绩17吨") || !strings.Contains(lastTool(), original.Source) {
				t.Error("original source not read after recovery")
				w.WriteHeader(500)
				return
			}
			binaryFixtureCompletion(w, Message{Role: "assistant", Content: "受控验收：修订实绩17吨，下月20吨是计划；原文已重新定位。"})
		default:
			t.Error("unexpected provider request", n)
			w.WriteHeader(500)
		}
	}))
	defer model.Close()
	killed := make(chan struct{})
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var inv Invocation
		if json.NewDecoder(req.Body).Decode(&inv) != nil {
			w.WriteHeader(400)
			return
		}
		b, err := proxy.Invoke(req.Context(), inv)
		if err != nil {
			t.Error("fixture tool failed", err)
			w.WriteHeader(500)
			return
		}
		if inv.Name == "run_recall" && recallInvocations.Add(1) == 1 {
			// Block after the actual Runtime receipt commits. The test kills the
			// independent Worker executable before this tool reply is returned.
			close(killed)
			<-req.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}))
	defer hub.Close()
	snap := basicSnapshot(model.URL)
	snap.Agent.Tools = append(snap.Agent.Tools, "run_recall", "artifact_read", "document_read")
	snap.Agent.ContextTokens = 8192
	snap.Agent.MaxSteps = 40
	id := submit(t, s, snap)
	archives := newArchiveHTTPFixture(t, id, nil)
	s.StorageURL = archives.server.URL
	config := map[string]string{"DATABASE_URL": workerDSN, "SERVICE_TOKEN": testToken, "SERVICE_PEERS": string(JSON(map[string]string{"worker": servicehttp.TokenHash(testToken)})), "RUNTIME_URL": runtimeServer.URL, "REGISTRY_URL": runtimeServer.URL, "TOOLHUB_URL": hub.URL, "STORAGE_URL": archives.server.URL, "AGENTOS_NODE": node, "DSH_RUNNER": runner, "LISTEN": "127.0.0.1:0", "INSTANCE_ID": "first-attempt", "DB_MAX_CONNS": "3"}
	first, firstDone := startEvidenceWorker(t, ctx, binary, config)
	select {
	case <-killed:
	case <-firstDone:
		t.Fatal("first Worker exited before interruption point")
	case <-ctx.Done():
		failed, _ := s.Run(ctx, id)
		t.Fatal("interruption point not reached", failed.Status, failed.State.Error)
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal("actual Worker kill failed", err)
	}
	select {
	case err := <-firstDone:
		if err == nil {
			t.Fatal("Worker did not exit abnormally")
		}
	case <-ctx.Done():
		t.Fatal("Worker remained alive")
	}
	before := models.Load()
	beforeOrdinary, beforeSummaries := ordinary.Load(), summaries.Load()
	if !sourceMissing.Load() || summaries.Load() == 0 {
		t.Fatal("test did not actually lose source ID through DSH compaction")
	}
	if _, e = s.DB.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", id); e != nil {
		t.Fatal(e)
	}
	config["INSTANCE_ID"] = "replacement-attempt"
	_, replacementDone := startEvidenceWorker(t, ctx, binary, config)
	var done Run
	for {
		done, e = s.Run(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		if done.Status == "completed" || done.Status == "failed" || done.Status == "waiting_unknown" {
			break
		}
		select {
		case <-replacementDone:
			t.Fatal("replacement Worker exited")
		case <-ctx.Done():
			t.Fatal("replacement did not finish")
		case <-time.After(30 * time.Millisecond):
		}
	}
	if done.Fence != 2 || done.Owner != "replacement-attempt" {
		t.Fatal("actual replacement fence/owner not established")
	}
	if e != nil || done.Status != "completed" || !strings.Contains(done.State.Result, "修订实绩17吨") {
		t.Fatal("recovery failed", done.Status, done.State.Error, e)
	}
	if ordinary.Load() != beforeOrdinary+3 || models.Load() != before+3+summaries.Load()-beforeSummaries || recallInvocations.Load() != 2 {
		t.Fatal("replay repeated provider or committed recall", before, models.Load(), recallInvocations.Load())
	}
	var duplicateAttempts int
	if e = s.DB.QueryRow(ctx, `SELECT count(*) FROM (SELECT a.call_id FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 GROUP BY a.call_id HAVING count(*)>1) repeated`, id).Scan(&duplicateAttempts); e != nil || duplicateAttempts != 0 {
		t.Fatal("cached model invocation was charged again", duplicateAttempts, e)
	}
	rows, e := s.DB.Query(ctx, "SELECT manifest_hash,batch IS NULL FROM harness_archives WHERE run_id=$1 ORDER BY seq", id)
	if e != nil {
		t.Fatal(e)
	}
	hashes := []string{}
	for rows.Next() {
		var hash string
		var cleared bool
		if e = rows.Scan(&hash, &cleared); e != nil || !cleared {
			rows.Close()
			t.Fatal("staging not retired", e)
		}
		hashes = append(hashes, hash)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		t.Fatal(e)
	}
	bytes, end := 0, 0
	previous := ""
	originalFound, compactFound := false, false
	for _, hash := range hashes {
		raw, a := archives.read(t, hash)
		if a.EventStart != end || a.Previous != previous {
			t.Fatal("archive chain discontinuity")
		}
		bytes += len(raw)
		end = a.EventEnd
		previous = hash
		originalFound = originalFound || strings.Contains(string(raw), original.Source)
		compactFound = compactFound || strings.Contains(string(raw), "compaction/end")
	}
	if bytes <= 4<<20 || !originalFound || !compactFound || previous != done.State.HarnessArtifact {
		t.Fatal("large history retention not proven", bytes, originalFound, compactFound)
	}
	var stored int64
	archives.mu.Lock()
	for _, body := range archives.bodies {
		stored += int64(len(body))
	}
	archives.mu.Unlock()
	if stored > int64(bytes)+int64(len(hashes))*4096 {
		t.Fatal("retention still duplicates growing prefixes", stored, bytes)
	}
	t.Logf("Retained %d event bytes in %d batches; %d compact calls; source omitted, real DSH process interrupted, source rediscovered and original reread; stored artifact bytes %d", bytes, len(hashes), summaries.Load(), stored)
}
