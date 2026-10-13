package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// Real Worker.exe + source-built DSH, real restricted PostgreSQL roles and
// Runtime HTTP. Provider, ToolHub routing and immutable Storage are fixtures.
func TestIndependentSourceWorkerRecoversCommittedStateAfterCompactionAndLostReply(t *testing.T) {
	testIndependentSourceWorker(t, true, false)
}

func TestIndependentSourceWorkerSurvivesLostStateHTTPReplyWithoutProcessDeath(t *testing.T) {
	testIndependentSourceWorker(t, false, false)
}

func TestIndependentSourceWorkerExhaustedReplyRetriesRequireSafeOperatorRecovery(t *testing.T) {
	testIndependentSourceWorker(t, false, true)
}

func testIndependentSourceWorker(t *testing.T, kill, exhausted bool) {
	binary, _, node := requireEvidenceWorker(t)
	runner := os.Getenv("TEST_SOURCE_DSH_RUNNER")
	if runner == "" {
		t.Skip("source artifact not configured")
	}
	db, role, workerRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := Migrate(ctx, db, role, workerRole); err != nil {
		t.Fatal(err)
	}
	s := NewStore(rolePool(t, db, role), &serviceclient.Client{Token: testToken}, "")
	workerToken := testToken + "source-worker"
	api := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"toolhub": servicehttp.TokenHash(testToken), "worker": servicehttp.TokenHash(workerToken), "control": servicehttp.TokenHash(testToken + "control")}})
	killPoint := make(chan struct{})
	var updateHTTP atomic.Int32
	runtimeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/source-state" {
			api.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(400)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var inv Invocation
		if json.Unmarshal(body, &inv) != nil {
			w.WriteHeader(400)
			return
		}
		if inv.Name != "working_state_update" {
			api.ServeHTTP(w, r)
			return
		}
		n := updateHTTP.Add(1)
		retained := httptest.NewRecorder()
		api.ServeHTTP(retained, r)
		if retained.Code != 200 || strings.Contains(retained.Body.String(), `"rejected":true`) {
			t.Error("actual state update failed", retained.Code, retained.Body.String())
		}
		if n == 2 || (exhausted && n >= 3 && n <= 4) {
			// State revision and exchange.response are already committed. No
			// response bytes have reached the real Worker at this kill point.
			if n == 2 {
				close(killPoint)
			}
			if kill {
				<-r.Context().Done()
			} else {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
				} else {
					_ = conn.Close()
				}
			}
			return
		}
		for key, values := range retained.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(retained.Code)
		_, _ = w.Write(retained.Body.Bytes())
	}))
	defer runtimeServer.Close()
	proxy := serviceclient.Client{RuntimeURL: runtimeServer.URL, Token: testToken}
	var toolHTTP, ordinary, summaries, models atomic.Int32
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var inv Invocation
		if r.Header.Get("Authorization") != "Bearer "+workerToken || json.NewDecoder(r.Body).Decode(&inv) != nil {
			w.WriteHeader(400)
			return
		}
		toolHTTP.Add(1)
		reply, err := proxy.Invoke(r.Context(), inv)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write(reply)
	}))
	defer hub.Close()
	var sourceReceipt string
	var afterCompact atomic.Bool
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages  []Message `json:"messages"`
			MaxTokens int       `json:"max_tokens"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Messages) == 0 {
			w.WriteHeader(400)
			return
		}
		models.Add(1)
		if body.MaxTokens != 512 {
			t.Error("operator output cap not applied to final request", body.MaxTokens)
		}
		if strings.Contains(body.Messages[len(body.Messages)-1].Content, "Create a concise continuity checkpoint") {
			summaries.Add(1)
			binaryFixtureCompletion(w, Message{Role: "assistant", Content: "Synthetic summary deliberately omits the source, date, receipt and quantity."})
			return
		}
		n := ordinary.Add(1)
		var projected struct {
			View WorkingStateView `json:"view"`
		}
		for _, m := range body.Messages {
			if m.Role != "user" {
				continue
			}
			if start := strings.Index(m.Content, "BEGIN_WORKING_STATE_DATA\n"); start >= 0 {
				data := strings.SplitN(m.Content[start+len("BEGIN_WORKING_STATE_DATA\n"):], "\nEND_WORKING_STATE_DATA", 2)[0]
				if err := json.Unmarshal([]byte(data), &projected); err != nil {
					t.Error(err)
				}
			}
		}
		if projected.View.RunID == "" {
			t.Error("authoritative working read missing")
		}
		if n >= 3 {
			v := projected.View
			if v.State == nil || len(v.State.Observations) != 1 || v.State.Observations[0].Text != "SOURCE-417: actual 17 units on 2026-10-01; planned 20 units are separate." || len(v.State.Observations[0].Receipts) != 1 || v.State.Observations[0].Receipts[0] != sourceReceipt {
				t.Error("source state lost before final provider request")
			}
			if summaries.Load() > 0 {
				afterCompact.Store(true)
			}
		}
		call := func(name string, args any) Message {
			return Message{Role: "assistant", ToolCalls: []ToolCall{{ID: fmt.Sprintf("source-call-%d", n), Type: "function", Function: FunctionCall{Name: name, Arguments: string(JSON(args))}}}}
		}
		switch {
		case n == 1:
			binaryFixtureCompletion(w, call("note_save", map[string]string{"kind": "fact", "title": "source fixture", "content": "SOURCE-417 actual 17 units on 2026-10-01", "source": "fixture://SOURCE-417"}))
		case n == 2 || n == 10:
			if n == 2 {
				for i := len(body.Messages) - 1; i >= 0; i-- {
					if body.Messages[i].Role == "tool" {
						var receipt struct {
							Receipt struct {
								OperationID string `json:"operation_id"`
								Committed   bool   `json:"committed"`
							} `json:"receipt"`
						}
						if json.Unmarshal([]byte(body.Messages[i].Content), &receipt) != nil || !receipt.Receipt.Committed {
							t.Error("source tool receipt missing")
						}
						sourceReceipt = receipt.Receipt.OperationID
						break
					}
				}
			}
			state := WorkingSet{Format: 1, Phase: "compare actual stock and planned supply", Questions: []WorkingQuestion{}, Observations: []WorkingObservation{{ID: "actual", Kind: "observation", Text: "SOURCE-417: actual 17 units on 2026-10-01; planned 20 units are separate.", Receipts: []string{sourceReceipt}}}, Conflicts: []WorkingConflict{}, NextSteps: []WorkingStep{}}
			binaryFixtureCompletion(w, call("working_state_update", WorkingStateChange{ExpectedRevision: projected.View.Revision, Reason: "Preserve dated evidence across compaction", State: state}))
		case n >= 3 && n <= 9:
			m := call("note_save", map[string]string{"kind": "fact", "title": "controlled filler", "content": "synthetic fixture only", "source": "fixture://filler"})
			m.Content = strings.Repeat("synthetic history ", 1100)
			binaryFixtureCompletion(w, m)
		case n == 11:
			if projected.View.Revision != 2 {
				t.Error("committed state did not survive replacement", projected.View.Revision)
			}
			binaryFixtureCompletion(w, Message{Role: "assistant", Content: "SOURCE-417 actual 17 units on 2026-10-01; plan 20 units remains separate."})
		default:
			t.Error("provider operation repeated", n)
			w.WriteHeader(500)
		}
	}))
	defer model.Close()
	snap := basicSnapshot(model.URL)
	snap.Agent.ContextTokens = 32768
	snap.Agent.MaxSteps = 50
	snap.Agent.Model.MaxTokens = 512
	// Select only new roots through the same deployment option as the API.
	if err := s.ConfigureSubmissionEngine(EngineDSHSource); err != nil {
		t.Fatal(err)
	}
	id := submit(t, s, snap)
	archives := newArchiveHTTPFixtureToken(t, id, nil, workerToken)
	s.StorageURL = archives.server.URL
	config := map[string]string{"DATABASE_URL": evidenceWorkerDSN(t, db, workerRole), "SERVICE_TOKEN": workerToken, "SERVICE_PEERS": string(JSON(map[string]string{"worker": servicehttp.TokenHash(workerToken)})), "RUNTIME_URL": runtimeServer.URL, "REGISTRY_URL": runtimeServer.URL, "TOOLHUB_URL": hub.URL, "STORAGE_URL": archives.server.URL, "AGENTOS_NODE": node, "DSH_SOURCE_RUNNER": runner, "LISTEN": "127.0.0.1:0", "INSTANCE_ID": "source-first-attempt", "DB_MAX_CONNS": "3"}
	first, firstDone := startEvidenceWorker(t, ctx, binary, config)
	select {
	case <-killPoint:
	case <-firstDone:
		t.Fatal("first source Worker exited")
	case <-ctx.Done():
		r, _ := s.Run(context.Background(), id)
		t.Fatal("state commit interruption point not reached", r.Status, r.State.Error)
	}
	if summaries.Load() == 0 || !afterCompact.Load() {
		t.Fatal("test did not exercise real compaction before kill")
	}
	beforeModels, beforeTools := models.Load(), toolHTTP.Load()
	secondDone := firstDone
	if kill {
		if err := first.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-firstDone:
			if err == nil {
				t.Fatal("Worker was not interrupted")
			}
		case <-ctx.Done():
			t.Fatal("Worker kill timeout")
		}
		if _, err := db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", id); err != nil {
			t.Fatal(err)
		}
		config["INSTANCE_ID"] = "source-replacement-attempt"
		_, secondDone = startEvidenceWorker(t, ctx, binary, config)
	}
	if exhausted {
		for {
			r, err := s.Run(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status == "waiting_unknown" {
				break
			}
			if r.Status == "failed" || r.Status == "completed" {
				t.Fatal("uncertain source state not held for recovery", r.Status)
			}
			select {
			case <-ctx.Done():
				t.Fatal("retry exhaustion not reached")
			case <-time.After(20 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, "POST", runtimeServer.URL+"/v1/runs/"+id+"/resume-source-state", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+testToken+"control")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 202 {
			t.Fatal("safe source recovery denied", res.StatusCode, string(body))
		}
	}
	var done Run
	for {
		var err error
		done, err = s.Run(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if done.Status == "completed" || done.Status == "failed" || done.Status == "waiting_unknown" {
			break
		}
		select {
		case <-secondDone:
			t.Fatal("replacement exited")
		case <-ctx.Done():
			t.Fatal("replacement timed out")
		case <-time.After(25 * time.Millisecond):
		}
	}
	wantFence, wantUpdates := int64(1), int32(3)
	if kill {
		wantFence = 2
		wantUpdates = 2
	}
	if exhausted {
		wantFence = 2
		wantUpdates = 4
	}
	if done.Status != "completed" || done.Fence != wantFence || done.Owner != config["INSTANCE_ID"] || !strings.Contains(done.State.Result, "actual 17 units") {
		t.Fatal("source recovery failed", done.Status, done.Fence, done.State.Error)
	}
	if (kill && models.Load() != beforeModels+1) || ordinary.Load() != 11 || toolHTTP.Load() != beforeTools || updateHTTP.Load() != wantUpdates {
		t.Fatal("replay repeated external calls", models.Load(), beforeModels, ordinary.Load(), toolHTTP.Load(), beforeTools, updateHTTP.Load())
	}
	var states, duplicates, unacknowledged int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM run_working_states WHERE run_id=$1", id).Scan(&states); err != nil || states != 2 {
		t.Fatal("duplicated state revision", states, err)
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM (SELECT a.call_id FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 GROUP BY a.call_id HAVING count(*)>1) x`, id).Scan(&duplicates); err != nil || duplicates != 0 {
		t.Fatal("duplicated provider attempt", duplicates, err)
	}
	if err := db.QueryRow(ctx, "SELECT count(*) FROM harness_archives WHERE run_id=$1 AND (manifest_hash='' OR batch IS NOT NULL)", id).Scan(&unacknowledged); err != nil || unacknowledged != 0 {
		t.Fatal("unconfirmed event archive", unacknowledged, err)
	}
	rows, err := db.Query(ctx, "SELECT manifest_hash FROM harness_archives WHERE run_id=$1 ORDER BY seq", id)
	if err != nil {
		t.Fatal(err)
	}
	var hashes []string
	for rows.Next() {
		var hash string
		if err = rows.Scan(&hash); err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, hash)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	previous := ""
	end := 0
	compacted := false
	total := 0
	for _, hash := range hashes {
		raw, a := archives.read(t, hash)
		if a.Previous != previous || a.EventStart != end {
			t.Fatal("source archive chain discontinuity")
		}
		previous = hash
		end = a.EventEnd
		total += len(raw)
		compacted = compacted || strings.Contains(string(raw), "compaction/end")
	}
	if !compacted || previous != done.State.HarnessArtifact {
		t.Fatal("compaction checkpoint missing")
	}
	t.Logf("Actual source Worker state revision 2 committed before lost HTTP reply; process killed=%t, retries exhausted=%t, fence=%d; no repeated model/tool/state effects; %d summaries, %d archives, %d event bytes", kill, exhausted, done.Fence, summaries.Load(), len(hashes), total)
}
