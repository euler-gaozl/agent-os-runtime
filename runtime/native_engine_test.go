package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

type nativeEffects struct {
	sourceEffects
	complete func(context.Context, Run, string, []Message, []Tool) (json.RawMessage, error)
}

func (e nativeEffects) CompleteNativeModel(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
	return e.complete(c, r, id, m, ts)
}
func nativeSnapshot() Snapshot {
	s := basicSnapshot("https://fixture.invalid")
	s.Agent.Model.Model = "native-fixture"
	s.Agent.Model.MaxTokens = 393216
	s.Agent.ContextTokens = 1048576
	s.Agent.MaxSteps = 30
	s.Agent.MaxModelAttempts = 12
	s.Agent.ModelTokenBudget = 0
	s.Agent.Model.Limits = &contracts.ModelLimits{Model: s.Agent.Model.Model, BaseURL: s.Agent.Model.BaseURL,
		Protocol: "openai-chat-completions", ContextWindow: s.Agent.ContextTokens, MaxOutputTokens: s.Agent.Model.MaxTokens,
		Source: "https://fixture.invalid/models", ObservedAt: "2026-10-14T00:00:00Z", MetadataSHA256: strings.Repeat("a", 64)}
	return s
}
func nativeSetup(t *testing.T) *sourceFixture {
	t.Helper()
	f := sourceSetupVersion(t, EngineDSHNative)
	f.r.Snapshot = nativeSnapshot()
	f.r.State.PlanRevision = 0
	if _, e := f.db.Exec(context.Background(), "UPDATE runs SET snapshot=$2,state=$3 WHERE id=$1", f.r.ID, JSON(f.r.Snapshot), JSON(f.r.State)); e != nil {
		t.Fatal(e)
	}
	return f
}
func nativeFrame(seq int, purpose, content string) execution.Frame {
	return stateFrame(seq, "model", JSON(map[string]any{"messages": []Message{{Role: "user", Content: content}}, "max_tokens": 393216, "purpose": purpose}))
}
func nativeProviderResult(content string) []byte {
	return JSON(map[string]any{"choices": []any{map[string]any{"message": Message{Role: "assistant", Content: content}, "finish_reason": "stop"}},
		"usage": map[string]int{"prompt_tokens": 200, "completion_tokens": 50, "prompt_cache_hit_tokens": 120, "total_tokens": 250}})
}
func nativeRecord(t *testing.T, f *sourceFixture, status int, body []byte, calls *int) nativeEffects {
	t.Helper()
	return nativeEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
		request := execution.NativeModelRequest(r.Snapshot.Agent.Model, m, ts)
		attempt, cached, e := f.engine.BeginModel(c, r, id, request)
		if e != nil {
			return nil, e
		}
		if cached == nil {
			*calls++
			receipt, err := execution.NativeReceipt(status, body)
			if err != nil {
				return nil, err
			}
			e = f.engine.EndModel(c, r, id, attempt, receipt, nil)
		}
		return request, e
	}}
}

func TestNativeCommitFailureTakeoverReusesCompletedProviderOutcome(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion", true: "provider_context_rejection"}[reject], func(t *testing.T) {
			f := nativeSetup(t)
			ctx := context.Background()
			calls := 0
			status, body := 200, nativeProviderResult("fixture")
			if reject {
				status = 400
				body = []byte(`{"error":{"code":"context_length_exceeded","message":"context rejected"}}`)
			}
			effects := nativeRecord(t, f, status, body, &calls)
			frame := nativeFrame(1, "generation", "供给和库存")
			installBudgetReplyFault(t, f)
			if _, _, e := f.engine.Exchange(ctx, &f.r, frame, effects); e == nil || !strings.Contains(e.Error(), "controlled budget response failure") {
				t.Fatal("reply fault missing", e)
			}
			if modelExchangeResponse(t, f) != nil {
				t.Fatal("reply committed despite trigger")
			}
			if _, e := f.db.Exec(ctx, "DROP TRIGGER reject_budget_reply ON harness_exchanges"); e != nil {
				t.Fatal(e)
			}
			next := budgetTakeover(t, f)
			if _, _, e := f.engine.Exchange(ctx, &f.r, frame, effects); !errors.Is(e, ErrFence) {
				t.Fatal("old owner admitted", e)
			}
			v, _, e := f.engine.Exchange(ctx, &next, frame, effects)
			if e != nil || calls != 1 {
				t.Fatal("receipt recovery repeated provider", e, calls)
			}
			u, e := f.s.Usage(ctx, f.r.ID)
			if e != nil || u.Attempts != 1 || u.TokenBudget != 0 {
				t.Fatal("native accounting", u, e)
			}
			text := string(JSON(v))
			if !strings.Contains(text, `"format":2`) {
				t.Fatal("wrong protocol", text)
			}
			if reject {
				if next.State.PlanRevision != 0 || u.UnknownUsageAttempts != 1 || !strings.Contains(text, `"not_sent":false`) || !strings.Contains(text, `"kind":"provider_context_rejected"`) {
					t.Fatal("rejection lost receipt semantics", text, u)
				}
			} else if next.State.PlanRevision != next.RuleRevision || u.AccountedTokens != 250 || u.CachedPromptTokens != 120 {
				t.Fatal("completion or usage lost", u)
			}
			frozen := modelExchangeResponse(t, f)
			next.Step = 0
			v, _, e = f.engine.Exchange(ctx, &next, frame, effects)
			if e != nil || calls != 1 || canonicalHash(frozen) != canonicalHash(JSON(v)) {
				t.Fatal("frozen replay changed", e, calls)
			}
		})
	}
}

func TestNativeIntentRejectsTamperingAndUnprovenOutcomes(t *testing.T) {
	for _, mode := range []string{"cap", "purpose", "snapshot", "request", "missing_receipt", "partial", "generic_400", "auth", "truncated", "archive"} {
		t.Run(mode, func(t *testing.T) {
			f := nativeSetup(t)
			ctx := context.Background()
			calls := 0
			frame := nativeFrame(1, "compaction", "fixture")
			if mode == "cap" {
				frame.Params = []byte(strings.Replace(string(frame.Params), "393216", "2048", 1))
			}
			if mode == "purpose" {
				frame.Params = []byte(strings.Replace(string(frame.Params), "compaction", "other", 1))
			}
			if mode == "archive" {
				f.archive.fail = true
			}
			effects := nativeEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
				calls++
				if mode == "snapshot" {
					r.Snapshot.Agent.Model.BaseURL = "https://other.invalid"
				}
				request := execution.NativeModelRequest(r.Snapshot.Agent.Model, m, ts)
				if mode == "request" {
					request = execution.NativeModelRequest(r.Snapshot.Agent.Model, []Message{{Role: "user", Content: "replacement"}}, ts)
				}
				if mode == "missing_receipt" {
					return request, nil
				}
				a, _, e := f.engine.BeginModel(c, r, id, request)
				if e != nil {
					return nil, e
				}
				if mode == "partial" {
					return request, f.engine.EndModel(c, r, id, a, []byte(`null`), errors.New("synthetic interrupted body"))
				}
				status, body := 200, nativeProviderResult("fixture")
				if mode == "generic_400" {
					status = 400
					body = []byte(`{"error":{"code":"invalid_request","message":"request too large"}}`)
				}
				if mode == "auth" {
					status = 401
					body = []byte(`{"error":{"code":"context_length_exceeded"}}`)
				}
				if mode == "truncated" {
					body = []byte(strings.Replace(string(body), `"stop"`, `"length"`, 1))
				}
				receipt, _ := execution.NativeReceipt(status, body)
				return request, f.engine.EndModel(c, r, id, a, receipt, nil)
			}}
			if _, _, e := f.engine.Exchange(ctx, &f.r, frame, effects); e == nil {
				t.Fatal("invalid native outcome accepted", mode)
			}
			if modelExchangeResponse(t, f) != nil || f.r.State.PlanRevision != 0 {
				t.Fatal("invalid outcome promoted or frozen")
			}
			if (mode == "cap" || mode == "purpose" || mode == "archive") && calls != 0 {
				t.Fatal("invalid frame reached provider callback")
			}
		})
	}
}

func TestNativeLongRequestResponseAndFiniteAttempts(t *testing.T) {
	f := nativeSetup(t)
	ctx := context.Background()
	f.r.Snapshot.Agent.MaxModelAttempts = 2
	if _, e := f.db.Exec(ctx, "UPDATE runs SET snapshot=$2 WHERE id=$1", f.r.ID, JSON(f.r.Snapshot)); e != nil {
		t.Fatal(e)
	}
	calls := 0
	input := strings.Repeat("资料。", 500000)  // 4.5 MB UTF-8; not a token-count claim.
	output := strings.Repeat("事实。", 583334) // Over 5 MiB, beyond the historical HTTP cap.
	for seq, purpose := range []string{"generation", "compaction"} {
		effects := nativeRecord(t, f, 200, nativeProviderResult(output), &calls)
		v, _, e := f.engine.Exchange(ctx, &f.r, nativeFrame(seq+1, purpose, input), effects)
		if e != nil {
			t.Fatal(e)
		}
		var envelope struct {
			Message Message `json:"message"`
		}
		if json.Unmarshal(JSON(v), &envelope) != nil || envelope.Message.Content != output {
			t.Fatal("large response changed")
		}
	}
	_, _, e := f.engine.Exchange(ctx, &f.r, nativeFrame(3, "generation", "next"), nativeRecord(t, f, 200, nativeProviderResult("no"), &calls))
	if e == nil || !strings.Contains(e.Error(), "attempt budget") || calls != 2 {
		t.Fatal("attempt guard removed", e, calls)
	}
	u, e := f.s.Usage(ctx, f.r.ID)
	if e != nil || u.Attempts != 2 || u.AccountedTokens != 500 || u.TokenBudget != 0 {
		t.Fatal(u, e)
	}
}

func TestNativeSubmissionLargeTextEngineBindingAndCancellation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := nativeSnapshot()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, Resource{Kind: "agent", ID: "test", Revision: 1, Spec: JSON(snap.Agent)})
	}))
	defer peer.Close()
	s.Remote = &serviceclient.Client{RegistryURL: peer.URL, Token: testToken}
	h := (&Server{Store: s, RegistryURL: peer.URL}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"control": servicehttp.TokenHash(testToken + "control"), "futures": servicehttp.TokenHash(testToken + "futures")}})
	for _, app := range []bool{false, true} {
		path, role := "/v1/runs", "control"
		if app {
			path, role = "/v1/apps/futures/runs", "futures"
		}
		input := strings.Repeat("资料。", 500000)
		payload := string(JSON(map[string]any{"agent": Ref{ID: "test", Revision: 1}, "task": input}))
		post := func(key string) *httptest.ResponseRecorder {
			r := httptest.NewRequest("POST", path, strings.NewReader(payload))
			r.Header.Set("Authorization", "Bearer "+testToken+role)
			r.Header.Set("Idempotency-Key", key)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			return w
		}
		if e := s.ConfigureSubmissionEngine(EngineDSHNative); e != nil {
			t.Fatal(e)
		}
		key := ID()
		w := post(key)
		if w.Code != 201 {
			t.Fatal(w.Code, w.Body.String())
		}
		var accepted struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &accepted)
		r, e := s.Run(ctx, accepted.ID)
		if e != nil || r.Task != input || r.Engine != EngineDSHNative {
			t.Fatal("accepted task changed", e)
		}
		if retry := post(key); retry.Code != 201 || retry.Body.String() != w.Body.String() {
			t.Fatal("large submission retry changed")
		}
		_ = s.ConfigureSubmissionEngine(EngineDSHBudget)
		if retry := post(key); retry.Code != 201 || retry.Body.String() != w.Body.String() {
			t.Fatal("default switch blocked existing large submission retry", retry.Code)
		}
		if got := post(ID()); got.Code == 201 {
			t.Fatal("historical engine accepted a new native request")
		}
		_ = s.ConfigureSubmissionEngine(EngineDSHNative)
		if e = s.Cancel(ctx, r.ID); e != nil {
			t.Fatal(e)
		}
		r, e = s.Run(ctx, r.ID)
		if e != nil || r.Status != "cancelled" {
			t.Fatal("native queued cancellation", r.Status, e)
		}
	}
	// Changed deployment defaults may not rewrite an existing run or admit a
	// new native snapshot to an engine with historical smaller limits.
	key := ID()
	id, e := s.Submit(ctx, key, "small", snap, nil)
	if e != nil {
		t.Fatal(e)
	}
	_ = s.ConfigureSubmissionEngine(EngineDSHBudget)
	if same, err := s.Submit(ctx, key, "small", snap, nil); err != nil || same != id {
		t.Fatal("idempotency after deployment switch", err)
	}
	if _, e = s.Submit(ctx, ID(), "small", snap, nil); e == nil {
		t.Fatal("native snapshot submitted to old engine")
	}
	_ = s.ConfigureSubmissionEngine(EngineDSHNative)
	for _, mode := range []string{"limits", "attempts", "family_budget", "child"} {
		bad := nativeSnapshot()
		switch mode {
		case "limits":
			bad.Agent.Model.Limits = nil
		case "attempts":
			bad.Agent.MaxModelAttempts = 0
		case "family_budget":
			bad.Agent.ModelTokenBudget = 250000
		case "child":
			bad.Children = map[string]Snapshot{"old": basicSnapshot("https://fixture.invalid")}
		}
		if _, e = s.Submit(ctx, ID(), "small", bad, nil); e == nil {
			t.Fatal("invalid native snapshot accepted", mode)
		}
	}
}

func TestNativeDefaultKeepsLegacyAgentsAndChildrenUsable(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if err := s.ConfigureSubmissionEngine(EngineDSHNative); err != nil {
		t.Fatal(err)
	}
	legacy := basicSnapshot("https://fixture.invalid")
	legacy.Children = map[string]Snapshot{"old": basicSnapshot("https://fixture.invalid")}
	key := ID()
	id, err := s.Submit(ctx, key, "legacy revision", legacy, nil)
	if err != nil {
		t.Fatal("native default broke frozen legacy agent", err)
	}
	r, err := s.Run(ctx, id)
	if err != nil || r.Engine != EngineDSHBudget {
		t.Fatal("legacy tree did not retain v3 protocol", err)
	}
	if err = s.ConfigureSubmissionEngine(EngineDSHSource); err != nil {
		t.Fatal(err)
	}
	if retry, err := s.Submit(ctx, key, "legacy revision", legacy, nil); err != nil || retry != id {
		t.Fatal("retry changed after default switch", err)
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	childID, err := insertRun(ctx, tx, ID(), "child", legacy.Children["old"], &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	child, err := s.Run(ctx, childID)
	if err != nil || child.Engine != EngineDSHBudget {
		t.Fatal("child did not inherit selected v3", err)
	}
	_ = s.ConfigureSubmissionEngine(EngineDSHNative)
	mixed := basicSnapshot("https://fixture.invalid")
	mixed.Children = map[string]Snapshot{"new": nativeSnapshot()}
	if _, err = s.Submit(ctx, ID(), "mixed tree", mixed, nil); err == nil {
		t.Fatal("mixed legacy parent/native child accepted")
	}
}

func TestNativeDefaultPreservesExpandedLegacyPrompts(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	agent := basicSnapshot("https://fixture.invalid").Agent
	agent.Prompts = []Ref{{ID: "bound", Revision: 1}}
	root := agent
	root.ChildAgents = []Ref{{ID: "child", Revision: 1}}
	bound := strings.Repeat("p", 33000)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("snapshot lost Registry identity")
		}
		switch r.URL.Path {
		case "/v1/resources/agent/root/1":
			jsonResponse(w, 200, Resource{Kind: "agent", ID: "root", Revision: 1, Spec: JSON(root)})
		case "/v1/resources/agent/child/1":
			jsonResponse(w, 200, Resource{Kind: "agent", ID: "child", Revision: 1, Spec: JSON(agent)})
		case "/v1/resources/prompt/bound/1":
			jsonResponse(w, 200, Resource{Kind: "prompt", ID: "bound", Revision: 1, Spec: JSON(Skill{Content: bound})})
		default:
			http.NotFound(w, r)
		}
	}))
	defer peer.Close()
	s.Remote = &serviceclient.Client{RegistryURL: peer.URL, Token: testToken}
	snap, err := s.Snapshot(ctx, Ref{ID: "root", Revision: 1}, 0)
	if err != nil {
		t.Fatal("valid legacy definition failed to resolve", err)
	}
	wantPrompt := agent.Prompt + "\n\n" + bound
	if snap.Agent.Prompt != wantPrompt || snap.Children["child"].Agent.Prompt != wantPrompt {
		t.Fatal("bound prompt missing from root or child")
	}
	for _, engine := range []string{EngineDSHBudget, EngineDSHNative} {
		if err := s.ConfigureSubmissionEngine(engine); err != nil {
			t.Fatal(err)
		}
		id, err := s.Submit(ctx, ID(), "legacy bound prompts", snap, nil)
		if err != nil {
			t.Fatal("deployment default changed valid legacy submission", engine, err)
		}
		run, err := s.Run(ctx, id)
		if err != nil || run.Engine != EngineDSHBudget || run.Snapshot.Agent.Prompt != wantPrompt || run.Snapshot.Children["child"].Agent.Prompt != wantPrompt {
			t.Fatal("legacy engine or expanded prompts changed", err)
		}
	}
}

func TestNativeEngineNeedsExplicitClaimOptIn(t *testing.T) {
	f := nativeSetup(t)
	ctx := context.Background()
	if _, e := f.db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", f.r.ID); e != nil {
		t.Fatal(e)
	}
	for _, versions := range [][]string{nil, {EngineDSHSource, EngineDSHResident, EngineDSHBudget}} {
		e, _ := execution.NewSourceVersions(f.worker, f.archive, versions)
		if _, err := e.Claim(ctx, "old-worker", time.Minute); !errors.Is(err, execution.ErrNoWork) {
			t.Fatal("old worker claimed native", err)
		}
	}
	if r, e := f.engine.Claim(ctx, "native-worker", time.Minute); e != nil || r.Engine != EngineDSHNative {
		t.Fatal("native claim failed", e)
	}
}
