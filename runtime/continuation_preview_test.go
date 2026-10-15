package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const continuationPrivateMarker = "synthetic-private-record-must-not-leak"

type continuationArchiveTrap struct{ calls atomic.Int32 }

func (a *continuationArchiveTrap) PutArtifact(context.Context, []byte, string, string) (string, error) {
	a.calls.Add(1)
	return "", errors.New("preview unexpectedly accessed archive writer")
}

type continuationFixture struct {
	n      *nativeNotesRun
	server *httptest.Server
	trap   *continuationArchiveTrap
	remote atomic.Int32
}

// Snapshot limits are selected before normal Submit, never edited in SQL.
// The native model callback and archive are synthetic; no provider or Node runs.
func continuationSetup(t *testing.T, steps, attempts int, customize func(*Snapshot)) *continuationFixture {
	t.Helper()
	base := nativeNotesSetup(t)
	snap := nativeSnapshot()
	snap.AgentRef.ID = "continuation-" + ID()
	snap.Agent.Prompt = continuationPrivateMarker
	snap.Agent.MaxSteps, snap.Agent.MaxModelAttempts = steps, attempts
	if customize != nil {
		customize(&snap)
	}
	id, err := base.store.Submit(base.ctx, ID(), continuationPrivateMarker, snap, nil)
	nativeNotesNeed(t, err)
	a := &archiveFixture{}
	e, err := execution.NewSourceVersions(base.worker, a, []string{EngineDSHNative})
	nativeNotesNeed(t, err)
	r, err := e.Claim(base.ctx, "continuation-worker-"+ID(), time.Minute)
	nativeNotesNeed(t, err)
	if r.ID != id || r.Engine != EngineDSHNative || r.State.PlanRevision != 0 {
		t.Fatal("normal native fixture submission/claim mismatch")
	}
	f := &continuationFixture{n: &nativeNotesRun{fixture: base, engine: e, archive: a, run: r}, trap: &continuationArchiveTrap{}}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.remote.Add(1)
		http.Error(w, "unexpected remote access", 502)
	}))
	t.Cleanup(remote.Close)
	base.store.Remote = &serviceclient.Client{RegistryURL: remote.URL, CatalogURL: remote.URL, RuntimeURL: remote.URL, Token: testToken}
	base.store.StorageURL, base.store.CatalogURL = remote.URL, remote.URL
	base.store.Archives = f.trap
	f.server = httptest.NewServer(continuationHandler(base.store))
	t.Cleanup(f.server.Close)
	return f
}

func continuationHandler(s *Store) http.Handler {
	peers := map[string]string{}
	for _, role := range []string{"control", "runtime", "worker", "toolhub", "knowledge", "futures", "telemetry"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	return (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
}

func (f *continuationFixture) path() string { return "/v1/runs/" + f.n.run.ID + "/continuation" }

func continuationRequest(ctx context.Context, client *http.Client, base, role, method, path string) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, base+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if role != "" {
		req.Header.Set("Authorization", "Bearer "+testToken+role)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if len(body) > 64<<10 {
		return 0, nil, nil, errors.New("preview response exceeded 64 KiB test boundary")
	}
	return resp.StatusCode, resp.Header, body, err
}

func (f *continuationFixture) request(t *testing.T, role, method, path string, want int) []byte {
	t.Helper()
	status, header, body, err := continuationRequest(f.n.fixture.ctx, f.server.Client(), f.server.URL, role, method, path)
	nativeNotesNeed(t, err)
	if status != want {
		t.Fatalf("continuation %s %s as %s: status=%d want=%d", method, path, role, status, want)
	}
	if role == "control" && method == "GET" && header.Get("Cache-Control") != "no-store" {
		t.Fatal("control preview response must disable caching")
	}
	if bytes.Contains(body, []byte(continuationPrivateMarker)) || bytes.Contains(body, []byte("fixture.invalid")) {
		t.Fatal("private task/model/response information leaked from projection")
	}
	return body
}

type continuationView struct {
	Protocol        string    `json:"protocol"`
	RunID           string    `json:"run_id"`
	Engine          string    `json:"engine"`
	Status          string    `json:"status"`
	Assessment      string    `json:"assessment"`
	Reason          string    `json:"reason"`
	ReadOnly        bool      `json:"read_only"`
	ActionAvailable bool      `json:"action_available"`
	ObservedAt      time.Time `json:"observed_at"`
	Basis           *struct {
		Agent           Ref    `json:"agent"`
		ModelRounds     *int64 `json:"model_rounds"`
		MaxSteps        *int64 `json:"max_steps"`
		FamilyAttempts  *int64 `json:"family_model_attempts"`
		MaxAttempts     *int64 `json:"max_model_attempts"`
		RetryHTTPStatus *int   `json:"retry_http_status"`
	} `json:"basis"`
}

func continuationDecode(t *testing.T, body []byte, id, assessment, reason string) continuationView {
	t.Helper()
	var v continuationView
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	nativeNotesNeed(t, d.Decode(&v))
	if v.Protocol != "failed-run-continuation-preview-v1" || v.RunID != id || v.Engine != EngineDSHNative || v.Assessment != assessment || v.Reason != reason || !v.ReadOnly || v.ActionAvailable || v.ObservedAt.IsZero() {
		t.Fatalf("unexpected continuation projection: %+v", v)
	}
	return v
}

func (f *continuationFixture) preview(t *testing.T, assessment, reason string) continuationView {
	t.Helper()
	return continuationDecode(t, f.request(t, "control", "GET", f.path(), 200), f.n.run.ID, assessment, reason)
}

func continuationBasis(t *testing.T, v continuationView, rounds, attempts, maxSteps, maxAttempts int64) {
	t.Helper()
	if v.Basis == nil || v.Basis.ModelRounds == nil || *v.Basis.ModelRounds != rounds || v.Basis.FamilyAttempts == nil || *v.Basis.FamilyAttempts != attempts || v.Basis.MaxSteps == nil || *v.Basis.MaxSteps != maxSteps || v.Basis.MaxAttempts == nil || *v.Basis.MaxAttempts != maxAttempts || v.Basis.Agent.ID == "" || v.Basis.Agent.Revision != 1 {
		t.Fatalf("unknown/mismatched continuation budget basis: %+v", v.Basis)
	}
}

func (f *continuationFixture) failModel(t *testing.T, seq, status int) {
	t.Helper()
	n := f.n
	effects := n.effects(nil)
	effects.complete = func(ctx context.Context, r Run, op string, messages []Message, tools []Tool) (json.RawMessage, error) {
		request := execution.NativeModelRequest(r.Snapshot.Agent.Model, messages, tools)
		attempt, cached, err := n.engine.BeginModel(ctx, r, op, request)
		if err != nil {
			return nil, err
		}
		if attempt == "" || cached != nil {
			t.Fatal("expected a fresh failed native attempt")
		}
		n.modelCalls.Add(1)
		receipt, err := execution.NativeReceipt(status, JSON(map[string]any{"error": map[string]string{"message": continuationPrivateMarker}}))
		if err != nil {
			return nil, err
		}
		providerErr := fmt.Errorf("synthetic provider HTTP %d: %s", status, continuationPrivateMarker)
		if status != 503 {
			// Classification must use the retained receipt, not a misleading
			// error string which happens to contain the retryable status.
			providerErr = fmt.Errorf("synthetic text says HTTP 503; retained status is %d: %s", status, continuationPrivateMarker)
		}
		if err = n.engine.EndModel(ctx, r, op, attempt, receipt, providerErr); err != nil {
			return nil, err
		}
		return request, providerErr
	}
	_, _, err := n.engine.Exchange(n.fixture.ctx, &n.run, nativeFrame(seq, "generation", continuationPrivateMarker), effects)
	if err == nil {
		t.Fatal("synthetic failed provider did not fail Exchange")
	}
	nativeNotesNeed(t, n.engine.CheckpointFailure(n.fixture.ctx, n.run, err))
	r, err := n.fixture.store.Run(n.fixture.ctx, n.run.ID)
	nativeNotesNeed(t, err)
	if r.Status != "failed" {
		t.Fatal("normal failure checkpoint did not settle failed")
	}
}

func (f *continuationFixture) successModel(t *testing.T, seq int) {
	t.Helper()
	_, done, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, nativeFrame(seq, "generation", continuationPrivateMarker), f.n.effects(nil))
	nativeNotesNeed(t, err)
	if done {
		t.Fatal("model unexpectedly completed task")
	}
}

// The dedicated fixture contains no unrelated data. Full table JSON digests
// include runs/leases/snapshots, all family members and every immutable ledger.
func (f *continuationFixture) digest(t *testing.T) string {
	t.Helper()
	var body []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT jsonb_build_object(
 'runs',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM runs x),
 'calls',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM calls x),
 'attempts',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM model_attempts x),
 'exchanges',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,seq),'[]') FROM harness_exchanges x),
 'archives',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,seq),'[]') FROM harness_archives x),
 'events',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM events x),
 'working_states',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,revision),'[]') FROM run_working_states x),
 'documents',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM documents x),
 'note_roots',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY root_id),'[]') FROM note_roots x),
 'note_versions',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY document_id),'[]') FROM note_versions x),
 'note_audits',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM note_audits x),
 'note_receipts',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY request_id),'[]') FROM note_admin_receipts x))`).Scan(&body))
	return Hash(body)
}

func (f *continuationFixture) unchanged(t *testing.T, before string, model, tool int32) {
	t.Helper()
	if f.digest(t) != before || f.n.modelCalls.Load() != model || f.n.toolCalls.Load() != tool || f.trap.calls.Load() != 0 || f.remote.Load() != 0 {
		t.Fatal("preview wrote state, changed model/tool counts, or requested a remote service/archive")
	}
}

func TestContinuationPreviewHTTPReadOnlyAndAuth(t *testing.T) {
	f := continuationSetup(t, 2, 3, nil)
	f.successModel(t, 1)
	nativeNotesSave(t, f.n)
	f.failModel(t, 3, 503)
	usage, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if usage.Attempts != 2 || usage.UnknownUsageAttempts != 1 || usage.AccountedTokens <= 250 {
		t.Fatal("failed 503 fixture lost its unknown usage reservation")
	}
	before := f.digest(t)
	for i := 0; i < 2; i++ {
		v := f.preview(t, "same_run_candidate", "retained_transient_model_failure")
		continuationBasis(t, v, 2, 2, 2, 3)
		if v.Status != "failed" || v.Basis.Agent != f.n.run.Snapshot.AgentRef || v.Basis.RetryHTTPStatus == nil || *v.Basis.RetryHTTPStatus != 503 {
			t.Fatal("candidate lost fixed identity or retained status")
		}
	}
	for _, role := range []string{"worker", "toolhub", "knowledge", "futures", "runtime", "telemetry"} {
		f.request(t, role, "GET", f.path(), 403)
	}
	f.request(t, "", "GET", f.path(), 401)
	f.request(t, "invalid-token", "GET", f.path(), 401)
	for _, query := range []string{"?force=1", "?budget=", "?force=1&force=1", "?=", "?x", "?%zz"} {
		f.request(t, "control", "GET", f.path()+query, 400)
	}
	for _, method := range []string{"PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} {
		f.request(t, "control", method, f.path(), 403)
	}
	f.request(t, "control", "POST", f.path(), 400) // New action requires its strict body.
	f.request(t, "control", "GET", "/v1/runs/absent-continuation-fixture/continuation", 404)
	afterUsage, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if !jsonEqual(JSON(usage), JSON(afterUsage)) {
		t.Fatal("preview reset or changed unknown usage/accounting")
	}
	f.unchanged(t, before, 2, 1)
}

func TestContinuationPreviewNativeBudgetBoundary(t *testing.T) {
	for _, tc := range []struct {
		name               string
		steps, attempts    int
		prior              bool
		assessment, reason string
		rounds, used       int64
	}{
		{"last-round-same-frame", 1, 2, false, "same_run_candidate", "retained_transient_model_failure", 1, 1},
		{"attempt-cap", 1, 1, false, "blocked", "attempt_budget_exhausted", 1, 1},
		{"next-round-over-cap", 1, 3, true, "blocked", "step_budget_exhausted", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := continuationSetup(t, tc.steps, tc.attempts, nil)
			seq := 1
			if tc.prior {
				f.successModel(t, 1)
				seq = 2
			}
			f.failModel(t, seq, 503)
			before := f.digest(t)
			v := f.preview(t, tc.assessment, tc.reason)
			continuationBasis(t, v, tc.rounds, tc.used, int64(tc.steps), int64(tc.attempts))
			f.unchanged(t, before, int32(tc.used), 0)
		})
	}
	t.Run("family-attempt-cap", func(t *testing.T) {
		f := continuationSetup(t, 2, 3, func(s *Snapshot) {
			child := nativeSnapshot()
			child.Agent.MaxSteps = 1
			child.Agent.MaxModelAttempts = 3
			s.Children = map[string]Snapshot{"child": child}
		})
		f.successModel(t, 1)
		f.n.exchange(t, nativeNotesFrame(2, "spawn_agent", map[string]string{"agent_id": "child", "task": "Synthetic child only"}))
		a := &archiveFixture{}
		e, err := execution.NewSourceVersions(f.n.fixture.worker, a, []string{EngineDSHNative})
		nativeNotesNeed(t, err)
		r, err := e.Claim(f.n.fixture.ctx, "continuation-child-"+ID(), time.Minute)
		nativeNotesNeed(t, err)
		if r.ParentID == nil || *r.ParentID != f.n.run.ID {
			t.Fatal("normal spawn did not produce the native child")
		}
		child := &continuationFixture{n: &nativeNotesRun{fixture: f.n.fixture, engine: e, archive: a, run: r}}
		child.failModel(t, 1, 503)
		f.failModel(t, 3, 503)
		before := f.digest(t)
		v := f.preview(t, "blocked", "attempt_budget_exhausted")
		continuationBasis(t, v, 2, 3, 2, 3)
		childBody := f.request(t, "control", "GET", "/v1/runs/"+r.ID+"/continuation", 200)
		continuationDecode(t, childBody, r.ID, "manual_review", "child_requires_family_review")
		f.unchanged(t, before, 2, 1)
	})
}

func TestContinuationPreviewSafetyGuards(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		f := continuationSetup(t, 1, 2, nil)
		f.failModel(t, 1, 503)
		nativeNotesNeed(t, f.n.fixture.store.Cancel(f.n.fixture.ctx, f.n.run.ID))
		before := f.digest(t)
		f.preview(t, "blocked", "cancelled")
		f.unchanged(t, before, 1, 0)
	})
	t.Run("unknown-external-effect", func(t *testing.T) {
		f := continuationSetup(t, 2, 3, func(s *Snapshot) {
			s.Connectors = []Connector{{Name: "remote", Endpoint: "https://fixture.invalid/mcp", Protocol: "2025-11-25", Tools: []Tool{tool("write", "synthetic external effect", `{"type":"object","additionalProperties":false}`)}}}
		})
		f.successModel(t, 1)
		var effects atomic.Int32
		e := f.n.effects(nil)
		e.tool = func(ctx context.Context, inv Invocation) ([]byte, error) {
			return f.n.fixture.store.ExternalTool(ctx, inv, func(context.Context, Run) ([]byte, error) {
				effects.Add(1)
				return nil, errors.New("synthetic external reply lost")
			})
		}
		_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, nativeNotesFrame(2, "mcp_remote_write", map[string]any{}), e)
		if !errors.Is(err, ErrUnknown) {
			t.Fatal("normal external failure was not unknown", err)
		}
		nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
		before := f.digest(t)
		f.preview(t, "blocked", "unresolved_tool_effect")
		f.unchanged(t, before, 1, 0)
		nativeNotesNeed(t, f.n.fixture.store.Cancel(f.n.fixture.ctx, f.n.run.ID))
		before = f.digest(t)
		f.preview(t, "blocked", "cancelled")
		f.unchanged(t, before, 1, 0)
		if effects.Load() != 1 {
			t.Fatal("preview repeated unknown external effect")
		}
	})
	t.Run("archive-unconfirmed", func(t *testing.T) {
		f := continuationSetup(t, 1, 2, nil)
		f.n.archive.fail = true
		f.failModel(t, 1, 503)
		before := f.digest(t)
		f.preview(t, "manual_review", "archive_unconfirmed")
		f.unchanged(t, before, 0, 0)
	})
	t.Run("constraints-changed", func(t *testing.T) {
		f := continuationSetup(t, 1, 2, nil)
		f.failModel(t, 1, 503)
		nativeNotesNeed(t, f.n.fixture.store.Constraints(f.n.fixture.ctx, f.n.run.ID, []string{"Revised actual period"}))
		before := f.digest(t)
		f.preview(t, "manual_review", "constraints_changed")
		f.unchanged(t, before, 1, 0)
	})
	t.Run("retained-other-http-status", func(t *testing.T) {
		f := continuationSetup(t, 1, 2, nil)
		f.failModel(t, 1, 401)
		before := f.digest(t)
		f.preview(t, "manual_review", "failure_not_assessed")
		f.unchanged(t, before, 1, 0)
	})
	t.Run("unsettled-model-attempt", func(t *testing.T) {
		f := continuationSetup(t, 1, 2, nil)
		effects := f.n.effects(nil)
		effects.complete = func(ctx context.Context, r Run, op string, messages []Message, tools []Tool) (json.RawMessage, error) {
			request := execution.NativeModelRequest(r.Snapshot.Agent.Model, messages, tools)
			attempt, cached, err := f.n.engine.BeginModel(ctx, r, op, request)
			if err != nil {
				return nil, err
			}
			if attempt == "" || cached != nil {
				t.Fatal("missing normally staged inflight attempt")
			}
			return nil, errors.New("synthetic worker interruption before EndModel")
		}
		_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, nativeFrame(1, "generation", continuationPrivateMarker), effects)
		if err == nil {
			t.Fatal("missing interrupted model callback")
		}
		nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
		before := f.digest(t)
		f.preview(t, "manual_review", "unsettled_model_attempt")
		f.unchanged(t, before, 0, 0)
	})
	t.Run("children-not-settled", func(t *testing.T) {
		f := continuationSetup(t, 2, 3, func(s *Snapshot) { s.Children = map[string]Snapshot{"child": nativeSnapshot()} })
		f.successModel(t, 1)
		f.n.exchange(t, nativeNotesFrame(2, "spawn_agent", map[string]string{"agent_id": "child", "task": "Synthetic pending child"}))
		nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, errors.New("synthetic worker interruption after child submission")))
		before := f.digest(t)
		f.preview(t, "blocked", "children_not_settled")
		f.unchanged(t, before, 1, 1)
	})
	t.Run("latest-non-model", func(t *testing.T) {
		f := continuationSetup(t, 2, 3, nil)
		f.successModel(t, 1)
		_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, stateFrame(2, "failed", []byte(`{"error":"synthetic failure"}`)), f.n.effects(nil))
		if err == nil {
			t.Fatal("failed frame unexpectedly succeeded")
		}
		nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
		before := f.digest(t)
		f.preview(t, "manual_review", "unsupported_checkpoint")
		f.unchanged(t, before, 1, 0)
	})
}

func TestContinuationPreviewCorruptLedgerIsNotARecoveryProof(t *testing.T) {
	for _, mode := range []string{"malformed-retained-receipt", "attempt-call-mismatch", "invalid-intent-purpose", "invalid-intent-output", "request-intent-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			f := continuationSetup(t, 1, 2, nil)
			f.failModel(t, 1, 503)
			// Explicit NEGATIVE corruption fixture, not a normally produced run.
			// Never use this injection to establish a positive recovery candidate.
			bad := []byte(`{"unexpected":"not a complete native receipt"}`)
			var err error
			switch mode {
			case "invalid-intent-purpose":
				_, err = f.n.fixture.db.Exec(f.n.fixture.ctx, "UPDATE harness_exchanges SET request=jsonb_set(request,'{purpose}','\"unsupported\"'::jsonb) WHERE run_id=$1 AND seq=1", f.n.run.ID)
			case "invalid-intent-output":
				_, err = f.n.fixture.db.Exec(f.n.fixture.ctx, "UPDATE harness_exchanges SET request=jsonb_set(request,'{max_tokens}','1'::jsonb) WHERE run_id=$1 AND seq=1", f.n.run.ID)
			case "request-intent-mismatch":
				_, err = f.n.fixture.db.Exec(f.n.fixture.ctx, "UPDATE harness_exchanges SET request=jsonb_set(request,'{messages}', '[{\"role\":\"user\",\"content\":\"different frozen intent\"}]'::jsonb) WHERE run_id=$1 AND seq=1", f.n.run.ID)
			default:
				_, err = f.n.fixture.db.Exec(f.n.fixture.ctx, "UPDATE model_attempts SET response=$2 WHERE call_id=$1", nativeNotesOperation(f.n.run, 1), bad)
				if err == nil && mode == "malformed-retained-receipt" {
					_, err = f.n.fixture.db.Exec(f.n.fixture.ctx, "UPDATE calls SET response=$2 WHERE id=$1", nativeNotesOperation(f.n.run, 1), bad)
				}
			}
			nativeNotesNeed(t, err)
			before := f.digest(t)
			body := f.request(t, "control", "GET", f.path(), 200)
			var v continuationView
			nativeNotesNeed(t, json.Unmarshal(body, &v))
			if v.Assessment != "manual_review" || v.Reason == "" || !v.ReadOnly || v.ActionAvailable {
				t.Fatal("corrupt negative fixture became an executable/safe candidate")
			}
			f.unchanged(t, before, 1, 0)
		})
	}
}

// TraceQueryEnd runs after the first data read established the MVCC snapshot.
// The test releases it only after a second normal failed attempt has committed.
type continuationTraceKey struct{}
type continuationReadGate struct {
	armed                    atomic.Bool
	entered, release         chan struct{}
	once                     sync.Once
	repeatableRead, readOnly atomic.Bool
}

func (g *continuationReadGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if g.armed.Load() {
		sql := strings.ToLower(d.SQL)
		if strings.HasPrefix(strings.TrimSpace(sql), "begin") {
			g.repeatableRead.Store(strings.Contains(sql, "repeatable read"))
			g.readOnly.Store(strings.Contains(sql, "read only"))
		}
		return context.WithValue(ctx, continuationTraceKey{}, sql)
	}
	return ctx
}
func (g *continuationReadGate) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(continuationTraceKey{}).(string)
	if d.Err != nil || !g.armed.Load() || !strings.Contains(sql, "select") || !strings.Contains(sql, "from runs") {
		return
	}
	g.once.Do(func() {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
		}
	})
}

func TestContinuationPreviewConsistentReadSnapshot(t *testing.T) {
	f := continuationSetup(t, 1, 2, nil)
	f.failModel(t, 1, 503)
	g := &continuationReadGate{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(g.release) }) }
	defer unblock()
	cfg := f.n.fixture.store.DB.Config().Copy()
	cfg.ConnConfig.Tracer = g
	pool, err := pgxpool.NewWithConfig(f.n.fixture.ctx, cfg)
	nativeNotesNeed(t, err)
	t.Cleanup(pool.Close)
	readStore := NewStore(pool, f.n.fixture.store.Remote, f.n.fixture.store.StorageURL)
	readStore.Archives = f.trap
	server := httptest.NewServer(continuationHandler(readStore))
	t.Cleanup(server.Close)
	g.armed.Store(true)
	type response struct {
		status int
		body   []byte
		err    error
	}
	result := make(chan response, 1)
	go func() {
		status, _, body, e := continuationRequest(f.n.fixture.ctx, server.Client(), server.URL, "control", "GET", f.path())
		result <- response{status, body, e}
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preview never reached the deterministic read barrier")
	}
	// A separate explicit action is the concurrent actor. The preview GET
	// remains a repeatable-read observation with no continuation authority.
	continuationActionQueue(t, f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", f.n.run.ID)
	next, err := f.n.engine.Claim(f.n.fixture.ctx, "continuation-concurrent-retry-"+ID(), time.Minute)
	nativeNotesNeed(t, err)
	if next.ID != f.n.run.ID || next.Fence != f.n.run.Fence+1 {
		t.Fatal("concurrent explicit retry did not obtain normal new fence")
	}
	next.Step = 0 // Worker.driveDSH reconstructs Step from zero for each replay.
	f.n.run = next
	f.failModel(t, 1, 503)
	before := f.digest(t)
	unblock()
	var first response
	select {
	case first = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot preview did not complete after releasing barrier")
	}
	nativeNotesNeed(t, first.err)
	if first.status != 200 {
		t.Fatalf("first snapshot HTTP status=%d", first.status)
	}
	v := continuationDecode(t, first.body, f.n.run.ID, "same_run_candidate", "retained_transient_model_failure")
	continuationBasis(t, v, 1, 1, 1, 2)
	if !g.repeatableRead.Load() || !g.readOnly.Load() {
		t.Fatal("preview did not begin a repeatable-read read-only transaction")
	}
	v = f.preview(t, "blocked", "attempt_budget_exhausted")
	continuationBasis(t, v, 1, 2, 1, 2)
	f.unchanged(t, before, 2, 0)
}

func TestContinuationPreviewDatabaseFailureIsUnavailable(t *testing.T) {
	f := continuationSetup(t, 1, 2, nil)
	f.failModel(t, 1, 503)
	before := f.digest(t)
	closed, err := pgxpool.NewWithConfig(f.n.fixture.ctx, f.n.fixture.store.DB.Config().Copy())
	nativeNotesNeed(t, err)
	closed.Close()
	s := NewStore(closed, f.n.fixture.store.Remote, f.n.fixture.store.StorageURL)
	s.Archives = f.trap
	for _, cancelled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(f.n.fixture.ctx)
		if cancelled {
			cancel()
		}
		req := httptest.NewRequest("GET", f.path(), nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+testToken+"control")
		target := s
		if cancelled {
			target = f.n.fixture.store
		}
		recorder := httptest.NewRecorder()
		continuationHandler(target).ServeHTTP(recorder, req)
		cancel()
		if recorder.Code != 503 || recorder.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("database failure returned a partial/optimistic preview", recorder.Code)
		}
		body := recorder.Body.String()
		if !strings.Contains(body, "continuation preview unavailable") || strings.Contains(body, "closed") || strings.Contains(body, "SELECT") || strings.Contains(body, "postgres") || strings.Contains(body, continuationPrivateMarker) {
			t.Fatal("database failure exposed internal details or lost the bounded error")
		}
	}
	f.unchanged(t, before, 1, 0)
}
