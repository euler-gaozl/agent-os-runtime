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
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Owner HTTP/transaction fixtures: real PostgreSQL, normal Submit/Claim/native
// Exchange, synthetic complete model receipts, memory archives. No provider or
// official Node loop runs here. SQL only reads evidence or holds read locks;
// server-context cancellation below is explicitly a negative rollback fixture.
type actionHTTPResult struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func actionHTTP(ctx context.Context, client *http.Client, base, token, method, path string, body []byte) actionHTTPResult {
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return actionHTTPResult{err: err}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return actionHTTPResult{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, (16<<10)+1))
	if len(b) > 16<<10 {
		err = errors.New("continuation response exceeded bounded fixture read")
	}
	return actionHTTPResult{resp.StatusCode, resp.Header, b, err}
}
func actionCheck(t *testing.T, r actionHTTPResult, want int) []byte {
	t.Helper()
	nativeNotesNeed(t, r.err)
	if r.status != want {
		t.Fatalf("continuation HTTP status=%d want=%d body=%s", r.status, want, r.body)
	}
	if bytes.Contains(r.body, []byte(continuationPrivateMarker)) || bytes.Contains(r.body, []byte("fixture.invalid")) {
		t.Fatal("continuation leaked synthetic private evidence")
	}
	return r.body
}
func actionRequest(t *testing.T, f *continuationFixture, method, path string, body []byte, want int) []byte {
	t.Helper()
	r := actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", method, path, body)
	b := actionCheck(t, r, want)
	if r.header.Get("Cache-Control") != "no-store" {
		t.Fatal("authorized continuation response lacks no-store")
	}
	return b
}

func actionContext(t *testing.T, ctx context.Context, client *http.Client, base, token, id string) string {
	t.Helper()
	r := actionHTTP(ctx, client, base, token, "GET", "/v1/runs/"+id+"/continuation/request-context", nil)
	b := actionCheck(t, r, 200)
	if r.header.Get("Cache-Control") != "no-store" {
		t.Fatal("action context must disable caching")
	}
	var v struct {
		Protocol       string    `json:"protocol"`
		RunID          string    `json:"run_id"`
		Assessment     string    `json:"assessment"`
		FailureVersion string    `json:"failure_version"`
		ObservedAt     time.Time `json:"observed_at"`
	}
	nativeNotesNeed(t, json.Unmarshal(b, &v))
	if v.Protocol != "failed-run-continuation-action-context-v1" || v.RunID != id || v.Assessment != "same_run_candidate" || len(v.FailureVersion) != 64 || v.ObservedAt.IsZero() {
		t.Fatalf("not a versioned candidate: %s", b)
	}
	return v.FailureVersion
}
func actionBody(id, version string) []byte {
	return JSON(map[string]string{"request_id": id, "expected_failure_version": version})
}

// Shared by the migrated preview, execution-guard and opt-in actual Worker tests.
func continuationActionQueue(t *testing.T, ctx context.Context, client *http.Client, base, token, id string) []byte {
	t.Helper()
	version := actionContext(t, ctx, client, base, token, id)
	return actionCheck(t, actionHTTP(ctx, client, base, token, "POST", "/v1/runs/"+id+"/continuation", actionBody("fixture-"+ID(), version)), 202)
}
func actionVersion(t *testing.T, f *continuationFixture) string {
	t.Helper()
	return actionContext(t, f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", f.n.run.ID)
}
func actionPost(t *testing.T, f *continuationFixture, id, version string, want int) []byte {
	t.Helper()
	return actionRequest(t, f, "POST", f.path(), actionBody(id, version), want)
}
func actionReceipt(t *testing.T, b []byte, id, run, version, outcome, reason string) {
	t.Helper()
	var v struct {
		Protocol       string     `json:"protocol"`
		RequestID      string     `json:"request_id"`
		RunID          string     `json:"run_id"`
		Outcome        string     `json:"outcome"`
		FailureVersion string     `json:"failure_version"`
		Committed      bool       `json:"committed"`
		Status         string     `json:"status_at_acceptance"`
		Assessment     string     `json:"assessment"`
		Reason         string     `json:"reason"`
		Accepted       *time.Time `json:"accepted_at"`
		Rejected       *time.Time `json:"rejected_at"`
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	nativeNotesNeed(t, d.Decode(&v))
	if v.Protocol != "failed-run-continuation-receipt-v1" || v.RequestID != id || v.RunID != run || v.FailureVersion != version || !v.Committed || v.Outcome != outcome || v.Reason != reason {
		t.Fatalf("invalid historical receipt: %s", b)
	}
	if outcome == "queued" && (v.Status != "queued" || v.Accepted == nil || v.Accepted.IsZero() || v.Rejected != nil || v.Assessment != "") {
		t.Fatal("queued receipt lost historical acceptance")
	}
	if outcome == "rejected" && (v.Status != "" || v.Accepted != nil || v.Rejected == nil || v.Rejected.IsZero() || v.Assessment == "") {
		t.Fatal("refusal is not a committed decision")
	}
}
func actionDigest(t *testing.T, f *continuationFixture) string {
	t.Helper()
	var receipts []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY request_id),'[]') FROM run_continuation_receipts x").Scan(&receipts))
	return Hash(JSON([]string{f.digest(t), string(receipts)})) // all rows in 13 tables
}
func actionCounts(t *testing.T, f *continuationFixture, receipts, queued int) {
	t.Helper()
	var nr, nq int
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT (SELECT count(*) FROM run_continuation_receipts),(SELECT count(*) FROM events WHERE kind='continuation_queued')").Scan(&nr, &nq))
	if nr != receipts || nq != queued {
		t.Fatalf("receipt/queue counts=%d/%d want=%d/%d", nr, nq, receipts, queued)
	}
	if f.trap.calls.Load() != 0 || f.remote.Load() != 0 {
		t.Fatal("management accessed an archive/remote service")
	}
}
func actionClaim(t *testing.T, f *continuationFixture) {
	t.Helper()
	old := f.n.run
	next, err := f.n.engine.Claim(f.n.fixture.ctx, "continuation-action-"+ID(), time.Minute)
	nativeNotesNeed(t, err)
	if next.ID != old.ID || next.Fence != old.Fence+1 || next.Owner == old.Owner {
		t.Fatal("continuation did not yield one normal fenced Claim")
	}
	next.Step = 0 // same in-memory replay reset used by Worker.driveDSH
	f.n.run = next
}
func actionRunInvariant(t *testing.T, f *continuationFixture) string {
	t.Helper()
	var b []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT to_jsonb(r)-ARRAY['status','owner','lease_until','updated_at'] FROM runs r WHERE id=$1", f.n.run.ID).Scan(&b))
	return Hash(b)
}

func actionFrozenEvidence(t *testing.T, f *continuationFixture) string {
	t.Helper()
	var b []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT jsonb_build_object(
 'exchanges',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,seq),'[]') FROM harness_exchanges x),
 'archives',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,seq),'[]') FROM harness_archives x),
 'working_states',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,revision),'[]') FROM run_working_states x),
 'documents',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM documents x),
 'note_roots',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY root_id),'[]') FROM note_roots x),
 'note_versions',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY document_id),'[]') FROM note_versions x),
 'note_audits',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM note_audits x),
 'note_receipts',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY request_id),'[]') FROM note_admin_receipts x))`).Scan(&b))
	return Hash(b)
}

func TestContinuationActionNormalReplayAndBudget(t *testing.T) {
	f := continuationSetup(t, 2, 3, nil)
	f.successModel(t, 1)
	frame := nativeNotesFrame(2, "note_save", map[string]any{"kind": "fact", "title": "Synthetic observed volume", "content": "Observed 42 units; next month 9 units is a plan, not an actual.", "source": "fixture://native-notes/original", "metadata": map[string]any{"synthetic": true, "actual": 42, "planned": 9}})
	note := f.n.exchange(t, frame)
	f.failModel(t, 3, 503)
	before := actionDigest(t, f)
	v := actionVersion(t, f)
	if actionVersion(t, f) != v || actionDigest(t, f) != before {
		t.Fatal("context changed immutable evidence or failure version")
	}
	invariant, ledger, frozen := actionRunInvariant(t, f), nativeRevisionLedger(t, f), actionFrozenEvidence(t, f)
	var failedID string
	var failedRow []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT a.id,to_jsonb(a) FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 AND a.status='failed'", f.n.run.ID).Scan(&failedID, &failedRow))
	accepted := actionPost(t, f, "normal", v, 202)
	actionReceipt(t, accepted, "normal", f.n.run.ID, v, "queued", "")
	if actionRunInvariant(t, f) != invariant || nativeRevisionLedger(t, f) != ledger || actionFrozenEvidence(t, f) != frozen || f.n.modelCalls.Load() != 2 || f.n.toolCalls.Load() != 1 {
		t.Fatal("queue changed frozen execution evidence or spent a model attempt")
	}
	actionCounts(t, f, 1, 1)
	actionClaim(t, f)
	f.successModel(t, 1)
	nativeNotesSame(t, note, f.n.exchange(t, frame), "continued note receipt")
	if f.n.modelCalls.Load() != 2 || f.n.toolCalls.Load() != 1 {
		t.Fatal("replay repeated a model or local note effect")
	}
	f.successModel(t, 3)
	_, done, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, stateFrame(4, "finish", JSON(map[string]string{"result": "Synthetic factual report complete."})), f.n.effects(nil))
	nativeNotesNeed(t, err)
	if !done {
		t.Fatal("same-frame continuation did not finish")
	}
	var retained []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT to_jsonb(a) FROM model_attempts a WHERE id=$1", failedID).Scan(&retained))
	u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if !bytes.Equal(retained, failedRow) || u.Attempts != 3 || u.UnknownUsageAttempts != 1 || f.n.modelCalls.Load() != 3 || f.n.toolCalls.Load() != 1 {
		t.Fatal("continuation lost an old failure/unknown usage or repeated effects")
	}
	var docs, audits int
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM note_audits)").Scan(&docs, &audits))
	if docs != 1 || audits != 1 {
		t.Fatal("continued note was duplicated")
	}
	before = actionDigest(t, f)
	if !bytes.Equal(accepted, actionPost(t, f, "normal", v, 202)) || !bytes.Equal(accepted, actionRequest(t, f, "GET", f.path()+"/requests/normal", nil, 200)) || actionDigest(t, f) != before {
		t.Fatal("historical completed-run receipt replay mutated state")
	}
	actionCounts(t, f, 1, 1)
}

func TestContinuationActionLostReplyAndFailureGenerations(t *testing.T) {
	for _, engine := range []string{EngineDSHNative, EngineDSHFinal} {
		t.Run(engine, func(t *testing.T) { testContinuationLostReply(t, engine) })
	}
}

func testContinuationLostReply(t *testing.T, engine string) {
	f := continuationSetupVersion(t, engine, 1, 3, nil)
	f.failModel(t, 1, 503)
	va := actionVersion(t, f)
	actionRequest(t, f, "GET", f.path()+"/requests/lost", nil, 404)
	committed := make(chan []byte, 1)
	// Actual HTTP connection closes AFTER the real application Handler returns
	// its committed receipt. This is not PostgreSQL COMMIT-response fault proof.
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		continuationHandler(f.n.fixture.store).ServeHTTP(rec, r)
		committed <- append([]byte(nil), rec.Body.Bytes()...)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer drop.Close()
	lost := actionHTTP(f.n.fixture.ctx, drop.Client(), drop.URL, testToken+"control", "POST", f.path(), actionBody("lost", va))
	if lost.err == nil {
		t.Fatal("client received a response instead of the injected lost reply")
	}
	accepted := <-committed
	actionReceipt(t, accepted, "lost", f.n.run.ID, va, "queued", "")
	if !bytes.Equal(accepted, actionRequest(t, f, "GET", f.path()+"/requests/lost", nil, 200)) || !bytes.Equal(accepted, actionPost(t, f, "lost", va, 202)) {
		t.Fatal("lost-response retry did not converge to durable body")
	}
	rejected := actionPost(t, f, "double-click", va, 409)
	actionReceipt(t, rejected, "double-click", f.n.run.ID, va, "rejected", "not_failed")
	actionClaim(t, f)
	f.failModel(t, 1, 503)
	vb := actionVersion(t, f)
	if va == vb {
		t.Fatal("second normal failure generation reused old version")
	}
	before := actionDigest(t, f)
	if !bytes.Equal(accepted, actionPost(t, f, "lost", va, 202)) || !bytes.Equal(rejected, actionPost(t, f, "double-click", va, 409)) || actionDigest(t, f) != before {
		t.Fatal("historical request replay changed the next failed generation")
	}
	stale := actionPost(t, f, "stale-generation", va, 409)
	actionReceipt(t, stale, "stale-generation", f.n.run.ID, va, "rejected", "failure_version_changed")
	conflict := actionPost(t, f, "lost", vb, 409)
	if !bytes.Contains(conflict, []byte("request_id_conflict")) {
		t.Fatal("same identity accepted another version")
	}
	actionPost(t, f, "second", vb, 202)
	actionClaim(t, f)
	f.failModel(t, 1, 503)
	cap := actionPost(t, f, "at-cap", vb, 409)
	actionReceipt(t, cap, "at-cap", f.n.run.ID, vb, "rejected", "attempt_budget_exhausted")
	u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if u.Attempts != 3 || u.UnknownUsageAttempts != 3 {
		t.Fatal("failed retries were made free in budget accounting")
	}
	nativeNotesNeed(t, f.n.fixture.store.Cancel(f.n.fixture.ctx, f.n.run.ID))
	before = actionDigest(t, f)
	if !bytes.Equal(accepted, actionPost(t, f, "lost", va, 202)) || actionDigest(t, f) != before {
		t.Fatal("old request replay cleared cancellation")
	}
	actionCounts(t, f, 5, 2)
}

// A query-end gate pauses only after PostgreSQL has executed a selected query,
// while that transaction still owns its locks. PID assertions use real server
// lock state; a quick return is not accepted as proof of serialization.
type actionTraceKey struct{}
type actionGate struct {
	match   func(string) bool
	entered chan uint32
	release chan struct{}
	once    sync.Once
}

func newActionGate(match func(string) bool) *actionGate {
	return &actionGate{match: match, entered: make(chan uint32, 1), release: make(chan struct{})}
}
func (g *actionGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, actionTraceKey{}, strings.ToLower(d.SQL))
}
func (g *actionGate) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(actionTraceKey{}).(string)
	if d.Err == nil && g.match(sql) {
		g.once.Do(func() {
			g.entered <- c.PgConn().PID()
			select {
			case <-g.release:
			case <-ctx.Done():
			}
		})
	}
}
func actionGatedStore(t *testing.T, f *continuationFixture, g *actionGate) *Store {
	t.Helper()
	cfg := f.n.fixture.store.DB.Config().Copy()
	cfg.ConnConfig.Tracer = g
	pool, err := pgxpool.NewWithConfig(f.n.fixture.ctx, cfg)
	nativeNotesNeed(t, err)
	t.Cleanup(pool.Close)
	s := NewStore(pool, f.n.fixture.store.Remote, f.n.fixture.store.StorageURL)
	s.Archives = f.trap
	return s
}
func actionEntered(t *testing.T, g *actionGate) uint32 {
	t.Helper()
	select {
	case pid := <-g.entered:
		return pid
	case <-time.After(2 * time.Second):
		t.Fatal("transaction did not reach query-end gate")
		return 0
	}
}
func actionAwait(t *testing.T, ch <-chan actionHTTPResult) actionHTTPResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(6 * time.Second):
		t.Fatal("bounded action did not finish")
		return actionHTTPResult{}
	}
}
func actionBlocked(t *testing.T, f *continuationFixture, pid uint32, minimum int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := f.n.fixture.db.QueryRow(f.n.fixture.ctx, `WITH RECURSIVE blocked(pid,path) AS (
 SELECT a.pid,ARRAY[a.pid] FROM pg_stat_activity a WHERE a.datname=current_database() AND a.wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(a.pid))
 UNION ALL SELECT a.pid,b.path||a.pid FROM pg_stat_activity a JOIN blocked b ON b.pid=ANY(pg_blocking_pids(a.pid)) WHERE a.datname=current_database() AND a.wait_event_type='Lock' AND NOT a.pid=ANY(b.path)) SELECT count(DISTINCT pid) FROM blocked`, int64(pid)).Scan(&n)
		nativeNotesNeed(t, err)
		if n >= minimum {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected independent transaction was never blocked by the held row")
}
func actionAsync(f *continuationFixture, base string, client *http.Client, id, v string) <-chan actionHTTPResult {
	ch := make(chan actionHTTPResult, 1)
	go func() {
		ch <- actionHTTP(f.n.fixture.ctx, client, base, testToken+"control", "POST", f.path(), actionBody(id, v))
	}()
	return ch
}

func TestContinuationActionConcurrentRequests(t *testing.T) {
	for _, same := range []bool{true, false} {
		t.Run(fmt.Sprintf("same-id-%v", same), func(t *testing.T) {
			f := continuationSetup(t, 1, 3, nil)
			f.failModel(t, 1, 503)
			v := actionVersion(t, f)
			cfg := f.n.fixture.store.DB.Config().Copy()
			pool, err := pgxpool.NewWithConfig(f.n.fixture.ctx, cfg)
			nativeNotesNeed(t, err)
			t.Cleanup(pool.Close)
			s := NewStore(pool, f.n.fixture.store.Remote, f.n.fixture.store.StorageURL)
			s.Archives = f.trap
			second := httptest.NewServer(continuationHandler(s))
			defer second.Close()
			lock, err := f.n.fixture.db.Begin(f.n.fixture.ctx)
			nativeNotesNeed(t, err)
			defer lock.Rollback(f.n.fixture.ctx)
			var pid uint32
			nativeNotesNeed(t, lock.QueryRow(f.n.fixture.ctx, "SELECT pg_backend_pid() FROM runs WHERE id=$1 FOR UPDATE", f.n.run.ID).Scan(&pid))
			id2 := "first"
			if !same {
				id2 = "second"
			}
			a := actionAsync(f, f.server.URL, f.server.Client(), "first", v)
			b := actionAsync(f, second.URL, second.Client(), id2, v)
			actionBlocked(t, f, pid, 2)
			nativeNotesNeed(t, lock.Commit(f.n.fixture.ctx))
			ra, rb := actionAwait(t, a), actionAwait(t, b)
			nativeNotesNeed(t, ra.err)
			nativeNotesNeed(t, rb.err)
			if same {
				actionCheck(t, ra, 202)
				actionCheck(t, rb, 202)
				if !bytes.Equal(ra.body, rb.body) {
					t.Fatal("same request raced to different persisted bodies")
				}
				actionCounts(t, f, 1, 1)
			} else {
				if !((ra.status == 202 && rb.status == 409) || (ra.status == 409 && rb.status == 202)) {
					t.Fatal("different IDs did not produce one acceptance and one refusal", ra.status, rb.status)
				}
				actionCounts(t, f, 2, 1)
			}
		})
	}
}

func TestContinuationActionSerializedDecisions(t *testing.T) {
	for _, mode := range []string{"cancel-first", "constraints-first", "action-first-cancel", "claim-skip-locked", "rollback-after-update"} {
		t.Run(mode, func(t *testing.T) {
			f := continuationSetup(t, 1, 3, nil)
			f.failModel(t, 1, 503)
			v := actionVersion(t, f)
			match := "update runs set status='queued'"
			if mode == "cancel-first" {
				match = "update runs r set cancel_requested=true"
			}
			if mode == "constraints-first" {
				match = "update runs set constraints="
			}
			g := newActionGate(func(sql string) bool { return strings.HasPrefix(sql, match) })
			var once sync.Once
			unblock := func() { once.Do(func() { close(g.release) }) }
			defer unblock()
			s := actionGatedStore(t, f, g)
			serverCtx, cancel := context.WithCancel(f.n.fixture.ctx)
			defer cancel()
			h := continuationHandler(s)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "rollback-after-update" {
					r = r.WithContext(serverCtx)
				}
				h.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			if mode == "cancel-first" || mode == "constraints-first" {
				manager := make(chan actionHTTPResult, 1)
				path, want, body := "/cancel", 202, []byte(nil)
				reason := "cancelled"
				if mode == "constraints-first" {
					path, want, body = "/constraints", 200, JSON(map[string]any{"values": []string{"synthetic new period"}})
					reason = "constraints_changed"
				}
				go func() {
					manager <- actionHTTP(f.n.fixture.ctx, server.Client(), server.URL, testToken+"control", "POST", "/v1/runs/"+f.n.run.ID+path, body)
				}()
				pid := actionEntered(t, g)
				result := actionAsync(f, f.server.URL, f.server.Client(), "serialized", v)
				actionBlocked(t, f, pid, 1)
				unblock()
				actionCheck(t, actionAwait(t, manager), want)
				b := actionCheck(t, actionAwait(t, result), 409)
				actionReceipt(t, b, "serialized", f.n.run.ID, v, "rejected", reason)
				actionCounts(t, f, 1, 0)
				return
			}
			before := actionDigest(t, f)
			result := actionAsync(f, server.URL, server.Client(), "serialized", v)
			pid := actionEntered(t, g)
			switch mode {
			case "action-first-cancel":
				manager := make(chan actionHTTPResult, 1)
				go func() {
					manager <- actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", "POST", "/v1/runs/"+f.n.run.ID+"/cancel", nil)
				}()
				actionBlocked(t, f, pid, 1)
				unblock()
				accepted := actionCheck(t, actionAwait(t, result), 202)
				actionCheck(t, actionAwait(t, manager), 202)
				r, err := f.n.fixture.store.Run(f.n.fixture.ctx, f.n.run.ID)
				nativeNotesNeed(t, err)
				if r.Status != "cancelled" || !r.CancelRequested {
					t.Fatal("cancel did not follow committed queue")
				}
				after := actionDigest(t, f)
				if !bytes.Equal(accepted, actionPost(t, f, "serialized", v, 202)) || actionDigest(t, f) != after {
					t.Fatal("historical receipt replay reversed cancellation")
				}
				actionCounts(t, f, 1, 1)
			case "claim-skip-locked":
				_, err := f.n.engine.Claim(f.n.fixture.ctx, "racing-claim", time.Minute)
				if !errors.Is(err, execution.ErrNoWork) {
					t.Fatal("Claim must skip uncommitted action's locked run", err)
				}
				unblock()
				actionCheck(t, actionAwait(t, result), 202)
				actionClaim(t, f)
				actionCounts(t, f, 1, 1)
			case "rollback-after-update":
				// Negative server-context fault after UPDATE executed but before event,
				// receipt or COMMIT. No client cancellation or SQL state injection.
				cancel()
				unblock()
				b := actionCheck(t, actionAwait(t, result), 503)
				if !bytes.Contains(b, []byte("continuation action unavailable")) || actionDigest(t, f) != before {
					t.Fatal("interrupted action committed a partial run/event/receipt")
				}
				actionCounts(t, f, 0, 0)
				actionPost(t, f, "serialized", v, 202)
				actionCounts(t, f, 1, 1)
			}
		})
	}
	t.Run("real-lock-timeout-no-receipt", func(t *testing.T) {
		f := continuationSetup(t, 1, 2, nil)
		f.failModel(t, 1, 503)
		v := actionVersion(t, f)
		before := actionDigest(t, f)
		lock, err := f.n.fixture.db.Begin(f.n.fixture.ctx)
		nativeNotesNeed(t, err)
		defer lock.Rollback(f.n.fixture.ctx)
		var pid uint32
		nativeNotesNeed(t, lock.QueryRow(f.n.fixture.ctx, "SELECT pg_backend_pid() FROM runs WHERE id=$1 FOR UPDATE", f.n.run.ID).Scan(&pid))
		result := actionAsync(f, f.server.URL, f.server.Client(), "timeout", v)
		actionBlocked(t, f, pid, 1)
		b := actionCheck(t, actionAwait(t, result), 503)
		if !bytes.Contains(b, []byte("continuation action unavailable")) || bytes.Contains(b, []byte("lock_timeout")) {
			t.Fatal("lock error was misclassified or leaked SQL")
		}
		nativeNotesNeed(t, lock.Commit(f.n.fixture.ctx))
		if actionDigest(t, f) != before {
			t.Fatal("timed-out request left partial state")
		}
		actionPost(t, f, "timeout", v, 202)
	})
}

func actionChild(t *testing.T, f *continuationFixture, worker *pgxpool.Pool) *nativeNotesRun {
	t.Helper()
	f.successModel(t, 1)
	f.n.exchange(t, nativeNotesFrame(2, "spawn_agent", map[string]string{"agent_id": "child", "task": "Synthetic child only"}))
	a := &archiveFixture{}
	e, err := execution.NewSourceVersions(worker, a, []string{EngineDSHNative})
	nativeNotesNeed(t, err)
	r, err := e.Claim(f.n.fixture.ctx, "continuation-child-"+ID(), time.Minute)
	nativeNotesNeed(t, err)
	if r.ParentID == nil || *r.ParentID != f.n.run.ID {
		t.Fatal("normal spawn/Claim did not produce this root's child")
	}
	return &nativeNotesRun{fixture: f.n.fixture, engine: e, archive: a, run: r}
}
func actionFamilySnapshot(s *Snapshot) {
	child := nativeSnapshot()
	child.Agent.MaxSteps = 1
	child.Agent.MaxModelAttempts = s.Agent.MaxModelAttempts
	s.Children = map[string]Snapshot{"child": child}
}
func TestContinuationActionFamilyLockOrderAndBudget(t *testing.T) {
	t.Run("active-child-row-before-family-budget", func(t *testing.T) {
		f := continuationSetup(t, 2, 5, actionFamilySnapshot)
		g := newActionGate(func(sql string) bool { return sql == "select rule_revision,engine from runs where id=$1" })
		var once sync.Once
		unblock := func() { once.Do(func() { close(g.release) }) }
		defer unblock()
		cfg := f.n.fixture.worker.Config().Copy()
		cfg.ConnConfig.Tracer = g
		pool, err := pgxpool.NewWithConfig(f.n.fixture.ctx, cfg)
		nativeNotesNeed(t, err)
		t.Cleanup(pool.Close)
		child := actionChild(t, f, pool)
		// Natural negative state: active child and root's ordinary local failure.
		// This is NOT a positive 503 candidate and the placeholder is not a permit.
		nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, errors.New("synthetic root interrupted while child remains active")))
		complete := make(chan error, 1)
		go func() {
			_, _, e := child.engine.Exchange(f.n.fixture.ctx, &child.run, nativeFrame(1, "generation", "synthetic child model"), child.effects(nil))
			complete <- e
		}()
		pid := actionEntered(t, g)
		result := actionAsync(f, f.server.URL, f.server.Client(), "family-lock", strings.Repeat("0", 64))
		actionBlocked(t, f, pid, 1)
		select {
		case r := <-result:
			t.Fatal("action returned before acquiring the active child's row", r.status)
		default:
		}
		unblock()
		select {
		case err = <-complete:
			nativeNotesNeed(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("child/action family budget lock order deadlocked")
		}
		b := actionCheck(t, actionAwait(t, result), 409)
		actionReceipt(t, b, "family-lock", f.n.run.ID, strings.Repeat("0", 64), "rejected", "children_not_settled")
		if child.modelCalls.Load() != 1 {
			t.Fatal("child did not complete its real BeginModel/EndModel transaction")
		}
		actionCounts(t, f, 1, 0)
	})
	t.Run("settled-child-attempts-remain-in-family-cap", func(t *testing.T) {
		f := continuationSetup(t, 2, 3, actionFamilySnapshot)
		child := actionChild(t, f, f.n.fixture.worker)
		cf := &continuationFixture{n: child}
		cf.failModel(t, 1, 503)
		f.failModel(t, 3, 503)
		v := strings.Repeat("0", 64)
		b := actionPost(t, f, "family-cap", v, 409)
		actionReceipt(t, b, "family-cap", f.n.run.ID, v, "rejected", "attempt_budget_exhausted")
		u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
		nativeNotesNeed(t, err)
		if u.Attempts != 3 || u.UnknownUsageAttempts != 2 {
			t.Fatal("settled child attempts/unknown usage disappeared from family cap")
		}
		actionCounts(t, f, 1, 0)
	})
}

func TestContinuationActionHTTPBoundaryAndUnavailable(t *testing.T) {

	t.Run("unknown-external-effect-cannot-queue", func(t *testing.T) {
		f := continuationSetup(t, 2, 3, func(s *Snapshot) {
			s.Connectors = []Connector{{Name: "remote", Endpoint: "https://fixture.invalid/mcp", Protocol: "2025-11-25", Tools: []Tool{tool("write", "synthetic external effect", `{"type":"object","additionalProperties":false}`)}}}
		})
		f.successModel(t, 1)
		effects := 0
		e := f.n.effects(nil)
		e.tool = func(ctx context.Context, inv Invocation) ([]byte, error) {
			return f.n.fixture.store.ExternalTool(ctx, inv, func(context.Context, Run) ([]byte, error) {
				effects++
				return nil, errors.New("synthetic external reply lost")
			})
		}
		_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, nativeNotesFrame(2, "mcp_remote_write", map[string]any{}), e)
		if !errors.Is(err, ErrUnknown) {
			t.Fatal("normal external failure did not remain unknown", err)
		}
		nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
		before := f.digest(t)
		v := strings.Repeat("0", 64)
		contextBody := actionRequest(t, f, "GET", f.path()+"/request-context", nil, 200)
		if bytes.Contains(contextBody, []byte("failure_version")) {
			t.Fatal("unresolved external effect received continuation version")
		}
		b := actionPost(t, f, "unknown", v, 409)
		actionReceipt(t, b, "unknown", f.n.run.ID, v, "rejected", "unresolved_tool_effect")
		if f.digest(t) != before || effects != 1 {
			t.Fatal("rejected action altered unresolved effect or retried it")
		}
		actionCounts(t, f, 1, 0)
	})

	f := continuationSetup(t, 1, 3, nil)
	f.failModel(t, 1, 503)
	v := actionVersion(t, f)
	before := actionDigest(t, f)
	// HTTP parsing/auth failures never create business refusal receipts.
	good := actionBody("valid", v)
	invalid := [][]byte{nil, []byte(`null`), []byte(`[]`), []byte(`{}`), []byte(`{"request_id":null,"expected_failure_version":"` + v + `"}`), []byte(`{"request_id":"a","request_id":"b","expected_failure_version":"` + v + `"}`), []byte(`{"request_id":"a","request_\u0069d":"a","expected_failure_version":"` + v + `"}`), []byte(`{"Request_id":"a","expected_failure_version":"` + v + `"}`), []byte(`{"request_id":"a","expected_failure_version":"` + v + `","force":true}`), append(append([]byte(nil), good...), []byte(` {}`)...), append(append([]byte(nil), good...), 0xff), []byte(strings.Repeat(" ", 1025)), actionBody("", v), actionBody("bad/id", v), actionBody(strings.Repeat("x", 129), v), actionBody("a", strings.Repeat("A", 64)), actionBody("a", strings.Repeat("0", 63))}
	for i, b := range invalid {
		r := actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", "POST", f.path(), b)
		if r.err != nil || r.status != 400 {
			t.Fatalf("invalid body %d status=%d err=%v", i, r.status, r.err)
		}
	}
	for _, suffix := range []string{"", "/request-context", "/requests/valid"} {
		method := "GET"
		if suffix == "" {
			method = "POST"
		}
		for _, role := range []string{"worker", "runtime", "toolhub", "knowledge", "futures", "telemetry"} {
			actionCheck(t, actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+role, method, f.path()+suffix, good), 403)
		}
		actionCheck(t, actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, "", method, f.path()+suffix, good), 401)
		for _, query := range []string{"?", "?force=1", "?x=1&x=1", "?="} {
			actionRequest(t, f, method, f.path()+suffix+query, good, 400)
		}
	}
	for _, suffix := range []string{"", "/request-context", "/requests/valid"} {
		for _, role := range []string{"toolhub", "knowledge"} {
			actionCheck(t, actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+role, "GET", "/v1/runs/a%2Fb/continuation"+suffix, nil), 403)
		}
	}
	for _, method := range []string{"PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} {
		actionCheck(t, actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", method, f.path(), good), 403)
	}
	actionRequest(t, f, "POST", "/v1/runs/absent-action/continuation", good, 404)
	actionRequest(t, f, "GET", "/v1/runs/absent-action/continuation/request-context", nil, 404)
	actionRequest(t, f, "GET", f.path()+"/requests/absent", nil, 404)
	legacy := actionHTTP(f.n.fixture.ctx, f.server.Client(), f.server.URL, testToken+"control", "POST", "/v1/runs/"+f.n.run.ID+"/resume", nil)
	if !bytes.Contains(actionCheck(t, legacy, 409), []byte("explicit_continuation_request_required")) {
		t.Fatal("legacy native resume bypass remains available")
	}
	if actionDigest(t, f) != before {
		t.Fatal("invalid/auth/missing/old-resume requests wrote a receipt or state")
	}
	actionCounts(t, f, 0, 0)
	accepted := actionPost(t, f, "valid", v, 202)
	before = actionDigest(t, f)
	// A global request identity cannot be reassigned to another real run.
	other, err := f.n.fixture.store.Submit(f.n.fixture.ctx, ID(), "synthetic second run", nativeSnapshot(), nil)
	nativeNotesNeed(t, err)
	before = actionDigest(t, f)
	actionRequest(t, f, "GET", "/v1/runs/"+other+"/continuation/requests/valid", nil, 404)
	conflict := actionRequest(t, f, "POST", "/v1/runs/"+other+"/continuation", good, 409)
	if !bytes.Contains(conflict, []byte("request_id_conflict")) {
		t.Fatal("global request ID was rebound to a second run")
	}
	if !bytes.Equal(accepted, actionRequest(t, f, "GET", f.path()+"/requests/valid", nil, 200)) || actionDigest(t, f) != before {
		t.Fatal("read receipt/cross-run collision changed durable decisions")
	}
	// Real closed-pool and server-context faults, not fabricated Store errors.
	closed, err := pgxpool.NewWithConfig(f.n.fixture.ctx, f.n.fixture.store.DB.Config().Copy())
	nativeNotesNeed(t, err)
	closed.Close()
	unavailable := NewStore(closed, f.n.fixture.store.Remote, f.n.fixture.store.StorageURL)
	unavailable.Archives = f.trap
	for _, path := range []string{f.path(), f.path() + "/request-context", f.path() + "/requests/valid"} {
		method := "GET"
		if path == f.path() {
			method = "POST"
		}
		req := httptest.NewRequest(method, path, bytes.NewReader(good))
		req.Header.Set("Authorization", "Bearer "+testToken+"control")
		rec := httptest.NewRecorder()
		continuationHandler(unavailable).ServeHTTP(rec, req)
		if rec.Code != 503 || rec.Header().Get("Cache-Control") != "no-store" || !strings.Contains(rec.Body.String(), "continuation action unavailable") || strings.Contains(rec.Body.String(), "closed") || strings.Contains(rec.Body.String(), "SELECT") {
			t.Fatal("unavailable database produced a misleading/leaky response", rec.Code)
		}
	}
	// Exercise the SDK's separate legacy single-token middleware branch. A
	// correct token without a named peer is insufficient, even with a closed DB.
	legacyHandler := (&Server{Store: unavailable}).Handler(servicehttp.Config{Role: "runtime", Token: testToken, Authorize: Authorize})
	for _, path := range []string{f.path(), f.path() + "/request-context", f.path() + "/requests/valid"} {
		method := "GET"
		if path == f.path() {
			method = "POST"
		}
		req := httptest.NewRequest(method, path, bytes.NewReader(good))
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()
		legacyHandler.ServeHTTP(rec, req)
		if rec.Code != 403 || !strings.Contains(rec.Body.String(), "control identity") {
			t.Fatal("unnamed legacy token obtained continuation authority", rec.Code)
		}
	}
	if actionDigest(t, f) != before {
		t.Fatal("unavailable database produced a refusal receipt")
	}
	actionCounts(t, f, 1, 1)
}

func TestContinuationActionReceiptPrivileges(t *testing.T) {
	f := continuationSetup(t, 1, 2, nil)
	f.failModel(t, 1, 503)
	v := actionVersion(t, f)
	actionPost(t, f, "privilege", v, 202)
	var runtimeRole, workerRole string
	nativeNotesNeed(t, f.n.fixture.store.DB.QueryRow(f.n.fixture.ctx, "SELECT current_user").Scan(&runtimeRole))
	nativeNotesNeed(t, f.n.fixture.worker.QueryRow(f.n.fixture.ctx, "SELECT current_user").Scan(&workerRole))
	before := actionDigest(t, f)
	nativeNotesNeed(t, Migrate(f.n.fixture.ctx, f.n.fixture.db, runtimeRole, workerRole))
	nativeNotesNeed(t, Readiness(f.n.fixture.ctx, f.n.fixture.store.DB))
	if actionDigest(t, f) != before {
		t.Fatal("idempotent Owner migration changed durable action evidence")
	}
	for _, tc := range []struct {
		pool    *pgxpool.Pool
		sql     string
		allowed bool
	}{
		{f.n.fixture.store.DB, "SELECT response FROM run_continuation_receipts WHERE request_id='privilege'", true},
		{f.n.fixture.store.DB, "UPDATE run_continuation_receipts SET created_at=created_at WHERE false", false},
		{f.n.fixture.store.DB, "DELETE FROM run_continuation_receipts WHERE false", false},
		{f.n.fixture.worker, "SELECT response FROM run_continuation_receipts WHERE false", false},
		{f.n.fixture.worker, "INSERT INTO run_continuation_receipts SELECT * FROM run_continuation_receipts WHERE false", false},
	} {
		_, err := tc.pool.Exec(f.n.fixture.ctx, tc.sql)
		if (err == nil) != tc.allowed {
			t.Fatal("receipt role privileges differ from append/read Owner contract", tc.sql, err)
		}
	}
	for _, role := range []string{runtimeRole, workerRole} {
		var selectOK, insertOK, updateOK, deleteOK bool
		nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT has_table_privilege($1,'run_continuation_receipts','SELECT'),has_table_privilege($1,'run_continuation_receipts','INSERT'),has_table_privilege($1,'run_continuation_receipts','UPDATE'),has_table_privilege($1,'run_continuation_receipts','DELETE')", role).Scan(&selectOK, &insertOK, &updateOK, &deleteOK))
		owner := role == runtimeRole
		if selectOK != owner || insertOK != owner || updateOK || deleteOK {
			t.Fatal("receipt privileges expanded beyond Runtime SELECT/INSERT")
		}
	}
	if actionDigest(t, f) != before {
		t.Fatal("privilege probes changed receipt history")
	}
}
