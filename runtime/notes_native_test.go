package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests exercise the Owner's native-v4 protocol, not the official Node
// decision loop. Each run gets its own in-memory archive and synthetic receipt;
// NOLOGIN/SET ROLE tests privileges, not separate credential authentication.
type nativeNotesFixture struct {
	store      *Store
	db, worker *pgxpool.Pool
	ctx        context.Context
}

type nativeNotesRun struct {
	fixture    *nativeNotesFixture
	engine     *execution.Engine
	archive    *archiveFixture
	run        Run
	modelCalls atomic.Int32
	toolCalls  atomic.Int32
}

var nativeNotesLostReply = errors.New("native note fixture discarded the reply after LocalTool committed")

func nativeNotesNeed(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func nativeNotesSetup(t *testing.T) *nativeNotesFixture {
	t.Helper()
	db, runtimeRole, workerRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	nativeNotesNeed(t, Migrate(ctx, db, runtimeRole, workerRole))
	app, worker := rolePool(t, db, runtimeRole), rolePool(t, db, workerRole)
	// Also register after the pools so an early failure cancels active queries
	// before pool cleanup waits for their connections to be returned.
	t.Cleanup(cancel)
	for _, item := range []struct {
		pool *pgxpool.Pool
		role string
	}{{app, runtimeRole}, {worker, workerRole}} {
		var identity string
		var super bool
		nativeNotesNeed(t, item.pool.QueryRow(ctx, "SELECT current_user,rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&identity, &super))
		if identity != item.role || super {
			t.Fatal("native note fixture application identity is not restricted")
		}
	}
	s := NewStore(app, nil, "")
	nativeNotesNeed(t, s.ConfigureSubmissionEngine(EngineDSHNative))
	return &nativeNotesFixture{store: s, db: db, worker: worker, ctx: ctx}
}

func (f *nativeNotesFixture) newRun(t *testing.T, scope string) *nativeNotesRun {
	t.Helper()
	snap := nativeSnapshot()
	snap.AgentRef.ID = scope
	snap.Agent.Tools = append(snap.Agent.Tools, "note_revise", "note_retire", "document_read")
	id, err := f.store.Submit(f.ctx, ID(), "Synthetic note lifecycle: retain facts, sources and corrections.", snap, nil)
	nativeNotesNeed(t, err)
	a := &archiveFixture{}
	engine, err := execution.NewSourceVersions(f.worker, a, []string{EngineDSHNative})
	nativeNotesNeed(t, err)
	r, err := engine.Claim(f.ctx, "native-notes-"+ID(), 120*time.Second)
	nativeNotesNeed(t, err)
	if r.ID != id || r.Engine != EngineDSHNative || r.State.PlanRevision != 0 {
		t.Fatal("normal native submission/claim did not preserve the expected initial state")
	}
	n := &nativeNotesRun{fixture: f, engine: engine, archive: a, run: r}
	_, _, err = engine.Exchange(f.ctx, &n.run, nativeNotesModelFrame(), n.effects(nil))
	nativeNotesNeed(t, err)
	if n.modelCalls.Load() != 1 || n.run.State.PlanRevision != n.run.RuleRevision || len(a.bodies) == 0 {
		t.Fatal("synthetic native model receipt did not establish an archived plan")
	}
	var complete int
	nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, "SELECT count(*) FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1 AND a.status='completed'", id).Scan(&complete))
	if complete != 1 {
		t.Fatal("native model attempt was not durably completed")
	}
	return n
}

func nativeNotesModelFrame() execution.Frame {
	return nativeFrame(1, "generation", "Synthetic plan: collect and correct observations; no prediction or trading judgment.")
}

func (n *nativeNotesRun) effects(after func(Invocation, []byte) error) nativeEffects {
	return nativeEffects{
		sourceEffects: sourceEffects{exchangeEffects: exchangeEffects{
			model: func(context.Context, Run, string, []Message, []Tool) (Message, error) {
				return Message{}, errors.New("native note fixture must not use a legacy model callback")
			},
			tool: func(ctx context.Context, inv Invocation) ([]byte, error) {
				n.toolCalls.Add(1)
				body, err := n.fixture.store.LocalTool(ctx, inv)
				if err == nil && after != nil {
					err = after(inv, body)
				}
				if err != nil {
					return nil, err
				}
				return body, nil
			},
		}},
		complete: func(ctx context.Context, r Run, op string, messages []Message, tools []Tool) (json.RawMessage, error) {
			request := execution.NativeModelRequest(r.Snapshot.Agent.Model, messages, tools)
			attempt, cached, err := n.engine.BeginModel(ctx, r, op, request)
			if err != nil {
				return nil, err
			}
			if cached == nil {
				n.modelCalls.Add(1)
				receipt, e := execution.NativeReceipt(200, nativeProviderResult("Synthetic plan established; observations and plans remain separate."))
				if e != nil {
					return nil, e
				}
				err = n.engine.EndModel(ctx, r, op, attempt, receipt, nil)
			}
			return request, err
		},
	}
}

func nativeNotesFrame(seq int, name string, args any) execution.Frame {
	return stateFrame(seq, "tool", JSON(map[string]any{"name": name, "arguments": args, "call_id": fmt.Sprintf("native-note-%d", seq)}))
}

func nativeNotesOperation(r Run, seq int) string { return fmt.Sprintf("%s/dsh/%d", r.ID, seq) }

// Native tool replies are a JSON string containing an envelope whose result is
// itself the original tool JSON string. A known conflict still has a receipt.
func nativeNotesBody(t *testing.T, r Run, seq int, value any) []byte {
	t.Helper()
	text, ok := value.(string)
	if !ok {
		t.Fatal("native tool reply was not its protocol string")
	}
	var envelope struct {
		Result  string `json:"result"`
		Receipt struct {
			OperationID string `json:"operation_id"`
			Committed   bool   `json:"committed"`
		} `json:"receipt"`
	}
	nativeNotesNeed(t, json.Unmarshal([]byte(text), &envelope))
	if !envelope.Receipt.Committed || envelope.Receipt.OperationID != nativeNotesOperation(r, seq) || !json.Valid([]byte(envelope.Result)) {
		t.Fatal("native tool reply lost the committed operation receipt")
	}
	return []byte(envelope.Result)
}

func (n *nativeNotesRun) exchange(t *testing.T, frame execution.Frame) []byte {
	t.Helper()
	v, done, err := n.engine.Exchange(n.fixture.ctx, &n.run, frame, n.effects(nil))
	nativeNotesNeed(t, err)
	if done {
		t.Fatal("tool unexpectedly completed the task")
	}
	return nativeNotesBody(t, n.run, frame.ID, v)
}

type nativeNotesView struct {
	Document   Document      `json:"document"`
	DocumentID string        `json:"document_id"`
	Lifecycle  NoteLifecycle `json:"lifecycle"`
}

func nativeNotesViewOf(t *testing.T, body []byte) nativeNotesView {
	t.Helper()
	var v nativeNotesView
	nativeNotesNeed(t, json.Unmarshal(body, &v))
	return v
}

func nativeNotesConflict(body []byte) bool {
	var result struct {
		Outcome string `json:"outcome"`
		Changed *bool  `json:"changed"`
	}
	return json.Unmarshal(body, &result) == nil && result.Outcome == "conflict" && result.Changed != nil && !*result.Changed
}

func nativeNotesSave(t *testing.T, n *nativeNotesRun) Document {
	t.Helper()
	b := n.exchange(t, nativeNotesFrame(2, "note_save", map[string]any{
		"kind": "fact", "title": "Synthetic observed volume", "content": "Observed 42 units; next month 9 units is a plan, not an actual.",
		"source": "fixture://native-notes/original", "metadata": map[string]any{"synthetic": true, "actual": 42, "planned": 9},
	}))
	var d Document
	nativeNotesNeed(t, json.Unmarshal(b, &d))
	if d.ID == "" || d.Scope != n.run.Snapshot.AgentRef.ID || d.Kind != "fact" || d.Supersedes != nil {
		t.Fatal("new native note identity/scope changed")
	}
	nativeNotesAudit(t, n, 2, d.ID, d.ID, "create", 1)
	return d
}

func nativeNotesRevision(d Document, label string, until *time.Time) NoteRevision {
	return NoteRevision{ExpectedCurrentID: d.ID, Title: "Synthetic observed volume", Content: "Corrected actual 35 units; plan 9 stays separate: " + label,
		Source: "fixture://native-notes/" + label, Metadata: map[string]any{"synthetic": true, "actual": 35, "label": label}, ValidUntil: until, Reason: "Synthetic source correction: " + label}
}

func nativeNotesAudit(t *testing.T, n *nativeNotesRun, seq int, root, document, action string, want int) {
	t.Helper()
	op := nativeNotesOperation(n.run, seq)
	var all, exact, completed, events int
	nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, `SELECT
 (SELECT count(*) FROM note_audits WHERE operation_id=$1),
 (SELECT count(*) FROM note_audits WHERE operation_id=$1 AND run_id=$2 AND root_id=$3 AND document_id=$4 AND action=$5 AND actor_kind='agent' AND actor_id=$6),
 (SELECT count(*) FROM calls WHERE id=$1 AND run_id=$2 AND kind='tool' AND status='completed'),
 (SELECT count(*) FROM events WHERE run_id=$2 AND kind='tool_completed' AND payload->>'operation_id'=$1)`,
		op, n.run.ID, root, document, action, n.run.Snapshot.AgentRef.ID).Scan(&all, &exact, &completed, &events))
	if all != want || exact != want || completed != 1 || events != 1 {
		t.Fatalf("operation %d lifecycle/call/event mismatch: audit=%d exact=%d completed=%d events=%d", seq, all, exact, completed, events)
	}
}

func nativeNotesReceipt(t *testing.T, n *nativeNotesRun, seq int, responseFrozen bool) []byte {
	t.Helper()
	var body, response []byte
	var status string
	nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, "SELECT status,response FROM calls WHERE id=$1 AND run_id=$2 AND kind='tool'", nativeNotesOperation(n.run, seq), n.run.ID).Scan(&status, &body))
	nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, "SELECT response FROM harness_exchanges WHERE run_id=$1 AND seq=$2", n.run.ID, seq).Scan(&response))
	if status != "completed" || (response != nil) != responseFrozen {
		t.Fatal("tool call and harness response were not in the expected durable states")
	}
	return body
}

func nativeNotesRows(t *testing.T, f *nativeNotesFixture) []byte {
	t.Helper()
	// Only reads. No SQL changes to engine, snapshot, lease, run state or notes.
	var value []byte
	nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, `SELECT jsonb_build_object(
 'documents',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM documents x),
 'versions',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY document_id),'[]') FROM note_versions x),
 'roots',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY root_id),'[]') FROM note_roots x),
 'audits',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM note_audits x),
 'calls',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM calls x),
 'events',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM events x),
 'attempts',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM model_attempts x),
 'exchanges',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,seq),'[]') FROM harness_exchanges x),
 'archives',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY run_id,seq),'[]') FROM harness_archives x))`).Scan(&value))
	return value
}

func nativeNotesSame(t *testing.T, before, after []byte, label string) {
	t.Helper()
	if canonicalHash(before) != canonicalHash(after) {
		t.Fatal(label + " changed retained rows or frozen receipt JSON")
	}
}

func nativeNotesDocumentRow(t *testing.T, f *nativeNotesFixture, id string) []byte {
	t.Helper()
	var raw []byte
	nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, "SELECT to_jsonb(d) FROM documents d WHERE id=$1", id).Scan(&raw))
	return raw
}

func nativeNotesWait(t *testing.T, ctx context.Context, ready func() (bool, error)) {
	t.Helper()
	timer := time.NewTimer(12 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, err := ready()
		nativeNotesNeed(t, err)
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("native note fixture wait cancelled", ctx.Err())
		case <-timer.C:
			t.Fatal("native note fixture condition did not become true within 12 seconds")
		case <-tick.C:
		}
	}
}

func nativeNotesEmptySearch(t *testing.T, f *nativeNotesFixture, scope string) {
	t.Helper()
	docs, err := searchNotes(f.ctx, f.store.DB, "Synthetic observed volume", "fact", scope)
	nativeNotesNeed(t, err)
	if len(docs) != 0 {
		t.Fatal("expired/retired lineage revived an older version in search")
	}
}

func TestNativeNoteLifecycleReplyLossExpiryAndRetirement(t *testing.T) {
	f := nativeNotesSetup(t)
	n := f.newRun(t, "native-note-lifecycle")
	original := nativeNotesSave(t, n)
	originalRow := nativeNotesDocumentRow(t, f, original.ID)
	var until time.Time
	nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, "SELECT clock_timestamp()+interval '8 seconds'").Scan(&until))
	change := nativeNotesRevision(original, "expires", &until)
	frame := nativeNotesFrame(3, "note_revise", change)
	_, _, err := n.engine.Exchange(f.ctx, &n.run, frame, n.effects(func(Invocation, []byte) error { return nativeNotesLostReply }))
	if !errors.Is(err, nativeNotesLostReply) {
		t.Fatal("committed revision reply loss was not injected", err)
	}
	frozen := nativeNotesReceipt(t, n, 3, false)
	revised := nativeNotesViewOf(t, frozen)
	if revised.Document.ID == "" || revised.Document.Supersedes == nil || *revised.Document.Supersedes != original.ID || revised.Document.ValidUntil == nil || !revised.Document.ValidUntil.Equal(until) || !revised.Lifecycle.Effective || revised.Lifecycle.Expired || !revised.Lifecycle.IsCurrent {
		t.Fatal("committed revision did not carry a nonempty future expiry and effective receipt")
	}
	if revised.Document.Scope != original.Scope || revised.Document.Kind != original.Kind || revised.Document.Title != change.Title || revised.Document.Content != change.Content || revised.Document.Source != change.Source {
		t.Fatal("committed revision changed scope, kind or supplied source content")
	}
	nativeNotesSame(t, JSON(change.Metadata), JSON(revised.Document.Metadata), "revision metadata")
	nativeNotesAudit(t, n, 3, original.ID, revised.Document.ID, "revise", 1)
	revisedRow := nativeNotesDocumentRow(t, f, revised.Document.ID)
	docs, err := searchNotes(f.ctx, f.store.DB, "Synthetic observed volume", "fact", n.run.Snapshot.AgentRef.ID)
	nativeNotesNeed(t, err)
	if len(docs) != 1 || docs[0].ID != revised.Document.ID {
		t.Fatal("future-valid revision was not observed effective before expiry")
	}
	retained := nativeNotesRows(t, f)
	nativeNotesWait(t, f.ctx, func() (bool, error) {
		var expired bool
		err := f.store.DB.QueryRow(f.ctx, "SELECT clock_timestamp()>valid_until FROM documents WHERE id=$1", revised.Document.ID).Scan(&expired)
		return expired, err
	})
	nativeNotesSame(t, retained, nativeNotesRows(t, f), "natural expiry")
	nativeNotesEmptySearch(t, f, n.run.Snapshot.AgentRef.ID)
	current, err := noteLifecycle(f.ctx, f.store.DB, revised.Document.ID)
	nativeNotesNeed(t, err)
	if !current.Expired || current.Effective || !current.IsCurrent || current.State != "active" {
		t.Fatal("natural expiry changed history instead of the current effective view")
	}

	old := n.run
	nativeNotesNeed(t, n.engine.Heartbeat(f.ctx, old, 250*time.Millisecond))
	nativeNotesWait(t, f.ctx, func() (bool, error) {
		var expired bool
		err := f.store.DB.QueryRow(f.ctx, "SELECT lease_until<clock_timestamp() FROM runs WHERE id=$1", old.ID).Scan(&expired)
		return expired, err
	})
	next, err := n.engine.Claim(f.ctx, "native-notes-successor-"+ID(), 120*time.Second)
	nativeNotesNeed(t, err)
	if next.ID != old.ID || next.Fence != old.Fence+1 || next.Owner == old.Owner {
		t.Fatal("natural lease expiry did not claim the same native task with a new fence")
	}
	if _, _, err = n.engine.Exchange(f.ctx, &old, frame, n.effects(nil)); !errors.Is(err, ErrFence) {
		t.Fatal("old owner was allowed to replay the tool", err)
	}
	n.run = next
	_, _, err = n.engine.Exchange(f.ctx, &n.run, nativeNotesModelFrame(), n.effects(nil))
	nativeNotesNeed(t, err)
	if n.modelCalls.Load() != 1 || n.run.State.PlanRevision != n.run.RuleRevision {
		t.Fatal("native model replay repeated a completion or lost the frozen plan")
	}
	calls := n.toolCalls.Load()
	recovered := n.exchange(t, frame)
	nativeNotesSame(t, frozen, recovered, "revision recovery after natural expiry")
	if n.toolCalls.Load() != calls || !nativeNotesViewOf(t, recovered).Lifecycle.Effective {
		t.Fatal("revision recovery refreshed the historical receipt or repeated its effect")
	}
	nativeNotesReceipt(t, n, 3, true)
	fresh := nativeNotesViewOf(t, n.exchange(t, nativeNotesFrame(4, "document_read", map[string]any{"id": revised.Document.ID})))
	if !fresh.Lifecycle.Expired || fresh.Lifecycle.Effective || fresh.Document.Content != change.Content || fresh.Document.Source != change.Source || fresh.Document.ValidUntil == nil || !fresh.Document.ValidUntil.Equal(until) {
		t.Fatal("fresh native read lost expired status or immutable revision content")
	}
	tampered := nativeNotesFrame(3, "note_revise", nativeNotesRevision(original, "different-input", &until))
	beforeTamper := nativeNotesRows(t, f)
	if _, _, err = n.engine.Exchange(f.ctx, &n.run, tampered, n.effects(nil)); err == nil || !strings.Contains(err.Error(), "replay diverged") {
		t.Fatal("changed replay input was not rejected", err)
	}
	nativeNotesSame(t, beforeTamper, nativeNotesRows(t, f), "divergent replay rejection")

	var renewalUntil time.Time
	nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, "SELECT clock_timestamp()+interval '1 hour'").Scan(&renewalUntil))
	renewal := nativeNotesViewOf(t, n.exchange(t, nativeNotesFrame(5, "note_revise", nativeNotesRevision(revised.Document, "explicit-renewal", &renewalUntil))))
	if !renewal.Lifecycle.Effective || renewal.Lifecycle.Expired || renewal.Document.Supersedes == nil || *renewal.Document.Supersedes != revised.Document.ID || renewal.Document.ValidUntil == nil || !renewal.Document.ValidUntil.Equal(renewalUntil) {
		t.Fatal("explicit renewal did not append the new valid head")
	}
	nativeNotesAudit(t, n, 5, original.ID, renewal.Document.ID, "revise", 1)
	retire := nativeNotesFrame(6, "note_retire", NoteRetirement{ExpectedCurrentID: renewal.Document.ID, Reason: "Synthetic source withdrawn explicitly"})
	_, _, err = n.engine.Exchange(f.ctx, &n.run, retire, n.effects(func(Invocation, []byte) error { return nativeNotesLostReply }))
	if !errors.Is(err, nativeNotesLostReply) {
		t.Fatal("committed retirement reply loss was not injected", err)
	}
	retiredReceipt := nativeNotesReceipt(t, n, 6, false)
	calls = n.toolCalls.Load()
	nativeNotesSame(t, retiredReceipt, n.exchange(t, retire), "retirement recovery")
	if n.toolCalls.Load() != calls {
		t.Fatal("retirement recovery repeated its effect")
	}
	nativeNotesAudit(t, n, 6, original.ID, renewal.Document.ID, "retire", 1)
	retired := nativeNotesViewOf(t, n.exchange(t, nativeNotesFrame(7, "document_read", map[string]any{"id": renewal.Document.ID})))
	if retired.Lifecycle.State != "retired" || retired.Lifecycle.RetiredAt == nil || retired.Lifecycle.Reason != "Synthetic source withdrawn explicitly" || retired.Lifecycle.Effective || !retired.Lifecycle.IsCurrent {
		t.Fatal("fresh native read lost explicit retirement metadata")
	}
	conflict := n.exchange(t, nativeNotesFrame(8, "note_revise", nativeNotesRevision(renewal.Document, "must-not-revive", nil)))
	if !nativeNotesConflict(conflict) {
		t.Fatal("revision of a retired head did not return a known completed conflict")
	}
	nativeNotesAudit(t, n, 8, original.ID, renewal.Document.ID, "revise", 0)
	historical := nativeNotesViewOf(t, n.exchange(t, nativeNotesFrame(9, "document_read", map[string]any{"id": original.ID})))
	if historical.Document.Content != original.Content || historical.Document.Source != original.Source || historical.Document.Scope != original.Scope || historical.Lifecycle.IsCurrent || historical.Lifecycle.Effective {
		t.Fatal("historical native read lost the original source or revived its version")
	}
	nativeNotesSame(t, originalRow, nativeNotesDocumentRow(t, f, original.ID), "original document")
	nativeNotesSame(t, revisedRow, nativeNotesDocumentRow(t, f, revised.Document.ID), "expired revision document")
	nativeNotesSame(t, frozen, nativeNotesReceipt(t, n, 3, true), "original revision receipt")
	nativeNotesEmptySearch(t, f, n.run.Snapshot.AgentRef.ID)
	var documents, versions, audits, pending, lineage int
	nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM note_versions),(SELECT count(*) FROM note_audits),(SELECT count(*) FROM calls WHERE status<>'completed'),(SELECT count(*) FROM note_versions WHERE root_id=$1 AND document_id=ANY($2))`, original.ID, []string{original.ID, revised.Document.ID, renewal.Document.ID}).Scan(&documents, &versions, &audits, &pending, &lineage))
	if documents != 3 || versions != 3 || audits != 4 || pending != 0 || lineage != 3 {
		t.Fatal("lifecycle duplicated effects or left an unknown call", documents, versions, audits, pending, lineage)
	}
	t.Logf("future expiry %s observed naturally; fence %d -> %d; immutable history and two lost-reply recoveries retained; synthetic model completions=%d", until.Format(time.RFC3339Nano), old.Fence, n.run.Fence, n.modelCalls.Load())
}

func TestNativeNoteConcurrentHeadConflicts(t *testing.T) {
	for _, mode := range []string{"revise-revise", "revise-retire"} {
		t.Run(mode, func(t *testing.T) {
			f := nativeNotesSetup(t)
			a, b := f.newRun(t, "native-notes-shared"), f.newRun(t, "native-notes-shared")
			original := nativeNotesSave(t, a)
			originalRow := nativeNotesDocumentRow(t, f, original.ID)
			fa := nativeNotesFrame(3, "note_revise", nativeNotesRevision(original, "writer-a", nil))
			fb := nativeNotesFrame(2, "note_revise", nativeNotesRevision(original, "writer-b", nil))
			if mode == "revise-retire" {
				fb = nativeNotesFrame(2, "note_retire", NoteRetirement{ExpectedCurrentID: original.ID, Reason: "Concurrent explicit withdrawal"})
			}
			gate, err := f.db.Begin(f.ctx)
			nativeNotesNeed(t, err)
			defer gate.Rollback(context.Background())
			_, err = gate.Exec(f.ctx, "SELECT 1 FROM note_roots WHERE root_id=$1 FOR UPDATE", original.ID)
			nativeNotesNeed(t, err)
			var gatePID int
			nativeNotesNeed(t, gate.QueryRow(f.ctx, "SELECT pg_backend_pid()").Scan(&gatePID))
			type result struct {
				run   *nativeNotesRun
				frame execution.Frame
				value any
				done  bool
				err   error
			}
			results := make(chan result, 2)
			for _, writer := range []struct {
				run   *nativeNotesRun
				frame execution.Frame
			}{{a, fa}, {b, fb}} {
				go func(n *nativeNotesRun, frame execution.Frame) {
					v, done, e := n.engine.Exchange(f.ctx, &n.run, frame, n.effects(nil))
					results <- result{n, frame, v, done, e}
				}(writer.run, writer.frame)
			}
			waiting := 0
			nativeNotesWait(t, f.ctx, func() (bool, error) {
				if _, e := gate.Exec(f.ctx, "SELECT pg_stat_clear_snapshot()"); e != nil {
					return false, e
				}
				// Include indirect blockers: the second writer may wait behind
				// the first writer's tuple-lock queue, which itself waits for us.
				e := gate.QueryRow(f.ctx, `WITH RECURSIVE blocked(pid,path) AS (
 SELECT a.pid,ARRAY[a.pid] FROM pg_stat_activity a
 WHERE a.datname=current_database() AND a.wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(a.pid))
 UNION ALL
 SELECT a.pid,b.path||a.pid FROM pg_stat_activity a JOIN blocked b ON b.pid=ANY(pg_blocking_pids(a.pid))
 WHERE a.datname=current_database() AND a.wait_event_type='Lock' AND NOT a.pid=ANY(b.path)
) SELECT count(DISTINCT pid) FROM blocked`, gatePID).Scan(&waiting)
				return waiting >= 2, e
			})
			nativeNotesNeed(t, gate.Commit(f.ctx))
			var winner, loser result
			var winnerBody []byte
			wins := 0
			for i := 0; i < 2; i++ {
				var got result
				select {
				case got = <-results:
				case <-f.ctx.Done():
					t.Fatal("native note competitors did not complete", f.ctx.Err())
				}
				nativeNotesNeed(t, got.err)
				if got.done {
					t.Fatal("head conflict unexpectedly completed its task")
				}
				body := nativeNotesBody(t, got.run.run, got.frame.ID, got.value)
				if nativeNotesConflict(body) {
					loser = got
				} else {
					wins++
					winner, winnerBody = got, body
				}
			}
			if wins != 1 || loser.run == nil || a.modelCalls.Load() != 1 || b.modelCalls.Load() != 1 || a.toolCalls.Load() != 2 || b.toolCalls.Load() != 1 {
				t.Fatal("head contention did not produce one effect and one completed conflict")
			}
			view := nativeNotesViewOf(t, winnerBody)
			currentID, state, action := view.Document.ID, "active", "revise"
			wantDocuments := 2
			if view.DocumentID != "" {
				currentID, state, action, wantDocuments = view.DocumentID, "retired", "retire", 1
			}
			if currentID == "" || view.Lifecycle.RootID != original.ID || view.Lifecycle.State != state || view.Lifecycle.CurrentID == nil || *view.Lifecycle.CurrentID != currentID {
				t.Fatal("winning receipt lost its lineage/head state")
			}
			if action == "revise" && (view.Document.Supersedes == nil || *view.Document.Supersedes != original.ID || view.Document.Scope != original.Scope || view.Document.Kind != original.Kind) {
				t.Fatal("concurrent revision changed scope/kind or branched")
			}
			nativeNotesAudit(t, winner.run, winner.frame.ID, original.ID, currentID, action, 1)
			nativeNotesAudit(t, loser.run, loser.frame.ID, original.ID, currentID, action, 0)
			var documents, versions, audits, pending int
			nativeNotesNeed(t, f.store.DB.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM note_versions),(SELECT count(*) FROM note_audits),(SELECT count(*) FROM calls WHERE status<>'completed')`).Scan(&documents, &versions, &audits, &pending))
			if documents != wantDocuments || versions != wantDocuments || audits != 2 || pending != 0 {
				t.Fatal("concurrent head effects forked or conflict became unknown", documents, versions, audits, pending)
			}
			actual, err := noteLifecycle(f.ctx, f.store.DB, currentID)
			nativeNotesNeed(t, err)
			if actual.RootID != original.ID || actual.State != state || !actual.IsCurrent {
				t.Fatal("database lineage differs from the winning receipt")
			}
			nativeNotesSame(t, originalRow, nativeNotesDocumentRow(t, f, original.ID), "concurrent original document")
			before := nativeNotesRows(t, f)
			for _, got := range []result{winner, loser} {
				first := nativeNotesReceipt(t, got.run, got.frame.ID, true)
				nativeNotesSame(t, first, got.run.exchange(t, got.frame), "completed native competitor replay")
			}
			if a.toolCalls.Load() != 2 || b.toolCalls.Load() != 1 {
				t.Fatal("native replay re-invoked a winner or conflict")
			}
			nativeNotesSame(t, before, nativeNotesRows(t, f), "competitor replays")
			t.Logf("%s: observed %d database lock waiters; one %s effect, one committed conflict; independent archives and all receipts replayed without mutation", mode, waiting, action)
		})
	}
}
