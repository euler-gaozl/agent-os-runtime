package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func noteRun(t *testing.T, s *Store, agent string) Run {
	t.Helper()
	snap := basicSnapshot("https://example.invalid")
	snap.AgentRef.ID = agent
	snap.Agent.Tools = append(snap.Agent.Tools, "note_revise", "note_retire", "document_read")
	submit(t, s, snap)
	return claim(t, s, "notes-"+ID())
}
func noteInv(r Run, name, op string, input any) Invocation {
	return Invocation{RunID: r.ID, Owner: r.Owner, Fence: r.Fence, RuleRevision: r.RuleRevision, OperationID: r.ID + "/" + op, Name: name, Arguments: JSON(input)}
}
func savedNote(t *testing.T, s *Store, r Run) Document {
	t.Helper()
	b, e := s.LocalTool(context.Background(), noteInv(r, "note_save", "save", map[string]any{"kind": "fact", "title": "实绩记录", "content": "初值 42；下月 9 为计划", "source": "fixture://notes", "metadata": map[string]any{"reviewed": true}}))
	var d Document
	if e != nil || json.Unmarshal(b, &d) != nil {
		t.Fatal(e, string(b))
	}
	return d
}
func revision(d Document) NoteRevision {
	return NoteRevision{ExpectedCurrentID: d.ID, Title: d.Title, Content: "实绩修订为 35；下月 9 仍为计划", Source: "fixture://correction", Metadata: map[string]any{"actual": 35}, Reason: "来源更正原始发布数值"}
}
func jsonEqual(a, b []byte) bool {
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && string(JSON(x)) == string(JSON(y))
}
func noteConflict(b []byte) bool {
	var v struct{ Outcome string }
	return json.Unmarshal(b, &v) == nil && v.Outcome == "conflict"
}
func attemptNote(s *Store, inv Invocation) error {
	b, e := s.LocalTool(context.Background(), inv)
	if e == nil && noteConflict(b) {
		return errors.New("active head conflict")
	}
	return e
}

func TestNoteRevisionRetirementAtomicReplayAndHistoricalRead(t *testing.T) {
	s := isolatedDomainDatabase(t, "runtime")
	ctx := context.Background()
	r := noteRun(t, s, "same-agent")
	original := savedNote(t, s, r)
	read := noteInv(r, "document_read", "read-before", map[string]any{"id": original.ID, "offset": 2, "limit": 4})
	before, e := s.LocalTool(ctx, read)
	if e != nil {
		t.Fatal(e)
	}
	in := revision(original)
	inv := noteInv(r, "note_revise", "correct", in)
	// Fault after DB trigger, audit and receipt, before transaction commit.
	_, e = s.DB.Exec(ctx, `CREATE FUNCTION reject_note_commit() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.kind='tool_completed' THEN RAISE EXCEPTION 'injected before note commit'; END IF;RETURN NEW;END $$;
 CREATE TRIGGER reject_note_commit BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION reject_note_commit()`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.LocalTool(ctx, inv); e == nil {
		t.Fatal("missing injected failure")
	}
	var docs, audits, receipts int
	if e = s.DB.QueryRow(ctx, "SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM note_audits),(SELECT count(*) FROM calls)").Scan(&docs, &audits, &receipts); e != nil || docs != 1 || audits != 1 || receipts != 2 {
		t.Fatal("partial commit", docs, audits, receipts, e)
	}
	if _, e = s.DB.Exec(ctx, "DROP TRIGGER reject_note_commit ON events"); e != nil {
		t.Fatal(e)
	}
	first, e := s.LocalTool(ctx, inv)
	if e != nil {
		t.Fatal(e)
	}
	var changed struct {
		Document  Document
		Lifecycle NoteLifecycle
	}
	if e = json.Unmarshal(first, &changed); e != nil || changed.Document.ID == original.ID || !changed.Lifecycle.Effective {
		t.Fatal(e, string(first))
	}
	// Commit succeeds but response is lost, then the lease expires.
	if _, e = s.DB.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", r.ID); e != nil {
		t.Fatal(e)
	}
	next := claim(t, s, "replacement")
	if _, e = s.LocalTool(ctx, inv); e == nil {
		t.Fatal("stale fence accepted")
	}
	inv.Owner, inv.Fence = next.Owner, next.Fence
	replay, e := s.LocalTool(ctx, inv)
	if e != nil || !jsonEqual(first, replay) {
		t.Fatal("revision replay changed", e)
	}
	inv.Arguments = JSON(revision(changed.Document))
	if _, e = s.LocalTool(ctx, inv); e == nil {
		t.Fatal("changed invocation accepted")
	}
	read.Owner, read.Fence = next.Owner, next.Fence
	replay, e = s.LocalTool(ctx, read)
	if e != nil || !jsonEqual(before, replay) {
		t.Fatal("historical receipt mutated", e)
	}
	read.OperationID += "-fresh"
	fresh, e := s.LocalTool(ctx, read)
	if e != nil {
		t.Fatal(e)
	}
	var history struct {
		Document   Document
		Lifecycle  NoteLifecycle
		NextOffset int `json:"next_offset"`
		Total      int `json:"total_characters"`
	}
	if e = json.Unmarshal(fresh, &history); e != nil || history.Lifecycle.IsCurrent || history.Lifecycle.Effective || history.Document.Content != string([]rune(original.Content)[2:6]) || history.NextOffset != 6 || history.Total != len([]rune(original.Content)) {
		t.Fatal("historical page or status", e, string(fresh))
	}
	retire := noteInv(next, "note_retire", "withdraw", NoteRetirement{changed.Document.ID, "来源宣布撤回该项数据"})
	removed, e := s.LocalTool(ctx, retire)
	if e != nil {
		t.Fatal(e)
	}
	replay, e = s.LocalTool(ctx, retire)
	if e != nil || !jsonEqual(removed, replay) {
		t.Fatal("withdraw replay", e)
	}
	if found, e := s.Search(ctx, "", "fact"); e != nil || len(found) != 0 {
		t.Fatal("withdrawn predecessor resurfaced", e, found)
	}
	if b, e := s.LocalTool(ctx, noteInv(next, "note_revise", "revive", revision(changed.Document))); e != nil || !noteConflict(b) {
		t.Fatal("implicitly revived withdrawn note")
	}
	if e = migrateHistoricalFixture(ctx, s); e != nil {
		t.Fatal(e)
	}
	state, e := noteLifecycle(ctx, s.DB, changed.Document.ID)
	if e != nil || state.State != "retired" {
		t.Fatal("migration reset retirement", e, state)
	}
	if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM note_audits WHERE actor_kind='agent' AND actor_id='same-agent' AND run_id=$1 AND operation_id IS NOT NULL", r.ID).Scan(&audits); e != nil || audits != 3 {
		t.Fatal("audit duplicated or actor forged", audits, e)
	}
}

func TestNoteConcurrentWritersShareHeadLock(t *testing.T) {
	for _, mode := range []string{"revise-revise", "revise-retire", "admin-revise"} {
		t.Run(mode, func(t *testing.T) {
			s := isolatedDomainDatabase(t, "runtime")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			a, b := noteRun(t, s, "same"), noteRun(t, s, "same")
			d := savedNote(t, s, a)
			tx, e := s.DB.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback(ctx)
			if _, e = tx.Exec(ctx, "SELECT 1 FROM note_roots WHERE root_id=$1 FOR UPDATE", d.ID); e != nil {
				t.Fatal(e)
			}
			results := make(chan error, 2)
			go func() { results <- attemptNote(s, noteInv(a, "note_revise", "a", revision(d))) }()
			go func() {
				var e error
				switch mode {
				case "revise-retire":
					e = attemptNote(s, noteInv(b, "note_retire", "b", NoteRetirement{d.ID, "withdrawal fixture"}))
				case "admin-revise":
					copy := d
					copy.ID = ""
					copy.Supersedes = &d.ID
					copy.Content = "management correction"
					_, e = s.putAdminDocument(ctx, copy, "control")
				default:
					e = attemptNote(s, noteInv(b, "note_revise", "b", revision(d)))
				}
				results <- e
			}()
			// Both writers must actually wait behind the same lineage lock.
			deadline := time.Now().Add(3 * time.Second)
			waiting := 0
			for time.Now().Before(deadline) {
				// The fixture itself holds the lineage lock while two writers use
				// the remaining connections. Observe on that existing connection,
				// rather than requiring an unavailable fourth pool slot.
				if _, e = tx.Exec(ctx, "SELECT pg_stat_clear_snapshot()"); e != nil {
					t.Fatal(e)
				}
				if e = tx.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()`).Scan(&waiting); e != nil {
					t.Fatal(e)
				}
				if waiting >= 2 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if waiting < 2 {
				t.Fatal("writers did not reach conflicting lock", waiting)
			}
			if e = tx.Commit(ctx); e != nil {
				t.Fatal(e)
			}
			wins := 0
			for i := 0; i < 2; i++ {
				if e := <-results; e == nil {
					wins++
				} else if !strings.Contains(e.Error(), "active head") {
					t.Fatal("unexpected concurrency failure", e)
				}
			}
			if wins != 1 {
				t.Fatal("not exactly one winner", wins)
			}
			var successors, audits int
			if e = s.DB.QueryRow(ctx, "SELECT (SELECT count(*) FROM documents WHERE supersedes=$1),(SELECT count(*) FROM note_audits WHERE action IN ('revise','retire'))", d.ID).Scan(&successors, &audits); e != nil || successors > 1 || audits != 1 {
				t.Fatal("fork or duplicate audit", successors, audits, e)
			}
		})
	}
}

func TestNoteScopeExpiryAndAdminIdempotency(t *testing.T) {
	s := isolatedDomainDatabase(t, "runtime")
	ctx := context.Background()
	a, b := noteRun(t, s, "a"), noteRun(t, s, "b")
	d := savedNote(t, s, a)
	global, e := s.PutDocument(ctx, Document{Kind: "fact", Title: "global", Content: "readable shared evidence", Source: "fixture://global"})
	if e != nil {
		t.Fatal(e)
	}
	for _, target := range []Document{d, global} {
		for _, name := range []string{"note_revise", "note_retire"} {
			var input any = revision(target)
			if name == "note_retire" {
				input = NoteRetirement{target.ID, "attempt"}
			}
			if _, e = s.LocalTool(ctx, noteInv(b, name, name+target.ID, input)); e == nil {
				t.Fatal("unauthorized mutation", name, target.Scope)
			}
		}
	}
	// A descendant relationship is irrelevant to write scope.
	if _, e = s.DB.Exec(ctx, "UPDATE runs SET parent_id=$1 WHERE id=$2", b.ID, a.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.LocalTool(ctx, noteInv(b, "note_retire", "child-scope", NoteRetirement{d.ID, "attempt"})); e == nil {
		t.Fatal("parent acquired child write permission")
	}
	bad := revision(d)
	bad.Metadata = map[string]any{"large": strings.Repeat("x", 33<<10)}
	if _, e = s.LocalTool(ctx, noteInv(a, "note_revise", "oversized", bad)); e == nil {
		t.Fatal("unbounded metadata")
	}
	expired := revision(d)
	past := time.Now().Add(-time.Hour)
	expired.ValidUntil = &past
	out, e := s.LocalTool(ctx, noteInv(a, "note_revise", "expires", expired))
	if e != nil {
		t.Fatal(e)
	}
	var changed struct{ Document Document }
	if e = json.Unmarshal(out, &changed); e != nil {
		t.Fatal(e)
	}
	found, e := searchNotes(ctx, s.DB, "实绩", "fact", "a")
	if e != nil || len(found) != 0 {
		t.Fatal("expired successor restored predecessor", e)
	}
	// Expiry is not withdrawal; explicit correction may create a new current note.
	if _, e = s.LocalTool(ctx, noteInv(a, "note_revise", "renew", revision(changed.Document))); e != nil {
		t.Fatal(e)
	}
	input := JSON(revision(global))
	first, e := s.AdminNoteMutation(ctx, "admin-request", "revise", "control", input)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.AdminNoteMutation(ctx, "admin-request", "revise", "control", input)
	if e != nil || !jsonEqual(first, again) {
		t.Fatal("admin reply-loss replay", e)
	}
	if _, e = s.AdminNoteMutation(ctx, "admin-request", "retire", "control", JSON(NoteRetirement{global.ID, "changed"})); e == nil {
		t.Fatal("admin request collision accepted")
	}
	var actor string
	if e = s.DB.QueryRow(ctx, "SELECT actor_id FROM note_audits WHERE action='revise' AND root_id=$1", global.ID).Scan(&actor); e != nil || actor != "control" {
		t.Fatal("administrator provenance", e, actor)
	}
}

func TestNoteMigrationPreservesHistoricalForkAndReceipts(t *testing.T) {
	s := isolatedDomainDatabase(t, "runtime")
	ctx := context.Background()
	r := noteRun(t, s, "historical")
	d := savedNote(t, s, r)
	// Construct exactly the old schema's permitted fork before first lifecycle migration.
	if _, e := s.DB.Exec(ctx, "DROP TRIGGER note_version_insert ON documents; DROP TABLE note_audits,note_versions,note_roots,note_admin_receipts"); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		tx, e := s.DB.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		copy := d
		copy.ID = ""
		copy.Supersedes = &d.ID
		copy.Content = fmt.Sprint("legacy branch ", i)
		if _, e = putDocument(ctx, tx, copy); e != nil {
			tx.Rollback(ctx)
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
	}
	fingerprint := func() string {
		t.Helper()
		var v string
		if e := s.DB.QueryRow(ctx, `SELECT md5((SELECT jsonb_agg(to_jsonb(d) ORDER BY id)::text FROM documents d)||(SELECT jsonb_agg(to_jsonb(c) ORDER BY id)::text FROM calls c))`).Scan(&v); e != nil {
			t.Fatal(e)
		}
		return v
	}
	before := fingerprint()
	if e := migrateHistoricalFixture(ctx, s); e != nil {
		t.Fatal(e)
	}
	if e := migrateHistoricalFixture(ctx, s); e != nil {
		t.Fatal(e)
	}
	if after := fingerprint(); after != before {
		t.Fatal("migration rewrote historical content or receipts")
	}
	state, e := noteLifecycle(ctx, s.DB, d.ID)
	if e != nil || state.State != "conflict" || state.CurrentID != nil || state.Effective {
		t.Fatal("fork hidden", e, state)
	}
	if found, e := s.Search(ctx, "", "fact"); e != nil || len(found) != 0 {
		t.Fatal("conflicted branches exposed as effective", e, found)
	}
	var leaf string
	if e = s.DB.QueryRow(ctx, "SELECT id FROM documents WHERE supersedes=$1 LIMIT 1", d.ID).Scan(&leaf); e != nil {
		t.Fatal(e)
	}
	if b, e := s.LocalTool(ctx, noteInv(r, "note_retire", "retire-conflict", NoteRetirement{leaf, "choose branch"})); e != nil || !noteConflict(b) {
		t.Fatal("silently resolved conflict")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	copy := d
	copy.ID = ""
	copy.Supersedes = &leaf
	if _, e = putDocument(ctx, tx, copy); e == nil {
		t.Fatal("generic insertion bypassed runtime lineage protection")
	}
}
