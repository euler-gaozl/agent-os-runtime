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
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// This is an Owner/native-v4 protocol test with real PostgreSQL and HTTP auth.
// The model and archive are synthetic in-process fixtures, not a Node replay.
type nativeStateRecoveryFixture struct {
	n           *nativeNotesRun
	server      *httptest.Server
	stateCalls  int
	droppedBody []byte
}

func nativeStateRecoverySetup(t *testing.T) *nativeStateRecoveryFixture {
	t.Helper()
	f := nativeNotesSetup(t)
	n := f.newRun(t, "native-source-recovery-"+ID())
	peers := map[string]string{}
	for _, role := range []string{"control", "worker", "toolhub", "futures", "knowledge"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	server := httptest.NewServer((&Server{Store: f.store}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize}))
	t.Cleanup(server.Close)
	return &nativeStateRecoveryFixture{n: n, server: server}
}

func (f *nativeStateRecoveryFixture) http(t *testing.T, role, method, path string, input []byte, want int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(f.n.fixture.ctx, method, f.server.URL+path, bytes.NewReader(input))
	nativeNotesNeed(t, err)
	req.Header.Set("Authorization", "Bearer "+testToken+role)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.server.Client().Do(req)
	nativeNotesNeed(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	nativeNotesNeed(t, err)
	if len(body) > 2<<20 || resp.StatusCode != want {
		t.Fatalf("native state HTTP %s %s as %s: got status %d, want %d", method, path, role, resp.StatusCode, want)
	}
	return body
}

func (f *nativeStateRecoveryFixture) path(suffix string) string {
	return "/v1/runs/" + f.n.run.ID + suffix
}

func (f *nativeStateRecoveryFixture) preview(t *testing.T, allowed bool, reason string) {
	t.Helper()
	var v SourceRecovery
	nativeNotesNeed(t, json.Unmarshal(f.http(t, "control", "GET", f.path("/source-recovery"), nil, 200), &v))
	if v.RunID != f.n.run.ID || v.Allowed != allowed || v.Reason != reason {
		t.Fatalf("unexpected native recovery preview: %+v", v)
	}
}

func (f *nativeStateRecoveryFixture) effects(t *testing.T, loseReply bool) nativeEffects {
	t.Helper()
	e := f.n.effects(nil)
	e.state = func(_ context.Context, inv Invocation) ([]byte, error) {
		attempts := 1
		if loseReply {
			attempts = 3
		}
		for i := 0; i < attempts; i++ {
			f.stateCalls++
			body := f.http(t, "worker", "POST", "/v1/source-state", JSON(inv), 200)
			if !loseReply {
				return body, nil
			}
			// Discard the successful callback result after the real HTTP handler
			// commits, for all three identical attempts. This models the terminal
			// uncertainty classification of Worker.SourceState, not a TCP fault.
			if i > 0 && !jsonEqual(f.droppedBody, body) {
				t.Fatal("identical source-state retries changed the committed response")
			}
			f.droppedBody = append([]byte(nil), body...)
		}
		return nil, fmt.Errorf("%w: fixture discarded all three committed source-state replies", ErrUnknown)
	}
	return e
}

func (f *nativeStateRecoveryFixture) checkpointUnknown(t *testing.T, frame execution.Frame, effects nativeEffects) {
	t.Helper()
	_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, frame, effects)
	if !errors.Is(err, ErrUnknown) {
		t.Fatal("expected an uncertain effect result before normal checkpoint", err)
	}
	nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
	r, err := f.n.fixture.store.Run(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if r.Status != "waiting_unknown" || r.CancelRequested || r.Owner != f.n.run.Owner || r.Fence != f.n.run.Fence {
		t.Fatal("normal checkpoint did not retain the unknown native run")
	}
}

// These immutable rows must survive management actions and receipt replay.
// Queries only observe state: the tests never SQL-update engine/status/fence.
func (f *nativeStateRecoveryFixture) ledger(t *testing.T) []byte {
	t.Helper()
	var body []byte
	nativeNotesNeed(t, f.n.fixture.store.DB.QueryRow(f.n.fixture.ctx, `SELECT jsonb_build_object(
 'calls',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM calls x WHERE run_id=$1),
 'attempts',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY id),'[]') FROM model_attempts x WHERE call_id IN(SELECT id FROM calls WHERE run_id=$1)),
 'exchanges',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM harness_exchanges x WHERE run_id=$1),
 'archives',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY seq),'[]') FROM harness_archives x WHERE run_id=$1),
 'states',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY revision),'[]') FROM run_working_states x WHERE run_id=$1))`, f.n.run.ID).Scan(&body))
	return body
}

func (f *nativeStateRecoveryFixture) resumed(t *testing.T, want int, method string) {
	t.Helper()
	var total, matching int
	nativeNotesNeed(t, f.n.fixture.store.DB.QueryRow(f.n.fixture.ctx, `SELECT count(*),count(*) FILTER(WHERE payload->>'method'=$2)
 FROM events WHERE run_id=$1 AND kind='source_state_resumed'`, f.n.run.ID, method).Scan(&total, &matching))
	if total != want || matching != want {
		t.Fatalf("native resume audit mismatch: total=%d matching=%d want=%d", total, matching, want)
	}
}

func TestNativeSourceStateHTTPRecovery(t *testing.T) {
	for _, method := range []string{"working_state_read", "working_state_update"} {
		t.Run(method, func(t *testing.T) {
			f := nativeStateRecoverySetup(t)
			n := f.n
			nativeNotesSave(t, n) // Completed operation /dsh/2 supplies real evidence.
			source := Invocation{OperationID: nativeNotesOperation(n.run, 2)}
			first := stateFrame(3, "working_state_update", workingInput(n.run, source, "unused", 0).Arguments)
			_, _, err := n.engine.Exchange(n.fixture.ctx, &n.run, first, f.effects(t, false))
			nativeNotesNeed(t, err)
			target := stateFrame(4, method, []byte(`{}`))
			wantRevision := int64(1)
			if method == "working_state_update" {
				target.Params = workingInput(n.run, source, "unused", 1).Arguments
				wantRevision = 2
			}
			f.checkpointUnknown(t, target, f.effects(t, true))
			if f.stateCalls != 4 {
				t.Fatal("expected one initial write and three discarded target replies")
			}
			f.preview(t, true, "ready")
			before := f.ledger(t)
			// All four peer identities are denied by the real policy for both
			// preview and mutation, despite knowing a recoverable run ID.
			allBefore := nativeNotesRows(t, n.fixture)
			for _, role := range []string{"worker", "toolhub", "futures", "knowledge"} {
				f.http(t, role, "GET", f.path("/source-recovery"), nil, 403)
				f.http(t, role, "POST", f.path("/resume-source-state"), nil, 403)
			}
			if !jsonEqual(allBefore, nativeNotesRows(t, n.fixture)) {
				t.Fatal("denied peer requests mutated the ledger or events")
			}
			// A later user constraint is a real API change, not a rewritten old
			// snapshot or receipt. Replayed reads must retain the old rules.
			f.http(t, "control", "POST", f.path("/constraints"), []byte(`{"values":["Only the corrected reference period is current."]}`), 200)
			f.http(t, "control", "POST", f.path("/resume-source-state"), nil, 202)
			f.http(t, "control", "POST", f.path("/resume-source-state"), nil, 409)
			queued, err := n.fixture.store.Run(n.fixture.ctx, n.run.ID)
			nativeNotesNeed(t, err)
			if queued.Status != "queued" || queued.Owner != "" || queued.Fence != n.run.Fence {
				t.Fatal("native recovery failed to atomically release the queued task")
			}
			f.resumed(t, 1, method)
			old := n.run
			next, err := n.engine.Claim(n.fixture.ctx, "native-state-recovered-"+ID(), time.Minute)
			nativeNotesNeed(t, err)
			if next.ID != old.ID || next.Fence != old.Fence+1 || next.Owner == old.Owner || next.Engine != EngineDSHNative {
				t.Fatal("native recovery did not obtain a new owner/fence")
			}
			if _, _, err = n.engine.Exchange(n.fixture.ctx, &old, target, f.effects(t, false)); !errors.Is(err, ErrFence) {
				t.Fatal("previous owner replay survived recovery", err)
			}
			n.run = next
			var noteFrame execution.Frame
			// Reuse the original staged tool input rather than fabricate a new
			// operation. This reconstructs protocol frames, not a DSH loop.
			var noteParams []byte
			nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, "SELECT request FROM harness_exchanges WHERE run_id=$1 AND seq=2", n.run.ID).Scan(&noteParams))
			noteFrame = stateFrame(2, "tool", noteParams)
			for _, frame := range []execution.Frame{nativeNotesModelFrame(), noteFrame, first, target} {
				var frozen []byte
				nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, "SELECT response FROM harness_exchanges WHERE run_id=$1 AND seq=$2", n.run.ID, frame.ID).Scan(&frozen))
				value, done, e := n.engine.Exchange(n.fixture.ctx, &n.run, frame, f.effects(t, false))
				nativeNotesNeed(t, e)
				if done || !jsonEqual(frozen, JSON(value)) {
					t.Fatalf("native frame %d did not replay its frozen response", frame.ID)
				}
				if frame.ID == target.ID && !jsonEqual(JSON(value), f.droppedBody) {
					t.Fatal("recovery did not return the source-state reply lost by the callback")
				}
			}
			if n.modelCalls.Load() != 1 || n.toolCalls.Load() != 1 || f.stateCalls != 4 || !jsonEqual(before, f.ledger(t)) {
				t.Fatal("native recovery repeated an effect or changed a retained ledger row")
			}
			var current WorkingStateView
			nativeNotesNeed(t, json.Unmarshal(f.http(t, "control", "GET", f.path("/working-state"), nil, 200), &current))
			if current.Revision != wantRevision || current.RuleRevision != old.RuleRevision+1 || !current.Stale || current.SourceRuleRevision != old.RuleRevision {
				t.Fatal("current management view lost the corrected rules or retained working set")
			}
			value, done, err := n.engine.Exchange(n.fixture.ctx, &n.run, stateFrame(5, "working_state_read", []byte(`{}`)), f.effects(t, false))
			nativeNotesNeed(t, err)
			var fresh struct {
				View WorkingStateView `json:"view"`
			}
			nativeNotesNeed(t, json.Unmarshal(JSON(value), &fresh))
			if done || !jsonEqual(JSON(fresh.View), JSON(current)) || f.stateCalls != 5 || n.modelCalls.Load() != 1 {
				t.Fatal("new source-state read did not expose the current view without model use")
			}
			f.resumed(t, 1, method)
		})
	}
}

func TestNativeSourceStateHTTPCancelAfterPreview(t *testing.T) {
	f := nativeStateRecoverySetup(t)
	frame := stateFrame(2, "working_state_read", []byte(`{}`))
	f.checkpointUnknown(t, frame, f.effects(t, true))
	f.preview(t, true, "ready")
	before := f.ledger(t)
	f.http(t, "control", "POST", f.path("/cancel"), nil, 202)
	f.preview(t, false, "cancelled")
	f.http(t, "control", "POST", f.path("/resume-source-state"), nil, 409)
	r, err := f.n.fixture.store.Run(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if r.Status != "cancelled" || !r.CancelRequested || !jsonEqual(before, f.ledger(t)) || f.stateCalls != 3 || f.n.modelCalls.Load() != 1 {
		t.Fatal("preview permission bypassed later cancellation or rewrote frozen evidence")
	}
	f.resumed(t, 0, frame.Method)
}

func TestNativeSourceStateHTTPUnresolvedAndArchive(t *testing.T) {
	t.Run("inflight-model", func(t *testing.T) {
		f := nativeStateRecoverySetup(t)
		n := f.n
		// BeginModel durably reserves an attempt; a simulated interrupted
		// callback cannot produce an EndModel receipt. No provider is called.
		effects := f.effects(t, false)
		effects.complete = func(ctx context.Context, r Run, op string, messages []Message, tools []Tool) (json.RawMessage, error) {
			request := execution.NativeModelRequest(r.Snapshot.Agent.Model, messages, tools)
			attempt, cached, err := n.engine.BeginModel(ctx, r, op, request)
			if err != nil {
				return nil, err
			}
			if attempt == "" || cached != nil {
				t.Fatal("missing new inflight model attempt")
			}
			return nil, fmt.Errorf("%w: fixture interrupted before model receipt", ErrUnknown)
		}
		f.checkpointUnknown(t, nativeFrame(2, "generation", "Synthetic interrupted second generation"), effects)
		var inflight int
		nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, "SELECT count(*) FROM calls WHERE run_id=$1 AND status='inflight' AND kind='model'", n.run.ID).Scan(&inflight))
		if inflight != 1 {
			t.Fatal("unresolved attempt was not durably staged")
		}
		f.preview(t, false, "unresolved_calls")
		before := nativeNotesRows(t, n.fixture)
		f.http(t, "control", "POST", f.path("/resume-source-state"), nil, 409)
		if !jsonEqual(before, nativeNotesRows(t, n.fixture)) {
			t.Fatal("rejected unresolved recovery mutated evidence or events")
		}
		f.resumed(t, 0, "model")
	})
	t.Run("unconfirmed-archive", func(t *testing.T) {
		f := nativeStateRecoverySetup(t)
		n := f.n
		n.archive.fail = true
		_, _, err := n.engine.Exchange(n.fixture.ctx, &n.run, stateFrame(2, "working_state_read", []byte(`{}`)), f.effects(t, false))
		if err == nil || errors.Is(err, ErrUnknown) {
			t.Fatal("expected the actual ordinary archive-ack error", err)
		}
		nativeNotesNeed(t, n.engine.CheckpointFailure(n.fixture.ctx, n.run, err))
		var unconfirmed bool
		nativeNotesNeed(t, n.fixture.store.DB.QueryRow(n.fixture.ctx, `SELECT a.manifest_hash='' AND a.batch IS NOT NULL AND e.response IS NULL
 FROM harness_archives a JOIN harness_exchanges e USING(run_id,seq) WHERE a.run_id=$1 AND a.seq=2`, n.run.ID).Scan(&unconfirmed))
		r, err := n.fixture.store.Run(n.fixture.ctx, n.run.ID)
		nativeNotesNeed(t, err)
		if !unconfirmed || r.Status != "failed" || f.stateCalls != 0 || n.modelCalls.Load() != 1 {
			t.Fatal("archive barrier did not stop the state effect")
		}
		// Real archive failure naturally checkpoints failed, so not_waiting
		// wins before archive_unconfirmed. Do not manufacture waiting_unknown
		// merely to claim coverage of that later diagnostic branch.
		f.preview(t, false, "not_waiting")
		before := nativeNotesRows(t, n.fixture)
		f.http(t, "control", "POST", f.path("/resume-source-state"), nil, 409)
		if !jsonEqual(before, nativeNotesRows(t, n.fixture)) {
			t.Fatal("rejected archive recovery mutated evidence or events")
		}
		f.resumed(t, 0, "working_state_read")
	})
}
