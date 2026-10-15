package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
)

// These are Owner/public-execution tests with real PostgreSQL transactions,
// synthetic model receipts and memory archives. No provider or Node runs.
// All state transitions use APIs; SQL below only reads evidence. The existing
// native /resume endpoint is a test actor, not the proposed continuation API.
func nativeRevisionReclaim(t *testing.T, f *continuationFixture, revise bool) {
	t.Helper()
	old := f.n.run
	f.request(t, "control", "POST", "/v1/runs/"+old.ID+"/resume", 202)
	if revise {
		nativeNotesNeed(t, f.n.fixture.store.Constraints(f.n.fixture.ctx, old.ID, []string{"synthetic revised observation period"}))
	}
	next, err := f.n.engine.Claim(f.n.fixture.ctx, "native-revision-"+ID(), time.Minute)
	nativeNotesNeed(t, err)
	if next.ID != old.ID || next.Fence != old.Fence+1 {
		t.Fatal("normal explicit resume/Claim did not advance this run's fence")
	}
	if revise && next.RuleRevision <= old.RuleRevision {
		t.Fatal("normal Constraints did not advance the run revision")
	}
	next.Step = 0 // Worker.driveDSH resets its in-memory replay counter, not SQL.
	f.n.run = next
}

// Budget has no separate mutable reservation table: Usage is derived from the
// immutable Snapshot and all attempts. Hash the complete relevant rows, model
// start events and computed usage, not only an implementation-shaped row count.
func nativeRevisionLedger(t *testing.T, f *continuationFixture) string {
	t.Helper()
	var raw []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT jsonb_build_object(
 'snapshot',(SELECT snapshot FROM runs WHERE id=$1),
 'calls',(SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.id),'[]') FROM calls c WHERE c.run_id=$1),
 'attempts',(SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY a.id),'[]') FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1),
 'starts',(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.seq),'[]') FROM events e WHERE e.run_id=$1 AND e.kind='model_started'))`, f.n.run.ID).Scan(&raw))
	u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	return Hash(JSON([]json.RawMessage{raw, JSON(u)}))
}

func nativeRevisionReject(t *testing.T, f *continuationFixture, frame execution.Frame, wantCalls int32) {
	t.Helper()
	before := nativeRevisionLedger(t, f)
	_, done, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, frame, f.n.effects(nil))
	if err == nil || errors.Is(err, ErrFence) || !strings.Contains(err.Error(), "constraint revision") {
		t.Errorf("stale native intent must fail with an ordinary constraint revision error, got done=%v err=%v", done, err)
	}
	if nativeRevisionLedger(t, f) != before || f.n.modelCalls.Load() != wantCalls {
		t.Errorf("rejected stale native intent created a model attempt, budget usage, start event or synthetic send: sends=%d want=%d", f.n.modelCalls.Load(), wantCalls)
	}
	if err == nil {
		return // Red baseline has already exposed the unwanted successful send.
	}
	nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
	r, e := f.n.fixture.store.Run(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, e)
	if r.Status != "failed" || r.CancelRequested {
		t.Fatal("ordinary stale-intent error did not naturally settle failed")
	}
}

func TestNativeAttemptRevisionRejectsFailedRetry(t *testing.T) {
	f := continuationSetup(t, 1, 3, nil)
	f.failModel(t, 1, 503)
	u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if u.Attempts != 1 || u.UnknownUsageAttempts != 1 {
		t.Fatal("complete 503 did not retain its unknown-usage attempt")
	}
	nativeRevisionReclaim(t, f, true)
	nativeRevisionReject(t, f, nativeFrame(1, "generation", continuationPrivateMarker), 1)
}

func TestNativeAttemptRevisionRejectsUnsentIntent(t *testing.T) {
	f := continuationSetup(t, 1, 3, nil)
	interrupted := errors.New("fixture callback stopped after intent/archive but before BeginModel")
	effects := f.n.effects(nil)
	effects.complete = func(context.Context, Run, string, []Message, []Tool) (json.RawMessage, error) {
		return nil, interrupted
	}
	frame := nativeFrame(1, "generation", continuationPrivateMarker)
	_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, frame, effects)
	if !errors.Is(err, interrupted) {
		t.Fatal("unsent fixture did not stop at the intended callback boundary")
	}
	nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
	var archived bool
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT EXISTS(SELECT 1 FROM harness_exchanges e JOIN harness_archives a USING(run_id,seq) WHERE e.run_id=$1 AND e.seq=1 AND e.method='model' AND e.response IS NULL AND a.manifest_hash<>'')`, f.n.run.ID).Scan(&archived))
	u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	if !archived || u.Attempts != 0 || f.n.modelCalls.Load() != 0 {
		t.Fatal("unsent fixture lacks an archived intent with zero attempts")
	}
	nativeRevisionReclaim(t, f, true)
	nativeRevisionReject(t, f, frame, 0)
}

func TestNativeAttemptRevisionCompletedCacheCanFreezeAndReplay(t *testing.T) {
	f := continuationSetup(t, 2, 3, nil)
	lost := errors.New("fixture discarded callback result after successful EndModel")
	effects := f.n.effects(nil)
	complete := effects.complete
	effects.complete = func(ctx context.Context, r Run, op string, messages []Message, tools []Tool) (json.RawMessage, error) {
		if _, err := complete(ctx, r, op, messages, tools); err != nil {
			return nil, err
		}
		return nil, lost
	}
	frame := nativeFrame(1, "generation", continuationPrivateMarker)
	_, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, frame, effects)
	if !errors.Is(err, lost) {
		t.Fatal("completed-cache fixture did not discard the callback result")
	}
	nativeNotesNeed(t, f.n.engine.CheckpointFailure(f.n.fixture.ctx, f.n.run, err))
	var pending bool
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT e.response IS NULL AND c.status='completed' FROM harness_exchanges e JOIN calls c ON c.id=e.run_id||'/dsh/'||e.seq::text WHERE e.run_id=$1 AND e.seq=1`, f.n.run.ID).Scan(&pending))
	if !pending {
		t.Fatal("successful receipt must exist before the model outcome is frozen")
	}
	nativeRevisionReclaim(t, f, true)
	before := nativeRevisionLedger(t, f)
	callbacks := 0
	effects = f.n.effects(nil)
	complete = effects.complete
	effects.complete = func(ctx context.Context, r Run, op string, messages []Message, tools []Tool) (json.RawMessage, error) {
		callbacks++
		return complete(ctx, r, op, messages, tools)
	}
	value, done, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, frame, effects)
	nativeNotesNeed(t, err)
	var outcome struct {
		Kind string `json:"kind"`
	}
	nativeNotesNeed(t, json.Unmarshal(JSON(value), &outcome))
	if done || outcome.Kind != "completion" || callbacks != 1 || f.n.modelCalls.Load() != 1 || nativeRevisionLedger(t, f) != before {
		t.Fatal("completed cache failed to freeze without a new attempt/send")
	}
	if f.n.run.State.PlanRevision == f.n.run.RuleRevision {
		t.Fatal("freezing an old successful receipt silently refreshed its plan revision")
	}
	var frozen any
	nativeNotesNeed(t, json.Unmarshal(JSON(value), &frozen))
	replayed, _, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, frame, effects)
	nativeNotesNeed(t, err)
	if string(JSON(replayed)) != string(JSON(frozen)) || callbacks != 1 || f.n.modelCalls.Load() != 1 || nativeRevisionLedger(t, f) != before {
		t.Fatal("frozen replay changed the old result or repeated an effect")
	}
	_, done, err = f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, stateFrame(2, "finish", JSON(map[string]string{"result": "synthetic old-plan result"})), effects)
	nativeNotesNeed(t, err)
	if done {
		t.Fatal("old cached plan bypassed the existing finish constraint guard")
	}
}

func TestNativeAttemptRevisionSameRevisionRetryRetainsFailure(t *testing.T) {
	f := continuationSetup(t, 1, 3, nil)
	f.failModel(t, 1, 503)
	var oldID string
	var oldRow []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT a.id,to_jsonb(a) FROM model_attempts a JOIN calls c ON c.id=a.call_id WHERE c.run_id=$1`, f.n.run.ID).Scan(&oldID, &oldRow))
	nativeRevisionReclaim(t, f, false)
	_, done, err := f.n.engine.Exchange(f.n.fixture.ctx, &f.n.run, nativeFrame(1, "generation", continuationPrivateMarker), f.n.effects(nil))
	nativeNotesNeed(t, err)
	if done || f.n.modelCalls.Load() != 2 {
		t.Fatal("same-revision retry did not make one new synthetic send")
	}
	var retained []byte
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, "SELECT to_jsonb(a) FROM model_attempts a WHERE id=$1", oldID).Scan(&retained))
	u, err := f.n.fixture.store.Usage(f.n.fixture.ctx, f.n.run.ID)
	nativeNotesNeed(t, err)
	var calls, completed int
	nativeNotesNeed(t, f.n.fixture.db.QueryRow(f.n.fixture.ctx, `SELECT count(*),count(*) FILTER(WHERE status='completed') FROM calls WHERE run_id=$1 AND kind='model'`, f.n.run.ID).Scan(&calls, &completed))
	if string(retained) != string(oldRow) || u.Attempts != 2 || u.UnknownUsageAttempts != 1 || calls != 1 || completed != 1 {
		t.Fatal("retry rewrote old 503 evidence, lost unknown usage or changed logical operation identity")
	}
}
