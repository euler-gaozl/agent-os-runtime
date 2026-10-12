package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"strings"
	"testing"
	"time"
)

// An explicit synthetic immutable byte service, not a real Storage deployment.
type archiveFixture struct {
	bodies map[string][]byte
	fail   bool
}

func (a *archiveFixture) PutArtifact(_ context.Context, b []byte, media, root string) (string, error) {
	if a.bodies == nil {
		a.bodies = map[string][]byte{}
	}
	h := Hash(b)
	a.bodies[h] = append([]byte(nil), b...)
	if a.fail {
		a.fail = false
		return "", errors.New("fixture acknowledgement lost after byte retention")
	}
	return h, nil
}

type exchangeEffects struct {
	model func(context.Context, Run, string, []Message, []Tool) (Message, error)
	tool  func(context.Context, Invocation) ([]byte, error)
}

func (e exchangeEffects) CompleteModel(c context.Context, r Run, id string, m []Message, t []Tool) (Message, error) {
	return e.model(c, r, id, m, t)
}
func (e exchangeEffects) InvokeTool(c context.Context, i Invocation) ([]byte, error) {
	return e.tool(c, i)
}

func TestPublicExecutionBarrierReceiptReplayAndFence(t *testing.T) {
	db, role, worker := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e := Migrate(ctx, db, role, worker); e != nil {
		t.Fatal(e)
	}
	app, work := rolePool(t, db, role), rolePool(t, db, worker)
	s := NewStore(app, nil, "")
	snap := basicSnapshot("https://fixture.invalid")
	snap.Agent.Model.MaxTokens = 64
	id := submit(t, s, snap)
	writer := &archiveFixture{fail: true}
	engine := execution.New(work, writer)
	r, e := engine.Claim(ctx, "first", 10*time.Second)
	if e != nil || r.ID != id {
		t.Fatal(e)
	}
	modelCalls, toolCalls := 0, 0
	effects := exchangeEffects{
		model: func(c context.Context, modelRun Run, op string, m []Message, tools []Tool) (Message, error) {
			modelCalls++
			if modelRun.Snapshot.Agent.Model.MaxTokens != 64 || op != id+"/dsh/1" || !strings.Contains(m[0].Content, "Do not predict prices.") {
				t.Fatal("model arguments drift")
			}
			var acknowledged bool
			if e := db.QueryRow(c, "SELECT batch IS NULL AND manifest_hash<>'' FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&acknowledged); e != nil || !acknowledged {
				t.Fatal("model preceded archive acknowledgement", e)
			}
			request := JSON(m)
			attempt, cached, e := engine.BeginModel(c, modelRun, op, request)
			if e != nil {
				return Message{}, e
			}
			if cached != nil {
				t.Fatal("unexpected initial model cache")
			}
			message := Message{Role: "assistant", Content: "fixture plan"}
			if e = engine.EndModel(c, modelRun, op, attempt, JSON(message), nil); e != nil {
				return Message{}, e
			}
			return message, nil
		},
		tool: func(c context.Context, inv Invocation) ([]byte, error) { toolCalls++; return s.LocalTool(c, inv) },
	}
	original := JSON([]any{map[string]string{"type": "fixture", "text": "first durable event"}})
	frame := execution.Frame{ID: 1, Method: "model", Params: JSON(map[string]any{"messages": []Message{{Role: "user", Content: "fixture"}}, "max_tokens": 128}), Events: original, EventStart: 0, EventEnd: 1}
	if _, _, e = engine.Exchange(ctx, &r, frame, effects); e == nil {
		t.Fatal("archive failure ignored")
	}
	if modelCalls != 0 || toolCalls != 0 {
		t.Fatal("external work crossed failed barrier")
	}
	var staged []byte
	if e = db.QueryRow(ctx, "SELECT batch FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&staged); e != nil || string(staged) != string(original) {
		t.Fatal("durable first batch missing", e)
	}
	// Replayed native event timestamps/IDs may differ; the first committed batch wins.
	frame.Events = JSON([]any{map[string]string{"type": "fixture", "text": "regenerated event must not replace evidence"}})
	if _, _, e = engine.Exchange(ctx, &r, frame, effects); e != nil {
		t.Fatal(e)
	}
	var manifest []byte
	if e = db.QueryRow(ctx, "SELECT manifest FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&manifest); e != nil {
		t.Fatal(e)
	}
	var archive runstate.HarnessArchive
	if e = json.Unmarshal(manifest, &archive); e != nil || archive.SHA256 != Hash(original) {
		t.Fatal("archive original changed", e)
	}
	if modelCalls != 1 {
		t.Fatal("model count", modelCalls)
	}
	// Fail after LocalTool's complete atomic effect/receipt, before exchange response.
	if _, e = db.Exec(ctx, `CREATE FUNCTION reject_exchange_reply() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.seq=2 AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'controlled exchange response failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_exchange_reply BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION reject_exchange_reply()`); e != nil {
		t.Fatal(e)
	}
	toolFrame := execution.Frame{ID: 2, Method: "tool", Params: JSON(map[string]any{"name": "note_save", "arguments": json.RawMessage(invocation(r).Arguments), "call_id": "fixture-save"}), Events: JSON([]any{map[string]string{"type": "tool-plan"}}), EventStart: 1, EventEnd: 2}
	if _, _, e = engine.Exchange(ctx, &r, toolFrame, effects); e == nil || !strings.Contains(e.Error(), "controlled exchange response failure") {
		t.Fatal("response fault not reached", e)
	}
	var notes, receipts int
	if e = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM calls WHERE kind='tool' AND status='completed')").Scan(&notes, &receipts); e != nil || notes != 1 || receipts != 1 {
		t.Fatal("effect/receipt boundary", notes, receipts, e)
	}
	if _, e = db.Exec(ctx, "DROP TRIGGER reject_exchange_reply ON harness_exchanges; UPDATE runs SET lease_until=now()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	replacement := execution.New(work, writer)
	next, e := replacement.Claim(ctx, "replacement", 10*time.Second)
	if e != nil || next.Fence <= r.Fence {
		t.Fatal(e)
	}
	if _, _, e = engine.Exchange(ctx, &r, toolFrame, effects); !errors.Is(e, ErrFence) {
		t.Fatal("old fence accepted", e)
	}
	next.Step = 0
	if _, _, e = replacement.Exchange(ctx, &next, frame, effects); e != nil {
		t.Fatal(e)
	}
	value, done, e := replacement.Exchange(ctx, &next, toolFrame, effects)
	if e != nil || done {
		t.Fatal(e)
	}
	if modelCalls != 1 || toolCalls != 1 {
		t.Fatal("committed work repeated", modelCalls, toolCalls)
	}
	receipt, e := replacement.ToolReceipt(ctx, id, id+"/dsh/2")
	if e != nil || receipt.Status != "completed" || value != string(receipt.Response) {
		t.Fatal("receipt replay changed", e)
	}
	frame.Params = JSON(map[string]any{"messages": []Message{{Role: "user", Content: "changed"}}})
	if _, _, e = replacement.Exchange(ctx, &next, frame, effects); e == nil || !strings.Contains(e.Error(), "replay diverged") {
		t.Fatal("semantic drift accepted", e)
	}
}

func TestPublicExecutionCancellationKeepsUnresolvedEffects(t *testing.T) {
	db, role, worker := fixtureDatabase(t)
	ctx := context.Background()
	if e := Migrate(ctx, db, role, worker); e != nil {
		t.Fatal(e)
	}
	s := NewStore(rolePool(t, db, role), nil, "")
	engine := execution.New(rolePool(t, db, worker), nil)
	id := submit(t, s, basicSnapshot("https://fixture.invalid"))
	r, e := engine.Claim(ctx, "cancelling", 10*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(ctx, "INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence) VALUES($1,$2,'tool','fixture','{}','inflight','fixture',$3)", id+"/tool/uncertain", id, r.Fence); e != nil {
		t.Fatal(e)
	}
	if e = s.Cancel(ctx, id); e != nil {
		t.Fatal(e)
	}
	if e = engine.CheckpointFailure(ctx, r, errors.New("interrupted")); e != nil {
		t.Fatal(e)
	}
	fresh, e := s.Run(ctx, id)
	if e != nil || fresh.Status != "waiting_unknown" {
		t.Fatal("unknown effect hidden by cancellation", fresh.Status, e)
	}
}
