package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
)

type finalFixture struct {
	*sourceFixture
	seq, cursor, models         int
	draft, operation, selection string
	selectValue                 any
}

func finalSetup(t *testing.T, versions ...string) *finalFixture {
	t.Helper()
	version := EngineDSHFinal
	if len(versions) == 1 {
		version = versions[0]
	}
	f := sourceSetupVersion(t, version)
	f.r.Snapshot = nativeSnapshot()
	f.r.Snapshot.Agent.FinalOutput = &contracts.FinalOutputPolicy{Protocol: contracts.FinalOutputProtocol, Tool: "mcp_fixture_check", InputField: "draft", ReadyPath: []string{"structuredContent", "ready"}}
	f.r.Snapshot.Connectors = []Connector{{Name: "fixture", Tools: []Tool{{Name: "check", InputSchema: JSON(map[string]any{"type": "object", "properties": map[string]any{"draft": map[string]string{"type": "string"}}, "required": []string{"draft"}})}}}}
	if _, e := f.db.Exec(context.Background(), "UPDATE runs SET snapshot=$2,state=$3 WHERE id=$1", f.r.ID, JSON(f.r.Snapshot), JSON(f.r.State)); e != nil {
		t.Fatal(e)
	}
	return &finalFixture{sourceFixture: f, draft: "中文🙂e\u0301\r\n" + strings.Repeat("checked original\n", 12000) + "END  \n"}
}
func (f *finalFixture) frame(method string, args any, events ...any) execution.Frame {
	f.seq++
	start := f.cursor
	f.cursor += len(events)
	if events == nil {
		events = []any{}
	}
	return execution.Frame{ID: f.seq, Method: method, Params: JSON(args), Events: JSON(events), EventStart: start, EventEnd: f.cursor}
}
func (f *finalFixture) exchange(t *testing.T, frame execution.Frame, effects execution.Effects) (any, bool) {
	t.Helper()
	v, done, e := f.engine.Exchange(context.Background(), &f.r, frame, effects)
	if e != nil {
		t.Fatal(frame.Method, e)
	}
	return v, done
}
func (f *finalFixture) model(t *testing.T, name string, args any) {
	t.Helper()
	body := JSON(map[string]any{"choices": []any{map[string]any{"message": Message{Role: "assistant", ToolCalls: []ToolCall{{ID: "call-final", Type: "function", Function: FunctionCall{Name: name, Arguments: string(JSON(args))}}}}, "finish_reason": "tool_calls"}}, "usage": map[string]int{"prompt_tokens": 200, "completion_tokens": 50}})
	f.exchange(t, f.frame("model", map[string]any{"messages": []Message{{Role: "user", Content: "controlled final delivery"}}, "max_tokens": 393216, "purpose": "generation"}), nativeRecord(t, f.sourceFixture, 200, body, &f.models))
}
func (f *finalFixture) candidate(t *testing.T, ready any) {
	t.Helper()
	f.model(t, "mcp_fixture_check", map[string]any{"draft": f.draft})
	effects := f.effects(t)
	effects.tool = func(ctx context.Context, inv Invocation) ([]byte, error) {
		tx, e := f.s.DB.Begin(ctx)
		if e != nil {
			return nil, e
		}
		defer tx.Rollback(ctx)
		if _, e = checkInvocation(ctx, tx, inv); e != nil {
			return nil, e
		}
		response := JSON(map[string]any{"structuredContent": map[string]any{"ready": ready}})
		if e = insertCall(ctx, tx, inv, "completed", response); e != nil {
			return nil, e
		}
		return response, tx.Commit(ctx)
	}
	frame := f.frame("tool", map[string]any{"name": "mcp_fixture_check", "arguments": map[string]string{"draft": f.draft}, "call_id": "call-final"})
	value, _ := f.exchange(t, frame, effects)
	f.operation = fmt.Sprintf("%s/dsh/%d", f.r.ID, frame.ID)
	var wire struct {
		Receipt struct {
			Final struct {
				SHA   string `json:"sha256"`
				Ready bool   `json:"ready"`
			} `json:"final_output"`
		} `json:"receipt"`
	}
	if e := json.Unmarshal([]byte(value.(string)), &wire); e != nil || wire.Receipt.Final.SHA != Hash([]byte(f.draft)) {
		t.Fatal("candidate digest missing", e)
	}
}
func (f *finalFixture) prepare(t *testing.T) {
	f.candidate(t, true)
	f.model(t, contracts.FinalOutputToolName, map[string]string{"operation_id": f.operation, "sha256": Hash([]byte(f.draft))})
}
func (f *finalFixture) selectFrame() execution.Frame {
	return f.frame("final_output_select", map[string]string{"operation_id": f.operation, "sha256": Hash([]byte(f.draft)), "call_id": "call-final"})
}
func (f *finalFixture) selectDraft(t *testing.T) {
	frame := f.selectFrame()
	f.selectValue, _ = f.exchange(t, frame, f.effects(t))
	f.selection = fmt.Sprintf("%s/dsh/%d", f.r.ID, frame.ID)
	if !strings.Contains(string(JSON(f.selectValue)), `"selected":true`) {
		t.Fatal("selection rejected", string(JSON(f.selectValue)))
	}
}
func (f *finalFixture) resultEvents(failed bool) []any {
	return []any{
		map[string]any{"type": "tool/result", "data": map[string]any{"turn": 1, "step": 2, "message": map[string]any{"source": map[string]string{"callId": "call-final"}, "isError": failed, "content": []any{map[string]string{"type": "text", "text": string(JSON(f.selectValue))}}}}},
		map[string]any{"type": "turn/end", "data": map[string]any{"turn": 1, "reason": map[string]string{"kind": "completed"}}},
	}
}
func (f *finalFixture) checkpoint(t *testing.T, failed bool) {
	f.exchange(t, f.frame("checkpoint", map[string]any{}, f.resultEvents(failed)...), f.effects(t))
}
func (f *finalFixture) finishFrame() execution.Frame {
	return f.frame("finish_ref", map[string]string{"selection_id": f.selection})
}

func TestFinalOutputAtomicOriginalAndCompletedReplay(t *testing.T) {
	f := finalSetup(t)
	f.prepare(t)
	f.selectDraft(t)
	f.checkpoint(t, false)
	frame := f.finishFrame()
	value, done := f.exchange(t, frame, f.effects(t))
	if !done {
		t.Fatal("finish rejected", string(JSON(value)))
	}
	r, e := f.s.Run(context.Background(), f.r.ID)
	if e != nil || r.Status != "completed" || r.State.Result != f.draft {
		t.Fatal("original bytes not atomically retained", e)
	}
	var count int
	if e = f.db.QueryRow(context.Background(), "SELECT count(*) FROM events WHERE run_id=$1 AND kind='final_output_completed'", r.ID).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	before := f.models
	again, done := f.exchange(t, frame, f.effects(t))
	if !done || canonicalHash(JSON(value)) != canonicalHash(JSON(again)) || f.models != before {
		t.Fatal("lost reply replay changed completion")
	}
	if e = f.engine.Save(context.Background(), f.r, "completed"); e == nil {
		t.Fatal("generic save bypassed finish_ref")
	}
}
func TestFinalOutputRejectsUnreadyAndStaleCandidates(t *testing.T) {
	for _, mode := range []string{"false", "null", "string_true", "is_error", "missing", "wrong_hash", "other_run", "newer_failed", "wrong_model", "mixed_model", "revision", "unknown_call", "pending_child"} {
		t.Run(mode, func(t *testing.T) {
			f := finalSetup(t)
			f.prepare(t)
			ctx := context.Background()
			must := func(e error) {
				t.Helper()
				if e != nil {
					t.Fatal(e)
				}
			}
			switch mode {
			case "false", "null", "string_true", "is_error", "missing":
				var raw string
				switch mode {
				case "false":
					raw = `{"structuredContent":{"ready":false}}`
				case "null":
					raw = `{"structuredContent":{"ready":null}}`
				case "string_true":
					raw = `{"structuredContent":{"ready":"true"}}`
				case "is_error":
					raw = `{"isError":true,"structuredContent":{"ready":true}}`
				case "missing":
					raw = `{}`
				}
				_, e := f.db.Exec(ctx, "UPDATE calls SET response=$2 WHERE id=$1", f.operation, raw)
				must(e)
			case "wrong_hash":
				f.draft += "changed"
			case "other_run":
				f.operation = "other/dsh/2"
			case "newer_failed":
				_, e := f.db.Exec(ctx, "INSERT INTO harness_exchanges(run_id,seq,method,input_hash,request,rule_revision) VALUES($1,500,'tool','fixture',$2,1)", f.r.ID, JSON(map[string]any{"name": "mcp_fixture_check", "arguments": map[string]string{"draft": "newer"}}))
				must(e)
			case "wrong_model":
				_, e := f.db.Exec(ctx, "UPDATE harness_exchanges SET response=jsonb_set(response,'{message,tool_calls,0,function,name}','\"other\"') WHERE run_id=$1 AND method='model'", f.r.ID)
				must(e)
			case "mixed_model":
				_, e := f.db.Exec(ctx, "UPDATE harness_exchanges SET response=jsonb_set(response,'{message,tool_calls}',(response#>'{message,tool_calls}')||(response#>'{message,tool_calls}')) WHERE run_id=$1 AND method='model'", f.r.ID)
				must(e)
			case "revision":
				must(f.s.Constraints(ctx, f.r.ID, []string{"new constraints"}))
			case "unknown_call":
				_, e := f.db.Exec(ctx, "INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence) VALUES($1,$2,'tool','x','{}','unknown','x',1)", f.r.ID+"/unknown", f.r.ID)
				must(e)
			case "pending_child":
				_, e := f.db.Exec(ctx, "INSERT INTO runs(id,submission_key,input_hash,parent_id,snapshot,task) VALUES($1,$1,'x',$2,'{}','child')", f.r.ID+"-child", f.r.ID)
				must(e)
			}
			frame := f.selectFrame()
			v, done := f.exchange(t, frame, f.effects(t))
			if done || !strings.Contains(string(JSON(v)), `"selected":false`) {
				t.Fatal("unsafe selection", string(JSON(v)))
			}
			v2, _ := f.exchange(t, frame, f.effects(t))
			if canonicalHash(JSON(v)) != canonicalHash(JSON(v2)) {
				t.Fatal("rejection did not freeze")
			}
		})
	}
}
func TestFinalOutputRequiresDurableSuccessfulEnd(t *testing.T) {
	for _, mode := range []string{"missing", "failed_tool", "changed_rules", "changed_candidate", "late_model"} {
		t.Run(mode, func(t *testing.T) {
			f := finalSetup(t)
			f.prepare(t)
			f.selectDraft(t)
			if mode != "missing" {
				f.checkpoint(t, mode == "failed_tool")
			}
			switch mode {
			case "changed_rules":
				if e := f.s.Constraints(context.Background(), f.r.ID, []string{"updated"}); e != nil {
					t.Fatal(e)
				}
			case "changed_candidate":
				f.candidate(t, false)
			case "late_model":
				f.model(t, "knowledge_search", map[string]string{"query": "later"})
			}
			v, done := f.exchange(t, f.finishFrame(), f.effects(t))
			if done || !strings.Contains(string(JSON(v)), `"done":false`) {
				t.Fatal("unsafe completion", string(JSON(v)))
			}
		})
	}
}
func TestFinalOutputCommitFaultRollbackAndFenceTakeover(t *testing.T) {
	f := finalSetup(t)
	f.prepare(t)
	f.selectDraft(t)
	f.checkpoint(t, false)
	ctx := context.Background()
	frame := f.finishFrame()
	if _, e := f.db.Exec(ctx, `CREATE FUNCTION reject_final_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='final_output_completed' THEN RAISE EXCEPTION 'injected final commit failure'; END IF;RETURN NEW;END $$;
 CREATE TRIGGER reject_final_event BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION reject_final_event()`); e != nil {
		t.Fatal(e)
	}
	if _, _, e := f.engine.Exchange(ctx, &f.r, frame, f.effects(t)); e == nil {
		t.Fatal("fault missing")
	}
	var status, result string
	var response []byte
	if e := f.db.QueryRow(ctx, "SELECT status,COALESCE(state->>'result','') FROM runs WHERE id=$1", f.r.ID).Scan(&status, &result); e != nil || status != "running" || result != "" {
		t.Fatal("partial completion", status, result, e)
	}
	if e := f.db.QueryRow(ctx, "SELECT response FROM harness_exchanges WHERE run_id=$1 AND seq=$2", f.r.ID, frame.ID).Scan(&response); e != nil || response != nil {
		t.Fatal("finish reply escaped rollback", e)
	}
	if _, e := f.db.Exec(ctx, "DROP TRIGGER reject_final_event ON events"); e != nil {
		t.Fatal(e)
	}
	next := budgetTakeover(t, f.sourceFixture)
	if _, _, e := f.engine.Exchange(ctx, &f.r, frame, f.effects(t)); !errors.Is(e, ErrFence) {
		t.Fatal("stale owner accepted", e)
	}
	if _, done, e := f.engine.Exchange(ctx, &next, frame, f.effects(t)); e != nil || !done {
		t.Fatal("takeover failed", done, e)
	}
	if f.models != 2 {
		t.Fatal("repeated model", f.models)
	}
}
func TestFinalOutputCancelledAndUnsupportedPaths(t *testing.T) {
	f := finalSetup(t)
	f.prepare(t)
	f.selectDraft(t)
	f.checkpoint(t, false)
	if _, _, e := f.engine.Exchange(context.Background(), &f.r, f.frame("finish", map[string]string{"result": "substitute"}), f.effects(t)); e == nil {
		t.Fatal("text fallback accepted")
	}
	if e := f.s.Cancel(context.Background(), f.r.ID); e != nil {
		t.Fatal(e)
	}
	if _, _, e := f.engine.Exchange(context.Background(), &f.r, f.finishFrame(), f.effects(t)); !errors.Is(e, ErrFence) {
		t.Fatal("cancelled run finished", e)
	}
	old := nativeSetup(t)
	if _, _, e := old.engine.Exchange(context.Background(), &old.r, stateFrame(1, "final_output_select", []byte(`{}`)), old.effects(t)); e == nil {
		t.Fatal("old engine accepted new protocol")
	}
}

func TestFinalOutputUsesDatabasePolicyAndRejectsLegacyResume(t *testing.T) {
	f := finalSetup(t)
	f.prepare(t)
	ctx := context.Background()
	forged := f.r
	forged.Snapshot.Agent.FinalOutput = nil
	if _, _, e := f.engine.Exchange(ctx, &forged, f.frame("finish", map[string]string{"result": "substitute"}), f.effects(t)); e == nil {
		t.Fatal("caller removed frozen final policy")
	}
	forged.Engine = EngineDSHNative
	if e := f.engine.Save(ctx, forged, "completed"); e == nil {
		t.Fatal("generic Save trusted caller policy/engine")
	}
	if _, e := f.db.Exec(ctx, "UPDATE runs SET status='failed' WHERE id=$1", f.r.ID); e != nil {
		t.Fatal(e)
	}
	if e := f.s.ResumeFailedLegacy(ctx, f.r.ID); e == nil {
		t.Fatal("v5 used legacy continuation bypass")
	}
	var status string
	if e := f.db.QueryRow(ctx, "SELECT status FROM runs WHERE id=$1", f.r.ID).Scan(&status); e != nil || status != "failed" {
		t.Fatal(status, e)
	}
}

func TestFinalOutputSelectionLostReplyAndCommitFault(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			f := finalSetup(t)
			f.prepare(t)
			ctx := context.Background()
			frame := f.selectFrame()
			if fault {
				if _, e := f.db.Exec(ctx, `CREATE FUNCTION reject_select_reply() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.method='final_output_select' AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'injected selection response failure'; END IF;RETURN NEW;END $$;
 CREATE TRIGGER reject_select_reply BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION reject_select_reply()`); e != nil {
					t.Fatal(e)
				}
			}
			value, _, err := f.engine.Exchange(ctx, &f.r, frame, f.effects(t))
			if fault {
				if err == nil {
					t.Fatal("selection fault missing")
				}
				if _, e := f.db.Exec(ctx, "DROP TRIGGER reject_select_reply ON harness_exchanges"); e != nil {
					t.Fatal(e)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			next := budgetTakeover(t, f.sourceFixture)
			v, done, e := f.engine.Exchange(ctx, &next, frame, f.effects(t))
			if e != nil || done || !strings.Contains(string(JSON(v)), `"selected":true`) {
				t.Fatal("selection recovery", string(JSON(v)), e)
			}
			if !fault && canonicalHash(JSON(v)) != canonicalHash(JSON(value)) {
				t.Fatal("selection changed after lost reply")
			}
			f.r = next
			f.selectValue = v
			f.selection = fmt.Sprintf("%s/dsh/%d", next.ID, frame.ID)
			f.checkpoint(t, false)
			v, done = f.exchange(t, f.finishFrame(), f.effects(t))
			if !done || f.models != 2 {
				t.Fatal("recovered selection did not complete once", string(JSON(v)))
			}
		})
	}
}

func TestFinalOutputCheckpointReplayKeepsFirstEventProjection(t *testing.T) {
	for _, firstFailed := range []bool{false, true} {
		t.Run(fmt.Sprint(firstFailed), func(t *testing.T) {
			f := finalSetup(t)
			f.prepare(t)
			f.selectDraft(t)
			ctx := context.Background()
			frame := f.frame("checkpoint", map[string]any{}, f.resultEvents(firstFailed)...)
			if _, e := f.db.Exec(ctx, `CREATE FUNCTION reject_final_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.method='checkpoint' AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'injected checkpoint response failure'; END IF;RETURN NEW;END $$;
 CREATE TRIGGER reject_final_checkpoint BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION reject_final_checkpoint()`); e != nil {
				t.Fatal(e)
			}
			if _, _, e := f.engine.Exchange(ctx, &f.r, frame, f.effects(t)); e == nil {
				t.Fatal("checkpoint fault missing")
			}
			if _, e := f.db.Exec(ctx, "DROP TRIGGER reject_final_checkpoint ON harness_exchanges"); e != nil {
				t.Fatal(e)
			}
			frame.Events = JSON(f.resultEvents(!firstFailed)) // A regenerated batch cannot replace retained evidence.
			f.exchange(t, frame, f.effects(t))
			value, done := f.exchange(t, f.finishFrame(), f.effects(t))
			if done == firstFailed {
				t.Fatal("replayed events replaced first committed projection", string(JSON(value)))
			}
		})
	}
}
