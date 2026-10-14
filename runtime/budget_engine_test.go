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

type budgetEffects struct {
	sourceEffects
	complete func(context.Context, Run, string, []Message, []Tool) (json.RawMessage, error)
}

func (e budgetEffects) CompleteBudgetModel(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
	return e.complete(c, r, id, m, ts)
}

func budgetSetup(t *testing.T) *sourceFixture {
	t.Helper()
	f := sourceSetupVersion(t, EngineDSHBudget)
	f.r.Snapshot.Agent.Model.MaxTokens = 6144
	f.r.Snapshot.Agent.ContextTokens = 32000
	f.r.Snapshot.Agent.MaxSteps = 30
	f.r.Snapshot.Agent.MaxModelAttempts = 6
	f.r.Snapshot.Agent.ModelTokenBudget = 500000
	f.r.State.PlanRevision = 0
	if _, e := f.db.Exec(context.Background(), "UPDATE runs SET snapshot=$2,state=$3 WHERE id=$1", f.r.ID, JSON(f.r.Snapshot), JSON(f.r.State)); e != nil {
		t.Fatal(e)
	}
	return f
}
func budgetFrame() execution.Frame {
	return stateFrame(1, "model", JSON(map[string]any{"messages": []Message{{Role: "user", Content: "按日期整理供给和库存。"}}, "max_tokens": 6144, "purpose": "generation"}))
}
func budgetRequest(r Run, m []Message) []byte {
	return execution.BudgetModelRequest(r.Snapshot.Agent.Model, m, nil)
}
func setOverflowConstraints(t *testing.T, f *sourceFixture) {
	t.Helper()
	if _, e := f.db.Exec(context.Background(), "UPDATE runs SET constraints=$2,rule_revision=rule_revision+1 WHERE id=$1", f.r.ID, JSON([]string{strings.Repeat("最新约束\n\"<>", 10000)})); e != nil {
		t.Fatal(e)
	}
}
func modelExchangeResponse(t *testing.T, f *sourceFixture) []byte {
	t.Helper()
	var raw []byte
	if e := f.db.QueryRow(context.Background(), "SELECT response FROM harness_exchanges WHERE run_id=$1 AND seq=1", f.r.ID).Scan(&raw); e != nil {
		t.Fatal(e)
	}
	return raw
}
func budgetTakeover(t *testing.T, f *sourceFixture) Run {
	t.Helper()
	ctx := context.Background()
	if _, e := f.db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", f.r.ID); e != nil {
		t.Fatal(e)
	}
	r, e := f.engine.Claim(ctx, "budget-replacement", time.Minute)
	if e != nil || r.Fence <= f.r.Fence {
		t.Fatal("takeover failed", e)
	}
	r.Step = 0
	return r
}

func TestBudgetRejectionFrozenBeforeReplyAndLateBeginBlocked(t *testing.T) {
	f := budgetSetup(t)
	ctx := context.Background()
	setOverflowConstraints(t, f)
	calls := 0
	var final []byte
	effects := budgetEffects{sourceEffects: f.effects(t), complete: func(_ context.Context, r Run, _ string, m []Message, _ []Tool) (json.RawMessage, error) {
		calls++
		final = budgetRequest(r, m)
		rejected, e := execution.InspectBudgetRequest(final, r.Snapshot.Agent.ContextTokens, r.Snapshot.Agent.Model.MaxTokens)
		if e != nil || rejected == nil {
			t.Fatal("late constraints did not participate", e)
		}
		return final, nil
	}}
	v, _, e := f.engine.Exchange(ctx, &f.r, budgetFrame(), effects)
	if e != nil || !strings.Contains(string(JSON(v)), `"kind":"context_rejected"`) || f.r.State.PlanRevision != 0 {
		t.Fatal("rejection not frozen", e)
	}
	frozen := modelExchangeResponse(t, f)
	if len(frozen) == 0 {
		t.Fatal("reply preceded commit")
	}
	u, e := f.s.Usage(ctx, f.r.ID)
	if e != nil || u.Attempts != 0 || u.AccountedTokens != 0 {
		t.Fatal("preflight charged a provider attempt", u, e)
	}
	// Even a changed smaller request cannot bypass the immutable rejection.
	for _, engine := range []string{EngineDSHBudget, EngineDSHSource} {
		late := f.r
		late.Engine = engine
		if _, _, e = f.engine.BeginModel(ctx, late, f.r.ID+"/dsh/1", budgetRequest(late, []Message{{Role: "user", Content: "small"}})); e == nil {
			t.Fatal("late model after rejection was admitted", engine)
		}
	}
	next := budgetTakeover(t, f)
	if _, _, e = f.engine.Exchange(ctx, &f.r, budgetFrame(), effects); !errors.Is(e, ErrFence) {
		t.Fatal("old fence accepted", e)
	}
	v, _, e = f.engine.Exchange(ctx, &next, budgetFrame(), effects)
	if e != nil || calls != 1 || next.State.PlanRevision != 0 || next.Step != 1 {
		t.Fatal("rejected replay repeated callback or promoted plan", e, calls, next.Step)
	}
	var stored any
	_ = json.Unmarshal(frozen, &stored)
	if string(JSON(stored)) != string(JSON(v)) {
		t.Fatal("frozen rejection changed")
	}
}

func installBudgetReplyFault(t *testing.T, f *sourceFixture) {
	t.Helper()
	if _, e := f.db.Exec(context.Background(), `CREATE FUNCTION reject_budget_reply() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.seq=1 AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'controlled budget response failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_budget_reply BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION reject_budget_reply()`); e != nil {
		t.Fatal(e)
	}
}

func TestBudgetReplyCommitFailureRecoversRejectedAndCompletedWithoutDoubleCharge(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "not_sent"}[reject], func(t *testing.T) {
			f := budgetSetup(t)
			ctx := context.Background()
			if reject {
				setOverflowConstraints(t, f)
			}
			installBudgetReplyFault(t, f)
			providerCalls, callbacks := 0, 0
			effects := budgetEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, _ []Tool) (json.RawMessage, error) {
				callbacks++
				request := budgetRequest(r, m)
				if reject {
					return request, nil
				}
				a, cached, e := f.engine.BeginModel(c, r, id, request)
				if e != nil {
					return nil, e
				}
				if cached == nil {
					providerCalls++
					e = f.engine.EndModel(c, r, id, a, []byte(`{"choices":[{"message":{"role":"assistant","content":"fixture"},"finish_reason":"stop"}],"usage":{"prompt_tokens":200,"completion_tokens":50,"prompt_cache_hit_tokens":120,"total_tokens":250}}`), nil)
				}
				return request, e
			}}
			if _, _, e := f.engine.Exchange(ctx, &f.r, budgetFrame(), effects); e == nil || !strings.Contains(e.Error(), "controlled budget response failure") {
				t.Fatal("commit fault not reached", e)
			}
			if modelExchangeResponse(t, f) != nil {
				t.Fatal("failed commit got a reply")
			}
			if _, e := f.db.Exec(ctx, "DROP TRIGGER reject_budget_reply ON harness_exchanges"); e != nil {
				t.Fatal(e)
			}
			next := budgetTakeover(t, f)
			v, _, e := f.engine.Exchange(ctx, &next, budgetFrame(), effects)
			if e != nil {
				t.Fatal(e)
			}
			u, e := f.s.Usage(ctx, f.r.ID)
			if e != nil {
				t.Fatal(e)
			}
			if reject {
				if providerCalls != 0 || u.Attempts != 0 || next.State.PlanRevision != 0 {
					t.Fatal("rejected retry charged", u)
				}
			} else {
				if providerCalls != 1 || u.Attempts != 1 || u.AccountedTokens != 250 || u.CachedPromptTokens != 120 {
					t.Fatal("committed provider was repeated or usage drifted", u, providerCalls)
				}
				if !strings.Contains(string(JSON(v)), `"cached_prompt_tokens":120`) {
					t.Fatal("persisted usage not forwarded", v)
				}
			}
			var frozen any
			_ = json.Unmarshal(JSON(v), &frozen)
			before := JSON(frozen)
			next.Step = 0
			v, _, e = f.engine.Exchange(ctx, &next, budgetFrame(), effects)
			if e != nil || callbacks != 2 || string(before) != string(JSON(v)) {
				t.Fatal("reply replay drifted", e, callbacks)
			}
		})
	}
}

func TestBudgetAttemptCannotBeRelabelledAsNotSent(t *testing.T) {
	f := budgetSetup(t)
	ctx := context.Background()
	effects := budgetEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, _ []Tool) (json.RawMessage, error) {
		small := budgetRequest(r, m)
		if _, _, e := f.engine.BeginModel(c, r, id, small); e != nil {
			return nil, e
		}
		// Same semantic body, but a faulty callback returns an oversized wire
		// representation after registering an attempt. Never claim not-sent.
		return append(small, []byte(strings.Repeat(" ", 150000))...), nil
	}}
	if _, _, e := f.engine.Exchange(ctx, &f.r, budgetFrame(), effects); e == nil || !strings.Contains(e.Error(), "registered attempt") {
		t.Fatal("unknown provider operation was relabelled not sent", e)
	}
	if modelExchangeResponse(t, f) != nil {
		t.Fatal("unknown operation got a rejection response")
	}
	u, e := f.s.Usage(ctx, f.r.ID)
	if e != nil || u.Attempts != 1 || u.UnknownUsageAttempts != 1 {
		t.Fatal("unknown reservation lost", u, e)
	}
}

func TestBudgetProtocolRejectsNonContextFailuresAndInvalidCaps(t *testing.T) {
	for _, mode := range []string{"transport", "cancel", "fence", "budget", "cap", "archive", "missing_receipt"} {
		t.Run(mode, func(t *testing.T) {
			f := budgetSetup(t)
			ctx := context.Background()
			frame := budgetFrame()
			callbacks := 0
			if mode == "cap" {
				frame.Params = JSON(map[string]any{"messages": []Message{{Role: "user", Content: "x"}}, "purpose": "compaction", "max_tokens": 6144})
			}
			if mode == "archive" {
				f.archive.fail = true
			}
			if mode == "budget" {
				f.r.Snapshot.Agent.ModelTokenBudget = 100
				if _, e := f.db.Exec(ctx, "UPDATE runs SET snapshot=$2 WHERE id=$1", f.r.ID, JSON(f.r.Snapshot)); e != nil {
					t.Fatal(e)
				}
			}
			effects := budgetEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, _ []Tool) (json.RawMessage, error) {
				callbacks++
				request := budgetRequest(r, m)
				switch mode {
				case "transport":
					return nil, errors.New("controlled transport failure")
				case "cancel":
					if _, e := f.db.Exec(c, "UPDATE runs SET cancel_requested=true WHERE id=$1", r.ID); e != nil {
						return nil, e
					}
				case "fence":
					if _, e := f.db.Exec(c, "UPDATE runs SET fence=fence+1 WHERE id=$1", r.ID); e != nil {
						return nil, e
					}
				case "budget":
					_, _, e := f.engine.BeginModel(c, r, id, request)
					return request, e
				}
				return request, nil
			}}
			if _, _, e := f.engine.Exchange(ctx, &f.r, frame, effects); e == nil {
				t.Fatal("non-context failure returned a successful result")
			}
			if modelExchangeResponse(t, f) != nil {
				t.Fatal("failure fabricated an envelope")
			}
			if (mode == "cap" || mode == "archive") && callbacks != 0 {
				t.Fatal("callback crossed policy/archive rejection")
			}
		})
	}
}

func TestBudgetFinalProviderFieldsBoundToImmutableIntent(t *testing.T) {
	for _, field := range []string{"model", "tools", "delete_tools", "tool_schema", "thinking", "reasoning_effort", "max_tokens", "stream", "extra"} {
		t.Run(field, func(t *testing.T) {
			f := budgetSetup(t)
			ctx := context.Background()
			frame := budgetFrame()
			frame.Params = JSON(map[string]any{"messages": []Message{{Role: "user", Content: "read"}}, "max_tokens": 6144, "purpose": "generation",
				"tools": []Tool{{Name: "read_fixture", Description: "Bound schema", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`)}}})
			effects := budgetEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
				var body map[string]any
				_ = json.Unmarshal(execution.BudgetModelRequest(r.Snapshot.Agent.Model, m, ts), &body)
				body[field] = "different"
				if field == "delete_tools" {
					delete(body, field)
					delete(body, "tools")
				}
				if field == "tool_schema" {
					delete(body, field)
					body["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["parameters"] = map[string]any{"type": "object", "additionalProperties": true}
				}
				changed := JSON(body)
				if _, _, e := f.engine.BeginModel(c, r, id, changed); e == nil {
					t.Fatal("changed provider field admitted", field)
				}
				return changed, nil
			}}
			if _, _, e := f.engine.Exchange(ctx, &f.r, frame, effects); e == nil || !strings.Contains(e.Error(), "frozen intent") {
				t.Fatal("callback drift accepted", e)
			}
			if modelExchangeResponse(t, f) != nil {
				t.Fatal("different request got frozen envelope")
			}
			u, e := f.s.Usage(ctx, f.r.ID)
			if e != nil || u.Attempts != 0 {
				t.Fatal("bad request charged", u, e)
			}
		})
	}
}

func TestBudgetSameFenceConcurrentBeginAndRejectionAreExclusive(t *testing.T) {
	f := budgetSetup(t)
	ctx := context.Background()
	f.r.Snapshot.Agent.MaxModelAttempts = 64
	if _, e := f.db.Exec(ctx, "UPDATE runs SET snapshot=$2 WHERE id=$1", f.r.ID, JSON(f.r.Snapshot)); e != nil {
		t.Fatal(e)
	}
	for round := 1; round <= 8; round++ {
		frame := budgetFrame()
		frame.ID = round
		type pending struct {
			run     Run
			id      string
			request []byte
		}
		ready := make(chan pending, 1)
		start := make(chan struct{})
		exchangeDone := make(chan error, 1)
		beginDone := make(chan error, 1)
		effects := budgetEffects{sourceEffects: f.effects(t), complete: func(_ context.Context, r Run, id string, m []Message, _ []Tool) (json.RawMessage, error) {
			request := budgetRequest(r, m)
			ready <- pending{r, id, request}
			<-start
			return append(request, []byte(strings.Repeat(" ", 150000))...), nil
		}}
		go func() { _, _, e := f.engine.Exchange(ctx, &f.r, frame, effects); exchangeDone <- e }()
		p := <-ready
		go func() { <-start; _, _, e := f.engine.BeginModel(ctx, p.run, p.id, p.request); beginDone <- e }()
		close(start)
		beginErr, exchangeErr := <-beginDone, <-exchangeDone
		var attempts int
		var reply []byte
		if e := f.db.QueryRow(ctx, "SELECT (SELECT count(*) FROM calls WHERE id=$3),response FROM harness_exchanges WHERE run_id=$1 AND seq=$2", f.r.ID, round, p.id).Scan(&attempts, &reply); e != nil {
			t.Fatal(e)
		}
		if beginErr == nil {
			if exchangeErr == nil || !strings.Contains(exchangeErr.Error(), "registered attempt") || attempts != 1 || reply != nil {
				t.Fatal("attempt winner also got not-sent receipt", exchangeErr, attempts)
			}
		} else {
			if exchangeErr != nil || !strings.Contains(beginErr.Error(), "already rejected") || attempts != 0 || !strings.Contains(string(reply), "context_rejected") {
				t.Fatal("rejection winner admitted or lost an attempt", beginErr, exchangeErr, attempts)
			}
		}
	}
}

func TestBudgetFrozenIntentKeepsOldRulesButCannotPromoteThemAfterChange(t *testing.T) {
	f := budgetSetup(t)
	ctx := context.Background()
	setOverflowConstraints(t, f)
	var first []byte
	callbacks := 0
	effects := budgetEffects{sourceEffects: f.effects(t), complete: func(_ context.Context, r Run, _ string, m []Message, _ []Tool) (json.RawMessage, error) {
		callbacks++
		request := budgetRequest(r, m)
		if callbacks == 1 {
			first = request
			return nil, errors.New("controlled interruption after frozen intent")
		}
		if string(first) != string(request) {
			t.Fatal("rule update rewrote old model intent")
		}
		return request, nil
	}}
	if _, _, e := f.engine.Exchange(ctx, &f.r, budgetFrame(), effects); e == nil {
		t.Fatal("interruption missing")
	}
	if _, e := f.db.Exec(ctx, "UPDATE runs SET constraints='[\"new short constraint\"]',rule_revision=rule_revision+1 WHERE id=$1", f.r.ID); e != nil {
		t.Fatal(e)
	}
	v, _, e := f.engine.Exchange(ctx, &f.r, budgetFrame(), effects)
	if e != nil || !strings.Contains(string(JSON(v)), "context_rejected") || f.r.State.PlanRevision != 0 || f.r.RuleRevision < 2 {
		t.Fatal("old rejection promoted new rules", e, f.r.State.PlanRevision, f.r.RuleRevision)
	}
	finish := stateFrame(2, "finish", []byte(`{"result":"cannot finish from a rejected plan"}`))
	v, done, e := f.engine.Exchange(ctx, &f.r, finish, effects)
	if e != nil || done || !strings.Contains(string(JSON(v)), "continue") {
		t.Fatal("rejected plan completed task under new rules", e)
	}
}

func TestBudgetRejectionCannotMaskOperationCollision(t *testing.T) {
	f := budgetSetup(t)
	ctx := context.Background()
	setOverflowConstraints(t, f)
	effects := budgetEffects{sourceEffects: f.effects(t), complete: func(c context.Context, r Run, id string, m []Message, _ []Tool) (json.RawMessage, error) {
		request := budgetRequest(r, m)
		// Explicit damaged-ledger fixture: another effect occupies the same ID.
		_, err := f.db.Exec(c, "INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence) VALUES($1,$2,'tool',$3,$4,'unknown','collision',$5)", id, r.ID, Hash(request), request, r.Fence)
		return request, err
	}}
	if _, _, e := f.engine.Exchange(ctx, &f.r, budgetFrame(), effects); e == nil || !strings.Contains(e.Error(), "registered attempt") {
		t.Fatal("unknown effect collision hidden by preflight", e)
	}
	if modelExchangeResponse(t, f) != nil {
		t.Fatal("collision produced not-sent receipt")
	}
}
