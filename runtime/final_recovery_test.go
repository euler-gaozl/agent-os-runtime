package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/euler-gaozl/agent-os-runtime/execution"
	"testing"
	"time"
)

func TestFinalOutputWorkerFailureClassificationAndSourceRecovery(t *testing.T) {
	for _, point := range []string{"select", "finish"} {
		t.Run(point, func(t *testing.T) {
			f := finalSetup(t)
			f.prepare(t)
			ctx := context.Background()
			var frame execution.Frame
			if point == "finish" {
				f.selectDraft(t)
				f.checkpoint(t, false)
				frame = f.finishFrame()
			} else {
				frame = f.selectFrame()
			}
			if _, e := f.db.Exec(ctx, `CREATE FUNCTION reject_final_response() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.method IN('finish_ref','final_output_select') AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'injected final response failure'; END IF;RETURN NEW;END $$;
 CREATE TRIGGER reject_final_response BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION reject_final_response()`); e != nil {
				t.Fatal(e)
			}
			_, _, fault := f.engine.Exchange(ctx, &f.r, frame, f.effects(t))
			if fault == nil {
				t.Fatal("fault missing")
			}
			// The same execution facade called by Worker after Exchange fails.
			if e := f.engine.CheckpointFailure(ctx, f.r, fault); e != nil {
				t.Fatal(e)
			}
			run, e := f.s.Run(ctx, f.r.ID)
			if e != nil || run.Status != "waiting_unknown" {
				t.Fatal("failure cannot be safely resumed", run.Status, e)
			}
			preview, e := readSourceRecovery(ctx, f.s.DB, f.r.ID)
			if e != nil || !preview.Allowed {
				t.Fatal("recovery unavailable", preview, e)
			}
			if _, e = f.db.Exec(ctx, "DROP TRIGGER reject_final_response ON harness_exchanges"); e != nil {
				t.Fatal(e)
			}
			if e = f.s.resumeSourceState(ctx, f.r.ID); e != nil {
				t.Fatal(e)
			}
			next, e := f.engine.Claim(ctx, "recovered-final-worker", time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			value, done, e := f.engine.Exchange(ctx, &next, frame, f.effects(t))
			if e != nil {
				t.Fatal(e)
			}
			f.r = next
			if point == "select" {
				if done {
					t.Fatal("selection completed prematurely")
				}
				f.selectValue = value
				f.selection = fmt.Sprintf("%s/dsh/%d", next.ID, frame.ID)
				f.checkpoint(t, false)
				_, done = f.exchange(t, f.finishFrame(), f.effects(t))
			}
			if !done || f.models != 2 {
				t.Fatal("recovery duplicated requests or did not finish", done, f.models)
			}
			if e = f.engine.CheckpointFailure(ctx, f.r, errors.New("lost completion reply")); e != nil {
				t.Fatal("completed state was treated as a new failure", e)
			}
		})
	}
}

func TestFinalOutputCheckpointFailureCancellationConverges(t *testing.T) {
	f := finalSetup(t)
	f.prepare(t)
	f.selectDraft(t)
	ctx := context.Background()
	frame := f.frame("checkpoint", map[string]any{}, f.resultEvents(false)...)
	if _, e := f.db.Exec(ctx, `CREATE FUNCTION reject_cancel_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.method='checkpoint' AND NEW.response IS NOT NULL THEN RAISE EXCEPTION 'injected checkpoint failure'; END IF;RETURN NEW;END $$;
 CREATE TRIGGER reject_cancel_checkpoint BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION reject_cancel_checkpoint()`); e != nil {
		t.Fatal(e)
	}
	_, _, fault := f.engine.Exchange(ctx, &f.r, frame, f.effects(t))
	if fault == nil {
		t.Fatal("fault missing")
	}
	if e := f.engine.CheckpointFailure(ctx, f.r, fault); e != nil {
		t.Fatal(e)
	}
	if e := f.s.Cancel(ctx, f.r.ID); e != nil {
		t.Fatal(e)
	}
	run, e := f.s.Run(ctx, f.r.ID)
	if e != nil || run.Status != "cancelled" || !run.CancelRequested {
		t.Fatal("cancelled internal exchange remained stuck", run.Status, e)
	}
	preview, e := readSourceRecovery(ctx, f.s.DB, f.r.ID)
	if e != nil || preview.Allowed {
		t.Fatal("cancelled task can resume", preview, e)
	}
	if e = f.s.resumeSourceState(ctx, f.r.ID); e == nil {
		t.Fatal("cancelled task resumed")
	}
}
