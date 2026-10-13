package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestSourceStateOperatorRecoveryBoundary(t *testing.T) {
	for _, mode := range []string{"valid", "cancelled", "historical", "running", "unconfirmed", "later-model", "later-unconfirmed-state", "unknown-tool", "inflight-model", "worker", "toolhub"} {
		t.Run(mode, func(t *testing.T) {
			f := sourceSetup(t)
			ctx := context.Background()
			frame := stateFrame(1, "working_state_read", []byte(`{}`))
			if _, _, err := f.engine.Exchange(ctx, &f.r, frame, f.effects(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.Exec(ctx, "UPDATE runs SET status='waiting_unknown' WHERE id=$1", f.r.ID); err != nil {
				t.Fatal(err)
			}
			var before []byte
			if err := f.db.QueryRow(ctx, "SELECT response FROM harness_exchanges WHERE run_id=$1 AND seq=1", f.r.ID).Scan(&before); err != nil {
				t.Fatal(err)
			}
			exec := func(q string, args ...any) {
				t.Helper()
				if _, err := f.db.Exec(ctx, q, args...); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "cancelled":
				exec("UPDATE runs SET cancel_requested=true WHERE id=$1", f.r.ID)
			case "historical":
				exec("UPDATE runs SET engine=$2 WHERE id=$1", f.r.ID, EngineDSHEvidence)
			case "running":
				exec("UPDATE runs SET status='running' WHERE id=$1", f.r.ID)
			case "unconfirmed":
				exec("UPDATE harness_archives SET manifest_hash='',manifest=NULL,batch=convert_to('[]','UTF8') WHERE run_id=$1", f.r.ID)
			case "later-model", "later-unconfirmed-state":
				method := "model"
				if mode == "later-unconfirmed-state" {
					method = "working_state_read"
				}
				exec("INSERT INTO harness_exchanges(run_id,seq,method,input_hash,request,rule_revision) VALUES($1,2,$2,'fixture','{}',1)", f.r.ID, method)
			case "unknown-tool", "inflight-model":
				kind, status := "tool", "unknown"
				if mode == "inflight-model" {
					kind = "model"
					status = "inflight"
				}
				exec("INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence) VALUES($1,$2,$3,'fixture','{}',$4,'fixture-attempt',1)", f.r.ID+"/unresolved", f.r.ID, kind, status)
			}
			caller := "control"
			if mode == "worker" || mode == "toolhub" {
				caller = mode
			}
			req, err := http.NewRequest("POST", f.server.URL+"/v1/runs/"+f.r.ID+"/resume-source-state", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+testToken+caller)
			res, err := f.server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			want := 409
			if mode == "valid" {
				want = 202
			}
			if caller != "control" {
				want = 403
			}
			if res.StatusCode != want {
				t.Fatal("recovery boundary", mode, res.StatusCode, string(body))
			}
			var after []byte
			var events int
			if err = f.db.QueryRow(ctx, "SELECT response FROM harness_exchanges WHERE run_id=$1 AND seq=1", f.r.ID).Scan(&after); err != nil || string(after) != string(before) {
				t.Fatal("frozen read rewritten", err)
			}
			if err = f.db.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id=$1 AND kind='source_state_resumed'", f.r.ID).Scan(&events); err != nil {
				t.Fatal(err)
			}
			r, err := f.s.Run(ctx, f.r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "valid" {
				if events != 1 || r.Status != "queued" || r.Owner != "" {
					t.Fatal("recovery not atomically queued", r.Status, r.Owner, events)
				}
			} else if events != 0 || r.Status == "queued" {
				t.Fatal("rejected recovery mutated run")
			}
		})
	}
}

func TestSourceStateUnknownCancellationDoesNotResumeEffects(t *testing.T) {
	for _, unresolved := range []bool{false, true} {
		t.Run(fmt.Sprint(unresolved), func(t *testing.T) {
			f := sourceSetup(t)
			ctx := context.Background()
			if _, _, err := f.engine.Exchange(ctx, &f.r, stateFrame(1, "working_state_read", []byte(`{}`)), f.effects(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.Exec(ctx, "UPDATE runs SET status='waiting_unknown' WHERE id=$1", f.r.ID); err != nil {
				t.Fatal(err)
			}
			if unresolved {
				if _, err := f.db.Exec(ctx, "INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence) VALUES($1,$2,'tool','fixture','{}','unknown','fixture',1)", f.r.ID+"/unknown", f.r.ID); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.s.Cancel(ctx, f.r.ID); err != nil {
				t.Fatal(err)
			}
			r, err := f.s.Run(ctx, f.r.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := "cancelled"
			if unresolved {
				want = "waiting_unknown"
			}
			if r.Status != want || !r.CancelRequested {
				t.Fatal("source cancellation failed to converge", r.Status, r.CancelRequested)
			}
			if err = f.s.resumeSourceState(ctx, f.r.ID); err == nil {
				t.Fatal("cancelled source was requeued")
			}
			var frames int
			if err = f.db.QueryRow(ctx, "SELECT count(*) FROM harness_exchanges WHERE run_id=$1", f.r.ID).Scan(&frames); err != nil || frames != 1 {
				t.Fatal("cancellation executed another frame", frames, err)
			}
		})
	}
}

func TestSourceStateUnknownCancellationWaitsForActiveChild(t *testing.T) {
	f := sourceSetup(t)
	ctx := context.Background()
	if _, _, err := f.engine.Exchange(ctx, &f.r, stateFrame(1, "working_state_read", []byte(`{}`)), f.effects(t)); err != nil {
		t.Fatal(err)
	}
	child := submit(t, f.s, basicSnapshot("https://fixture.invalid"))
	if _, err := f.db.Exec(ctx, "UPDATE runs SET parent_id=$2,status='running',owner='child-worker',fence=1,lease_until=now()+interval '1 minute' WHERE id=$1", child, f.r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec(ctx, "UPDATE runs SET status='waiting_unknown' WHERE id=$1", f.r.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Cancel(ctx, f.r.ID); err != nil {
		t.Fatal(err)
	}
	parent, err := f.s.Run(ctx, f.r.ID)
	if err != nil || parent.Status != "waiting_children" {
		t.Fatal("parent cancelled before active child stopped", parent.Status, err)
	}
	r, err := f.s.Run(ctx, child)
	if err != nil || !r.CancelRequested {
		t.Fatal("child cancellation not propagated", err)
	}
	if err = f.engine.CheckpointFailure(ctx, r, ErrFence); err != nil {
		t.Fatal(err)
	}
	r, err = f.engine.Claim(ctx, "cancellation-cleanup", time.Minute)
	if err != nil || r.ID != f.r.ID || !r.CancelRequested {
		t.Fatal("parent cleanup not claimable", err)
	}
	if err = f.engine.CheckpointFailure(ctx, r, ErrFence); err != nil {
		t.Fatal(err)
	}
	parent, err = f.s.Run(ctx, f.r.ID)
	if err != nil || parent.Status != "cancelled" {
		t.Fatal("parent did not settle after child", parent.Status, err)
	}
	var frames int
	if err = f.db.QueryRow(ctx, "SELECT count(*) FROM harness_exchanges WHERE run_id=$1", f.r.ID).Scan(&frames); err != nil || frames != 1 {
		t.Fatal("cancel cleanup executed source effects", frames, err)
	}
}
