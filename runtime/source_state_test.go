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
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sourceEffects struct {
	exchangeEffects
	state func(context.Context, Invocation) ([]byte, error)
}

func (e sourceEffects) SourceState(c context.Context, inv Invocation) ([]byte, error) {
	return e.state(c, inv)
}

type sourceFixture struct {
	s          *Store
	db, worker *pgxpool.Pool
	engine     *execution.Engine
	r          Run
	archive    *archiveFixture
	server     *httptest.Server
}

func sourceSetup(t *testing.T) *sourceFixture {
	return sourceSetupVersion(t, EngineDSHSource)
}

func sourceSetupVersion(t *testing.T, version string) *sourceFixture {
	t.Helper()
	s, db, work, id := archiveOwnerFixture(t)
	// Test only: select a pinned version for the existing archive fixture run.
	if _, err := db.Exec(context.Background(), "UPDATE runs SET engine=$2 WHERE id=$1", id, version); err != nil {
		t.Fatal(err)
	}
	a := &archiveFixture{}
	e, err := execution.NewSourceVersions(work, a, []string{version})
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.Claim(context.Background(), "source-fixture", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r.State.PlanRevision = r.RuleRevision // Explicit synthetic plan; no model call.
	peers := map[string]string{}
	for _, role := range []string{"control", "toolhub", "knowledge", "worker", "futures"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	server := httptest.NewServer((&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize}))
	t.Cleanup(server.Close)
	return &sourceFixture{s: s, db: db, worker: work, engine: e, r: r, archive: a, server: server}
}

func (f *sourceFixture) post(t *testing.T, role string, inv Invocation) ([]byte, int) {
	t.Helper()
	req, err := http.NewRequest("POST", f.server.URL+"/v1/source-state", bytes.NewReader(JSON(inv)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken+role)
	req.Header.Set("Content-Type", "application/json")
	res, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	return body, res.StatusCode
}

func (f *sourceFixture) effects(t *testing.T) sourceEffects {
	return sourceEffects{state: func(_ context.Context, inv Invocation) ([]byte, error) {
		body, status := f.post(t, "worker", inv)
		if status != 200 {
			return nil, fmt.Errorf("source-state status %d: %s", status, body)
		}
		return body, nil
	}}
}

func stateFrame(seq int, method string, args []byte) execution.Frame {
	return execution.Frame{ID: seq, Method: method, Params: args, Events: []byte("[]")}
}

func TestSourceEngineOptInAndCheckpointBarrier(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		t.Run(version, func(t *testing.T) {
			f := sourceSetupVersion(t, version)
			ctx := context.Background()
			if _, err := f.db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", f.r.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := execution.New(f.worker, f.archive).Claim(ctx, "historical", time.Minute); !errors.Is(err, execution.ErrNoWork) {
				t.Fatal("old Worker claimed source engine", err)
			}
			r, err := f.engine.Claim(ctx, "source-enabled", time.Minute)
			if err != nil || r.Fence <= f.r.Fence {
				t.Fatal("opt-in claim failed", err)
			}
			f.archive.fail = true
			frame := stateFrame(1, "checkpoint", []byte("{}"))
			frame.Events = []byte(`[{"type":"compact/end","synthetic":true}]`)
			frame.EventEnd = 1
			if _, _, err = f.engine.Exchange(ctx, &r, frame, f.effects(t)); err == nil {
				t.Fatal("checkpoint acknowledged before archive")
			}
			var complete bool
			if err = f.db.QueryRow(ctx, "SELECT response IS NOT NULL FROM harness_exchanges WHERE run_id=$1 AND seq=1", r.ID).Scan(&complete); err != nil || complete {
				t.Fatal("failed checkpoint got response", err)
			}
			value, _, err := f.engine.Exchange(ctx, &r, frame, f.effects(t))
			if err != nil || value.(map[string]any)["persisted"] != true {
				t.Fatal("checkpoint retry failed", value, err)
			}
			if r.State.HarnessArtifact == "" {
				t.Fatal("checkpoint lost manifest")
			}
			oldID := submit(t, f.s, basicSnapshot("https://fixture.invalid"))
			old, err := execution.New(f.worker, f.archive).Claim(ctx, "historical", time.Minute)
			if err != nil || old.ID != oldID {
				t.Fatal(err)
			}
			if _, _, err = f.engine.Exchange(ctx, &old, stateFrame(1, "working_state_read", []byte("{}")), f.effects(t)); err == nil {
				t.Fatal("source method admitted for old engine")
			}

		})
	}
}

func TestSourceStateFrozenReadAndRejectedReplyRecovery(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		t.Run(version, func(t *testing.T) {
			f := sourceSetupVersion(t, version)
			ctx := context.Background()
			source := invocation(f.r)
			if _, err := f.s.LocalTool(ctx, source); err != nil {
				t.Fatal(err)
			}
			calls := 0
			effects := f.effects(t)
			transport := effects.state
			effects.state = func(c context.Context, inv Invocation) ([]byte, error) {
				calls++
				body, err := transport(c, inv)
				if err != nil {
					return nil, err
				}
				return body, errors.New("fixture HTTP reply discarded after Runtime commit")
			}
			read := stateFrame(1, "working_state_read", []byte("{}"))
			if _, _, err := f.engine.Exchange(ctx, &f.r, read, effects); err == nil {
				t.Fatal("read reply fault missing")
			}
			if _, err := f.s.commitWorkingState(ctx, workingInput(f.r, source, "later-1", 0)); err != nil {
				t.Fatal(err)
			}
			value, _, err := f.engine.Exchange(ctx, &f.r, read, effects)
			if err != nil || value.(map[string]any)["view"].(map[string]any)["revision"] != float64(0) || calls != 1 {
				t.Fatal("replay refreshed frozen read", value, err, calls)
			}
			change := workingInput(f.r, source, "ignored", 3)
			reject := stateFrame(2, "working_state_update", change.Arguments)
			if _, _, err = f.engine.Exchange(ctx, &f.r, reject, effects); err == nil {
				t.Fatal("rejection reply fault missing")
			}
			for rev := int64(1); rev < 3; rev++ {
				if _, err = f.s.commitWorkingState(ctx, workingInput(f.r, source, fmt.Sprintf("later-%d", rev+1), rev)); err != nil {
					t.Fatal(err)
				}
			}
			value, _, err = f.engine.Exchange(ctx, &f.r, reject, effects)
			if err != nil || value.(map[string]any)["rejected"] != true || calls != 2 {
				t.Fatal("rejected retry became a write", value, err, calls)
			}
			view, err := f.s.WorkingState(ctx, f.r.ID, 0)
			if err != nil || view.Revision != 3 {
				t.Fatal("rejection added a version", view, err)
			}
			value, _, err = f.engine.Exchange(ctx, &f.r, stateFrame(3, "working_state_read", []byte("{}")), f.effects(t))
			if err != nil || value.(map[string]any)["view"].(map[string]any)["revision"] != float64(3) {
				t.Fatal("new read failed to refresh", value, err)
			}

		})
	}
}

func TestSourceStateAtomicResponseTakeoverRulesAndCancellation(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		t.Run(version, func(t *testing.T) {
			f := sourceSetupVersion(t, version)
			ctx := context.Background()
			source := invocation(f.r)
			if _, err := f.s.LocalTool(ctx, source); err != nil {
				t.Fatal(err)
			}
			change := workingInput(f.r, source, "ignored", 0)
			frame := stateFrame(1, "working_state_update", change.Arguments)
			if _, err := f.db.Exec(ctx, `CREATE FUNCTION fail_source_response() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.response IS NOT NULL THEN RAISE EXCEPTION 'controlled source response failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER fail_source_response BEFORE UPDATE ON harness_exchanges FOR EACH ROW EXECUTE FUNCTION fail_source_response()`); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.engine.Exchange(ctx, &f.r, frame, f.effects(t)); err == nil {
				t.Fatal("response rollback fault absent")
			}
			view, err := f.s.WorkingState(ctx, f.r.ID, 0)
			if err != nil || view.Revision != 0 {
				t.Fatal("state committed without response", view, err)
			}
			var count int
			if err = f.db.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id=$1 AND kind='working_state_committed'", f.r.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("audit escaped rollback", count, err)
			}
			if _, err = f.db.Exec(ctx, "DROP TRIGGER fail_source_response ON harness_exchanges"); err != nil {
				t.Fatal(err)
			}
			effects := f.effects(t)
			transport := effects.state
			requests := 0
			effects.state = func(c context.Context, inv Invocation) ([]byte, error) {
				requests++
				body, err := transport(c, inv)
				if err != nil {
					return nil, err
				}
				return body, errors.New("controlled response loss before outer Save")
			}
			if _, _, err = f.engine.Exchange(ctx, &f.r, frame, effects); err == nil {
				t.Fatal("reply loss absent")
			}
			receipt, err := f.s.WorkingReceipt(ctx, f.r.ID, f.r.ID+"/working-state/1")
			if err != nil || receipt.Revision != 1 {
				t.Fatal("atomic response commit missing", receipt, err)
			}
			if err = f.s.Constraints(ctx, f.r.ID, []string{"corrected user constraints"}); err != nil {
				t.Fatal(err)
			}
			if _, err = f.db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", f.r.ID); err != nil {
				t.Fatal(err)
			}
			replacement, err := f.engine.Claim(ctx, "takeover", time.Minute)
			if err != nil || replacement.Fence <= f.r.Fence {
				t.Fatal(err)
			}
			value, _, err := f.engine.Exchange(ctx, &replacement, frame, effects)
			if err != nil || canonicalHash(JSON(value)) != canonicalHash(JSON(receipt)) || requests != 1 {
				t.Fatal("committed update executed again", value, err, requests)
			}
			if err = f.db.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id=$1 AND kind='working_state_committed'", f.r.ID).Scan(&count); err != nil || count != 1 {
				t.Fatal("duplicated state event", count, err)
			}
			stale := workingInput(f.r, source, "1", 0)
			if _, status := f.post(t, "worker", stale); status != 412 {
				t.Fatal("old owner accepted", status)
			}
			replacement.State.PlanRevision = f.r.RuleRevision
			newChange := workingInput(replacement, source, "ignored", 1)
			spoof := f.effects(t)
			base := spoof.state
			spoof.state = func(c context.Context, inv Invocation) ([]byte, error) {
				inv.RuleRevision = replacement.RuleRevision
				return base(c, inv)
			}
			value, _, err = f.engine.Exchange(ctx, &replacement, stateFrame(2, "working_state_update", newChange.Arguments), spoof)
			if err != nil || value.(map[string]any)["rejected"] != true {
				t.Fatal("current rules reauthorized old model plan", value, err)
			}
			if err = f.s.Cancel(ctx, f.r.ID); err != nil {
				t.Fatal(err)
			}
			stale.Owner, stale.Fence = replacement.Owner, replacement.Fence
			if _, status := f.post(t, "worker", stale); status != 412 {
				t.Fatal("cancelled Worker resumed", status)
			}
			if got, err := f.s.WorkingReceipt(ctx, f.r.ID, receipt.OperationID); err != nil || got != receipt {
				t.Fatal("cancel hid original receipt", err)
			}

		})
	}
}

func TestSourceStateIdentityIntentArchiveAndSoleResponseWriter(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		t.Run(version, func(t *testing.T) {
			f := sourceSetupVersion(t, version)
			ctx := context.Background()
			inv := Invocation{RunID: f.r.ID, Owner: f.r.Owner, Fence: f.r.Fence, RuleRevision: f.r.RuleRevision, OperationID: f.r.ID + "/working-state/1", Name: "working_state_read", Arguments: []byte("{}")}
			for _, role := range []string{"control", "toolhub", "knowledge", "futures"} {
				if _, status := f.post(t, role, inv); status != 403 {
					t.Fatal("source route leaked", role, status)
				}
			}
			if _, status := f.post(t, "worker", inv); status != 409 {
				t.Fatal("missing intent accepted", status)
			}
			bad := inv
			bad.OperationID = "foreign/working-state/1"
			if _, status := f.post(t, "worker", bad); status != 400 {
				t.Fatal("foreign operation accepted", status)
			}
			f.archive.fail = true
			frame := stateFrame(1, "working_state_read", []byte("{}"))
			if _, _, err := f.engine.Exchange(ctx, &f.r, frame, f.effects(t)); err == nil {
				t.Fatal("archive fault absent")
			}
			if _, status := f.post(t, "worker", inv); status != 409 {
				t.Fatal("unacknowledged archive bypassed", status)
			}
			corrupt := f.effects(t)
			base := corrupt.state
			corrupt.state = func(c context.Context, i Invocation) ([]byte, error) {
				if _, err := base(c, i); err != nil {
					return nil, err
				}
				return []byte(`{"rejected":false,"forged":true}`), nil
			}
			if _, _, err := f.engine.Exchange(ctx, &f.r, frame, corrupt); err == nil || !strings.Contains(err.Error(), "transport mismatch") {
				t.Fatal("forged bridge reply accepted", err)
			}
			value, _, err := f.engine.Exchange(ctx, &f.r, frame, corrupt)
			if err != nil || value.(map[string]any)["view"] == nil || value.(map[string]any)["forged"] != nil {
				t.Fatal("outer response overwrote Runtime snapshot", value, err)
			}
			bad = inv
			bad.Arguments = []byte(`{"revision":9}`)
			if _, status := f.post(t, "worker", bad); status != 409 {
				t.Fatal("changed intent accepted", status)
			}
			oldID := submit(t, f.s, basicSnapshot("https://fixture.invalid"))
			old, err := execution.New(f.worker, f.archive).Claim(ctx, "old", time.Minute)
			if err != nil || old.ID != oldID {
				t.Fatal(err)
			}
			inv.RunID, inv.Owner, inv.Fence, inv.OperationID = old.ID, old.Owner, old.Fence, old.ID+"/working-state/1"
			if _, status := f.post(t, "worker", inv); status != 400 {
				t.Fatal("historical run admitted", status)
			}

		})
	}
}

func TestSourceToolReceiptLabelsLargeAndSkippedResults(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		t.Run(version, func(t *testing.T) {
			f := sourceSetupVersion(t, version)
			ctx := context.Background()
			raw := JSON(map[string]string{"source": strings.Repeat("synthetic 原始 evidence;", 1000)})
			calls := 0
			effects := f.effects(t)
			effects.tool = func(c context.Context, inv Invocation) ([]byte, error) {
				calls++
				_, err := f.db.Exec(c, "INSERT INTO calls(id,run_id,kind,request_hash,request,status,response,fence,attempt) VALUES($1,$2,'tool','fixture',$3,'completed',$4,$5,'source-fixture')", inv.OperationID, inv.RunID, JSON(inv), raw, inv.Fence)
				return raw, err
			}
			frame := stateFrame(1, "tool", JSON(map[string]any{"name": "synthetic_source", "arguments": map[string]any{}}))
			value, _, err := f.engine.Exchange(ctx, &f.r, frame, effects)
			if err != nil {
				t.Fatal(err)
			}
			var envelope struct {
				Result  string `json:"result"`
				Receipt struct {
					Operation string `json:"operation_id"`
					Committed bool   `json:"committed"`
				} `json:"receipt"`
			}
			if err = json.Unmarshal([]byte(value.(string)), &envelope); err != nil || !envelope.Receipt.Committed || envelope.Receipt.Operation != f.r.ID+"/dsh/1" {
				t.Fatal("tool source missing", envelope, err)
			}
			var link struct {
				Hash string `json:"artifact_hash"`
			}
			if err = json.Unmarshal([]byte(envelope.Result), &link); err != nil || !bytes.Equal(f.archive.bodies[link.Hash], raw) {
				t.Fatal("large source not retained", err)
			}
			if err = f.s.Constraints(ctx, f.r.ID, []string{"invalidate pending plan"}); err != nil {
				t.Fatal(err)
			}
			frame.ID = 2
			value, _, err = f.engine.Exchange(ctx, &f.r, frame, effects)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal([]byte(value.(string)), &envelope); err != nil || envelope.Receipt.Committed || !strings.Contains(envelope.Result, "skipped") || calls != 1 {
				t.Fatal("skipped tool got false source receipt", envelope, err, calls)
			}

		})
	}
}
