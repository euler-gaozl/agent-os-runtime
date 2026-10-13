package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func workingFixture(t *testing.T) (*Store, Run, Invocation) {
	t.Helper()
	db, role, worker := fixtureDatabase(t)
	if err := Migrate(context.Background(), db, role, worker); err != nil {
		t.Fatal(err)
	}
	s := NewStore(rolePool(t, db, role), nil, "")
	submit(t, s, basicSnapshot("https://fixture.invalid"))
	r := claim(t, s, "working-state-fixture")
	source := invocation(r)
	if _, err := s.LocalTool(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	return s, r, source
}
func workingInput(r Run, source Invocation, key string, revision int64) Invocation {
	in := WorkingStateChange{ExpectedRevision: revision, Reason: "Synthetic observations separated from plans", State: WorkingSet{
		Format: 1, Phase: "compare actual inventory with planned supply",
		Questions: []WorkingQuestion{{ID: "q1", Text: "Which quantity is an actual observation?", Status: "open"}},
		Observations: []WorkingObservation{
			{ID: "actual", Kind: "observation", Text: "Synthetic source: 17 units actual on 2026-10-01", Receipts: []string{source.OperationID}},
			{ID: "planned", Kind: "plan", Text: "Synthetic source: 20 units planned for 2026-11-01", Receipts: []string{source.OperationID}},
		},
		Conflicts: []WorkingConflict{{ID: "conflict", Text: "These are different reference periods", Observations: []string{"actual", "planned"}}},
		NextSteps: []WorkingStep{{ID: "next", Text: "Read source before using either number", Status: "pending"}},
	}}
	return Invocation{RunID: r.ID, Owner: r.Owner, Fence: r.Fence, RuleRevision: r.RuleRevision, OperationID: r.ID + "/working-state/" + key, Name: "working_state_update", Arguments: JSON(in)}
}
func requireStateStatus(t *testing.T, err error, code int) {
	t.Helper()
	var status *servicehttp.StatusError
	if !errors.As(err, &status) || status.Status != code {
		t.Fatalf("expected state status %d, got %v", code, err)
	}
}
func TestWorkingStateReceiptHistoryAndOldSaveRetention(t *testing.T) {
	s, r, source := workingFixture(t)
	ctx := context.Background()
	empty, err := s.WorkingState(ctx, r.ID, 0)
	if err != nil || empty.State != nil || empty.Revision != 0 || empty.Task != r.Task {
		t.Fatal("empty ledger or immutable task projection", empty, err)
	}
	inv := workingInput(r, source, "first", 0)
	anotherSource := source
	anotherSource.OperationID += "/second"
	if _, err = s.LocalTool(ctx, anotherSource); err != nil {
		t.Fatal(err)
	}
	change, _, err := workingChange(inv.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	change.State.Observations[0].Receipts = append(change.State.Observations[0].Receipts, anotherSource.OperationID)
	inv.Arguments = JSON(change)
	first, err := s.commitWorkingState(ctx, inv)
	if err != nil || first.Revision != 1 {
		t.Fatal(first, err)
	}
	// Discard the write response, then retrieve/replay the stable receipt.
	recovered, err := s.WorkingReceipt(ctx, r.ID, inv.OperationID)
	if err != nil || recovered != first {
		t.Fatal("committed response not recovered", err)
	}
	second := workingInput(r, source, "second", 1)
	if receipt, err := s.commitWorkingState(ctx, second); err != nil || receipt.Revision != 2 {
		t.Fatal(receipt, err)
	}
	if replayed, err := s.commitWorkingState(ctx, inv); err != nil || replayed != first {
		t.Fatal("replay returned latest state instead of original receipt", err)
	}
	changed := inv
	changed.Arguments = second.Arguments
	_, err = s.commitWorkingState(ctx, changed)
	requireStateStatus(t, err, 409)
	// Legacy Save replaces its entire RunState. The separate ledger survives.
	r.State.Result = "old engine checkpoint"
	if err = s.Save(ctx, r, "running"); err != nil {
		t.Fatal(err)
	}
	old, err := s.WorkingState(ctx, r.ID, 1)
	if err != nil || old.Revision != 1 || canonicalHash(JSON(old.State)) != first.StateSHA256 || len(old.Evidence) != 2 || old.Evidence[0].OperationID != source.OperationID || old.Evidence[1].OperationID != anotherSource.OperationID {
		t.Fatal("version, evidence or old Save retention failed", old, err)
	}
	for _, evidence := range old.Evidence {
		var response []byte
		if err = s.DB.QueryRow(ctx, "SELECT response FROM calls WHERE id=$1", evidence.OperationID).Scan(&response); err != nil || canonicalHash(response) != evidence.ResponseSHA256 {
			t.Fatal("recorded source content hash mismatch", err)
		}
	}
	var count int
	if err = s.DB.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id=$1 AND kind='working_state_committed'", r.ID).Scan(&count); err != nil || count != 2 {
		t.Fatal("duplicate/missing state audit", count, err)
	}
	if err = s.Constraints(ctx, r.ID, []string{"new authoritative user correction"}); err != nil {
		t.Fatal(err)
	}
	latest, err := s.WorkingState(ctx, r.ID, 0)
	if err != nil || !latest.Stale || latest.RuleRevision == latest.SourceRuleRevision || latest.Constraints[0] != "new authoritative user correction" || latest.Task != r.Task {
		t.Fatal("constraint change not projected distinctly", latest, err)
	}
}

func TestWorkingStateConcurrentCAS(t *testing.T) {
	s, r, source := workingFixture(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, key := range []string{"one", "two"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.commitWorkingState(context.Background(), workingInput(r, source, key, 0))
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	passed := 0
	for err := range results {
		if err == nil {
			passed++
		} else {
			requireStateStatus(t, err, 409)
		}
	}
	if passed != 1 {
		t.Fatal("concurrent replacements not serialized", passed)
	}
}

func TestWorkingStateFenceCancellationAndReceiptLookup(t *testing.T) {
	s, r, source := workingFixture(t)
	ctx := context.Background()
	inv := workingInput(r, source, "lost-response", 0)
	saved, err := s.commitWorkingState(ctx, inv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", r.ID); err != nil {
		t.Fatal(err)
	}
	replacement := claim(t, s, "replacement")
	if replacement.ID != r.ID || replacement.Fence <= r.Fence {
		t.Fatal("replacement fixture did not advance fence")
	}
	_, err = s.commitWorkingState(ctx, workingInput(r, source, "stale", 1))
	if !errors.Is(err, ErrFence) {
		t.Fatal("stale owner accepted", err)
	}
	inv.Owner, inv.Fence = replacement.Owner, replacement.Fence
	if replayed, err := s.commitWorkingState(ctx, inv); err != nil || replayed != saved {
		t.Fatal("owner/fence incorrectly entered idempotency hash", err)
	}
	if err = s.Constraints(ctx, r.ID, []string{"new rules"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.commitWorkingState(ctx, workingInput(replacement, source, "old-rules", 1)); err == nil || !strings.Contains(err.Error(), "constraints changed") {
		t.Fatal("stale user rules accepted", err)
	}
	if err = s.Cancel(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	_, err = s.commitWorkingState(ctx, inv)
	if !errors.Is(err, ErrFence) {
		t.Fatal("cancelled run allowed mutation/replay through write path", err)
	}
	if got, err := s.WorkingReceipt(ctx, r.ID, inv.OperationID); err != nil || got != saved {
		t.Fatal("cancellation hid committed response", err)
	}
}

func TestWorkingStateRejectsForeignAndUncommittedEvidence(t *testing.T) {
	s, r, source := workingFixture(t)
	ctx := context.Background()
	otherID := submit(t, s, basicSnapshot("https://fixture.invalid"))
	other := claim(t, s, "other")
	if other.ID != otherID {
		t.Fatal("wrong other run")
	}
	foreign := invocation(other)
	if _, err := s.LocalTool(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	mixed := workingInput(r, source, "mixed", 0)
	change, _, err := workingChange(mixed.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	change.State.Observations[0].Receipts = append(change.State.Observations[0].Receipts, foreign.OperationID)
	mixed.Arguments = JSON(change)
	_, err = s.commitWorkingState(ctx, mixed)
	requireStateStatus(t, err, 400)
	for _, ref := range []string{foreign.OperationID, r.ID + "/missing"} {
		inv := workingInput(r, source, ID(), 0)
		inv.Arguments = []byte(strings.ReplaceAll(string(inv.Arguments), source.OperationID, ref))
		_, err := s.commitWorkingState(ctx, inv)
		requireStateStatus(t, err, 400)
	}
	for _, statement := range []string{"UPDATE calls SET status='unknown' WHERE id=$1", "UPDATE calls SET status='completed',kind='model' WHERE id=$1"} {
		if _, err := s.DB.Exec(ctx, statement, source.OperationID); err != nil {
			t.Fatal(err)
		}
		_, err := s.commitWorkingState(ctx, workingInput(r, source, ID(), 0))
		requireStateStatus(t, err, 400)
	}
	view, err := s.WorkingState(ctx, r.ID, 0)
	if err != nil || view.Revision != 0 {
		t.Fatal("invalid evidence left a revision", err)
	}
}

func TestWorkingStateAtomicAuditAndRestrictedRoles(t *testing.T) {
	db, role, worker := fixtureDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, role, worker); err != nil {
		t.Fatal(err)
	}
	app, work := rolePool(t, db, role), rolePool(t, db, worker)
	s := NewStore(app, nil, "")
	submit(t, s, basicSnapshot("https://fixture.invalid"))
	r := claim(t, s, "atomic")
	source := invocation(r)
	if _, err := s.LocalTool(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `CREATE FUNCTION fail_state_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.kind='working_state_committed' THEN RAISE EXCEPTION 'controlled state event failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER fail_state_event BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION fail_state_event()`); err != nil {
		t.Fatal(err)
	}
	inv := workingInput(r, source, "atomic", 0)
	if _, err := s.commitWorkingState(ctx, inv); err == nil || !strings.Contains(err.Error(), "controlled state event failure") {
		t.Fatal("event fault not reached", err)
	}
	view, err := s.WorkingState(ctx, r.ID, 0)
	if err != nil || view.Revision != 0 {
		t.Fatal("partial state survived event rollback", err)
	}
	_, err = s.WorkingReceipt(ctx, r.ID, inv.OperationID)
	requireStateStatus(t, err, 404)
	if _, err = db.Exec(ctx, "DROP TRIGGER fail_state_event ON events"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.commitWorkingState(ctx, inv); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"UPDATE run_working_states SET body='{}'", "DELETE FROM run_working_states"} {
		_, err := app.Exec(ctx, sql)
		permissionDenied(t, err)
	}
	for _, sql := range []string{"SELECT * FROM run_working_states", "INSERT INTO run_working_states(run_id) VALUES('forbidden')", "UPDATE run_working_states SET body='{}'", "DELETE FROM run_working_states"} {
		_, err := work.Exec(ctx, sql)
		permissionDenied(t, err)
	}
	var payload string
	if err := db.QueryRow(ctx, "SELECT payload::text FROM events WHERE run_id=$1 AND kind='working_state_committed'", r.ID).Scan(&payload); err != nil || strings.Contains(payload, "Synthetic") || strings.Contains(payload, "17 units") {
		t.Fatal("working-state text leaked through event outbox", err)
	}
}

func TestWorkingStateReadHTTPDoesNotInheritGenericRunGrants(t *testing.T) {
	s, r, source := workingFixture(t)
	inv := workingInput(r, source, "http-receipt", 0)
	if _, err := s.commitWorkingState(context.Background(), inv); err != nil {
		t.Fatal(err)
	}
	peers := map[string]string{}
	for _, role := range []string{"control", "toolhub", "knowledge", "worker", "futures"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	server := httptest.NewServer((&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize}))
	defer server.Close()
	for _, suffix := range []string{"", "?revision=1", "?operation_id=" + url.QueryEscape(inv.OperationID)} {
		for role := range peers {
			req, err := http.NewRequest("GET", server.URL+"/v1/runs/"+r.ID+"/working-state"+suffix, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+testToken+role)
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			want := 403
			if role == "control" {
				want = 200
			}
			if res.StatusCode != want {
				t.Fatal("state read authorization", role, res.StatusCode, want)
			}
		}
	}
}

func TestWorkingStateRejectsInvalidSchemaAndBounds(t *testing.T) {
	r := Run{ID: "run"}
	inv := workingInput(r, Invocation{OperationID: "run/tool/source"}, "schema", 0)
	valid := string(inv.Arguments)
	cases := []string{
		valid[:len(valid)-1] + `,"state":{"format":1}}`,
		strings.Replace(valid, `"format":1`, `"format":1,"format":1`, 1),
		strings.Replace(valid, `"format":1`, `"format":2`, 1),
		strings.Replace(valid, `"format":1`, `"task":"override","format":1`, 1),
		strings.Replace(valid, `"kind":"plan"`, `"kind":"verified_fact"`, 1),
		strings.Replace(valid, `"id":"planned"`, `"id":"actual"`, 1),
		strings.Replace(valid, `"actual","planned"`, `"actual","missing"`, 1),
		strings.Replace(valid, `"expected_revision":0`, `"expected_revision":-1`, 1),
		valid + "{}",
		strings.Repeat("字", 12000),
		string([]byte{0xff}),
	}
	for i, raw := range cases {
		if _, _, err := workingChange([]byte(raw)); err == nil {
			t.Fatal("invalid schema accepted", i)
		}
	}
	if _, refs, err := workingChange(inv.Arguments); err != nil || len(refs) != 1 {
		t.Fatal("valid schema rejected", err)
	}
}
