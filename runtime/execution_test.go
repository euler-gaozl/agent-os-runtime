package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func basicSnapshot(endpoint string) Snapshot {
	return Snapshot{AgentRef: Ref{ID: "test", Revision: 1}, Agent: Agent{Name: "Test agent", Prompt: "Use recorded facts only.", Tools: []string{"note_save", "knowledge_search", "spawn_agent"}, Model: ModelConfig{BaseURL: endpoint, Model: "fixture-only"}, MaxSteps: 6, ContextBytes: 64000, MaxChildren: 2}}
}
func submit(t *testing.T, s *Store, snap Snapshot) string {
	t.Helper()
	id, e := s.Submit(context.Background(), ID(), "integration fixture; not research data", snap, []string{"Do not predict prices."})
	if e != nil {
		t.Fatal(e)
	}
	return id
}
func claim(t *testing.T, s *Store, owner string) Run {
	t.Helper()
	r, e := s.Claim(context.Background(), owner, 10*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func invocation(r Run) Invocation {
	return Invocation{RunID: r.ID, Owner: r.Owner, Fence: r.Fence, RuleRevision: r.RuleRevision, OperationID: r.ID + "/tool/1/save", Name: "note_save", Arguments: json.RawMessage(`{"kind":"fact","title":"Test observation","content":"fixture value 42","source":"test://fixture"}`)}
}

func TestConcurrentClaimsAndFencing(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for i := 0; i < 12; i++ {
		submit(t, s, basicSnapshot("http://127.0.0.1"))
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[string]Run{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := s.Claim(ctx, fmt.Sprint("worker-", i), 10*time.Second)
			if e != nil {
				t.Error(e)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if _, exists := claimed[r.ID]; exists {
				t.Error("duplicate claim")
			}
			claimed[r.ID] = r
		}(i)
	}
	wg.Wait()
	if len(claimed) != 12 {
		t.Fatal("not all runs claimed")
	}
	var old Run
	for _, r := range claimed {
		old = r
		break
	}
	_, e := s.DB.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", old.ID)
	if e != nil {
		t.Fatal(e)
	}
	newOwner, e := s.Claim(ctx, "replacement", 10*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if newOwner.ID != old.ID || newOwner.Fence <= old.Fence {
		t.Fatal("takeover did not advance fence")
	}
	if _, e = s.LocalTool(ctx, invocation(old)); !errors.Is(e, ErrFence) {
		t.Fatalf("stale worker effect accepted: %v", e)
	}
	if e = s.Save(ctx, old, "completed"); !errors.Is(e, ErrFence) {
		t.Fatalf("stale checkpoint accepted: %v", e)
	}
}

func TestAtomicToolEffectAndReceipt(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("http://127.0.0.1")
	key := ID()
	id, e := s.Submit(ctx, key, "same task", snap, nil)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.Submit(ctx, key, "same task", snap, nil)
	if e != nil || id != again {
		t.Fatal("submission is not idempotent", e)
	}
	if _, e = s.Submit(ctx, key, "different", snap, nil); e == nil {
		t.Fatal("key/input mismatch accepted")
	}
	r := claim(t, s, "one")
	inv := invocation(r)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := s.LocalTool(ctx, inv); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	var docs, calls int
	s.DB.QueryRow(ctx, "SELECT count(*) FROM documents").Scan(&docs)
	s.DB.QueryRow(ctx, "SELECT count(*) FROM calls").Scan(&calls)
	if docs != 1 || calls != 1 {
		t.Fatalf("duplicate effect: docs=%d calls=%d", docs, calls)
	}
	inv.Arguments = json.RawMessage(`{"kind":"fact","title":"Changed","content":"different","source":"test://fixture"}`)
	if _, e = s.LocalTool(ctx, inv); e == nil {
		t.Fatal("changed operation input accepted")
	}
}

func TestUnknownExternalIsNotBlindlyRetried(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("http://127.0.0.1")
	snap.Connectors = []Connector{{Name: "remote", Endpoint: "https://example.invalid/mcp", Protocol: "2025-11-25", Tools: []Tool{tool("write", "write fixture", `{"type":"object","additionalProperties":false}`)}}}
	submit(t, s, snap)
	r := claim(t, s, "one")
	inv := invocation(r)
	inv.Name = "mcp_remote_write"
	inv.Arguments = json.RawMessage(`{}`)
	var effects atomic.Int32
	execute := func(context.Context, Run) ([]byte, error) {
		effects.Add(1)
		return nil, errors.New("response lost after remote effect")
	}
	if _, e := s.ExternalTool(ctx, inv, execute); !errors.Is(e, ErrUnknown) {
		t.Fatal(e)
	}
	if _, e := s.ExternalTool(ctx, inv, execute); !errors.Is(e, ErrUnknown) {
		t.Fatal(e)
	}
	if effects.Load() != 1 {
		t.Fatal("unknown external effect repeated")
	}
	if e := s.Save(ctx, r, "waiting_unknown"); e != nil {
		t.Fatal(e)
	}
	if e := s.Resolve(ctx, r.ID, inv.OperationID, "completed", "remote audit receipt ABC", json.RawMessage(`{"receipt":"ABC"}`)); e != nil {
		t.Fatal(e)
	}
	r = claim(t, s, "two")
	inv.Owner = r.Owner
	inv.Fence = r.Fence
	result, e := s.ExternalTool(ctx, inv, execute)
	if e != nil || !strings.Contains(string(result), "ABC") || effects.Load() != 1 {
		t.Fatal("verified receipt not replayed", e)
	}
}

func TestCancellationAndChildSpawn(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("http://127.0.0.1")
	snap.Children = map[string]Snapshot{"child": basicSnapshot("http://127.0.0.1")}
	parentID := submit(t, s, snap)
	r := claim(t, s, "one")
	inv := invocation(r)
	inv.Name = "spawn_agent"
	inv.Arguments = json.RawMessage(`{"agent_id":"child","task":"test child"}`)
	a, e := s.LocalTool(ctx, inv)
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.LocalTool(ctx, inv)
	var first, second map[string]string
	_ = json.Unmarshal(a, &first)
	_ = json.Unmarshal(b, &second)
	var children int
	_ = s.DB.QueryRow(ctx, "SELECT count(*) FROM runs WHERE parent_id=$1", parentID).Scan(&children)
	if e != nil || first["child_run_id"] != second["child_run_id"] || children != 1 {
		t.Fatal("duplicate child spawn", e, children)
	}
	if e = s.Cancel(ctx, parentID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.LocalTool(ctx, invocation(r)); !errors.Is(e, ErrFence) {
		t.Fatal("write after cancellation", e)
	}
	var uncancelled int
	s.DB.QueryRow(ctx, "SELECT count(*) FROM runs WHERE NOT cancel_requested").Scan(&uncancelled)
	if uncancelled != 0 {
		t.Fatal("child cancellation not propagated")
	}
}

func TestConstraintRevisionRejectsOldPlan(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	submit(t, s, basicSnapshot("http://127.0.0.1"))
	r := claim(t, s, "one")
	if e := s.Constraints(ctx, r.ID, []string{"new correction"}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.LocalTool(ctx, invocation(r)); e == nil {
		t.Fatal("old plan ignored revised constraints")
	}
	if e := s.Save(ctx, r, "completed"); e == nil {
		t.Fatal("outdated final answer committed")
	}
	fresh, e := s.Run(ctx, r.ID)
	if e != nil || fresh.Constraints[0] != "new correction" {
		t.Fatal("constraint lost")
	}
}
