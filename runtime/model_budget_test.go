package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestFamilyBudgetSerializesParentAndChildAttempts(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("https://example.invalid")
	snap.Agent.MaxModelAttempts = 1
	root := submit(t, s, snap)
	parent := claim(t, s, "budget-parent")
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	_, e = insertRun(ctx, tx, ID(), "child", basicSnapshot("https://example.invalid"), &root, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	child := claim(t, s, "budget-child")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, r := range []Run{parent, child} {
		wg.Add(1)
		go func(r Run) {
			defer wg.Done()
			_, _, e := s.BeginModel(ctx, r, r.ID+"/budget", []byte(`{"max_tokens":100,"messages":[]}`))
			results <- e
		}(r)
	}
	wg.Wait()
	close(results)
	accepted, rejected := 0, 0
	for e := range results {
		if e == nil {
			accepted++
		} else if strings.Contains(e.Error(), "attempt budget exhausted") {
			rejected++
		} else {
			t.Fatal(e)
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("concurrent budget exceeded: accepted %d rejected %d", accepted, rejected)
	}
	u, e := s.Usage(ctx, child.ID)
	if e != nil || u.RootID != root || u.Attempts != 1 || u.UnknownUsageAttempts != 1 {
		t.Fatalf("family accounting incorrect: %+v %v", u, e)
	}
}
func TestModelBudgetCountsUnknownRetryButAllowsCachedReplay(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "cached", true: "lost_receipt"}[failed], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			snap := basicSnapshot("https://example.invalid")
			snap.Agent.MaxModelAttempts = 1
			submit(t, s, snap)
			r := claim(t, s, "budget-worker")
			req := []byte(`{"max_tokens":100,"messages":[]}`)
			a, _, e := s.BeginModel(ctx, r, r.ID+"/call", req)
			if e != nil {
				t.Fatal(e)
			}
			if !failed {
				if e = s.EndModel(ctx, r, r.ID+"/call", a, []byte(`{"usage":{"prompt_tokens":20,"completion_tokens":5}}`), nil); e != nil {
					t.Fatal(e)
				}
			}
			_, cached, e := s.BeginModel(ctx, r, r.ID+"/call", req)
			if failed {
				if e == nil || !strings.Contains(e.Error(), "attempt budget exhausted") {
					t.Fatalf("unknown attempt repeated: %v", e)
				}
			} else if e != nil || cached == nil {
				t.Fatalf("budget blocked committed replay: %v", e)
			}
		})
	}
}
func TestTokenBudgetStopsBeforeAttemptIsRegistered(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("https://example.invalid")
	snap.Agent.ModelTokenBudget = 100
	submit(t, s, snap)
	r := claim(t, s, "token-budget")
	_, _, e := s.BeginModel(ctx, r, r.ID+"/too-large", []byte(`{"max_tokens":100,"messages":[]}`))
	if e == nil || !strings.Contains(e.Error(), "token budget exhausted") {
		t.Fatal(errors.New("token budget did not reject request"))
	}
	u, e := s.Usage(ctx, r.ID)
	if e != nil || u.Attempts != 0 {
		t.Fatalf("rejected request consumed attempt: %+v %v", u, e)
	}
}
