package runtime

import (
	"context"
	"errors"
	"math"
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

func TestModelBudgetInvalidUsageKeepsReservationAndTotalsDoNotWrap(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("https://example.invalid")
	snap.Agent.MaxModelAttempts = 0
	snap.Agent.ModelTokenBudget = 0
	submit(t, s, snap)
	r := claim(t, s, "usage-validation")
	request := []byte(`{"max_tokens":100,"messages":[]}`)
	var reservation int64
	for i, raw := range []string{`{"usage":{}}`, `{"usage":{"prompt_tokens":20}}`, `{"usage":{"prompt_tokens":null,"completion_tokens":0}}`,
		`{"usage":{"prompt_tokens":20,"completion_tokens":1,"prompt_cache_hit_tokens":30}}`,
		`{"usage":{"prompt_tokens":20,"completion_tokens":1,"prompt_cache_hit_tokens":-1}}`,
		`{"usage":{"prompt_tokens":20.5,"completion_tokens":1}}`} {
		id := r.ID + "/" + ID()
		attempt, _, e := s.BeginModel(ctx, r, id, request)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.EndModel(ctx, r, id, attempt, []byte(raw), nil); e != nil {
			t.Fatal(e)
		}
		// jsonb has its own stable serialization; use the retained request, as
		// the family ledger does, rather than the compact fixture input bytes.
		var stored []byte
		if e = s.DB.QueryRow(ctx, "SELECT request FROM model_attempts WHERE id=$1", attempt).Scan(&stored); e != nil {
			t.Fatal(e)
		}
		reservation += int64(len(stored) + 100 + 512)
		u, e := s.Usage(ctx, r.ID)
		if e != nil || u.Attempts != i+1 || u.UnknownUsageAttempts != i+1 || u.AccountedTokens != reservation {
			t.Fatal("invalid usage consumed zero", u, e)
		}
	}
	for range 2 {
		id := r.ID + "/" + ID()
		attempt, _, e := s.BeginModel(ctx, r, id, request)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.EndModel(ctx, r, id, attempt, []byte(`{"usage":{"prompt_tokens":6000000000000000000,"completion_tokens":1}}`), nil); e != nil {
			t.Fatal(e)
		}
	}
	u, e := s.Usage(ctx, r.ID)
	if e != nil || u.AccountedTokens != math.MaxInt64 || u.PromptTokens != math.MaxInt64 {
		t.Fatal("family totals wrapped", u, e)
	}
	snap.Agent.ModelTokenBudget = 500000
	if _, e = s.DB.Exec(ctx, "UPDATE runs SET snapshot=$2 WHERE id=$1", r.ID, JSON(snap)); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.BeginModel(ctx, r, r.ID+"/after-overflow", request); e == nil || !strings.Contains(e.Error(), "token budget exhausted") {
		t.Fatal("overflow reopened budget", e)
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
