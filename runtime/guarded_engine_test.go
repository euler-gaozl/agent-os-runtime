package runtime

import (
	"context"
	"errors"
	"github.com/euler-gaozl/agent-os-runtime/execution"
	"testing"
	"time"
)

func TestGuardedFinalOutputAtomicAndReplay(t *testing.T) {
	f := finalSetup(t, EngineDSHGuarded)
	f.prepare(t)
	f.selectDraft(t)
	f.checkpoint(t, false)
	frame := f.finishFrame()
	value, done := f.exchange(t, frame, f.effects(t))
	if !done {
		t.Fatal("guarded finish rejected", string(JSON(value)))
	}
	r, e := f.s.Run(context.Background(), f.r.ID)
	if e != nil || r.Engine != EngineDSHGuarded || r.Status != "completed" || r.State.Result != f.draft {
		t.Fatal("guarded original not retained", e)
	}
	before := f.models
	again, done := f.exchange(t, frame, f.effects(t))
	if !done || canonicalHash(JSON(value)) != canonicalHash(JSON(again)) || f.models != before {
		t.Fatal("guarded completion replay changed")
	}
	if e = f.engine.Save(context.Background(), f.r, "completed"); e == nil {
		t.Fatal("generic save bypassed receipt selection")
	}
}

func TestGuardedV6AndV7AreBothAdmitted(t *testing.T) {
	if !execution.IsFinalEngine(execution.EngineDSHGuarded) || !execution.IsSourceEngine(execution.EngineDSHGuarded) {
		t.Fatal("v7 final/source semantics missing")
	}
	if !execution.IsFinalEngine(execution.EngineDSHGuardedV6) || !execution.IsSourceEngine(execution.EngineDSHGuardedV6) {
		t.Fatal("v6 final/source semantics missing")
	}
	s, _ := testStore(t)
	if _, e := execution.NewSourceVersions(s.DB, &archiveFixture{}, []string{execution.EngineDSHGuarded}); e != nil {
		t.Fatal("v7 guarded admission failed", e)
	}
	if _, e := execution.NewSourceVersions(s.DB, &archiveFixture{}, []string{execution.EngineDSHGuardedV6}); e != nil {
		t.Fatal("v6 guarded recovery admission failed", e)
	}
}

func TestGuardedAdmissionAndExplicitWorkerOptIn(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if e := s.ConfigureSubmissionEngine(EngineDSHGuarded); e != nil {
		t.Fatal(e)
	}
	snap := nativeSnapshot() // Native Agents without final policy retain guarded argument protection.
	id, e := s.Submit(ctx, "guarded-native", "read source", snap, nil)
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.Run(ctx, id)
	if e != nil || r.Engine != EngineDSHGuarded {
		t.Fatal("native Agent lost guarded engine", e)
	}
	if e = s.ConfigureSubmissionEngine(EngineDSHFinal); e != nil {
		t.Fatal(e)
	}
	retried, e := s.Submit(ctx, "guarded-native", "read source", snap, nil)
	if e != nil || retried != id {
		t.Fatal("retry replaced run", e)
	}
	r, e = s.Run(ctx, id)
	if e != nil || r.Engine != EngineDSHGuarded {
		t.Fatal("deployment switch rewrote immutable engine", e)
	}
	old, e := execution.NewSourceVersions(s.DB, &archiveFixture{}, []string{EngineDSHFinal})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = old.Claim(ctx, "old-final-only", time.Minute); !errors.Is(e, execution.ErrNoWork) {
		t.Fatal("old Worker claimed guarded work")
	}
	fresh, e := execution.NewSourceVersions(s.DB, &archiveFixture{}, []string{EngineDSHGuarded})
	if e != nil {
		t.Fatal(e)
	}
	claimed, e := fresh.Claim(ctx, "guarded-worker", time.Minute)
	if e != nil || claimed.ID != id {
		t.Fatal("explicit v6 Worker could not claim", e)
	}
}
