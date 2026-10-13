package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
)

func TestSourceVersionClaimsRequireExactOptIn(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		for _, config := range []string{"historical", "v1-api", "v1", "v2", "both"} {
			t.Run(version+"/"+config, func(t *testing.T) {
				f := sourceSetupVersion(t, version)
				ctx := context.Background()
				if _, err := f.db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", f.r.ID); err != nil {
					t.Fatal(err)
				}
				e := execution.New(f.worker, f.archive)
				var err error
				switch config {
				case "v1-api":
					e = execution.NewSourceEnabled(f.worker, f.archive)
				case "v1":
					e, err = execution.NewSourceVersions(f.worker, f.archive, []string{EngineDSHSource})
				case "v2":
					e, err = execution.NewSourceVersions(f.worker, f.archive, []string{EngineDSHResident})
				case "both":
					e, err = execution.NewSourceVersions(f.worker, f.archive, []string{EngineDSHSource, EngineDSHResident})
				}
				if err != nil {
					t.Fatal(err)
				}
				r, err := e.Claim(ctx, "explicit-version", time.Minute)
				allowed := config == "both" || (version == EngineDSHSource && (config == "v1" || config == "v1-api")) || (version == EngineDSHResident && config == "v2")
				if allowed {
					if err != nil || r.Engine != version || r.Fence <= f.r.Fence {
						t.Fatal("wrong engine claim", r.Engine, err)
					}
				} else if !errors.Is(err, execution.ErrNoWork) {
					t.Fatal("unconfigured engine was claimed", r.Engine, err)
				}
			})
		}
	}
}

func TestSourceVersionConfigurationRejectsUnknownAndDuplicate(t *testing.T) {
	for _, versions := range [][]string{{"future"}, {EngineDSHEvidence}, {EngineDSHSource, EngineDSHSource}, {EngineDSHResident, EngineDSHResident}} {
		if _, err := execution.NewSourceVersions(nil, nil, versions); err == nil {
			t.Fatal("invalid source version list accepted", versions)
		}
	}
}

func TestExchangeRejectsEngineMismatchBeforeIntentAndEffects(t *testing.T) {
	for _, version := range []string{EngineDSHSource, EngineDSHResident} {
		t.Run(version, func(t *testing.T) {
			f := sourceSetupVersion(t, version)
			other := f.r
			if version == EngineDSHSource {
				other.Engine = EngineDSHResident
			} else {
				other.Engine = EngineDSHSource
			}
			called := false
			effects := f.effects(t)
			effects.state = func(context.Context, Invocation) ([]byte, error) { called = true; return []byte(`{}`), nil }
			if _, _, err := f.engine.Exchange(context.Background(), &other, stateFrame(1, "working_state_read", []byte(`{}`)), effects); err == nil {
				t.Fatal("mismatched engine accepted")
			}
			var exchanges, archives int
			if err := f.db.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM harness_exchanges WHERE run_id=$1),(SELECT count(*) FROM harness_archives WHERE run_id=$1)", f.r.ID).Scan(&exchanges, &archives); err != nil {
				t.Fatal(err)
			}
			if called || exchanges != 0 || archives != 0 {
				t.Fatal("engine mismatch caused durable intent or effect", exchanges, archives, called)
			}
		})
	}
}
