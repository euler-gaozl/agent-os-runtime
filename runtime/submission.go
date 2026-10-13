package runtime

import (
	"context"
	"errors"

	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/jackc/pgx/v5"
)

// ConfigureSubmissionEngine is deployment configuration, set once before the
// HTTP server starts. Agent definitions and clients cannot select an engine.
// The source option requires a matching migrated Runtime and pinned Workers;
// accepting configuration here is not a deployment compatibility check.
func (s *Store) ConfigureSubmissionEngine(engine string) error {
	if engine == "" {
		engine = EngineDSHEvidence
	}
	if engine != EngineDSHEvidence && !isSourceEngine(engine) {
		return errors.New("DEFAULT_RUN_ENGINE must name the evidence-v1, source state-v1 or source resident-v2 engine")
	}
	s.submissionEngine = engine
	return nil
}

func (s *Store) defaultRunEngine() string {
	if s.submissionEngine == "" {
		return EngineDSHEvidence
	}
	return s.submissionEngine
}

func (s *Store) insertRoot(ctx context.Context, tx pgx.Tx, key, task string, snap Snapshot, pinned []string) (string, error) {
	return runstate.InsertRootRun(ctx, tx, key, task, snap, pinned, s.defaultRunEngine())
}

func (s *Store) Submit(ctx context.Context, key, task string, snap Snapshot, pinned []string) (string, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := s.insertRoot(ctx, tx, key, task, snap, pinned)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}
