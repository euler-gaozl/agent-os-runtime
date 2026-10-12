package runtime

import "context"

func migrateHistoricalFixture(ctx context.Context, s *Store) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	for _, sql := range []string{migrationSQL, notesSQL} {
		if _, e = tx.Exec(ctx, sql); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
