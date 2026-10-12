package runtime

import (
	"context"
	_ "embed"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var migrationSQL string

//go:embed notes.sql
var notesSQL string

var runtimeTables = []string{"documents", "runs", "events", "calls", "model_attempts", "instances", "business_runs", "harness_exchanges", "app_runs", "harness_archives", "note_roots", "note_versions", "note_admin_receipts", "note_audits"}

// Migrate owns this domain only. Both roles already exist; DDL and grants
// commit atomically. It never moves legacy data or silently revokes old grants.
func Migrate(ctx context.Context, db *pgxpool.Pool, runtimeRole, workerRole string) error {
	if runtimeRole == "" || workerRole == "" || runtimeRole == workerRole {
		return errors.New("distinct existing Runtime and Worker roles required")
	}
	tx, e := db.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SET LOCAL search_path=public; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='120s'"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(8843921)"); e != nil {
		return e
	}
	for _, role := range []string{runtimeRole, workerRole} {
		var exists bool
		if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); e != nil {
			return e
		}
		if !exists {
			return errors.New("application role is not provisioned")
		}
	}
	for _, sql := range []string{migrationSQL, notesSQL} {
		if _, e = tx.Exec(ctx, sql); e != nil {
			return e
		}
	}
	grant := func(privilege, kind, name, role string) error {
		_, err := tx.Exec(ctx, "GRANT "+privilege+" ON "+kind+" "+pgx.Identifier{"public", name}.Sanitize()+" TO "+pgx.Identifier{role}.Sanitize())
		return err
	}
	for _, role := range []string{runtimeRole, workerRole} {
		if _, e = tx.Exec(ctx, "GRANT USAGE ON SCHEMA public TO "+pgx.Identifier{role}.Sanitize()); e != nil {
			return e
		}
	}
	for _, table := range runtimeTables {
		if e = grant("SELECT,INSERT,UPDATE,DELETE", "TABLE", table, runtimeRole); e != nil {
			return e
		}
	}
	// Worker cannot create runs/notes, reconcile effects or touch another domain.
	if e = grant("SELECT,UPDATE", "TABLE", "runs", workerRole); e != nil {
		return e
	}
	for _, table := range []string{"calls", "model_attempts", "harness_exchanges", "harness_archives"} {
		if e = grant("SELECT,INSERT,UPDATE", "TABLE", table, workerRole); e != nil {
			return e
		}
	}
	if e = grant("SELECT,INSERT", "TABLE", "events", workerRole); e != nil {
		return e
	}
	for _, role := range []string{runtimeRole, workerRole} {
		if e = grant("USAGE,SELECT", "SEQUENCE", "events_seq_seq", role); e != nil {
			return e
		}
	}
	if e = grant("USAGE,SELECT", "SEQUENCE", "note_audits_seq_seq", runtimeRole); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
