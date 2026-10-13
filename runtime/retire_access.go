package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AccessProfile identifies the current execution-domain SQL privileges. It is
// not a deployment approval: the coordinator must verify all consumers and
// reject rollback to binaries requiring retired permissions before calling.
const AccessProfile = "runtime-access-v1"

var workerAccess = map[string]string{
	"runs": "SELECT,UPDATE", "calls": "SELECT,INSERT,UPDATE",
	"model_attempts": "SELECT,INSERT,UPDATE", "harness_exchanges": "SELECT,INSERT,UPDATE",
	"harness_archives": "SELECT,INSERT,UPDATE", "events": "SELECT,INSERT",
}

func grantWorkerAccess(ctx context.Context, tx pgx.Tx, role string) error {
	for _, table := range runtimeTables {
		if privileges := workerAccess[table]; privileges != "" {
			if _, e := tx.Exec(ctx, "GRANT "+privileges+" ON TABLE "+pgx.Identifier{"public", table}.Sanitize()+" TO "+pgx.Identifier{role}.Sanitize()); e != nil {
				return e
			}
		}
	}
	_, e := tx.Exec(ctx, "GRANT USAGE,SELECT ON SEQUENCE public.events_seq_seq TO "+pgx.Identifier{role}.Sanitize())
	return e
}

// RetireLegacyAccess contracts only this owner's tables and sequences. Normal
// Migrate remains expand-only. RESTRICT preserves downstream grants by refusing
// to cascade; any validation failure rolls the entire transaction back.
// Foreign roles retain their own schema/table grants and login configuration.
// Only non-administrative leaf roles are supported; inherited/SET ROLE paths
// must be resolved by the operator, never silently revoked across domains.
func RetireLegacyAccess(ctx context.Context, db *pgxpool.Pool, runtimeRole, workerRole string, foreignRoles []string) error {
	if runtimeRole == "" || workerRole == "" || runtimeRole == workerRole || len(foreignRoles) > 16 {
		return errors.New("distinct Runtime and Worker roles and at most 16 foreign roles required")
	}
	seen := map[string]bool{runtimeRole: true, workerRole: true}
	for _, role := range foreignRoles {
		if role == "" || seen[role] {
			return errors.New("foreign roles must be nonempty, distinct, and outside the execution identities")
		}
		seen[role] = true
	}
	tx, e := db.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SET LOCAL search_path=pg_catalog; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='15s'; SELECT pg_advisory_xact_lock(8843921)"); e != nil {
		return e
	}
	roles := append([]string{workerRole}, foreignRoles...)
	all := append([]string{runtimeRole}, roles...)
	for _, role := range all {
		var suitable bool
		if e = tx.QueryRow(ctx, `SELECT NOT (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls OR rolreplication)
 AND rolname<>current_user AND NOT EXISTS(SELECT 1 FROM pg_auth_members a WHERE a.member=r.oid)
 AND NOT has_schema_privilege(r.oid,'public','CREATE')
 FROM pg_roles r WHERE rolname=$1`, role).Scan(&suitable); e != nil {
			return errors.New("all selected service roles must exist")
		}
		if !suitable {
			return errors.New("service roles must be non-administrative leaf roles without public schema creation")
		}
	}
	var busy bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND usename=ANY($1) AND pid<>pg_backend_pid()) OR EXISTS(SELECT 1 FROM pg_prepared_xacts WHERE database=current_database() AND owner=ANY($1))", all).Scan(&busy); e != nil {
		return e
	}
	if busy {
		return errors.New("selected consumers still have database sessions or prepared transactions")
	}
	var defaults bool
	if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a
 WHERE d.defaclnamespace IN (0,'public'::regnamespace) AND d.defaclobjtype IN ('r','S')
 AND (a.grantee=0 OR a.grantee IN (SELECT oid FROM pg_roles WHERE rolname=ANY($1))))`, roles).Scan(&defaults); e != nil {
		return e
	}
	if defaults {
		return errors.New("default ACLs can restore retired access; resolve them explicitly before contraction")
	}
	// New connections must also be prevented by the external maintenance window.
	// The connection check is a guard, not a cross-host deployment fence.
	for _, table := range runtimeTables {
		var kind, owner string
		if e = tx.QueryRow(ctx, "SELECT c.relkind::text,pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=$1", table).Scan(&kind, &owner); e != nil {
			return errors.New("current Runtime schema required before retirement")
		}
		if kind != "r" || seen[owner] {
			return errors.New("execution tables must be ordinary tables owned by a separate migration identity")
		}
	}
	sequences := []string{"events_seq_seq", "note_audits_seq_seq"}
	for _, sequence := range sequences {
		var kind, owner string
		if e = tx.QueryRow(ctx, "SELECT c.relkind::text,pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=$1", sequence).Scan(&kind, &owner); e != nil {
			return errors.New("current Runtime sequences required before retirement")
		}
		if kind != "S" || seen[owner] {
			return errors.New("execution sequences must have a separate migration owner")
		}
	}
	for _, role := range roles {
		for _, table := range runtimeTables {
			if _, e = tx.Exec(ctx, "REVOKE ALL PRIVILEGES ON TABLE "+pgx.Identifier{"public", table}.Sanitize()+" FROM "+pgx.Identifier{role}.Sanitize()+" RESTRICT"); e != nil {
				return e
			}
		}
		for _, sequence := range sequences {
			if _, e = tx.Exec(ctx, "REVOKE ALL PRIVILEGES ON SEQUENCE "+pgx.Identifier{"public", sequence}.Sanitize()+" FROM "+pgx.Identifier{role}.Sanitize()+" RESTRICT"); e != nil {
				return e
			}
		}
	}
	if e = grantWorkerAccess(ctx, tx, workerRole); e != nil {
		return e
	}
	for _, role := range roles {
		for _, table := range runtimeTables {
			want := ""
			if role == workerRole {
				want = workerAccess[table]
			}
			for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"} {
				var allowed, grantOption bool
				if e = tx.QueryRow(ctx, "SELECT has_table_privilege($1,$2,$3),has_table_privilege($1,$2,$3||' WITH GRANT OPTION')", role, "public."+table, privilege).Scan(&allowed, &grantOption); e != nil {
					return e
				}
				expected := containsPrivilege(want, privilege)
				if allowed != expected || grantOption {
					return fmt.Errorf("effective table permissions differ from %s; inspect PUBLIC and grantor paths", AccessProfile)
				}
				if containsPrivilege("SELECT,INSERT,UPDATE,REFERENCES", privilege) {
					var column, columnGrant bool
					if e = tx.QueryRow(ctx, "SELECT has_any_column_privilege($1,$2,$3),has_any_column_privilege($1,$2,$3||' WITH GRANT OPTION')", role, "public."+table, privilege).Scan(&column, &columnGrant); e != nil {
						return e
					}
					if column != expected || columnGrant {
						return errors.New("effective column permissions remain outside the execution profile")
					}
				}
			}
		}
		for _, sequence := range sequences {
			for _, privilege := range []string{"USAGE", "SELECT", "UPDATE"} {
				var allowed, grantOption bool
				if e = tx.QueryRow(ctx, "SELECT has_sequence_privilege($1,$2,$3),has_sequence_privilege($1,$2,$3||' WITH GRANT OPTION')", role, "public."+sequence, privilege).Scan(&allowed, &grantOption); e != nil {
					return e
				}
				want := role == workerRole && sequence == "events_seq_seq" && privilege != "UPDATE"
				if allowed != want || grantOption {
					return errors.New("effective sequence permissions remain outside the execution profile")
				}
			}
		}
	}
	return tx.Commit(ctx)
}

func containsPrivilege(list, privilege string) bool {
	for _, p := range strings.Split(list, ",") {
		if p == privilege {
			return true
		}
	}
	return false
}
