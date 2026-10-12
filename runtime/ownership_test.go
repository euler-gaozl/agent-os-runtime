package runtime

import (
	"context"
	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"testing"
	"time"
)

func TestOwnerMigrationRestrictedRuntimeAndWorker(t *testing.T) {
	db, role, worker := fixtureDatabase(t)
	ctx := context.Background()
	if Readiness(ctx, db) == nil || execution.Readiness(ctx, db) == nil {
		t.Fatal("empty domain declared ready")
	}
	if _, e := db.Exec(ctx, "CREATE TABLE public.foreign_domain_sentinel(value text); INSERT INTO public.foreign_domain_sentinel VALUES('preserve')"); e != nil {
		t.Fatal(e)
	}
	for _, roles := range [][2]string{{role, role}, {role, "missing_p2_worker_role"}, {"missing_p2_runtime_role", worker}} {
		if e := Migrate(ctx, db, roles[0], roles[1]); e == nil {
			t.Fatal("invalid roles accepted")
		}
	}
	for range 2 {
		if e := Migrate(ctx, db, role, worker); e != nil {
			t.Fatal(e)
		}
	}
	app, work := rolePool(t, db, role), rolePool(t, db, worker)
	if e := Readiness(ctx, app); e != nil {
		t.Fatal(e)
	}
	if e := execution.Readiness(ctx, work); e != nil {
		t.Fatal(e)
	}
	for _, p := range []*pgxpool.Pool{app, work} {
		for _, query := range []string{"SELECT * FROM public.foreign_domain_sentinel", "CREATE TABLE public.must_not_create(id int)"} {
			_, e := p.Exec(ctx, query)
			permissionDenied(t, e)
		}
	}
	for _, query := range []string{"SELECT * FROM documents", "INSERT INTO runs(id) VALUES('forbidden')", "DELETE FROM calls", "SELECT * FROM note_admin_receipts", "UPDATE events SET kind='forbidden'", "SELECT * FROM instances", "SELECT * FROM app_runs"} {
		_, e := work.Exec(ctx, query)
		permissionDenied(t, e)
	}
	store := NewStore(app, nil, "")
	id := submit(t, store, basicSnapshot("https://fixture.invalid"))
	engine := execution.New(work, nil)
	r, e := engine.Claim(ctx, "restricted-worker", 10*time.Second)
	if e != nil || r.ID != id {
		t.Fatal(e)
	}
	req := []byte(`{"max_tokens":100,"messages":[]}`)
	op := r.ID + "/model/one"
	attempt, cached, e := engine.BeginModel(ctx, r, op, req)
	if e != nil || cached != nil {
		t.Fatal(e)
	}
	raw := []byte(`{"usage":{"prompt_tokens":20,"completion_tokens":5}}`)
	if e = engine.EndModel(ctx, r, op, attempt, raw, nil); e != nil {
		t.Fatal(e)
	}
	if _, cached, e = engine.BeginModel(ctx, r, op, req); e != nil || cached == nil || !jsonEqual(cached, raw) {
		t.Fatal("model cached receipt", e)
	}
	if _, e = store.LocalTool(ctx, invocation(r)); e != nil {
		t.Fatal("restricted note transaction", e)
	}
	if e = engine.Heartbeat(ctx, r, 10*time.Second); e != nil {
		t.Fatal(e)
	}
	r.State.Result = "fixture"
	if e = engine.Save(ctx, r, "completed"); e != nil {
		t.Fatal(e)
	}
	var count int
	if e = db.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename<>'foreign_domain_sentinel'").Scan(&count); e != nil || count != len(runtimeTables) {
		t.Fatal("wrong owner tables", count, e)
	}
}

func TestMigrationDDLFailureRollsBackAllObjects(t *testing.T) {
	db, role, worker := fixtureDatabase(t)
	saved := notesSQL
	notesSQL += "\nSELECT 1/0;"
	defer func() { notesSQL = saved }()
	if e := Migrate(context.Background(), db, role, worker); e == nil {
		t.Fatal("fault not reached")
	}
	var n int
	if e := db.QueryRow(context.Background(), "SELECT count(*) FROM pg_tables WHERE schemaname='public'").Scan(&n); e != nil || n != 0 {
		t.Fatal("partial migration persisted", n, e)
	}
}

func TestMigrationGrantFailureRollsBackEarlierGrants(t *testing.T) {
	db, role, worker := fixtureDatabase(t)
	ctx := context.Background()
	if e := Migrate(ctx, db, role, worker); e != nil {
		t.Fatal(e)
	}
	for _, r := range []string{role, worker} {
		if _, e := db.Exec(ctx, "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM "+pgx.Identifier{r}.Sanitize()+"; REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM "+pgx.Identifier{r}.Sanitize()); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := db.Exec(ctx, `CREATE FUNCTION fail_worker_grant() RETURNS event_trigger LANGUAGE plpgsql AS $$
 BEGIN IF position('GRANT SELECT,UPDATE ON TABLE' in current_query())>0 THEN RAISE EXCEPTION 'controlled worker grant failure'; END IF; END $$;
 CREATE EVENT TRIGGER fail_worker_grant ON ddl_command_end WHEN TAG IN ('GRANT') EXECUTE FUNCTION fail_worker_grant();`); e != nil {
		t.Fatal(e)
	}
	if e := Migrate(ctx, db, role, worker); e == nil || !strings.Contains(e.Error(), "controlled worker grant failure") {
		t.Fatal("fault not reached", e)
	}
	for _, r := range []string{role, worker} {
		for _, table := range runtimeTables {
			for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
				var yes bool
				if e := db.QueryRow(ctx, "SELECT has_table_privilege($1,$2,$3)", r, "public."+table, priv).Scan(&yes); e != nil || yes {
					t.Fatal("partial grant persisted", table, priv, e)
				}
			}
		}
	}
	var n int
	if e := db.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='public'").Scan(&n); e != nil || n != len(runtimeTables) {
		t.Fatal("existing tables lost", n, e)
	}
}
