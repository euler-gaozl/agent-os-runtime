package runtime

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	goRuntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func retirementRole(t *testing.T, db *pgxpool.Pool, name string) string {
	t.Helper()
	_, e := db.Exec(context.Background(), "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" LOGIN PASSWORD 'fixture-only'")
	if e != nil {
		t.Fatal(e)
	}
	// fixtureDatabase owns cleanup after the isolated database is dropped.
	return name
}

func retirementLogin(t *testing.T, db *pgxpool.Pool, role string) *pgxpool.Pool {
	t.Helper()
	config := db.Config().Copy()
	config.ConnConfig.User, config.ConnConfig.Password = role, "fixture-only"
	config.MaxConns = 1
	p, e := pgxpool.NewWithConfig(context.Background(), config)
	if e != nil {
		t.Fatal("fixture login configuration failed")
	}
	t.Cleanup(p.Close)
	var session, effective string
	if e = p.QueryRow(context.Background(), "SELECT session_user,current_user").Scan(&session, &effective); e != nil || session != role || effective != role {
		t.Fatal("real service LOGIN required", e, session, effective)
	}
	return p
}

func retirementFixture(t *testing.T) (*pgxpool.Pool, string, string, string) {
	t.Helper()
	db, runtimeRole, worker := fixtureDatabase(t, "foreign", "parent", "child")
	ctx := context.Background()
	if e := Migrate(ctx, db, runtimeRole, worker); e != nil {
		t.Fatal(e)
	}
	foreign := retirementRole(t, db, runtimeRole+"_foreign")
	for _, role := range []string{runtimeRole, worker} {
		if _, e := db.Exec(ctx, "ALTER ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN PASSWORD 'fixture-only'"); e != nil {
			t.Fatal(e)
		}
	}
	_, e := db.Exec(ctx, `CREATE TABLE public.foreign_domain_sentinel(value text); INSERT INTO public.foreign_domain_sentinel VALUES('preserve');
 INSERT INTO documents(id,kind,title,content,source,metadata,scope) VALUES('retained','memory','fixture','keep original evidence','fixture://legacy','{}','agent-a')`)
	if e != nil {
		t.Fatal(e)
	}
	for _, role := range []string{worker, foreign} {
		_, e = db.Exec(ctx, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "+pgx.Identifier{role}.Sanitize()+"; GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "+pgx.Identifier{role}.Sanitize())
		if e != nil {
			t.Fatal(e)
		}
	}
	return db, runtimeRole, worker, foreign
}

func retirementACL(t *testing.T, db *pgxpool.Pool) string {
	t.Helper()
	var v string
	e := db.QueryRow(context.Background(), `SELECT jsonb_agg(jsonb_build_object('name',c.relname,'owner',c.relowner,'acl',c.relacl::text,
 'columns',(SELECT jsonb_agg(jsonb_build_object('name',a.attname,'acl',a.attacl::text) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0)) ORDER BY c.relname)::text
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','S')`).Scan(&v)
	if e != nil {
		t.Fatal(e)
	}
	return v
}

func TestRetireLegacyAccessPreservesNotesAndRestrictedExecution(t *testing.T) {
	db, runtimeRole, worker, foreign := retirementFixture(t)
	ctx := context.Background()
	legacy := retirementACL(t, db)
	if e := Migrate(ctx, db, runtimeRole, worker); e != nil {
		t.Fatal(e)
	}
	if legacy != retirementACL(t, db) {
		t.Fatal("normal migration contracted legacy access before explicit retirement")
	}
	for range 2 {
		if e := RetireLegacyAccess(ctx, db, runtimeRole, worker, []string{foreign}); e != nil {
			t.Fatal(e)
		}
	}
	before := retirementACL(t, db)
	if e := Migrate(ctx, db, runtimeRole, worker); e != nil {
		t.Fatal(e)
	}
	if before != retirementACL(t, db) {
		t.Fatal("expand migration widened contracted access")
	}
	app, work, other := retirementLogin(t, db, runtimeRole), retirementLogin(t, db, worker), retirementLogin(t, db, foreign)
	if e := Readiness(ctx, app); e != nil {
		t.Fatal(e)
	}
	if e := execution.Readiness(ctx, work); e != nil {
		t.Fatal(e)
	}
	for _, p := range []*pgxpool.Pool{work, other} {
		for _, query := range []string{"SELECT content FROM public.documents", "INSERT INTO public.documents(id) VALUES('forbidden')", "DELETE FROM public.calls", "INSERT INTO public.runs(id) VALUES('forbidden')", "SELECT nextval('public.note_audits_seq_seq')", "SELECT * FROM public.run_working_states"} {
			_, e := p.Exec(ctx, query)
			permissionDenied(t, e)
		}
		var value string
		if e := p.QueryRow(ctx, "SELECT value FROM public.foreign_domain_sentinel").Scan(&value); e != nil || value != "preserve" {
			t.Fatal("foreign data/permission changed", e)
		}
	}
	for _, query := range []string{"UPDATE public.events SET kind='forbidden'", "SELECT setval('public.events_seq_seq',1)", "SELECT * FROM public.instances"} {
		_, e := work.Exec(ctx, query)
		permissionDenied(t, e)
	}
	for _, query := range []string{"SELECT * FROM public.events", "SELECT * FROM public.runs", "SELECT nextval('public.events_seq_seq')"} {
		_, e := other.Exec(ctx, query)
		permissionDenied(t, e)
	}
	var original string
	if e := app.QueryRow(ctx, "SELECT content FROM public.documents WHERE id='retained'").Scan(&original); e != nil || original != "keep original evidence" {
		t.Fatal("legacy note changed", e)
	}
	store := NewStore(app, nil, "")
	id := submit(t, store, basicSnapshot("https://fixture.invalid"))
	engine := execution.New(work, nil)
	r, e := engine.Claim(ctx, "retired-permissions-worker", 10*time.Second)
	if e != nil || r.ID != id {
		t.Fatal("restricted claim failed", e)
	}
	op := id + "/model/one"
	attempt, cached, e := engine.BeginModel(ctx, r, op, []byte(`{"max_tokens":100,"messages":[]}`))
	if e != nil || cached != nil {
		t.Fatal(e)
	}
	if e = engine.EndModel(ctx, r, op, attempt, []byte(`{"usage":{"prompt_tokens":20,"completion_tokens":5}}`), nil); e != nil {
		t.Fatal(e)
	}
	first, e := store.LocalTool(ctx, invocation(r))
	if e != nil {
		t.Fatal(e)
	}
	second, e := store.LocalTool(ctx, invocation(r))
	if e != nil || !jsonEqual(first, second) {
		t.Fatal("committed tool replay changed", e)
	}
	if e = engine.Heartbeat(ctx, r, 10*time.Second); e != nil {
		t.Fatal(e)
	}
	r.State.Result = "fixture after permission contraction"
	if e = engine.Save(ctx, r, "completed"); e != nil {
		t.Fatal(e)
	}
	var notes int
	if e = app.QueryRow(ctx, "SELECT count(*) FROM public.documents").Scan(&notes); e != nil || notes != 2 {
		t.Fatal("note duplicated/lost", notes, e)
	}
}

func TestRetirementRefusesBypassesAndRollsBack(t *testing.T) {
	for _, name := range []string{"public-table", "public-column", "inherited", "set-only", "owner", "default-table", "default-sequence", "grant-dependency", "revoke-failure", "missing-table", "active-login", "administrative-role", "replication-role", "mixed-roles", "missing-role"} {
		t.Run(name, func(t *testing.T) {
			db, runtimeRole, worker, foreign := retirementFixture(t)
			ctx := context.Background()
			var expected string
			sql := func(q string) {
				t.Helper()
				if _, e := db.Exec(ctx, q); e != nil {
					t.Fatal(e)
				}
			}
			foreignRoles := []string{foreign}
			switch name {
			case "public-table":
				sql("GRANT SELECT ON public.documents TO PUBLIC")
				expected = "effective table"
			case "public-column":
				sql("GRANT SELECT(content) ON public.documents TO PUBLIC")
				expected = "effective column"
			case "inherited", "set-only":
				parent := retirementRole(t, db, runtimeRole+"_parent")
				sql("REVOKE ALL ON public.documents FROM " + pgx.Identifier{foreign}.Sanitize())
				sql("GRANT SELECT ON public.documents TO " + pgx.Identifier{parent}.Sanitize())
				options := " WITH INHERIT TRUE, SET TRUE"
				if name == "set-only" {
					options = " WITH INHERIT FALSE, SET TRUE"
				}
				sql("GRANT " + pgx.Identifier{parent}.Sanitize() + " TO " + pgx.Identifier{foreign}.Sanitize() + options)
				p := retirementLogin(t, db, foreign)
				_, direct := p.Exec(ctx, "SELECT content FROM public.documents")
				if name == "set-only" {
					permissionDenied(t, direct)
				} else if direct != nil {
					t.Fatal("inherited grant was not effective", direct)
				}
				if _, e := p.Exec(ctx, "SET ROLE "+pgx.Identifier{parent}.Sanitize()); e != nil {
					t.Fatal("real foreign login cannot reach parent", e)
				}
				var value string
				if e := p.QueryRow(ctx, "SELECT content FROM public.documents WHERE id='retained'").Scan(&value); e != nil || value != "keep original evidence" {
					t.Fatal("SET ROLE fixture missing reachable privilege", e)
				}
				p.Close()
				expected = "leaf roles"
			case "owner":
				sql("ALTER TABLE public.documents OWNER TO " + pgx.Identifier{foreign}.Sanitize())
				expected = "separate migration"
			case "default-table":
				sql("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO " + pgx.Identifier{foreign}.Sanitize())
				expected = "default ACLs"
			case "default-sequence":
				sql("ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO " + pgx.Identifier{worker}.Sanitize())
				expected = "default ACLs"
			case "grant-dependency":
				child := retirementRole(t, db, runtimeRole+"_child")
				sql("GRANT SELECT ON public.documents TO " + pgx.Identifier{worker}.Sanitize() + " WITH GRANT OPTION")
				p := retirementLogin(t, db, worker)
				if _, e := p.Exec(ctx, "GRANT SELECT ON public.documents TO "+pgx.Identifier{child}.Sanitize()); e != nil {
					t.Fatal(e)
				}
				p.Close()
				expected = "dependent privileges"
			case "revoke-failure":
				sql(`CREATE FUNCTION reject_second_revoke() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF current_query() LIKE '%ON TABLE "public"."runs"%' THEN RAISE EXCEPTION 'controlled partial revoke failure'; END IF; END $$;
CREATE EVENT TRIGGER reject_second_revoke ON ddl_command_end WHEN TAG IN ('REVOKE') EXECUTE FUNCTION reject_second_revoke()`)
				expected = "controlled partial revoke"
			case "missing-table":
				sql("ALTER TABLE public.run_working_states RENAME TO old_working_states")
				expected = "current Runtime schema"
			case "active-login":
				retirementLogin(t, db, worker)
				expected = "still have database sessions"
			case "administrative-role":
				sql("ALTER ROLE " + pgx.Identifier{foreign}.Sanitize() + " CREATEROLE")
				expected = "leaf roles"
			case "replication-role":
				sql("ALTER ROLE " + pgx.Identifier{foreign}.Sanitize() + " REPLICATION")
				expected = "leaf roles"
			case "mixed-roles":
				foreignRoles = []string{foreign, worker}
				expected = "distinct"
			case "missing-role":
				foreignRoles = []string{foreign + "_absent"}
				expected = "must exist"
			}
			before := retirementACL(t, db)
			e := RetireLegacyAccess(ctx, db, runtimeRole, worker, foreignRoles)
			if e == nil || !strings.Contains(e.Error(), expected) {
				t.Fatal("intended refusal not observed", name, e)
			}
			if before != retirementACL(t, db) {
				t.Fatal("failed retirement changed table/column/sequence ACLs", name)
			}
			var value string
			if e = db.QueryRow(ctx, "SELECT content FROM public.documents WHERE id='retained'").Scan(&value); e != nil || value != "keep original evidence" {
				t.Fatal("rejected retirement changed original data", e)
			}
		})
	}
}

func TestRetirementRemovesDirectColumnGrants(t *testing.T) {
	db, runtimeRole, worker, foreign := retirementFixture(t)
	ctx := context.Background()
	if _, e := db.Exec(ctx, "REVOKE ALL ON public.documents FROM "+pgx.Identifier{foreign}.Sanitize()+"; GRANT SELECT(content) ON public.documents TO "+pgx.Identifier{foreign}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if e := RetireLegacyAccess(ctx, db, runtimeRole, worker, []string{foreign}); e != nil {
		t.Fatal(e)
	}
	p := retirementLogin(t, db, foreign)
	_, e := p.Exec(ctx, "SELECT content FROM public.documents")
	var denied *pgconn.PgError
	if !errors.As(e, &denied) || denied.Code != "42501" {
		t.Fatal("direct column access survived", e)
	}
}

// ConnConfig.ConnString retains the original admin DSN even after the fixture
// changes ConnConfig.Database. Rewrite both path and query database selectors,
// then validate the parsed result before any CLI can execute against it.
func retirementDatabaseURL(raw, database string) (string, error) {
	connection, e := url.Parse(raw)
	if e != nil || (connection.Scheme != "postgres" && connection.Scheme != "postgresql") || !strings.HasPrefix(database, "p2_runtime_") {
		return "", errors.New("invalid isolated fixture URL")
	}
	query, e := url.ParseQuery(connection.RawQuery)
	if e != nil {
		return "", errors.New("invalid fixture query parameters")
	}
	query.Del("dbname")
	query.Del("database")
	connection.Path, connection.RawPath, connection.RawQuery = "/"+database, "", query.Encode()
	dsn := connection.String()
	config, e := pgxpool.ParseConfig(dsn)
	if e != nil || config.ConnConfig.Database != database {
		return "", errors.New("fixture database override not isolated")
	}
	return dsn, nil
}

func TestRetirementCommandDatabaseIsolation(t *testing.T) {
	for _, query := range []string{"sslmode=disable", "sslmode=disable&dbname=original", "sslmode=disable&database=original", "sslmode=disable&dbname=original&database=other&dbname=third", "sslmode=disable&db%6eame=original"} {
		dsn, e := retirementDatabaseURL("postgres://fixture:fixture-only@127.0.0.1:55591/original?"+query, "p2_runtime_isolated")
		if e != nil {
			t.Fatal("isolated URL construction failed")
		}
		config, e := pgxpool.ParseConfig(dsn)
		if e != nil || config.ConnConfig.Database != "p2_runtime_isolated" || config.ConnConfig.User != "fixture" || config.ConnConfig.Host != "127.0.0.1" || config.ConnConfig.Port != 55591 {
			t.Fatal("fixture database or login target changed")
		}
	}
	if _, e := retirementDatabaseURL("postgres://fixture@127.0.0.1/original", "original"); e == nil {
		t.Fatal("non-fixture database accepted")
	}
}

func TestRetirementCommandRepeatAndSanitizedFailure(t *testing.T) {
	db, runtimeRole, worker, foreign := retirementFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := ""
	if goRuntime.GOOS == "windows" {
		suffix = ".exe"
	}
	binary := filepath.Join(t.TempDir(), "retire-access"+suffix)
	build := exec.CommandContext(ctx, filepath.Join(goRuntime.GOROOT(), "bin", "go"+suffix), "build", "-mod=vendor", "-trimpath", "-o", binary, "../cmd/retire-access")
	if b, e := build.CombinedOutput(); e != nil {
		t.Fatalf("retirement command build failed: %s", b)
	}
	connection, e := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal("fixture admin URL invalid")
	}
	query := connection.Query()
	query.Set("dbname", "must_never_target_legacy")
	query.Set("database", "must_never_target_legacy")
	connection.RawQuery = query.Encode()
	cliConnection, e := retirementDatabaseURL(connection.String(), db.Config().ConnConfig.Database)
	if e != nil {
		t.Fatal("isolated CLI URL construction failed")
	}
	// Verify the actual server-side database as well as the parsed DSN. Do not
	// expose the original or rewritten URL on a failed assertion.
	probe, e := pgxpool.New(ctx, cliConnection)
	if e != nil {
		t.Fatal("isolated CLI connection invalid")
	}
	var actual string
	e = probe.QueryRow(ctx, "SELECT current_database()").Scan(&actual)
	probe.Close()
	if e != nil || actual != db.Config().ConnConfig.Database {
		t.Fatal("CLI does not reach the isolated fixture database")
	}
	run := func(target string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, "--role", runtimeRole, "--worker-role", worker, "--foreign-roles", target)
		for _, key := range []string{"PATH", "SystemRoot", "WINDIR", "TEMP", "TMP"} {
			if value := os.Getenv(key); value != "" {
				cmd.Env = append(cmd.Env, key+"="+value)
			}
		}
		cmd.Env = append(cmd.Env, "MIGRATION_DATABASE_URL="+cliConnection)
		return cmd.CombinedOutput()
	}
	// Ignore the first successful command's output, then retry from actual state.
	// This is a local caller-retry test, not a lost PostgreSQL COMMIT packet test.
	if _, e := run(foreign); e != nil {
		t.Fatal("initial retirement command failed")
	}
	before := retirementACL(t, db)
	if b, e := run(foreign); e != nil || strings.TrimSpace(string(b)) != "runtime legacy access retired: "+AccessProfile {
		t.Fatal("retirement command retry failed")
	}
	if before != retirementACL(t, db) {
		t.Fatal("retirement command retry changed ACLs")
	}
	b, e := run(foreign + "_missing")
	if e == nil || !strings.Contains(string(b), "retirement not confirmed") || strings.Contains(string(b), "fixture-only") || strings.Contains(string(b), cliConnection) {
		t.Fatal("CLI error classification or redaction failed")
	}
	if before != retirementACL(t, db) {
		t.Fatal("failed retirement command changed ACLs")
	}
}
