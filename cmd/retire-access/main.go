// Explicit contraction job; never invoked by normal schema migration/startup.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/runtime"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	role := flag.String("role", "agentos_runtime", "existing Runtime database role")
	worker := flag.String("worker-role", "agentos_worker", "existing Worker database role to contract")
	foreign := flag.String("foreign-roles", "", "comma-separated existing foreign consumer roles to remove from execution tables")
	flag.Parse()
	dsn := os.Getenv("MIGRATION_DATABASE_URL")
	config, e := pgxpool.ParseConfig(dsn)
	if dsn == "" || e != nil || flag.NArg() != 0 {
		fail()
	}
	config.MaxConns = 2
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, e := pgxpool.NewWithConfig(ctx, config)
	if e != nil {
		fail()
	}
	defer db.Close()
	var foreignRoles []string
	if *foreign != "" {
		foreignRoles = strings.Split(*foreign, ",")
	}
	if e = runtime.RetireLegacyAccess(ctx, db, *role, *worker, foreignRoles); e != nil {
		fail()
	}
	fmt.Println("runtime legacy access retired: " + runtime.AccessProfile)
}

func fail() {
	// Never print connection errors, connection strings, or server error details.
	fmt.Fprintln(os.Stderr, "runtime access retirement not confirmed; verify stopped consumers, current schema, leaf identities and inherited/PUBLIC grants; inspect actual ACLs before retry if commit outcome is uncertain")
	os.Exit(1)
}
