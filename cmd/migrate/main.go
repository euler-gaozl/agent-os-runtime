package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/euler-gaozl/agent-os-runtime/runtime"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

func main() {
	role := flag.String("role", "agentos_runtime", "existing application database role")
	workerRole := flag.String("worker-role", "agentos_worker", "existing Worker database role in the same runtime domain")
	flag.Parse()
	dsn := os.Getenv("MIGRATION_DATABASE_URL")
	config, err := pgxpool.ParseConfig(dsn)
	if dsn == "" || err != nil {
		fail()
	}
	config.MaxConns = 2
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		fail()
	}
	defer db.Close()
	if err = runtime.Migrate(ctx, db, *role, *workerRole); err != nil {
		fail()
	}
	fmt.Println("runtime migration committed")
}
func fail() {
	fmt.Fprintln(os.Stderr, "runtime migration failed; verify migration connection, existing role, permissions and lock availability")
	os.Exit(1)
}
