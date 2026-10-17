package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/euler-gaozl/agent-os-runtime/runtime"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"os"
	"time"
)

func main() {
	role := flag.String("role", "agentos_runtime", "existing application database role")
	workerRole := flag.String("worker-role", "agentos_worker", "existing Worker database role in the same runtime domain")
	configPath := flag.String("config", "", "protected migration configuration for managed process recovery")
	flag.Parse()
	dsn := os.Getenv("MIGRATION_DATABASE_URL")
	if *configPath != "" {
		explicitRole := false
		flag.Visit(func(f *flag.Flag) { explicitRole = explicitRole || f.Name == "role" || f.Name == "worker-role" })
		if explicitRole || dsn != "" || flag.NArg() != 0 {
			fail()
		}
		values, err := readMigrationConfig(*configPath)
		if err != nil {
			fail()
		}
		dsn, *role, *workerRole = values["MIGRATION_DATABASE_URL"], values["ROLE"], values["WORKER_ROLE"]
	}
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
func readMigrationConfig(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("migration configuration unavailable")
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(body) > 65536 {
		return nil, fmt.Errorf("invalid migration configuration")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, fmt.Errorf("invalid migration configuration")
	}
	values := map[string]string{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || (key != "MIGRATION_DATABASE_URL" && key != "ROLE" && key != "WORKER_ROLE") {
			return nil, fmt.Errorf("invalid migration configuration")
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("invalid migration configuration")
		}
		var value string
		if err := d.Decode(&value); err != nil || value == "" {
			return nil, fmt.Errorf("invalid migration configuration")
		}
		values[key] = value
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF || len(values) != 3 || values["ROLE"] == values["WORKER_ROLE"] {
		return nil, fmt.Errorf("invalid migration configuration")
	}
	return values, nil
}
func fail() {
	fmt.Fprintln(os.Stderr, "runtime migration failed; verify migration connection, existing role, permissions and lock availability")
	os.Exit(1)
}
