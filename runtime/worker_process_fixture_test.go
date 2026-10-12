package runtime

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func requireEvidenceWorker(t *testing.T) (binary, runner, node string) {
	t.Helper()
	binary, runner, node = os.Getenv("TEST_NEW_WORKER_BINARY"), os.Getenv("TEST_NEW_DSH_RUNNER"), os.Getenv("AGENTOS_NODE")
	if binary == "" || runner == "" || node == "" {
		t.Skip("actual independent Worker executable and pinned DSH are not configured")
	}
	for _, file := range []string{binary, runner, node} {
		info, err := os.Stat(file)
		if !filepath.IsAbs(file) || err != nil || !info.Mode().IsRegular() {
			t.Fatal("absolute Worker/Harness input unavailable")
		}
	}
	return
}

func evidenceWorkerDSN(t *testing.T, db *pgxpool.Pool, workerRole string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.Exec(ctx, "ALTER ROLE "+pgx.Identifier{workerRole}.Sanitize()+" LOGIN PASSWORD 'fixture-only'"); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid fixture URL")
	}
	u.Path = "/" + db.Config().ConnConfig.Database
	u.User = url.UserPassword(workerRole, "fixture-only")
	q := u.Query()
	for k := range q {
		switch strings.ToLower(k) {
		case "user", "password", "dbname", "database":
			q.Del(k)
		}
	}
	q.Set("search_path", "public")
	u.RawQuery = q.Encode()
	probe, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal("fixture Worker connection unavailable")
	}
	defer probe.Close()
	var database, role string
	if err = probe.QueryRow(ctx, "SELECT current_database(),current_user").Scan(&database, &role); err != nil || role != workerRole || database != db.Config().ConnConfig.Database {
		t.Fatal("fixture connection identity mismatch", err)
	}
	return u.String()
}

func startEvidenceWorker(t *testing.T, ctx context.Context, binary string, config map[string]string) (*exec.Cmd, <-chan error) {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "worker.json")
	if err := os.WriteFile(configPath, JSON(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "worker.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "--config", configPath)
	cmd.Dir = dir
	cmd.Stdout = log
	cmd.Stderr = log
	for _, key := range []string{"PATH", "SystemRoot", "WINDIR", "TEMP", "TMP", "LANG"} {
		if value := os.Getenv(key); value != "" {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	if err = cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fixture Worker did not exit")
		}
		_ = log.Close()
		if evidence := os.Getenv("TEST_WORKER_DIAGNOSTICS"); evidence != "" {
			body, e := os.ReadFile(logPath)
			if e != nil {
				t.Error(e)
				return
			}
			name := strings.NewReplacer("/", "-", "\\", "-").Replace(t.Name()) + "-" + config["INSTANCE_ID"] + ".log"
			if e = os.WriteFile(filepath.Join(evidence, name), body, 0600); e != nil {
				t.Error(e)
			}
		}
	})
	return cmd, done
}
