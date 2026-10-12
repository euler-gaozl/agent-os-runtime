package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Opt-in executable acceptance, not a replacement for ordinary CI. Both actual
// Worker versions share only a disposable Runtime database. Runtime HTTP and
// transaction code are real; model, ToolHub routing and immutable Storage HTTP
// are explicit fixtures, not a deployed complete service chain or paid provider.
func TestWorkerBinaryCompatibilityAfterCommittedEffect(t *testing.T) {
	oldBinary, newBinary := os.Getenv("TEST_OLD_WORKER_BINARY"), os.Getenv("TEST_NEW_WORKER_BINARY")
	oldRunner, newRunner := os.Getenv("TEST_OLD_DSH_RUNNER"), os.Getenv("TEST_NEW_DSH_RUNNER")
	node := os.Getenv("AGENTOS_NODE")
	if oldBinary == "" || newBinary == "" || oldRunner == "" || newRunner == "" || node == "" {
		t.Skip("actual old/new Worker executables and fixed Harness paths not configured")
	}
	for _, file := range []string{oldBinary, newBinary, oldRunner, newRunner, node} {
		if !filepath.IsAbs(file) {
			t.Fatal("binary compatibility inputs require absolute paths")
		}
		if info, e := os.Stat(file); e != nil || !info.Mode().IsRegular() {
			t.Fatal("binary compatibility input unavailable")
		}
	}
	for _, engine := range []string{"legacy-go", "dsh-0.2.0-rc.2", EngineDSHEvidence} {
		for _, direction := range []string{"old-to-new", "new-to-old"} {
			t.Run(engine+"/"+direction, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				db, appRole, workerRole := fixtureDatabase(t)
				if e := Migrate(ctx, db, appRole, workerRole); e != nil {
					t.Fatal(e)
				}
				// Explicit rollback window: the old readiness probe needs SELECT on
				// all Runtime tables, but never mutation rights for notes/metadata.
				if _, e := db.Exec(ctx, "ALTER ROLE "+pgx.Identifier{workerRole}.Sanitize()+" LOGIN PASSWORD 'fixture-only'"); e != nil {
					t.Fatal(e)
				}
				for _, table := range runtimeTables {
					if _, e := db.Exec(ctx, "GRANT SELECT ON "+pgx.Identifier{"public", table}.Sanitize()+" TO "+pgx.Identifier{workerRole}.Sanitize()); e != nil {
						t.Fatal(e)
					}
				}
				u, e := url.Parse(os.Getenv("TEST_DATABASE_URL"))
				if e != nil {
					t.Fatal(e)
				}
				u.Path = "/" + db.Config().ConnConfig.Database
				u.User = url.UserPassword(workerRole, "fixture-only")
				q := u.Query()
				for key := range q {
					switch strings.ToLower(key) {
					case "user", "password", "dbname", "database":
						q.Del(key)
					}
				}
				q.Set("search_path", "public")
				u.RawQuery = q.Encode()
				workerDSN := u.String()
				check, e := pgxpool.New(ctx, workerDSN)
				if e != nil {
					t.Fatal("worker fixture connection invalid")
				}
				var actualDB, actualRole string
				e = check.QueryRow(ctx, "SELECT current_database(),current_user").Scan(&actualDB, &actualRole)
				check.Close()
				if e != nil || actualDB != db.Config().ConnConfig.Database || actualRole != workerRole {
					t.Fatal("worker DSN escaped its restricted fixture")
				}
				var archivesMu sync.Mutex
				archives := map[string][]byte{}
				storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "PUT" || !strings.HasPrefix(r.URL.Path, "/v1/artifacts/") || r.Header.Get("Authorization") != "Bearer "+testToken+"worker" {
						w.WriteHeader(403)
						return
					}
					body, e := io.ReadAll(io.LimitReader(r.Body, (4<<20)+1))
					hash := Hash(body)
					if e != nil || len(body) > 4<<20 || strings.TrimPrefix(r.URL.Path, "/v1/artifacts/") != hash {
						w.WriteHeader(400)
						return
					}
					archivesMu.Lock()
					archives[hash] = append([]byte(nil), body...)
					archivesMu.Unlock()
					jsonResponse(w, 200, map[string]string{"hash": hash})
				}))
				defer storage.Close()
				remote := &serviceclient.Client{Token: testToken + "runtime"}
				store := NewStore(rolePool(t, db, appRole), remote, storage.URL)
				runtimeServer := httptest.NewServer((&Server{Store: store}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"toolhub": servicehttp.TokenHash(testToken + "hub"), "worker": servicehttp.TokenHash(testToken + "worker")}}))
				defer runtimeServer.Close()
				var models, hubCalls atomic.Int32
				replacementModel := make(chan struct{}, 1)
				allowModel := make(chan struct{})
				var allowOnce sync.Once
				resumeModel := func() { allowOnce.Do(func() { close(allowModel) }) }
				defer resumeModel()
				model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					models.Add(1)
					var request struct {
						Messages []Message `json:"messages"`
					}
					if json.NewDecoder(r.Body).Decode(&request) != nil {
						w.WriteHeader(400)
						return
					}
					if len(request.Messages) > 0 && request.Messages[len(request.Messages)-1].Role == "tool" {
						select {
						case replacementModel <- struct{}{}:
						default:
						}
						select {
						case <-allowModel:
						case <-r.Context().Done():
							return
						}
						binaryFixtureCompletion(w, Message{Role: "assistant", Content: "fixture recovered once"})
						return
					}
					binaryFixtureCompletion(w, Message{Role: "assistant", ToolCalls: []ToolCall{{ID: "fixture-save", Type: "function", Function: FunctionCall{Name: "note_save", Arguments: `{"kind":"fact","title":"Binary handoff","content":"fixture only","source":"test://binary-handoff"}`}}}})
				}))
				defer model.Close()
				committed := make(chan Invocation, 1)
				release := make(chan struct{})
				defer close(release)
				proxy := serviceclient.Client{RuntimeURL: runtimeServer.URL, Token: testToken + "hub"}
				hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "POST" || r.URL.Path != "/invoke" || r.Header.Get("Authorization") != "Bearer "+testToken+"worker" {
						w.WriteHeader(403)
						return
					}
					n := hubCalls.Add(1)
					var inv Invocation
					if json.NewDecoder(r.Body).Decode(&inv) != nil {
						w.WriteHeader(400)
						return
					}
					raw, e := proxy.Invoke(r.Context(), inv)
					if e != nil {
						t.Error("real Runtime rejected fixture tool", e)
						w.WriteHeader(500)
						return
					}
					if n == 1 {
						committed <- inv
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(raw)
				}))
				defer hub.Close()
				snap := basicSnapshot(model.URL)
				snap.Agent.Tools = []string{"note_save"}
				snap.Agent.Model.MaxTokens = 64
				id := submit(t, store, snap)
				if _, e = db.Exec(ctx, "UPDATE runs SET engine=$2 WHERE id=$1", id, engine); e != nil {
					t.Fatal(e)
				}
				firstBinary, secondBinary, firstRunner, secondRunner := oldBinary, newBinary, oldRunner, newRunner
				if direction == "new-to-old" {
					firstBinary, secondBinary, firstRunner, secondRunner = newBinary, oldBinary, newRunner, oldRunner
				}
				start := func(binary, runner, identity string) (*exec.Cmd, <-chan error) {
					t.Helper()
					dir := t.TempDir()
					config := map[string]string{"DATABASE_URL": workerDSN, "SERVICE_TOKEN": testToken + "worker", "SERVICE_PEERS": string(JSON(map[string]string{"runtime": servicehttp.TokenHash(testToken + "runtime"), "worker": servicehttp.TokenHash(testToken + "worker")})), "RUNTIME_URL": runtimeServer.URL, "REGISTRY_URL": runtimeServer.URL, "TOOLHUB_URL": hub.URL, "STORAGE_URL": storage.URL, "AGENTOS_NODE": node, "DSH_RUNNER": runner, "LISTEN": "127.0.0.1:0", "INSTANCE_ID": identity, "DB_MAX_CONNS": "3"}
					path := filepath.Join(dir, "fixture.json")
					if e := os.WriteFile(path, JSON(config), 0600); e != nil {
						t.Fatal(e)
					}
					log, e := os.Create(filepath.Join(dir, "worker.log"))
					if e != nil {
						t.Fatal(e)
					}
					t.Cleanup(func() {
						_ = log.Close()
						if evidence := os.Getenv("TEST_WORKER_DIAGNOSTICS"); evidence != "" {
							body, err := os.ReadFile(log.Name())
							if err != nil {
								t.Error(err)
								return
							}
							name := strings.NewReplacer("/", "-", "\\", "-").Replace(t.Name()) + "-" + identity + ".log"
							if err = os.WriteFile(filepath.Join(evidence, name), body, 0600); err != nil {
								t.Error(err)
							}
						}
					})
					cmd := exec.CommandContext(ctx, binary, "--config", path)
					cmd.Dir = dir
					cmd.Stdout = log
					cmd.Stderr = log
					for _, key := range []string{"PATH", "SystemRoot", "WINDIR", "TEMP", "TMP", "LANG"} {
						if value := os.Getenv(key); value != "" {
							cmd.Env = append(cmd.Env, key+"="+value)
						}
					}
					if e := cmd.Start(); e != nil {
						t.Fatal(e)
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
					})
					return cmd, done
				}
				first, firstDone := start(firstBinary, firstRunner, "first-worker")
				var prior Invocation
				select {
				case prior = <-committed:
				case <-firstDone:
					t.Fatal("first Worker exited before committed effect")
				case <-ctx.Done():
					t.Fatal("first Worker never reached committed effect")
				}
				var receipts int
				if e = db.QueryRow(ctx, "SELECT count(*) FROM calls WHERE id=$1 AND status='completed'", prior.OperationID).Scan(&receipts); e != nil || receipts != 1 {
					t.Fatal("kill point did not contain committed receipt", e)
				}
				if e = first.Process.Kill(); e != nil {
					t.Fatal("actual fault injection failed", e)
				}
				select {
				case exitErr := <-firstDone:
					if exitErr == nil {
						t.Fatal("fault injection did not produce a failed process exit")
					}
				case <-ctx.Done():
					t.Fatal("killed Worker still running")
				}
				// Accelerate only this dead fixture's lease; production lease settings
				// and all takeover/fence logic remain unchanged.
				if _, e = db.Exec(ctx, "UPDATE runs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", id); e != nil {
					t.Fatal(e)
				}
				_, secondDone := start(secondBinary, secondRunner, "replacement-worker")
				select {
				case <-replacementModel:
				case <-secondDone:
					t.Fatal("replacement exited before active fence check")
				case <-ctx.Done():
					t.Fatal("replacement did not reach its second model request")
				}
				active, err := store.Run(ctx, id)
				if err != nil || active.Status != "running" || active.Owner != "replacement-worker" || active.Fence != 2 {
					t.Fatal("stale-fence probe was not during an active replacement lease", err)
				}
				if _, err = proxy.Invoke(ctx, prior); !errors.Is(err, ErrFence) {
					t.Fatal("stale active-lease invocation did not return the exact fencing error", err)
				}
				var afterNotes, afterReceipts int
				if err = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM calls WHERE kind='tool' AND status='completed')").Scan(&afterNotes, &afterReceipts); err != nil || afterNotes != 1 || afterReceipts != 1 {
					t.Fatal("stale active-lease invocation changed durable effects", err)
				}
				resumeModel()
				var result Run
				for {
					result, e = store.Run(ctx, id)
					if e != nil {
						t.Fatal(e)
					}
					if result.Status == "completed" {
						break
					}
					if result.Status == "failed" || result.Status == "waiting_reconciliation" {
						t.Fatalf("handoff stopped: %s %s", result.Status, result.State.Error)
					}
					select {
					case <-secondDone:
						t.Fatal("replacement exited")
					case <-ctx.Done():
						t.Fatalf("replacement failed to finish: %s %s", result.Status, result.State.Error)
					case <-time.After(30 * time.Millisecond):
					}
				}
				var notes, toolReceipts, modelReceipts int
				if e = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM calls WHERE kind='tool' AND status='completed'),(SELECT count(*) FROM model_attempts WHERE status='completed')").Scan(&notes, &toolReceipts, &modelReceipts); e != nil {
					t.Fatal(e)
				}
				if notes != 1 || toolReceipts != 1 || modelReceipts != 2 || models.Load() != 2 || hubCalls.Load() != 1 || result.Fence != 2 || result.Owner != "replacement-worker" || result.State.Result != "fixture recovered once" {
					t.Fatalf("effect/model repeated or result lost: notes=%d receipts=%d models=%d HTTP=%d hub=%d fence=%d", notes, toolReceipts, modelReceipts, models.Load(), hubCalls.Load(), result.Fence)
				}
				if engine != "legacy-go" {
					archivesMu.Lock()
					retained := len(archives) > 0 && len(archives[result.State.HarnessArtifact]) > 0
					archivesMu.Unlock()
					if !retained {
						t.Fatal("completed Harness evidence is not retained")
					}
				}
				t.Logf("actual executable handoff %s %s: killed after effect commit/before tool reply; one note, one tool HTTP, two model HTTP; fence=2; stale effect rejected", engine, direction)
			})
		}
	}
}

func binaryFixtureCompletion(w http.ResponseWriter, message Message) {
	finish := "stop"
	if len(message.ToolCalls) > 0 {
		finish = "tool_calls"
	}
	jsonResponse(w, 200, map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}})
}
