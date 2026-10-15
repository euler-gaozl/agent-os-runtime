//go:build darwin || linux

package runtime

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// Opt-in, real independently compiled Worker + its own execution.Engine +
// pinned official Node runner. Runtime HTTP/PG are real; model TLS and archive
// storage are bounded fixtures. No mocked BeginModel, SQL state mutation,
// forced kill or replacement of a running platform service is used.
func TestIndependentNativeWorkerRevisionGuard(t *testing.T) {
	old, next := os.Getenv("TEST_OLD_WORKER_BINARY"), os.Getenv("TEST_NEW_WORKER_BINARY")
	runner, node := os.Getenv("TEST_NATIVE_DSH_RUNNER"), os.Getenv("AGENTOS_NODE")
	if old == "" && next == "" && runner == "" {
		t.Skip("explicit independent old/new Worker revision fixture not configured")
	}
	for _, path := range []string{old, next, runner, node} {
		info, err := os.Stat(path)
		if !filepath.IsAbs(path) || err != nil || !info.Mode().IsRegular() {
			t.Fatal("all absolute old/new Worker, native runner and Node paths are required")
		}
	}
	oldHash := nativeRevisionBinary(t, old, "v0.1.0-rc.5")
	// A build-info version is not a source-content attestation. Root supplies
	// the independently built candidate hash and its declared module version;
	// its build receipt must bind the actual vendored revision as well.
	wantHash, wantVersion := os.Getenv("TEST_NEW_WORKER_SHA256"), os.Getenv("TEST_NEW_WORKER_RUNTIME_VERSION")
	if len(wantHash) != 64 || wantVersion == "" {
		t.Fatal("explicit candidate binary SHA-256 and Runtime module version required")
	}
	newHash := nativeRevisionBinary(t, next, wantVersion)
	if newHash != wantHash || newHash == oldHash {
		t.Fatal("candidate Worker binary is not the explicitly attested distinct build")
	}
	for _, tc := range []struct {
		name, binary string
		revise       bool
		wantCalls    int
	}{
		{"old_pin_stale", old, true, 2},
		{"new_pin_stale", next, true, 1},
		{"new_pin_same_revision", next, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) { nativeRevisionProcessCase(t, tc.binary, runner, node, tc.revise, tc.wantCalls) })
	}
}

func nativeRevisionBinary(t *testing.T, path, expected string) string {
	t.Helper()
	info, err := buildinfo.ReadFile(path)
	nativeNotesNeed(t, err)
	version := ""
	for _, dep := range info.Deps {
		if dep.Path == "github.com/euler-gaozl/agent-os-runtime" {
			if dep.Replace != nil {
				t.Fatal("fixture Worker must not hide a local module replace")
			}
			version = dep.Version
		}
	}
	if version != expected {
		t.Fatal("Worker compiled Runtime module version mismatch", version, expected)
	}
	b, err := os.ReadFile(path)
	nativeNotesNeed(t, err)
	hash := Hash(b)
	t.Logf("fixture Worker sha256=%s runtime_module=%s", hash, version)
	return hash
}

// JSONB may reorder keys inside Tool.InputSchema (a json.RawMessage) between
// the first request and a replay. Compare the entire parsed JSON value with
// UseNumber, as production intent validation does; preserve every field and
// array element/order. Keep raw wire hashes separately rather than claiming
// transport bytes are identical.
func nativeRevisionCanonical(t *testing.T, b []byte) []byte {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var value any
	nativeNotesNeed(t, d.Decode(&value))
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatal("expected exactly one JSON value for complete request comparison")
	}
	return JSON(value)
}

type nativeRevisionProcess struct {
	cmd      *exec.Cmd
	done     chan struct{}
	err      error
	identity string
	log      *os.File
	idle     func(context.Context) bool
	term     sync.Once
	reap     sync.Once
}

func nativeRevisionIdentity(ctx context.Context, pid int) (string, error) {
	b, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "pid=,ppid=,uid=,lstart=,command=").Output()
	if err != nil {
		return "", err
	}
	f := strings.Fields(string(b))
	if len(f) < 9 || f[0] != strconv.Itoa(pid) || f[1] != strconv.Itoa(os.Getpid()) || f[2] != strconv.Itoa(os.Getuid()) {
		return "", errors.New("fixture process PID/PPID/UID identity mismatch")
	}
	return strings.Join(f, " "), nil
}

func nativeRevisionHasChild(ctx context.Context, pid int) (bool, error) {
	// Global inspection contains only IDs; no unrelated process command lines.
	b, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=").Output()
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] == strconv.Itoa(pid) {
			return true, nil
		}
	}
	return false, nil
}

func nativeRevisionStart(t *testing.T, binary string, config map[string]string, idle func(context.Context) bool) *nativeRevisionProcess {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "worker.json")
	nativeNotesNeed(t, os.WriteFile(configPath, JSON(config), 0600))
	log, err := os.OpenFile(filepath.Join(dir, "worker.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	nativeNotesNeed(t, err)
	// No CommandContext: its default cancellation would silently SIGKILL.
	cmd := exec.Command(binary, "--config", configPath)
	cmd.Dir, cmd.Stdout, cmd.Stderr = dir, log, log
	for _, key := range []string{"PATH", "LANG", "TMPDIR"} {
		if value := os.Getenv(key); value != "" {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	p := &nativeRevisionProcess{cmd: cmd, done: make(chan struct{}), log: log, idle: idle}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.stop(t) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p.identity, err = nativeRevisionIdentity(ctx, cmd.Process.Pid)
	nativeNotesNeed(t, err)
	t.Logf("fixture Worker started pid=%d", cmd.Process.Pid)
	return p
}

func (p *nativeRevisionProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		p.checkExit(t)
		return
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	// Only stop our own terminal/idle fixture, after its Node child has exited.
	// On a stuck/non-idle failure, report PID and leave escalation to root.
	for {
		child, err := nativeRevisionHasChild(ctx, p.cmd.Process.Pid)
		if err != nil {
			t.Errorf("cannot confirm fixture Worker idle pid=%d: %v", p.cmd.Process.Pid, err)
			return
		}
		if !child && p.idle(ctx) {
			break
		}
		select {
		case <-p.done:
			p.checkExit(t)
			return
		case <-ctx.Done():
			t.Errorf("fixture Worker not idle; no signal sent, residual pid=%d", p.cmd.Process.Pid)
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
	identity, err := nativeRevisionIdentity(ctx, p.cmd.Process.Pid)
	if err != nil || identity != p.identity {
		t.Errorf("fixture Worker identity changed; no signal sent pid=%d", p.cmd.Process.Pid)
		return
	}
	p.term.Do(func() {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Errorf("fixture SIGTERM failed: %v", err)
		}
	})
	select {
	case <-p.done:
		p.checkExit(t)
	case <-ctx.Done():
		t.Errorf("fixture Worker still present after bounded SIGTERM; residual pid=%d", p.cmd.Process.Pid)
	}
}

func (p *nativeRevisionProcess) checkExit(t *testing.T) {
	t.Helper()
	p.reap.Do(func() {
		_ = p.log.Close()
		if p.err != nil {
			t.Errorf("fixture Worker did not shut down cleanly: %v", p.err)
		}
	})
}

func nativeRevisionProcessCase(t *testing.T, binary, runner, node string, revise bool, wantCalls int) {
	db, role, workerRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	nativeNotesNeed(t, Migrate(ctx, db, role, workerRole))
	workerToken, controlToken := testToken+"native-revision-worker", testToken+"native-revision-control"
	s := NewStore(rolePool(t, db, role), &serviceclient.Client{Token: workerToken}, "")
	nativeNotesNeed(t, s.ConfigureSubmissionEngine(EngineDSHNative))
	server := httptest.NewServer((&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"worker": servicehttp.TokenHash(workerToken), "control": servicehttp.TokenHash(controlToken)}}))
	t.Cleanup(server.Close)
	var mu sync.Mutex
	wires := [][]byte{}
	model := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
		if err != nil || req.Method != "POST" || req.URL.Path != "/chat/completions" || !json.Valid(body) {
			t.Error("unexpected synthetic provider request")
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		wires = append(wires, body)
		n := len(wires)
		mu.Unlock()
		if n > 2 {
			t.Error("fixture exceeded its two synthetic request allowance")
			w.WriteHeader(500)
			return
		}
		if n == 1 || revise {
			jsonResponse(w, 503, map[string]any{"error": map[string]string{"message": "synthetic complete transient response"}})
			return
		}
		_, _ = w.Write(nativeProviderResult("Synthetic supplied facts: actual 17, plan 20; no prediction."))
	}))
	t.Cleanup(model.Close)
	caFile := filepath.Join(t.TempDir(), "fixture-ca.pem")
	nativeNotesNeed(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: model.Certificate().Raw}), 0600))
	snap := nativeSnapshot()
	snap.Agent.Model.BaseURL, snap.Agent.Model.Limits.BaseURL = model.URL, model.URL
	snap.Agent.Model.Limits.Source = model.URL + "/models"
	snap.Agent.MaxSteps, snap.Agent.MaxModelAttempts = 3, 3
	id, err := s.Submit(ctx, ID(), "Synthetic supplied facts only: actual 17, plan 20.", snap, []string{"synthetic-original-period"})
	nativeNotesNeed(t, err)
	archive := newArchiveHTTPFixtureToken(t, id, nil, workerToken)
	s.StorageURL = archive.server.URL
	config := map[string]string{"DATABASE_URL": evidenceWorkerDSN(t, db, workerRole), "SERVICE_TOKEN": workerToken, "SERVICE_PEERS": string(JSON(map[string]string{"worker": servicehttp.TokenHash(workerToken)})), "RUNTIME_URL": server.URL, "REGISTRY_URL": server.URL, "TOOLHUB_URL": server.URL, "STORAGE_URL": archive.server.URL, "AGENTOS_NODE": node, "DSH_NATIVE_RUNNER": runner, "SSL_CERT_FILE": caFile, "GODEBUG": "x509sslcertoverrideplatform=1", "LISTEN": "127.0.0.1:0", "INSTANCE_ID": "native-revision-first", "DB_MAX_CONNS": "3"}
	idle := func(c context.Context) bool {
		r, e := s.Run(c, id)
		return e == nil && (r.Status == "failed" || r.Status == "completed" || r.Status == "cancelled" || r.Status == "waiting_unknown")
	}
	wait := func(p *nativeRevisionProcess, fence int64) Run {
		for {
			r, e := s.Run(ctx, id)
			nativeNotesNeed(t, e)
			if r.Fence >= fence && (r.Status == "failed" || r.Status == "completed" || r.Status == "cancelled" || r.Status == "waiting_unknown") {
				return r
			}
			select {
			case <-p.done:
				t.Fatal("fixture Worker exited before terminal checkpoint", p.err)
			case <-ctx.Done():
				t.Fatal("fixture Worker terminal checkpoint timeout")
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	first := nativeRevisionStart(t, binary, config, idle)
	r := wait(first, 1)
	if r.Status != "failed" || !strings.Contains(r.State.Error, "503") {
		t.Fatal("actual Worker did not naturally checkpoint a complete 503", r.Status, r.State.Error)
	}
	first.stop(t)
	select {
	case <-first.done:
	default:
		t.Fatal("refusing to queue until first fixture Worker has exited")
	}
	var oldID, callID string
	var oldRow, oldCalls, oldCallRequest, oldAttemptRequest, frozenInput []byte
	nativeNotesNeed(t, db.QueryRow(ctx, `SELECT a.id,to_jsonb(a),c.id,c.request,a.request,e.request FROM model_attempts a JOIN calls c ON c.id=a.call_id JOIN harness_exchanges e ON c.id=e.run_id||'/dsh/'||e.seq::text WHERE c.run_id=$1 AND c.kind='model'`, id).Scan(&oldID, &oldRow, &callID, &oldCallRequest, &oldAttemptRequest, &frozenInput))
	nativeNotesNeed(t, db.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM calls c WHERE c.run_id=$1`, id).Scan(&oldCalls))
	var input struct {
		Messages  []Message `json:"messages"`
		Tools     []Tool    `json:"tools"`
		MaxTokens int       `json:"max_tokens"`
		Purpose   string    `json:"purpose"`
	}
	nativeNotesNeed(t, json.Unmarshal(frozenInput, &input))
	if input.Purpose != "generation" || input.MaxTokens != r.Snapshot.Agent.Model.MaxTokens {
		t.Fatal("first failed model input differs from frozen native capabilities")
	}
	derived := execution.NativeModelRequest(r.Snapshot.Agent.Model, input.Messages, input.Tools)
	frozenCanonical := nativeRevisionCanonical(t, derived)
	if !bytes.Equal(nativeRevisionCanonical(t, oldCallRequest), frozenCanonical) || !bytes.Equal(nativeRevisionCanonical(t, oldAttemptRequest), frozenCanonical) {
		t.Fatal("first call/attempt request differs from the complete frozen model input")
	}
	request := func(path string, body []byte, want int) {
		req, e := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/runs/"+id+path, bytes.NewReader(body))
		nativeNotesNeed(t, e)
		req.Header.Set("Authorization", "Bearer "+controlToken)
		req.Header.Set("Content-Type", "application/json")
		resp, e := server.Client().Do(req)
		nativeNotesNeed(t, e)
		defer resp.Body.Close()
		_, e = io.Copy(io.Discard, io.LimitReader(resp.Body, 8192))
		nativeNotesNeed(t, e)
		if resp.StatusCode != want {
			t.Fatal("fixture management API rejected operation", path, resp.StatusCode)
		}
	}
	// Admit under the original failed generation first, then change constraints
	// before the second Worker starts: this preserves the actual pinned-worker gap.
	continuationActionQueue(t, ctx, server.Client(), server.URL, controlToken, id)
	if revise {
		request("/constraints", JSON(map[string]any{"values": []string{"synthetic-revised-period"}}), 200)
	}
	config["INSTANCE_ID"] = "native-revision-second"
	second := nativeRevisionStart(t, binary, config, idle)
	result := wait(second, r.Fence+1)
	second.stop(t)
	u, err := s.Usage(ctx, id)
	nativeNotesNeed(t, err)
	mu.Lock()
	got := len(wires)
	sameRaw := got == 2 && bytes.Equal(wires[0], wires[1])
	oldConstraint := got > 0 && bytes.Contains(wires[0], []byte("synthetic-original-period"))
	newConstraintSent := got == 2 && bytes.Contains(wires[1], []byte("synthetic-revised-period"))
	captured := append([][]byte(nil), wires...)
	mu.Unlock()
	canonicalWires, rawHashes, canonicalHashes := [][]byte{}, []string{}, []string{}
	wiresMatchFrozen := true
	for _, wire := range captured {
		canonical := nativeRevisionCanonical(t, wire)
		canonicalWires = append(canonicalWires, canonical)
		rawHashes = append(rawHashes, Hash(wire))
		canonicalHashes = append(canonicalHashes, Hash(canonical))
		wiresMatchFrozen = wiresMatchFrozen && bytes.Equal(canonical, frozenCanonical)
	}
	sameCanonical := got == 2 && bytes.Equal(canonicalWires[0], canonicalWires[1])
	var currentRequest, attemptRequests []byte
	var modelCalls int
	nativeNotesNeed(t, db.QueryRow(ctx, `SELECT c.request,(SELECT count(*) FROM calls WHERE run_id=$1 AND kind='model'),(SELECT jsonb_agg(a.request ORDER BY a.id) FROM model_attempts a WHERE a.call_id=c.id) FROM calls c WHERE c.run_id=$1 AND c.id=$2`, id, callID).Scan(&currentRequest, &modelCalls, &attemptRequests))
	var attempts []json.RawMessage
	nativeNotesNeed(t, json.Unmarshal(attemptRequests, &attempts))
	ledgerMatchesFrozen := modelCalls == 1 && len(attempts) == wantCalls && bytes.Equal(nativeRevisionCanonical(t, currentRequest), frozenCanonical)
	for _, request := range attempts {
		ledgerMatchesFrozen = ledgerMatchesFrozen && bytes.Equal(nativeRevisionCanonical(t, request), frozenCanonical)
	}
	t.Logf("synthetic request evidence: raw_sha256=%v canonical_sha256=%v frozen_canonical_sha256=%s oldConstraint=%v newConstraintSent=%v sameRaw=%v sameCanonical=%v wiresMatchFrozen=%v ledgerMatchesFrozen=%v", rawHashes, canonicalHashes, Hash(frozenCanonical), oldConstraint, newConstraintSent, sameRaw, sameCanonical, wiresMatchFrozen, ledgerMatchesFrozen)
	if got != wantCalls || u.Attempts != wantCalls || !oldConstraint || newConstraintSent || got == 2 && !sameCanonical || !wiresMatchFrozen || !ledgerMatchesFrozen {
		t.Fatal("actual Worker request/attempt provenance mismatch", got, wantCalls, u)
	}
	var retained []byte
	nativeNotesNeed(t, db.QueryRow(ctx, "SELECT to_jsonb(a) FROM model_attempts a WHERE id=$1", oldID).Scan(&retained))
	if !bytes.Equal(retained, oldRow) || result.Fence != r.Fence+1 || result.Owner != "native-revision-second" {
		t.Fatal("retry changed the retained 503 attempt or normal Claim identity")
	}
	if wantCalls == 1 {
		var calls []byte
		nativeNotesNeed(t, db.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM calls c WHERE c.run_id=$1`, id).Scan(&calls))
		if result.Status != "failed" || !strings.Contains(result.State.Error, "constraint revision") || !bytes.Equal(calls, oldCalls) || u.UnknownUsageAttempts != 1 {
			t.Fatal("candidate did not reject stale intent before any attempt/send", result.Status, result.State.Error)
		}
	} else if revise {
		if result.Status != "failed" || !strings.Contains(result.State.Error, "503") || u.UnknownUsageAttempts != 2 {
			t.Fatal("old pin did not reproduce a second complete 503 under the frozen old request")
		}
	} else if result.Status != "completed" || u.UnknownUsageAttempts != 1 {
		t.Fatal("same-revision candidate retry failed or erased unknown usage", result.Status, result.State.Error)
	}
	t.Logf("actual Worker revision case: revise=%v sends=%d attempts=%d unknown_usage_attempts=%d status=%s", revise, got, u.Attempts, u.UnknownUsageAttempts, result.Status)
}
