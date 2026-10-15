//go:build darwin

package runtime

// This opt-in Owner transaction fixture requires the reviewed disposable-volume
// controller. It has no Worker/Node, provider HTTP or real Storage dependency.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const runtimeFaultCapBytes = 512 << 20

type runtimeFaultFixture struct {
	Root, Mount, Image, Marker, Nonce, PGData, PGBin, PGLog, Evidence string
	AdminURL                                                          string
	VolumeDev                                                         uint64
	Port                                                              int
}

func runtimeFaultNeed(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func runtimeFaultDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func runtimeFaultSave(t *testing.T, f runtimeFaultFixture, name string, v any) {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	runtimeFaultNeed(t, e)
	runtimeFaultNeed(t, os.WriteFile(filepath.Join(f.Evidence, name), append(b, '\n'), 0600))
}

func runtimeFaultVolume(t *testing.T, f runtimeFaultFixture) syscall.Statfs_t {
	t.Helper()
	platform := os.Getenv("RUNTIME_ENOSPC_WORKSPACE")
	if !filepath.IsAbs(platform) || platform == string(filepath.Separator) || filepath.Clean(platform) != platform {
		t.Fatal("explicit absolute workspace required")
	}
	physical, err := filepath.EvalSymlinks(platform)
	runtimeFaultNeed(t, err)
	if physical != platform {
		t.Fatal("workspace must be physical")
	}
	nonce, e := hex.DecodeString(f.Nonce)
	if e != nil || len(nonce) != 16 || hex.EncodeToString(nonce) != f.Nonce || f.Root != platform+"/local/data/runtime-enospc-"+f.Nonce || f.Mount != filepath.Join(f.Root, "volume") || f.Image != filepath.Join(f.Root, "volume.dmg") || f.Marker != filepath.Join(f.Mount, ".agentos-enospc") || f.PGData != filepath.Join(f.Mount, "pgdata") || f.Evidence != platform+"/local/evidence/runtime-enospc-"+f.Nonce || f.PGBin != platform+"/local/toolchains/postgresql-18.6/bin" || f.PGLog != filepath.Join(f.Root, "postgres.log") {
		t.Fatal("unexpected fixture path identity")
	}
	for _, p := range []string{f.Root, f.Mount, f.PGData, f.Evidence} {
		r, e := filepath.EvalSymlinks(p)
		runtimeFaultNeed(t, e)
		if r != p {
			t.Fatal("fixture path must be physical")
		}
	}
	owner, e := os.ReadFile(filepath.Join(f.Root, ".fixture-owner"))
	runtimeFaultNeed(t, e)
	if string(owner) != f.Nonce {
		t.Fatal("fixture owner marker mismatch")
	}
	image, e := os.Lstat(f.Image)
	runtimeFaultNeed(t, e)
	if !image.Mode().IsRegular() || image.Size() > runtimeFaultCapBytes {
		t.Fatal("fixture image is not a bounded regular file")
	}
	marker, e := os.ReadFile(f.Marker)
	runtimeFaultNeed(t, e)
	if string(marker) != f.Nonce {
		t.Fatal("mounted volume marker mismatch")
	}
	mi, e := os.Stat(f.Mount)
	runtimeFaultNeed(t, e)
	ri, e := os.Stat(f.Root)
	runtimeFaultNeed(t, e)
	if uint64(mi.Sys().(*syscall.Stat_t).Dev) != f.VolumeDev || mi.Sys().(*syscall.Stat_t).Dev == ri.Sys().(*syscall.Stat_t).Dev {
		t.Fatal("fixture mount device mismatch")
	}
	var st syscall.Statfs_t
	runtimeFaultNeed(t, syscall.Statfs(f.Mount, &st))
	if st.Blocks*uint64(st.Bsize) > runtimeFaultCapBytes {
		t.Fatal("volume exceeds fixed 512 MiB bound")
	}
	return st
}

func runtimeFaultPostmaster(t *testing.T, f runtimeFaultFixture, allowStopped bool) int {
	t.Helper()
	runtimeFaultVolume(t, f)
	b, e := os.ReadFile(filepath.Join(f.PGData, "postmaster.pid"))
	if errors.Is(e, os.ErrNotExist) && allowStopped {
		return 0
	}
	runtimeFaultNeed(t, e)
	lines := strings.Split(string(b), "\n")
	if len(lines) < 2 || lines[1] != f.PGData {
		t.Fatal("postmaster data directory identity mismatch")
	}
	pid, e := strconv.Atoi(lines[0])
	runtimeFaultNeed(t, e)
	if pid <= 1 {
		t.Fatal("invalid fixture postmaster pid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, e := exec.CommandContext(ctx, "/bin/ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if e != nil && allowStopped && errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return 0
	}
	runtimeFaultNeed(t, e)
	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 3 || fields[0] != filepath.Join(f.PGBin, "postgres") {
		t.Fatal("postmaster executable identity mismatch")
	}
	found := false
	for i := 1; i+1 < len(fields); i++ {
		if fields[i] == "-D" && fields[i+1] == f.PGData {
			found = true
		}
	}
	if !found {
		t.Fatal("postmaster command does not belong to fixture")
	}
	return pid
}

// Each operation needs a fresh disk-space error from its own log offset.
func runtimeFaultDiskErrorLines(t *testing.T, f runtimeFaultFixture, before os.FileInfo, targetSQL string) ([]string, bool) {
	t.Helper()
	current, e := os.Lstat(f.PGLog)
	runtimeFaultNeed(t, e)
	if !current.Mode().IsRegular() || !os.SameFile(before, current) || current.Size() < before.Size() || current.Size()-before.Size() > 4<<20 {
		t.Fatal("fixture PostgreSQL log identity or bounded tail changed")
	}
	log, e := os.Open(f.PGLog)
	runtimeFaultNeed(t, e)
	defer log.Close()
	_, e = log.Seek(before.Size(), io.SeekStart)
	runtimeFaultNeed(t, e)
	b, e := io.ReadAll(io.LimitReader(log, 4<<20))
	runtimeFaultNeed(t, e)
	var matches []string
	pidPattern := regexp.MustCompile(`\[([0-9]+)\]`)
	faultPIDs := map[string]bool{}
	statementMatched := false
	for _, line := range strings.Split(string(b), "\n") {
		pid := pidPattern.FindStringSubmatch(line)
		if len(pid) != 2 {
			continue
		}
		if strings.Contains(line, "No space left on device") && strings.Contains(line, "ERROR:") && strings.Contains(line, "53100:") {
			matches = append(matches, line)
			faultPIDs[pid[1]] = true
		}
		if faultPIDs[pid[1]] && strings.Contains(line, "STATEMENT:") && strings.Contains(line, targetSQL) {
			matches = append(matches, line)
			statementMatched = true
		}
	}
	return matches, statementMatched
}

// Allocate only on the positively identified disposable volume, with one
// cumulative bound. HFS+ may report reserve blocks after a larger write fails.
func runtimeFaultFill(t *testing.T, ctx context.Context, f runtimeFaultFixture) (func(), map[string]any) {
	t.Helper()
	initial := runtimeFaultVolume(t, f)
	runtimeFaultPostmaster(t, f, false)
	fillPath := filepath.Join(f.Mount, "fill-"+f.Nonce)
	fd, e := syscall.Open(fillPath, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0600)
	runtimeFaultNeed(t, e)
	file := os.NewFile(uintptr(fd), fillPath)
	defer file.Close()
	opened, e := file.Stat()
	runtimeFaultNeed(t, e)
	if uint64(opened.Sys().(*syscall.Stat_t).Dev) != f.VolumeDev {
		t.Fatal("fill file is not on the bounded volume")
	}
	released := false
	release := func() {
		if released {
			return
		}
		runtimeFaultVolume(t, f)
		current, err := os.Lstat(fillPath)
		runtimeFaultNeed(t, err)
		if !os.SameFile(opened, current) {
			t.Fatal("fill file identity changed")
		}
		runtimeFaultNeed(t, os.Remove(fillPath))
		released = true
	}
	// Registered after fixture DB/pool cleanup so free space comes first even
	// when a fault assertion fails. The controller independently owns fallback.
	t.Cleanup(release)
	if initial.Bsize < 1 || initial.Bsize > 1<<20 {
		t.Fatal("unexpected filesystem block size")
	}
	var written int64
	var stages []map[string]any
	for _, size := range []int{1 << 20, int(initial.Bsize), 1} {
		chunk := make([]byte, size)
		start := written
		var fullErr error
		for written+int64(size) <= runtimeFaultCapBytes {
			if ctx.Err() != nil {
				t.Fatal("bounded fill timed out")
			}
			n, err := file.Write(chunk)
			written += int64(n)
			if err != nil {
				fullErr = err
				break
			}
			if n != len(chunk) {
				t.Fatal("short fill without an error")
			}
		}
		if !errors.Is(fullErr, syscall.ENOSPC) {
			t.Fatal("actual ENOSPC not reached within fixed bound", size)
		}
		st := runtimeFaultVolume(t, f)
		stages = append(stages, map[string]any{"write_size": size, "bytes_written": written - start, "write_enospc": true, "available_blocks": st.Bavail, "available_bytes": st.Bavail * uint64(st.Bsize)})
	}
	if e = file.Sync(); e != nil && !errors.Is(e, syscall.ENOSPC) {
		runtimeFaultNeed(t, e)
	}
	if e = file.Close(); e != nil && !errors.Is(e, syscall.ENOSPC) {
		runtimeFaultNeed(t, e)
	}
	full := runtimeFaultVolume(t, f)
	return release, map[string]any{"fill_stages": stages, "filler_bytes": written, "volume_capacity_bytes": full.Blocks * uint64(full.Bsize), "available_blocks": full.Bavail, "available_bytes": full.Bavail * uint64(full.Bsize)}
}

func runtimeFaultRequire53100(t *testing.T, f runtimeFaultFixture, name, targetSQL string, before os.FileInfo, cause error) {
	t.Helper()
	var pg *pgconn.PgError
	code := ""
	if errors.As(cause, &pg) {
		code = pg.Code
	}
	var lines []string
	var statementMatched bool
	for until := time.Now().Add(2 * time.Second); time.Now().Before(until); {
		lines, statementMatched = runtimeFaultDiskErrorLines(t, f, before, targetSQL)
		if statementMatched {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Do not emit DSNs, requests or raw provider data. All inputs are synthetic.
	runtimeFaultSave(t, f, name+"-fault.json", map[string]any{"sqlstate": code, "error_type": fmt.Sprintf("%T", cause), "postgres_disk_errors": lines, "target_statement_matched": statementMatched, "log_offset_before": before.Size(), "operation_failed": cause != nil})
	if code != "53100" || !statementMatched {
		t.Fatal(name+" did not independently reach PostgreSQL disk-full failure", code)
	}
}

// Compare committed rows, not sequences (rollback can consume sequence values).
// All tables belong to this fresh Runtime fixture database. There is no repair
// SQL, engine rewrite, forced lease expiry, or cross-service table access.
func runtimeFaultRows(t *testing.T, ctx context.Context, db *pgxpool.Pool) map[string]json.RawMessage {
	t.Helper()
	result := map[string]json.RawMessage{}
	for _, table := range []string{"documents", "note_roots", "note_versions", "note_audits", "calls", "model_attempts", "events", "runs", "harness_exchanges", "harness_archives"} {
		var b []byte
		runtimeFaultNeed(t, db.QueryRow(ctx, "SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text),'[]'::jsonb) FROM "+table+" x").Scan(&b))
		result[table] = b
	}
	return result
}
func runtimeFaultRowDigests(rows map[string]json.RawMessage) map[string]string {
	result := map[string]string{}
	for table, raw := range rows {
		result[table] = runtimeFaultDigest(raw)
	}
	return result
}
func runtimeFaultSameRows(t *testing.T, before, after map[string]json.RawMessage) {
	t.Helper()
	for table, raw := range before {
		if !jsonEqual(raw, after[table]) {
			t.Fatal("committed rows changed across failed writes and recovery", table)
		}
	}
}

func runtimeFaultRestart(t *testing.T, ctx context.Context, f runtimeFaultFixture, pools ...*pgxpool.Pool) int {
	t.Helper()
	runtimeFaultPostmaster(t, f, true)
	stopCtx, stopCancel := context.WithTimeout(ctx, 15*time.Second)
	stop := exec.CommandContext(stopCtx, filepath.Join(f.PGBin, "pg_ctl"), "-D", f.PGData, "stop", "-m", "immediate", "-w", "-t", "10")
	stop.Env = []string{"LC_ALL=C", "LANG=C"}
	_ = stop.Run()
	stopCancel()
	if runtimeFaultPostmaster(t, f, true) != 0 {
		t.Fatal("fixture PostgreSQL did not stop before recovery")
	}
	for _, pool := range pools {
		pool.Reset()
	}
	probe, done := context.WithTimeout(ctx, time.Second)
	err := pools[0].Ping(probe)
	done()
	if err == nil {
		t.Fatal("old database endpoint still accepts queries")
	}
	runtimeFaultVolume(t, f)
	startCtx, startCancel := context.WithTimeout(ctx, 20*time.Second)
	start := exec.CommandContext(startCtx, filepath.Join(f.PGBin, "pg_ctl"), "-D", f.PGData, "-l", f.PGLog, "start", "-w", "-t", "15")
	start.Env = []string{"LC_ALL=C", "LANG=C"}
	err = start.Run()
	startCancel()
	runtimeFaultNeed(t, err)
	for _, pool := range pools {
		pool.Reset()
	}
	return runtimeFaultPostmaster(t, f, false)
}

func TestRuntimeDiskFullPreservesNotesAndModelReceipts(t *testing.T) {
	config := os.Getenv("RUNTIME_ENOSPC_FIXTURE")
	if config == "" {
		t.Skip("explicit disposable Mac volume controller required")
	}
	fi, e := os.Lstat(config)
	runtimeFaultNeed(t, e)
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0077 != 0 {
		t.Fatal("private regular fixture config required")
	}
	raw, e := os.ReadFile(config)
	runtimeFaultNeed(t, e)
	var f runtimeFaultFixture
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	runtimeFaultNeed(t, decoder.Decode(&f))
	if decoder.Decode(new(any)) != io.EOF {
		t.Fatal("trailing fixture config data")
	}
	runtimeFaultVolume(t, f)
	if config != filepath.Join(f.Root, "fixture.json") || f.Port < 1024 || f.Port > 65535 || f.Port == 55432 || (f.Port >= 18090 && f.Port <= 18109) {
		t.Fatal("unexpected fixture config or port")
	}
	u, e := url.Parse(f.AdminURL)
	if e != nil || u.Scheme != "postgres" || u.Host != fmt.Sprintf("127.0.0.1:%d", f.Port) || u.RawQuery != "sslmode=disable" || u.Fragment != "" || u.Path != "/postgres" || u.RawPath != "" || u.Opaque != "" || u.User == nil || u.User.Username() != "enospc_admin" {
		t.Fatal("unexpected isolated database endpoint")
	}
	initialPID := runtimeFaultPostmaster(t, f, false)
	ctx, cancel := context.WithTimeout(context.Background(), 130*time.Second)
	defer cancel()
	admin, e := pgxpool.New(ctx, f.AdminURL)
	runtimeFaultNeed(t, e)
	defer admin.Close()
	var data, systemID string
	var port int
	runtimeFaultNeed(t, admin.QueryRow(ctx, "SELECT current_setting('data_directory'),inet_server_port(),system_identifier::text FROM pg_control_system()").Scan(&data, &port, &systemID))
	if data != f.PGData || port != f.Port {
		t.Fatal("connected cluster is not the disposable fixture")
	}
	t.Setenv("TEST_DATABASE_URL", f.AdminURL)
	db, role, workerRole := fixtureDatabase(t)
	runtimeFaultNeed(t, Migrate(ctx, db, role, workerRole))
	app, worker := rolePool(t, db, role), rolePool(t, db, workerRole)
	s := NewStore(app, nil, "")
	runtimeFaultNeed(t, s.ConfigureSubmissionEngine(EngineDSHNative))
	archives := &archiveFixture{} // In-memory immutable bytes, explicitly not Storage.
	engine, e := execution.NewSourceVersions(worker, archives, []string{EngineDSHNative})
	runtimeFaultNeed(t, e)
	snap := nativeSnapshot()
	idA, e := s.Submit(ctx, "enospc-a-"+f.Nonce, "Synthetic retained note and model receipt", snap, nil)
	runtimeFaultNeed(t, e)
	a, e := engine.Claim(ctx, "enospc-original-a", 120*time.Second)
	runtimeFaultNeed(t, e)
	if a.ID != idA || a.Engine != EngineDSHNative {
		t.Fatal("wrong first native run")
	}
	frameA := nativeFrame(1, "generation", "Synthetic observation 42, not a forecast.")
	var requestA, receiptA []byte
	var attemptA string
	syntheticCompletions := 0
	effectsA := nativeEffects{complete: func(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
		request := execution.NativeModelRequest(r.Snapshot.Agent.Model, m, ts)
		attempt, cached, err := engine.BeginModel(c, r, id, request)
		if err != nil {
			return nil, err
		}
		if cached == nil {
			syntheticCompletions++
			requestA, attemptA = request, attempt
			receiptA, err = execution.NativeReceipt(200, nativeProviderResult("Synthetic committed result: observation 42."))
			if err == nil {
				err = engine.EndModel(c, r, id, attempt, receiptA, nil)
			}
		}
		return request, err
	}}
	valueA, _, e := engine.Exchange(ctx, &a, frameA, effectsA)
	runtimeFaultNeed(t, e)
	if syntheticCompletions != 1 {
		t.Fatal("baseline synthetic completion count")
	}
	baselineInv := noteInv(a, "note_save", "note/baseline", map[string]any{"kind": "fact", "title": "Committed synthetic note", "content": "Observation 42; preserve source, metadata and scope.", "source": "fixture://runtime-enospc/baseline", "metadata": map[string]any{"synthetic": true, "observation": 42}})
	baselineReceipt, e := s.LocalTool(ctx, baselineInv)
	runtimeFaultNeed(t, e)
	var baselineDocument Document
	runtimeFaultNeed(t, json.Unmarshal(baselineReceipt, &baselineDocument))
	if baselineDocument.ID == "" || baselineDocument.Scope != snap.AgentRef.ID {
		t.Fatal("baseline note scope missing")
	}
	idB, e := s.Submit(ctx, "enospc-b-"+f.Nonce, "Synthetic uncommitted model receipt", snap, nil)
	runtimeFaultNeed(t, e)
	b, e := engine.Claim(ctx, "enospc-original-b", 120*time.Second)
	runtimeFaultNeed(t, e)
	if b.ID != idB {
		t.Fatal("wrong second native run")
	}
	payload := make([]byte, 1<<20)
	_, e = rand.Read(payload)
	runtimeFaultNeed(t, e)
	content := hex.EncodeToString(payload)             // Incompressible 2 MiB model receipt.
	noteContent := hex.EncodeToString(payload[:8<<10]) // 16,384 ASCII characters, within the tool's 20,000-character limit.
	failedInv := noteInv(a, "note_save", "note/disk-full", map[string]any{"kind": "fact", "title": "Failed synthetic note", "content": noteContent, "source": "fixture://runtime-enospc/failed", "metadata": map[string]any{"synthetic": true}})
	// Exercise the real frozen tool schema before filling. A validation failure
	// must never be mistaken for a database write fault.
	runtimeFaultNeed(t, authorize(a, failedInv))
	largeReceipt, e := execution.NativeReceipt(200, nativeProviderResult(content))
	runtimeFaultNeed(t, e)
	var before map[string]json.RawMessage
	var usageA, usageB json.RawMessage
	var release func()
	var attemptB string
	var endErr error
	effectsB := nativeEffects{complete: func(c context.Context, r Run, id string, m []Message, ts []Tool) (json.RawMessage, error) {
		request := execution.NativeModelRequest(r.Snapshot.Agent.Model, m, ts)
		attempt, cached, err := engine.BeginModel(c, r, id, request)
		if err != nil {
			return nil, err
		}
		if cached != nil {
			t.Fatal("unexpected initial pending-model cache")
		}
		attemptB = attempt
		var staged bool
		runtimeFaultNeed(t, db.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM harness_archives h JOIN harness_exchanges x USING(run_id,seq) JOIN calls q ON q.id=$2 JOIN model_attempts a ON a.id=$3 WHERE h.run_id=$1 AND h.seq=1 AND h.batch IS NULL AND h.manifest_hash<>'' AND x.method='model' AND x.response IS NULL AND q.status='inflight' AND q.response IS NULL AND a.status='inflight' AND a.response IS NULL)`, r.ID, id, attempt).Scan(&staged))
		if !staged {
			t.Fatal("model archive acknowledgement and attempt not durable before fill")
		}
		runtimeFaultNeed(t, engine.Heartbeat(c, a, 120*time.Second))
		runtimeFaultNeed(t, engine.Heartbeat(c, b, 120*time.Second))
		ua, err := s.Usage(c, a.ID)
		runtimeFaultNeed(t, err)
		ub, err := s.Usage(c, b.ID)
		runtimeFaultNeed(t, err)
		if ua.Attempts != 1 || ua.UnknownUsageAttempts != 0 || ua.AccountedTokens != 250 || ua.CachedPromptTokens != 120 || ub.Attempts != 1 || ub.UnknownUsageAttempts != 1 || ub.AccountedTokens <= 0 {
			t.Fatal("pre-fault budget accounting differs", ua, ub)
		}
		usageA, usageB = JSON(ua), JSON(ub)
		before = runtimeFaultRows(t, c, db)
		runtimeFaultSave(t, f, "baseline.json", map[string]any{"table_sha256": runtimeFaultRowDigests(before), "usage_a": ua, "usage_b": ub, "baseline_note_receipt_sha256": Hash(baselineReceipt), "baseline_model_receipt_sha256": Hash(receiptA), "native_engine": EngineDSHNative, "role_mode": "NOLOGIN roles via SET ROLE; not separate login authentication", "real_provider_calls": 0})
		_, err = admin.Exec(c, "CHECKPOINT")
		runtimeFaultNeed(t, err)
		var fill map[string]any
		release, fill = runtimeFaultFill(t, c, f)
		fill["initial_postmaster_pid"] = initialPID
		runtimeFaultSave(t, f, "fill.json", fill)
		logNote, err := os.Lstat(f.PGLog)
		runtimeFaultNeed(t, err)
		_, noteErr := s.LocalTool(c, failedInv)
		runtimeFaultRequire53100(t, f, "note", "INSERT INTO documents(", logNote, noteErr)
		// A separate offset and exact PgError prove this is not merely reusing
		// the note's error. A crashed/unavailable server does not pass.
		logModel, err := os.Lstat(f.PGLog)
		runtimeFaultNeed(t, err)
		endErr = engine.EndModel(c, r, id, attempt, largeReceipt, nil)
		runtimeFaultRequire53100(t, f, "model", "UPDATE calls SET", logModel, endErr)
		return request, endErr
	}}
	_, _, e = engine.Exchange(ctx, &b, nativeFrame(1, "generation", "Synthetic pending model result"), effectsB)
	if endErr == nil || !errors.Is(e, endErr) || before == nil || release == nil {
		t.Fatal("expected model receipt failure was not propagated")
	}
	release()
	recoveredPID := runtimeFaultRestart(t, ctx, f, admin, db, app, worker)
	var recoveredSystemID string
	runtimeFaultNeed(t, admin.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&recoveredSystemID))
	if recoveredSystemID != systemID {
		t.Fatal("recovery replaced the cluster")
	}
	// All comparisons below precede any heartbeat, takeover, retry or replay.
	after := runtimeFaultRows(t, ctx, db)
	runtimeFaultSameRows(t, before, after)
	ua, e := s.Usage(ctx, a.ID)
	runtimeFaultNeed(t, e)
	ub, e := s.Usage(ctx, b.ID)
	runtimeFaultNeed(t, e)
	if !jsonEqual(usageA, JSON(ua)) || !jsonEqual(usageB, JSON(ub)) {
		t.Fatal("recovery changed model accounting")
	}
	var failedDocs, failedCalls, failedAudits, failedEvents int
	runtimeFaultNeed(t, db.QueryRow(ctx, `SELECT (SELECT count(*) FROM documents WHERE title='Failed synthetic note'),(SELECT count(*) FROM calls WHERE id=$1),(SELECT count(*) FROM note_audits WHERE operation_id=$1),(SELECT count(*) FROM events WHERE payload->>'operation_id'=$1)`, failedInv.OperationID).Scan(&failedDocs, &failedCalls, &failedAudits, &failedEvents))
	if failedDocs+failedCalls+failedAudits+failedEvents != 0 {
		t.Fatal("partial failed note survived")
	}
	var callStatus, attemptStatus string
	var responseAbsent, unfinished bool
	runtimeFaultNeed(t, db.QueryRow(ctx, `SELECT c.status,a.status,c.response IS NULL AND a.response IS NULL,a.finished_at IS NULL FROM calls c JOIN model_attempts a ON a.id=c.attempt WHERE c.id=$1 AND a.id=$2`, b.ID+"/dsh/1", attemptB).Scan(&callStatus, &attemptStatus, &responseAbsent, &unfinished))
	if callStatus != "inflight" || attemptStatus != "inflight" || !responseAbsent || !unfinished {
		t.Fatal("failed EndModel left a partial outcome")
	}
	runtimeFaultSave(t, f, "before-retry.json", map[string]any{"table_sha256": runtimeFaultRowDigests(after), "all_committed_rows_unchanged": true, "failed_note_documents": failedDocs, "failed_note_calls": failedCalls, "failed_note_audits": failedAudits, "failed_note_events": failedEvents, "pending_model_call_status": callStatus, "pending_model_attempt_status": attemptStatus, "pending_model_response_absent": responseAbsent, "usage_a": ua, "usage_b": ub, "same_cluster_system_identifier": true, "recovered_postmaster_pid": recoveredPID})
	// Use the public lease API and the clock; never rewrite leases with SQL.
	runtimeFaultNeed(t, engine.Heartbeat(ctx, a, 200*time.Millisecond))
	var next Run
	for until := time.Now().Add(5 * time.Second); ; {
		next, e = engine.Claim(ctx, "enospc-replacement-a", 30*time.Second)
		if e == nil {
			break
		}
		if !errors.Is(e, execution.ErrNoWork) || time.Now().After(until) {
			t.Fatal("natural takeover failed", e)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if next.ID != a.ID || next.Fence <= a.Fence {
		t.Fatal("takeover selected another run or reused the fence")
	}
	if _, err := s.LocalTool(ctx, baselineInv); !errors.Is(err, ErrFence) {
		t.Fatal("stale note owner accepted", err)
	}
	if err := engine.EndModel(ctx, a, a.ID+"/dsh/1", attemptA, receiptA, nil); !errors.Is(err, ErrFence) {
		t.Fatal("stale model completion accepted", err)
	}
	if _, _, err := engine.Exchange(ctx, &a, frameA, effectsA); !errors.Is(err, ErrFence) {
		t.Fatal("stale native exchange accepted", err)
	}
	value, _, e := engine.Exchange(ctx, &next, frameA, effectsA)
	runtimeFaultNeed(t, e)
	if !jsonEqual(JSON(valueA), JSON(value)) || syntheticCompletions != 1 {
		t.Fatal("frozen native replay changed or repeated completion")
	}
	_, cached, e := engine.BeginModel(ctx, next, a.ID+"/dsh/1", requestA)
	runtimeFaultNeed(t, e)
	if !jsonEqual(cached, receiptA) {
		t.Fatal("completed raw model receipt cache differs")
	}
	baselineInv.Owner, baselineInv.Fence = next.Owner, next.Fence
	replayed, e := s.LocalTool(ctx, baselineInv)
	runtimeFaultNeed(t, e)
	if !jsonEqual(replayed, baselineReceipt) {
		t.Fatal("retained note receipt changed")
	}
	failedInv.Owner, failedInv.Fence = next.Owner, next.Fence
	retried, e := s.LocalTool(ctx, failedInv)
	runtimeFaultNeed(t, e)
	retriedAgain, e := s.LocalTool(ctx, failedInv)
	runtimeFaultNeed(t, e)
	if !jsonEqual(retried, retriedAgain) {
		t.Fatal("note retry duplicated or changed receipt")
	}
	var retryDocument Document
	runtimeFaultNeed(t, json.Unmarshal(retried, &retryDocument))
	if retryDocument.ID == "" || retryDocument.Content != noteContent || retryDocument.Source != "fixture://runtime-enospc/failed" || retryDocument.Scope != snap.AgentRef.ID {
		t.Fatal("retried note bytes, source or scope differ")
	}
	// Check the actual retained lifecycle relationships as well as total rows.
	// The deferred audit FK must resolve to the same completed operation.
	for _, note := range []struct {
		doc Document
		inv Invocation
	}{{baselineDocument, baselineInv}, {retryDocument, failedInv}} {
		var linked int
		runtimeFaultNeed(t, db.QueryRow(ctx, `SELECT count(*) FROM documents d JOIN note_versions v ON v.document_id=d.id JOIN note_roots n ON n.root_id=v.root_id JOIN note_audits a ON a.document_id=d.id AND a.root_id=n.root_id JOIN calls c ON c.id=a.operation_id JOIN events e ON e.run_id=c.run_id AND e.kind='tool_completed' AND e.payload->>'operation_id'=c.id WHERE d.id=$1 AND n.root_id=d.id AND n.current_id=d.id AND n.state='active' AND a.action='create' AND a.actor_kind='agent' AND a.actor_id=d.scope AND a.run_id=$2 AND a.operation_id=$3 AND c.run_id=$2 AND c.kind='tool' AND c.status='completed' AND c.response->>'id'=d.id`, note.doc.ID, note.inv.RunID, note.inv.OperationID).Scan(&linked))
		if linked != 1 {
			t.Fatal("note lifecycle, audit, receipt and event are not linked exactly once")
		}
	}
	var docs, roots, versions, audits, noteCalls, noteEvents, modelAttempts, completedEvents int
	runtimeFaultNeed(t, db.QueryRow(ctx, `SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM note_roots),(SELECT count(*) FROM note_versions),(SELECT count(*) FROM note_audits),(SELECT count(*) FROM calls WHERE kind='tool' AND status='completed'),(SELECT count(*) FROM events WHERE kind='tool_completed'),(SELECT count(*) FROM model_attempts),(SELECT count(*) FROM events WHERE kind='model_completed')`).Scan(&docs, &roots, &versions, &audits, &noteCalls, &noteEvents, &modelAttempts, &completedEvents))
	if docs != 2 || roots != 2 || versions != 2 || audits != 2 || noteCalls != 2 || noteEvents != 2 || modelAttempts != 2 || completedEvents != 1 {
		t.Fatal("retry/replay duplicated lifecycle, calls, events or attempts", docs, roots, versions, audits, noteCalls, noteEvents, modelAttempts, completedEvents)
	}
	ua, e = s.Usage(ctx, a.ID)
	runtimeFaultNeed(t, e)
	ub, e = s.Usage(ctx, b.ID)
	runtimeFaultNeed(t, e)
	if !jsonEqual(usageA, JSON(ua)) || !jsonEqual(usageB, JSON(ub)) {
		t.Fatal("replay or retry changed model accounting")
	}
	runtimeFaultSave(t, f, "result.json", map[string]any{"passed": true, "real_enospc": true, "note_sqlstate": "53100", "end_model_sqlstate": "53100", "same_pgdata_recovery": true, "pre_retry_rows_unchanged": true, "documents": docs, "note_roots": roots, "note_versions": versions, "note_audits": audits, "tool_calls": noteCalls, "tool_completed_events": noteEvents, "model_attempts": modelAttempts, "model_completed_events": completedEvents, "unknown_attempts_preserved": ub.UnknownUsageAttempts, "natural_fence_before": a.Fence, "natural_fence_after": next.Fence, "synthetic_completed_outcomes": syntheticCompletions, "real_provider_calls": 0, "scope": "Mac Runtime Owner transactions and NOLOGIN SET ROLE pools; archive is in memory; excludes independent Runtime/Worker processes, Node, real Storage, paid providers, Windows, host-disk exhaustion, Compact and remote tool receipts"})
}
