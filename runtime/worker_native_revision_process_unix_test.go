//go:build darwin || linux

package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

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
