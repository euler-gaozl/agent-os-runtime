//go:build windows

package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

type nativeRevisionProcess struct {
	cmd        *exec.Cmd
	done       chan struct{}
	err        error
	log        *os.File
	idle       func(context.Context) bool
	term, reap sync.Once
	terminated bool
}

func nativeRevisionHasChild(pid int) (bool, error) {
	snapshot, err := syscall.CreateToolhelp32Snapshot(syscall.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return false, err
	}
	defer syscall.CloseHandle(snapshot)
	var entry syscall.ProcessEntry32
	entry.Size = uint32(unsafe.Sizeof(entry))
	for err = syscall.Process32First(snapshot, &entry); err == nil; err = syscall.Process32Next(snapshot, &entry) {
		if entry.ParentProcessID == uint32(pid) {
			return true, nil
		}
	}
	if !errors.Is(err, syscall.ERROR_NO_MORE_FILES) {
		return false, err
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
	cmd := exec.Command(binary, "--config", configPath)
	cmd.Dir, cmd.Stdout, cmd.Stderr = dir, log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	for _, key := range []string{"PATH", "SystemRoot", "WINDIR", "TEMP", "TMP", "LANG"} {
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
	t.Logf("Windows fixture Worker started pid=%d", cmd.Process.Pid)
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
	for {
		child, err := nativeRevisionHasChild(p.cmd.Process.Pid)
		if err != nil {
			t.Errorf("cannot inspect fixture descendants pid=%d: %v", p.cmd.Process.Pid, err)
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
			t.Errorf("fixture still busy; no termination sent, residual pid=%d", p.cmd.Process.Pid)
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
	// os.Process retains the handle returned by this exact Start, so PID reuse
	// cannot target another process. This is idle fixture teardown, not a claim
	// of graceful shutdown or an in-flight crash/recovery test.
	p.term.Do(func() {
		if err := p.cmd.Process.Kill(); err == nil {
			p.terminated = true
		} else if !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("fixture termination failed: %v", err)
		}
	})
	select {
	case <-p.done:
		p.checkExit(t)
	case <-ctx.Done():
		t.Errorf("fixture did not exit, residual pid=%d", p.cmd.Process.Pid)
	}
}

func (p *nativeRevisionProcess) checkExit(t *testing.T) {
	t.Helper()
	p.reap.Do(func() {
		_ = p.log.Close()
		var exit *exec.ExitError
		if p.err != nil && !(p.terminated && errors.As(p.err, &exit) && exit.ExitCode() == 1) {
			t.Errorf("fixture exited unexpectedly: %v", p.err)
		}
	})
}
