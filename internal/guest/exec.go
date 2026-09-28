package guest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
	"golang.org/x/sys/unix"
)

const maxOutput = 1 << 20

type boundedBuffer struct {
	mu        sync.Mutex
	b         []byte
	truncated bool
}

func (w *boundedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	room := maxOutput - len(w.b)
	if room < 0 {
		room = 0
	}
	if len(p) > room {
		w.truncated = true
		p = p[:room]
	}
	w.b = append(w.b, p...)
	return n, nil
}

// RunChild is the only re-execution mode used for workload processes. The
// credential switch happens when the supervisor starts this binary; this mode
// sets no-new-privileges before replacing itself with the fixed target.
func RunChild(argv []string) error {
	if len(argv) == 0 || !filepath.IsAbs(argv[0]) {
		return errors.New("child executable must be absolute")
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	return unix.Exec(argv[0], argv, os.Environ())
}

func run(ctx context.Context, self string, id guestapi.Identity, argv []string, dir, home string, env map[string]string, timeoutMS int64) (guestapi.ExecResult, error) {
	return runWithGroups(ctx, self, id, nil, argv, dir, home, env, timeoutMS)
}

func runWithGroups(ctx context.Context, self string, id guestapi.Identity, groups []uint32, argv []string, dir, home string, env map[string]string, timeoutMS int64) (guestapi.ExecResult, error) {
	if len(argv) == 0 || len(argv) > 128 || !filepath.IsAbs(argv[0]) {
		return guestapi.ExecResult{}, errors.New("argv must start with an absolute executable")
	}
	for _, a := range argv {
		if strings.ContainsRune(a, 0) || len(a) > 8192 {
			return guestapi.ExecResult{}, errors.New("invalid argument")
		}
	}
	if timeoutMS < 0 || timeoutMS > 300000 {
		return guestapi.ExecResult{}, errors.New("timeout must be between 0 and 300000 ms")
	}
	if timeoutMS == 0 {
		timeoutMS = 30000
	}
	for k, v := range env {
		if !validEnv(k, v) {
			return guestapi.ExecResult{}, fmt.Errorf("invalid environment variable %q", k)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMS)*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, append([]string{"__child"}, argv...)...)
	// A child can spawn a new process group that keeps the inherited output
	// pipes open after the direct child exits. Bound that wait independently
	// of the command timeout and report incomplete output below.
	cmd.WaitDelay = time.Second
	cmd.Dir = dir
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + home, "LANG=C.UTF-8"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// The production supervisor runs as root: clear inherited supplementary
	// groups before switching identity. Unprivileged local tests cannot call setgroups.
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: id.UID, Gid: id.GID, Groups: groups, NoSetGroups: os.Geteuid() != 0 && len(groups) == 0}, Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	var stdout, stderr boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Stdin = nil
	err := cmd.Run()
	r := guestapi.ExecResult{Stdout: string(stdout.b), Stderr: string(stderr.b), Truncated: stdout.truncated || stderr.truncated}
	if errors.Is(err, exec.ErrWaitDelay) {
		r.Truncated = true
		if ctx.Err() != nil {
			r.ExitCode = 124
		}
		return r, nil
	}
	if err == nil {
		return r, nil
	}
	var e *exec.ExitError
	if errors.As(err, &e) {
		r.ExitCode = e.ExitCode()
		if ctx.Err() != nil {
			r.ExitCode = 124
			r.Truncated = true
		}
		return r, nil
	}
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	return r, err
}

func copyLimited(dst io.Writer, src io.Reader, max int64) (int64, error) {
	n, err := io.Copy(dst, io.LimitReader(src, max+1))
	if n > max {
		return n, errors.New("content exceeds limit")
	}
	return n, err
}
