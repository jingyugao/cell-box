package guest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
	"golang.org/x/sys/unix"
)

func writeCredential(root, slot string, body io.Reader, id guestapi.Identity) error {
	if !namePattern.MatchString(slot) {
		return errors.New("invalid credential slot")
	}
	d, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(d)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	tmp := "." + slot + "." + hex.EncodeToString(nonce[:])
	fd, err := unix.Openat2(d, tmp, &unix.OpenHow{Flags: unix.O_WRONLY | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC, Mode: 0600, Resolve: resolveFlags})
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), tmp)
	defer unix.Unlinkat(d, tmp, 0)
	if err := f.Chown(int(id.UID), int(id.GID)); err != nil {
		f.Close()
		return err
	}
	if _, err := copyLimited(f, body, 1<<20); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := unix.Renameat(d, tmp, d, slot); err != nil {
		return err
	}
	return unix.Fsync(d)
}

func writeCredentialBatch(root string, batch guestapi.CredentialBatch, id guestapi.Identity) error {
	if err := batch.Validate(); err != nil {
		return err
	}
	slots := make([]string, 0, len(batch.Slots))
	for slot := range batch.Slots {
		slots = append(slots, slot)
	}
	sort.Strings(slots)
	for _, slot := range slots {
		if err := writeCredential(root, slot, bytes.NewReader(batch.Slots[slot]), id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) runTool(ctx context.Context, t compiledTool, req guestapi.ToolRequest) (result guestapi.ExecResult, err error) {
	if err := validateToolArgs(t, req.Args); err != nil {
		return guestapi.ExecResult{}, err
	}
	if err := checkImmutable(t.spec.Executable, true); err != nil {
		return guestapi.ExecResult{}, err
	}
	if t.spec.WorkspaceWrite {
		workspace, statErr := os.Lstat(s.cfg.Workspace)
		if statErr != nil || !workspace.IsDir() || workspace.Mode()&os.ModeSymlink != 0 {
			return guestapi.ExecResult{}, errors.New("unsafe workspace for writable tool")
		}
		owner, ok := workspace.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != s.cfg.Agent.UID || owner.Gid != s.cfg.Agent.GID {
			return guestapi.ExecResult{}, errors.New("workspace owner differs from agent")
		}
		mode := workspace.Mode().Perm()
		// The debug identity cannot write agent-owned 0700 directories. Transfer
		// ownership only for the duration of this explicitly admitted tool.
		if s.cfg.Debug.UID != 0 {
			if err := chownWorkspaceTree(s.cfg.Workspace, s.cfg.Debug); err != nil {
				_ = chownWorkspaceTree(s.cfg.Workspace, s.cfg.Agent)
				return guestapi.ExecResult{}, err
			}
		}
		defer func() {
			err = errors.Join(err, chownWorkspaceTree(s.cfg.Workspace, s.cfg.Agent), os.Chmod(s.cfg.Workspace, mode))
		}()
	}
	env := make(map[string]string, len(t.spec.CredentialEnv))
	for key, slot := range t.spec.CredentialEnv {
		p := filepath.Join(debugRoot, slot)
		fi, err := os.Lstat(p)
		if err != nil {
			return guestapi.ExecResult{}, fmt.Errorf("credential slot %q unavailable", slot)
		}
		if !fi.Mode().IsRegular() || fi.Mode().Perm() != 0600 {
			return guestapi.ExecResult{}, fmt.Errorf("credential slot %q unsafe", slot)
		}
		env[key] = p
	}
	argv := append([]string{t.spec.Executable}, t.spec.Args...)
	argv = append(argv, req.Args...)
	groups := []uint32(nil)
	if t.spec.WorkspaceRead {
		groups = []uint32{s.cfg.Agent.GID}
	}
	home := debugRoot
	if s.cfg.DebugHome != "" {
		home = s.cfg.DebugHome
	}
	workingDir := home
	if t.spec.WorkspaceRead {
		workingDir = s.cfg.Workspace
	}
	result, err = runWithGroups(ctx, s.self, s.cfg.Debug, groups, argv, workingDir, home, env, req.TimeoutMS)
	return result, err
}

func validateToolArgs(t compiledTool, args []string) error {
	if t.spec.PassThroughArgs {
		if len(args) > 32 {
			return errors.New("too many tool arguments")
		}
	} else if len(args) != len(t.patterns) {
		return errors.New("tool argument count mismatch")
	}
	total := 0
	for i, a := range args {
		if len(a) > 8192 || strings.ContainsRune(a, 0) {
			return fmt.Errorf("tool argument %d rejected", i)
		}
		if t.spec.PassThroughArgs {
			total += len(a)
			if total > 8192 {
				return errors.New("tool arguments exceed 8192 bytes")
			}
		} else if !t.patterns[i].MatchString(a) {
			return fmt.Errorf("tool argument %d rejected", i)
		}
	}
	return nil
}

func chownWorkspaceTree(root string, owner guestapi.Identity) error {
	return filepath.WalkDir(root, func(path string, _ os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		return os.Lchown(path, int(owner.UID), int(owner.GID))
	})
}

func startWorkload(self string, c guestapi.Config) (*os.Process, error) {
	cmd := exec.Command(self, append([]string{"__child"}, c.Command...)...)
	cmd.Dir = c.Workspace
	if c.CommandDir != "" {
		cmd.Dir = c.CommandDir
	}
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=/home/agent", "LANG=C.UTF-8"}
	for k, v := range c.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: c.Agent.UID, Gid: c.Agent.GID, NoSetGroups: os.Geteuid() != 0}, Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() { _ = cmd.Wait() }()
	return cmd.Process, nil
}

type peerListener struct {
	*net.UnixListener
	uid uint32
}

func (l peerListener) Accept() (net.Conn, error) {
	for {
		c, err := l.AcceptUnix()
		if err != nil {
			return nil, err
		}
		var cred *unix.Ucred
		rc, err := c.SyscallConn()
		if err == nil {
			err = rc.Control(func(fd uintptr) { cred, err = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
		}
		if err == nil && cred != nil && cred.Uid == l.uid {
			return c, nil
		}
		c.Close()
	}
}

func (s *Server) startToolSocket() error {
	if err := os.Remove(toolSocket); err != nil && !os.IsNotExist(err) {
		return err
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: toolSocket, Net: "unix"})
	if err != nil {
		return err
	}
	if err := os.Chown(toolSocket, int(s.cfg.Agent.UID), int(s.cfg.Agent.GID)); err != nil {
		l.Close()
		return err
	}
	if err := os.Chmod(toolSocket, 0600); err != nil {
		l.Close()
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tools/{id}", s.toolHandler)
	go func() {
		_ = (&http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}).Serve(peerListener{l, s.cfg.Agent.UID})
	}()
	return nil
}

// ToolClient is used only from an agent process, through the peer-credential
// checked Unix socket. It never accepts an executable path or environment.
func ToolClient(id string, args []string) (guestapi.ExecResult, error) {
	if !namePattern.MatchString(id) {
		return guestapi.ExecResult{}, errors.New("invalid tool ID")
	}
	body, err := json.Marshal(guestapi.ToolRequest{Args: args})
	if err != nil {
		return guestapi.ExecResult{}, err
	}
	client := &http.Client{Timeout: 5 * time.Minute, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", toolSocket)
	}}}
	resp, err := client.Post("http://unix/v1/tools/"+id, "application/json", bytes.NewReader(body))
	if err != nil {
		return guestapi.ExecResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return guestapi.ExecResult{}, fmt.Errorf("tool request failed: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var result guestapi.ExecResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}
