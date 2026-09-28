package guest

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"cellbox.local/cellbox/internal/guestapi"
	"golang.org/x/sys/unix"
)

func Bootstrap(c guestapi.Config) (string, error) {
	tools, err := validateConfig(c)
	if err != nil {
		return "", err
	}
	if os.Geteuid() != 0 {
		return "", errors.New("guest bootstrap requires root")
	}
	if err := ensureDir("/etc", 0, 0, 0755); err != nil {
		return "", err
	}
	if err := ensureAccount("cellbox-agent", c.Agent, "/home/agent"); err != nil {
		return "", err
	}
	debugHome := debugRoot
	if c.DebugHome != "" {
		debugHome = c.DebugHome
	}
	if c.Debug.UID != 0 || c.Debug.GID != 0 {
		if err := ensureDebugAccount(c.Debug, debugHome); err != nil {
			return "", err
		}
	}
	for _, d := range []struct {
		path     string
		uid, gid int
		mode     os.FileMode
	}{
		{c.Workspace, int(c.Agent.UID), int(c.Agent.GID), 0750},
		{"/home/agent", int(c.Agent.UID), int(c.Agent.GID), 0750},
		{debugRoot, int(c.Debug.UID), int(c.Debug.GID), 0700},
		{"/run/cellbox", 0, 0, 0711},
		{toolRoot, 0, 0, 0755},
	} {
		if err := ensureDir(d.path, d.uid, d.gid, d.mode); err != nil {
			return "", err
		}
	}
	if c.DebugHome != "" {
		info, err := os.Lstat(c.DebugHome)
		if err != nil {
			return "", fmt.Errorf("debug host home: %w", err)
		}
		owner, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.IsDir() || uint32(owner.Uid) != c.Debug.UID || uint32(owner.Gid) != c.Debug.GID || info.Mode().Perm()&0300 != 0300 {
			return "", fmt.Errorf("debug host home must be a writable directory owned by the debug identity")
		}
	}
	if err := checkTrustedParents(c.Workspace, c.Agent); err != nil {
		return "", err
	}
	for _, t := range tools {
		if err := checkImmutable(t.spec.Executable, true); err != nil {
			return "", err
		}
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw[:])
	fd, err := unix.Open(guestapi.TokenPath, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), guestapi.TokenPath)
	defer f.Close()
	if err := f.Chmod(0600); err != nil {
		return "", err
	}
	if err := f.Chown(0, 0); err != nil {
		return "", err
	}
	if _, err := f.WriteString(token + "\n"); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", err
	}
	return token, nil
}

func checkTrustedParents(path string, agent guestapi.Identity) error {
	cur := "/"
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Dir(path), "/"), "/") {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			return err
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workspace parent is not a trusted directory: %s", cur)
		}
		if cur == "/home/agent" && path == "/home/agent/workspace" {
			if st.Uid != agent.UID || st.Gid != agent.GID || fi.Mode().Perm()&022 != 0 {
				return fmt.Errorf("workspace parent is not agent-owned and private: %s", cur)
			}
			continue
		}
		if st.Uid != 0 || fi.Mode().Perm()&022 != 0 {
			return fmt.Errorf("workspace parent is not root-owned and immutable: %s", cur)
		}
	}
	return nil
}

func ensureDir(path string, uid, gid int, mode os.FileMode) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("unsafe directory %q", path)
	}
	cur := "/"
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, part := range parts {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if os.IsNotExist(err) {
			createMode := os.FileMode(0755)
			if i == len(parts)-1 {
				createMode = mode
			}
			if err = os.Mkdir(cur, createMode); err != nil {
				return err
			}
			if i < len(parts)-1 {
				if err = os.Chmod(cur, 0755); err != nil {
					return err
				}
			}
			fi, err = os.Lstat(cur)
		}
		if err != nil {
			return err
		}
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe directory component %s", cur)
		}
		if i == len(parts)-1 {
			if err := os.Chown(cur, uid, gid); err != nil {
				return err
			}
			if err := os.Chmod(cur, mode); err != nil {
				return err
			}
		}
	}
	return nil
}

// Reuse a base image account with the host owner's numeric identity when
// present; tool execution sets HOME explicitly to the admitted debug mount.
func ensureDebugAccount(id guestapi.Identity, home string) error {
	f, err := openAccountFile("/etc/passwd", 0)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) < 7 {
			continue
		}
		uid, err := strconv.ParseUint(fields[2], 10, 32)
		if err != nil || uid != uint64(id.UID) {
			continue
		}
		gid, err := strconv.ParseUint(fields[3], 10, 32)
		if err != nil || gid != uint64(id.GID) {
			return fmt.Errorf("existing UID %d has a different GID", id.UID)
		}
		return nil
	}
	return ensureAccount("cellbox-debug", id, home)
}

func ensureAccount(name string, id guestapi.Identity, home string) error {
	if err := ensureGroup(name, id.GID); err != nil {
		return err
	}
	f, err := openAccountFile("/etc/passwd", 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	existing, err := checkPasswd(f, name, id, home)
	if err != nil {
		return err
	}
	if existing {
		return nil
	}
	_, err = f.WriteString(fmt.Sprintf("%s:x:%d:%d:Cellbox guest:%s:/sbin/nologin\n", name, id.UID, id.GID, home))
	if err != nil {
		return err
	}
	return f.Sync()
}

func checkPasswd(r io.Reader, name string, id guestapi.Identity, home string) (bool, error) {
	existing := false
	s := bufio.NewScanner(r)
	for s.Scan() {
		fields := strings.Split(s.Text(), ":")
		if len(fields) < 7 {
			continue
		}
		uid, _ := strconv.ParseUint(fields[2], 10, 32)
		if fields[0] == name {
			if existing || uid != uint64(id.UID) || fields[3] != strconv.FormatUint(uint64(id.GID), 10) || fields[5] != home || fields[6] != "/sbin/nologin" {
				return false, fmt.Errorf("account %s conflicts", name)
			}
			existing = true
		}
		if uid == uint64(id.UID) && fields[0] != name {
			return false, fmt.Errorf("UID %d already belongs to %s", id.UID, fields[0])
		}
	}
	return existing, s.Err()
}

func ensureGroup(name string, gid uint32) error {
	f, err := openAccountFile("/etc/group", 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	existing, err := checkGroup(f, name, gid)
	if err != nil {
		return err
	}
	if existing {
		return nil
	}
	_, err = f.WriteString(fmt.Sprintf("%s:x:%d:\n", name, gid))
	if err != nil {
		return err
	}
	return f.Sync()
}

func openAccountFile(path string, owner uint32) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_APPEND|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0644)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.Mode().IsRegular() || st.Uid != owner || fi.Mode().Perm()&022 != 0 {
		f.Close()
		return nil, fmt.Errorf("unsafe account file %s", path)
	}
	if err := f.Chmod(0644); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func checkGroup(r io.Reader, name string, gid uint32) (bool, error) {
	existing := false
	s := bufio.NewScanner(r)
	for s.Scan() {
		fields := strings.Split(s.Text(), ":")
		if len(fields) < 3 {
			continue
		}
		n, _ := strconv.ParseUint(fields[2], 10, 32)
		if fields[0] == name {
			if existing || n != uint64(gid) {
				return false, fmt.Errorf("group %s conflicts", name)
			}
			existing = true
		}
		if n == uint64(gid) && fields[0] != name {
			return false, fmt.Errorf("GID %d already belongs to %s", gid, fields[0])
		}
	}
	return existing, s.Err()
}
