package guest

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jingyugao/cell-box/internal/guestapi"
	"golang.org/x/sys/unix"
)

var revisionPattern = regexp.MustCompile("^[0-9a-f]{64}$")

func mountedWorkload(cfg guestapi.Config) bool {
	return len(cfg.Command) == 2 && cfg.Command[0] == guestapi.Binary && cfg.Command[1] == "mounted-workload"
}

// The control plane owns both the per-Box selection and the immutable executables.
// A release-native profile can boot directly from the published default.
func (s *Server) selectedWorkload() (guestapi.Config, string, error) {
	cfg := s.cfg
	s.identityMu.Lock()
	id := s.boxID
	s.identityMu.Unlock()
	root := cfg.Env[guestapi.WorkloadReleasesEnv]
	if root == "" || id == "" {
		return cfg, "", nil
	}
	if !boxIDPattern.MatchString(id) {
		return cfg, "", errors.New("invalid workload Box ID")
	}
	b, err := os.ReadFile(filepath.Join(root, "boxes", id))
	if errors.Is(err, os.ErrNotExist) && cfg.Env[guestapi.ReleaseToolsEnv] != "" {
		b, err = os.ReadFile(filepath.Join(root, "current"))
	}
	if errors.Is(err, os.ErrNotExist) {
		if mountedWorkload(cfg) {
			return cfg, "", errors.New("no mounted workload has been published")
		}
		return cfg, "", nil
	}
	if err != nil {
		return cfg, "", err
	}
	revision := strings.TrimSpace(string(b))
	if !revisionPattern.MatchString(revision) {
		return cfg, "", errors.New("invalid workload revision")
	}
	entry := filepath.Join(root, "versions", revision, "launch")
	st, err := os.Stat(entry)
	if err != nil {
		return cfg, "", err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0111 == 0 {
		return cfg, "", errors.New("workload entry is not executable")
	}
	cfg.Command = []string{entry}
	return cfg, revision, nil
}

func (s *Server) updateWorkload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Revision string `json:"revision"`
	}
	if !decodeJSONLimit(w, r, &req, 256) {
		return
	}
	if !revisionPattern.MatchString(req.Revision) {
		http.Error(w, "invalid workload revision", 400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, revision, err := s.selectedWorkload()
	if err != nil || revision != req.Revision {
		http.Error(w, "requested workload is not available", 422)
		return
	}
	if !s.active {
		http.Error(w, "workload has not been activated", 409)
		return
	}
	if s.proc != nil {
		err := s.proc.Signal(syscall.Signal(0))
		switch {
		case err == nil:
			if s.revision == revision {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if err := stopWorkload(s.proc); err != nil {
				http.Error(w, "could not stop workload", 500)
				return
			}
		case !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, unix.ESRCH):
			http.Error(w, "could not inspect workload", 500)
			return
		}
	}
	s.proc, s.revision = nil, ""
	p, err := s.launch(cfg)
	if err != nil {
		http.Error(w, "could not start workload", 500)
		return
	}
	s.proc, s.revision = p, revision
	w.WriteHeader(http.StatusNoContent)
}

// The launcher handles TERM, including its ACP subprocesses. Bound the wait for
// an unresponsive launcher using its private process group.
func stopWorkload(p *os.Process) error {
	for _, sig := range []syscall.Signal{unix.SIGTERM, unix.SIGKILL} {
		if err := unix.Kill(-p.Pid, sig); err != nil && !errors.Is(err, unix.ESRCH) {
			return err
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err := p.Signal(syscall.Signal(0)); errors.Is(err, os.ErrProcessDone) || errors.Is(err, unix.ESRCH) {
				return nil
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	return errors.New("workload termination timed out")
}

func releaseToolEnabled(c guestapi.Config, id string) bool {
	return c.Env[guestapi.WorkloadReleasesEnv] != "" && slices.Contains(strings.Split(c.Env[guestapi.ReleaseToolsEnv], ","), id)
}

func (s *Server) toolExecutable(t guestapi.Tool) (string, error) {
	if releaseToolEnabled(s.cfg, t.ID) {
		s.mu.Lock()
		revision := s.revision
		s.mu.Unlock()
		if revision == "" {
			_, selected, err := s.selectedWorkload()
			if err != nil {
				return "", err
			}
			revision = selected
		}
		if revision != "" {
			root := s.cfg.Env[guestapi.WorkloadReleasesEnv]
			entry := filepath.Join(root, "versions", revision, "protected", filepath.Base(t.Executable))
			// Older releases ship no protected directory and retain their image tools.
			f, err := openReleaseFile(entry)
			if err == nil {
				defer f.Close()
				var fs unix.Statfs_t
				if err = unix.Fstatfs(int(f.Fd()), &fs); err != nil {
					return "", err
				}
				if fs.Flags&unix.ST_RDONLY == 0 {
					return "", errors.New("release tools require a read-only mount")
				}
				// The shared mount is administered by the product publisher. The
				// executable subtree for a privileged tool must also be root-owned.
				for path := entry; path != filepath.Dir(root); path = filepath.Dir(path) {
					info, e := os.Lstat(path)
					if e != nil {
						return "", e
					}
					stat, ok := info.Sys().(*syscall.Stat_t)
					if !ok || stat.Uid != 0 || info.Mode()&(os.ModeSymlink|022) != 0 {
						return "", fmt.Errorf("untrusted protected release path %s", path)
					}
					if path == entry && info.Mode().Perm()&0111 == 0 {
						return "", fmt.Errorf("protected tool is not executable: %s", path)
					}
				}
				return entry, nil
			}
			if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
			if _, e := os.Lstat(filepath.Dir(entry)); !errors.Is(e, os.ErrNotExist) {
				return "", fmt.Errorf("missing protected release tool %s: %w", t.ID, err)
			}
		}
		if mountedWorkload(s.cfg) {
			return "", fmt.Errorf("mounted workload requires a published protected tool: %s", t.ID)
		}
	}
	return t.Executable, checkImmutable(t.Executable, true)
}
