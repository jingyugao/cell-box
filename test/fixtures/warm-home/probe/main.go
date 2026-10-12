package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const homePath = "/home/agent"

type fdInfo struct {
	Path   string `json:"path"`
	Open   bool   `json:"open"`
	Device uint64 `json:"device,omitempty"`
	Inode  uint64 `json:"inode,omitempty"`
	Size   int64  `json:"size,omitempty"`
}

type pathCheck struct {
	Path     string `json:"path"`
	Exists   bool   `json:"exists"`
	IsDir    bool   `json:"is_dir,omitempty"`
	Readable bool   `json:"readable"`
}

type heartbeat struct {
	Token           string      `json:"token"`
	Counter         uint64      `json:"counter"`
	PID             int         `json:"pid"`
	CWD             string      `json:"cwd"`
	FD              fdInfo      `json:"fd"`
	DeletedFD       fdInfo      `json:"deleted_fd"`
	MmapValue       string      `json:"mmap_value"`
	Identity        string      `json:"identity"`
	IsolationChecks []pathCheck `json:"isolation_checks"`
	UpdatedAt       time.Time   `json:"updated_at"`
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: probe pause|worker|absent <path>...|present <path>")
	}
	switch os.Args[1] {
	case "pause":
		for {
			time.Sleep(time.Hour)
		}
	case "worker":
		if len(os.Args) != 2 {
			fail("usage: probe worker")
		}
		if err := runWorker(); err != nil {
			fail(err.Error())
		}
	case "absent":
		if len(os.Args) < 3 {
			fail("usage: probe absent <path>...")
		}
		if err := checkAbsent(os.Args[2:]); err != nil {
			fail(err.Error())
		}
	case "present":
		if len(os.Args) != 3 {
			fail("usage: probe present <path>")
		}
		if err := printPresent(os.Args[2]); err != nil {
			fail(err.Error())
		}
	default:
		fail("unknown mode: " + os.Args[1])
	}
}

type absentResult struct {
	Path         string `json:"path"`
	StatError    string `json:"stat_error"`
	OpenError    string `json:"open_error"`
	Inaccessible bool   `json:"inaccessible"`
}

func checkAbsent(paths []string) error {
	results := make([]absentResult, 0, len(paths))
	allInaccessible := true
	for _, path := range paths {
		result := absentResult{Path: path}
		_, statErr := os.Stat(path)
		if statErr != nil {
			result.StatError = statErr.Error()
		}
		f, openErr := os.Open(path)
		if openErr == nil {
			_ = f.Close()
		} else {
			result.OpenError = openErr.Error()
		}
		result.Inaccessible = inaccessibleError(statErr) && inaccessibleError(openErr)
		if !result.Inaccessible {
			allInaccessible = false
		}
		results = append(results, result)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"mode": "absent", "all_inaccessible": allInaccessible, "results": results,
	}); err != nil {
		return fmt.Errorf("write result: %w", err)
	}
	if !allInaccessible {
		return fmt.Errorf("one or more paths were accessible")
	}
	return nil
}

func inaccessibleError(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, fs.ErrPermission)
}

func printPresent(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{
		"mode": "present", "path": path, "content": string(data),
	})
}

func runWorker() error {
	if err := os.Setenv("HOME", homePath); err != nil {
		return fmt.Errorf("set HOME: %w", err)
	}
	workspace := filepath.Join(homePath, "workspace")
	if err := os.MkdirAll(workspace, 0700); err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	if err := os.Chdir(workspace); err != nil {
		return fmt.Errorf("chdir workspace: %w", err)
	}

	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return fmt.Errorf("generate startup token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)

	filePath := filepath.Join(homePath, "open.txt")
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open persistent file: %w", err)
	}
	defer f.Close()

	deletedPath := filepath.Join(homePath, "deleted-open.txt")
	deleted, err := os.OpenFile(deletedPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open deleted-file descriptor: %w", err)
	}
	if err := os.Remove(deletedPath); err != nil {
		_ = deleted.Close()
		return fmt.Errorf("unlink open file: %w", err)
	}
	defer deleted.Close()

	mmapPath := filepath.Join(homePath, "shared-map.bin")
	mmapFile, err := os.OpenFile(mmapPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("open mmap file: %w", err)
	}
	if err := mmapFile.Truncate(4096); err != nil {
		_ = mmapFile.Close()
		return fmt.Errorf("size mmap file: %w", err)
	}
	mapped, err := syscall.Mmap(int(mmapFile.Fd()), 0, 4096, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = mmapFile.Close()
		return fmt.Errorf("map shared file: %w", err)
	}
	defer syscall.Munmap(mapped)
	defer mmapFile.Close()

	for counter := uint64(1); ; counter++ {
		identityBytes, err := os.ReadFile(filepath.Join(homePath, "identity"))
		if err != nil {
			return fmt.Errorf("read identity marker: %w", err)
		}
		identity := string(identityBytes)
		if _, err := fmt.Fprintf(f, "counter=%d token=%s identity=%q\n", counter, token, identity); err != nil {
			return fmt.Errorf("write open file: %w", err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync open file: %w", err)
		}
		if _, err := fmt.Fprintf(deleted, "counter=%d token=%s\n", counter, token); err != nil {
			return fmt.Errorf("write deleted-open file: %w", err)
		}
		if err := deleted.Sync(); err != nil {
			return fmt.Errorf("sync deleted-open file: %w", err)
		}
		mmapValue := fmt.Sprintf("counter=%d token=%s", counter, token)
		copy(mapped, mmapValue)
		if err := unix.Msync(mapped, unix.MS_SYNC); err != nil {
			return fmt.Errorf("sync mmap: %w", err)
		}
		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat open file: %w", err)
		}
		deletedInfo, err := deleted.Stat()
		if err != nil {
			return fmt.Errorf("stat deleted-open file: %w", err)
		}
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("get cwd: %w", err)
		}
		checks := []pathCheck{
			checkPath("/other-home/secret"),
			checkPath("/var/lib/cellbox/homes"),
			checkPath("/data"),
			checkPath(filepath.Join(homePath, "..", "other-home", "secret")),
		}
		beat := heartbeat{
			Token: token, Counter: counter, PID: os.Getpid(), CWD: cwd,
			FD: describeFD(filePath, f, fi), Identity: identity,
			DeletedFD: describeFD(deletedPath, deleted, deletedInfo), MmapValue: string(mapped[:len(mmapValue)]),
			IsolationChecks: checks, UpdatedAt: time.Now().UTC(),
		}
		if err := writeHeartbeat(filepath.Join(homePath, "heartbeat.json"), beat); err != nil {
			return fmt.Errorf("write heartbeat: %w", err)
		}
		if counter == 1 {
			fmt.Printf("worker started: token=%s pid=%d cwd=%s\n", token, os.Getpid(), cwd)
		}
		time.Sleep(time.Second)
	}
}

func describeFD(path string, f *os.File, fi os.FileInfo) fdInfo {
	info := fdInfo{Path: path, Open: f != nil, Size: fi.Size()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		info.Device = uint64(st.Dev)
		info.Inode = st.Ino
	}
	return info
}

func checkPath(path string) pathCheck {
	c := pathCheck{Path: path}
	fi, err := os.Stat(path)
	if err == nil {
		c.Exists = true
		c.IsDir = fi.IsDir()
		if f, openErr := os.Open(path); openErr == nil {
			c.Readable = true
			_ = f.Close()
		}
	}
	return c
}

func writeHeartbeat(path string, beat heartbeat) error {
	data, err := json.Marshal(beat)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func fail(msg string) {
	fmt.Println("probe error: " + msg)
	os.Exit(1)
}
