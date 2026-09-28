// SPDX-License-Identifier: Apache-2.0

package node

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// matchesShim requires the installed executable, namespace, socket and full
// sandbox ID. A name substring or stale PID is never sufficient authorization.
func matchesShim(exe string, cmd []byte, id, socket string) bool {
	if exe != "/usr/local/bin/containerd-shim-runsc-v1" {
		return false
	}
	args := strings.Split(string(bytes.TrimSuffix(cmd, []byte{0})), "\x00")
	values := map[string]string{}
	for i := 1; i+1 < len(args); i++ {
		switch args[i] {
		case "-id", "-namespace", "-address":
			values[args[i]] = args[i+1]
			i++
		}
	}
	return values["-id"] == id && values["-namespace"] == "k8s.io" && values["-address"] == socket
}

// reapShim is only called after an identity-checked owned sandbox was forcibly
// destroyed. Failed Start can strand the shim's IO/wait goroutines indefinitely.
// pidfds prevent sending a signal to a process that reused the numeric PID.
func (b *Backend) reapShim(id string) error {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		proc := filepath.Join("/proc", entry.Name())
		exe, err := os.Readlink(filepath.Join(proc, "exe"))
		if err != nil || exe != "/usr/local/bin/containerd-shim-runsc-v1" {
			continue
		}
		fd, err := unix.PidfdOpen(pid, 0)
		if err == unix.ESRCH {
			continue
		}
		if err != nil {
			return fmt.Errorf("open shim pidfd: %w", err)
		}
		exe, exeErr := os.Readlink(filepath.Join(proc, "exe"))
		cmd, cmdErr := os.ReadFile(filepath.Join(proc, "cmdline"))
		if exeErr == nil && cmdErr == nil && matchesShim(exe, cmd, id, b.Socket) {
			err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			unix.Close(fd)
			if err != nil && err != unix.ESRCH {
				return err
			}
			log.Printf("reaped owned sandbox shim sandbox=%s pid=%d", id, pid)
		} else {
			unix.Close(fd)
		}
	}
	return nil
}
