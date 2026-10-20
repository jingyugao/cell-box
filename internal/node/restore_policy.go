// SPDX-License-Identifier: Apache-2.0

package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const restorePolicyFile = "restore.json"

// RestorePolicy is the root-owned, node-local policy for checkpoint restore.
// An empty PageLoading value preserves the runsc default. Prefetch is opt-in;
// PrefetchBinary may be set without Prefetch to enable checkpoint hotset capture
// for later evaluation.
type RestorePolicy struct {
	SandboxMode    string `json:"sandboxMode,omitempty"`
	PageLoading    string `json:"pageLoading,omitempty"`
	Prefetch       bool   `json:"prefetch,omitempty"`
	PrefetchBinary string `json:"prefetchBinary,omitempty"`
}

func (p RestorePolicy) noRoot() bool { return p.SandboxMode == "no-root" }

// LoadRestorePolicy reads Base/restore.json. A missing policy preserves the
// runtime's current defaults. A present policy must be an immutable-to-users,
// root-owned regular file below a root-owned, non-writable-by-group/other Base.
func LoadRestorePolicy(base string) (RestorePolicy, error) {
	if base == "" {
		return RestorePolicy{}, nil
	}
	baseInfo, err := os.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return RestorePolicy{}, nil
	}
	if err != nil {
		return RestorePolicy{}, fmt.Errorf("stat restore policy base: %w", err)
	}
	if !baseInfo.IsDir() || baseInfo.Mode()&os.ModeSymlink != 0 {
		return RestorePolicy{}, fmt.Errorf("restore policy base is not a real directory")
	}
	path := filepath.Join(base, restorePolicyFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return RestorePolicy{}, nil
	}
	if err != nil {
		return RestorePolicy{}, fmt.Errorf("stat restore policy: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return RestorePolicy{}, fmt.Errorf("restore policy is not a regular file")
	}
	baseStat, ok := baseInfo.Sys().(*syscall.Stat_t)
	if !ok || baseStat.Uid != 0 || baseInfo.Mode().Perm()&0022 != 0 {
		return RestorePolicy{}, fmt.Errorf("restore policy base must be root-owned and not writable by group or other")
	}
	policyStat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || policyStat.Uid != 0 || info.Mode().Perm()&0022 != 0 {
		return RestorePolicy{}, fmt.Errorf("restore policy must be root-owned and not writable by group or other")
	}
	if info.Size() > 64*1024 {
		return RestorePolicy{}, fmt.Errorf("restore policy exceeds 64 KiB")
	}

	// O_NOFOLLOW closes the symlink race between Lstat and Open.
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return RestorePolicy{}, fmt.Errorf("open restore policy: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	openedInfo, err := f.Stat()
	if err != nil {
		return RestorePolicy{}, fmt.Errorf("stat opened restore policy: %w", err)
	}
	if !os.SameFile(info, openedInfo) {
		return RestorePolicy{}, fmt.Errorf("restore policy changed while opening")
	}
	contents, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return RestorePolicy{}, fmt.Errorf("read restore policy: %w", err)
	}
	if len(contents) > 64*1024 {
		return RestorePolicy{}, fmt.Errorf("restore policy exceeds 64 KiB")
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(contents))
	if err := decoder.Decode(&fields); err != nil {
		return RestorePolicy{}, fmt.Errorf("decode restore policy: %w", err)
	}
	if fields == nil {
		return RestorePolicy{}, fmt.Errorf("restore policy must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return RestorePolicy{}, fmt.Errorf("restore policy has trailing JSON value")
		}
		return RestorePolicy{}, fmt.Errorf("decode restore policy trailer: %w", err)
	}
	for field := range fields {
		switch field {
		case "sandboxMode", "pageLoading", "prefetch", "prefetchBinary":
		default:
			return RestorePolicy{}, fmt.Errorf("restore policy has unknown field %q", field)
		}
	}
	var policy RestorePolicy
	if err := json.Unmarshal(contents, &policy); err != nil {
		return RestorePolicy{}, fmt.Errorf("decode restore policy fields: %w", err)
	}
	if err := validateRestorePolicy(policy); err != nil {
		return RestorePolicy{}, err
	}
	return policy, nil
}

func validateRestorePolicy(policy RestorePolicy) error {
	switch policy.SandboxMode {
	case "", "root", "no-root":
	default:
		return fmt.Errorf("unknown restore sandbox mode %q", policy.SandboxMode)
	}
	switch policy.PageLoading {
	case "", "eager", "lazy":
	default:
		return fmt.Errorf("unknown restore page-loading mode %q", policy.PageLoading)
	}
	if policy.PrefetchBinary != "" {
		if !filepath.IsAbs(policy.PrefetchBinary) || filepath.Clean(policy.PrefetchBinary) != policy.PrefetchBinary {
			return fmt.Errorf("prefetch binary path must be absolute and clean")
		}
	}
	if policy.Prefetch && policy.PageLoading != "lazy" {
		return fmt.Errorf("prefetch requires lazy page loading")
	}
	if policy.Prefetch && policy.PrefetchBinary == "" {
		return fmt.Errorf("prefetch requires a configured prefetch binary")
	}
	return nil
}
