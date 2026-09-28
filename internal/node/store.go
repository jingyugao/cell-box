// SPDX-License-Identifier: Apache-2.0

// Package node owns checkpoint storage and the host runtime boundary.
package node

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

const DefaultBase = "/var/lib/resumablepod"
const DefaultRunsc = "/usr/local/bin/runsc"
const DefaultRoot = "/run/containerd/runsc/k8s.io"

var safeID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{0,100}$`)

func ValidID(s string) bool { return safeID.MatchString(s) }

type Manifest struct {
	OwnerUID  string            `json:"ownerUID"`
	SpecHash  string            `json:"specHash"`
	ImageID   string            `json:"imageID"`
	RunscHash string            `json:"runscHash"`
	Files     map[string]string `json:"files"`
}
type Ticket struct {
	OwnerUID  string `json:"ownerUID"`
	PodUID    string `json:"podUID"`
	Namespace string `json:"namespace"`
	PodName   string `json:"podName"`
	Snapshot  string `json:"snapshot"`
	SpecHash  string `json:"specHash"`
}

func SnapshotPath(base, owner, snapshot string) (string, error) {
	if !ValidID(owner) || !ValidID(snapshot) {
		return "", fmt.Errorf("invalid storage identifier")
	}
	return filepath.Join(base, "workloads", owner, snapshot), nil
}
func Digest(path string) (string, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// AtomicJSON fsyncs both content and the containing directory before returning.
func AtomicJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".write-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}
func SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func ReadJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
func Seal(path string, m Manifest) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	m.Files = map[string]string{}
	for _, e := range entries {
		if e.Name() == "intent.json" {
			continue
		}
		h, err := Digest(filepath.Join(path, e.Name()))
		if err != nil {
			return err
		}
		m.Files[e.Name()] = h
		f, err := os.Open(filepath.Join(path, e.Name()))
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	if _, ok := m.Files["checkpoint.img"]; !ok {
		return fmt.Errorf("checkpoint.img missing")
	}
	return AtomicJSON(filepath.Join(path, "manifest.json"), m)
}
func Verify(path, owner, hash, runsc string) (*Manifest, error) {
	// All ancestors are root-owned private directories. Reject symlinked checkpoints.
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("invalid checkpoint directory")
	}
	var m Manifest
	if err = ReadJSON(filepath.Join(path, "manifest.json"), &m); err != nil {
		return nil, err
	}
	if m.OwnerUID != owner || m.SpecHash != hash {
		return nil, fmt.Errorf("snapshot owner/spec mismatch")
	}
	actual, err := Digest(runsc)
	if err != nil {
		return nil, err
	}
	if actual != m.RunscHash {
		return nil, fmt.Errorf("runsc binary changed since checkpoint")
	}
	if len(m.Files) == 0 || m.Files["checkpoint.img"] == "" {
		return nil, fmt.Errorf("empty snapshot manifest")
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.Name() != "manifest.json" && e.Name() != "intent.json" && m.Files[e.Name()] == "" {
			return nil, fmt.Errorf("unmanifested checkpoint file %q", e.Name())
		}
	}
	for name, want := range m.Files {
		if filepath.Base(name) != name || name == "." || name == ".." {
			return nil, fmt.Errorf("unsafe manifest filename")
		}
		got, err := Digest(filepath.Join(path, name))
		if err != nil {
			return nil, err
		}
		if got != want {
			return nil, fmt.Errorf("checkpoint integrity mismatch: %s", name)
		}
	}
	return &m, nil
}
