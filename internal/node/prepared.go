package node

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
)

// The node cache assumes private node-owned local storage. It is not
// a substitute for immutable object identities in a multi-writer store.
type fingerprint struct {
	Device, Inode               uint64
	Size, ModifiedNS, ChangedNS int64
}

type prepared struct {
	Manifest Manifest
	Files    map[string]fingerprint
}

func identify(path string) (fingerprint, error) {
	s, err := os.Lstat(path)
	if err != nil {
		return fingerprint{}, err
	}
	if !s.Mode().IsRegular() {
		return fingerprint{}, fmt.Errorf("not a regular file: %s", path)
	}
	v := s.Sys().(*syscall.Stat_t)
	return fingerprint{Device: uint64(v.Dev), Inode: v.Ino, Size: s.Size(), ModifiedNS: v.Mtim.Nano(), ChangedNS: v.Ctim.Nano()}, nil
}

func fingerprints(path, runsc string) (map[string]fingerprint, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("invalid checkpoint directory")
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	paths := map[string]string{"runsc": runsc}
	for _, e := range entries {
		paths["checkpoint/"+e.Name()] = filepath.Join(path, e.Name())
	}
	files := map[string]fingerprint{}
	for key, file := range paths {
		files[key], err = identify(file)
		if err != nil {
			return nil, err
		}
	}
	return files, nil
}

// VerifyPrepared is the request-path check shared by the controller and adapter.
// A cache record is local to this node and never included in an OSS archive.
func VerifyPrepared(path, owner, spec, runsc string) (*Manifest, error) {
	var p prepared
	if err := ReadJSON(path+".verified.json", &p); err != nil {
		return nil, err
	}
	files, err := fingerprints(path, runsc)
	if err != nil {
		return nil, err
	}
	if p.Manifest.OwnerUID != owner || p.Manifest.SpecHash != spec || !reflect.DeepEqual(p.Files, files) {
		return nil, fmt.Errorf("checkpoint/runtime changed since preparation")
	}
	return &p.Manifest, nil
}

// VerifyCached performs full verification on a miss and seals a node-local
// identity record. Only private, immutable checkpoint directories may use it.
func VerifyCached(path, owner, spec, runsc string) (*Manifest, error) {
	if m, err := VerifyPrepared(path, owner, spec, runsc); err == nil {
		return m, nil
	}
	before, err := fingerprints(path, runsc)
	if err != nil {
		return nil, err
	}
	m, err := Verify(path, owner, spec, runsc)
	if err != nil {
		return nil, err
	}
	after, err := fingerprints(path, runsc)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(before, after) {
		return nil, fmt.Errorf("checkpoint changed while verifying")
	}
	if err = AtomicJSON(path+".verified.json", prepared{Manifest: *m, Files: after}); err != nil {
		return nil, err
	}
	return m, nil
}
