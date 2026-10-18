// Package homevolume manages node-local, per-owner persistent home directories.
package homevolume

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const DefaultBase = "/var/lib/cellbox"
const MountPath = "/home/agent"

// VolumeSpecHash identifies the immutable on-disk format for independent HOME volumes.
const VolumeSpecHash = "cellbox-home-volume-v1"

const (
	identityName = ".identity.json"
	initName     = ".identity.initializing"
	stateName    = ".state.json"
	lockDir      = ".home-locks"
)

var ownerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Identity ties a home directory to one Box lifecycle and its backing inode.
type Identity struct {
	ID       string `json:"id"`
	OwnerUID string `json:"ownerUID"`
	NodeName string `json:"nodeName"`
	SpecHash string `json:"specHash"`
	Device   uint64 `json:"device"`
	Inode    uint64 `json:"inode"`
}

type executionState struct {
	Phase            string `json:"phase"`
	Pod              string `json:"pod,omitempty"`
	Attachment       string `json:"attachment,omitempty"`
	Snapshot         string `json:"snapshot,omitempty"`
	ConsumedSnapshot string `json:"consumedSnapshot,omitempty"`
}

// Attach claims an existing independent HOME volume for one sandbox. It never
// creates a volume or changes its identity/data inode.
func Attach(base, volumeUID, node, attachment string) (Identity, error) {
	if !validOwner(attachment) || strings.TrimSpace(node) == "" {
		return Identity{}, errors.New("node and attachment are required")
	}
	unlock, err := lockOwner(base, volumeUID, false)
	if err != nil {
		return Identity{}, err
	}
	defer unlock()
	identity, err := verify(base, volumeUID, node, VolumeSpecHash, "")
	if err != nil {
		return Identity{}, err
	}
	_, dir, _, _ := paths(base, volumeUID)
	statePath := filepath.Join(dir, stateName)
	state, err := readState(statePath)
	if err != nil {
		return Identity{}, err
	}
	if state.Attachment == attachment {
		return identity, nil
	}
	if state.Attachment != "" {
		return Identity{}, errors.New("home volume is attached to another sandbox")
	}
	if state.Phase != "new" || state.Pod != "" || state.Snapshot != "" || state.ConsumedSnapshot != "" {
		return Identity{}, errors.New("home volume is not available for attachment")
	}
	state.Attachment = attachment
	if err := writeJSONAtomic(statePath, state, 0600); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

// VerifyAttachment validates volume identity and its current sandbox attachment.
func VerifyAttachment(base, volumeUID, node, attachment string) (Identity, error) {
	if !validOwner(attachment) || strings.TrimSpace(node) == "" {
		return Identity{}, errors.New("node and attachment are required")
	}
	unlock, err := lockOwner(base, volumeUID, false)
	if err != nil {
		return Identity{}, err
	}
	defer unlock()
	identity, err := verify(base, volumeUID, node, VolumeSpecHash, "")
	if err != nil {
		return Identity{}, err
	}
	_, dir, _, _ := paths(base, volumeUID)
	state, err := readState(filepath.Join(dir, stateName))
	if err != nil {
		return Identity{}, err
	}
	if state.Attachment != attachment {
		return Identity{}, errors.New("home volume is not attached to this sandbox")
	}
	return identity, nil
}

// Detach releases a sandbox's attachment after its runtime has stopped. Passing
// an empty attachment makes a retry after a successful detach idempotent.
func Detach(base, volumeUID, node, attachment string) error {
	if strings.TrimSpace(node) == "" || (attachment != "" && !validOwner(attachment)) {
		return errors.New("node and a valid attachment are required")
	}
	unlock, err := lockOwner(base, volumeUID, false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := verify(base, volumeUID, node, VolumeSpecHash, ""); err != nil {
		return err
	}
	_, dir, data, _ := paths(base, volumeUID)
	statePath := filepath.Join(dir, stateName)
	state, err := readState(statePath)
	if err != nil {
		return err
	}
	if state.Attachment == "" {
		if state.Phase == "new" && state.Pod == "" && state.Snapshot == "" && state.ConsumedSnapshot == "" {
			return nil
		}
		return errors.New("home volume is not attached to this sandbox")
	}
	if state.Attachment != attachment || attachment == "" {
		return errors.New("home volume is attached to another sandbox")
	}
	f, err := os.Open(data)
	if err != nil {
		return err
	}
	err = syncFS(int(f.Fd()))
	closeErr := f.Close()
	if err != nil {
		return fmt.Errorf("sync home filesystem: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}
	state.Phase, state.Pod, state.Attachment = "new", "", ""
	state.Snapshot, state.ConsumedSnapshot = "", ""
	return writeJSONAtomic(statePath, state, 0600)
}

// RemoveDetached deletes an independently managed volume only after its
// attachment and execution state have been safely cleared.
func RemoveDetached(base, volumeUID, node string) error {
	if strings.TrimSpace(node) == "" {
		return errors.New("node is required")
	}
	homes, dir, _, err := paths(base, volumeUID)
	if err != nil {
		return err
	}
	if err := ensurePrivateDir(homes, false); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	unlock, err := lockOwner(base, volumeUID, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unlock()
	if err := checkPathNoSymlink(base, dir); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err := verify(base, volumeUID, node, VolumeSpecHash, ""); err != nil {
		return err
	}
	state, err := readState(filepath.Join(dir, stateName))
	if err != nil {
		return err
	}
	if state.Attachment != "" || state.Phase != "new" || state.Pod != "" || state.Snapshot != "" || state.ConsumedSnapshot != "" {
		return errors.New("refusing to remove an attached or occupied home volume")
	}
	return os.RemoveAll(dir)
}

// PromoteLegacy converts a legacy per-sandbox HOME into an independent HOME
// volume in place. The caller must first ensure the legacy runtime is stopped
// and cleaned up. A missing legacy HOME is initialized as an empty volume.
func PromoteLegacy(base, owner, node, spec string) error {
	if strings.TrimSpace(node) == "" {
		return errors.New("node is required")
	}
	homes, dir, data, err := paths(base, owner)
	if err != nil {
		return err
	}
	if err := ensurePrivateDir(homes, false); errors.Is(err, fs.ErrNotExist) {
		_, err := Prepare(base, owner, node, VolumeSpecHash, true)
		return err
	} else if err != nil {
		return err
	}
	// Older layouts may have the data directory but no lock directory. Create
	// the lock directory before taking the owner lock so promotion is atomic
	// with respect to all normal HOME operations.
	if err := ensureLockDir(base, true); err != nil {
		return err
	}
	unlock, err := lockOwner(base, owner, true)
	if err != nil {
		return err
	}
	defer unlock()
	if err := checkPathNoSymlink(base, dir); errors.Is(err, fs.ErrNotExist) {
		if err := ensureOwnerDir(dir); err != nil {
			return err
		}
		_, err := initializeHome(dir, data, owner, node, VolumeSpecHash)
		return err
	} else if err != nil {
		return err
	}
	markerPath := filepath.Join(dir, identityName)
	if _, markerErr := os.Lstat(markerPath); errors.Is(markerErr, fs.ErrNotExist) {
		if spec != "" {
			return errors.New("legacy spec is required to identify an existing HOME")
		}
		// With no legacy identity, initializeHome accepts only a genuinely empty
		// unknown directory (or a matching interrupted initialization). It
		// refuses non-empty data whose ownership cannot be established.
		_, err := initializeHome(dir, data, owner, node, VolumeSpecHash)
		return err
	} else if markerErr != nil {
		return markerErr
	}
	if spec == "" {
		// Empty legacy spec cannot identify a legacy marker. A marker already
		// promoted to the independent-volume format is safe to retry.
		if _, err := verify(base, owner, node, VolumeSpecHash, ""); err == nil {
			return nil
		}
		return errors.New("legacy spec is required to promote an existing HOME")
	}
	identity, err := verify(base, owner, node, spec, "")
	if err != nil {
		// A successful conversion is an idempotent retry. It deliberately
		// leaves any later attachment or execution state untouched.
		if _, promotedErr := verify(base, owner, node, VolumeSpecHash, ""); promotedErr == nil {
			return nil
		}
		return err
	}
	statePath := filepath.Join(dir, stateName)
	state, err := readState(statePath)
	if err != nil {
		return err
	}
	f, err := os.Open(data)
	if err != nil {
		return err
	}
	err = syncFS(int(f.Fd()))
	closeErr := f.Close()
	if err != nil {
		return fmt.Errorf("sync home filesystem: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}
	state.Phase, state.Pod, state.Attachment = "new", "", ""
	state.Snapshot, state.ConsumedSnapshot = "", ""
	// Commit the reset before changing the identity marker. If the marker write
	// fails, retry still recognizes the legacy identity and repeats this reset;
	// the reverse order could leave a promoted identity with stale active state.
	if err := writeJSONAtomic(statePath, state, 0600); err != nil {
		return err
	}
	identity.SpecHash = VolumeSpecHash
	return writeJSONAtomic(filepath.Join(dir, identityName), identity, 0600)
}

type initializingIdentity struct {
	ID       string `json:"id"`
	OwnerUID string `json:"ownerUID"`
	NodeName string `json:"nodeName"`
	SpecHash string `json:"specHash"`
}

// Path returns the data directory for an owner after validating the owner key.
func Path(base, owner string) (string, error) {
	if !validOwner(owner) {
		return "", fmt.Errorf("invalid home owner %q", owner)
	}
	if strings.TrimSpace(base) == "" {
		return "", errors.New("home base is empty")
	}
	if !filepath.IsAbs(filepath.Clean(base)) {
		return "", errors.New("home base must be an absolute path")
	}
	return filepath.Join(base, "homes", owner, "data"), nil
}

func validOwner(owner string) bool { return ownerPattern.MatchString(owner) }

func paths(base, owner string) (homes, dir, data string, err error) {
	data, err = Path(base, owner)
	if err != nil {
		return
	}
	dir = filepath.Dir(data)
	homes = filepath.Dir(dir)
	return
}

// Prepare creates a home only when initialize is true. Existing homes are
// returned only after their immutable identity and backing inode are verified.
func Prepare(base, owner, node, spec string, initialize bool) (Identity, error) {
	if initialize && (strings.TrimSpace(node) == "" || strings.TrimSpace(spec) == "") {
		return Identity{}, errors.New("node and spec are required to initialize a home")
	}
	homes, dir, data, err := paths(base, owner)
	if err != nil {
		return Identity{}, err
	}
	if initialize {
		if err := mkdirPrivatePath(base); err != nil {
			return Identity{}, err
		}
		if err := ensurePrivateDir(homes, true); err != nil {
			return Identity{}, err
		}
		if err := ensureLockDir(base, true); err != nil {
			return Identity{}, err
		}
	} else if err := ensurePrivateDir(homes, false); err != nil {
		return Identity{}, err
	}
	unlock, err := lockOwner(base, owner, initialize)
	if err != nil {
		return Identity{}, err
	}
	defer unlock()

	if initialize {
		if err := ensureOwnerDir(dir); err != nil {
			return Identity{}, err
		}
		return initializeHome(dir, data, owner, node, spec)
	}
	return verify(base, owner, node, spec, "")
}

// Verify validates the marker and data inode without modifying the filesystem.
// Empty node or spec values skip that corresponding comparison.
func Verify(base, owner, node, spec, id string) (Identity, error) {
	return verify(base, owner, node, spec, id)
}

// VerifyExecution checks the parent sandbox's durable claim before a child
// Gofer may open HOME. A valid directory alone does not authorize execution.
func VerifyExecution(base, owner, spec, id, pod string) error {
	if id == "" || pod == "" {
		return errors.New("home and execution identities are required")
	}
	unlock, err := lockOwner(base, owner, false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := Verify(base, owner, "", spec, id); err != nil {
		return err
	}
	_, dir, _, _ := paths(base, owner)
	state, err := readState(filepath.Join(dir, stateName))
	if err != nil {
		return err
	}
	if state.Phase != "active" || state.Pod != pod {
		return errors.New("home is not claimed by this execution Pod")
	}
	return nil
}

func verify(base, owner, node, spec, id string) (Identity, error) {
	_, dir, data, err := paths(base, owner)
	if err != nil {
		return Identity{}, err
	}
	if err := checkPathNoSymlink(base, filepath.Join(dir, identityName)); err != nil {
		return Identity{}, err
	}
	identity, err := readIdentity(filepath.Join(dir, identityName))
	if err != nil {
		return Identity{}, err
	}
	if identity.OwnerUID != owner || (node != "" && identity.NodeName != node) || (spec != "" && identity.SpecHash != spec) || (id != "" && identity.ID != id) {
		return Identity{}, errors.New("home identity does not match requested owner, node, spec, or id")
	}
	if err := verifyData(data, identity); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

// Checkpoint flushes the home filesystem and records a suspended snapshot for
// the currently active execution Pod.
func Checkpoint(base, owner, id, pod, snapshot string) error {
	if pod == "" || snapshot == "" {
		return errors.New("pod and snapshot are required")
	}
	if _, err := Verify(base, owner, "", "", id); err != nil {
		return err
	}
	unlock, err := lockOwner(base, owner, false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := Verify(base, owner, "", "", id); err != nil {
		return err
	}
	_, dir, data, _ := paths(base, owner)
	state, err := readState(filepath.Join(dir, stateName))
	if err != nil {
		return err
	}
	if state.Phase != "active" || state.Pod != pod {
		return errors.New("home execution pod is not active")
	}
	if snapshot == state.ConsumedSnapshot {
		return errors.New("snapshot was already consumed")
	}
	f, err := os.Open(data)
	if err != nil {
		return err
	}
	err = syncFS(int(f.Fd()))
	closeErr := f.Close()
	if err != nil {
		return fmt.Errorf("sync home filesystem: %w", err)
	}
	if closeErr != nil {
		return closeErr
	}
	state.Phase, state.Pod, state.Snapshot = "suspended", "", snapshot
	return writeJSONAtomic(filepath.Join(dir, stateName), state, 0600)
}

// ValidateCheckpoint confirms that the home is suspended at the given snapshot.
func ValidateCheckpoint(base, owner, id, snapshot string) error {
	if _, err := Verify(base, owner, "", "", id); err != nil {
		return err
	}
	_, dir, _, _ := paths(base, owner)
	state, err := readState(filepath.Join(dir, stateName))
	if err != nil {
		return err
	}
	if snapshot == "" || state.Phase != "suspended" || state.Snapshot != snapshot {
		return errors.New("home checkpoint does not match")
	}
	return nil
}

// ClaimExecution atomically claims a new or suspended home for a single Pod.
func ClaimExecution(base, owner, id, pod, snapshot string) error {
	if pod == "" {
		return errors.New("pod is required")
	}
	unlock, err := lockOwner(base, owner, false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := Verify(base, owner, "", "", id); err != nil {
		return err
	}
	return claimExecutionLocked(base, owner, id, pod, snapshot, "", false)
}

// ClaimAttachedExecution validates the independent volume's current sandbox
// attachment and claims its execution under one owner lock. This prevents a
// stale adapter from claiming a volume after it was detached and reattached.
func ClaimAttachedExecution(base, volumeUID, node, attachment, id, pod, snapshot string) error {
	if !validOwner(attachment) || strings.TrimSpace(node) == "" || id == "" || pod == "" {
		return errors.New("node, attachment, home, and pod are required")
	}
	unlock, err := lockOwner(base, volumeUID, false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := verify(base, volumeUID, node, VolumeSpecHash, id); err != nil {
		return err
	}
	return claimExecutionLocked(base, volumeUID, id, pod, snapshot, attachment, true)
}

// claimExecutionLocked requires the caller to hold lockOwner for owner.
func claimExecutionLocked(base, owner, id, pod, snapshot, attachment string, requireAttachment bool) error {
	_, dir, _, _ := paths(base, owner)
	state, err := readState(filepath.Join(dir, stateName))
	if err != nil {
		return err
	}
	if requireAttachment && state.Attachment != attachment {
		return errors.New("home volume is not attached to this sandbox")
	}
	if snapshot == "" {
		if state.Phase != "new" || state.Snapshot != "" {
			return errors.New("home cannot be claimed as a new execution")
		}
	} else if state.Phase != "suspended" || state.Snapshot != snapshot || state.ConsumedSnapshot == snapshot {
		return errors.New("home checkpoint cannot be resumed")
	}
	state.Phase, state.Pod = "active", pod
	if snapshot != "" {
		state.ConsumedSnapshot = snapshot
		state.Snapshot = ""
	}
	return writeJSONAtomic(filepath.Join(dir, stateName), state, 0600)
}

// Remove deletes only the validated owner's directory. Missing homes are
// already removed; recursive deletion does not follow symlinks inside data.
func Remove(base, owner, node, spec string) error {
	homes, dir, _, err := paths(base, owner)
	if err != nil {
		return err
	}
	if err := ensurePrivateDir(homes, false); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	unlock, err := lockOwner(base, owner, false)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unlock()
	if err := checkPathNoSymlink(base, dir); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	identity, err := Verify(base, owner, node, spec, "")
	if err != nil {
		return err
	}
	if identity.OwnerUID != owner {
		return errors.New("refusing to remove a home owned by another identity")
	}
	return os.RemoveAll(dir)
}

var syncFS = func(fd int) error { return unix.Syncfs(fd) }

func initializeHome(dir, data, owner, node, spec string) (Identity, error) {
	marker := filepath.Join(dir, identityName)
	initMarker := filepath.Join(dir, initName)
	if _, err := os.Lstat(marker); err == nil {
		identity, err := readIdentity(marker)
		if err != nil {
			return Identity{}, err
		}
		if identity.OwnerUID != owner || identity.NodeName != node || identity.SpecHash != spec {
			return Identity{}, errors.New("home identity does not match requested owner, node, or spec")
		}
		if err := verifyData(data, identity); err != nil {
			return Identity{}, err
		}
		_, stateErr := readState(filepath.Join(dir, stateName))
		if errors.Is(stateErr, fs.ErrNotExist) {
			partial, initErr := readInitializing(initMarker)
			if initErr != nil || !matchesInitializing(partial, identity) {
				return Identity{}, errors.New("missing home state without a matching interrupted initialization")
			}
			entries, readErr := os.ReadDir(data)
			if readErr != nil {
				return Identity{}, readErr
			}
			if len(entries) != 0 {
				return Identity{}, errors.New("refusing to reset state for a non-empty home")
			}
			if err := writeJSONAtomic(filepath.Join(dir, stateName), executionState{Phase: "new"}, 0600); err != nil {
				return Identity{}, err
			}
			return identity, removeInitMarker(initMarker)
		}
		if stateErr != nil {
			return Identity{}, stateErr
		}
		if err := removeMatchingInitMarker(initMarker, identity); err != nil {
			return Identity{}, err
		}
		return identity, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return Identity{}, err
	}

	st, err := os.Lstat(dir)
	if err != nil {
		return Identity{}, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return Identity{}, errors.New("home owner path is not a real directory")
	}
	init, err := readInitializing(initMarker)
	if errors.Is(err, fs.ErrNotExist) {
		entries, readErr := os.ReadDir(dir)
		if readErr != nil {
			return Identity{}, readErr
		}
		if len(entries) != 0 {
			return Identity{}, errors.New("refusing to initialize a non-empty unknown home directory")
		}
		id, idErr := randomID()
		if idErr != nil {
			return Identity{}, idErr
		}
		init = initializingIdentity{ID: id, OwnerUID: owner, NodeName: node, SpecHash: spec}
		if err := writeJSONAtomic(initMarker, init, 0600); err != nil {
			return Identity{}, err
		}
	} else if err != nil {
		return Identity{}, err
	} else if init.OwnerUID != owner || init.NodeName != node || init.SpecHash != spec || init.ID == "" {
		return Identity{}, errors.New("partial home initialization identity does not match")
	}

	if err := ensurePrivateDir(data, true); err != nil {
		return Identity{}, err
	}
	entries, err := os.ReadDir(data)
	if err != nil {
		return Identity{}, err
	}
	if len(entries) != 0 {
		return Identity{}, errors.New("refusing to adopt non-empty data directory during partial initialization")
	}
	identity, err := identityForData(data, init.ID, owner, node, spec)
	if err != nil {
		return Identity{}, err
	}
	if err := writeJSONAtomic(marker, identity, 0600); err != nil {
		return Identity{}, err
	}
	if err := writeJSONAtomic(filepath.Join(dir, stateName), executionState{Phase: "new"}, 0600); err != nil {
		return Identity{}, err
	}
	if err := os.Remove(initMarker); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return Identity{}, err
	}
	return identity, nil
}

func matchesInitializing(partial initializingIdentity, identity Identity) bool {
	return partial.ID == identity.ID && partial.OwnerUID == identity.OwnerUID && partial.NodeName == identity.NodeName && partial.SpecHash == identity.SpecHash
}

func removeMatchingInitMarker(path string, identity Identity) error {
	partial, err := readInitializing(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !matchesInitializing(partial, identity) {
		return errors.New("initializing marker does not match home identity")
	}
	return removeInitMarker(path)
}

func removeInitMarker(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func identityForData(data, id, owner, node, spec string) (Identity, error) {
	fi, err := os.Lstat(data)
	if err != nil {
		return Identity{}, err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return Identity{}, errors.New("home data is not a real directory")
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return Identity{}, errors.New("home data inode information is unavailable")
	}
	return Identity{ID: id, OwnerUID: owner, NodeName: node, SpecHash: spec, Device: uint64(st.Dev), Inode: st.Ino}, nil
}

func verifyData(data string, identity Identity) error {
	current, err := identityForData(data, identity.ID, identity.OwnerUID, identity.NodeName, identity.SpecHash)
	if err != nil {
		return err
	}
	if current.Device != identity.Device || current.Inode != identity.Inode {
		return errors.New("home data inode does not match its identity")
	}
	return nil
}

func readIdentity(path string) (Identity, error) {
	var identity Identity
	if err := readPrivateJSON(path, &identity); err != nil {
		return Identity{}, err
	}
	if identity.ID == "" || identity.OwnerUID == "" || identity.NodeName == "" || identity.SpecHash == "" || identity.Device == 0 || identity.Inode == 0 {
		return Identity{}, errors.New("home identity marker is incomplete")
	}
	return identity, nil
}

func readState(path string) (executionState, error) {
	var state executionState
	if err := readPrivateJSON(path, &state); err != nil {
		return state, err
	}
	if state.Attachment != "" && !validOwner(state.Attachment) {
		return state, errors.New("invalid home attachment identity")
	}
	switch state.Phase {
	case "new":
		if state.Pod != "" || state.Snapshot != "" {
			return state, errors.New("invalid new home state")
		}
	case "active":
		if state.Pod == "" || state.Snapshot != "" {
			return state, errors.New("invalid active home state")
		}
	case "suspended":
		if state.Pod != "" || state.Snapshot == "" {
			return state, errors.New("invalid suspended home state")
		}
	default:
		return state, errors.New("invalid home execution state")
	}
	return state, nil
}

func readInitializing(path string) (initializingIdentity, error) {
	var identity initializingIdentity
	if err := readPrivateJSON(path, &identity); err != nil {
		return identity, err
	}
	if identity.ID == "" {
		return identity, errors.New("partial home identity is incomplete")
	}
	return identity, nil
}

func readPrivateJSON(path string, dst any) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Mode().Perm()&0077 != 0 {
		return errors.New("home metadata is not a private regular file")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("decode home metadata: %w", err)
	}
	return nil
}

func writeJSONAtomic(path string, value any, mode fs.FileMode) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".home-tmp-")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(append(b, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	closeErr := d.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func mkdirPrivatePath(path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return errors.New("home base must be an absolute path")
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(clean, current), current) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		fi, err := os.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			if err := os.Mkdir(current, 0700); err != nil && !errors.Is(err, fs.ErrExist) {
				return err
			}
			fi, err = os.Lstat(current)
		}
		if err != nil {
			return err
		}
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("home base path component %q is not a real directory", current)
		}
	}
	return nil
}

func ensurePrivateDir(path string, create bool) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) && create {
		if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		fi, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a real directory", path)
	}
	if fi.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%q is not private", path)
	}
	return nil
}

func ensureOwnerDir(path string) error {
	if err := ensurePrivateDir(path, true); err != nil {
		return err
	}
	return nil
}

func ensureLockDir(base string, create bool) error {
	path := filepath.Join(base, "homes", lockDir)
	return ensurePrivateDir(path, create)
}

func lockOwner(base, owner string, create bool) (func(), error) {
	if !validOwner(owner) {
		return nil, fmt.Errorf("invalid home owner %q", owner)
	}
	if err := ensureLockDir(base, create); err != nil {
		return nil, err
	}
	path := filepath.Join(base, "homes", lockDir, owner+".lock")
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func checkPathNoSymlink(base, target string) error {
	base = filepath.Clean(base)
	target = filepath.Clean(target)
	if !filepath.IsAbs(base) || !filepath.IsAbs(target) {
		return errors.New("home paths must be absolute")
	}
	rel, err := filepath.Rel(base, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("home path escapes configured base")
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(target, current), current) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		fi, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in home path at %q", current)
		}
	}
	return nil
}
