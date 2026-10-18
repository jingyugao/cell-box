package node

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
)

func TestVolumeCreationAssignsGuestOwnership(t *testing.T) {
	b := &Backend{Base: t.TempDir()}
	agent := guestapi.Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
	if os.Geteuid() == 0 {
		agent = guestapi.Identity{UID: 11000, GID: 11000}
	}
	if err := b.CreateVolume("data-volume", "node", agent); err != nil {
		t.Fatal(err)
	}
	path, _ := homevolume.Path(b.Base, "data-volume")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	owner := info.Sys().(*syscall.Stat_t)
	if owner.Uid != agent.UID || owner.Gid != agent.GID || info.Mode().Perm() != 0700 {
		t.Fatalf("volume root has uid=%d gid=%d mode=%o; want guest %d:%d mode 0700", owner.Uid, owner.Gid, info.Mode().Perm(), agent.UID, agent.GID)
	}
	if _, err := homevolume.Verify(b.Base, "data-volume", "node", homevolume.VolumeSpecHash, ""); err != nil {
		t.Fatalf("assigning ownership changed the volume identity: %v", err)
	}
	metadata, err := os.Stat(filepath.Dir(path))
	if err != nil || metadata.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		t.Fatalf("private volume metadata ownership changed: %v", err)
	}
	if os.Geteuid() != 0 {
		// An unprivileged publisher must fail creation instead of exposing a
		// ready volume that the configured guest identity cannot use.
		other := guestapi.Identity{UID: uint32(os.Getuid()) + 1, GID: uint32(os.Getgid())}
		if err := b.CreateVolume("other-volume", "node", other); err == nil {
			t.Fatal("creation ignored an ownership initialization failure")
		}
	}
}
