package adapter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
)

func homeChildFixture(t *testing.T) (Adapter, node.Ticket, string, string, string, map[string]string) {
	t.Helper()
	a := Adapter{Base: t.TempDir()}
	h, err := homevolume.Prepare(a.Base, "owner", "node", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	ticket := node.Ticket{OwnerUID: "owner", PodUID: "pod", PodName: "name", Namespace: "test", SpecHash: "spec", HomeID: h.ID}
	if err := homevolume.ClaimExecution(a.Base, "owner", h.ID, "pod", ""); err != nil {
		t.Fatal(err)
	}
	sid, id := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for _, path := range []string{filepath.Join(a.Base, "tickets", "pod.json"), filepath.Join(a.Base, "requests", sid+".json")} {
		if err := node.AtomicJSON(path, ticket); err != nil {
			t.Fatal(err)
		}
	}
	bundle := filepath.Join(a.Base, "bundle")
	if err := node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{"x-extension": map[string]any{"keep": true}, "mounts": []any{map[string]any{"destination": "/proc", "type": "proc", "source": "proc"}}}); err != nil {
		t.Fatal(err)
	}
	annotations := map[string]string{"io.kubernetes.cri.sandbox-id": sid, "io.kubernetes.cri.sandbox-uid": "pod", "io.kubernetes.cri.sandbox-name": "name", "io.kubernetes.cri.sandbox-namespace": "test"}
	return a, ticket, sid, id, bundle, annotations
}

func TestHomeChildBindingAndSingleExecution(t *testing.T) {
	a, ticket, _, id, bundle, annotations := homeChildFixture(t)
	if err := a.childCreate(id, annotations, bundle); err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Extension map[string]bool `json:"x-extension"`
		Mounts    []struct {
			Destination, Source, Type string
			Options                   []string
		}
	}
	path := filepath.Join(bundle, "config.json")
	if err := node.ReadJSON(path, &spec); err != nil {
		t.Fatal(err)
	}
	source, _ := homevolume.Path(a.Base, ticket.OwnerUID)
	if !spec.Extension["keep"] || len(spec.Mounts) != 2 || spec.Mounts[1].Source != source || spec.Mounts[1].Destination != homevolume.MountPath {
		t.Fatalf("incorrect OCI binding: %+v", spec)
	}
	before, _ := os.ReadFile(path)
	if err := a.childCreate(id, annotations, bundle); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("valid existing cold mount changed")
	}
	if err := a.childCreate(strings.Repeat("c", 64), annotations, bundle); err == nil {
		t.Fatal("second writable HOME child accepted")
	}
	if err := a.childStart(id); err != nil {
		t.Fatal(err)
	}
	if err := a.childStart(id); err == nil {
		t.Fatal("duplicate child start accepted")
	}
}

func TestHomeChildRejectsUnsafeMounts(t *testing.T) {
	for _, kind := range []string{"other-owner", "read-only", "parent", "descendant", "duplicate", "idmap", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			a, ticket, _, id, bundle, annotations := homeChildFixture(t)
			source, _ := homevolume.Path(a.Base, ticket.OwnerUID)
			mount := map[string]any{"destination": homevolume.MountPath, "source": source, "type": "bind", "options": []string{"rbind", "rprivate", "rw"}}
			switch kind {
			case "other-owner":
				mount["source"] = filepath.Join(a.Base, "homes", "other", "data")
			case "read-only":
				mount["options"] = []string{"rbind", "rprivate", "ro"}
			case "parent":
				mount["destination"] = "/home"
			case "descendant":
				mount["destination"] = "/home/agent/hidden"
			case "idmap":
				mount["uidMappings"] = []any{map[string]int{"containerID": 0, "hostID": 1, "size": 1}}
			}
			mounts := []any{mount}
			if kind == "duplicate" {
				mounts = append(mounts, mount)
			}
			path := filepath.Join(bundle, "config.json")
			if err := node.AtomicJSON(path, map[string]any{"mounts": mounts}); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				if err := os.Rename(path, path+".real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".real", path); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.childCreate(id, annotations, bundle); err == nil {
				t.Fatal("unsafe mount accepted")
			}
			if _, err := os.Stat(filepath.Join(a.Base, "children", id+".json")); !os.IsNotExist(err) {
				t.Fatal("rejected child was authorized")
			}
		})
	}
}

func TestHomeChildRechecksAuthorizationAndIdentityAtStart(t *testing.T) {
	for _, stage := range []string{"create", "start"} {
		for _, kind := range []string{"revoked", "inactive", "wrong-pod", "replaced", "symlink"} {
			t.Run(stage+"/"+kind, func(t *testing.T) {
				a, ticket, _, id, bundle, annotations := homeChildFixture(t)
				if stage == "start" {
					if err := a.childCreate(id, annotations, bundle); err != nil {
						t.Fatal(err)
					}
				}
				home, _ := homevolume.Path(a.Base, ticket.OwnerUID)
				switch kind {
				case "revoked":
					if err := os.Remove(filepath.Join(a.Base, "tickets", "pod.json")); err != nil {
						t.Fatal(err)
					}
				case "inactive", "wrong-pod":
					if err := homevolume.Checkpoint(a.Base, ticket.OwnerUID, ticket.HomeID, ticket.PodUID, "snap"); err != nil {
						t.Fatal(err)
					}
					if kind == "wrong-pod" {
						if err := homevolume.ClaimExecution(a.Base, ticket.OwnerUID, ticket.HomeID, "other-pod", "snap"); err != nil {
							t.Fatal(err)
						}
					}
				case "replaced", "symlink":
					if err := os.Rename(home, home+".old"); err != nil {
						t.Fatal(err)
					}
					var err error
					if kind == "symlink" {
						err = os.Symlink(home+".old", home)
					} else {
						err = os.Mkdir(home, 0700)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				var err error
				if stage == "start" {
					err = a.childStart(id)
				} else {
					err = a.childCreate(id, annotations, bundle)
				}
				if err == nil {
					t.Fatal("invalid HOME execution accepted")
				}
			})
		}
	}
}
