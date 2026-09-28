package image

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const baseID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const builtID = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

type fakeBuilder struct {
	tag             string
	key             string
	builds          int
	buildArgs       []string
	baseTags        []string
	removedTags     []string
	cleanupDeadline bool
	buildErr        error
	dockerfile      string
	guest           []byte
	product         []byte
	emptyDir        bool
}

func (f *fakeBuilder) Run(ctx context.Context, args []string) ([]byte, error) {
	if len(args) > 2 && args[0] == "image" && args[1] == "inspect" {
		if args[2] == "base:latest" {
			return json.Marshal([]any{map[string]any{"Id": baseID}})
		}
		if args[2] == f.tag && f.builds > 0 {
			return json.Marshal([]any{map[string]any{"Id": builtID, "Config": map[string]any{"Labels": map[string]string{ManagedLabel: ImageVersion, IdentityLabel: f.key}}}})
		}
		return nil, errors.New("No such image")
	}
	if len(args) == 4 && args[0] == "image" && args[1] == "tag" {
		if args[2] != baseID {
			return nil, errors.New("tagged mutable or wrong base")
		}
		f.baseTags = append(f.baseTags, args[3])
		return nil, nil
	}
	if len(args) == 3 && args[0] == "image" && args[1] == "rm" {
		f.removedTags = append(f.removedTags, args[2])
		_, f.cleanupDeadline = ctx.Deadline()
		return nil, nil
	}
	if args[0] == "build" {
		f.builds++
		f.buildArgs = append([]string(nil), args...)
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--tag" {
				f.tag = args[i+1]
			}
		}
		if f.buildErr != nil {
			return nil, f.buildErr
		}
		dir := args[len(args)-1]
		b, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
		if err != nil {
			return nil, err
		}
		f.dockerfile = string(b)
		b, err = os.ReadFile(filepath.Join(dir, "guest"))
		if err != nil {
			return nil, err
		}
		f.guest = b
		if b, err = os.ReadFile(filepath.Join(dir, "product", "app.txt")); err == nil {
			f.product = b
		}
		if info, e := os.Stat(filepath.Join(dir, "product", "empty")); e == nil && info.IsDir() {
			f.emptyDir = true
		}
		fields := strings.Split(f.dockerfile, IdentityLabel+"=\"")
		if len(fields) != 2 {
			return nil, errors.New("missing image key")
		}
		f.key = strings.Split(fields[1], "\"")[0]
		return []byte(builtID), nil
	}
	return nil, errors.New("unexpected command")
}
func TestPrepareDeterministicCache(t *testing.T) {
	guest := staticGuest(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("product"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "empty"), 0755); err != nil {
		t.Fatal(err)
	}
	f := &fakeBuilder{}
	b := NewWithRunner(f)
	o := Options{Base: "base:latest", GuestBinary: guest, ProductDir: root, Platform: "linux/amd64"}
	r, err := b.Prepare(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r.ImageID != builtID || r.BaseID != baseID || f.builds != 1 || len(f.baseTags) != 1 || len(f.removedTags) != 1 || f.baseTags[0] != f.removedTags[0] || !strings.HasPrefix(f.baseTags[0], "cellbox-local-base:tmp-") || !strings.Contains(f.dockerfile, "FROM "+f.baseTags[0]+"\n") || !containsArg(f.buildArgs, "--pull=false") || !f.cleanupDeadline || string(f.product) != "product" || !f.emptyDir {
		t.Fatalf("bad build: %+v, %q", r, f.dockerfile)
	}
	r2, err := b.Prepare(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r2 != r || f.builds != 1 {
		t.Fatalf("cache miss: %+v %+v", r, r2)
	}
	if err = os.WriteFile(filepath.Join(root, "app.txt"), []byte("changed"), 0644); err != nil {
		t.Fatal(err)
	}
	r3, err := b.Prepare(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Key == r.Key || f.builds != 2 || len(f.baseTags) != 2 || len(f.removedTags) != 2 || f.baseTags[0] == f.baseTags[1] || f.baseTags[1] != f.removedTags[1] {
		t.Fatal("payload change did not change image key")
	}
}
func TestTemporaryBaseTagRemovedAfterBuildFailure(t *testing.T) {
	guest := staticGuest(t)
	f := &fakeBuilder{buildErr: errors.New("build failed")}
	_, err := NewWithRunner(f).Prepare(context.Background(), Options{Base: "base:latest", GuestBinary: guest})
	if err == nil || !strings.Contains(err.Error(), "build failed") {
		t.Fatalf("build failure lost: %v", err)
	}
	if len(f.baseTags) != 1 || len(f.removedTags) != 1 || f.baseTags[0] != f.removedTags[0] || !f.cleanupDeadline {
		t.Fatalf("temporary tag not cleaned: tagged=%v removed=%v bounded=%v", f.baseTags, f.removedTags, f.cleanupDeadline)
	}
}
func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}
func TestRejectSymlinkAndInvalidBase(t *testing.T) {
	guest := staticGuest(t)
	root := t.TempDir()
	if err := os.Symlink(guest, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	b := NewWithRunner(&fakeBuilder{})
	if _, err := b.Prepare(context.Background(), Options{Base: "base:latest", GuestBinary: guest, ProductDir: root}); err == nil {
		t.Fatal("accepted symlink")
	}
	if _, err := b.Prepare(context.Background(), Options{Base: "bad\nRUN evil", GuestBinary: guest}); err == nil {
		t.Fatal("accepted unsafe base")
	}
}

func staticGuest(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	binary := filepath.Join(dir, "guest")
	if err := os.WriteFile(src, []byte("package main\nfunc main() {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "-o", binary, src)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build static guest: %v: %s", err, out)
	}
	return binary
}
