// SPDX-License-Identifier: Apache-2.0

package node

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"context"
	"google.golang.org/grpc"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type imageClient struct {
	cri.ImageServiceClient
	id string
}

func (i imageClient) ImageStatus(context.Context, *cri.ImageStatusRequest, ...grpc.CallOption) (*cri.ImageStatusResponse, error) {
	return &cri.ImageStatusResponse{Image: &cri.Image{Id: i.id}}, nil
}
func TestImageReplacementCannotAuthorizeRestore(t *testing.T) {
	base := t.TempDir()
	binary := filepath.Join(base, "runsc")
	os.WriteFile(binary, []byte("runtime"), 0700)
	hash, _ := Digest(binary)
	path, _ := SnapshotPath(base, "owner", "snapshot")
	os.MkdirAll(path, 0700)
	os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("memory"), 0600)
	if err := Seal(path, Manifest{OwnerUID: "owner", SpecHash: "spec", ImageID: "original", RunscHash: hash}); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: binary, Images: imageClient{id: "replacement"}}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{Container: core.Container{Image: "image:mutable"}}, Status: api.Status{SpecHash: "spec", Snapshot: "snapshot"}}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod", Namespace: "test", UID: "pod-uid"}}
	if err := b.Prepare(context.Background(), w, p); err == nil || !strings.Contains(err.Error(), "image changed") {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "tickets", "pod-uid.json")); !os.IsNotExist(err) {
		t.Fatal("authorization created for changed image")
	}
	b.Images = imageClient{id: "original"}
	os.WriteFile(binary, []byte("changed-runtime"), 0700)
	if err := b.Prepare(context.Background(), w, p); err == nil || !strings.Contains(err.Error(), "runsc binary changed") {
		t.Fatal(err)
	}
}

func TestRunningImageMatchesLocalImage(t *testing.T) {
	image := &cri.Image{
		Id:          "sha256:config",
		RepoDigests: []string{"docker.io/example/sandbox@sha256:manifest"},
	}
	for _, actual := range []string{
		"containerd://sha256:config",
		"docker.io/example/sandbox@sha256:manifest",
		"containerd://docker.io/example/sandbox@sha256:manifest",
	} {
		if !runningImageMatches(actual, image) {
			t.Errorf("valid running image %q was rejected", actual)
		}
	}
	for _, actual := range []string{
		"sha256:other-config",
		"docker.io/example/sandbox@sha256:other-manifest",
		"docker.io/other/sandbox@sha256:manifest",
	} {
		if runningImageMatches(actual, image) {
			t.Errorf("unrelated running image %q was accepted", actual)
		}
	}
}
