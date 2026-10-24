// SPDX-License-Identifier: Apache-2.0

package node

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"context"
	"errors"
	"google.golang.org/grpc"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type restoreImages struct {
	cri.ImageServiceClient
	cached, afterPull  string
	statusErr, pullErr error
	pulls              int
	request            *cri.PullImageRequest
}

func (i *restoreImages) ImageStatus(context.Context, *cri.ImageStatusRequest, ...grpc.CallOption) (*cri.ImageStatusResponse, error) {
	if i.statusErr != nil {
		return nil, i.statusErr
	}
	if i.cached == "" {
		return &cri.ImageStatusResponse{}, nil
	}
	return &cri.ImageStatusResponse{Image: &cri.Image{Id: i.cached}}, nil
}

func (i *restoreImages) PullImage(_ context.Context, request *cri.PullImageRequest, _ ...grpc.CallOption) (*cri.PullImageResponse, error) {
	i.pulls++
	i.request = request
	if i.pullErr != nil {
		return nil, i.pullErr
	}
	i.cached = i.afterPull
	return &cri.PullImageResponse{ImageRef: i.cached}, nil
}

func TestPrepareRestoresEvictedCheckpointImageWithoutReplayingRuntime(t *testing.T) {
	registryErr := errors.New("registry unavailable")
	statusErr := errors.New("image status unavailable")
	for _, tc := range []struct {
		name, cached, afterPull string
		policy                  core.PullPolicy
		failure                 error
		wantError               string
		wantPulls               int
		corrupt                 bool
	}{
		{name: "cached", cached: "original"},
		{name: "evicted", afterPull: "original", wantPulls: 1},
		{name: "different image rejected", afterPull: "replacement", wantPulls: 1, wantError: "image changed since checkpoint"},
		{name: "pull failure returned once", failure: registryErr, wantPulls: 1, wantError: registryErr.Error()},
		{name: "status failure is not a cache miss", failure: statusErr, wantError: statusErr.Error()},
		{name: "never policy respected", policy: core.PullNever, wantError: "image not present locally"},
		{name: "invalid snapshot rejected before pull", corrupt: true, wantError: "checkpoint integrity mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			binary := filepath.Join(base, "runsc")
			if err := os.WriteFile(binary, []byte("runtime"), 0700); err != nil {
				t.Fatal(err)
			}
			path := testCheckpoint(t, base, "owner", "snapshot", "spec", "original", binary)
			if tc.corrupt {
				if err := os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			images := &restoreImages{cached: tc.cached, afterPull: tc.afterPull}
			if tc.failure == statusErr {
				images.statusErr = tc.failure
			} else {
				images.pullErr = tc.failure
			}
			b := &Backend{Base: base, Runsc: binary, Images: images}
			w := testWorkload("owner", "snapshot", "spec", "prepared@sha256:digest")
			w.Spec.Container.ImagePullPolicy = tc.policy
			p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod", Namespace: "test", UID: "pod-uid"}}
			err := b.Prepare(context.Background(), w, p)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Prepare error = %v, want %q", err, tc.wantError)
			}
			if tc.failure != nil && !errors.Is(err, tc.failure) {
				t.Fatalf("original failure was lost: %v", err)
			}
			if images.pulls != tc.wantPulls {
				t.Fatalf("pull calls = %d, want %d", images.pulls, tc.wantPulls)
			}
			if images.request != nil && (images.request.Image.Image != w.Spec.Container.Image || images.request.Image.RuntimeHandler != api.RuntimeClass) {
				t.Fatalf("pulled a different image/runtime: %+v", images.request)
			}
			_, ticketErr := os.Stat(filepath.Join(base, "tickets", "pod-uid.json"))
			if tc.wantError == "" && ticketErr != nil {
				t.Fatalf("verified image was not authorized: %v", ticketErr)
			}
			if tc.wantError != "" && !os.IsNotExist(ticketErr) {
				t.Fatalf("failed image check authorized restore: %v", ticketErr)
			}
			if b.snapshotConsumed("owner", "snapshot") {
				t.Fatal("prepare consumed the checkpoint")
			}
		})
	}
}

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
