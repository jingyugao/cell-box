package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
)

// Exercise real disk identities behind the HTTP service, while the
// process provider remains a deterministic substitute for Kubernetes/runsc.
type diskCoreProvider struct {
	*fakeCoreProvider
	base string
}

func (p *diskCoreProvider) CreateVolume(_ context.Context, spec boxprovider.VolumeSpec) error {
	return (&node.Backend{Base: p.base}).CreateVolume(spec.UID, spec.NodeName)
}
func (p *diskCoreProvider) DeleteVolume(_ context.Context, spec boxprovider.VolumeSpec) error {
	return (&node.Backend{Base: p.base}).DeleteVolume(spec.UID, spec.NodeName)
}
func (p *diskCoreProvider) Create(ctx context.Context, spec boxprovider.Spec) (boxprovider.Handle, error) {
	h, err := p.fakeCoreProvider.Create(ctx, spec)
	if err != nil {
		return h, err
	}
	seen := make(map[string]bool)
	for _, mount := range spec.Mounts {
		if seen[mount.UID] {
			continue
		}
		seen[mount.UID] = true
		if _, err := homevolume.Verify(p.base, mount.UID, spec.NodeName, homevolume.VolumeSpecHash, ""); err != nil {
			return h, err
		}
	}
	return h, nil
}

func volumeFixture(t *testing.T) (*coreFixture, *diskCoreProvider) {
	t.Helper()
	f := newCoreFixture(t)
	useK3sProfile(t, f)
	p := &diskCoreProvider{fakeCoreProvider: f.provider, base: t.TempDir()}
	f.service.providers["resumable-k8s-pod"] = p
	return f, p
}

func createTestVolume(t *testing.T, f *coreFixture, id string) Volume {
	t.Helper()
	status, body := f.call(t, "POST", "/v1/volumes", testClientToken, "", createVolumeRequest{ID: id, OwnerKey: "owner-a", ProfileID: "profile-a"})
	wantStatus(t, status, 202, body)
	f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/volumes/"+id, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	return decodeResponse[Volume](t, body)
}

func createVolumeBox(t *testing.T, f *coreFixture, input createRequest) Box {
	t.Helper()
	op, err := f.service.create("client-a", input, "", false)
	if err != nil {
		t.Fatal(err)
	}
	f.waitOperation(t, op.ID, "succeeded")
	b, err := f.service.rawBox(op.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	return b.Box
}

func destroyVolumeBox(t *testing.T, f *coreFixture, id string) {
	t.Helper()
	op, err := f.service.action("client-a", id, "destroy")
	if err != nil {
		t.Fatal(err)
	}
	f.waitOperation(t, op.ID, "succeeded")
}

func TestVolumeSurvivesSandboxUpgradeAndExplicitDelete(t *testing.T) {
	f, p := volumeFixture(t)
	v := createTestVolume(t, f, "project-disk")
	template := f.template(t)
	first := createVolumeBox(t, f, createRequest{ID: "box-v1", OwnerKey: v.OwnerKey, TemplateID: template, HomeVolumeID: v.ID})
	record, err := f.service.store.VolumeRecord("client-a", v.ID)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := homevolume.Path(p.base, record.UID)
	file := filepath.Join(root, "project-data")
	if err := os.WriteFile(file, []byte("keep through upgrade"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := homevolume.Verify(p.base, record.UID, "test-node", homevolume.VolumeSpecHash, "")
	if err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "DELETE", "/v1/volumes/"+v.ID, testClientToken, "", nil)
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "GET", "/v1/volumes/"+v.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	destroyVolumeBox(t, f, first.ID)
	status, body = f.call(t, "GET", "/v1/volumes/"+v.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[Volume](t, body); got.Phase != "available" {
		t.Fatalf("destroy changed volume readiness: %+v", got)
	}
	v2 := buildCatalogRevision(t, f, template, "v2", "registry.example.test/upgraded:latest")
	second := createVolumeBox(t, f, createRequest{ID: "box-v2", OwnerKey: v.OwnerKey, RevisionID: v2.ID, HomeVolumeID: v.ID})
	if second.Image == first.Image || second.HomeVolumeID != first.HomeVolumeID {
		t.Fatalf("upgrade did not change image and preserve volume: %+v -> %+v", first, second)
	}
	after, err := homevolume.Verify(p.base, record.UID, "test-node", homevolume.VolumeSpecHash, before.ID)
	if err != nil || after.Inode != before.Inode {
		t.Fatalf("disk identity changed: %+v -> %+v, %v", before, after, err)
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != "keep through upgrade" {
		t.Fatalf("project data lost: %q, %v", data, err)
	}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		old, err := tx.Box("client-a", first.ID)
		if err != nil {
			return err
		}
		return tx.PutBox(old)
	}); err != nil {
		t.Fatal(err)
	}
	if current, err := f.service.store.VolumeRecord("client-a", v.ID); err != nil || current.Volume.Version != record.Volume.Version || current.Volume.Phase != "available" {
		t.Fatalf("sandbox lifecycle changed volume metadata: %+v, %v", current, err)
	}
	destroyVolumeBox(t, f, second.ID)
	status, body = f.call(t, "DELETE", "/v1/volumes/"+v.ID, testClientToken, "", nil)
	wantStatus(t, status, 202, body)
	f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("explicit delete retained disk: %v", err)
	}
	status, body = f.call(t, "POST", "/v1/volumes", testClientToken, "", createVolumeRequest{ID: v.ID, OwnerKey: v.OwnerKey, ProfileID: "profile-a"})
	wantStatus(t, status, 409, body)
}

func TestSharedVolumeKeepsResourceIDConflicts(t *testing.T) {
	f, _ := volumeFixture(t)
	v := createTestVolume(t, f, "shared-disk")
	template := f.template(t)
	input := createRequest{ID: "box-one", TemplateID: template, OwnerKey: v.OwnerKey, HomeVolumeID: v.ID}
	createVolumeBox(t, f, input)
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "", input)
	wantStatus(t, status, 409, body)
	input.ID = "box-two"
	shared := createVolumeBox(t, f, input)
	if shared.HomeVolumeID != v.ID {
		t.Fatalf("second sandbox did not reference the same volume: %+v", shared)
	}
	input.ID = "box-default"
	input.HomeVolumeID = ""
	box := createVolumeBox(t, f, input)
	if box.HomeVolumeID == "" || box.HomeVolumeID == v.ID {
		t.Fatalf("default create did not allocate an independent disk: %+v", box)
	}
	destroyVolumeBox(t, f, box.ID)
	if err := f.service.purgeBox(context.Background(), "client-a", box.ID); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "", input)
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "GET", "/v1/volumes/"+box.HomeVolumeID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
}

func TestConcurrentBoxesCanShareVolume(t *testing.T) {
	f, p := volumeFixture(t)
	v := createTestVolume(t, f, "race-disk")
	template := f.template(t)
	type result struct {
		op  Operation
		err error
	}
	results := make(chan result, 2)
	for _, id := range []string{"box-race-a", "box-race-b"} {
		go func() {
			op, err := f.service.create("client-a", createRequest{ID: id, OwnerKey: v.OwnerKey, TemplateID: template, Mounts: []Mount{{VolumeID: v.ID, MountPath: "/data"}}}, "", false)
			results <- result{op, err}
		}()
	}
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		f.waitOperation(t, r.op.ID, "succeeded")
	}
	record, err := f.service.store.VolumeRecord("client-a", v.ID)
	if err != nil || record.Volume.Phase != "available" || record.Volume.Version != v.Version {
		t.Fatalf("sharing changed volume metadata: %+v, %v", record, err)
	}
	root, _ := homevolume.Path(p.base, record.UID)
	file := filepath.Join(root, "shared-data")
	if err := os.WriteFile(file, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	destroyVolumeBox(t, f, "box-race-a")
	status, body := f.call(t, "DELETE", "/v1/volumes/"+v.ID, testClientToken, "", nil)
	wantStatus(t, status, 409, body)
	if data, err := os.ReadFile(file); err != nil || string(data) != "retained" {
		t.Fatalf("destroying one sandbox changed shared storage: %q, %v", data, err)
	}
	if _, err := homevolume.Verify(p.base, record.UID, "test-node", homevolume.VolumeSpecHash, ""); err != nil {
		t.Fatalf("remaining sandbox's volume became unavailable: %v", err)
	}
	destroyVolumeBox(t, f, "box-race-b")
	status, body = f.call(t, "DELETE", "/v1/volumes/"+v.ID, testClientToken, "", nil)
	wantStatus(t, status, 202, body)
	f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
}

func TestVolumeAdmissionRejectsMissingForeignAndIncompatibleDisks(t *testing.T) {
	f, _ := volumeFixture(t)
	v := createTestVolume(t, f, "owned-disk")
	template := f.template(t)
	for _, tc := range []struct {
		name, volume, owner string
		status              int
	}{
		{"missing", "unknown-disk", v.OwnerKey, 404},
		{"different-owner", v.ID, "owner-b", 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "", createRequest{
				ID: "box-invalid", OwnerKey: tc.owner, TemplateID: template, HomeVolumeID: tc.volume,
			})
			wantStatus(t, status, tc.status, body)
		})
	}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		v, err := tx.Volume("client-a", v.ID)
		if err != nil {
			return err
		}
		v.Agent.UID++
		return tx.PutVolume(v)
	}); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "", createRequest{
		ID: "box-invalid", OwnerKey: v.OwnerKey, TemplateID: template, HomeVolumeID: v.ID,
	})
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "GET", "/v1/volumes", otherClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[[]Volume](t, body); len(got) != 0 {
		t.Fatalf("other client can list disks: %+v", got)
	}
}

func TestLegacySandboxDeletionRegistersRetainedVolume(t *testing.T) {
	f := newCoreFixture(t)
	useK3sProfile(t, f)
	_, box := f.createBox(t, "legacy-box")
	var legacyUID string
	// Reproduce a pre-volume Box record; the node-layer test exercises the
	// physical PromoteLegacy path after its runtime has been stopped.
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		b, err := tx.Box("client-a", box.ID)
		if err != nil {
			return err
		}
		legacyUID = b.Handle.ID
		b.HomeVolumeUID, b.Box.HomeVolumeID = "", ""
		b.VolumeMounts, b.Box.Mounts = nil, nil
		return tx.PutBox(b)
	}); err != nil {
		t.Fatal(err)
	}
	destroyVolumeBox(t, f, box.ID)
	b, err := f.service.rawBox(box.ID)
	if err != nil || b.Box.HomeVolumeID == "" || b.HomeVolumeUID != "" {
		t.Fatalf("legacy layout was changed before termination: %+v, %v", b, err)
	}
	v, err := f.service.store.VolumeRecord("client-a", b.Box.HomeVolumeID)
	if err != nil || v.UID != legacyUID || v.Volume.Phase != "available" {
		t.Fatalf("legacy disk was not registered and retained: %+v, %v", v, err)
	}
}

func TestDeletedObservationPreservesVolumeMetadata(t *testing.T) {
	f, _ := volumeFixture(t)
	v := createTestVolume(t, f, "restart-disk")
	box := createVolumeBox(t, f, createRequest{ID: "box-interrupted-delete", OwnerKey: v.OwnerKey, TemplateID: f.template(t), HomeVolumeID: v.ID})
	// Observing a deleted Box never changes an independent volume's readiness.
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		b, err := tx.Box("client-a", box.ID)
		if err != nil {
			return err
		}
		b.Box.Phase = "deleted"
		return tx.PutBox(b)
	}); err != nil {
		t.Fatal(err)
	}
	got, err := f.service.store.VolumeRecord("client-a", v.ID)
	if err != nil || got.Volume.Phase != "available" || got.Volume.Version != v.Version {
		t.Fatalf("terminal observation changed volume metadata: %+v, %v", got, err)
	}
}

func TestLegacyVolumePromotionCompletesAfterInterruptedDestroy(t *testing.T) {
	f := newCoreFixture(t)
	useK3sProfile(t, f)
	_, box := f.createBox(t, "legacy-interrupted")
	var volumeID string
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		b, err := tx.Box("client-a", box.ID)
		if err != nil {
			return err
		}
		b.HomeVolumeUID, b.Box.HomeVolumeID = "", ""
		b.VolumeMounts, b.Box.Mounts = nil, nil
		if err := f.service.registerLegacyVolume(tx, &b); err != nil {
			return err
		}
		volumeID, b.Box.Phase = b.Box.HomeVolumeID, "deleting"
		return tx.PutBox(b)
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.service.store.InterruptOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := f.service.store.VolumeRecord("client-a", volumeID)
	if err != nil || v.Volume.Phase != "failed" {
		t.Fatalf("interrupted promotion was not surfaced: %+v, %v", v, err)
	}
	// The node may finish promotion while the service is restarting. Its
	// deleted observation makes the retained volume ready for a new sandbox.
	if err := f.service.store.ChangeBox(context.Background(), box.ID, func(b *boxRecord) error {
		b.Box.Phase = "deleted"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	v, err = f.service.store.VolumeRecord("client-a", volumeID)
	if err != nil || v.Volume.Phase != "available" || v.Volume.Error != nil {
		t.Fatalf("completed promotion remained unavailable after restart: %+v, %v", v, err)
	}
}

func TestVolumeRestartPreservesDataAndAllowsInterruptedDeleteCleanup(t *testing.T) {
	f, p := volumeFixture(t)
	v := createTestVolume(t, f, "restart-volume")
	record, err := f.service.store.VolumeRecord("client-a", v.ID)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := homevolume.Path(p.base, record.UID)
	file := filepath.Join(root, "retained")
	if err := os.WriteFile(file, []byte("durable"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		record.Volume.Phase, record.Volume.OperationID = "deleting", "op-interrupted-volume"
		if err := tx.PutVolume(record); err != nil {
			return err
		}
		return tx.PutOperation(operationRecord{ClientID: "client-a", Operation: Operation{
			ID: record.Volume.OperationID, Kind: "volume-delete", TargetID: v.ID, Status: "running", Version: 1,
		}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service, err = New(f.config, map[string]boxprovider.Provider{"resumable-k8s-pod": p})
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.service.store.VolumeRecord("client-a", v.ID)
	if err != nil || current.Volume.Phase != "failed" || current.Volume.OperationID != "" || current.Volume.Error.Code != "UNKNOWN_OUTCOME" {
		t.Fatalf("interrupted operation was not surfaced: %+v, %v", current, err)
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != "durable" {
		t.Fatalf("startup removed data or replayed delete: %q, %v", data, err)
	}
	status, body := f.call(t, "DELETE", "/v1/volumes/"+v.ID, testClientToken, "", nil)
	wantStatus(t, status, 202, body)
	f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
}

func TestPurgeDoesNotDeleteVolumeOperationsWithSamePublicID(t *testing.T) {
	f, _ := volumeFixture(t)
	v := createTestVolume(t, f, "shared-resource-name")
	box := createVolumeBox(t, f, createRequest{ID: v.ID, OwnerKey: v.OwnerKey, TemplateID: f.template(t), HomeVolumeID: v.ID})
	destroyVolumeBox(t, f, box.ID)
	if err := f.service.purgeBox(context.Background(), "client-a", box.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := f.service.store.db.QueryRow(`SELECT count(*) FROM runtime_records WHERE kind='operations' AND json_extract(record,'$.operation.kind')='volume-create'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("purge removed volume history: count=%d, %v", count, err)
	}
}

func TestMultipleMountsSurviveSandboxReplacement(t *testing.T) {
	f, p := volumeFixture(t)
	home := createTestVolume(t, f, "multi-home")
	data := createTestVolume(t, f, "multi-data")
	template := f.template(t)
	mounts := []Mount{{VolumeID: home.ID, MountPath: "/home/agent"}, {VolumeID: data.ID, MountPath: "/data", ReadOnly: true}}
	first := createVolumeBox(t, f, createRequest{ID: "multi-v1", OwnerKey: home.OwnerKey, TemplateID: template, Mounts: mounts})
	if len(first.Mounts) != 2 || first.Mounts[1] != mounts[1] || first.HomeVolumeID != home.ID {
		t.Fatalf("mounts changed during admission: %+v", first)
	}
	for _, volume := range []Volume{home, data} {
		record, err := f.service.store.VolumeRecord("client-a", volume.ID)
		if err != nil {
			t.Fatal(err)
		}
		root, _ := homevolume.Path(p.base, record.UID)
		if err := os.WriteFile(filepath.Join(root, "marker"), []byte(volume.ID), 0600); err != nil {
			t.Fatal(err)
		}
	}
	destroyVolumeBox(t, f, first.ID)
	mounts[1].MountPath, mounts[1].ReadOnly = "/srv/data", false
	v2 := buildCatalogRevision(t, f, template, "multi-v2", "registry.example.test/multi-v2:latest")
	second := createVolumeBox(t, f, createRequest{ID: "multi-v2", OwnerKey: home.OwnerKey, RevisionID: v2.ID, Mounts: mounts})
	if second.Image == first.Image || second.Mounts[1].MountPath != "/srv/data" {
		t.Fatalf("replacement did not apply new image and paths: %+v", second)
	}
	for _, volume := range []Volume{home, data} {
		record, err := f.service.store.VolumeRecord("client-a", volume.ID)
		if err != nil || record.Volume.Phase != "available" || record.Volume.Version != volume.Version {
			t.Fatalf("replacement changed volume metadata: %+v, %v", record, err)
		}
		root, _ := homevolume.Path(p.base, record.UID)
		if content, err := os.ReadFile(filepath.Join(root, "marker")); err != nil || string(content) != volume.ID {
			t.Fatalf("disk data lost: %q, %v", content, err)
		}
	}
	destroyVolumeBox(t, f, second.ID)
}

func TestRepeatedAndNestedMountPathsArePassedThrough(t *testing.T) {
	f, _ := volumeFixture(t)
	first := createTestVolume(t, f, "mount-a")
	second := createTestVolume(t, f, "mount-b")
	mounts := []Mount{
		{VolumeID: first.ID, MountPath: "/data"},
		{VolumeID: second.ID, MountPath: "/data/logs"},
		{VolumeID: first.ID, MountPath: "/data", ReadOnly: true},
	}
	box := createVolumeBox(t, f, createRequest{ID: "mount-paths", OwnerKey: first.OwnerKey, TemplateID: f.template(t), Mounts: mounts})
	if len(box.Mounts) != len(mounts) || box.HomeVolumeID != "" || box.Capabilities.Archives != "none" {
		t.Fatalf("data-only mounts introduced an implicit HOME: %+v", box)
	}
	for i := range mounts {
		if box.Mounts[i] != mounts[i] {
			t.Fatalf("mount[%d] was rewritten: %+v", i, box.Mounts)
		}
	}
	status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[Box](t, body); got.Capabilities.PersistentHome || got.Capabilities.Archives != "none" || len(got.Mounts) != 3 {
		t.Fatalf("GET replaced mount-derived capabilities: %+v", got)
	}
	destroyVolumeBox(t, f, box.ID)
	status, body = f.call(t, "GET", "/v1/volumes", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	volumes := decodeResponse[[]Volume](t, body)
	if len(volumes) != 2 {
		t.Fatalf("destroy created an unexpected legacy HOME: %+v", volumes)
	}
	for _, volume := range volumes {
		if volume.Phase != "available" {
			t.Fatalf("destroy changed volume readiness: %+v", volume)
		}
	}
}

func TestMultiVolumeAdmissionFailureKeepsBoxIDAvailable(t *testing.T) {
	f, _ := volumeFixture(t)
	a := createTestVolume(t, f, "atomic-a")
	b := createTestVolume(t, f, "atomic-b")
	template := f.template(t)
	createVolumeBox(t, f, createRequest{ID: "existing-owner", OwnerKey: b.OwnerKey, TemplateID: template, Mounts: []Mount{{VolumeID: b.ID, MountPath: "/data"}}})
	input := createRequest{ID: "atomic-box", OwnerKey: a.OwnerKey, TemplateID: template, Mounts: []Mount{{VolumeID: a.ID, MountPath: "/data"}, {VolumeID: "missing-volume", MountPath: "/other"}}}
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "", input)
	wantStatus(t, status, 404, body)
	if record, err := f.service.store.VolumeRecord("client-a", a.ID); err != nil || record.Volume.Phase != "available" || record.Volume.Version != a.Version {
		t.Fatalf("failed mount admission changed the first volume: %+v, %v", record, err)
	}
	input.Mounts[1].VolumeID = b.ID
	createVolumeBox(t, f, input)
}

func TestMountFormatAndArchiveCoverage(t *testing.T) {
	f, _ := volumeFixture(t)
	v := createTestVolume(t, f, "format-disk")
	template := f.template(t)
	for _, mountPath := range []string{"relative", "/", "/data/../other", "/data\x01", "/data\n"} {
		status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "", createRequest{ID: "invalid-format", OwnerKey: v.OwnerKey, TemplateID: template, Mounts: []Mount{{VolumeID: v.ID, MountPath: mountPath}}})
		wantStatus(t, status, 400, body)
	}
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "", createRequest{ID: "invalid-format", OwnerKey: v.OwnerKey, TemplateID: template, HomeVolumeID: v.ID, Mounts: []Mount{{VolumeID: v.ID, MountPath: "/data"}}})
	wantStatus(t, status, 400, body)
	box := createVolumeBox(t, f, createRequest{ID: "invalid-format", OwnerKey: v.OwnerKey, TemplateID: template, Mounts: []Mount{{VolumeID: v.ID, MountPath: "/home/agent", ReadOnly: true}}})
	if box.Capabilities.Archives != "none" {
		t.Fatal("read-only HOME advertised workspace capture")
	}
	record, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	home := record.VolumeMounts[0]
	home.ReadOnly = false
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"/data", true},
		{"/home/agent/other", true},
		{"/home/agent/workspace", false},
		{"/home/agent/workspace/logs", false},
		{"/home", false},
	} {
		overlay := api.VolumeMount{UID: "another-volume", MountPath: tc.path}
		record.VolumeMounts = []api.VolumeMount{home, overlay}
		if got := boxCanArchiveWorkspace(record); got != tc.want {
			t.Fatalf("archive coverage for %s = %v, want %v", tc.path, got, tc.want)
		}
	}
}
