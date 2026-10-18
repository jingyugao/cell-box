package service

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
)

// Mount selects an independent volume and its destination inside a Box.
type Mount struct {
	VolumeID  string `json:"volumeId"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// Volume is a node-local filesystem with a lifetime independent of a Box.
type Volume struct {
	ID          string    `json:"id"`
	OwnerKey    string    `json:"ownerKey"`
	ProfileID   string    `json:"profileId"`
	NodeName    string    `json:"nodeName"`
	Phase       string    `json:"phase"`
	Version     uint64    `json:"resourceVersion"`
	CreatedAt   time.Time `json:"createdAt"`
	OperationID string    `json:"operationId,omitempty"`
	Error       *APIError `json:"error,omitempty"`
}

type volumeRecord struct {
	Volume    Volume            `json:"volume"`
	UID       string            `json:"uid"`
	ClientID  string            `json:"clientId"`
	Provider  string            `json:"provider"`
	Namespace string            `json:"namespace"`
	Agent     guestapi.Identity `json:"agent"`
}

type createVolumeRequest struct {
	ID        string `json:"id,omitempty"`
	OwnerKey  string `json:"ownerKey"`
	ProfileID string `json:"profileId"`
}

// Box IDs also form Kubernetes names. Volume IDs use the same portable syntax.
var resourceID = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,53}[a-z0-9])?$`)

func validateResourceID(id string) error {
	if !resourceID.MatchString(id) {
		return apiError("INVALID_REQUEST", "id must be a lowercase DNS label of at most 55 characters")
	}
	return nil
}

// Reservations outlive history retention and purge, preventing delayed requests
// from targeting a new Box that reused an old public ID.
func (tx *storeTx) reserveID(kind, id, client string) error {
	var exists bool
	if err := tx.tx.QueryRowContext(tx.ctx, `SELECT EXISTS(SELECT 1 FROM runtime_records WHERE kind IN (?,?) AND id=?)`, kind, kind+"_ids", id).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return apiError("CONFLICT", "Resource ID has already been used")
	}
	return tx.put(kind+"_ids", id, client, "", struct{}{})
}

func (tx *storeTx) Volume(client, id string) (volumeRecord, error) {
	v, err := readRecord[volumeRecord](tx.ctx, tx.tx, "volumes", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && client != "" && v.ClientID != client {
		return volumeRecord{}, apiError("NOT_FOUND", "Volume not found")
	}
	return v, err
}

func (tx *storeTx) PutVolume(v volumeRecord) error {
	return tx.put("volumes", v.Volume.ID, v.ClientID, "", v)
}

func (s *Store) VolumeRecord(client, id string) (volumeRecord, error) {
	v, err := readRecord[volumeRecord](context.Background(), s.db, "volumes", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && v.ClientID != client {
		return volumeRecord{}, apiError("NOT_FOUND", "Volume not found")
	}
	return v, err
}

func (v volumeRecord) spec() boxprovider.VolumeSpec {
	return boxprovider.VolumeSpec{UID: v.UID, NodeName: v.Volume.NodeName, Namespace: v.Namespace, Agent: v.Agent}
}

func newVolume(id, owner, client string, p Profile) volumeRecord {
	return volumeRecord{Volume: Volume{ID: id, OwnerKey: owner, ProfileID: p.ID,
		NodeName: p.NodeName, Phase: "creating", Version: 1, CreatedAt: time.Now().UTC()},
		UID: randomID("vol-"), ClientID: client, Provider: p.Provider, Namespace: p.Namespace, Agent: p.Guest.Agent}
}

func (s *Service) volumeProvider(p Profile) (boxprovider.VolumeProvider, error) {
	vp, ok := s.providers[p.Provider].(boxprovider.VolumeProvider)
	if !ok || p.Provider != "resumable-k8s-pod" || !p.PersistentHome {
		return nil, apiError("UNSUPPORTED_CAPABILITY", "Volumes require the node-local resumable provider")
	}
	return vp, nil
}

func (s *Service) createVolume(client string, input createVolumeRequest) (Operation, error) {
	if input.OwnerKey == "" || len(input.OwnerKey) > 256 {
		return Operation{}, apiError("INVALID_REQUEST", "ownerKey must contain 1..256 bytes")
	}
	if input.ID == "" {
		input.ID = randomID("vol-")
	}
	if err := validateResourceID(input.ID); err != nil {
		return Operation{}, err
	}
	p, err := s.profile(client, input.ProfileID)
	if err != nil {
		return Operation{}, err
	}
	vp, err := s.volumeProvider(p)
	if err != nil {
		return Operation{}, err
	}
	v := newVolume(input.ID, input.OwnerKey, client, p)
	op, err := s.prepareOperation(client, "volume-create", "volume:"+input.ID, func(tx *storeTx, op *Operation) error {
		op.TargetID = input.ID
		if err := tx.reserveID("volumes", input.ID, client); err != nil {
			return err
		}
		v.Volume.OperationID = op.ID
		return tx.PutVolume(v)
	})
	if err != nil {
		return op, err
	}
	s.launchVolume(op, v, "available", func(ctx context.Context) error { return vp.CreateVolume(ctx, v.spec()) })
	return op, nil
}

func (s *Service) launchVolume(op Operation, v volumeRecord, phase string, work func(context.Context) error) {
	s.launchWithCommitContext(s.ctx, op, func(ctx context.Context) (map[string]string, error) {
		err := work(ctx)
		// Persist failure as well as success; never expose an unfinished filesystem
		// as available after a restart or failed create.
		persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		saveErr := s.store.Write(persist, func(tx *storeTx) error {
			current, readErr := tx.Volume(v.ClientID, v.Volume.ID)
			if readErr != nil {
				return readErr
			}
			if current.Volume.OperationID != op.ID {
				return boxprovider.ErrStaleTransition
			}
			current.Volume.Phase, current.Volume.Error = phase, nil
			if err != nil {
				current.Volume.Phase, current.Volume.Error = "failed", cloneError(err)
			}
			current.Volume.OperationID = ""
			current.Volume.Version++
			return tx.PutVolume(current)
		})
		return map[string]string{"volumeId": v.Volume.ID}, errors.Join(err, saveErr)
	}, nil)
}

func (s *Service) deleteVolume(client, id string) (Operation, error) {
	var v volumeRecord
	op, err := s.prepareOperation(client, "volume-delete", "volume:"+id, func(tx *storeTx, op *Operation) error {
		op.TargetID = id
		var err error
		v, err = tx.Volume(client, id)
		if err != nil {
			return err
		}
		if v.Volume.OperationID != "" || v.Volume.Phase != "available" && v.Volume.Phase != "failed" {
			return apiError("CONFLICT", "Volume must be ready or failed before deletion")
		}
		boxes, err := listRecords[boxRecord](tx.ctx, tx.tx, "boxes", "AND client_id=? AND json_extract(record,'$.box.phase')!='deleted'", client)
		if err != nil {
			return err
		}
		for _, box := range boxes {
			for _, mount := range boxVolumeMounts(box) {
				if mount.VolumeID == id {
					return apiError("CONFLICT", "Volume is referenced by a Box")
				}
			}
		}
		v.Volume.Phase, v.Volume.OperationID, v.Volume.Error = "deleting", op.ID, nil
		v.Volume.Version++
		return tx.PutVolume(v)
	})
	if err != nil {
		return op, err
	}
	vp, ok := s.providers[v.Provider].(boxprovider.VolumeProvider)
	if !ok {
		s.launchVolume(op, v, "deleted", func(context.Context) error { return boxprovider.ErrUnsupported })
	} else {
		s.launchVolume(op, v, "deleted", func(ctx context.Context) error { return vp.DeleteVolume(ctx, v.spec()) })
	}
	return op, nil
}

func (s *Service) registerVolumes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/volumes", func(w http.ResponseWriter, r *http.Request) {
		var input createVolumeRequest
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		op, err := s.createVolume(clientID(r), input)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, op)
	})
	mux.HandleFunc("GET /v1/volumes", func(w http.ResponseWriter, r *http.Request) {
		records, err := listRecords[volumeRecord](r.Context(), s.store.db, "volumes", "AND client_id=?", clientID(r))
		if err != nil {
			fail(w, err)
			return
		}
		out := make([]Volume, 0, len(records))
		for _, record := range records {
			out = append(out, record.Volume)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		writeJSON(w, http.StatusOK, out)
	})
	mux.HandleFunc("GET /v1/volumes/{id}", func(w http.ResponseWriter, r *http.Request) {
		v, err := s.store.VolumeRecord(clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, v.Volume)
	})
	mux.HandleFunc("DELETE /v1/volumes/{id}", func(w http.ResponseWriter, r *http.Request) {
		op, err := s.deleteVolume(clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, op)
	})
}

// Resolve mount identities in the Box creation transaction. Existing volumes
// may be referenced by any number of compatible Boxes without a reservation.
func (s *Service) admitBoxMounts(tx *storeTx, b *boxRecord, mounts []Mount, homeID string) error {
	if !b.Profile.PersistentHome {
		return nil
	}
	automatic := len(mounts) == 0 && homeID == ""
	if len(mounts) == 0 {
		id := homeID
		if automatic {
			id = randomID("vol-")
		}
		mounts = []Mount{{VolumeID: id, MountPath: "/home/agent"}}
	}
	resolved := make(map[string]volumeRecord)
	for _, mount := range mounts {
		if err := validateResourceID(mount.VolumeID); err != nil {
			return err
		}
		if !path.IsAbs(mount.MountPath) || path.Clean(mount.MountPath) != mount.MountPath || mount.MountPath == "/" || strings.IndexFunc(mount.MountPath, unicode.IsControl) >= 0 {
			return apiError("INVALID_REQUEST", "mountPath must be a clean absolute directory path other than /")
		}
		// Preserve the caller's ordering and repeated/nested destinations. Those
		// are runtime mount semantics, not an admission conflict policy.
		v, exists := resolved[mount.VolumeID]
		if !exists {
			var err error
			v, err = s.resolveBoxVolume(tx, b, mount.VolumeID, automatic)
			if err != nil {
				return err
			}
			resolved[mount.VolumeID] = v
		}
		b.Box.Mounts = append(b.Box.Mounts, mount)
		b.VolumeMounts = append(b.VolumeMounts, api.VolumeMount{UID: v.UID, MountPath: mount.MountPath, ReadOnly: mount.ReadOnly})
		if mount.MountPath == "/home/agent" {
			b.Box.HomeVolumeID, b.HomeVolumeUID = v.Volume.ID, v.UID
		}
	}
	b.Box.Capabilities = boxCapabilities(*b)
	return nil
}

func boxCapabilities(b boxRecord) Capabilities {
	c := profileCapabilities(b.Profile)
	if len(b.VolumeMounts) == 0 {
		return c
	}
	c.PersistentHome = false
	for _, mount := range b.VolumeMounts {
		if mount.MountPath == "/home/agent" {
			c.PersistentHome = true
		}
	}
	if !boxCanArchiveWorkspace(b) || !workspaceUnderPersistentHome(b.Profile.Guest.Workspace) {
		c.Archives = "none"
	}
	return c
}

func (s *Service) resolveBoxVolume(tx *storeTx, b *boxRecord, id string, automatic bool) (volumeRecord, error) {
	var v volumeRecord
	if automatic {
		if err := tx.reserveID("volumes", id, b.ClientID); err != nil {
			return v, err
		}
		v = newVolume(id, b.Box.OwnerKey, b.ClientID, b.Profile)
		return v, tx.PutVolume(v)
	}
	var err error
	v, err = tx.Volume(b.ClientID, id)
	if err != nil {
		return v, err
	}
	if v.Volume.Phase != "available" || v.Volume.OperationID != "" {
		return v, apiError("CONFLICT", "Volume is not available")
	}
	if v.Volume.OwnerKey != b.Box.OwnerKey || v.Provider != b.Profile.Provider ||
		v.Volume.NodeName != b.Profile.NodeName || v.Namespace != b.Profile.Namespace || v.Agent != b.Profile.Guest.Agent {
		return v, apiError("CONFLICT", "Volume owner, node, namespace, or agent UID/GID differs from the target Box")
	}
	return v, nil
}

func (s *Service) prepareBoxVolume(ctx context.Context, b boxRecord) error {
	seen := make(map[string]bool)
	for i, mount := range boxVolumeMounts(b) {
		if seen[mount.VolumeID] {
			continue
		}
		seen[mount.VolumeID] = true
		uid := b.HomeVolumeUID
		if i < len(b.VolumeMounts) {
			uid = b.VolumeMounts[i].UID
		}
		if err := s.prepareVolumeForBox(ctx, b, mount.VolumeID, uid); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) prepareVolumeForBox(ctx context.Context, b boxRecord, id, uid string) error {
	v, err := s.store.VolumeRecord(b.ClientID, id)
	if err != nil {
		return err
	}
	if v.UID != uid {
		return apiError("CONFLICT", "Volume identity changed")
	}
	if v.Volume.Phase != "creating" {
		if v.Volume.Phase != "available" || v.Volume.OperationID != "" {
			return apiError("CONFLICT", "Volume is not ready")
		}
		return nil
	}
	vp, err := s.volumeProvider(b.Profile)
	if err != nil {
		return err
	}
	createErr := vp.CreateVolume(ctx, v.spec())
	persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err = s.store.Write(persist, func(tx *storeTx) error {
		current, err := tx.Volume(b.ClientID, v.Volume.ID)
		if err != nil {
			return err
		}
		if current.UID != v.UID || current.Volume.Phase != "creating" {
			return boxprovider.ErrStaleTransition
		}
		current.Volume.Phase = "available"
		if createErr != nil {
			current.Volume.Phase, current.Volume.Error = "failed", cloneError(createErr)
		}
		current.Volume.Version++
		return tx.PutVolume(current)
	})
	return errors.Join(createErr, err)
}

// Existing Boxes retain their original runtime layout until termination. The
// node finalizer promotes that HOME in place after all of its writers stop.
func (s *Service) registerLegacyVolume(tx *storeTx, b *boxRecord) error {
	if !b.Profile.PersistentHome || len(b.VolumeMounts) != 0 || b.Box.HomeVolumeID != "" || b.Handle.ID == "" {
		return nil
	}
	id := randomID("vol-")
	if err := tx.reserveID("volumes", id, b.ClientID); err != nil {
		return err
	}
	v := newVolume(id, b.Box.OwnerKey, b.ClientID, b.Profile)
	v.UID = b.Handle.ID
	b.Box.HomeVolumeID = id
	return tx.PutVolume(v)
}

// Legacy storage is promoted by the node during deletion. Publish it only after
// that conversion completes; managed volumes need no change when a Box ends.
func finishLegacyVolume(tx *storeTx, b boxRecord) error {
	if len(b.VolumeMounts) != 0 || b.Box.HomeVolumeID == "" || b.Handle.ID == "" {
		return nil
	}
	v, err := tx.Volume(b.ClientID, b.Box.HomeVolumeID)
	if err != nil {
		return err
	}
	interrupted := v.Volume.Phase == "failed" && v.Volume.Error != nil && v.Volume.Error.Code == "UNKNOWN_OUTCOME"
	if v.UID != b.Handle.ID || v.Volume.Phase != "creating" && !interrupted {
		return nil
	}
	v.Volume.Phase, v.Volume.Error = "available", nil
	v.Volume.Version++
	return tx.PutVolume(v)
}

func boxVolumeMounts(b boxRecord) []Mount {
	if len(b.Box.Mounts) != 0 {
		return b.Box.Mounts
	}
	if b.Box.HomeVolumeID != "" {
		return []Mount{{VolumeID: b.Box.HomeVolumeID, MountPath: "/home/agent"}}
	}
	return nil
}

func boxHasWritableHome(b boxRecord) bool {
	if len(b.VolumeMounts) == 0 {
		return b.Profile.PersistentHome // Legacy layout.
	}
	writable := false
	for _, mount := range b.VolumeMounts {
		if mount.MountPath == "/home/agent" {
			writable = !mount.ReadOnly
		}
	}
	return writable
}

// Offline workspace capture reads a backing directory, not a running mount
// namespace. It cannot faithfully include another volume overlaid in that tree.
// Such mounts remain valid for creation; only archive capture is unavailable.
func boxCanArchiveWorkspace(b boxRecord) bool {
	if !boxHasWritableHome(b) {
		return false
	}
	homeIndex := -1
	for i, mount := range b.VolumeMounts {
		if mount.MountPath == "/home/agent" {
			homeIndex = i
		}
	}
	workspace := b.Profile.Guest.Workspace
	for i, mount := range b.VolumeMounts {
		if i == homeIndex {
			continue
		}
		if workspace == mount.MountPath || strings.HasPrefix(workspace, mount.MountPath+"/") || strings.HasPrefix(mount.MountPath, workspace+"/") {
			return false
		}
	}
	return true
}
