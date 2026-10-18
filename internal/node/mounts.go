package node

import (
	"fmt"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
)

func configuredVolumeMounts(r *api.ResumablePod) []VolumeMountAuthorization {
	if r.Spec.Mounts != nil {
		mounts := make([]VolumeMountAuthorization, len(r.Spec.Mounts))
		for i, mount := range r.Spec.Mounts {
			mounts[i] = VolumeMountAuthorization{UID: mount.UID, MountPath: mount.MountPath, ReadOnly: mount.ReadOnly}
		}
		return mounts
	}
	if !r.Spec.PersistentHome {
		return nil
	}
	uid := r.Spec.HomeVolumeUID
	legacy := uid == ""
	if legacy {
		uid = string(r.UID)
	}
	return []VolumeMountAuthorization{{UID: uid, MountPath: homevolume.MountPath, Legacy: legacy}}
}

func uniqueVolumeMounts(mounts []VolumeMountAuthorization) []VolumeMountAuthorization {
	seen := make(map[string]struct{}, len(mounts))
	unique := make([]VolumeMountAuthorization, 0, len(mounts))
	for _, mount := range mounts {
		if _, ok := seen[mount.UID]; ok {
			continue
		}
		seen[mount.UID] = struct{}{}
		unique = append(unique, mount)
	}
	return unique
}

func volumeSpecHash(r *api.ResumablePod, mount VolumeMountAuthorization) string {
	if mount.Legacy {
		return r.Status.SpecHash
	}
	return homevolume.VolumeSpecHash
}

func (b *Backend) prepareVolumeMounts(r *api.ResumablePod, initialize bool) ([]VolumeMountAuthorization, error) {
	mounts := configuredVolumeMounts(r)
	identities := make(map[string]homevolume.Identity, len(mounts))
	for _, mount := range uniqueVolumeMounts(mounts) {
		var identity homevolume.Identity
		var err error
		if mount.Legacy {
			identity, err = homevolume.Prepare(b.Base, mount.UID, r.Spec.NodeName, volumeSpecHash(r, mount), initialize)
		} else if initialize {
			identity, err = homevolume.Attach(b.Base, mount.UID, r.Spec.NodeName, string(r.UID))
		} else {
			identity, err = homevolume.VerifyAttachment(b.Base, mount.UID, r.Spec.NodeName, string(r.UID))
		}
		if err != nil {
			return nil, fmt.Errorf("persistent volume %s: %w", mount.UID, err)
		}
		identities[mount.UID] = identity
	}
	for i := range mounts {
		mounts[i].HomeID = identities[mounts[i].UID].ID
	}
	return mounts, nil
}

func (b *Backend) detachVolumeMounts(r *api.ResumablePod, mounts []VolumeMountAuthorization) error {
	var firstErr error
	for _, mount := range uniqueVolumeMounts(mounts) {
		var err error
		if mount.Legacy {
			err = homevolume.PromoteLegacy(b.Base, mount.UID, r.Spec.NodeName, r.Status.SpecHash)
		} else {
			err = homevolume.Detach(b.Base, mount.UID, r.Spec.NodeName, string(r.UID))
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("detach persistent volume %s: %w", mount.UID, err)
		}
	}
	return firstErr
}
