package service

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"encoding/json"
	"maps"
	"slices"
	"strings"
)

func applyImageConfiguration(profile Profile, imageRef string, command []string, commandDir string, imageEnv map[string]string) Profile {
	data, err := json.Marshal(profile)
	if err != nil {
		return Profile{}
	}
	var out Profile
	if json.Unmarshal(data, &out) != nil {
		return Profile{}
	}
	out.Image = imageRef
	out.Guest.Command = append([]string(nil), command...)
	out.Guest.CommandDir = commandDir
	out.Guest.Env = maps.Clone(imageEnv)
	if out.Guest.Env == nil {
		out.Guest.Env = map[string]string{}
	}
	if out.AutoTrustImportedImages {
		for _, images := range []*[]string{&out.TrustedToolImages, &out.MountedToolRuntimeImages, &out.DebugHomeImages} {
			if !slices.Contains(*images, imageRef) {
				*images = append(*images, imageRef)
			}
		}
	}
	if !slices.Contains(out.TrustedToolImages, imageRef) {
		out.Guest.Tools = nil
		out.SharedReadOnlyHostPath = ""
	}
	out.DebugReadOnlyHostPath = ""
	if !profileCapabilities(out).MountedDebugHome {
		out.DebugReadWriteHostPath = ""
	}
	for key, value := range profile.Guest.Env {
		if out.SharedReadOnlyHostPath == "" && (value == api.SharedMountPath || strings.HasPrefix(value, api.SharedMountPath+"/")) {
			continue
		}
		out.Guest.Env[key] = value
	}
	return out
}

type imageUsage struct {
	Deletable      bool     `json:"deletable"`
	Blockers       []string `json:"blockers"`
	ManifestShared bool     `json:"manifestShared"`
}

// preparedImageIdentity returns the repository and digest that remote.Delete
// would address. Invalid or non-digest references never qualify for deletion.
