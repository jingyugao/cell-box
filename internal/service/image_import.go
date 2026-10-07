package service

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/image"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	remotetransport "github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

type ImportedImage struct {
	ID             string            `json:"id"`
	Source         string            `json:"source"`
	ResolvedSource string            `json:"resolvedSource"`
	Image          string            `json:"image"`
	Platform       string            `json:"platform"`
	Command        []string          `json:"command"`
	Env            map[string]string `json:"env"`
	WorkingDir     string            `json:"workingDir"`
	BuildCommand   string            `json:"buildCommand,omitempty"`
	Ports          []int             `json:"ports"`
	Warnings       []string          `json:"warnings"`
	Key            string            `json:"key"`
	CreatedAt      time.Time         `json:"createdAt"`
	Deleting       bool              `json:"deleting,omitempty"`
}

type importedImageRecord struct {
	ImportedImage ImportedImage `json:"image"`
	ClientID      string        `json:"clientId"`
}

type importImageRequest struct {
	URL          string              `json:"url"`
	RunCommand   string              `json:"runCommand,omitempty"`
	BuildCommand string              `json:"buildCommand,omitempty"`
	Platform     string              `json:"platform,omitempty"`
	RegistryAuth *image.RegistryAuth `json:"registryAuth,omitempty"`
}

func imageBuildBusy(st *State) error {
	for _, record := range st.Operations {
		op := record.Operation
		if (op.Kind == "image-build" || op.Kind == "image-import" || op.Kind == "image-delete") && (op.Status == "running" || op.Status == "queued") {
			return apiError("BUSY", "Another image preparation is running")
		}
	}
	return nil
}

func (s *Service) importImage(w http.ResponseWriter, r *http.Request) {
	if s.config.ImageBuild == (ImageBuildConfig{}) {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Image building is not configured"))
		return
	}
	var input importImageRequest
	if err := decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	if err := image.ValidateBuildCommand(input.BuildCommand); err != nil {
		fail(w, apiError("INVALID_REQUEST", err.Error()))
		return
	}
	if input.Platform == "" {
		input.Platform = "linux/amd64"
	}
	if input.Platform != "linux/amd64" {
		fail(w, apiError("INVALID_REQUEST", "image import currently supports linux/amd64"))
		return
	}
	ref, err := image.ParseRegistryReference(input.URL, s.config.ImageBuild.InsecureRegistry)
	if err != nil {
		fail(w, apiError("INVALID_REQUEST", err.Error()))
		return
	}
	input.URL = ref.Name()
	run, err := image.ParseRunCommand(input.RunCommand)
	if err != nil {
		fail(w, apiError("INVALID_REQUEST", err.Error()))
		return
	}
	if run.Image != "" {
		runRef, err := image.ParseRegistryReference(run.Image, s.config.ImageBuild.InsecureRegistry)
		if err != nil || runRef.Name() != ref.Name() {
			fail(w, apiError("INVALID_REQUEST", "runCommand image must match url"))
			return
		}
	}
	if input.RegistryAuth != nil && (input.RegistryAuth.Username == "" || input.RegistryAuth.Password == "" || len(input.RegistryAuth.Username) > 1024 || len(input.RegistryAuth.Password) > 8192) {
		fail(w, apiError("INVALID_REQUEST", "registryAuth requires bounded username and password"))
		return
	}
	client := clientID(r)
	op, fresh, err := s.prepareOperation(client, r.Header.Get("Idempotency-Key"), "image-import", "", input, func(st *State, op *Operation) error {
		if err := imageBuildBusy(st); err != nil {
			return err
		}
		op.TargetID = randomID("img-")
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if fresh {
		s.launch(op, func(ctx context.Context) (map[string]string, error) {
			resolved, err := image.ResolveRegistryImage(ctx, input.URL, input.Platform, s.config.ImageBuild.InsecureRegistry, input.RegistryAuth, run)
			if err != nil {
				return nil, apiError("IMAGE_IMPORT_FAILED", err.Error())
			}
			base, credentials := resolved.Source, input.RegistryAuth
			destination, err := image.ParseRegistryReference(s.config.ImageBuild.Repository+":import", s.config.ImageBuild.InsecureRegistry)
			if err != nil {
				return nil, err
			}
			if credentials != nil && ref.Context().RegistryStr() == destination.Context().RegistryStr() {
				base, err = image.MirrorRegistryImage(ctx, resolved, s.config.ImageBuild.Repository, s.config.ImageBuild.InsecureRegistry)
				if err != nil {
					return nil, apiError("IMAGE_IMPORT_FAILED", err.Error())
				}
				credentials = nil
			}
			dockerConfig, cleanup, err := importDockerConfig(ref.Context().RegistryStr(), credentials)
			if err != nil {
				return nil, err
			}
			defer cleanup()
			prepared, err := image.PrepareBuildKit(ctx, image.BuildKitOptions{Address: s.config.ImageBuild.Address, Binary: s.config.ImageBuild.BuildctlBinary, Repository: s.config.ImageBuild.Repository, Base: base, GuestBinary: s.config.ImageBuild.GuestBinary, Platform: resolved.Platform, DockerConfig: dockerConfig, BuildCommand: input.BuildCommand})
			if err != nil {
				return nil, apiError("IMAGE_IMPORT_FAILED", "Cannot prepare and publish image; check BuildKit and registry access")
			}
			cacheStarted := time.Now()
			err = s.cacheImportedImage(ctx, prepared.Image)
			runtimeTiming(op.TargetID, "cellbox.cache_imported_image", cacheStarted, err)
			if err != nil {
				return nil, apiError("IMAGE_IMPORT_FAILED", "Cannot cache prepared image on runtime nodes; check node controller and registry access")
			}
			out := ImportedImage{ID: op.TargetID, Source: input.URL, ResolvedSource: resolved.Source, Image: prepared.Image, Platform: resolved.Platform, Command: resolved.Command, Env: resolved.Env, WorkingDir: resolved.WorkingDir, BuildCommand: input.BuildCommand, Ports: append([]int{}, run.Ports...), Warnings: append([]string{}, resolved.Warnings...), Key: prepared.Key, CreatedAt: time.Now().UTC()}
			if err := s.store.Update(func(st *State) error {
				st.ImportedImages[out.ID] = importedImageRecord{ImportedImage: out, ClientID: client}
				return nil
			}); err != nil {
				return nil, err
			}
			return map[string]string{"importedImageId": out.ID, "image": out.Image}, nil
		})
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (s *Service) cacheImportedImage(ctx context.Context, image string) error {
	seen := map[string]bool{}
	for _, profile := range s.config.Profiles {
		key := profile.Provider + ":" + profile.NodeName + ":" + profile.Namespace
		if seen[key] {
			continue
		}
		seen[key] = true
		provider, ok := s.providers[profile.Provider].(boxprovider.ImageCacheProvider)
		if !ok {
			return boxprovider.ErrUnsupported
		}
		if err := provider.CacheImage(ctx, image, profile.NodeName, profile.Namespace); err != nil {
			return err
		}
	}
	return nil
}

// Source credentials exist only in an operation-local 0600 file and are never
// serialized into API state. Existing destination credentials are preserved.
func importDockerConfig(registry string, credentials *image.RegistryAuth) (string, func(), error) {
	if credentials == nil {
		return "", func() {}, nil
	}
	dir, err := os.MkdirTemp("", "cellbox-registry-")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	cfg := map[string]any{}
	configDir := os.Getenv("DOCKER_CONFIG")
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			configDir = filepath.Join(home, ".docker")
		}
	}
	if configDir != "" {
		data, err := os.ReadFile(filepath.Join(configDir, "config.json"))
		if err == nil {
			if err = json.Unmarshal(data, &cfg); err != nil {
				cleanup()
				return "", func() {}, errors.New("invalid server Docker authentication configuration")
			}
		}
		if err != nil && !os.IsNotExist(err) {
			cleanup()
			return "", func() {}, err
		}
	}
	if cfg["credsStore"] != nil {
		cleanup()
		return "", func() {}, errors.New("per-import credentials require a server Docker config with inline auths")
	}
	auths, _ := cfg["auths"].(map[string]any)
	if auths == nil {
		auths = map[string]any{}
	}
	if registry == "index.docker.io" {
		registry = "https://index.docker.io/v1/"
	}
	auths[registry] = map[string]string{"username": credentials.Username, "password": credentials.Password}
	cfg["auths"] = auths
	// A configured credential helper must not supersede the submitted source credentials.
	if helpers, ok := cfg["credHelpers"].(map[string]any); ok {
		delete(helpers, registry)
	}
	data, err := json.Marshal(cfg)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "config.json"), data, 0600)
	}
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	return dir, cleanup, nil
}

func (s *Service) listImportedImages(w http.ResponseWriter, r *http.Request) {
	out := []ImportedImage{}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	records, err := s.store.ListImages(ctx)
	if err == nil {
		for _, record := range records {
			if record.ClientID == clientID(r) {
				out = append(out, record.ImportedImage)
			}
		}
	}

	if err != nil {
		fail(w, err)
		return
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) getImportedImage(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	record, err := s.store.ReadImage(ctx, r.PathValue("id"))
	if err == nil && record.ClientID != clientID(r) {
		err = apiError("NOT_FOUND", "Imported image not found")
	}
	out := record.ImportedImage

	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) importedProfile(client, id string, profile Profile) (Profile, error) {
	var out Profile
	// Isolate the captured profile from shared maps and slices in service config.
	data, err := json.Marshal(profile)
	if err != nil {
		return out, err
	}
	if err = json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	err = s.store.View(func(st State) error {
		record, ok := st.ImportedImages[id]
		if !ok || record.ClientID != client {
			return apiError("NOT_FOUND", "Imported image not found")
		}
		if record.ImportedImage.Deleting {
			return apiError("CONFLICT", "Imported image is being deleted")
		}
		imported := record.ImportedImage
		out.Image = imported.Image
		out.Guest.Command = imported.Command
		out.Guest.CommandDir = imported.WorkingDir
		out.Guest.Env = maps.Clone(imported.Env)
		if out.Guest.Env == nil {
			out.Guest.Env = map[string]string{}
		}
		// Admitted CoCell images reuse the operator's read-only shared directory.
		// Other imported workloads keep their isolated startup configuration.
		if !slices.Contains(profile.TrustedToolImages, imported.Image) {
			out.Guest.Tools = nil
			out.SharedReadOnlyHostPath = ""
		}
		out.DebugReadOnlyHostPath = ""
		// Only an explicitly admitted HOME runner inherits the managed directory.
		if !profileCapabilities(out).MountedDebugHome {
			out.DebugReadWriteHostPath = ""
		}
		for key, value := range profile.Guest.Env {
			if out.SharedReadOnlyHostPath == "" && (value == api.SharedMountPath || strings.HasPrefix(value, api.SharedMountPath+"/")) {
				continue
			}
			out.Guest.Env[key] = value
		}
		return nil
	})
	return out, err
}

type imageUsage struct {
	Deletable      bool     `json:"deletable"`
	Blockers       []string `json:"blockers"`
	ManifestShared bool     `json:"manifestShared"`
}

// preparedImageIdentity returns the repository and digest that remote.Delete
// would address. Invalid or non-digest references never qualify for deletion.
func preparedImageIdentity(value, repository, insecureRegistry string) (string, bool) {
	ref, err := image.ParseRegistryReference(value, insecureRegistry)
	if err != nil {
		return "", false
	}
	digest, ok := ref.(name.Digest)
	if !ok {
		return "", false
	}
	repo, err := image.ParseRegistryReference(repository+":cellbox-validation", insecureRegistry)
	if err != nil || digest.Context().Name() != repo.Context().Name() {
		return "", false
	}
	return digest.Name(), true
}

func (s *Service) imageUsage(st State, id string) imageUsage {
	out := imageUsage{Blockers: []string{}}
	target, ok := st.ImportedImages[id]
	if !ok {
		return out
	}
	identity, valid := preparedImageIdentity(target.ImportedImage.Image, s.config.ImageBuild.Repository, s.config.ImageBuild.InsecureRegistry)
	matchesPrepared := func(value string) bool {
		if value == target.ImportedImage.Image {
			return valid
		}
		if valid && strings.HasPrefix(value, "sha256:") && strings.HasSuffix(identity, "@"+value) {
			return true
		}
		other, ok := preparedImageIdentity(value, s.config.ImageBuild.Repository, s.config.ImageBuild.InsecureRegistry)
		return valid && ok && identity == other
	}
	if !valid {
		out.Blockers = append(out.Blockers, "manifest-not-deletable")
	}
	if target.ImportedImage.Deleting {
		out.Blockers = append(out.Blockers, "deletion-pending")
	}
	for otherID, rec := range st.ImportedImages {
		if otherID == id {
			continue
		}
		other, otherValid := preparedImageIdentity(rec.ImportedImage.Image, s.config.ImageBuild.Repository, s.config.ImageBuild.InsecureRegistry)
		if valid && otherValid && identity == other {
			out.ManifestShared = true
			break
		}
	}
	for _, rec := range st.Boxes {
		if rec.Box.Phase != "deleted" && (rec.Box.ImportedImageID == id || matchesPrepared(rec.Box.Image) || matchesPrepared(rec.Box.ImageID) || matchesPrepared(rec.Profile.Image)) {
			out.Blockers = append(out.Blockers, "boxes-reference-image")
			break
		}
	}
	for _, profile := range s.config.Profiles {
		if matchesPrepared(profile.Image) {
			out.Blockers = append(out.Blockers, "profiles-reference-image")
			break
		}
	}
	for _, rec := range st.Archives {
		if rec.Archive.ImportedImageID == id || matchesPrepared(rec.Archive.PreparedImage) {
			out.Blockers = append(out.Blockers, "archives-reference-image")
			break
		}

	}
	if imageBuildBusy(&st) != nil {
		out.Blockers = append(out.Blockers, "image-operation-running")
	}
	out.Deletable = len(out.Blockers) == 0
	return out
}

func (s *Service) imageUsageHandler(w http.ResponseWriter, r *http.Request) {
	var usage imageUsage
	err := s.store.View(func(st State) error {
		rec, ok := st.ImportedImages[r.PathValue("id")]
		if !ok || rec.ClientID != clientID(r) {
			return apiError("NOT_FOUND", "Imported image not found")
		}
		usage = s.imageUsage(st, r.PathValue("id"))
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, usage)
}

func (s *Service) deleteImportedImage(w http.ResponseWriter, r *http.Request) {
	if s.config.ImageBuild == (ImageBuildConfig{}) {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Image building is not configured"))
		return
	}
	client, id := clientID(r), r.PathValue("id")
	var imported ImportedImage
	op, fresh, err := s.prepareOperation(client, r.Header.Get("Idempotency-Key"), "image-delete", id, nil, func(st *State, op *Operation) error {
		record, ok := st.ImportedImages[id]
		if !ok || record.ClientID != client {
			return apiError("NOT_FOUND", "Imported image not found")
		}
		if err := imageBuildBusy(st); err != nil {
			return err
		}
		usage := s.imageUsage(*st, id)
		for _, blocker := range usage.Blockers {
			if blocker != "deletion-pending" {
				return apiError("CONFLICT", "Imported image is still in use")
			}
		}
		identity, valid := preparedImageIdentity(record.ImportedImage.Image, s.config.ImageBuild.Repository, s.config.ImageBuild.InsecureRegistry)
		if !valid {
			return apiError("CONFLICT", "Imported image does not reference a deletable prepared manifest")
		}
		imported = record.ImportedImage
		imported.Image = identity
		if usage.ManifestShared {
			delete(st.ImportedImages, id)
			op.Result = map[string]string{"metadataOnly": "true"}
		} else {
			record.ImportedImage.Deleting = true
			st.ImportedImages[id] = record
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if fresh {
		s.launch(op, func(ctx context.Context) (map[string]string, error) {
			if op.Result["metadataOnly"] == "true" {
				return op.Result, nil
			}
			ref, err := image.ParseRegistryReference(imported.Image, s.config.ImageBuild.InsecureRegistry)
			if err != nil {
				return nil, apiError("IMAGE_DELETE_FAILED", "Prepared image reference is invalid")
			}
			if _, ok := ref.(name.Digest); !ok {
				return nil, apiError("IMAGE_DELETE_FAILED", "Prepared image reference is not immutable")
			}
			if err := remote.Delete(ref, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
				var registryError *remotetransport.Error
				if !errors.As(err, &registryError) || registryError.StatusCode != http.StatusNotFound {
					return nil, apiError("IMAGE_DELETE_FAILED", "Registry could not delete the prepared image; retry deletion")
				}
			}
			if err := s.store.Update(func(st *State) error {
				record, ok := st.ImportedImages[id]
				if !ok || record.ClientID != client || !record.ImportedImage.Deleting {
					return apiError("CONFLICT", "Imported image deletion state changed")
				}
				delete(st.ImportedImages, id)
				return nil
			}); err != nil {
				return nil, err
			}
			return map[string]string{"deleted": "true"}, nil
		})
	}
	writeJSON(w, http.StatusAccepted, op)
}
