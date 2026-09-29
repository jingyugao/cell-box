package service

import (
	"archive/tar"
	"cellbox.local/cellbox/internal/image"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const maxImageUpload = 128 << 20
const maxProductFiles = 10000

func (s *Service) buildImage(w http.ResponseWriter, r *http.Request) {
	if s.config.ImageBuild == (ImageBuildConfig{}) {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Image building is not configured"))
		return
	}
	if r.Header.Get("Idempotency-Key") == "" {
		fail(w, apiError("INVALID_REQUEST", "Idempotency-Key header is required"))
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data;") {
		fail(w, apiError("INVALID_REQUEST", "multipart/form-data is required"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxImageUpload)
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		fail(w, apiError("INVALID_REQUEST", "Invalid or oversized image build request"))
		return
	}
	defer r.MultipartForm.RemoveAll()
	form := r.MultipartForm
	for name := range form.Value {
		if name != "baseImage" && name != "platform" && name != "manifest" {
			fail(w, apiError("INVALID_REQUEST", "Unknown image build field"))
			return
		}
	}
	for name := range form.File {
		if name != "product" {
			fail(w, apiError("INVALID_REQUEST", "Unknown image build file"))
			return
		}
	}
	field := func(name string) (string, bool) {
		values := form.Value[name]
		if len(values) > 1 {
			return "", false
		}
		if len(values) == 0 {
			return "", true
		}
		return values[0], true
	}
	base, ok := field("baseImage")
	platform, platformOK := field("platform")
	manifest, manifestOK := field("manifest")
	if !ok || !platformOK || !manifestOK || !image.ValidRegistryImage(base) || !image.ValidPlatform(platform) || len(manifest) > 1<<20 || (manifest != "" && !json.Valid([]byte(manifest))) || len(form.File["product"]) > 1 {
		fail(w, apiError("INVALID_REQUEST", "Invalid base image, platform, manifest, or product upload"))
		return
	}
	dir, err := os.MkdirTemp("", "cellbox-upload-")
	if err != nil {
		fail(w, err)
		return
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(dir)
		}
	}()
	productDir := ""
	productHash := ""
	if parts := form.File["product"]; len(parts) == 1 {
		productDir = filepath.Join(dir, "product")
		if err = os.Mkdir(productDir, 0700); err != nil {
			fail(w, err)
			return
		}
		file, openErr := parts[0].Open()
		if openErr != nil {
			fail(w, apiError("INVALID_REQUEST", "Cannot read product archive"))
			return
		}
		h := sha256.New()
		if _, err = io.Copy(h, file); err == nil {
			_, err = file.Seek(0, io.SeekStart)
		}
		if err == nil {
			err = extractProduct(tar.NewReader(file), productDir)
		}
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			fail(w, apiError("INVALID_REQUEST", "Product must be a bounded tar archive of regular files"))
			return
		}
		productHash = hex.EncodeToString(h.Sum(nil))
	}
	manifestPath := ""
	if manifest != "" {
		if productDir != "" {
			if _, statErr := os.Stat(filepath.Join(productDir, "manifest.json")); statErr == nil {
				fail(w, apiError("INVALID_REQUEST", "Product already contains manifest.json"))
				return
			}
		}
		manifestPath = filepath.Join(dir, "manifest.json")
		if err = os.WriteFile(manifestPath, []byte(manifest), 0600); err != nil {
			fail(w, err)
			return
		}
	}
	input := struct{ Base, Platform, Manifest, ProductHash string }{base, platform, manifest, productHash}
	op, fresh, err := s.prepareOperation(clientID(r), r.Header.Get("Idempotency-Key"), "image-build", "", input, func(st *State, _ *Operation) error {
		for _, record := range st.Operations {
			if record.Operation.Kind == "image-build" && (record.Operation.Status == "running" || record.Operation.Status == "queued") {
				return apiError("BUSY", "Another image build is running")
			}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if fresh {
		cleanup = false
		s.launch(op, func(ctx context.Context) (map[string]string, error) {
			defer os.RemoveAll(dir)
			result, buildErr := image.PrepareBuildKit(ctx, image.BuildKitOptions{
				Address: s.config.ImageBuild.Address, Binary: s.config.ImageBuild.BuildctlBinary,
				Repository: s.config.ImageBuild.Repository, Base: base,
				GuestBinary: s.config.ImageBuild.GuestBinary, ProductDir: productDir,
				Manifest: manifestPath, Platform: platform,
			})
			if buildErr != nil {
				return nil, buildErr
			}
			return map[string]string{"image": result.Image, "digest": result.Digest, "tag": result.Tag, "key": result.Key}, nil
		})
	}
	writeJSON(w, http.StatusAccepted, op)
}

func extractProduct(tr *tar.Reader, root string) error {
	seen := map[string]bool{}
	dirs := map[string]os.FileMode{}
	var total int64
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			names := make([]string, 0, len(dirs))
			for name := range dirs {
				names = append(names, name)
			}
			sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
			for _, name := range names {
				if err := os.Chmod(filepath.Join(root, filepath.FromSlash(name)), dirs[name]); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			return err
		}
		name := h.Name
		if name == "" || path.IsAbs(name) || strings.Contains(name, "\\") || path.Clean(name) != strings.TrimSuffix(name, "/") || name == "." || strings.HasPrefix(name, "../") || name == ".." || seen[strings.TrimSuffix(name, "/")] || len(seen) >= maxProductFiles {
			return errors.New("invalid product path")
		}
		name = strings.TrimSuffix(name, "/")
		seen[name] = true
		if h.Mode < 0 || h.Mode > 0777 || h.Size < 0 || h.Size > maxImageUpload-total {
			return errors.New("invalid product mode or size")
		}
		dst := filepath.Join(root, filepath.FromSlash(name))
		switch h.Typeflag {
		case tar.TypeDir:
			if h.Size != 0 {
				return errors.New("directory has content")
			}
			if err = os.MkdirAll(dst, 0700); err != nil {
				return err
			}
			dirs[name] = os.FileMode(h.Mode) & 0777
		case tar.TypeReg, tar.TypeRegA:
			if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
				return err
			}
			f, createErr := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0777)
			if createErr != nil {
				return createErr
			}
			_, copyErr := io.CopyN(f, tr, h.Size)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			total += h.Size
		default:
			return errors.New("product contains unsupported entry")
		}
	}
}
