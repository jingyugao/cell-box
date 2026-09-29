package image

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var registryImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}$`)
var registryRepository = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*$`)

func ValidRegistryImage(value string) bool { return registryImage.MatchString(value) }
func ValidRepository(value string) bool    { return registryRepository.MatchString(value) }
func ValidPlatform(value string) bool      { return value == "" || validPlatform(value) }

type BuildKitOptions struct {
	Address     string
	Binary      string
	Repository  string
	Base        string
	GuestBinary string
	ProductDir  string
	Manifest    string
	Platform    string
}

type BuildKitResult struct {
	Image  string `json:"image"`
	Digest string `json:"digest"`
	Tag    string `json:"tag"`
	Key    string `json:"key"`
	Base   string `json:"base"`
}

type boundedOutput struct {
	mu   sync.Mutex
	data []byte
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	const limit = 64 << 10
	if len(b.data) < limit {
		remaining := limit - len(b.data)
		b.data = append(b.data, p[:min(len(p), remaining)]...)
	}
	return len(p), nil
}

// PrepareBuildKit sends a fixed build context to BuildKit and publishes it.
// The returned manifest digest can be used directly by Kubernetes profiles.
func PrepareBuildKit(ctx context.Context, opts BuildKitOptions) (BuildKitResult, error) {
	var zero BuildKitResult
	if !strings.HasPrefix(opts.Address, "unix:///") || strings.ContainsAny(opts.Address, "\x00\n\r") {
		return zero, errors.New("a Unix BuildKit socket is required")
	}
	if !registryImage.MatchString(opts.Base) || !registryRepository.MatchString(opts.Repository) {
		return zero, errors.New("immutable base image and registry repository are required")
	}
	if opts.Platform != "" && !validPlatform(opts.Platform) {
		return zero, errors.New("invalid platform")
	}
	if err := validateGuest(opts.GuestBinary); err != nil {
		return zero, fmt.Errorf("guest binary: %w", err)
	}
	guestHash, err := hashFile(opts.GuestBinary)
	if err != nil {
		return zero, err
	}
	productHash, files, err := hashTree(opts.ProductDir)
	if err != nil {
		return zero, fmt.Errorf("product payload: %w", err)
	}
	manifestHash := ""
	if opts.Manifest != "" {
		manifestHash, err = hashFile(opts.Manifest)
		if err != nil {
			return zero, err
		}
		manifestBytes, readErr := os.ReadFile(opts.Manifest)
		if readErr != nil || !json.Valid(manifestBytes) {
			return zero, errors.New("manifest must be valid JSON")
		}
		if _, ok := files["manifest.json"]; ok {
			return zero, errors.New("product payload already contains manifest.json")
		}
	}
	keyBytes := sha256.Sum256([]byte(strings.Join([]string{ImageVersion, opts.Base, opts.Platform, guestHash, productHash, manifestHash}, "\n")))
	key := hex.EncodeToString(keyBytes[:])
	tag := opts.Repository + ":cellbox-" + key
	dir, err := os.MkdirTemp("", "cellbox-buildkit-")
	if err != nil {
		return zero, err
	}
	defer os.RemoveAll(dir)
	contextDir := filepath.Join(dir, "context")
	if err = os.Mkdir(contextDir, 0700); err != nil {
		return zero, err
	}
	if err = copyRegular(opts.GuestBinary, filepath.Join(contextDir, "guest"), 0755); err != nil {
		return zero, err
	}
	if copiedHash, e := hashFile(filepath.Join(contextDir, "guest")); e != nil || copiedHash != guestHash {
		return zero, errors.New("guest changed while preparing image")
	}
	if opts.ProductDir != "" || opts.Manifest != "" {
		if err = os.Mkdir(filepath.Join(contextDir, "product"), 0700); err != nil {
			return zero, err
		}
		for _, rel := range sortedKeys(files) {
			dst := filepath.Join(contextDir, "product", filepath.FromSlash(rel))
			if files[rel].IsDir() {
				if err = os.MkdirAll(dst, 0700); err != nil {
					return zero, err
				}
				continue
			}
			if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
				return zero, err
			}
			if err = copyRegular(filepath.Join(opts.ProductDir, filepath.FromSlash(rel)), dst, files[rel]); err != nil {
				return zero, err
			}
		}
		keys := sortedKeys(files)
		for i := len(keys) - 1; i >= 0; i-- {
			if files[keys[i]].IsDir() {
				if err = os.Chmod(filepath.Join(contextDir, "product", filepath.FromSlash(keys[i])), files[keys[i]].Perm()); err != nil {
					return zero, err
				}
			}
		}
		if opts.ProductDir != "" {
			if copiedHash, _, e := hashTree(filepath.Join(contextDir, "product")); e != nil || copiedHash != productHash {
				return zero, errors.New("product changed while preparing image")
			}
		}
		if opts.Manifest != "" {
			if err = copyRegular(opts.Manifest, filepath.Join(contextDir, "product", "manifest.json"), 0644); err != nil {
				return zero, err
			}
			if copiedHash, e := hashFile(filepath.Join(contextDir, "product", "manifest.json")); e != nil || copiedHash != manifestHash {
				return zero, errors.New("manifest changed while preparing image")
			}
		}
	}
	dockerfile := "FROM " + opts.Base + "\nLABEL " + ManagedLabel + "=\"" + ImageVersion + "\" " + IdentityLabel + "=\"" + key + "\"\nCOPY --chmod=0755 guest /opt/cellbox/bin/cellbox-guest\n"
	if opts.ProductDir != "" || opts.Manifest != "" {
		dockerfile += "COPY product/ /opt/product/\n"
	}
	if err = os.WriteFile(filepath.Join(contextDir, "Dockerfile"), []byte(dockerfile), 0600); err != nil {
		return zero, err
	}
	if opts.Binary == "" {
		opts.Binary = "buildctl"
	}
	metadata := filepath.Join(dir, "metadata.json")
	args := []string{"--addr", opts.Address, "build", "--frontend", "dockerfile.v0", "--local", "context=" + contextDir, "--local", "dockerfile=" + contextDir, "--output", "type=image,name=" + tag + ",push=true", "--metadata-file", metadata}
	if opts.Platform != "" {
		args = append(args, "--opt", "platform="+opts.Platform)
	}
	cmd := exec.CommandContext(ctx, opts.Binary, args...)
	output := &boundedOutput{}
	cmd.Stdout, cmd.Stderr = output, output
	err = cmd.Run()
	if err != nil {
		return zero, fmt.Errorf("BuildKit build failed: %w: %s", err, strings.TrimSpace(string(output.data)))
	}
	data, err := os.ReadFile(metadata)
	if err != nil {
		return zero, fmt.Errorf("BuildKit metadata: %w", err)
	}
	var result struct {
		Digest string `json:"containerimage.digest"`
	}
	if err = json.Unmarshal(data, &result); err != nil || !imageIDPattern.MatchString(result.Digest) {
		return zero, errors.New("BuildKit returned no valid registry digest")
	}
	return BuildKitResult{Image: opts.Repository + "@" + result.Digest, Digest: result.Digest, Tag: tag, Key: key, Base: opts.Base}, nil
}
