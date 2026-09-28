// Package image builds Cellbox images from an immutable user base and explicit payloads.
package image

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ManagedLabel  = "cellbox.managed-image"
	IdentityLabel = "cellbox.image-key"
	ImageVersion  = "1"
)

var imageIDPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type Options struct {
	Binary      string // Docker executable; empty uses docker.
	Base        string
	GuestBinary string // Statically linked Linux executable supplied by caller.
	ProductDir  string // Optional regular-file tree installed at /opt/product.
	Manifest    string // Optional regular JSON manifest installed at /opt/product/manifest.json.
	Platform    string // Optional Docker platform, e.g. linux/amd64.
}

type Result struct {
	ImageID string `json:"imageId"`
	Tag     string `json:"tag"`
	BaseID  string `json:"baseId"`
	Key     string `json:"key"`
}

type Runner interface {
	Run(context.Context, []string) ([]byte, error)
}

type execRunner struct{ binary string }

func (r execRunner) Run(ctx context.Context, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.binary, args...)
	b, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(string(b)))
	}
	return b, nil
}

type Builder struct{ runner Runner }

func New(binary string) *Builder {
	if binary == "" {
		binary = "docker"
	}
	return &Builder{runner: execRunner{binary}}
}
func NewWithRunner(r Runner) *Builder { return &Builder{runner: r} }
func Prepare(ctx context.Context, opts Options) (Result, error) {
	return New(opts.Binary).Prepare(ctx, opts)
}

type imageInfo struct {
	ID     string `json:"Id"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

func (b *Builder) inspect(ctx context.Context, ref string) (imageInfo, error) {
	var info imageInfo
	out, err := b.runner.Run(ctx, []string{"image", "inspect", ref})
	if err != nil {
		return info, err
	}
	var infos []imageInfo
	if err = json.Unmarshal(out, &infos); err != nil || len(infos) != 1 || !imageIDPattern.MatchString(infos[0].ID) {
		return info, errors.New("invalid docker image inspect result")
	}
	return infos[0], nil
}

func (b *Builder) Prepare(ctx context.Context, opts Options) (result Result, err error) {
	var zero Result
	if opts.Base == "" || strings.HasPrefix(opts.Base, "-") || strings.ContainsAny(opts.Base, "\x00\n\r") {
		return zero, errors.New("valid base image required")
	}
	if opts.GuestBinary == "" {
		return zero, errors.New("guest binary required")
	}
	if opts.Platform != "" && !validPlatform(opts.Platform) {
		return zero, errors.New("invalid platform")
	}
	base, err := b.inspect(ctx, opts.Base)
	if err != nil {
		return zero, fmt.Errorf("inspect base: %w", err)
	}
	if !imageIDPattern.MatchString(base.ID) {
		return zero, errors.New("base has no immutable image ID")
	}
	if err = validateGuest(opts.GuestBinary); err != nil {
		return zero, fmt.Errorf("guest binary: %w", err)
	}
	guestHash, err := hashFile(opts.GuestBinary)
	if err != nil {
		return zero, fmt.Errorf("guest binary: %w", err)
	}
	productHash, files, err := hashTree(opts.ProductDir)
	if err != nil {
		return zero, fmt.Errorf("product payload: %w", err)
	}
	manifestHash := ""
	if opts.Manifest != "" {
		manifestHash, err = hashFile(opts.Manifest)
		if err != nil {
			return zero, fmt.Errorf("manifest: %w", err)
		}
		manifestBytes, readErr := os.ReadFile(opts.Manifest)
		if readErr != nil || !json.Valid(manifestBytes) {
			return zero, errors.New("manifest must be valid JSON")
		}
		if _, ok := files["manifest.json"]; ok {
			return zero, errors.New("product payload already contains manifest.json")
		}
	}
	keyBytes := sha256.Sum256([]byte(strings.Join([]string{ImageVersion, base.ID, opts.Platform, guestHash, productHash, manifestHash}, "\n")))
	key := hex.EncodeToString(keyBytes[:])
	tag := "cellbox-prepared:" + key
	result = Result{Tag: tag, BaseID: base.ID, Key: key}
	if cached, err := b.inspect(ctx, tag); err == nil {
		if cached.Config.Labels[ManagedLabel] != ImageVersion || cached.Config.Labels[IdentityLabel] != key {
			return zero, errors.New("prepared image tag owned by incompatible image")
		}
		result.ImageID = cached.ID
		return result, nil
	} else if !strings.Contains(err.Error(), "No such image") && !strings.Contains(err.Error(), "No such object") {
		return zero, fmt.Errorf("inspect cached image: %w", err)
	}
	dir, err := os.MkdirTemp("", "cellbox-image-")
	if err != nil {
		return zero, err
	}
	defer os.RemoveAll(dir)
	if err = os.Chmod(dir, 0700); err != nil {
		return zero, err
	}
	if err = copyRegular(opts.GuestBinary, filepath.Join(dir, "guest"), 0755); err != nil {
		return zero, err
	}
	if copiedHash, e := hashFile(filepath.Join(dir, "guest")); e != nil || copiedHash != guestHash {
		return zero, errors.New("guest changed while preparing image")
	}
	if opts.ProductDir != "" {
		if err = os.MkdirAll(filepath.Join(dir, "product"), 0700); err != nil {
			return zero, err
		}
		for _, rel := range sortedKeys(files) {
			src := filepath.Join(opts.ProductDir, filepath.FromSlash(rel))
			dst := filepath.Join(dir, "product", filepath.FromSlash(rel))
			if files[rel].IsDir() {
				if err = os.MkdirAll(dst, 0700); err != nil {
					return zero, err
				}
				continue
			}
			if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
				return zero, err
			}
			if err = copyRegular(src, dst, files[rel]); err != nil {
				return zero, err
			}
		}
		keys := sortedKeys(files)
		for i := len(keys) - 1; i >= 0; i-- {
			if files[keys[i]].IsDir() {
				if err = os.Chmod(filepath.Join(dir, "product", filepath.FromSlash(keys[i])), files[keys[i]].Perm()); err != nil {
					return zero, err
				}
			}
		}
		if copiedHash, _, e := hashTree(filepath.Join(dir, "product")); e != nil || copiedHash != productHash {
			return zero, errors.New("product payload changed while preparing image")
		}
	}
	if opts.Manifest != "" {
		if err = os.MkdirAll(filepath.Join(dir, "product"), 0700); err != nil {
			return zero, err
		}
		if err = copyRegular(opts.Manifest, filepath.Join(dir, "product", "manifest.json"), 0644); err != nil {
			return zero, err
		}
		if copiedHash, e := hashFile(filepath.Join(dir, "product", "manifest.json")); e != nil || copiedHash != manifestHash {
			return zero, errors.New("manifest changed while preparing image")
		}
	}
	// Dockerfile FROM sha256:<image ID> is parsed as a repository tag. Give the
	// resolved image a private, unique local name instead of resolving remotely.
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return zero, err
	}
	localBaseTag := "cellbox-local-base:tmp-" + hex.EncodeToString(nonce[:])
	if _, err = b.runner.Run(ctx, []string{"image", "tag", base.ID, localBaseTag}); err != nil {
		return zero, fmt.Errorf("tag local base image: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, cleanupErr := b.runner.Run(cleanupCtx, []string{"image", "rm", localBaseTag})
		cancel()
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove temporary base tag: %w", cleanupErr))
		}
	}()
	// Fixed instructions prevent payload or base image names from injecting Dockerfile commands.
	dockerfile := "FROM " + localBaseTag + "\nLABEL " + ManagedLabel + "=\"" + ImageVersion + "\" " + IdentityLabel + "=\"" + key + "\"\nCOPY --chmod=0755 guest /opt/cellbox/bin/cellbox-guest\n"
	if opts.ProductDir != "" || opts.Manifest != "" {
		dockerfile += "COPY product/ /opt/product/\n"
	}
	if err = os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0600); err != nil {
		return zero, err
	}
	args := []string{"build", "--quiet", "--pull=false", "--tag", tag}
	if opts.Platform != "" {
		args = append(args, "--platform", opts.Platform)
	}
	args = append(args, dir)
	if _, err = b.runner.Run(ctx, args); err != nil {
		return zero, fmt.Errorf("build image: %w", err)
	}
	built, err := b.inspect(ctx, tag)
	if err != nil {
		return zero, err
	}
	if built.Config.Labels[ManagedLabel] != ImageVersion || built.Config.Labels[IdentityLabel] != key {
		return zero, errors.New("built image labels do not match composition")
	}
	result.ImageID = built.ID
	return result, nil
}

func validPlatform(s string) bool {
	p := strings.Split(s, "/")
	if len(p) < 2 || len(p) > 3 {
		return false
	}
	for _, part := range p {
		if part == "" {
			return false
		}
		for _, r := range part {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-') {
				return false
			}
		}
	}
	return true
}
func hashFile(path string) (string, error) {
	linkInfo, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !linkInfo.Mode().IsRegular() {
		return "", errors.New("not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("not a regular file")
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func validateGuest(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("guest must be an executable regular file")
	}
	f, err := elf.Open(path)
	if err != nil {
		return fmt.Errorf("guest must be Linux ELF: %w", err)
	}
	defer f.Close()
	if f.Type != elf.ET_EXEC && f.Type != elf.ET_DYN {
		return errors.New("guest is not executable ELF")
	}
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			return errors.New("guest must be statically linked")
		}
	}
	return nil
}
func hashTree(root string) (string, map[string]os.FileMode, error) {
	files := map[string]os.FileMode{}
	if root == "" {
		return "", files, nil
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", nil, err
	}
	if !info.IsDir() {
		return "", nil, errors.New("product path is not a directory")
	}
	h := sha256.New()
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink forbidden: %s", rel)
		}
		if !d.IsDir() && !d.Type().IsRegular() {
			return fmt.Errorf("special file forbidden: %s", rel)
		}
		mode := d.Type()
		if info, err := d.Info(); err == nil {
			mode = info.Mode()
		} else {
			return err
		}
		if mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
			return fmt.Errorf("special mode forbidden: %s", rel)
		}
		files[rel] = mode
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	for _, rel := range sortedKeys(files) {
		if files[rel].IsDir() {
			fmt.Fprintf(h, "D%s\x00%04o\n", rel, files[rel].Perm())
			continue
		}
		fh, err := hashFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(h, "F%s\x00%04o\x00%s\n", rel, files[rel].Perm(), fh)
	}
	return hex.EncodeToString(h.Sum(nil)), files, nil
}
func sortedKeys(m map[string]os.FileMode) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func copyRegular(src, dst string, mode os.FileMode) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("source is not a regular file")
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err = io.Copy(out, f); err != nil {
		return err
	}
	if err = out.Chmod(mode.Perm()); err != nil {
		return err
	}
	return out.Sync()
}
