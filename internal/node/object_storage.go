// SPDX-License-Identifier: Apache-2.0

package node

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/objectstorage"
	runtimebackend "cellbox.local/cellbox/internal/runtime"
)

const (
	maxCheckpointArchiveBytes = int64(64 << 30)
	maxCheckpointFiles        = 4096
	maxCheckpointManifest     = int64(4 << 20)
)

func checkpointObjectKey(owner, snapshot string) (string, error) {
	if !ValidID(owner) || !ValidID(snapshot) {
		return "", fmt.Errorf("invalid storage identifier")
	}
	return "checkpoints/" + owner + "/" + snapshot + ".tar", nil
}

func (b *Backend) uploadSnapshot(ctx context.Context, r *api.ResumablePod, snapshotPath string) error {
	if b.Objects == nil {
		return nil
	}
	key, err := checkpointObjectKey(string(r.UID), r.Status.Snapshot)
	if err != nil {
		return err
	}
	if _, err = VerifyCached(snapshotPath, string(r.UID), r.Status.SpecHash, b.Runsc); err != nil {
		return err
	}
	b.durableMu.Lock()
	durable := b.durableSnapshots[string(r.UID)+"/"+r.Status.Snapshot]
	b.durableMu.Unlock()
	if durable {
		return nil
	}
	if body, _, _, getErr := b.Objects.Get(ctx, key); getErr == nil {
		_ = body.Close()
		b.rememberDurable(r)
		b.clearForgetSuccess(r)
		return nil
	} else if !errors.Is(getErr, objectstorage.ErrNotFound) {
		return retryableStorage(getErr)
	}
	manifest, err := readManifest(snapshotPath)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(manifest.Files)+1)
	names = append(names, "manifest.json")
	for name := range manifest.Files {
		if !safeCheckpointName(name) {
			return fmt.Errorf("unsafe manifest filename")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > maxCheckpointFiles {
		return fmt.Errorf("checkpoint contains too many files")
	}
	ownerPath := filepath.Join(b.Base, "workloads", string(r.UID))
	if err = os.MkdirAll(ownerPath, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(ownerPath, ".checkpoint-upload-*.tar")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	tw := tar.NewWriter(tmp)
	var total int64
	for _, name := range names {
		path := filepath.Join(snapshotPath, name)
		info, statErr := os.Lstat(path)
		if statErr != nil {
			err = statErr
			break
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxCheckpointArchiveBytes-total {
			err = fmt.Errorf("invalid or oversized checkpoint file")
			break
		}
		mode := int64(0600)
		header := &tar.Header{Name: name, Mode: mode, Size: info.Size(), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		if err = tw.WriteHeader(header); err != nil {
			break
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			err = openErr
			break
		}
		written, copyErr := io.CopyN(tw, file, info.Size())
		closeErr := file.Close()
		if copyErr != nil {
			err = copyErr
			break
		}
		if closeErr != nil {
			err = closeErr
			break
		}
		if written != info.Size() {
			err = io.ErrUnexpectedEOF
			break
		}
		total += written
	}
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = tmp.Sync()
	}
	if err != nil {
		tmp.Close()
		return err
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		return err
	}
	stat, err := os.Stat(tmpPath)
	if err != nil {
		tmp.Close()
		return err
	}
	if stat.Size() > maxCheckpointArchiveBytes {
		tmp.Close()
		return fmt.Errorf("checkpoint archive exceeds size limit")
	}
	_, err = b.Objects.Put(ctx, key, tmp, stat.Size(), "")
	if err != nil {
		err = retryableStorage(err)
	} else {
		b.rememberDurable(r)
		b.clearForgetSuccess(r)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	return err
}

func retryableStorage(err error) error {
	if errors.Is(err, objectstorage.ErrUnavailable) {
		return fmt.Errorf("%w: %w", runtimebackend.ErrRetryableStorage, err)
	}
	return err
}

func (b *Backend) downloadSnapshot(ctx context.Context, r *api.ResumablePod, finalPath string) error {
	if b.Objects == nil {
		return fmt.Errorf("snapshot is not cached and object storage is unavailable")
	}
	key, err := checkpointObjectKey(string(r.UID), r.Status.Snapshot)
	if err != nil {
		return err
	}
	body, size, _, err := b.Objects.Get(ctx, key)
	if err != nil {
		return retryableStorage(err)
	}
	defer body.Close()
	if size < 0 || size > maxCheckpointArchiveBytes {
		return fmt.Errorf("checkpoint archive exceeds size limit")
	}
	ownerPath := filepath.Join(b.Base, "workloads", string(r.UID))
	if err = os.MkdirAll(ownerPath, 0700); err != nil {
		return err
	}
	archive, err := os.CreateTemp(ownerPath, ".checkpoint-download-*.tar")
	if err != nil {
		return err
	}
	archivePath := archive.Name()
	defer os.Remove(archivePath)
	writer := &archiveFileWriter{file: archive}
	written, copyErr := io.Copy(writer, io.LimitReader(body, maxCheckpointArchiveBytes+1))
	if writer.err != nil {
		copyErr = writer.err
	} else if copyErr != nil {
		copyErr = retryableTransportRead(copyErr)
	}
	if copyErr == nil && written > maxCheckpointArchiveBytes {
		copyErr = fmt.Errorf("checkpoint archive exceeds size limit")
	}
	if copyErr == nil && size != written {
		copyErr = retryableTransportRead(io.ErrUnexpectedEOF)
	}
	if copyErr == nil {
		copyErr = archive.Sync()
	}
	if closeErr := archive.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return copyErr
	}
	stage := finalPath + ".cache-pending"
	if err = os.MkdirAll(filepath.Dir(finalPath), 0700); err != nil {
		return err
	}
	if err = os.RemoveAll(stage); err != nil {
		return err
	}
	if err = os.Mkdir(stage, 0700); err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err = extractCheckpointArchive(archivePath, stage); err != nil {
		return err
	}
	if _, err = Verify(stage, string(r.UID), r.Status.SpecHash, b.Runsc); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(finalPath), 0700); err != nil {
		return err
	}
	if _, err = os.Lstat(finalPath); err == nil {
		_, verifyErr := Verify(finalPath, string(r.UID), r.Status.SpecHash, b.Runsc)
		return verifyErr
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Rename(stage, finalPath); err != nil {
		return err
	}
	if err = SyncDir(filepath.Dir(finalPath)); err != nil {
		return err
	}
	b.rememberDurable(r)
	return nil
}

type archiveFileWriter struct {
	file *os.File
	err  error
}

func (w *archiveFileWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

func retryableTransportRead(err error) error {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return err
	}
	var networkErr net.Error
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkErr) {
		return fmt.Errorf("%w: %w", runtimebackend.ErrRetryableStorage, err)
	}
	return err
}

func readManifest(path string) (*Manifest, error) {
	info, err := os.Lstat(filepath.Join(path, "manifest.json"))
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxCheckpointManifest {
		return nil, fmt.Errorf("invalid checkpoint manifest")
	}
	var manifest Manifest
	if err = ReadJSON(filepath.Join(path, "manifest.json"), &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func safeCheckpointName(name string) bool {
	return name != "" && name != "manifest.json" && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\\x00\r\n") && name != "." && name != ".."
}

func extractCheckpointArchive(archivePath, destination string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	seen := map[string]bool{}
	var total int64
	for {
		header, nextErr := tr.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return nextErr
		}
		name := header.Name
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return fmt.Errorf("checkpoint archive contains non-file entry")
		}
		if header.Size < 0 || header.Size > maxCheckpointArchiveBytes-total || len(seen) >= maxCheckpointFiles {
			return fmt.Errorf("checkpoint archive exceeds limits")
		}
		if name != "manifest.json" && !safeCheckpointName(name) {
			return fmt.Errorf("unsafe checkpoint archive filename")
		}
		if seen[name] {
			return fmt.Errorf("duplicate checkpoint archive entry")
		}
		seen[name] = true
		out, createErr := os.OpenFile(filepath.Join(destination, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if createErr != nil {
			return createErr
		}
		written, copyErr := io.CopyN(out, tr, header.Size)
		if copyErr == nil && written != header.Size {
			copyErr = io.ErrUnexpectedEOF
		}
		if copyErr == nil {
			copyErr = out.Sync()
		}
		if closeErr := out.Close(); copyErr == nil {
			copyErr = closeErr
		}
		if copyErr != nil {
			return copyErr
		}
		total += written
	}
	if !seen["manifest.json"] {
		return fmt.Errorf("checkpoint manifest missing from archive")
	}
	manifest, err := readManifest(destination)
	if err != nil {
		return err
	}
	if len(manifest.Files)+1 != len(seen) {
		return fmt.Errorf("checkpoint archive contents differ from manifest")
	}
	for name := range manifest.Files {
		if !safeCheckpointName(name) || !seen[name] {
			return fmt.Errorf("checkpoint archive contents differ from manifest")
		}
	}
	return SyncDir(destination)
}

func (b *Backend) rememberDurable(r *api.ResumablePod) {
	b.durableMu.Lock()
	defer b.durableMu.Unlock()
	if b.durableSnapshots == nil {
		b.durableSnapshots = map[string]bool{}
	}
	b.durableSnapshots[string(r.UID)+"/"+r.Status.Snapshot] = true
}
