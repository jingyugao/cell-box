package node

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/archivelimits"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/objectstorage"
	"golang.org/x/sys/unix"
)

const (
	workspaceArchiveEntries = 100000
	workspaceArchiveDepth   = 64
	workspaceMount          = "/home/agent"
)

var workspaceArchiveID = regexp.MustCompile(`^arc-[0-9a-f]{32}$`)

type wireLimitWriter struct {
	w     io.Writer
	n     int64
	limit int64
}

func (w *wireLimitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.n {
		return 0, errors.New("workspace archive exceeds wire limit")
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func configuredWorkspace(r *api.ResumablePod) (string, error) {
	args := r.Spec.Container.Args
	for i := range args {
		if args[i] != "--config-base64" || i+1 >= len(args) {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(args[i+1])
		if err != nil {
			return "", errors.New("invalid embedded guest configuration")
		}
		var config guestapi.Config
		if err = json.Unmarshal(data, &config); err != nil {
			return "", errors.New("invalid embedded guest configuration")
		}
		if !filepath.IsAbs(config.Workspace) || filepath.Clean(config.Workspace) != config.Workspace || config.Workspace == workspaceMount || !strings.HasPrefix(config.Workspace, workspaceMount+"/") {
			return "", errors.New("configured workspace is outside persistent HOME")
		}
		rel := strings.TrimPrefix(config.Workspace, workspaceMount+"/")
		if rel == "" || path.Clean(rel) != rel || rel == "." || strings.HasPrefix(rel, "../") {
			return "", errors.New("invalid configured workspace path")
		}
		return filepath.Join(rel), nil
	}
	return "", errors.New("embedded guest configuration is missing")
}

func (b *Backend) CaptureWorkspaceArchive(ctx context.Context, r *api.ResumablePod, request api.ArchiveCaptureRequest) (api.ArchiveCaptureResult, error) {
	result := api.ArchiveCaptureResult{ID: request.ID, Snapshot: request.Snapshot}
	if b.Objects == nil {
		return result, errors.New("object storage is required for suspended workspace archives")
	}
	if !workspaceArchiveID.MatchString(request.ID) {
		return result, errors.New("invalid workspace archive identifier")
	}
	if r == nil || r.Status.Phase != "Suspended" || r.Spec.DesiredState != "Suspended" || !r.Spec.PersistentHome ||
		r.Status.Snapshot == "" || r.Status.Snapshot != request.Snapshot || request.Snapshot == "" || r.Status.PodUID != "" || r.Status.PodName != "" || r.DeletionTimestamp != nil {
		return result, errors.New("workspace archive source is not a suspended persistent HOME")
	}
	workspace, err := configuredWorkspace(r)
	if err != nil {
		return result, err
	}
	home, err := homevolume.Verify(b.Base, string(r.UID), r.Spec.NodeName, r.Status.SpecHash, "")
	if err != nil {
		return result, err
	}
	if err = homevolume.ValidateCheckpoint(b.Base, string(r.UID), home.ID, r.Status.Snapshot); err != nil {
		return result, err
	}
	key := "archives/" + request.ID + ".tar.gz"
	if existing, _, _, getErr := b.Objects.Get(ctx, key); getErr == nil {
		_ = existing.Close()
		return b.existingWorkspaceArchive(ctx, key, request.ID, request.Snapshot)
	} else if !errors.Is(getErr, objectstorage.ErrNotFound) {
		return result, retryableStorage(getErr)
	}
	if !request.ExpiresAt.IsZero() && !time.Now().Before(request.ExpiresAt) {
		return result, errors.New("workspace archive request expired")
	}
	root, err := homevolume.Path(b.Base, string(r.UID))
	if err != nil {
		return result, err
	}
	workspaceDir, err := openWorkspaceRoot(root, workspace)
	if err != nil {
		return result, errors.New("configured persistent workspace directory is unavailable")
	}
	defer workspaceDir.Close()
	archiveDir := filepath.Join(b.Base, "workloads", string(r.UID))
	if err = os.MkdirAll(archiveDir, 0700); err != nil {
		return result, err
	}
	f, err := os.CreateTemp(archiveDir, ".workspace-archive-*")
	if err != nil {
		return result, err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return result, err
	}
	h := sha256.New()
	wire := &wireLimitWriter{w: io.MultiWriter(f, h), limit: archivelimits.MaxWireBytes}
	gz := gzip.NewWriter(wire)
	tw := tar.NewWriter(gz)
	count := 0
	var total int64
	err = addWorkspaceDirectory(ctx, tw, workspaceDir, "", 0, &count, &total)
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = f.Sync()
	}
	stat, statErr := f.Stat()
	if err == nil {
		err = statErr
	}
	if err == nil && (stat.Size() < 1 || stat.Size() > archivelimits.MaxWireBytes) {
		err = errors.New("workspace archive exceeds wire limit")
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return result, err
	}
	size := stat.Size()
	digest := hex.EncodeToString(h.Sum(nil))
	_, err = b.Objects.Put(ctx, key, f, size, "*")
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if errors.Is(err, objectstorage.ErrConflict) {
		return b.existingWorkspaceArchive(ctx, key, request.ID, request.Snapshot)
	}
	if err != nil {
		return result, retryableStorage(err)
	}
	result.Size, result.SHA256 = size, digest
	return result, nil
}

func (b *Backend) existingWorkspaceArchive(ctx context.Context, key, id, snapshot string) (api.ArchiveCaptureResult, error) {
	result := api.ArchiveCaptureResult{ID: id, Snapshot: snapshot}
	body, size, _, err := b.Objects.Get(ctx, key)
	if err != nil {
		return result, retryableStorage(err)
	}
	defer body.Close()
	if size < 1 || size > archivelimits.MaxWireBytes {
		return result, errors.New("existing workspace archive has invalid size")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(body, size+1))
	if err != nil || n != size {
		return result, errors.New("existing workspace archive is incomplete")
	}
	result.Size, result.SHA256 = size, hex.EncodeToString(h.Sum(nil))
	return result, nil
}

func openWorkspaceRoot(homePath, rel string) (*os.File, error) {
	if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, errors.New("invalid persistent workspace path")
	}
	fd, err := unix.Open(homePath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, component := range strings.Split(rel, string(filepath.Separator)) {
		if component == "" || component == "." || component == ".." {
			unix.Close(fd)
			return nil, errors.New("invalid persistent workspace component")
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), filepath.Join(homePath, rel)), nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func addWorkspaceDirectory(ctx context.Context, tw *tar.Writer, dir *os.File, rel string, depth int, count *int, total *int64) error {
	if depth > workspaceArchiveDepth {
		return errors.New("workspace archive nesting too deep")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	remaining := workspaceArchiveEntries - *count
	entries, err := dir.ReadDir(remaining + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) > remaining {
		return errors.New("too many workspace archive entries")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		*count++
		if *count > workspaceArchiveEntries {
			return errors.New("too many workspace archive entries")
		}
		name := entry.Name()
		child := name
		if rel != "" {
			child = rel + "/" + name
		}
		if name == "." || name == ".." || path.Clean(child) != child || strings.HasPrefix(child, "../") {
			return errors.New("unsafe workspace archive path")
		}
		var entryStat unix.Stat_t
		if err = unix.Fstatat(int(dir.Fd()), name, &entryStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if entryStat.Mode&unix.S_IFMT == unix.S_IFLNK || (entryStat.Mode&unix.S_IFMT != unix.S_IFDIR && entryStat.Mode&unix.S_IFMT != unix.S_IFREG) {
			continue
		}
		if entryStat.Mode&unix.S_IFMT == unix.S_IFDIR {
			fd, openErr := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr != nil {
				return openErr
			}
			subdir := os.NewFile(uintptr(fd), name)
			var openedStat unix.Stat_t
			if openErr = unix.Fstat(fd, &openedStat); openErr != nil || openedStat.Dev != entryStat.Dev || openedStat.Ino != entryStat.Ino {
				subdir.Close()
				return errors.New("workspace directory changed during archive capture")
			}
			if err = tw.WriteHeader(&tar.Header{Name: child + "/", Mode: int64(entryStat.Mode & 0777), Typeflag: tar.TypeDir}); err != nil {
				subdir.Close()
				return err
			}
			if err = addWorkspaceDirectory(ctx, tw, subdir, child, depth+1, count, total); err != nil {
				subdir.Close()
				return err
			}
			subdir.Close()
			continue
		}
		if entryStat.Size < 0 || entryStat.Size > archivelimits.MaxEntryBytes || entryStat.Size > archivelimits.MaxContentBytes-*total {
			return errors.New("workspace exceeds archive limits")
		}
		fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		file := os.NewFile(uintptr(fd), name)
		var openedStat unix.Stat_t
		err = unix.Fstat(fd, &openedStat)
		if err != nil || openedStat.Mode&unix.S_IFMT != unix.S_IFREG || openedStat.Dev != entryStat.Dev || openedStat.Ino != entryStat.Ino || openedStat.Size != entryStat.Size {
			file.Close()
			return fmt.Errorf("workspace file changed during archive capture")
		}
		if err = tw.WriteHeader(&tar.Header{Name: child, Mode: int64(openedStat.Mode & 0777), Typeflag: tar.TypeReg, Size: openedStat.Size}); err == nil {
			_, err = io.CopyN(tw, contextReader{ctx: ctx, r: file}, openedStat.Size)
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		*total += openedStat.Size
	}
	return nil
}
