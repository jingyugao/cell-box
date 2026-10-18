package cellbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// MaxBufferedFileSize is the SDK Read buffer limit and the API Write limit.
// Open streams downloads without this size limit.
const MaxBufferedFileSize = 16 << 20

// Files accesses the sandbox's current workspace. File operations do not resume
// suspended sandboxes and the API does not accept a generation precondition.
type Files struct{ sandbox *Sandbox }

type FileEntry struct {
	Name      string `json:"name"`
	Directory bool   `json:"directory"`
	Size      int64  `json:"size"`
}

func (f *Files) path(name string, list bool) (string, error) {
	if list && (name == "" || name == ".") {
		name = "."
	} else {
		if name == "" || len(name) > 4096 || strings.ContainsRune(name, 0) {
			return "", fmt.Errorf("cellbox: invalid workspace-relative file path")
		}
		for _, part := range strings.Split(name, "/") {
			if part == "" || part == "." || part == ".." {
				return "", fmt.Errorf("cellbox: file path must be relative without empty, dot or parent segments")
			}
		}
	}
	path, err := resourcePath("/v1/boxes/", f.sandbox.ID)
	if err != nil {
		return "", err
	}
	q := url.Values{"path": {name}}
	if list {
		q.Set("list", "1")
	}
	return path + "/files?" + q.Encode(), nil
}

// Open opens a download stream. The caller must close it to release the server's
// IO lease. The context and HTTP client timeout apply until the stream is closed.
func (f *Files) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	path, err := f.path(name, false)
	if err != nil {
		return nil, err
	}
	res, err := f.sandbox.client.request(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// Read returns raw file bytes up to MaxBufferedFileSize. Use Open for larger files.
func (f *Files) Read(ctx context.Context, name string) ([]byte, error) {
	body, err := f.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, MaxBufferedFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("cellbox: read file: %w", err)
	}
	if len(data) > MaxBufferedFileSize {
		return nil, fmt.Errorf("cellbox: file exceeds %d-byte buffer limit; use Files.Open", MaxBufferedFileSize)
	}
	return data, nil
}

// Write overwrites a file with raw bytes and agent ownership. Parent directories
// must exist. Writes are not atomic or automatically retried; an interrupted
// write may leave a partial or empty file. The server rejects symlink traversal.
func (f *Files) Write(ctx context.Context, name string, data []byte) error {
	if len(data) > MaxBufferedFileSize {
		return fmt.Errorf("cellbox: file exceeds %d-byte write limit", MaxBufferedFileSize)
	}
	path, err := f.path(name, false)
	if err != nil {
		return err
	}
	res, err := f.sandbox.client.request(ctx, http.MethodPut, path, bytes.NewReader(data), "application/octet-stream")
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	return err
}

// List lists direct children of a directory. Empty name or "." selects the
// workspace root. Symlinks and special files are omitted by the server.
func (f *Files) List(ctx context.Context, name string) ([]FileEntry, error) {
	path, err := f.path(name, true)
	if err != nil {
		return nil, err
	}
	var entries []FileEntry
	err = f.sandbox.client.do(ctx, http.MethodGet, path, nil, &entries)
	return entries, err
}
