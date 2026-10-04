package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"

	"cellbox.local/cellbox/internal/objectstorage"
)

var imageObjectID = regexp.MustCompile(`^img-[a-f0-9]{32}$`)

type storedImage struct {
	Revision string               `json:"revision"`
	Deleted  bool                 `json:"deleted,omitempty"`
	Record   *importedImageRecord `json:"record,omitempty"`
}
type imageChange struct {
	Key        string          `json:"key"`
	BeforeETag string          `json:"beforeETag"`
	Data       json.RawMessage `json:"data"`
	Delete     bool            `json:"delete,omitempty"`
}

func imageMetadataKey(id string) (string, error) {
	if !imageObjectID.MatchString(id) {
		return "", errors.New("invalid imported image ID")
	}
	return "images/" + id + "/metadata.json", nil
}
func readObject(ctx context.Context, objects objectstorage.Objects, key string) ([]byte, string, error) {
	body, size, etag, err := objects.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer body.Close()
	if size < 1 || size > stateObjectLimit || etag == "" {
		return nil, "", errors.New("invalid metadata object")
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) != size {
		return nil, "", errors.New("metadata object size mismatch")
	}
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		compressed, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, "", err
		}
		data, err = io.ReadAll(io.LimitReader(compressed, stateObjectLimit+1))
		closeErr := compressed.Close()
		if err != nil {
			return nil, "", err
		}
		if closeErr != nil {
			return nil, "", closeErr
		}
		if len(data) == 0 || int64(len(data)) > stateObjectLimit {
			return nil, "", errors.New("invalid expanded metadata object size")
		}
	}
	return data, etag, nil
}
func writeObject(ctx context.Context, objects objectstorage.Objects, key string, data []byte, before string) (string, error) {
	if len(data) == 0 || int64(len(data)) > stateObjectLimit {
		return "", errors.New("invalid metadata object size")
	}
	wire := data
	// Operation/idempotency histories contain repeated JSON keys and IDs.
	// Compress larger objects so their growth does not add network round trips
	// to each lifecycle commit. Small metadata stays directly readable JSON.
	if len(data) >= 16<<10 {
		var buffer bytes.Buffer
		compressor, _ := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
		if _, err := compressor.Write(data); err != nil {
			return "", err
		}
		if err := compressor.Close(); err != nil {
			return "", err
		}
		if buffer.Len() < len(data) {
			wire = buffer.Bytes()
		}
	}
	condition := before
	if condition == "" {
		condition = "*"
	}
	etag, err := objects.Put(ctx, key, bytes.NewReader(wire), int64(len(wire)), condition)
	if err == nil && etag != "" {
		return etag, nil
	}
	// Accept an ambiguous response only when our exact revision was committed.
	actual, current, getErr := readObject(ctx, objects, key)
	if getErr == nil && bytes.Equal(actual, data) {
		return current, nil
	}
	if err == nil {
		err = errors.New("object storage did not return an ETag")
	}
	return "", err
}

func (s *Store) loadImageObjects(ctx context.Context) error {
	keys, err := s.objects.List(ctx, "images")
	if err != nil {
		return err
	}
	for _, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) != 3 || parts[2] != "metadata.json" {
			continue
		}
		expected, err := imageMetadataKey(parts[1])
		if err != nil || expected != key {
			return errors.New("invalid imported image metadata key")
		}
		data, etag, err := readObject(ctx, s.objects, key)
		if err != nil {
			return err
		}
		var stored storedImage
		if err = json.Unmarshal(data, &stored); err != nil || stored.Revision == "" {
			return errors.New("invalid imported image metadata")
		}
		s.records.etags[key] = etag
		if stored.Deleted {
			continue
		}
		if stored.Record == nil || stored.Record.ImportedImage.ID != parts[1] {
			return errors.New("image metadata identity mismatch")
		}
		s.state.ImportedImages[parts[1]] = *stored.Record
	}
	return nil
}
func (s *Store) failRemote() error {
	s.failure = errors.New("metadata write failed; API stopped to avoid stale state")
	if s.onFailure != nil {
		s.onFailure()
	}
	return s.failure
}
