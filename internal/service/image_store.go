package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/objectstorage"
)

const pendingObjectKey = "metadata/pending.json"

var errStaleTransaction = errors.New("metadata transaction was superseded")

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
type objectTransaction struct {
	Revision   string          `json:"revision"`
	BeforeETag string          `json:"beforeETag"`
	State      json.RawMessage `json:"state"`
	Images     []imageChange   `json:"images"`
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
	return data, etag, nil
}
func writeObject(ctx context.Context, objects objectstorage.Objects, key string, data []byte, before string) (string, error) {
	condition := before
	if condition == "" {
		condition = "*"
	}
	etag, err := objects.Put(ctx, key, bytes.NewReader(data), int64(len(data)), condition)
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

// The API's leader and local mutex serialize transactions. A small durable
// intent makes image/operation updates recoverable across process failure;
// images themselves are independently versioned objects.
func applyObjectTransaction(ctx context.Context, objects objectstorage.Objects, tx objectTransaction) (string, error) {
	current, etag, err := readObject(ctx, objects, stateObjectKey)
	if errors.Is(err, objectstorage.ErrNotFound) {
		current = nil
		etag = ""
	} else if err != nil {
		return "", err
	}
	committed := bytes.Equal(current, tx.State)
	if !committed && etag != tx.BeforeETag {
		return "", errStaleTransaction
	}
	deleteVersions := map[string]string{}
	for _, change := range tx.Images {
		actual, version, err := readObject(ctx, objects, change.Key)
		if errors.Is(err, objectstorage.ErrNotFound) {
			if committed && change.Delete {
				continue
			}
			if change.BeforeETag != "" {
				return "", objectstorage.ErrConflict
			}
			version = ""
		} else if err != nil {
			return "", err
		}
		if bytes.Equal(actual, change.Data) {
			if change.Delete {
				deleteVersions[change.Key] = version
			}
			continue
		}
		if version != change.BeforeETag {
			return "", objectstorage.ErrConflict
		}
		updated, err := writeObject(ctx, objects, change.Key, change.Data, version)
		if err != nil {
			return "", err
		}
		if change.Delete {
			deleteVersions[change.Key] = updated
		}
	}
	if !committed {
		etag, err = writeObject(ctx, objects, stateObjectKey, tx.State, tx.BeforeETag)
		if err != nil {
			return "", err
		}
	}
	for _, change := range tx.Images {
		if change.Delete && deleteVersions[change.Key] != "" {
			if err = objects.DeleteIfMatch(ctx, change.Key, deleteVersions[change.Key]); err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
				return "", err
			}
		}
	}
	return etag, nil
}
func decodeTransaction(data []byte) (objectTransaction, error) {
	var tx objectTransaction
	if err := json.Unmarshal(data, &tx); err != nil || tx.Revision == "" || len(tx.State) == 0 {
		return tx, errors.New("invalid pending metadata transaction")
	}
	var state State
	if err := json.Unmarshal(tx.State, &state); err != nil || state.Revision != tx.Revision || state.Schema != 2 || len(state.ImportedImages) > 0 {
		return tx, errors.New("invalid pending service ledger")
	}
	seen := map[string]bool{}
	for _, change := range tx.Images {
		parts := strings.Split(change.Key, "/")
		if len(parts) != 3 || parts[0] != "images" || parts[2] != "metadata.json" || !imageObjectID.MatchString(parts[1]) || seen[change.Key] {
			return tx, errors.New("invalid image transaction key")
		}
		seen[change.Key] = true
		var image storedImage
		if err := json.Unmarshal(change.Data, &image); err != nil || image.Revision != tx.Revision || image.Deleted != change.Delete || !image.Deleted && (image.Record == nil || image.Record.ImportedImage.ID != parts[1]) {
			return tx, errors.New("invalid image transaction data")
		}
	}
	return tx, nil
}
func recoverObjectTransaction(ctx context.Context, objects objectstorage.Objects) error {
	data, journalETag, err := readObject(ctx, objects, pendingObjectKey)
	if errors.Is(err, objectstorage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	tx, err := decodeTransaction(data)
	if err != nil {
		return err
	}
	if _, err = applyObjectTransaction(ctx, objects, tx); err != nil && !errors.Is(err, errStaleTransaction) {
		return err
	}
	return objects.DeleteIfMatch(ctx, pendingObjectKey, journalETag)
}
func (s *Store) loadImageObjects(ctx context.Context) error {
	keys, err := s.objects.List(ctx, "images")
	if err != nil {
		return err
	}
	s.imageEtags = map[string]string{}
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
		if stored.Deleted {
			continue
		}
		if stored.Record == nil || stored.Record.ImportedImage.ID != parts[1] {
			return errors.New("image metadata identity mismatch")
		}
		s.state.ImportedImages[parts[1]] = *stored.Record
		s.imageEtags[parts[1]] = etag
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
func (s *Store) persistRemoteLocked(next State) error {
	next.Revision = randomID("rev-")
	ledger := next
	ledger.ImportedImages = nil
	serialized, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(serialized, &fields); err != nil {
		return err
	}
	delete(fields, "importedImages")
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	if len(data) > stateObjectLimit {
		return errors.New("service ledger exceeds object storage limit")
	}
	tx := objectTransaction{Revision: next.Revision, BeforeETag: s.etag, State: data}
	ids := map[string]bool{}
	for id := range s.state.ImportedImages {
		ids[id] = true
	}
	for id := range next.ImportedImages {
		ids[id] = true
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, id := range ordered {
		before, was := s.state.ImportedImages[id]
		after, exists := next.ImportedImages[id]
		if was == exists && reflect.DeepEqual(before, after) {
			continue
		}
		key, err := imageMetadataKey(id)
		if err != nil {
			return err
		}
		stored := storedImage{Revision: tx.Revision, Deleted: !exists}
		if exists {
			stored.Record = &after
		}
		document, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		tx.Images = append(tx.Images, imageChange{Key: key, BeforeETag: s.imageEtags[id], Data: document, Delete: !exists})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Check the ledger before acquiring the intent slot. A stale process must
	// never begin overwriting independently stored image metadata.
	_, current, err := readObject(ctx, s.objects, stateObjectKey)
	if errors.Is(err, objectstorage.ErrNotFound) {
		current = ""
		err = nil
	}
	if err != nil || current != s.etag {
		return s.failRemote()
	}
	journal, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	if len(journal) > stateObjectLimit {
		return errors.New("metadata transaction exceeds storage limit")
	}
	journalETag, err := writeObject(ctx, s.objects, pendingObjectKey, journal, "")
	if err != nil {
		return s.failRemote()
	}
	etag, err := applyObjectTransaction(ctx, s.objects, tx)
	if err != nil {
		if errors.Is(err, errStaleTransaction) {
			_ = s.objects.DeleteIfMatch(ctx, pendingObjectKey, journalETag)
		}
		return s.failRemote()
	}
	// Read the committed image versions before clearing the recoverable intent.
	for _, change := range tx.Images {
		id := strings.Split(change.Key, "/")[1]
		if change.Delete {
			delete(s.imageEtags, id)
			continue
		}
		_, version, err := readObject(ctx, s.objects, change.Key)
		if err != nil {
			return s.failRemote()
		}
		s.imageEtags[id] = version
	}
	if err = s.objects.DeleteIfMatch(ctx, pendingObjectKey, journalETag); err != nil {
		return s.failRemote()
	}
	s.etag = etag
	s.state = next
	return nil
}
