package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
)

const recordPrefix = "metadata/records"
const profilePrefix = "metadata/profiles"
const resultPrefix = "metadata/results"

// The head is a commit record for ONLY the latest transaction, not a ledger.
// Its conditional PUT is the commit point and fences previous API leaders.
// Materialization is idempotent, so a crash after committing any subset of the
// objects is recovered before the next writer accepts requests.
type recordHead struct {
	Schema   int           `json:"schema"`
	Revision string        `json:"revision"`
	Changes  []imageChange `json:"changes,omitempty"`
}

type resourceRecords struct {
	Revision       string `json:"revision"`
	ParentRevision string `json:"parentRevision,omitempty"`
	Deleted        bool   `json:"deleted,omitempty"`
	State          *State `json:"state,omitempty"`
}

type recordStore struct {
	initialized  bool
	headRevision string
	data         map[string][]byte
	etags        map[string]string
	profiles     map[string]Profile
	orphans      map[string]time.Time
}

func recordKey(kind, id string) string {
	digest := sha256.Sum256([]byte(id))
	return recordPrefix + "/" + kind + "/" + hex.EncodeToString(digest[:]) + ".json"
}

func validRecordKey(key string) bool {
	if strings.HasPrefix(key, "images/") {
		parts := strings.Split(key, "/")
		return len(parts) == 3 && imageObjectID.MatchString(parts[1]) && parts[2] == "metadata.json"
	}
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0]+"/"+parts[1] != recordPrefix {
		return false
	}
	if parts[2] != "boxes" && parts[2] != "operations" && parts[2] != "archives" && parts[2] != "keys" {
		return false
	}
	name := strings.TrimSuffix(parts[3], ".json")
	decoded, err := hex.DecodeString(name)
	return err == nil && len(decoded) == 32 && name+".json" == parts[3]
}

func decodeRecordHead(data []byte) (recordHead, error) {
	var head recordHead
	if json.Unmarshal(data, &head) != nil || head.Schema != stateSchema || head.Revision == "" {
		return head, errors.New("invalid core metadata head")
	}
	seen := map[string]bool{}
	for _, change := range head.Changes {
		if !validRecordKey(change.Key) || seen[change.Key] {
			return head, errors.New("invalid core metadata change")
		}
		seen[change.Key] = true
		var header struct {
			Revision string `json:"revision"`
			Deleted  bool   `json:"deleted"`
		}
		if json.Unmarshal(change.Data, &header) != nil || header.Revision != head.Revision || header.Deleted != change.Delete {
			return head, errors.New("invalid core metadata revision")
		}
	}
	return head, nil
}

func materializeRecords(ctx context.Context, objects objectstorage.Objects, changes []imageChange, replay bool) (map[string]string, error) {
	versions := map[string]string{}
	for _, change := range changes {
		version := change.BeforeETag
		if replay {
			actual, current, err := readObject(ctx, objects, change.Key)
			if errors.Is(err, objectstorage.ErrNotFound) {
				if version != "" {
					return nil, errors.New("committed metadata object disappeared")
				}
			} else if err != nil {
				return nil, err
			} else if bytes.Equal(actual, change.Data) {
				versions[change.Key] = current
				continue
			} else if current != version {
				// A box-local CAS may supersede an already materialized head.
				// Its parent is the exact head it observed, so replay must not
				// roll that newer resource back to the global transaction.
				var descendant resourceRecords
				var committed resourceRecords
				if !strings.HasPrefix(change.Key, recordPrefix+"/boxes/") || json.Unmarshal(actual, &descendant) != nil || json.Unmarshal(change.Data, &committed) != nil || descendant.ParentRevision != committed.Revision || descendant.Revision == "" {
					return nil, objectstorage.ErrConflict
				}
				versions[change.Key] = current
				continue
			}
		}
		// Deleted records become small tombstones. Physically removing the key
		// would let a delayed create from an old leader pass If-None-Match and
		// resurrect the resource after a newer leader had already deleted it.
		updated, err := writeObject(ctx, objects, change.Key, change.Data, version)
		if err != nil {
			return nil, err
		}
		versions[change.Key] = updated
	}
	return versions, nil
}

// OpenObjectStore reads resource-sized metadata and recovers the last committed
// transaction. Only the current schema is supported; old ledgers are rejected.
func OpenObjectStore(ctx context.Context, objects objectstorage.Objects) (*Store, error) {
	data, etag, err := readObject(ctx, objects, stateObjectKey)
	s := &Store{objects: objects, etag: etag, state: newState(), records: &recordStore{data: map[string][]byte{}, etags: map[string]string{}, profiles: map[string]Profile{}}}
	if errors.Is(err, objectstorage.ErrNotFound) {
		// A missing head is a fresh installation only when there is no existing
		// metadata. Never adopt orphaned resources or an old pending ledger.
		for _, prefix := range []string{"metadata", "images"} {
			keys, err := objects.List(ctx, prefix)
			if err != nil {
				return nil, err
			}
			for _, key := range keys {
				if prefix == "metadata" || strings.HasSuffix(key, "/metadata.json") {
					return nil, errors.New("metadata head missing; use an empty object storage prefix")
				}
			}
		}
		if err := s.Update(func(*State) error { return nil }); err != nil {
			return nil, err
		}
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	head, err := decodeRecordHead(data)
	if err != nil {
		return nil, err
	}
	if _, err = materializeRecords(ctx, objects, head.Changes, true); err != nil {
		return nil, err
	}
	s.records.initialized = true
	keys, err := objects.List(ctx, recordPrefix)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if !validRecordKey(key) {
			return nil, errors.New("invalid metadata object key")
		}
		data, version, err := readObject(ctx, objects, key)
		if err != nil {
			return nil, err
		}
		var record resourceRecords
		if json.Unmarshal(data, &record) != nil || record.Revision == "" {
			return nil, errors.New("invalid resource metadata")
		}
		s.records.etags[key] = version
		if record.Deleted {
			if record.State != nil {
				return nil, errors.New("deleted resource contains state")
			}
			continue
		}
		if record.State == nil || record.State.Schema != stateSchema {
			return nil, errors.New("invalid resource metadata state")
		}
		st := record.State
		for id, box := range st.Boxes {
			profile, err := s.loadProfile(ctx, box.Profile.ID)
			if err != nil {
				return nil, err
			}
			box.Profile = profile
			box.Box.Capabilities = profileCapabilities(profile)
			if profile.Provider == "resumable-k8s-pod" && box.Box.Phase == "" {
				box.Box.Phase = "unknown"
			}
			st.Boxes[id] = box
		}
		if err := mergeRecordState(&s.state, *st); err != nil {
			return nil, err
		}
		s.records.etags[key] = version
	}
	if err := s.loadImageObjects(ctx); err != nil {
		return nil, err
	}
	// Cache canonical projections, so unrelated reads and volatile changes
	// cause no writes at all.
	projected, err := s.projectRecords(ctx, &s.state)
	if err != nil {
		return nil, err
	}
	s.records.data = projected
	// LIST and GET do not constitute a multi-object snapshot. If another leader
	// committed during loading, refuse to serve a mixed view.
	_, current, err := readObject(ctx, objects, stateObjectKey)
	if err != nil {
		return nil, err
	}
	if current != etag {
		return nil, objectstorage.ErrConflict
	}
	// Claim a fresh head even when startup has no state changes. Otherwise a
	// replaced process could still commit using the head this instance loaded.
	s.records.initialized = false
	if err := s.Update(func(*State) error { return nil }); err != nil {
		return nil, err
	}
	if err := s.claimBoxRecords(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func mergeMap[T any](dst map[string]T, src map[string]T) error {
	for key, value := range src {
		if _, exists := dst[key]; exists {
			return errors.New("duplicate core metadata record")
		}
		dst[key] = value
	}
	return nil
}
func mergeRecordState(dst *State, src State) error {
	for _, err := range []error{mergeMap(dst.Boxes, src.Boxes), mergeMap(dst.Operations, src.Operations), mergeMap(dst.Keys, src.Keys), mergeMap(dst.Executions, src.Executions), mergeMap(dst.Leases, src.Leases), mergeMap(dst.Archives, src.Archives)} {
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) loadProfile(ctx context.Context, digest string) (Profile, error) {
	if profile, ok := s.records.profiles[digest]; ok {
		return profile, nil
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return Profile{}, errors.New("invalid immutable profile reference")
	}
	data, _, err := readObject(ctx, s.objects, profilePrefix+"/"+digest+".json")
	if err != nil {
		return Profile{}, err
	}
	sum := sha256.Sum256(data)
	var profile Profile
	if hex.EncodeToString(sum[:]) != digest || json.Unmarshal(data, &profile) != nil {
		return profile, errors.New("invalid immutable profile")
	}
	s.records.profiles[digest] = profile
	return profile, nil
}

func (s *Store) profileReference(ctx context.Context, profile Profile) (string, error) {
	data, err := json.Marshal(profile)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if _, exists := s.records.profiles[digest]; !exists {
		if _, err := writeObject(ctx, s.objects, profilePrefix+"/"+digest+".json", data, ""); err != nil {
			return "", err
		}
		s.records.profiles[digest] = profile
	}
	return digest, nil
}

func (s *Store) projectRecords(ctx context.Context, state *State) (map[string][]byte, error) {
	groups := map[string]*State{}
	group := func(key string) *State {
		if groups[key] == nil {
			st := newState()
			groups[key] = &st
		}
		return groups[key]
	}
	operationKey := func(op operationRecord) string {
		if _, exists := state.Boxes[op.Operation.TargetID]; exists {
			return recordKey("boxes", op.Operation.TargetID)
		}
		return recordKey("operations", op.Operation.ID)
	}
	for id, box := range state.Boxes {
		digest, err := s.profileReference(ctx, box.Profile)
		if err != nil {
			return nil, err
		}
		box.Profile = Profile{ID: digest}
		box.Box.Capabilities = Capabilities{}
		if state.Boxes[id].Profile.Provider == "resumable-k8s-pod" {
			// Intent and recovery barriers remain durable; observed status comes
			// from the CR. Never erase a deletion or incomplete restore fence.
			switch box.Box.Phase {
			case "running", "suspended", "creating", "resuming", "staged", "unknown":
				box.Box.Phase = ""
			}
			box.Box.Generation = 0
			box.ExecutionID = ""
			box.Box.Version = 0
			box.Box.Error = nil
		}
		group(recordKey("boxes", id)).Boxes[id] = box
	}
	for id, op := range state.Operations {
		group(operationKey(op)).Operations[id] = op
	}
	for id, key := range state.Keys {
		objectKey := recordKey("keys", id)
		if op, exists := state.Operations[key.OperationID]; exists {
			objectKey = operationKey(op)
		}
		group(objectKey).Keys[id] = key
	}
	for id, execution := range state.Executions {
		if execution.Result != nil {
			data, err := json.Marshal(execution.Result)
			if err != nil {
				return nil, err
			}
			key := executionResultKey(execution.ID, data)
			if execution.ResultObject != key {
				if _, err := writeObject(ctx, s.objects, key, data, ""); err != nil {
					return nil, err
				}
			}
			execution.ResultObject = key
			execution.Result = nil
			state.Executions[id] = execution
		}
		group(recordKey("boxes", execution.BoxID)).Executions[id] = execution
	}
	for id, lease := range state.Leases {
		group(recordKey("boxes", lease.BoxID)).Leases[id] = lease
	}
	for id, archive := range state.Archives {
		group(recordKey("archives", id)).Archives[id] = archive
	}
	out := map[string][]byte{}
	for key, st := range groups {
		data, err := json.Marshal(st)
		if err != nil {
			return nil, err
		}
		out[key] = data
	}
	for id, record := range state.ImportedImages {
		key, err := imageMetadataKey(id)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		out[key] = data
	}
	return out, nil
}

func (s *Store) persistRecords(next State) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	projected, err := s.projectRecords(ctx, &next)
	if err != nil {
		return err
	}
	revision := randomID("rev-")
	keys := map[string]bool{}
	for key := range s.records.data {
		keys[key] = true
	}
	for key := range projected {
		keys[key] = true
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	head := recordHead{Schema: stateSchema, Revision: revision}
	for _, key := range ordered {
		before, existed := s.records.data[key]
		after, exists := projected[key]
		if existed == exists && bytes.Equal(before, after) {
			continue
		}
		var data []byte
		if strings.HasPrefix(key, "images/") {
			record := storedImage{Revision: revision, Deleted: !exists}
			if exists {
				record.Record = &importedImageRecord{}
				if err = json.Unmarshal(after, record.Record); err != nil {
					return err
				}
			}
			data, err = json.Marshal(record)
		} else {
			record := resourceRecords{Revision: revision, Deleted: !exists}
			if exists {
				record.State = &State{}
				if err = json.Unmarshal(after, record.State); err != nil {
					return err
				}
			}
			data, err = json.Marshal(record)
		}
		if err != nil {
			return err
		}
		head.Changes = append(head.Changes, imageChange{Key: key, BeforeETag: s.records.etags[key], Data: data, Delete: !exists})
	}
	if len(head.Changes) == 0 && s.records.initialized {
		return nil
	}
	data, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(data) > stateObjectLimit {
		return errors.New("core metadata transaction exceeds limit")
	}
	etag, err := writeObject(ctx, s.objects, stateObjectKey, data, s.etag)
	if err != nil {
		return err
	}
	versions, err := materializeRecords(ctx, s.objects, head.Changes, false)
	if err != nil {
		return err
	}
	for _, change := range head.Changes {
		delete(s.records.etags, change.Key)
	}
	for key, version := range versions {
		s.records.etags[key] = version
	}
	s.records.data = projected
	s.etag = etag
	s.records.headRevision = revision
	s.records.initialized = true
	return nil
}

func (s *Store) executionResult(ctx context.Context, execution executionRecord) (Execution, error) {
	if execution.ResultObject != "" {
		if !strings.HasPrefix(execution.ResultObject, resultPrefix+"/") {
			return Execution{}, errors.New("invalid execution result reference")
		}
		data, _, err := readObject(ctx, s.objects, execution.ResultObject)
		if err != nil {
			return Execution{}, err
		}
		if len(data) > 4<<20 {
			return Execution{}, errors.New("execution result exceeds limit")
		}
		if executionResultKey(execution.ID, data) != execution.ResultObject {
			return Execution{}, errors.New("execution result identity mismatch")
		}
		var result guestapi.ExecResult
		if err := json.Unmarshal(data, &result); err != nil {
			return Execution{}, err
		}
		execution.Result = &result
		execution.ResultObject = ""
	}
	if execution.State == "expired" {
		return Execution{}, apiError("RESULT_EXPIRED", "Execution output retention window has expired")
	}
	return execution.Execution, nil
}

func executionResultKey(id string, data []byte) string {
	sum := sha256.Sum256(append([]byte(id+"\x00"), data...))
	return resultPrefix + "/" + hex.EncodeToString(sum[:]) + ".json"
}
