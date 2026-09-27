package kv

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/surya-mp/go-rebac"
)

const (
	stateKey             = "go-rebac/state"
	tupleKeyPrefix       = "go-rebac/tuple/"
	subjectKeyPrefix     = "go-rebac/subject/"
	modelKeyPrefix       = "go-rebac/model/"
	activeModelKeyPrefix = "go-rebac/model-active/"
	tupleHistoryPrefix   = "go-rebac/tuple-history/"
	activeHistoryPrefix  = "go-rebac/model-active-history/"
	revisionKeyPrefix    = "go-rebac/revision/"
	resourceKeyPrefix    = "go-rebac/resource/"
	// ponytail: bounded local history; use a backend-native MVCC adapter for
	// longer or higher-throughput exact snapshots.
	defaultHistoryRevisions   = 1_000
	defaultTransactionRetries = 3
	// ponytail: retain 1,000 events; add configurable or disk-segmented retention only when needed.
	maxWatchHistory = 1_000
	watchBuffer     = 16
)

var ErrWatchOverflow = errors.New("rebac/kv: watch consumer fell behind")

var _ rebac.ProductionStorage = (*ReBACStore)(nil)

// ReBACStore is an embedded, versioned relationship and model store. It
// retains the latest 1,000 tuple events for resumable watches.
type ReBACStore struct {
	db                 Backend
	historyRevisions   uint64
	transactionRetries int
	retryBackoff       time.Duration
	mu                 sync.Mutex
	watchers           map[int]watcher
	nextWatch          int
}

type watcher struct {
	tenant     string
	namespaces map[string]struct{}
	global     bool
	tenants    map[string]struct{}
	events     chan rebac.WatchEvent
	errs       chan error
}

type rebacState struct {
	Revision     uint64                           `json:"revision"`
	HistoryAfter uint64                           `json:"history_after,omitempty"`
	TupleLayout  int                              `json:"tuple_layout,omitempty"`
	ModelLayout  int                              `json:"model_layout,omitempty"`
	WatchAfter   uint64                           `json:"watch_after,omitempty"`
	Tuples       []rebac.RelationTuple            `json:"tuples"`
	Models       map[string][]rebac.ModelDocument `json:"models"`
	ActiveModels map[string]rebac.Revision        `json:"active_models,omitempty"`
	Events       []rebac.WatchEvent               `json:"events,omitempty"`
	GlobalEvents []rebac.WatchEvent               `json:"global_events,omitempty"`
	GlobalAfter  uint64                           `json:"global_after,omitempty"`
	ByObject     map[string][]int                 `json:"by_object,omitempty"`
	BySubject    map[string][]int                 `json:"by_subject,omitempty"`
	Resources    map[string][]string              `json:"resources,omitempty"`
	Subjects     map[string][]string              `json:"subjects,omitempty"`
	Deleted      map[string]bool                  `json:"deleted,omitempty"`
}

type tupleHistoryRecord struct {
	Delete bool                `json:"delete,omitempty"`
	Tuple  rebac.RelationTuple `json:"tuple"`
}

type modelChange struct {
	tenant, id string
	document   *rebac.ModelDocument
	active     *rebac.Revision
}

// ReBACStoreOptions controls bounded local retention.
type ReBACStoreOptions struct {
	// HistoryRevisions is the number of recent exact revisions retained. Zero
	// selects the safe default of 1,000.
	HistoryRevisions uint64
	// KeyPrefix is prepended to every record written to the supplied backend.
	// It isolates independent ReBACStore instances sharing that backend.
	KeyPrefix string
	// TransactionRetries is the number of retries after a transaction
	// conflict. Zero selects the safe default of three; a negative value
	// disables retries.
	TransactionRetries int
	// RetryBackoff waits this long between transaction-conflict retries. Zero
	// retries immediately.
	RetryBackoff time.Duration
}

// NewReBACStore uses an application-supplied transactional KV backend.
func NewReBACStore(db Backend) *ReBACStore {
	return NewReBACStoreWithOptions(db, ReBACStoreOptions{})
}

// NewReBACStoreWithOptions uses an application-supplied backend with bounded
// record-history retention.
func NewReBACStoreWithOptions(db Backend, options ReBACStoreOptions) *ReBACStore {
	if options.HistoryRevisions == 0 {
		options.HistoryRevisions = defaultHistoryRevisions
	}
	retries := options.TransactionRetries
	if retries == 0 {
		retries = defaultTransactionRetries
	}
	if retries < 0 {
		retries = 0
	}
	return &ReBACStore{db: withPrefix(db, options.KeyPrefix), historyRevisions: options.HistoryRevisions, transactionRetries: retries, retryBackoff: options.RetryBackoff, watchers: make(map[int]watcher)}
}

func (s *ReBACStore) shouldRetry(ctx context.Context, attempt int, err error) bool {
	if !errors.Is(err, ErrTransactionConflict) || attempt >= s.transactionRetries {
		return false
	}
	if s.retryBackoff <= 0 {
		return true
	}
	timer := time.NewTimer(s.retryBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *ReBACStore) state(ctx context.Context) (rebacState, error) {
	return s.stateAt(ctx, "")
}

func (s *ReBACStore) stateAt(ctx context.Context, revision rebac.Revision) (rebacState, error) {
	snap, err := s.db.Snapshot(ctx)
	if err != nil {
		return rebacState{}, err
	}
	defer snap.Close()
	if revision != "" {
		version, err := revisionNumber(revision)
		if err != nil {
			return rebacState{}, err
		}
		if raw, found, err := snap.Get(ctx, stateKey); err != nil {
			return rebacState{}, err
		} else if found {
			var head rebacState
			if err := json.Unmarshal(raw, &head); err != nil {
				return rebacState{}, err
			}
			if version <= head.HistoryAfter {
				return rebacState{}, rebac.ErrInvalidRevision
			}
		}
		if _, found, err := snap.Get(ctx, revisionKey(revision)); err != nil {
			return rebacState{}, err
		} else if found {
			state := rebacState{Revision: version, TupleLayout: 1, ModelLayout: 1, Models: map[string][]rebac.ModelDocument{}, ActiveModels: map[string]rebac.Revision{}, Deleted: map[string]bool{}}
			if state.Tuples, err = readTupleHistory(ctx, snap, version); err != nil {
				return rebacState{}, err
			}
			if state.Models, err = readModelRecordsAt(ctx, snap, version); err != nil {
				return rebacState{}, err
			}
			if state.ActiveModels, err = readActiveModelHistory(ctx, snap, version); err != nil {
				return rebacState{}, err
			}
			state.index()
			return state, nil
		}
		// Legacy stores retained whole-state snapshots. Keep them readable until
		// the host's own retention policy removes them.
		key := stateKey + "/" + string(revision)
		raw, ok, err := snap.Get(ctx, key)
		if err != nil || !ok {
			if err == nil {
				err = rebac.ErrInvalidRevision
			}
			return rebacState{Models: map[string][]rebac.ModelDocument{}, ActiveModels: map[string]rebac.Revision{}}, err
		}
		var state rebacState
		if err := json.Unmarshal(raw, &state); err != nil {
			return rebacState{}, err
		}
		if revision != rebac.Revision(strconv.FormatUint(state.Revision, 10)) {
			return rebacState{}, rebac.ErrInvalidRevision
		}
		if state.Models == nil {
			state.Models = map[string][]rebac.ModelDocument{}
		}
		if state.ActiveModels == nil {
			state.ActiveModels = map[string]rebac.Revision{}
		}
		if state.Deleted == nil {
			state.Deleted = make(map[string]bool)
		}
		state.index()
		return state, nil
	}
	raw, ok, err := snap.Get(ctx, stateKey)
	if err != nil || !ok {
		return rebacState{Models: map[string][]rebac.ModelDocument{}, ActiveModels: map[string]rebac.Revision{}}, err
	}
	var state rebacState
	if err := json.Unmarshal(raw, &state); err != nil {
		return rebacState{}, err
	}
	if state.Models == nil {
		state.Models = map[string][]rebac.ModelDocument{}
	}
	if state.ActiveModels == nil {
		state.ActiveModels = map[string]rebac.Revision{}
	}
	if state.Deleted == nil {
		state.Deleted = make(map[string]bool)
	}
	if state.TupleLayout == 1 {
		state.Tuples, err = readTupleRecords(ctx, snap, rebac.RelationTuple{})
		if err != nil {
			return rebacState{}, err
		}
	}
	if state.ModelLayout == 1 {
		state.Models, err = readModelRecords(ctx, snap)
		if err != nil {
			return rebacState{}, err
		}
		state.ActiveModels, err = readActiveModelRecords(ctx, snap)
		if err != nil {
			return rebacState{}, err
		}
	}
	if state.ByObject == nil {
		state.index()
	}
	return state, nil
}

func keyPart(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }

func tupleRecordKey(tuple rebac.RelationTuple) (string, error) {
	identity := struct {
		TenantID, Namespace, ObjectID, Relation, User, Caveat string
		Context                                               rebac.CaveatContext
	}{tuple.TenantID, tuple.Namespace, tuple.ObjectID, tuple.Relation, tuple.User, tuple.Caveat, tuple.CaveatContext}
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return tupleKeyPrefix + keyPart(tuple.TenantID) + "/" + keyPart(tuple.Namespace) + "/" + keyPart(tuple.ObjectID) + "/" + keyPart(tuple.Relation) + "/" + base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func subjectRecordKey(tuple rebac.RelationTuple) (string, error) {
	key, err := tupleRecordKey(tuple)
	if err != nil {
		return "", err
	}
	return subjectKeyPrefix + keyPart(tuple.TenantID) + "/" + keyPart(tuple.User) + "/" + key[len(tupleKeyPrefix):], nil
}

func modelRecordKey(tenant, id string, version rebac.Revision) string {
	return modelKeyPrefix + keyPart(tenant) + "/" + keyPart(id) + "/" + keyPart(string(version))
}

func activeModelRecordKey(tenant, id string) string {
	return activeModelKeyPrefix + keyPart(tenant) + "/" + keyPart(id)
}

func revisionKey(revision rebac.Revision) string { return revisionKeyPrefix + string(revision) }

func revisionPart(revision uint64) string { return fmt.Sprintf("%020d", revision) }

func tupleHistoryKey(tuple rebac.RelationTuple, revision uint64) (string, error) {
	key, err := tupleRecordKey(tuple)
	if err != nil {
		return "", err
	}
	return tupleHistoryPrefix + keyPart(key) + "/" + revisionPart(revision), nil
}

func activeHistoryKey(tenant, id string, revision uint64) string {
	return activeHistoryPrefix + keyPart(tenant) + "/" + keyPart(id) + "/" + revisionPart(revision)
}

func resourceRecordKey(tuple rebac.RelationTuple) string {
	return resourceKeyPrefix + keyPart(tuple.TenantID) + "/" + keyPart(tuple.Namespace) + "/" + keyPart(tuple.ObjectID)
}

func readModelRecords(ctx context.Context, reader Reader) (map[string][]rebac.ModelDocument, error) {
	keys, err := reader.Ascend(ctx, modelKeyPrefix)
	if err != nil {
		return nil, err
	}
	models := make(map[string][]rebac.ModelDocument)
	for _, key := range keys {
		raw, ok, err := reader.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(key, modelKeyPrefix), "/")
		if len(parts) != 3 {
			return nil, errors.New("rebac/kv: invalid model key")
		}
		tenant, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
		id, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
		var document rebac.ModelDocument
		if err := json.Unmarshal(raw, &document); err != nil {
			return nil, err
		}
		models[string(tenant)+"/"+string(id)] = append(models[string(tenant)+"/"+string(id)], document)
	}
	for key := range models {
		sort.Slice(models[key], func(i, j int) bool {
			left, _ := revisionNumber(models[key][i].Version)
			right, _ := revisionNumber(models[key][j].Version)
			return left < right
		})
	}
	return models, nil
}

func readActiveModelRecords(ctx context.Context, reader Reader) (map[string]rebac.Revision, error) {
	keys, err := reader.Ascend(ctx, activeModelKeyPrefix)
	if err != nil {
		return nil, err
	}
	active := make(map[string]rebac.Revision, len(keys))
	for _, key := range keys {
		raw, ok, err := reader.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(key, activeModelKeyPrefix), "/")
		if len(parts) != 2 {
			return nil, errors.New("rebac/kv: invalid active model key")
		}
		tenant, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
		id, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
		active[string(tenant)+"/"+string(id)] = rebac.Revision(raw)
	}
	return active, nil
}

func readModelRecordsAt(ctx context.Context, reader Reader, revision uint64) (map[string][]rebac.ModelDocument, error) {
	models, err := readModelRecords(ctx, reader)
	if err != nil {
		return nil, err
	}
	for key, documents := range models {
		out := documents[:0]
		for _, document := range documents {
			version, err := revisionNumber(document.Version)
			if err != nil {
				return nil, err
			}
			if version <= revision {
				out = append(out, document)
			}
		}
		models[key] = out
	}
	return models, nil
}

func readActiveModelHistory(ctx context.Context, reader Reader, revision uint64) (map[string]rebac.Revision, error) {
	keys, err := reader.Ascend(ctx, activeHistoryPrefix)
	if err != nil {
		return nil, err
	}
	active := make(map[string]rebac.Revision)
	for _, key := range keys {
		parts := strings.Split(strings.TrimPrefix(key, activeHistoryPrefix), "/")
		if len(parts) != 3 {
			return nil, errors.New("rebac/kv: invalid active model history key")
		}
		at, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return nil, err
		}
		if at > revision {
			continue
		}
		raw, ok, err := reader.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		tenant, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
		id, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
		active[string(tenant)+"/"+string(id)] = rebac.Revision(raw)
	}
	return active, nil
}

func readTupleHistory(ctx context.Context, reader Reader, revision uint64) ([]rebac.RelationTuple, error) {
	keys, err := reader.Ascend(ctx, tupleHistoryPrefix)
	if err != nil {
		return nil, err
	}
	type entry struct {
		at     uint64
		record tupleHistoryRecord
	}
	latest := make(map[string]entry)
	for _, key := range keys {
		parts := strings.Split(strings.TrimPrefix(key, tupleHistoryPrefix), "/")
		if len(parts) != 2 {
			return nil, errors.New("rebac/kv: invalid tuple history key")
		}
		at, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		if at > revision || (latest[parts[0]].at >= at) {
			continue
		}
		raw, ok, err := reader.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		var record tupleHistoryRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, err
		}
		latest[parts[0]] = entry{at: at, record: record}
	}
	identities := make([]string, 0, len(latest))
	for identity := range latest {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	tuples := make([]rebac.RelationTuple, 0, len(identities))
	for _, identity := range identities {
		if record := latest[identity].record; !record.Delete {
			tuples = append(tuples, record.Tuple)
		}
	}
	return tuples, nil
}

func tupleRecordPrefix(filter rebac.RelationTuple) string {
	if filter.TenantID == "" {
		return tupleKeyPrefix
	}
	prefix := tupleKeyPrefix + keyPart(filter.TenantID) + "/"
	for _, value := range []string{filter.Namespace, filter.ObjectID, filter.Relation} {
		if value == "" {
			return prefix
		}
		prefix += keyPart(value) + "/"
	}
	return prefix
}

func readTupleRecords(ctx context.Context, reader Reader, filter rebac.RelationTuple) ([]rebac.RelationTuple, error) {
	prefix := tupleRecordPrefix(filter)
	if filter.TenantID != "" && filter.User != "" && (filter.Namespace == "" || filter.ObjectID == "") {
		prefix = subjectKeyPrefix + keyPart(filter.TenantID) + "/" + keyPart(filter.User) + "/"
	}
	keys, err := reader.Ascend(ctx, prefix)
	if err != nil {
		return nil, err
	}
	tuples := make([]rebac.RelationTuple, 0, len(keys))
	for _, key := range keys {
		raw, ok, err := reader.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		var tuple rebac.RelationTuple
		if err := json.Unmarshal(raw, &tuple); err != nil {
			return nil, err
		}
		if matches(tuple, filter) {
			tuples = append(tuples, tuple)
		}
	}
	return tuples, nil
}

func objectKey(tenant, namespace, objectID string) string {
	return tenant + "\x00" + namespace + "\x00" + objectID
}

func namespaceKey(tenant, namespace string) string { return tenant + "\x00" + namespace }

func subjectKey(tenant, subject string) string { return tenant + "\x00" + subject }

func (s *rebacState) index() {
	s.ByObject = make(map[string][]int)
	s.BySubject = make(map[string][]int)
	resourceSets := make(map[string]map[string]struct{})
	subjectSets := make(map[string]map[string]struct{})
	for index, tuple := range s.Tuples {
		s.ByObject[objectKey(tuple.TenantID, tuple.Namespace, tuple.ObjectID)] = append(s.ByObject[objectKey(tuple.TenantID, tuple.Namespace, tuple.ObjectID)], index)
		s.BySubject[subjectKey(tuple.TenantID, tuple.User)] = append(s.BySubject[subjectKey(tuple.TenantID, tuple.User)], index)
		resources := resourceSets[namespaceKey(tuple.TenantID, tuple.Namespace)]
		if resources == nil {
			resources = make(map[string]struct{})
			resourceSets[namespaceKey(tuple.TenantID, tuple.Namespace)] = resources
		}
		resources[tuple.ObjectID] = struct{}{}
		subjects := subjectSets[tuple.TenantID]
		if subjects == nil {
			subjects = make(map[string]struct{})
			subjectSets[tuple.TenantID] = subjects
		}
		subjects[tuple.User] = struct{}{}
	}
	s.Resources = make(map[string][]string, len(resourceSets))
	for key, resources := range resourceSets {
		s.Resources[key] = sorted(resources)
	}
	s.Subjects = make(map[string][]string, len(subjectSets))
	for tenant, subjects := range subjectSets {
		s.Subjects[tenant] = sorted(subjects)
	}
}
func (s *ReBACStore) save(ctx context.Context, state rebacState, changes []rebac.TupleChange, modelChanges ...modelChange) (rebac.Revision, error) {
	tx, err := s.db.Transaction(ctx)
	if err != nil {
		return "", err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if raw, found, err := tx.Get(ctx, stateKey); err != nil {
		return "", err
	} else if found {
		var current rebacState
		if err := json.Unmarshal(raw, &current); err != nil {
			return "", err
		}
		if current.Revision != state.Revision {
			return "", ErrTransactionConflict
		}
	} else if state.Revision != 0 {
		return "", ErrTransactionConflict
	}
	nextRevision := state.Revision + 1
	if state.TupleLayout == 0 {
		// Migrate legacy state on its next write without a separate rewrite step.
		for _, tuple := range state.Tuples {
			changes = append(changes, rebac.TupleChange{Operation: rebac.WriteOperation, Tuple: tuple})
		}
	}
	for _, change := range changes {
		key, err := tupleRecordKey(change.Tuple)
		if err != nil {
			return "", err
		}
		subjectKey, err := subjectRecordKey(change.Tuple)
		if err != nil {
			return "", err
		}
		historyKey, err := tupleHistoryKey(change.Tuple, nextRevision)
		if err != nil {
			return "", err
		}
		history, err := json.Marshal(tupleHistoryRecord{Delete: change.Operation == rebac.DeleteOperation, Tuple: change.Tuple})
		if err != nil {
			return "", err
		}
		if err := tx.Set(ctx, historyKey, history); err != nil {
			return "", err
		}
		if change.Operation == rebac.DeleteOperation {
			if err := tx.Delete(ctx, key); err != nil {
				return "", err
			}
			if err := tx.Delete(ctx, subjectKey); err != nil {
				return "", err
			}
			if !hasObjectTuple(state.Tuples, change.Tuple) {
				if err := tx.Delete(ctx, resourceRecordKey(change.Tuple)); err != nil {
					return "", err
				}
			}
			continue
		}
		raw, err := json.Marshal(change.Tuple)
		if err != nil {
			return "", err
		}
		if err := tx.Set(ctx, key, raw); err != nil {
			return "", err
		}
		if err := tx.Set(ctx, subjectKey, raw); err != nil {
			return "", err
		}
		// ponytail: resource markers are a durable, complete superset; compact
		// stale markers only when their retention becomes material.
		if err := tx.Set(ctx, resourceRecordKey(change.Tuple), nil); err != nil {
			return "", err
		}
	}
	if state.ModelLayout == 0 {
		modelChanges = nil // migration writes the complete legacy model state once.
		for key, documents := range state.Models {
			tenant, id, ok := strings.Cut(key, "/")
			if !ok {
				return "", errors.New("rebac/kv: invalid model identity")
			}
			for index := range documents {
				modelChanges = append(modelChanges, modelChange{tenant: tenant, id: id, document: &documents[index]})
			}
		}
		for key, version := range state.ActiveModels {
			tenant, id, ok := strings.Cut(key, "/")
			if !ok {
				return "", errors.New("rebac/kv: invalid active model identity")
			}
			active := version
			modelChanges = append(modelChanges, modelChange{tenant: tenant, id: id, active: &active})
		}
	}
	for _, change := range modelChanges {
		if change.document != nil {
			raw, err := json.Marshal(*change.document)
			if err != nil {
				return "", err
			}
			if err := tx.Set(ctx, modelRecordKey(change.tenant, change.id, change.document.Version), raw); err != nil {
				return "", err
			}
		}
		if change.active != nil {
			if err := tx.Set(ctx, activeModelRecordKey(change.tenant, change.id), []byte(*change.active)); err != nil {
				return "", err
			}
			if err := tx.Set(ctx, activeHistoryKey(change.tenant, change.id, nextRevision), []byte(*change.active)); err != nil {
				return "", err
			}
		}
	}
	state.Revision = nextRevision
	if nextRevision > s.historyRevisions {
		baseline := nextRevision - s.historyRevisions
		if baseline > state.HistoryAfter {
			if err := compactHistory(ctx, tx, baseline); err != nil {
				return "", err
			}
			state.HistoryAfter = baseline
		}
	}
	state.TupleLayout = 1
	state.ModelLayout = 1
	current := state
	current.Tuples = nil
	current.ByObject = nil
	current.BySubject = nil
	current.Resources = nil
	current.Subjects = nil
	current.Models = nil
	current.ActiveModels = nil
	raw, err := json.Marshal(current)
	if err != nil {
		return "", err
	}
	if err = tx.Set(ctx, stateKey, raw); err != nil {
		return "", err
	}
	if err = tx.Set(ctx, revisionKey(currentRevision(state)), nil); err != nil {
		return "", err
	}
	_, err = tx.Commit(ctx)
	if err != nil {
		return "", err
	}
	committed = true
	return currentRevision(state), nil
}

func hasObjectTuple(tuples []rebac.RelationTuple, target rebac.RelationTuple) bool {
	for _, tuple := range tuples {
		if tuple.TenantID == target.TenantID && tuple.Namespace == target.Namespace && tuple.ObjectID == target.ObjectID {
			return true
		}
	}
	return false
}

func compactHistory(ctx context.Context, tx WriteTransaction, baseline uint64) error {
	tuples, err := readTupleHistory(ctx, tx, baseline)
	if err != nil {
		return err
	}
	active, err := readActiveModelHistory(ctx, tx, baseline)
	if err != nil {
		return err
	}
	for _, prefix := range []string{tupleHistoryPrefix, activeHistoryPrefix} {
		keys, err := tx.Ascend(ctx, prefix)
		if err != nil {
			return err
		}
		for _, key := range keys {
			at, err := historyRevision(key)
			if err != nil {
				return err
			}
			if at < baseline {
				if err := tx.Delete(ctx, key); err != nil {
					return err
				}
			}
		}
	}
	markers, err := tx.Ascend(ctx, revisionKeyPrefix)
	if err != nil {
		return err
	}
	for _, key := range markers {
		at, err := strconv.ParseUint(strings.TrimPrefix(key, revisionKeyPrefix), 10, 64)
		if err != nil {
			return err
		}
		if at <= baseline {
			if err := tx.Delete(ctx, key); err != nil {
				return err
			}
		}
	}
	for _, tuple := range tuples {
		key, err := tupleHistoryKey(tuple, baseline)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(tupleHistoryRecord{Tuple: tuple})
		if err != nil {
			return err
		}
		if err := tx.Set(ctx, key, raw); err != nil {
			return err
		}
	}
	for identity, version := range active {
		tenant, id, ok := strings.Cut(identity, "/")
		if !ok {
			return errors.New("rebac/kv: invalid active model identity")
		}
		if err := tx.Set(ctx, activeHistoryKey(tenant, id, baseline), []byte(version)); err != nil {
			return err
		}
	}
	return nil
}

func historyRevision(key string) (uint64, error) {
	_, suffix, ok := strings.Cut(key, "/")
	if !ok {
		return 0, errors.New("rebac/kv: invalid history key")
	}
	parts := strings.Split(suffix, "/")
	return strconv.ParseUint(parts[len(parts)-1], 10, 64)
}

func currentRevision(state rebacState) rebac.Revision {
	return rebac.Revision(strconv.FormatUint(state.Revision, 10))
}

func (s *rebacState) record(changes []rebac.TupleChange) []rebac.WatchEvent {
	revision := rebac.Revision(strconv.FormatUint(s.Revision+1, 10))
	byTenant := make(map[string][]rebac.TupleChange)
	for _, change := range changes {
		byTenant[change.Tuple.TenantID] = append(byTenant[change.Tuple.TenantID], change)
	}
	tenants := make([]string, 0, len(byTenant))
	for tenant := range byTenant {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	events := make([]rebac.WatchEvent, 0, len(tenants))
	for _, tenant := range tenants {
		events = append(events, rebac.WatchEvent{Revision: revision, TenantID: tenant, Changes: byTenant[tenant]})
	}
	s.Events = append(s.Events, events...)
	if len(s.Events) > maxWatchHistory {
		dropped := s.Events[:len(s.Events)-maxWatchHistory]
		s.WatchAfter, _ = strconv.ParseUint(string(dropped[len(dropped)-1].Revision), 10, 64)
		s.Events = s.Events[len(dropped):]
	}
	global := rebac.WatchEvent{Revision: revision, Changes: append([]rebac.TupleChange(nil), changes...)}
	s.GlobalEvents = append(s.GlobalEvents, global)
	if len(s.GlobalEvents) > maxWatchHistory {
		dropped := s.GlobalEvents[:len(s.GlobalEvents)-maxWatchHistory]
		s.GlobalAfter, _ = strconv.ParseUint(string(dropped[len(dropped)-1].Revision), 10, 64)
		s.GlobalEvents = s.GlobalEvents[len(s.GlobalEvents)-maxWatchHistory:]
	}
	return append(events, global)
}

func revisionNumber(revision rebac.Revision) (uint64, error) {
	if revision == "" {
		return 0, nil
	}
	number, err := strconv.ParseUint(string(revision), 10, 64)
	if err != nil {
		return 0, rebac.ErrInvalidRevision
	}
	return number, nil
}
func matches(t, f rebac.RelationTuple) bool {
	return (f.TenantID == "" || f.TenantID == t.TenantID) && (f.Namespace == "" || f.Namespace == t.Namespace) && (f.ObjectID == "" || f.ObjectID == t.ObjectID) && (f.Relation == "" || f.Relation == t.Relation) && (f.User == "" || f.User == t.User)
}

func sameTuple(a, b rebac.RelationTuple) bool {
	return a.TenantID == b.TenantID && a.Namespace == b.Namespace && a.ObjectID == b.ObjectID &&
		a.Relation == b.Relation && a.User == b.User && a.Caveat == b.Caveat && reflect.DeepEqual(a.CaveatContext, b.CaveatContext)
}

func queryState(state rebacState, filter rebac.RelationTuple) []rebac.RelationTuple {
	candidates := state.Tuples
	if filter.TenantID != "" && filter.Namespace != "" && filter.ObjectID != "" {
		indexes := state.ByObject[objectKey(filter.TenantID, filter.Namespace, filter.ObjectID)]
		candidates = make([]rebac.RelationTuple, 0, len(indexes))
		for _, index := range indexes {
			candidates = append(candidates, state.Tuples[index])
		}
	} else if filter.TenantID != "" && filter.User != "" {
		indexes := state.BySubject[subjectKey(filter.TenantID, filter.User)]
		candidates = make([]rebac.RelationTuple, 0, len(indexes))
		for _, index := range indexes {
			candidates = append(candidates, state.Tuples[index])
		}
	}
	out := make([]rebac.RelationTuple, 0, len(candidates))
	for _, tuple := range candidates {
		if matches(tuple, filter) {
			out = append(out, tuple)
		}
	}
	return out
}

func (s *ReBACStore) QueryTuples(ctx context.Context, f rebac.RelationTuple) ([]rebac.RelationTuple, error) {
	snapshot, err := s.db.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	raw, ok, err := snapshot.Get(ctx, stateKey)
	if err != nil || !ok {
		return nil, err
	}
	var state rebacState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	if state.TupleLayout == 1 {
		return readTupleRecords(ctx, snapshot, f)
	}
	state.index()
	return queryState(state, f), nil
}
func (s *ReBACStore) WriteTuple(ctx context.Context, t rebac.RelationTuple) error {
	_, e := s.WriteTupleWithRevision(ctx, t)
	return e
}
func (s *ReBACStore) WriteTupleWithRevision(ctx context.Context, t rebac.RelationTuple) (rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		st, err := s.state(ctx)
		if err != nil {
			return "", err
		}
		revived := stateRevive(&st, t)
		changes := []rebac.TupleChange{{Operation: rebac.WriteOperation, Tuple: t}}
		changed := revived
		for index, existing := range st.Tuples {
			if !sameTuple(existing, t) {
				continue
			}
			if reflect.DeepEqual(existing, t) {
				break
			}
			st.Tuples[index] = t
			changed = true
			break
		}
		if !changed {
			found := false
			for _, existing := range st.Tuples {
				if sameTuple(existing, t) {
					found = true
					break
				}
			}
			if found {
				return currentRevision(st), nil
			}
			st.Tuples = append(st.Tuples, t)
			changed = true
		}
		st.index()
		events := st.record(changes)
		revision, err := s.save(ctx, st, changes)
		if s.shouldRetry(ctx, attempt, err) {
			continue
		}
		if err == nil {
			s.publish(events)
		}
		return revision, err
	}
	return "", ErrTransactionConflict
}

func stateRevive(state *rebacState, tuple rebac.RelationTuple) bool {
	key := objectKey(tuple.TenantID, tuple.Namespace, tuple.ObjectID)
	if !state.Deleted[key] {
		return false
	}
	delete(state.Deleted, key)
	return true
}
func (s *ReBACStore) DeleteTuple(ctx context.Context, t rebac.RelationTuple) error {
	_, e := s.DeleteTupleWithRevision(ctx, t)
	return e
}
func (s *ReBACStore) DeleteTupleWithRevision(ctx context.Context, t rebac.RelationTuple) (rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		st, err := s.state(ctx)
		if err != nil {
			return "", err
		}
		out := st.Tuples[:0]
		deleted := false
		for _, x := range st.Tuples {
			if !sameTuple(x, t) {
				out = append(out, x)
			} else {
				deleted = true
			}
		}
		if !deleted {
			return currentRevision(st), nil
		}
		st.Tuples = out
		st.index()
		changes := []rebac.TupleChange{{Operation: rebac.DeleteOperation, Tuple: t}}
		events := st.record(changes)
		revision, err := s.save(ctx, st, changes)
		if s.shouldRetry(ctx, attempt, err) {
			continue
		}
		if err == nil {
			s.publish(events)
		}
		return revision, err
	}
	return "", ErrTransactionConflict
}

// DeleteObject removes every tuple defined on one object in one commit and
// leaves a tombstone for CollectGarbage.
func (s *ReBACStore) DeleteObject(ctx context.Context, tenantID, namespace, objectID string) (rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		state, err := s.state(ctx)
		if err != nil {
			return "", err
		}
		key := objectKey(tenantID, namespace, objectID)
		indexes := state.ByObject[key]
		if len(indexes) == 0 && state.Deleted[key] {
			return currentRevision(state), nil
		}
		state.Deleted[key] = true
		deleted := make(map[int]struct{}, len(indexes))
		for _, index := range indexes {
			deleted[index] = struct{}{}
		}
		changes := make([]rebac.TupleChange, 0, len(indexes))
		tuples := make([]rebac.RelationTuple, 0, len(state.Tuples)-len(indexes))
		for index, tuple := range state.Tuples {
			if _, ok := deleted[index]; ok {
				changes = append(changes, rebac.TupleChange{Operation: rebac.DeleteOperation, Tuple: tuple})
				continue
			}
			tuples = append(tuples, tuple)
		}
		state.Tuples = tuples
		state.index()
		events := state.record(changes)
		revision, err := s.save(ctx, state, changes)
		if s.shouldRetry(ctx, attempt, err) {
			continue
		}
		if err == nil {
			s.publish(events)
		}
		return revision, err
	}
	return "", ErrTransactionConflict
}

// CollectGarbage removes inbound references to at most max deleted objects.
// Call it periodically; max <= 0 does no work.
func (s *ReBACStore) CollectGarbage(ctx context.Context, max int) (int, error) {
	if max <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		state, err := s.state(ctx)
		if err != nil {
			return 0, err
		}
		keys := make([]string, 0, len(state.Deleted))
		for key, deleted := range state.Deleted {
			if deleted {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		if len(keys) > max {
			keys = keys[:max]
		}
		if len(keys) == 0 {
			return 0, nil
		}
		dangling := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			parts := strings.Split(key, "\x00")
			dangling[subjectKey(parts[0], parts[1]+":"+parts[2])] = struct{}{}
		}
		changes := make([]rebac.TupleChange, 0)
		tuples := state.Tuples[:0]
		for _, tuple := range state.Tuples {
			base, _, _ := strings.Cut(tuple.User, "#")
			if _, deleted := dangling[subjectKey(tuple.TenantID, base)]; deleted {
				changes = append(changes, rebac.TupleChange{Operation: rebac.DeleteOperation, Tuple: tuple})
				continue
			}
			tuples = append(tuples, tuple)
		}
		state.Tuples = tuples
		for _, key := range keys {
			delete(state.Deleted, key)
		}
		state.index()
		events := state.record(changes)
		_, err = s.save(ctx, state, changes)
		if s.shouldRetry(ctx, attempt, err) {
			continue
		}
		if err != nil {
			return 0, err
		}
		s.publish(events)
		return len(keys), nil
	}
	return 0, ErrTransactionConflict
}
func (s *ReBACStore) SnapshotAt(ctx context.Context, r rebac.Revision) (rebac.TupleReader, rebac.Revision, func() error, error) {
	st, e := s.stateAt(ctx, r)
	if e != nil {
		return nil, "", nil, e
	}
	cur := rebac.Revision(strconv.FormatUint(st.Revision, 10))
	return tupleView(st), cur, func() error { return nil }, nil
}

// Mutate applies tuple changes atomically under one embedded transaction.
func (s *ReBACStore) Mutate(ctx context.Context, changes []rebac.TupleChange, preconditions []rebac.Precondition) (rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		st, err := s.state(ctx)
		if err != nil {
			return "", err
		}
		for _, p := range preconditions {
			found := false
			for _, t := range st.Tuples {
				if sameTuple(t, p.Tuple) {
					found = true
					break
				}
			}
			if found != p.MustExist {
				return "", rebac.ErrPreconditionFailed
			}
		}
		effective := make([]rebac.TupleChange, 0, len(changes))
		revived := false
		for _, c := range changes {
			if c.Operation == rebac.WriteOperation {
				revived = stateRevive(&st, c.Tuple) || revived
				found := false
				for index, t := range st.Tuples {
					if sameTuple(t, c.Tuple) {
						found = true
						if !reflect.DeepEqual(t, c.Tuple) {
							st.Tuples[index] = c.Tuple
							effective = append(effective, c)
						}
						break
					}
				}
				if !found {
					st.Tuples = append(st.Tuples, c.Tuple)
					effective = append(effective, c)
				}
			} else if c.Operation == rebac.DeleteOperation {
				out := st.Tuples[:0]
				deleted := false
				for _, t := range st.Tuples {
					if !sameTuple(t, c.Tuple) {
						out = append(out, t)
					} else {
						deleted = true
					}
				}
				st.Tuples = out
				if deleted {
					effective = append(effective, c)
				}
			} else {
				return "", errors.New("rebac/kv: invalid mutation")
			}
		}
		if len(effective) == 0 && !revived {
			return currentRevision(st), nil
		}
		st.index()
		events := st.record(effective)
		rev, err := s.save(ctx, st, effective)
		if s.shouldRetry(ctx, attempt, err) {
			continue
		}
		if err == nil {
			s.publish(events)
		}
		return rev, err
	}
	return "", ErrTransactionConflict
}

func (s *ReBACStore) LookupResourceCandidates(ctx context.Context, r rebac.LookupResourcesRequest) ([]string, error) {
	snapshot, err := s.db.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	state, err := currentState(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if state.TupleLayout != 1 {
		state.index()
		return append([]string(nil), state.Resources[namespaceKey(r.TenantID, r.Namespace)]...), nil
	}
	return readResourceCandidates(ctx, snapshot, r.TenantID, r.Namespace)
}
func (s *ReBACStore) LookupSubjectCandidates(ctx context.Context, r rebac.LookupSubjectsRequest) ([]string, error) {
	snapshot, err := s.db.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	state, err := currentState(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if state.TupleLayout != 1 {
		state.index()
		return append([]string(nil), state.Subjects[r.TenantID]...), nil
	}
	return readSubjectCandidates(ctx, snapshot, r.TenantID)
}
func (s *ReBACStore) LookupResourceCandidatesAt(ctx context.Context, rev rebac.Revision, r rebac.LookupResourcesRequest) ([]string, rebac.Revision, error) {
	st, err := s.stateAt(ctx, rev)
	if err != nil {
		return nil, "", err
	}
	return append([]string(nil), st.Resources[namespaceKey(r.TenantID, r.Namespace)]...), rev, nil
}
func (s *ReBACStore) LookupSubjectCandidatesAt(ctx context.Context, rev rebac.Revision, r rebac.LookupSubjectsRequest) ([]string, rebac.Revision, error) {
	st, err := s.stateAt(ctx, rev)
	if err != nil {
		return nil, "", err
	}
	return append([]string(nil), st.Subjects[r.TenantID]...), rev, nil
}

// CandidateIndexRevision reports the revision of the synchronously maintained
// current candidate indexes.
func (s *ReBACStore) CandidateIndexRevision(ctx context.Context) (rebac.Revision, error) {
	state, err := s.state(ctx)
	if err != nil {
		return "", err
	}
	return currentRevision(state), nil
}
func sorted(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func currentState(ctx context.Context, reader Reader) (rebacState, error) {
	raw, ok, err := reader.Get(ctx, stateKey)
	if err != nil || !ok {
		return rebacState{}, err
	}
	var state rebacState
	if err := json.Unmarshal(raw, &state); err != nil {
		return rebacState{}, err
	}
	return state, nil
}

func readResourceCandidates(ctx context.Context, reader Reader, tenant, namespace string) ([]string, error) {
	prefix := resourceKeyPrefix + keyPart(tenant) + "/" + keyPart(namespace) + "/"
	keys, err := reader.Ascend(ctx, prefix)
	if err != nil {
		return nil, err
	}
	resources := make([]string, 0, len(keys))
	for _, key := range keys {
		object, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, prefix))
		if err != nil {
			return nil, err
		}
		resources = append(resources, string(object))
	}
	sort.Strings(resources)
	return resources, nil
}

func readSubjectCandidates(ctx context.Context, reader Reader, tenant string) ([]string, error) {
	keys, err := reader.Ascend(ctx, subjectKeyPrefix+keyPart(tenant)+"/")
	if err != nil {
		return nil, err
	}
	subjects := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		parts := strings.Split(strings.TrimPrefix(key, subjectKeyPrefix+keyPart(tenant)+"/"), "/")
		if len(parts) < 2 {
			return nil, errors.New("rebac/kv: invalid subject index key")
		}
		subject, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
		subjects[string(subject)] = struct{}{}
	}
	return sorted(subjects), nil
}

func (s *ReBACStore) ReadAuthorizationModel(ctx context.Context, tenant, id string, v rebac.Revision) (rebac.ModelDocument, error) {
	st, e := s.state(ctx)
	if e != nil {
		return rebac.ModelDocument{}, e
	}
	items := st.Models[tenant+"/"+id]
	for i := len(items) - 1; i >= 0; i-- {
		if v == "" || items[i].Version == v {
			return items[i], nil
		}
	}
	return rebac.ModelDocument{}, rebac.ErrModelNotFound
}
func (s *ReBACStore) ListAuthorizationModelVersions(ctx context.Context, tenant, id string) ([]rebac.ModelDocument, error) {
	st, err := s.state(ctx)
	if err != nil {
		return nil, err
	}
	items := st.Models[tenant+"/"+id]
	if len(items) == 0 {
		return nil, rebac.ErrModelNotFound
	}
	return append([]rebac.ModelDocument(nil), items...), nil
}
func (s *ReBACStore) ReadActiveAuthorizationModel(ctx context.Context, tenant, id string) (rebac.ModelDocument, error) {
	st, err := s.state(ctx)
	if err != nil {
		return rebac.ModelDocument{}, err
	}
	active := st.ActiveModels[tenant+"/"+id]
	if active == "" {
		return rebac.ModelDocument{}, rebac.ErrModelNotFound
	}
	for _, document := range st.Models[tenant+"/"+id] {
		if document.Version == active {
			return document, nil
		}
	}
	return rebac.ModelDocument{}, rebac.ErrModelNotFound
}
func (s *ReBACStore) ActivateAuthorizationModel(ctx context.Context, tenant, id string, expectedActive, version rebac.Revision) (rebac.ModelDocument, rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
retry:
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		st, err := s.state(ctx)
		if err != nil {
			return rebac.ModelDocument{}, "", err
		}
		k := tenant + "/" + id
		if st.ActiveModels[k] != expectedActive {
			return rebac.ModelDocument{}, "", rebac.ErrPreconditionFailed
		}
		for _, document := range st.Models[k] {
			if document.Version != version {
				continue
			}
			if document.State != "" && document.State != rebac.ModelPublished {
				return rebac.ModelDocument{}, "", rebac.ErrModelNotPublished
			}
			if version == expectedActive {
				return document, currentRevision(st), nil
			}
			st.ActiveModels[k] = version
			active := version
			revision, err := s.save(ctx, st, nil, modelChange{tenant: tenant, id: id, active: &active})
			if s.shouldRetry(ctx, attempt, err) {
				continue retry
			}
			return document, revision, err
		}
		return rebac.ModelDocument{}, "", rebac.ErrModelNotFound
	}
	return rebac.ModelDocument{}, "", ErrTransactionConflict
}
func (s *ReBACStore) ReadAuthorizationModelAtRevision(ctx context.Context, tenant, id string, rev rebac.Revision) (rebac.ModelDocument, error) {
	st, err := s.stateAt(ctx, rev)
	if err != nil {
		return rebac.ModelDocument{}, err
	}
	items := st.Models[tenant+"/"+id]
	if len(items) == 0 {
		return rebac.ModelDocument{}, rebac.ErrModelNotFound
	}
	if active := st.ActiveModels[tenant+"/"+id]; active != "" {
		for _, document := range items {
			if document.Version == active {
				return document, nil
			}
		}
		return rebac.ModelDocument{}, rebac.ErrModelNotFound
	}
	return items[len(items)-1], nil
}
func (s *ReBACStore) WriteAuthorizationModel(ctx context.Context, tenant string, d rebac.ModelDocument, expected rebac.Revision) (rebac.ModelDocument, error) {
	stored, _, e := s.WriteAuthorizationModelWithRevision(ctx, tenant, d, expected)
	return stored, e
}
func (s *ReBACStore) WriteAuthorizationModelWithRevision(ctx context.Context, tenant string, d rebac.ModelDocument, expected rebac.Revision) (rebac.ModelDocument, rebac.Revision, error) {
	if err := d.Validate(); err != nil {
		return d, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt <= s.transactionRetries; attempt++ {
		st, e := s.state(ctx)
		if e != nil {
			return d, "", e
		}
		k := tenant + "/" + d.ID
		items := st.Models[k]
		if (len(items) == 0 && expected != "") || (len(items) > 0 && (expected == "" || items[len(items)-1].Version != expected)) {
			return d, "", rebac.ErrPreconditionFailed
		}
		d.Version = rebac.Revision(strconv.FormatUint(st.Revision+1, 10))
		if len(items) != 0 {
			d.ParentVersion = items[len(items)-1].Version
		} else {
			d.ParentVersion = ""
		}
		d.CreatedAtUnixNano = time.Now().UnixNano()
		if d.State == "" {
			d.State = rebac.ModelPublished
		}
		d.Checksum, e = d.ComputeChecksum()
		if e != nil {
			return d, "", e
		}
		st.Models[k] = append(items, d)
		rev, e := s.save(ctx, st, nil, modelChange{tenant: tenant, id: d.ID, document: &d})
		if s.shouldRetry(ctx, attempt, e) {
			continue
		}
		return d, rev, e
	}
	return d, "", ErrTransactionConflict
}
func (s *ReBACStore) Watch(ctx context.Context, tenant string, after rebac.Revision) (<-chan rebac.WatchEvent, <-chan error) {
	return s.WatchTuples(ctx, rebac.WatchRequest{TenantID: tenant, After: after})
}
func (s *ReBACStore) WatchTuples(ctx context.Context, r rebac.WatchRequest) (<-chan rebac.WatchEvent, <-chan error) {
	events := make(chan rebac.WatchEvent)
	errs := make(chan error, 1)
	if r.TenantID == "" {
		errs <- rebac.ErrTenantRequired
		close(events)
		close(errs)
		return events, errs
	}
	s.mu.Lock()
	state, err := s.state(ctx)
	if err != nil {
		s.mu.Unlock()
		errs <- err
		close(events)
		close(errs)
		return events, errs
	}
	after, err := revisionNumber(r.After)
	if err != nil || after > state.Revision || (r.After != "" && after < state.WatchAfter) {
		s.mu.Unlock()
		if err == nil {
			err = rebac.ErrInvalidRevision
		}
		errs <- err
		close(events)
		close(errs)
		return events, errs
	}
	namespaces := make(map[string]struct{}, len(r.Namespaces))
	for _, namespace := range r.Namespaces {
		namespaces[namespace] = struct{}{}
	}
	replay := make([]rebac.WatchEvent, 0)
	if r.After != "" {
		for _, event := range state.Events {
			if eventAfter(event, after) {
				if event, ok := filterEvent(event, r.TenantID, namespaces); ok {
					replay = append(replay, event)
				}
			}
		}
	}
	events = make(chan rebac.WatchEvent, len(replay)+watchBuffer)
	for _, event := range replay {
		events <- event
	}
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = watcher{tenant: r.TenantID, namespaces: namespaces, events: events, errs: errs}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if watcher, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(watcher.events)
			close(watcher.errs)
		}
		s.mu.Unlock()
	}()
	return events, errs
}

// WatchAllTuples resumes the durable, globally ordered changelog. One event
// represents one committed mutation, so a checkpoint cannot skip another
// tenant's change at the same revision.
func (s *ReBACStore) WatchAllTuples(ctx context.Context, r rebac.GlobalWatchRequest) (<-chan rebac.WatchEvent, <-chan error) {
	events := make(chan rebac.WatchEvent)
	errs := make(chan error, 1)
	s.mu.Lock()
	state, err := s.state(ctx)
	if err != nil {
		s.mu.Unlock()
		errs <- err
		close(events)
		close(errs)
		return events, errs
	}
	after, err := revisionNumber(r.After)
	if err != nil || after > state.Revision || (r.After != "" && after < state.GlobalAfter) {
		s.mu.Unlock()
		if err == nil {
			err = rebac.ErrInvalidRevision
		}
		errs <- err
		close(events)
		close(errs)
		return events, errs
	}
	tenants := make(map[string]struct{}, len(r.Tenants))
	for _, tenant := range r.Tenants {
		tenants[tenant] = struct{}{}
	}
	namespaces := make(map[string]struct{}, len(r.Namespaces))
	for _, namespace := range r.Namespaces {
		namespaces[namespace] = struct{}{}
	}
	replay := make([]rebac.WatchEvent, 0)
	if r.After != "" {
		for _, event := range state.GlobalEvents {
			if eventAfter(event, after) {
				if event, ok := filterGlobalEvent(event, tenants, namespaces); ok {
					replay = append(replay, event)
				}
			}
		}
	}
	events = make(chan rebac.WatchEvent, len(replay)+watchBuffer)
	for _, event := range replay {
		events <- event
	}
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = watcher{global: true, tenants: tenants, namespaces: namespaces, events: events, errs: errs}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if watcher, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(watcher.events)
			close(watcher.errs)
		}
		s.mu.Unlock()
	}()
	return events, errs
}

func eventAfter(event rebac.WatchEvent, after uint64) bool {
	revision, err := revisionNumber(event.Revision)
	return err == nil && revision > after
}

func filterEvent(event rebac.WatchEvent, tenant string, namespaces map[string]struct{}) (rebac.WatchEvent, bool) {
	if event.TenantID != tenant || event.Heartbeat || len(namespaces) == 0 {
		return event, event.TenantID == tenant
	}
	changes := make([]rebac.TupleChange, 0, len(event.Changes))
	for _, change := range event.Changes {
		if _, ok := namespaces[change.Tuple.Namespace]; ok {
			changes = append(changes, change)
		}
	}
	event.Changes = changes
	return event, len(changes) != 0
}

func filterGlobalEvent(event rebac.WatchEvent, tenants, namespaces map[string]struct{}) (rebac.WatchEvent, bool) {
	changes := make([]rebac.TupleChange, 0, len(event.Changes))
	for _, change := range event.Changes {
		if len(tenants) != 0 {
			if _, ok := tenants[change.Tuple.TenantID]; !ok {
				continue
			}
		}
		if len(namespaces) != 0 {
			if _, ok := namespaces[change.Tuple.Namespace]; !ok {
				continue
			}
		}
		changes = append(changes, change)
	}
	event.TenantID = ""
	event.Changes = changes
	return event, len(changes) != 0 || event.Heartbeat
}

func (s *ReBACStore) publish(events []rebac.WatchEvent) {
	for _, event := range events {
		for id, watcher := range s.watchers {
			var filtered rebac.WatchEvent
			var ok bool
			if watcher.global {
				if event.TenantID != "" {
					continue
				}
				filtered, ok = filterGlobalEvent(event, watcher.tenants, watcher.namespaces)
			} else {
				if event.TenantID == "" {
					continue
				}
				filtered, ok = filterEvent(event, watcher.tenant, watcher.namespaces)
			}
			if !ok {
				continue
			}
			select {
			case watcher.events <- filtered:
			default:
				delete(s.watchers, id)
				watcher.errs <- ErrWatchOverflow
				close(watcher.events)
				close(watcher.errs)
			}
		}
	}
}
func (s *ReBACStore) SnapshotAtLeast(ctx context.Context, r rebac.Revision) (rebac.TupleReader, rebac.Revision, func() error, error) {
	minimum, err := revisionNumber(r)
	if err != nil {
		return nil, "", nil, err
	}
	state, err := s.state(ctx)
	if err != nil {
		return nil, "", nil, err
	}
	if minimum > state.Revision {
		return nil, "", nil, rebac.ErrInvalidRevision
	}
	return tupleView(state), currentRevision(state), func() error { return nil }, nil
}

type tupleView rebacState

func (v tupleView) QueryTuples(ctx context.Context, f rebac.RelationTuple) ([]rebac.RelationTuple, error) {
	return queryState(rebacState(v), f), nil
}

var _ rebac.StorageEngine = (*ReBACStore)(nil)
var _ rebac.RevisionedStorage = (*ReBACStore)(nil)
var _ rebac.MutationStorage = (*ReBACStore)(nil)
var _ rebac.ModelStorage = (*ReBACStore)(nil)
var _ rebac.ModelVersionLister = (*ReBACStore)(nil)
var _ rebac.ActiveModelStorage = (*ReBACStore)(nil)
var _ rebac.ResourceCandidateReader = (*ReBACStore)(nil)
var _ rebac.SubjectCandidateReader = (*ReBACStore)(nil)
var _ rebac.CandidateIndexWatermark = (*ReBACStore)(nil)
var _ rebac.WatchStorage = (*ReBACStore)(nil)
var _ rebac.ConsistentStorage = (*ReBACStore)(nil)
var _ rebac.RevisionedModelStorage = (*ReBACStore)(nil)
