package kv

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/surya-mp/go-rebac"
)

const (
	stateKey = "go-rebac/state"
	// ponytail: retain 1,000 events; add configurable or disk-segmented retention only when needed.
	maxWatchHistory = 1_000
	watchBuffer     = 16
)

var ErrWatchOverflow = errors.New("rebac/kv: watch consumer fell behind")

// ReBACStore is an embedded, versioned relationship and model store. It
// retains the latest 1,000 tuple events for resumable watches.
type ReBACStore struct {
	db        *Database
	mu        sync.Mutex
	watchers  map[int]watcher
	nextWatch int
}

type watcher struct {
	tenant     string
	namespaces map[string]struct{}
	events     chan rebac.WatchEvent
	errs       chan error
}

type rebacState struct {
	Revision   uint64                           `json:"revision"`
	WatchAfter uint64                           `json:"watch_after,omitempty"`
	Tuples     []rebac.RelationTuple            `json:"tuples"`
	Models     map[string][]rebac.ModelDocument `json:"models"`
	Events     []rebac.WatchEvent               `json:"events,omitempty"`
	ByObject   map[string][]int                 `json:"by_object,omitempty"`
	BySubject  map[string][]int                 `json:"by_subject,omitempty"`
	Resources  map[string][]string              `json:"resources,omitempty"`
	Subjects   map[string][]string              `json:"subjects,omitempty"`
	Deleted    map[string]bool                  `json:"deleted,omitempty"`
}

// NewReBACStore uses an application-created KV database.
func NewReBACStore(db *Database) *ReBACStore {
	return &ReBACStore{db: db, watchers: make(map[int]watcher)}
}

func (s *ReBACStore) state(ctx context.Context) (rebacState, error) {
	return s.stateAt(ctx, "")
}

func (s *ReBACStore) stateAt(ctx context.Context, revision rebac.Revision) (rebacState, error) {
	snap, err := s.db.NewSnapshot(ctx)
	if err != nil {
		return rebacState{}, err
	}
	defer snap.Close()
	key := stateKey
	if revision != "" {
		key += "/" + string(revision)
	}
	raw, ok, err := snap.Get(ctx, key)
	if err != nil || !ok {
		if revision != "" && err == nil {
			err = rebac.ErrInvalidRevision
		}
		return rebacState{Models: map[string][]rebac.ModelDocument{}}, err
	}
	var state rebacState
	if err := json.Unmarshal(raw, &state); err != nil {
		return rebacState{}, err
	}
	if revision != "" && revision != rebac.Revision(strconv.FormatUint(state.Revision, 10)) {
		return rebacState{}, rebac.ErrInvalidRevision
	}
	if state.Models == nil {
		state.Models = map[string][]rebac.ModelDocument{}
	}
	if state.Deleted == nil {
		state.Deleted = make(map[string]bool)
	}
	if state.ByObject == nil {
		state.index()
	}
	return state, nil
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
func (s *ReBACStore) save(ctx context.Context, state rebacState) (rebac.Revision, error) {
	tx, err := s.db.NewTransaction(ctx)
	if err != nil {
		return "", err
	}
	state.Revision++
	raw, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	if err = tx.Set(ctx, stateKey, raw); err != nil {
		return "", err
	}
	if err = tx.Set(ctx, stateKey+"/"+strconv.FormatUint(state.Revision, 10), raw); err != nil {
		return "", err
	}
	rev, err := tx.Commit(ctx)
	if err != nil {
		return "", err
	}
	return rebac.Revision(strconv.FormatUint(rev, 10)), nil
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
	return events
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
	st, e := s.state(ctx)
	if e != nil {
		return nil, e
	}
	return queryState(st, f), nil
}
func (s *ReBACStore) WriteTuple(ctx context.Context, t rebac.RelationTuple) error {
	_, e := s.WriteTupleWithRevision(ctx, t)
	return e
}
func (s *ReBACStore) WriteTupleWithRevision(ctx context.Context, t rebac.RelationTuple) (rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, e := s.state(ctx)
	if e != nil {
		return "", e
	}
	revived := stateRevive(&st, t)
	for index, existing := range st.Tuples {
		if sameTuple(existing, t) {
			if reflect.DeepEqual(existing, t) {
				if revived {
					return s.save(ctx, st)
				}
				return currentRevision(st), nil
			}
			st.Tuples[index] = t
			st.index()
			events := st.record([]rebac.TupleChange{{Operation: rebac.WriteOperation, Tuple: t}})
			revision, err := s.save(ctx, st)
			if err == nil {
				s.publish(events)
			}
			return revision, err
		}
	}
	st.Tuples = append(st.Tuples, t)
	st.index()
	events := st.record([]rebac.TupleChange{{Operation: rebac.WriteOperation, Tuple: t}})
	revision, err := s.save(ctx, st)
	if err == nil {
		s.publish(events)
	}
	return revision, err
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
	st, e := s.state(ctx)
	if e != nil {
		return "", e
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
	events := st.record([]rebac.TupleChange{{Operation: rebac.DeleteOperation, Tuple: t}})
	revision, err := s.save(ctx, st)
	if err == nil {
		s.publish(events)
	}
	return revision, err
}

// DeleteObject removes every tuple defined on one object in one commit and
// leaves a tombstone for CollectGarbage.
func (s *ReBACStore) DeleteObject(ctx context.Context, tenantID, namespace, objectID string) (rebac.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	revision, err := s.save(ctx, state)
	if err == nil {
		s.publish(events)
	}
	return revision, err
}

// CollectGarbage removes inbound references to at most max deleted objects.
// Call it periodically; max <= 0 does no work.
func (s *ReBACStore) CollectGarbage(ctx context.Context, max int) (int, error) {
	if max <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
	_, err = s.save(ctx, state)
	if err != nil {
		return 0, err
	}
	s.publish(events)
	return len(keys), nil
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
	rev, err := s.save(ctx, st)
	if err == nil {
		s.publish(events)
	}
	return rev, err
}

func (s *ReBACStore) LookupResourceCandidates(ctx context.Context, r rebac.LookupResourcesRequest) ([]string, error) {
	st, e := s.state(ctx)
	if e != nil {
		return nil, e
	}
	return append([]string(nil), st.Resources[namespaceKey(r.TenantID, r.Namespace)]...), nil
}
func (s *ReBACStore) LookupSubjectCandidates(ctx context.Context, r rebac.LookupSubjectsRequest) ([]string, error) {
	st, e := s.state(ctx)
	if e != nil {
		return nil, e
	}
	return append([]string(nil), st.Subjects[r.TenantID]...), nil
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
func sorted(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
func (s *ReBACStore) ReadAuthorizationModelAtRevision(ctx context.Context, tenant, id string, rev rebac.Revision) (rebac.ModelDocument, error) {
	st, err := s.stateAt(ctx, rev)
	if err != nil {
		return rebac.ModelDocument{}, err
	}
	items := st.Models[tenant+"/"+id]
	if len(items) == 0 {
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
	st.Models[k] = append(items, d)
	rev, e := s.save(ctx, st)
	return d, rev, e
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

func (s *ReBACStore) publish(events []rebac.WatchEvent) {
	for _, event := range events {
		for id, watcher := range s.watchers {
			filtered, ok := filterEvent(event, watcher.tenant, watcher.namespaces)
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
var _ rebac.ConsistentStorage = (*ReBACStore)(nil)
var _ rebac.RevisionedModelStorage = (*ReBACStore)(nil)
