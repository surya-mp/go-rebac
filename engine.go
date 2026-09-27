package rebac

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrMaxDepthExceeded = errors.New("rebac: maximum graph depth exceeded")
	ErrMaxNodesExceeded = errors.New("rebac: maximum graph nodes exceeded")
	ErrTenantRequired   = errors.New("rebac: tenant ID is required")
	ErrModelTenant      = errors.New("rebac: selected model belongs to another tenant")
	ErrInvalidRequest   = errors.New("rebac: invalid request")
	ErrStorage          = errors.New("rebac: storage error")
)

const (
	defaultMaxDepth = 50
	defaultMaxNodes = 10_000
	asOfContextKey  = "_rebac_as_of_unix_nano"
)

// Engine evaluates tuples supplied by its storage backend.
type Engine struct {
	store       StorageEngine
	model       AuthorizationModel
	maxDepth    int
	maxNodes    int
	observer    Observer
	stats       *engineStats
	cache       DecisionCache
	modelTenant string
	modelID     string
	modelSource ModelSnapshotStorage
	caveats     map[string]CaveatDefinition
	coalescer   *checkCoalescer
}

// NewEngine compiles the application-owned authorization model and uses store
// only for durable tuple access.
func NewEngine(store StorageEngine, model AuthorizationModel) (*Engine, error) {
	if store == nil {
		return nil, errors.New("rebac: storage engine is nil")
	}
	if err := model.validate(); err != nil {
		return nil, err
	}
	return newEngine(store, model, defaultMaxDepth, defaultMaxNodes), nil
}

// NewProductionEngine constructs an Engine only for storage adapters that
// provide revision consistency, atomic mutations, and indexed lookup support.
func NewProductionEngine(store ProductionStorage, model AuthorizationModel) (*Engine, error) {
	if store == nil {
		return nil, ErrProductionStorage
	}
	return NewEngine(store, model)
}

// WithLimits returns a copy with explicit evaluation ceilings. Both values
// must be positive; defaults are retained otherwise.
func (e *Engine) WithLimits(maxDepth, maxNodes int) *Engine {
	configured := *e
	if maxDepth > 0 {
		configured.maxDepth = maxDepth
	}
	if maxNodes > 0 {
		configured.maxNodes = maxNodes
	}
	return &configured
}

// WithObserver returns a copy that emits application-owned telemetry events.
func (e *Engine) WithObserver(observer Observer) *Engine {
	configured := *e
	configured.observer = observer
	return &configured
}

// WithDecisionCache returns a copy that caches only revision-pinned checks.
// Cache failures never affect an authorization decision.
func (e *Engine) WithDecisionCache(cache DecisionCache) *Engine {
	configured := *e
	configured.cache = cache
	return &configured
}

// Stats returns process-local authorization counters.
func (e *Engine) Stats() Stats {
	if e == nil || e.stats == nil {
		return Stats{}
	}
	return e.stats.snapshot()
}

func newEngine(store StorageEngine, model AuthorizationModel, maxDepth, maxNodes int) *Engine {
	return &Engine{store: store, model: model, maxDepth: maxDepth, maxNodes: maxNodes, stats: &engineStats{}}
}

// Check reports whether user has relation on namespace:objectID within tenantID.
func (e *Engine) Check(ctx context.Context, tenantID, user, relation, namespace, objectID string) (bool, error) {
	if e != nil && e.coalescer != nil {
		return e.coalescer.do(ctx, checkFlightKey(tenantID, user, relation, namespace, objectID), func() (bool, error) {
			return e.checkUncoalesced(ctx, tenantID, user, relation, namespace, objectID)
		})
	}
	return e.checkUncoalesced(ctx, tenantID, user, relation, namespace, objectID)
}

func (e *Engine) checkUncoalesced(ctx context.Context, tenantID, user, relation, namespace, objectID string) (bool, error) {
	if e != nil && e.modelSource != nil {
		allowed, _, err := e.CheckWithConsistency(ctx, ConsistencyToken{}, tenantID, user, relation, namespace, objectID)
		return allowed, err
	}
	allowed, _, err := e.CheckWithRevision(ctx, "", tenantID, user, relation, namespace, objectID)
	return allowed, err
}

// CheckWithContext evaluates tuple caveats against caveatContext.
func (e *Engine) CheckWithContext(ctx context.Context, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string) (bool, error) {
	if e != nil && e.modelSource != nil {
		allowed, _, err := e.CheckWithConsistencyAndContext(ctx, ConsistencyToken{}, caveatContext, tenantID, user, relation, namespace, objectID)
		return allowed, err
	}
	allowed, _, err := e.CheckWithRevisionAndContext(ctx, "", caveatContext, tenantID, user, relation, namespace, objectID)
	return allowed, err
}

// CheckAt evaluates access at asOfUnixNano. A tuple with a validity interval is
// inactive unless this value falls within its half-open interval. Zero denies
// interval-bearing tuples, so ordinary Check calls remain fail-closed.
func (e *Engine) CheckAt(ctx context.Context, asOfUnixNano int64, tenantID, user, relation, namespace, objectID string) (bool, error) {
	return e.CheckWithContextAt(ctx, nil, asOfUnixNano, tenantID, user, relation, namespace, objectID)
}

// CheckWithContextAt evaluates caveats and tuple validity intervals together.
func (e *Engine) CheckWithContextAt(ctx context.Context, caveatContext CaveatContext, asOfUnixNano int64, tenantID, user, relation, namespace, objectID string) (bool, error) {
	return e.CheckWithContext(ctx, withAsOf(caveatContext, asOfUnixNano), tenantID, user, relation, namespace, objectID)
}

// CheckWithRevisionAt evaluates one revision with a fixed tuple-validity time.
func (e *Engine) CheckWithRevisionAt(ctx context.Context, revision Revision, caveatContext CaveatContext, asOfUnixNano int64, tenantID, user, relation, namespace, objectID string) (bool, Revision, error) {
	return e.CheckWithRevisionAndContext(ctx, revision, withAsOf(caveatContext, asOfUnixNano), tenantID, user, relation, namespace, objectID)
}

func withAsOf(caveatContext CaveatContext, asOfUnixNano int64) CaveatContext {
	contextAt := make(CaveatContext, len(caveatContext)+1)
	for key, value := range caveatContext {
		contextAt[key] = value
	}
	contextAt[asOfContextKey] = asOfUnixNano
	return contextAt
}

// CheckWithRevision evaluates at revision, or at the latest consistent view
// when revision is empty. It returns the exact view used for this decision.
func (e *Engine) CheckWithRevision(ctx context.Context, revision Revision, tenantID, user, relation, namespace, objectID string) (allowed bool, used Revision, err error) {
	return e.CheckWithRevisionAndContext(ctx, revision, nil, tenantID, user, relation, namespace, objectID)
}

// CheckWithRevisionAndContext evaluates a stable graph view and caveats.
func (e *Engine) CheckWithRevisionAndContext(ctx context.Context, revision Revision, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string) (allowed bool, used Revision, err error) {
	if e != nil && e.modelSource != nil {
		if revision != "" {
			return false, "", ErrConsistencyUnsupported
		}
		allowed, token, err := e.CheckWithConsistencyAndContext(ctx, ConsistencyToken{}, caveatContext, tenantID, user, relation, namespace, objectID)
		return allowed, token.TupleRevision, err
	}
	started := time.Now()
	nodes := 0
	defer func() {
		if e == nil || e.stats == nil {
			return
		}
		e.stats.checks.Add(1)
		reason := decisionReason(allowed, err)
		if allowed {
			e.stats.allowed.Add(1)
		} else if err == nil {
			e.stats.denied.Add(1)
		} else {
			e.stats.errors.Add(1)
		}
		if e.observer != nil {
			e.observer.ObserveCheck(ctx, CheckEvent{TenantID: tenantID, User: user, Relation: relation, Namespace: namespace, ObjectID: objectID, Revision: used, Allowed: allowed, Reason: reason, Err: err, Duration: time.Since(started), Nodes: nodes})
		}
	}()
	if e == nil || e.store == nil {
		return false, "", errors.New("rebac: storage engine is nil")
	}
	if err := e.validateTenant(tenantID); err != nil {
		return false, "", err
	}
	if err := e.model.validateCheck(user, relation, namespace, objectID); err != nil {
		return false, "", err
	}
	if key, ok := decisionCacheKey(revision, tenantID, user, relation, namespace, objectID, caveatContext); ok && e.cache != nil {
		if cached, found, cacheErr := e.cache.Get(ctx, key); cacheErr == nil && found {
			return cached, revision, nil
		}
	}

	reader := TupleReader(e.store)
	release := func() error { return nil }
	if revisions, ok := e.store.(RevisionedStorage); ok {
		reader, used, release, err = revisions.SnapshotAt(ctx, revision)
		if err != nil {
			return false, "", wrapStorageError(err)
		}
	} else if revision != "" {
		return false, "", ErrRevisionsUnsupported
	} else if snapshots, ok := e.store.(SnapshotStorage); ok {
		reader, release, err = snapshots.Snapshot(ctx)
		if err != nil && !errors.Is(err, ErrSnapshotsUnsupported) {
			return false, "", err
		}
		if errors.Is(err, ErrSnapshotsUnsupported) {
			reader, release = e.store, func() error { return nil }
		}
	}
	reader = &memoReader{reader: reader, cache: make(map[tupleFilterKey][]RelationTuple)}
	allowed, err = e.check(ctx, reader, caveatContext, tenantID, user, relation, namespace, objectID, make(map[checkKey]struct{}), 0, &nodes)
	if releaseErr := release(); err == nil && releaseErr != nil {
		return false, "", releaseErr
	}
	if err == nil && e.cache != nil {
		if key, ok := decisionCacheKey(used, tenantID, user, relation, namespace, objectID, caveatContext); ok {
			_ = e.cache.Set(ctx, key, allowed)
		}
	}
	return allowed, used, err
}

func decisionReason(allowed bool, err error) DecisionReason {
	if err == nil {
		if allowed {
			return DecisionAllowed
		}
		return DecisionDenied
	}
	if errors.Is(err, ErrMaxDepthExceeded) || errors.Is(err, ErrMaxNodesExceeded) {
		return DecisionLimitExceeded
	}
	if errors.Is(err, ErrStorage) {
		return DecisionStorageError
	}
	return DecisionInvalidRequest
}

// WriteTuple validates tuple against the application model before persisting it.
func (e *Engine) WriteTuple(ctx context.Context, tuple RelationTuple) error {
	_, err := e.WriteTupleWithRevision(ctx, tuple)
	return err
}

// WriteTupleWithRevision writes a validated tuple and returns its datastore
// revision when the storage supports revision tokens.
func (e *Engine) WriteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error) {
	if e == nil || e.store == nil {
		return "", errors.New("rebac: storage engine is nil")
	}
	if err := e.validateTenant(tuple.TenantID); err != nil {
		return "", err
	}
	if err := e.model.validateTuple(tuple); err != nil {
		return "", err
	}
	if revisions, ok := e.store.(RevisionedStorage); ok {
		revision, err := revisions.WriteTupleWithRevision(ctx, tuple)
		e.observeMutation(ctx, revision, 1, err)
		return revision, wrapStorageError(err)
	}
	if err := e.store.WriteTuple(ctx, tuple); err != nil {
		e.observeMutation(ctx, "", 1, err)
		return "", wrapStorageError(err)
	}
	e.observeMutation(ctx, "", 1, nil)
	return "", nil
}

// DeleteTuple validates tuple against the application model before deleting it.
func (e *Engine) DeleteTuple(ctx context.Context, tuple RelationTuple) error {
	_, err := e.DeleteTupleWithRevision(ctx, tuple)
	return err
}

// DeleteTupleWithRevision deletes a validated tuple and returns its datastore
// revision when the storage supports revision tokens.
func (e *Engine) DeleteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error) {
	if e == nil || e.store == nil {
		return "", errors.New("rebac: storage engine is nil")
	}
	if err := e.validateTenant(tuple.TenantID); err != nil {
		return "", err
	}
	if err := e.model.validateTuple(tuple); err != nil {
		return "", err
	}
	if revisions, ok := e.store.(RevisionedStorage); ok {
		revision, err := revisions.DeleteTupleWithRevision(ctx, tuple)
		e.observeMutation(ctx, revision, 1, err)
		return revision, wrapStorageError(err)
	}
	if err := e.store.DeleteTuple(ctx, tuple); err != nil {
		e.observeMutation(ctx, "", 1, err)
		return "", wrapStorageError(err)
	}
	e.observeMutation(ctx, "", 1, nil)
	return "", nil
}

// DeleteObject removes every relationship defined on namespace:objectID.
// Relationships that point at the object are left as inert references; they no
// longer grant because the deleted object's usersets are empty.
func (e *Engine) DeleteObject(ctx context.Context, tenantID, namespace, objectID string) (Revision, error) {
	if e == nil || e.store == nil {
		return "", errors.New("rebac: storage engine is nil")
	}
	if err := e.validateTenant(tenantID); err != nil {
		return "", err
	}
	if !validName(namespace) || !validObjectID(objectID) {
		return "", ErrInvalidTuple
	}
	if _, ok := e.model.Namespaces[namespace]; !ok {
		return "", ErrInvalidTuple
	}
	if storage, ok := e.store.(ObjectDeletionStorage); ok {
		revision, err := storage.DeleteObject(ctx, tenantID, namespace, objectID)
		e.observeMutation(ctx, revision, 0, err)
		return revision, wrapStorageError(err)
	}
	return "", ErrMutationUnsupported
}

// Mutate validates and atomically applies changes when the storage supports
// transactions. Preconditions are checked against the same committed view.
func (e *Engine) Mutate(ctx context.Context, changes []TupleChange, preconditions []Precondition) (Revision, error) {
	if e == nil || e.store == nil {
		return "", errors.New("rebac: storage engine is nil")
	}
	if len(changes) == 0 {
		return "", errors.New("rebac: mutation has no changes")
	}
	for _, change := range changes {
		if change.Operation != WriteOperation && change.Operation != DeleteOperation {
			return "", errors.New("rebac: invalid tuple operation")
		}
		if err := e.validateMutationTuple(change.Tuple); err != nil {
			return "", err
		}
	}
	for _, precondition := range preconditions {
		if err := e.validateMutationTuple(precondition.Tuple); err != nil {
			return "", err
		}
	}
	storage, ok := e.store.(MutationStorage)
	if !ok {
		return "", ErrMutationUnsupported
	}
	revision, err := storage.Mutate(ctx, changes, preconditions)
	e.observeMutation(ctx, revision, len(changes), err)
	return revision, wrapStorageError(err)
}

func (e *Engine) validateMutationTuple(tuple RelationTuple) error {
	if err := e.validateTenant(tuple.TenantID); err != nil {
		return err
	}
	return e.model.validateTuple(tuple)
}

func (e *Engine) validateTenant(tenantID string) error {
	if !validObjectID(tenantID) {
		return ErrTenantRequired
	}
	if e.modelTenant != "" && e.modelTenant != tenantID {
		return ErrModelTenant
	}
	return nil
}

// Watch streams committed changes for one tenant after revision. Cache users
// should discard entries affected by each event, then advance their revision.
func (e *Engine) Watch(ctx context.Context, tenantID string, after Revision) (<-chan WatchEvent, <-chan error) {
	events := make(chan WatchEvent)
	errorsCh := make(chan error, 1)
	if e == nil || e.store == nil {
		errorsCh <- errors.New("rebac: storage engine is nil")
		close(events)
		close(errorsCh)
		return events, errorsCh
	}
	if err := e.validateTenant(tenantID); err != nil {
		errorsCh <- err
		close(events)
		close(errorsCh)
		return events, errorsCh
	}
	storage, ok := e.store.(WatchStorage)
	if !ok {
		errorsCh <- ErrRevisionsUnsupported
		close(events)
		close(errorsCh)
		return events, errorsCh
	}
	return storage.Watch(ctx, tenantID, after)
}

type checkKey struct {
	tenantID, user, relation, namespace, objectID string
}

type tupleFilterKey struct {
	tenantID, namespace, objectID, relation, user string
}

type memoReader struct {
	reader TupleReader
	cache  map[tupleFilterKey][]RelationTuple
}

func (r *memoReader) QueryTuples(ctx context.Context, filter RelationTuple) ([]RelationTuple, error) {
	key := tupleFilterKey{filter.TenantID, filter.Namespace, filter.ObjectID, filter.Relation, filter.User}
	if tuples, ok := r.cache[key]; ok {
		return tuples, nil
	}
	tuples, err := r.reader.QueryTuples(ctx, filter)
	if err == nil {
		r.cache[key] = tuples
	}
	return tuples, err
}

func (e *Engine) check(ctx context.Context, store TupleReader, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string, visiting map[checkKey]struct{}, depth int, nodes *int) (bool, error) {
	if depth > e.maxDepth {
		return false, ErrMaxDepthExceeded
	}
	if *nodes >= e.maxNodes {
		return false, ErrMaxNodesExceeded
	}
	(*nodes)++
	key := checkKey{tenantID, user, relation, namespace, objectID}
	if _, ok := visiting[key]; ok {
		return false, nil // This path loops back to an in-progress graph node.
	}
	visiting[key] = struct{}{}
	defer delete(visiting, key)

	definition := e.model.Namespaces[namespace].Relations[relation]
	return e.evaluateRewrite(ctx, store, caveatContext, tenantID, user, relation, namespace, objectID, definition.Rewrite, visiting, depth, nodes)
}

func (e *Engine) evaluateRewrite(ctx context.Context, store TupleReader, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string, rewrite Rewrite, visiting map[checkKey]struct{}, depth int, nodes *int) (bool, error) {
	kind, err := rewrite.kind()
	if err != nil {
		return false, err
	}
	switch kind {
	case "this":
		return e.checkThis(ctx, store, caveatContext, tenantID, user, relation, namespace, objectID, visiting, depth, nodes)
	case "computed":
		return e.check(ctx, store, caveatContext, tenantID, user, rewrite.ComputedUserset, namespace, objectID, visiting, depth+1, nodes)
	case "tuple-to-userset":
		return e.checkTupleToUserset(ctx, store, caveatContext, tenantID, user, namespace, objectID, *rewrite.TupleToUserset, visiting, depth, nodes)
	case "union":
		for _, child := range rewrite.Union {
			allowed, err := e.evaluateRewrite(ctx, store, caveatContext, tenantID, user, relation, namespace, objectID, child, visiting, depth, nodes)
			if err != nil || allowed {
				return allowed, err
			}
		}
		return false, nil
	case "intersection":
		for _, child := range rewrite.Intersection {
			allowed, err := e.evaluateRewrite(ctx, store, caveatContext, tenantID, user, relation, namespace, objectID, child, visiting, depth, nodes)
			if err != nil || !allowed {
				return allowed, err
			}
		}
		return true, nil
	case "exclusion":
		allowed, err := e.evaluateRewrite(ctx, store, caveatContext, tenantID, user, relation, namespace, objectID, rewrite.Exclusion.Base, visiting, depth, nodes)
		if err != nil || !allowed {
			return allowed, err
		}
		excluded, err := e.evaluateRewrite(ctx, store, caveatContext, tenantID, user, relation, namespace, objectID, rewrite.Exclusion.Subtract, visiting, depth, nodes)
		return !excluded, err
	default:
		return false, errors.New("rebac: invalid rewrite")
	}
}

func (e *Engine) checkThis(ctx context.Context, store TupleReader, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string, visiting map[checkKey]struct{}, depth int, nodes *int) (bool, error) {
	tuples, err := e.queryTuples(ctx, store, tenantID, RelationTuple{Namespace: namespace, ObjectID: objectID, Relation: relation})
	if err != nil {
		return false, err
	}
	for _, tuple := range tuples {
		applies, err := e.evaluateCaveat(ctx, tuple, caveatContext)
		if err != nil {
			return false, err
		}
		if !applies {
			continue
		}
		if tuple.User == user {
			return true, nil
		}
		if tuple.User == "user:*" {
			if namespace, _, ok := parseDirectSubject(user); ok && namespace == "user" {
				return true, nil
			}
			continue
		}
		usersetNamespace, usersetObjectID, usersetRelation, ok := parseUserset(tuple.User)
		if !ok {
			continue
		}
		allowed, err := e.check(ctx, store, caveatContext, tenantID, user, usersetRelation, usersetNamespace, usersetObjectID, visiting, depth+1, nodes)
		if err != nil {
			return false, err
		}
		if allowed {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) checkTupleToUserset(ctx context.Context, store TupleReader, caveatContext CaveatContext, tenantID, user, namespace, objectID string, rewrite TupleToUserset, visiting map[checkKey]struct{}, depth int, nodes *int) (bool, error) {
	tuples, err := e.queryTuples(ctx, store, tenantID, RelationTuple{Namespace: namespace, ObjectID: objectID, Relation: rewrite.Tupleset})
	if err != nil {
		return false, err
	}
	for _, tuple := range tuples {
		applies, err := e.evaluateCaveat(ctx, tuple, caveatContext)
		if err != nil {
			return false, err
		}
		if !applies {
			continue
		}
		targetNamespace, targetObjectID, ok := parseDirectSubject(tuple.User)
		if !ok {
			continue
		}
		allowed, err := e.check(ctx, store, caveatContext, tenantID, user, rewrite.ComputedUserset, targetNamespace, targetObjectID, visiting, depth+1, nodes)
		if err != nil {
			return false, err
		}
		if allowed {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) evaluateCaveat(ctx context.Context, tuple RelationTuple, request CaveatContext) (bool, error) {
	if !tupleActiveAt(tuple, request) {
		return false, nil
	}
	if tuple.Caveat == "" {
		return true, nil
	}
	definition := e.model.Caveats[tuple.Caveat]
	allowed, err := definition.Evaluate(ctx, tuple.CaveatContext, request)
	if err != nil {
		return false, fmt.Errorf("rebac: caveat %q: %w", tuple.Caveat, err)
	}
	return allowed, nil
}

func tupleActiveAt(tuple RelationTuple, request CaveatContext) bool {
	if tuple.NotBeforeUnixNano == 0 && tuple.NotAfterUnixNano == 0 {
		return true
	}
	asOf, ok := request[asOfContextKey].(int64)
	if !ok || asOf <= 0 {
		return false
	}
	return (tuple.NotBeforeUnixNano == 0 || asOf >= tuple.NotBeforeUnixNano) &&
		(tuple.NotAfterUnixNano == 0 || asOf < tuple.NotAfterUnixNano)
}

func (e *Engine) queryTuples(ctx context.Context, store TupleReader, tenantID string, filter RelationTuple) ([]RelationTuple, error) {
	filter.TenantID = tenantID
	tuples, err := store.QueryTuples(ctx, filter)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	return e.validateTuples(tenantID, tuples)
}

func (e *Engine) validateTuples(tenantID string, tuples []RelationTuple) ([]RelationTuple, error) {
	for _, tuple := range tuples {
		if tuple.TenantID != tenantID {
			return nil, errors.New("rebac: storage returned a cross-tenant tuple")
		}
		if err := e.model.validateTuple(tuple); err != nil {
			return nil, err
		}
	}
	return tuples, nil
}

func (e *Engine) observeMutation(ctx context.Context, revision Revision, changes int, err error) {
	if e == nil || e.stats == nil {
		return
	}
	e.stats.mutations.Add(1)
	if e.observer != nil {
		e.observer.ObserveMutation(ctx, MutationEvent{Revision: revision, Changes: changes, Err: err})
	}
}

func wrapStorageError(err error) error {
	if err == nil || errors.Is(err, ErrInvalidRevision) || errors.Is(err, ErrPreconditionFailed) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrStorage, err)
}

// parseUserset accepts Zanzibar's namespace:object#relation subject syntax.
func parseUserset(user string) (namespace, objectID, relation string, ok bool) {
	resource, relation, ok := strings.Cut(user, "#")
	if !ok || !validName(relation) || strings.Contains(relation, "#") {
		return "", "", "", false
	}
	namespace, objectID, ok = strings.Cut(resource, ":")
	if !ok || !validName(namespace) || !validObjectID(objectID) {
		return "", "", "", false
	}
	return namespace, objectID, relation, true
}

func parseDirectSubject(user string) (namespace, objectID string, ok bool) {
	if strings.Contains(user, "#") {
		return "", "", false
	}
	namespace, objectID, ok = strings.Cut(user, ":")
	return namespace, objectID, ok && validName(namespace) && validObjectID(objectID)
}
