package rebac

import (
	"context"
	"fmt"
	"time"
)

// ConsistencyToken is an opaque lower bound for a Zanzibar-style read. Clients
// must store it with application content and pass it back unchanged. The
// storage implementation owns the meaning and ordering of TupleRevision.
type ConsistencyToken struct {
	TenantID      string   `json:"tenant_id"`
	ModelID       string   `json:"model_id"`
	ModelVersion  Revision `json:"model_version"`
	TupleRevision Revision `json:"tuple_revision"`
}

func (t ConsistencyToken) empty() bool {
	return t.TenantID == "" && t.ModelID == "" && t.ModelVersion == "" && t.TupleRevision == ""
}

func (t ConsistencyToken) validate(tenantID, modelID string) error {
	if t.empty() {
		return nil
	}
	if t.TenantID != tenantID || t.ModelID != modelID || t.ModelVersion == "" || t.TupleRevision == "" {
		return fmt.Errorf("%w: token belongs to another authorization view", ErrInvalidRevision)
	}
	return nil
}

// ContentChangeCheckRequest asks whether a principal may create a new version
// of application content. Persist the returned token atomically with that
// content version, then use it for later authorization checks.
type ContentChangeCheckRequest struct {
	TenantID       string           `json:"tenant_id"`
	User           string           `json:"user"`
	Relation       string           `json:"relation"`
	Namespace      string           `json:"namespace"`
	ObjectID       string           `json:"object_id"`
	CaveatContext  CaveatContext    `json:"caveat_context,omitempty"`
	AsOfUnixNano   int64            `json:"as_of_unix_nano,omitempty"`
	AtLeastAsFresh ConsistencyToken `json:"at_least_as_fresh,omitempty"`
}

// CheckWithConsistency evaluates at a snapshot at least as fresh as minimum.
// It is available only from NewConsistentEngineFromModelStorage.
func (e *Engine) CheckWithConsistency(ctx context.Context, minimum ConsistencyToken, tenantID, user, relation, namespace, objectID string) (allowed bool, token ConsistencyToken, err error) {
	return e.checkWithConsistency(ctx, minimum, nil, tenantID, user, relation, namespace, objectID)
}

// CheckWithConsistencyAndContext is CheckWithConsistency with caveat values.
func (e *Engine) CheckWithConsistencyAndContext(ctx context.Context, minimum ConsistencyToken, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string) (allowed bool, token ConsistencyToken, err error) {
	return e.checkWithConsistency(ctx, minimum, caveatContext, tenantID, user, relation, namespace, objectID)
}

// CheckWithConsistencyAt evaluates one at-least-as-fresh view at a fixed tuple-validity time.
func (e *Engine) CheckWithConsistencyAt(ctx context.Context, minimum ConsistencyToken, caveatContext CaveatContext, asOfUnixNano int64, tenantID, user, relation, namespace, objectID string) (allowed bool, token ConsistencyToken, err error) {
	return e.checkWithConsistency(ctx, minimum, withAsOf(caveatContext, asOfUnixNano), tenantID, user, relation, namespace, objectID)
}

func (e *Engine) checkWithConsistency(ctx context.Context, minimum ConsistencyToken, caveatContext CaveatContext, tenantID, user, relation, namespace, objectID string) (allowed bool, token ConsistencyToken, err error) {
	started := time.Now()
	nodes := 0
	defer func() {
		if e == nil || e.stats == nil {
			return
		}
		e.stats.checks.Add(1)
		if allowed {
			e.stats.allowed.Add(1)
		} else if err == nil {
			e.stats.denied.Add(1)
		} else {
			e.stats.errors.Add(1)
		}
		if e.observer != nil {
			e.observer.ObserveCheck(ctx, CheckEvent{TenantID: tenantID, User: user, Relation: relation, Namespace: namespace, ObjectID: objectID, Revision: token.TupleRevision, Allowed: allowed, Reason: decisionReason(allowed, err), Err: err, Duration: time.Since(started), Nodes: nodes})
		}
	}()

	view, reader, token, release, err := e.consistencyView(ctx, minimum, tenantID)
	if err != nil {
		return false, ConsistencyToken{}, err
	}
	if err := view.model.validateCheck(user, relation, namespace, objectID); err != nil {
		_ = release()
		return false, ConsistencyToken{}, err
	}
	allowed, err = view.check(ctx, reader, caveatContext, tenantID, user, relation, namespace, objectID, make(map[checkKey]struct{}), 0, &nodes)
	if releaseErr := release(); err == nil && releaseErr != nil {
		return false, ConsistencyToken{}, wrapStorageError(releaseErr)
	}
	return allowed, token, err
}

// ContentChangeCheck performs a consistency-aware check and returns the token
// that must accompany the resulting application-content version.
func (e *Engine) ContentChangeCheck(ctx context.Context, request ContentChangeCheckRequest) (bool, ConsistencyToken, error) {
	return e.CheckWithConsistencyAt(ctx, request.AtLeastAsFresh, request.CaveatContext, request.AsOfUnixNano, request.TenantID, request.User, request.Relation, request.Namespace, request.ObjectID)
}

// WatchWithConsistency resumes an ordered tuple watch from a content token.
// It is available only from NewConsistentEngineFromModelStorage.
func (e *Engine) WatchWithConsistency(ctx context.Context, minimum ConsistencyToken, tenantID string, namespaces []string) (<-chan WatchEvent, <-chan error) {
	events := make(chan WatchEvent)
	errorsCh := make(chan error, 1)
	if e == nil || e.modelSource == nil || e.store == nil {
		errorsCh <- ErrConsistencyUnsupported
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
	if err := minimum.validate(tenantID, e.modelID); err != nil {
		errorsCh <- err
		close(events)
		close(errorsCh)
		return events, errorsCh
	}
	storage, ok := e.store.(ResumableWatchStorage)
	if !ok {
		errorsCh <- ErrConsistencyUnsupported
		close(events)
		close(errorsCh)
		return events, errorsCh
	}
	return storage.WatchTuples(ctx, WatchRequest{TenantID: tenantID, After: minimum.TupleRevision, Namespaces: namespaces})
}

// ReadTuplesWithConsistency reads model-valid tuples from one at-least-fresh
// model-and-tuple view. It is intended for administration and debugging.
func (e *Engine) ReadTuplesWithConsistency(ctx context.Context, minimum ConsistencyToken, request ReadTuplesRequest) (TuplePage, ConsistencyToken, error) {
	view, reader, token, release, err := e.consistencyView(ctx, minimum, request.TenantID)
	if err != nil {
		return TuplePage{}, ConsistencyToken{}, err
	}
	defer release()
	if err := view.validateFilter(request.Filter); err != nil {
		return TuplePage{}, ConsistencyToken{}, err
	}
	tuples, err := view.queryTuples(ctx, reader, request.TenantID, request.Filter)
	if err != nil {
		return TuplePage{}, ConsistencyToken{}, err
	}
	sortTuples(tuples)
	page, err := pageTuples(tuples, request.PageSize, request.Cursor)
	if err != nil {
		return TuplePage{}, ConsistencyToken{}, err
	}
	page.Revision = token.TupleRevision
	return page, token, nil
}

// LookupResourcesWithConsistency performs a revision-matched reverse-index
// lookup and verifies every candidate against one shared graph snapshot.
func (e *Engine) LookupResourcesWithConsistency(ctx context.Context, minimum ConsistencyToken, request LookupResourcesRequest) (ResourcePage, ConsistencyToken, error) {
	view, reader, token, release, err := e.consistencyView(ctx, minimum, request.TenantID)
	if err != nil {
		return ResourcePage{}, ConsistencyToken{}, err
	}
	defer release()
	if err := view.model.validateCheck(request.User, request.Relation, request.Namespace, "lookup"); err != nil {
		return ResourcePage{}, ConsistencyToken{}, err
	}
	indexed, ok := e.store.(SnapshotResourceCandidateReader)
	if !ok {
		return ResourcePage{}, ConsistencyToken{}, ErrConsistencyUnsupported
	}
	request.Revision = token.TupleRevision
	candidates, indexedAt, err := indexed.LookupResourceCandidatesAt(ctx, token.TupleRevision, request)
	if err != nil {
		return ResourcePage{}, ConsistencyToken{}, wrapStorageError(err)
	}
	if indexedAt != token.TupleRevision {
		return ResourcePage{}, ConsistencyToken{}, ErrIndexSnapshot
	}
	allowed := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, objectID := range candidates {
		if _, duplicate := seen[objectID]; duplicate {
			continue
		}
		seen[objectID] = struct{}{}
		ok, err := view.check(ctx, reader, withAsOf(request.CaveatContext, request.AsOfUnixNano), request.TenantID, request.User, request.Relation, request.Namespace, objectID, make(map[checkKey]struct{}), 0, new(int))
		if err != nil {
			return ResourcePage{}, ConsistencyToken{}, err
		}
		if ok {
			allowed = append(allowed, objectID)
		}
	}
	page, err := pageStrings(allowed, request.PageSize, request.Cursor, token.TupleRevision)
	return page, token, err
}

// LookupSubjectsWithConsistency is LookupResourcesWithConsistency in the
// inverse direction.
func (e *Engine) LookupSubjectsWithConsistency(ctx context.Context, minimum ConsistencyToken, request LookupSubjectsRequest) (SubjectPage, ConsistencyToken, error) {
	view, reader, token, release, err := e.consistencyView(ctx, minimum, request.TenantID)
	if err != nil {
		return SubjectPage{}, ConsistencyToken{}, err
	}
	defer release()
	if request.SubjectNamespace == "" {
		return SubjectPage{}, ConsistencyToken{}, ErrInvalidRequest
	}
	if err := view.model.validateCheck(request.SubjectNamespace+":lookup", request.Relation, request.Namespace, request.ObjectID); err != nil {
		return SubjectPage{}, ConsistencyToken{}, err
	}
	indexed, ok := e.store.(SnapshotSubjectCandidateReader)
	if !ok {
		return SubjectPage{}, ConsistencyToken{}, ErrConsistencyUnsupported
	}
	request.Revision = token.TupleRevision
	candidates, indexedAt, err := indexed.LookupSubjectCandidatesAt(ctx, token.TupleRevision, request)
	if err != nil {
		return SubjectPage{}, ConsistencyToken{}, wrapStorageError(err)
	}
	if indexedAt != token.TupleRevision {
		return SubjectPage{}, ConsistencyToken{}, ErrIndexSnapshot
	}
	subjects := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, subject := range candidates {
		namespace, _, direct := parseDirectSubject(subject)
		if !direct || namespace != request.SubjectNamespace {
			continue
		}
		if _, duplicate := seen[subject]; duplicate {
			continue
		}
		seen[subject] = struct{}{}
		ok, err := view.check(ctx, reader, withAsOf(request.CaveatContext, request.AsOfUnixNano), request.TenantID, subject, request.Relation, request.Namespace, request.ObjectID, make(map[checkKey]struct{}), 0, new(int))
		if err != nil {
			return SubjectPage{}, ConsistencyToken{}, err
		}
		if ok {
			subjects = append(subjects, subject)
		}
	}
	items, next, err := pageStringsRaw(subjects, request.PageSize, request.Cursor)
	if err != nil {
		return SubjectPage{}, ConsistencyToken{}, err
	}
	return SubjectPage{Subjects: items, Revision: token.TupleRevision, NextCursor: next}, token, nil
}

// ExpandWithConsistency returns an expansion from one model-and-tuple view.
func (e *Engine) ExpandWithConsistency(ctx context.Context, minimum ConsistencyToken, tenantID, relation, namespace, objectID string) (Expansion, ConsistencyToken, error) {
	return e.ExpandWithConsistencyAt(ctx, minimum, 0, tenantID, relation, namespace, objectID)
}

// ExpandWithConsistencyAt expands an at-least-as-fresh view at a fixed
// tuple-validity time.
func (e *Engine) ExpandWithConsistencyAt(ctx context.Context, minimum ConsistencyToken, asOfUnixNano int64, tenantID, relation, namespace, objectID string) (Expansion, ConsistencyToken, error) {
	view, reader, token, release, err := e.consistencyView(ctx, minimum, tenantID)
	if err != nil {
		return Expansion{}, ConsistencyToken{}, err
	}
	defer release()
	if err := view.validateFilter(RelationTuple{Namespace: namespace, ObjectID: objectID, Relation: relation}); err != nil {
		return Expansion{}, ConsistencyToken{}, err
	}
	expansion, err := view.expand(ctx, reader, withAsOf(nil, asOfUnixNano), tenantID, relation, namespace, objectID, make(map[checkKey]struct{}), 0)
	return expansion, token, err
}

func (e *Engine) consistencyView(ctx context.Context, minimum ConsistencyToken, tenantID string) (*Engine, TupleReader, ConsistencyToken, func() error, error) {
	if e == nil || e.store == nil || e.modelSource == nil || e.modelID == "" {
		return nil, nil, ConsistencyToken{}, nil, ErrConsistencyUnsupported
	}
	if err := e.validateTenant(tenantID); err != nil {
		return nil, nil, ConsistencyToken{}, nil, err
	}
	if err := minimum.validate(tenantID, e.modelID); err != nil {
		return nil, nil, ConsistencyToken{}, nil, err
	}
	storage, ok := e.store.(AtLeastFreshStorage)
	if !ok {
		return nil, nil, ConsistencyToken{}, nil, ErrConsistencyUnsupported
	}
	reader, revision, release, err := storage.SnapshotAtLeast(ctx, minimum.TupleRevision)
	if err != nil {
		return nil, nil, ConsistencyToken{}, nil, wrapStorageError(err)
	}
	if revision == "" {
		_ = release()
		return nil, nil, ConsistencyToken{}, nil, ErrConsistencyUnsupported
	}
	document, err := e.modelSource.ReadAuthorizationModelAtRevision(ctx, tenantID, e.modelID, revision)
	if err != nil {
		_ = release()
		return nil, nil, ConsistencyToken{}, nil, err
	}
	if document.ID != e.modelID {
		_ = release()
		return nil, nil, ConsistencyToken{}, nil, ErrModelNotFound
	}
	model, err := document.Compile(e.caveats)
	if err != nil {
		_ = release()
		return nil, nil, ConsistencyToken{}, nil, err
	}
	view := *e
	view.model = model
	reader = &memoReader{reader: reader, cache: make(map[tupleFilterKey][]RelationTuple)}
	return &view, reader, ConsistencyToken{TenantID: tenantID, ModelID: e.modelID, ModelVersion: document.Version, TupleRevision: revision}, release, nil
}
