package rebac

import (
	"context"
	"errors"
	"sort"
	"strconv"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1_000
)

// ReadTuplesRequest selects tuples from one tenant. Cursor is returned by a
// previous call; callers should retain Revision for stable multi-page reads.
type ReadTuplesRequest struct {
	TenantID string
	Filter   RelationTuple
	Revision Revision
	PageSize int
	Cursor   string
}

// TuplePage is one deterministic page of tuple results.
type TuplePage struct {
	Tuples     []RelationTuple
	Revision   Revision
	NextCursor string
}

// TupleBatch is one revision-consistent result set for independent filters.
type TupleBatch struct {
	Tuples   [][]RelationTuple
	Revision Revision
}

// LookupResourcesRequest asks which objects grant a subject one relation.
type LookupResourcesRequest struct {
	TenantID, User, Relation, Namespace string
	Revision                            Revision
	CaveatContext                       CaveatContext
	PageSize                            int
	Cursor                              string
}

// ResourcePage is one deterministic page of authorized object IDs.
type ResourcePage struct {
	ObjectIDs  []string
	Revision   Revision
	NextCursor string
}

// LookupSubjectsRequest asks which direct subjects of one namespace have a
// relation on a resource.
type LookupSubjectsRequest struct {
	TenantID, Namespace, ObjectID, Relation, SubjectNamespace string
	Revision                                                  Revision
	CaveatContext                                             CaveatContext
	PageSize                                                  int
	Cursor                                                    string
}

// SubjectPage is one deterministic page of allowed direct subjects.
type SubjectPage struct {
	Subjects   []string
	Revision   Revision
	NextCursor string
}

// Expansion represents direct relationship edges and nested usersets for one
// relation. Rewrite describes the model operation applied at this node.
type Expansion struct {
	Namespace, ObjectID, Relation string
	Rewrite                       Rewrite
	Tuples                        []RelationTuple
	Children                      []Expansion
}

// ReadTuples returns a page of model-valid tuples. It is intended for
// administration and debugging; authorization decisions should use Check.
func (e *Engine) ReadTuples(ctx context.Context, request ReadTuplesRequest) (TuplePage, error) {
	if e == nil || e.store == nil {
		return TuplePage{}, errors.New("rebac: storage engine is nil")
	}
	if request.TenantID == "" {
		return TuplePage{}, ErrTenantRequired
	}
	if err := e.validateFilter(request.Filter); err != nil {
		return TuplePage{}, err
	}
	reader, revision, release, err := e.readView(ctx, request.Revision)
	if err != nil {
		return TuplePage{}, err
	}
	defer release()
	if paged, ok := reader.(PagedTupleReader); ok {
		limit, offset, err := pageBounds(request.PageSize, request.Cursor)
		if err != nil {
			return TuplePage{}, err
		}
		tuples, hasMore, err := paged.QueryTuplesPage(ctx, withTenant(request.TenantID, request.Filter), limit, offset)
		if err != nil {
			return TuplePage{}, wrapStorageError(err)
		}
		tuples, err = e.validateTuples(request.TenantID, tuples)
		if err != nil {
			return TuplePage{}, err
		}
		next := ""
		if hasMore {
			next = strconv.Itoa(offset + len(tuples))
		}
		return TuplePage{Tuples: tuples, Revision: revision, NextCursor: next}, nil
	}
	tuples, err := e.queryTuples(ctx, reader, request.TenantID, request.Filter)
	if err != nil {
		return TuplePage{}, err
	}
	sortTuples(tuples)
	page, err := pageTuples(tuples, request.PageSize, request.Cursor)
	if err != nil {
		return TuplePage{}, err
	}
	page.Revision = revision
	return page, nil
}

// ReadTuplesBatch reads independent filters through one storage call when the
// selected reader supports batching, and otherwise falls back to sequential reads.
func (e *Engine) ReadTuplesBatch(ctx context.Context, tenantID string, revision Revision, filters []RelationTuple) (TupleBatch, error) {
	if e == nil || e.store == nil {
		return TupleBatch{}, errors.New("rebac: storage engine is nil")
	}
	if tenantID == "" {
		return TupleBatch{}, ErrTenantRequired
	}
	for _, filter := range filters {
		if err := e.validateFilter(filter); err != nil {
			return TupleBatch{}, err
		}
	}
	reader, used, release, err := e.readView(ctx, revision)
	if err != nil {
		return TupleBatch{}, err
	}
	defer release()
	result := make([][]RelationTuple, len(filters))
	if batch, ok := reader.(BatchTupleReader); ok {
		for i := range filters {
			filters[i] = withTenant(tenantID, filters[i])
		}
		result, err = batch.QueryTuplesBatch(ctx, filters)
		if err != nil {
			return TupleBatch{}, wrapStorageError(err)
		}
		for i := range result {
			result[i], err = e.validateTuples(tenantID, result[i])
			if err != nil {
				return TupleBatch{}, err
			}
		}
		return TupleBatch{Tuples: result, Revision: used}, nil
	}
	for i, filter := range filters {
		result[i], err = e.queryTuples(ctx, reader, tenantID, filter)
		if err != nil {
			return TupleBatch{}, err
		}
	}
	return TupleBatch{Tuples: result, Revision: used}, nil
}

// LookupResources returns object IDs for which user is allowed relation.
func (e *Engine) LookupResources(ctx context.Context, request LookupResourcesRequest) (ResourcePage, error) {
	if err := e.model.validateCheck(request.User, request.Relation, request.Namespace, "lookup"); err != nil {
		return ResourcePage{}, err
	}
	tuples, revision, err := e.readAllTuples(ctx, request.TenantID, request.Revision, RelationTuple{Namespace: request.Namespace})
	if err != nil {
		return ResourcePage{}, err
	}
	candidates := uniqueObjectIDs(tuples)
	allowed := make([]string, 0, len(candidates))
	for _, objectID := range candidates {
		ok, _, err := e.CheckWithRevisionAndContext(ctx, revision, request.CaveatContext, request.TenantID, request.User, request.Relation, request.Namespace, objectID)
		if err != nil {
			return ResourcePage{}, err
		}
		if ok {
			allowed = append(allowed, objectID)
		}
	}
	return pageStrings(allowed, request.PageSize, request.Cursor, revision)
}

// LookupSubjects returns direct subjects of SubjectNamespace with the relation.
func (e *Engine) LookupSubjects(ctx context.Context, request LookupSubjectsRequest) (SubjectPage, error) {
	if request.SubjectNamespace == "" {
		return SubjectPage{}, ErrInvalidRequest
	}
	if err := e.model.validateCheck(request.SubjectNamespace+":lookup", request.Relation, request.Namespace, request.ObjectID); err != nil {
		return SubjectPage{}, err
	}
	tuples, revision, err := e.readAllTuples(ctx, request.TenantID, request.Revision, RelationTuple{})
	if err != nil {
		return SubjectPage{}, err
	}
	candidates := make(map[string]struct{})
	for _, tuple := range tuples {
		namespace, _, ok := parseDirectSubject(tuple.User)
		if ok && namespace == request.SubjectNamespace {
			candidates[tuple.User] = struct{}{}
		}
	}
	subjects := make([]string, 0, len(candidates))
	for subject := range candidates {
		ok, _, err := e.CheckWithRevisionAndContext(ctx, revision, request.CaveatContext, request.TenantID, subject, request.Relation, request.Namespace, request.ObjectID)
		if err != nil {
			return SubjectPage{}, err
		}
		if ok {
			subjects = append(subjects, subject)
		}
	}
	items, next, err := pageStringsRaw(subjects, request.PageSize, request.Cursor)
	if err != nil {
		return SubjectPage{}, err
	}
	return SubjectPage{Subjects: items, Revision: revision, NextCursor: next}, nil
}

// Expand returns direct tuples and userset edges for one relation at revision.
func (e *Engine) Expand(ctx context.Context, revision Revision, tenantID, relation, namespace, objectID string) (Expansion, Revision, error) {
	if tenantID == "" {
		return Expansion{}, "", ErrTenantRequired
	}
	if err := e.validateFilter(RelationTuple{Namespace: namespace, ObjectID: objectID, Relation: relation}); err != nil {
		return Expansion{}, "", err
	}
	reader, used, release, err := e.readView(ctx, revision)
	if err != nil {
		return Expansion{}, "", err
	}
	defer release()
	expansion, err := e.expand(ctx, reader, tenantID, relation, namespace, objectID, make(map[checkKey]struct{}), 0)
	return expansion, used, err
}

func (e *Engine) expand(ctx context.Context, reader TupleReader, tenantID, relation, namespace, objectID string, visiting map[checkKey]struct{}, depth int) (Expansion, error) {
	if depth > e.maxDepth {
		return Expansion{}, ErrMaxDepthExceeded
	}
	key := checkKey{tenantID: tenantID, relation: relation, namespace: namespace, objectID: objectID}
	if _, ok := visiting[key]; ok {
		return Expansion{Namespace: namespace, ObjectID: objectID, Relation: relation}, nil
	}
	visiting[key] = struct{}{}
	defer delete(visiting, key)

	tuples, err := e.queryTuples(ctx, reader, tenantID, RelationTuple{Namespace: namespace, ObjectID: objectID, Relation: relation})
	if err != nil {
		return Expansion{}, err
	}
	expansion := Expansion{Namespace: namespace, ObjectID: objectID, Relation: relation, Rewrite: e.model.Namespaces[namespace].Relations[relation].Rewrite, Tuples: tuples}
	for _, tuple := range tuples {
		childNamespace, childObjectID, childRelation, ok := parseUserset(tuple.User)
		if !ok {
			continue
		}
		child, err := e.expand(ctx, reader, tenantID, childRelation, childNamespace, childObjectID, visiting, depth+1)
		if err != nil {
			return Expansion{}, err
		}
		expansion.Children = append(expansion.Children, child)
	}
	return expansion, nil
}

func (e *Engine) readView(ctx context.Context, revision Revision) (TupleReader, Revision, func() error, error) {
	if revisions, ok := e.store.(RevisionedStorage); ok {
		return revisions.SnapshotAt(ctx, revision)
	}
	if revision != "" {
		return nil, "", nil, ErrRevisionsUnsupported
	}
	if snapshots, ok := e.store.(SnapshotStorage); ok {
		reader, release, err := snapshots.Snapshot(ctx)
		if err != nil {
			return nil, "", nil, wrapStorageError(err)
		}
		return reader, "", release, nil
	}
	return e.store, "", func() error { return nil }, nil
}

func (e *Engine) readAllTuples(ctx context.Context, tenantID string, revision Revision, filter RelationTuple) ([]RelationTuple, Revision, error) {
	reader, used, release, err := e.readView(ctx, revision)
	if err != nil {
		return nil, "", err
	}
	defer release()
	tuples, err := e.queryTuples(ctx, reader, tenantID, filter)
	return tuples, used, err
}

func (e *Engine) validateFilter(filter RelationTuple) error {
	if filter.Namespace == "" && filter.Relation != "" {
		return ErrInvalidRequest
	}
	if filter.Namespace != "" {
		definition, ok := e.model.Namespaces[filter.Namespace]
		if !ok {
			return ErrInvalidTuple
		}
		if filter.Relation != "" {
			if _, ok := definition.Relations[filter.Relation]; !ok {
				return ErrInvalidTuple
			}
		}
	}
	if filter.User != "" {
		if _, err := e.model.subjectReference(filter.User); err != nil {
			return err
		}
	}
	return nil
}

func sortTuples(tuples []RelationTuple) {
	sort.Slice(tuples, func(i, j int) bool {
		a, b := tuples[i], tuples[j]
		return a.Namespace+"\x00"+a.ObjectID+"\x00"+a.Relation+"\x00"+a.User < b.Namespace+"\x00"+b.ObjectID+"\x00"+b.Relation+"\x00"+b.User
	})
}

func uniqueObjectIDs(tuples []RelationTuple) []string {
	objects := make(map[string]struct{})
	for _, tuple := range tuples {
		objects[tuple.ObjectID] = struct{}{}
	}
	result := make([]string, 0, len(objects))
	for objectID := range objects {
		result = append(result, objectID)
	}
	sort.Strings(result)
	return result
}

func pageTuples(tuples []RelationTuple, size int, cursor string) (TuplePage, error) {
	items, next, err := pageSlice(tuples, size, cursor)
	return TuplePage{Tuples: items, NextCursor: next}, err
}

func pageStrings(items []string, size int, cursor string, revision Revision) (ResourcePage, error) {
	page, next, err := pageStringsRaw(items, size, cursor)
	return ResourcePage{ObjectIDs: page, Revision: revision, NextCursor: next}, err
}

func pageStringsRaw(items []string, size int, cursor string) ([]string, string, error) {
	sort.Strings(items)
	return pageSlice(items, size, cursor)
}

func pageSlice[T any](items []T, size int, cursor string) ([]T, string, error) {
	size, start, err := pageBounds(size, cursor)
	if err != nil || start > len(items) {
		return nil, "", ErrInvalidRequest
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

func pageBounds(size int, cursor string) (int, int, error) {
	if size <= 0 {
		size = defaultPageSize
	}
	if size > maxPageSize {
		return 0, 0, ErrInvalidRequest
	}
	if cursor == "" {
		return size, 0, nil
	}
	offset, err := strconv.Atoi(cursor)
	if err != nil || offset < 0 {
		return 0, 0, ErrInvalidRequest
	}
	return size, offset, nil
}

func withTenant(tenantID string, filter RelationTuple) RelationTuple {
	filter.TenantID = tenantID
	return filter
}
