package rebac

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type memoryStore []RelationTuple

func (s memoryStore) QueryTuples(_ context.Context, filter RelationTuple) ([]RelationTuple, error) {
	var result []RelationTuple
	for _, tuple := range s {
		if (filter.TenantID == "" || filter.TenantID == tuple.TenantID) &&
			(filter.Namespace == "" || filter.Namespace == tuple.Namespace) &&
			(filter.ObjectID == "" || filter.ObjectID == tuple.ObjectID) &&
			(filter.Relation == "" || filter.Relation == tuple.Relation) &&
			(filter.User == "" || filter.User == tuple.User) {
			result = append(result, tuple)
		}
	}
	return result, nil
}

func (s *memoryStore) WriteTuple(_ context.Context, tuple RelationTuple) error {
	*s = append(*s, tuple)
	return nil
}

func (s *memoryStore) DeleteTuple(_ context.Context, tuple RelationTuple) error {
	for i, candidate := range *s {
		if tupleEqual(candidate, tuple) {
			*s = append((*s)[:i], (*s)[i+1:]...)
			break
		}
	}
	return nil
}

func tupleEqual(a, b RelationTuple) bool {
	return a.TenantID == b.TenantID && a.Namespace == b.Namespace && a.ObjectID == b.ObjectID &&
		a.Relation == b.Relation && a.User == b.User && a.Caveat == b.Caveat && reflect.DeepEqual(a.CaveatContext, b.CaveatContext)
}

func TestCheckDirectNestedAndCycle(t *testing.T) {
	store := memoryStore{
		{TenantID: "acme", Namespace: "document", ObjectID: "direct", Relation: "viewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "nested", Relation: "viewer", User: "group:eng#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "cycle", Relation: "viewer", User: "group:a#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "a", Relation: "member", User: "group:b#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "b", Relation: "member", User: "group:a#member"},
	}
	engine, err := NewEngine(&store, testModel())
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		objectID string
		want     bool
	}{
		{"direct", true}, {"nested", true}, {"cycle", false},
	} {
		got, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", test.objectID)
		if err != nil || got != test.want {
			t.Fatalf("Check(%q) = %v, %v; want %v, nil", test.objectID, got, err, test.want)
		}
	}
}

func testModel() AuthorizationModel {
	return AuthorizationModel{Namespaces: map[string]NamespaceDefinition{
		"user": {},
		"group": {Relations: map[string]RelationDefinition{
			"member": {AllowedSubjects: []SubjectReference{{Namespace: "user"}, {Namespace: "group", Relation: "member"}}},
		}},
		"document": {Relations: map[string]RelationDefinition{
			"viewer": {AllowedSubjects: []SubjectReference{{Namespace: "user"}, {Namespace: "group", Relation: "member"}}},
		}},
	}}
}

type snapshotMemoryStore struct{ memoryStore }

func (s *snapshotMemoryStore) QueryTuples(ctx context.Context, filter RelationTuple) ([]RelationTuple, error) {
	return s.memoryStore.QueryTuples(ctx, filter)
}

func (s *snapshotMemoryStore) WriteTuple(ctx context.Context, tuple RelationTuple) error {
	return (&s.memoryStore).WriteTuple(ctx, tuple)
}

func (s *snapshotMemoryStore) DeleteTuple(ctx context.Context, tuple RelationTuple) error {
	return (&s.memoryStore).DeleteTuple(ctx, tuple)
}

func (s *snapshotMemoryStore) Snapshot(context.Context) (TupleReader, func() error, error) {
	return s.memoryStore, func() error { return nil }, nil
}

func TestEngineScopesAndValidatesTuples(t *testing.T) {
	store := &snapshotMemoryStore{}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	for _, tuple := range []RelationTuple{
		{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "group:eng#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:alice"},
	} {
		if err := engine.WriteTuple(context.Background(), tuple); err != nil {
			t.Fatal(err)
		}
	}
	allowed, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", "roadmap")
	if err != nil || !allowed {
		t.Fatalf("Check() = %v, %v; want true, nil", allowed, err)
	}
	if err := engine.WriteTuple(context.Background(), RelationTuple{
		TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "service:mallory",
	}); !errors.Is(err, ErrInvalidTuple) {
		t.Fatalf("WriteTuple() error = %v; want ErrInvalidTuple", err)
	}
}

func TestCheckStopsAtLimit(t *testing.T) {
	store := memoryStore{
		{TenantID: "acme", Namespace: "document", ObjectID: "one", Relation: "viewer", User: "group:a#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "a", Relation: "member", User: "group:b#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "b", Relation: "member", User: "user:alice"},
	}
	engine, err := NewEngine(&store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.WithLimits(1, 10).Check(context.Background(), "acme", "user:alice", "viewer", "document", "one")
	if !errors.Is(err, ErrMaxDepthExceeded) {
		t.Fatalf("Check() error = %v; want ErrMaxDepthExceeded", err)
	}
}

type revisionMemoryStore struct {
	memoryStore
	revision Revision
}

func (s *revisionMemoryStore) QueryTuples(ctx context.Context, filter RelationTuple) ([]RelationTuple, error) {
	return s.memoryStore.QueryTuples(ctx, filter)
}

func (s *revisionMemoryStore) WriteTuple(ctx context.Context, tuple RelationTuple) error {
	return (&s.memoryStore).WriteTuple(ctx, tuple)
}

func (s *revisionMemoryStore) DeleteTuple(ctx context.Context, tuple RelationTuple) error {
	return (&s.memoryStore).DeleteTuple(ctx, tuple)
}

func (s *revisionMemoryStore) SnapshotAt(_ context.Context, revision Revision) (TupleReader, Revision, func() error, error) {
	if revision != "" && revision != s.revision {
		return nil, "", nil, ErrInvalidRevision
	}
	return s.memoryStore, s.revision, func() error { return nil }, nil
}

func (s *revisionMemoryStore) WriteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error) {
	return s.revision, s.WriteTuple(ctx, tuple)
}

func (s *revisionMemoryStore) DeleteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error) {
	return s.revision, s.DeleteTuple(ctx, tuple)
}

func (s *revisionMemoryStore) Mutate(ctx context.Context, changes []TupleChange, preconditions []Precondition) (Revision, error) {
	for _, precondition := range preconditions {
		tuples, err := s.QueryTuples(ctx, precondition.Tuple)
		if err != nil {
			return "", err
		}
		if (len(tuples) != 0) != precondition.MustExist {
			return "", ErrPreconditionFailed
		}
	}
	for _, change := range changes {
		if change.Operation == WriteOperation {
			if err := s.WriteTuple(ctx, change.Tuple); err != nil {
				return "", err
			}
		} else if err := s.DeleteTuple(ctx, change.Tuple); err != nil {
			return "", err
		}
	}
	return s.revision, nil
}

func (s *revisionMemoryStore) Watch(_ context.Context, tenantID string, _ Revision) (<-chan WatchEvent, <-chan error) {
	events := make(chan WatchEvent, 1)
	errorsCh := make(chan error)
	events <- WatchEvent{Revision: s.revision, TenantID: tenantID}
	close(events)
	close(errorsCh)
	return events, errorsCh
}

func TestCheckWithRevision(t *testing.T) {
	store := &revisionMemoryStore{
		memoryStore: memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "user:alice"}},
		revision:    "42",
	}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	allowed, revision, err := engine.CheckWithRevision(context.Background(), "42", "acme", "user:alice", "viewer", "document", "roadmap")
	if err != nil || !allowed || revision != "42" {
		t.Fatalf("CheckWithRevision() = %v, %q, %v; want true, 42, nil", allowed, revision, err)
	}
	if _, _, err := engine.CheckWithRevision(context.Background(), "41", "acme", "user:alice", "viewer", "document", "roadmap"); !errors.Is(err, ErrInvalidRevision) {
		t.Fatalf("CheckWithRevision() error = %v; want ErrInvalidRevision", err)
	}
}

func TestMutateUsesPreconditions(t *testing.T) {
	store := &revisionMemoryStore{revision: "42"}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	tuple := RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "user:alice"}
	revision, err := engine.Mutate(context.Background(), []TupleChange{{Operation: WriteOperation, Tuple: tuple}}, []Precondition{{Tuple: tuple, MustExist: false}})
	if err != nil || revision != "42" {
		t.Fatalf("Mutate() = %q, %v; want 42, nil", revision, err)
	}
	_, err = engine.Mutate(context.Background(), []TupleChange{{Operation: DeleteOperation, Tuple: tuple}}, []Precondition{{Tuple: tuple, MustExist: false}})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("Mutate() error = %v; want ErrPreconditionFailed", err)
	}
}

func TestRelationRewrites(t *testing.T) {
	model := AuthorizationModel{Namespaces: map[string]NamespaceDefinition{
		"user": {},
		"folder": {Relations: map[string]RelationDefinition{
			"viewer": {AllowedSubjects: []SubjectReference{{Namespace: "user"}}},
		}},
		"document": {Relations: map[string]RelationDefinition{
			"editor":   {AllowedSubjects: []SubjectReference{{Namespace: "user"}}},
			"reviewer": {AllowedSubjects: []SubjectReference{{Namespace: "user"}}},
			"blocked":  {AllowedSubjects: []SubjectReference{{Namespace: "user"}}},
			"parent":   {AllowedSubjects: []SubjectReference{{Namespace: "folder"}}},
			"any": {
				AllowedSubjects: []SubjectReference{{Namespace: "user"}},
				Rewrite:         Rewrite{Union: []Rewrite{{This: true}, {ComputedUserset: "editor"}}},
			},
			"all": {Rewrite: Rewrite{Intersection: []Rewrite{{ComputedUserset: "editor"}, {ComputedUserset: "reviewer"}}}},
			"access": {Rewrite: Rewrite{Exclusion: &Exclusion{
				Base: Rewrite{ComputedUserset: "editor"}, Subtract: Rewrite{ComputedUserset: "blocked"},
			}}},
			"via_parent": {Rewrite: Rewrite{TupleToUserset: &TupleToUserset{Tupleset: "parent", ComputedUserset: "viewer"}}},
		}},
	}}
	store := memoryStore{
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "any", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "editor", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "editor", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "reviewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "blocked", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "parent", User: "folder:team"},
		{TenantID: "acme", Namespace: "folder", ObjectID: "team", Relation: "viewer", User: "user:carol"},
	}
	engine, err := NewEngine(&store, model)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		user, relation string
		want           bool
	}{
		{"user:alice", "any", true}, {"user:bob", "any", true},
		{"user:alice", "all", true}, {"user:bob", "all", false},
		{"user:alice", "access", true}, {"user:bob", "access", false},
		{"user:carol", "via_parent", true},
	} {
		allowed, err := engine.Check(context.Background(), "acme", test.user, test.relation, "document", "plan")
		if err != nil || allowed != test.want {
			t.Fatalf("Check(%s, %s) = %v, %v; want %v, nil", test.user, test.relation, allowed, err, test.want)
		}
	}
}

func TestCaveatUsesRequestContext(t *testing.T) {
	model := testModel()
	model.Caveats = map[string]CaveatDefinition{
		"same_region": {Evaluate: func(_ context.Context, tuple, request CaveatContext) (bool, error) {
			return tuple["region"] == request["region"], nil
		}},
	}
	store := memoryStore{{
		TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice",
		Caveat: "same_region", CaveatContext: CaveatContext{"region": "us"},
	}}
	engine, err := NewEngine(&store, model)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", "plan")
	if err != nil || allowed {
		t.Fatalf("Check() = %v, %v; want false, nil", allowed, err)
	}
	allowed, err = engine.CheckWithContext(context.Background(), CaveatContext{"region": "us"}, "acme", "user:alice", "viewer", "document", "plan")
	if err != nil || !allowed {
		t.Fatalf("CheckWithContext() = %v, %v; want true, nil", allowed, err)
	}
}

func TestReadLookupAndExpand(t *testing.T) {
	store := &revisionMemoryStore{
		memoryStore: memoryStore{
			{TenantID: "acme", Namespace: "document", ObjectID: "a", Relation: "viewer", User: "user:alice"},
			{TenantID: "acme", Namespace: "document", ObjectID: "b", Relation: "viewer", User: "user:alice"},
			{TenantID: "acme", Namespace: "document", ObjectID: "b", Relation: "viewer", User: "group:eng#member"},
			{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:bob"},
		},
		revision: "42",
	}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	page, err := engine.ReadTuples(context.Background(), ReadTuplesRequest{TenantID: "acme", Filter: RelationTuple{Namespace: "document"}, PageSize: 1})
	if err != nil || len(page.Tuples) != 1 || page.NextCursor == "" || page.Revision != "42" {
		t.Fatalf("ReadTuples() = %#v, %v", page, err)
	}
	resources, err := engine.LookupResources(context.Background(), LookupResourcesRequest{TenantID: "acme", User: "user:alice", Relation: "viewer", Namespace: "document"})
	if err != nil || !reflect.DeepEqual(resources.ObjectIDs, []string{"a", "b"}) {
		t.Fatalf("LookupResources() = %#v, %v", resources, err)
	}
	subjects, err := engine.LookupSubjects(context.Background(), LookupSubjectsRequest{TenantID: "acme", Namespace: "document", ObjectID: "b", Relation: "viewer", SubjectNamespace: "user"})
	if err != nil || !reflect.DeepEqual(subjects.Subjects, []string{"user:alice", "user:bob"}) {
		t.Fatalf("LookupSubjects() = %#v, %v", subjects, err)
	}
	expansion, revision, err := engine.Expand(context.Background(), "42", "acme", "viewer", "document", "b")
	if err != nil || revision != "42" || len(expansion.Tuples) != 2 || len(expansion.Children) != 1 {
		t.Fatalf("Expand() = %#v, %q, %v", expansion, revision, err)
	}
}

type testObserver struct {
	checks, mutations int
	lastReason        DecisionReason
}

func (o *testObserver) ObserveCheck(_ context.Context, event CheckEvent) {
	o.checks++
	o.lastReason = event.Reason
}

func (o *testObserver) ObserveMutation(context.Context, MutationEvent) { o.mutations++ }

func TestObserverStatsAndBatchReads(t *testing.T) {
	store := &revisionMemoryStore{
		memoryStore: memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}},
		revision:    "42",
	}
	observer := &testObserver{}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	engine = engine.WithObserver(observer)
	if _, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", "plan"); err != nil {
		t.Fatal(err)
	}
	batch, err := engine.ReadTuplesBatch(context.Background(), "acme", "", []RelationTuple{{Namespace: "document", ObjectID: "plan", Relation: "viewer"}})
	if err != nil || len(batch.Tuples) != 1 || len(batch.Tuples[0]) != 1 || batch.Revision != "42" {
		t.Fatalf("ReadTuplesBatch() = %#v, %v", batch, err)
	}
	stats := engine.Stats()
	if observer.checks != 1 || observer.lastReason != DecisionAllowed || stats.Checks != 1 || stats.Allowed != 1 {
		t.Fatalf("observer/stats = %#v, %#v", observer, stats)
	}
}

func TestWatch(t *testing.T) {
	store := &revisionMemoryStore{revision: "42"}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	events, errorsCh := engine.Watch(context.Background(), "acme", "")
	event, ok := <-events
	if !ok || event.Revision != "42" || event.TenantID != "acme" {
		t.Fatalf("Watch event = %#v, open=%v", event, ok)
	}
	if err, ok := <-errorsCh; ok || err != nil {
		t.Fatalf("Watch error = %v, open=%v", err, ok)
	}
}
