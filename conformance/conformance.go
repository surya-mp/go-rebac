// Package conformance provides a portable baseline test suite for
// go-rebac StorageEngine implementations.
package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/surya-mp/go-rebac"
)

// Run verifies direct relationships, nested usersets, tenant isolation, and
// deletion. Invoke it from each datastore adapter's _test.go file.
func Run(t testing.TB, newStore func(testing.TB) rebac.StorageEngine) {
	t.Helper()
	store := newStore(t)
	engine, err := rebac.NewEngine(store, model())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	ctx := context.Background()
	tuples := []rebac.RelationTuple{
		{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:eng#member"},
		{TenantID: "other", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"},
	}
	for _, tuple := range tuples {
		if err := engine.WriteTuple(ctx, tuple); err != nil {
			t.Fatalf("WriteTuple(%+v): %v", tuple, err)
		}
	}
	if allowed, err := engine.Check(ctx, "acme", "user:alice", "viewer", "document", "plan"); err != nil || !allowed {
		t.Fatalf("nested Check = %v, %v; want true, nil", allowed, err)
	}
	if allowed, err := engine.Check(ctx, "other", "user:bob", "viewer", "document", "plan"); err != nil || allowed {
		t.Fatalf("tenant Check = %v, %v; want false, nil", allowed, err)
	}
	if err := engine.DeleteTuple(ctx, tuples[1]); err != nil {
		t.Fatalf("DeleteTuple: %v", err)
	}
	if allowed, err := engine.Check(ctx, "acme", "user:alice", "viewer", "document", "plan"); err != nil || allowed {
		t.Fatalf("deleted Check = %v, %v; want false, nil", allowed, err)
	}
	RunReferenceScenarios(t, newStore)
}

// RunReferenceScenarios verifies the normative evaluator cases every basic
// adapter must preserve. Positive rows establish completeness for one known
// witness; negative rows establish soundness against nearby non-witnesses.
// It intentionally tests the public Engine, so adapters cannot bypass the
// same validation and graph semantics used by application calls.
func RunReferenceScenarios(t testing.TB, newStore func(testing.TB) rebac.StorageEngine) {
	t.Helper()
	store := newStore(t)
	engine, err := rebac.NewEngine(store, referenceModel())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	ctx := context.Background()
	direct := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "direct", Relation: "viewer", User: "user:alice"}
	tuples := []rebac.RelationTuple{
		direct,
		{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "nested", Relation: "viewer", User: "group:eng#member"},
		{TenantID: "acme", Namespace: "document", ObjectID: "public", Relation: "viewer", User: "user:*"},
		{TenantID: "acme", Namespace: "document", ObjectID: "computed", Relation: "owner", User: "user:alice"},
		{TenantID: "acme", Namespace: "folder", ObjectID: "root", Relation: "viewer", User: "user:erin"},
		{TenantID: "acme", Namespace: "document", ObjectID: "inherited", Relation: "parent", User: "folder:root"},
		{TenantID: "acme", Namespace: "document", ObjectID: "intersection", Relation: "viewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "intersection", Relation: "owner", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "union", Relation: "owner", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "restricted", Relation: "viewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "restricted", Relation: "viewer", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "restricted", Relation: "banned", User: "user:alice"},
	}
	for _, tuple := range tuples {
		if err := engine.WriteTuple(ctx, tuple); err != nil {
			t.Fatalf("WriteTuple(%+v): %v", tuple, err)
		}
	}
	for _, scenario := range []struct {
		name, tenant, user, relation, objectID string
		allowed                                bool
	}{
		{"direct", "acme", "user:alice", "viewer", "direct", true},
		{"nested userset", "acme", "user:bob", "viewer", "nested", true},
		{"wildcard", "acme", "user:carol", "viewer", "public", true},
		{"computed userset", "acme", "user:alice", "can_edit", "computed", true},
		{"tuple to userset", "acme", "user:erin", "inherited_viewer", "inherited", true},
		{"intersection", "acme", "user:alice", "both", "intersection", true},
		{"intersection denied", "acme", "user:bob", "both", "intersection", false},
		{"union", "acme", "user:alice", "can_access", "union", true},
		{"exclusion denied", "acme", "user:alice", "restricted", "restricted", false},
		{"exclusion allowed", "acme", "user:bob", "restricted", "restricted", true},
		{"missing tuple", "acme", "user:alice", "viewer", "missing", false},
		{"tenant isolation", "other", "user:alice", "viewer", "direct", false},
	} {
		allowed, err := engine.Check(ctx, scenario.tenant, scenario.user, scenario.relation, "document", scenario.objectID)
		if err != nil || allowed != scenario.allowed {
			t.Fatalf("%s Check = %v, %v; want %v, nil", scenario.name, allowed, err, scenario.allowed)
		}
	}
	if err := engine.DeleteTuple(ctx, direct); err != nil {
		t.Fatalf("DeleteTuple(witness): %v", err)
	}
	if allowed, err := engine.Check(ctx, "acme", "user:alice", "viewer", "document", "direct"); err != nil || allowed {
		t.Fatalf("removed witness Check = %v, %v; want false, nil", allowed, err)
	}
}

// RunRevisioned verifies that a write revision remains readable after a later
// mutation. Invoke it for adapters that implement RevisionedStorage.
func RunRevisioned(t testing.TB, newStore func(testing.TB) (rebac.StorageEngine, rebac.RevisionedStorage)) {
	t.Helper()
	store, revisions := newStore(t)
	ctx := context.Background()
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	revision, err := revisions.WriteTupleWithRevision(ctx, tuple)
	if err != nil || revision == "" {
		t.Fatalf("WriteTupleWithRevision = %q, %v; want revision, nil", revision, err)
	}
	if _, err := revisions.DeleteTupleWithRevision(ctx, tuple); err != nil {
		t.Fatalf("DeleteTupleWithRevision: %v", err)
	}
	reader, used, release, err := revisions.SnapshotAt(ctx, revision)
	if err != nil || used != revision {
		t.Fatalf("SnapshotAt = %v, %q, %v; want revision %q", reader, used, err, revision)
	}
	defer func() {
		if err := release(); err != nil {
			t.Fatalf("Snapshot release: %v", err)
		}
	}()
	tuples, err := reader.QueryTuples(ctx, tuple)
	if err != nil || len(tuples) != 1 {
		t.Fatalf("historical QueryTuples = %v, %v; want one tuple", tuples, err)
	}
	current, err := store.QueryTuples(ctx, tuple)
	if err != nil || len(current) != 0 {
		t.Fatalf("current QueryTuples = %v, %v; want no tuple", current, err)
	}
}

// RunProduction verifies the complete production adapter capability contract.
// Database-specific tests must additionally prove durability and behavior
// through failover; Go interfaces cannot establish those physical properties.
func RunProduction(t testing.TB, newStore func(testing.TB) rebac.ProductionStorage) {
	t.Helper()
	RunRevisioned(t, func(tb testing.TB) (rebac.StorageEngine, rebac.RevisionedStorage) {
		store := newStore(tb)
		return store, store
	})
	RunMutations(t, func(tb testing.TB) (rebac.StorageEngine, rebac.MutationStorage) {
		store := newStore(tb)
		return store, store
	})
	RunCandidateReaders(t, func(tb testing.TB) (rebac.StorageEngine, rebac.ResourceCandidateReader, rebac.SubjectCandidateReader) {
		store := newStore(tb)
		return store, store, store
	})
	RunWatches(t, func(tb testing.TB) (rebac.StorageEngine, rebac.WatchStorage) {
		store := newStore(tb)
		return store, store
	})
	RunGlobalWatches(t, func(tb testing.TB) rebac.ProductionStorage { return newStore(tb) })
	RunSnapshotDiscipline(t, func(tb testing.TB) (rebac.StorageEngine, rebac.RevisionedStorage) {
		store := newStore(tb)
		return store, store
	})
}

// RunSnapshotDiscipline proves that a revision-pinned recursive Check opens
// exactly one snapshot and uses its reader for every graph edge.
func RunSnapshotDiscipline(t testing.TB, newStore func(testing.TB) (rebac.StorageEngine, rebac.RevisionedStorage)) {
	t.Helper()
	store, revisions := newStore(t)
	guard := &snapshotGuard{StorageEngine: store, RevisionedStorage: revisions}
	engine, err := rebac.NewEngine(guard, model())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	ctx := context.Background()
	if _, err := guard.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	revision, err := guard.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:eng#member"})
	if err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := engine.CheckWithRevision(ctx, revision, "acme", "user:alice", "viewer", "document", "plan"); err != nil || !allowed {
		t.Fatalf("snapshot Check = %v, %v; want true, nil", allowed, err)
	}
	if guard.snapshots != 1 {
		t.Fatalf("SnapshotAt calls = %d; want 1", guard.snapshots)
	}
}

type snapshotGuard struct {
	rebac.StorageEngine
	rebac.RevisionedStorage
	snapshots int
}

func (s *snapshotGuard) SnapshotAt(ctx context.Context, revision rebac.Revision) (rebac.TupleReader, rebac.Revision, func() error, error) {
	s.snapshots++
	return s.RevisionedStorage.SnapshotAt(ctx, revision)
}

// RunGlobalWatches verifies that one atomic multi-tenant mutation is replayed
// as one globally checkpointable event.
func RunGlobalWatches(t testing.TB, newStore func(testing.TB) rebac.ProductionStorage) {
	t.Helper()
	store := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := store.Mutate(ctx, []rebac.TupleChange{
		{Operation: rebac.WriteOperation, Tuple: rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "one", Relation: "viewer", User: "user:alice"}},
		{Operation: rebac.WriteOperation, Tuple: rebac.RelationTuple{TenantID: "other", Namespace: "document", ObjectID: "two", Relation: "viewer", User: "user:bob"}},
	}, nil); err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	events, errs := store.WatchAllTuples(ctx, rebac.GlobalWatchRequest{After: "0"})
	select {
	case event := <-events:
		if event.Revision == "" || event.TenantID != "" || len(event.Changes) != 2 {
			t.Fatalf("global event = %#v; want one two-tenant mutation", event)
		}
	case err := <-errs:
		t.Fatalf("global watch error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("global watch did not replay mutation")
	}
}

// RunMutations verifies atomic preconditions and mutation visibility. Invoke
// it for adapters that implement MutationStorage.
func RunMutations(t testing.TB, newStore func(testing.TB) (rebac.StorageEngine, rebac.MutationStorage)) {
	t.Helper()
	store, mutations := newStore(t)
	ctx := context.Background()
	first := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "one", Relation: "viewer", User: "user:alice"}
	second := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "two", Relation: "viewer", User: "user:alice"}
	if _, err := mutations.Mutate(ctx, []rebac.TupleChange{{Operation: rebac.WriteOperation, Tuple: first}}, []rebac.Precondition{{Tuple: first, MustExist: false}}); err != nil {
		t.Fatalf("initial Mutate: %v", err)
	}
	if tuples, err := store.QueryTuples(ctx, first); err != nil || len(tuples) != 1 {
		t.Fatalf("written tuple = %v, %v; want one tuple", tuples, err)
	}
	if _, err := mutations.Mutate(ctx, []rebac.TupleChange{{Operation: rebac.WriteOperation, Tuple: second}}, []rebac.Precondition{{Tuple: second, MustExist: true}}); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("failed Mutate error = %v; want ErrPreconditionFailed", err)
	}
	if tuples, err := store.QueryTuples(ctx, second); err != nil || len(tuples) != 0 {
		t.Fatalf("failed mutation wrote tuple = %v, %v; want none", tuples, err)
	}
}

// RunModelStorage verifies immutable model versions and compare-and-set
// updates. Invoke it for adapters that implement ModelStorage.
func RunModelStorage(t testing.TB, newStore func(testing.TB) rebac.ModelStorage) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	document := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	stored, err := store.WriteAuthorizationModel(ctx, "acme", document, "")
	if err != nil || stored.Version == "" {
		t.Fatalf("create model = %#v, %v; want versioned model", stored, err)
	}
	if _, err := store.WriteAuthorizationModel(ctx, "acme", document, ""); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("duplicate create error = %v; want ErrPreconditionFailed", err)
	}
	updated, err := store.WriteAuthorizationModel(ctx, "acme", document, stored.Version)
	if err != nil || updated.Version == stored.Version {
		t.Fatalf("update model = %#v, %v; want a new version", updated, err)
	}
	historical, err := store.ReadAuthorizationModel(ctx, "acme", document.ID, stored.Version)
	if err != nil || historical.Version != stored.Version {
		t.Fatalf("historical model = %#v, %v; want version %q", historical, err, stored.Version)
	}
}

// RunActiveModels verifies atomic published-model activation. Invoke it for
// adapters that implement ActiveModelStorage.
func RunActiveModels(t testing.TB, newStore func(testing.TB) rebac.ActiveModelStorage) {
	t.Helper()
	store := newStore(t)
	ctx := context.Background()
	document := rebac.ModelDocument{ID: "access", State: rebac.ModelPublished, Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	first, err := store.WriteAuthorizationModel(ctx, "acme", document, "")
	if err != nil {
		t.Fatalf("create model: %v", err)
	}
	second, err := store.WriteAuthorizationModel(ctx, "acme", document, first.Version)
	if err != nil {
		t.Fatalf("update model: %v", err)
	}
	if _, _, err := store.ActivateAuthorizationModel(ctx, "acme", document.ID, "", first.Version); err != nil {
		t.Fatalf("initial activation: %v", err)
	}
	if _, _, err := store.ActivateAuthorizationModel(ctx, "acme", document.ID, "", second.Version); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("stale activation error = %v; want ErrPreconditionFailed", err)
	}
	if active, _, err := store.ActivateAuthorizationModel(ctx, "acme", document.ID, first.Version, second.Version); err != nil || active.Version != second.Version {
		t.Fatalf("activate new version = %#v, %v", active, err)
	}
}

// RunCandidateReaders verifies that optional candidate indexes include known
// direct resource and subject candidates.
func RunCandidateReaders(t testing.TB, newStore func(testing.TB) (rebac.StorageEngine, rebac.ResourceCandidateReader, rebac.SubjectCandidateReader)) {
	t.Helper()
	store, resources, subjects := newStore(t)
	ctx := context.Background()
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	if err := store.WriteTuple(ctx, tuple); err != nil {
		t.Fatalf("WriteTuple: %v", err)
	}
	resourceCandidates, err := resources.LookupResourceCandidates(ctx, rebac.LookupResourcesRequest{TenantID: "acme", Namespace: "document", Relation: "viewer", User: "user:alice"})
	if err != nil || !contains(resourceCandidates, "plan") {
		t.Fatalf("resource candidates = %v, %v; want plan", resourceCandidates, err)
	}
	subjectCandidates, err := subjects.LookupSubjectCandidates(ctx, rebac.LookupSubjectsRequest{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", SubjectNamespace: "user"})
	if err != nil || !contains(subjectCandidates, "user:alice") {
		t.Fatalf("subject candidates = %v, %v; want user:alice", subjectCandidates, err)
	}
}

// RunWatches verifies that a committed tenant mutation reaches a watch.
func RunWatches(t testing.TB, newStore func(testing.TB) (rebac.StorageEngine, rebac.WatchStorage)) {
	t.Helper()
	store, watches := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, errorsCh := watches.Watch(ctx, "acme", "")
	if err := store.WriteTuple(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatalf("WriteTuple: %v", err)
	}
	select {
	case event, ok := <-events:
		if !ok || event.TenantID != "acme" || event.Revision == "" {
			t.Fatalf("watch event = %#v, open=%v", event, ok)
		}
	case err, ok := <-errorsCh:
		if !ok {
			t.Fatal("watch closed without event")
		}
		t.Fatalf("watch error: %v", err)
	case <-ctx.Done():
		t.Fatal("watch timed out")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// RunConsistent verifies the portable Zanzibar-style path. The supplied
// stores must share one externally consistent revision sequence; this suite
// verifies the library-visible protocol, not a datastore's replication claims.
func RunConsistent(t testing.TB, newStore func(testing.TB) (rebac.ConsistentStorage, rebac.RevisionedModelStorage)) {
	t.Helper()
	store, models := newStore(t)
	ctx := context.Background()
	document := rebac.ModelDocument{ID: "document_access", Model: model()}
	stored, _, err := models.WriteAuthorizationModelWithRevision(ctx, "acme", document, "")
	if err != nil {
		t.Fatalf("WriteAuthorizationModelWithRevision: %v", err)
	}
	engine, _, err := rebac.NewConsistentEngineFromModelStorage(ctx, store, models, rebac.ModelSelection{TenantID: "acme", ModelID: stored.ID}, nil)
	if err != nil {
		t.Fatalf("NewConsistentEngineFromModelStorage: %v", err)
	}
	if err := engine.WriteTuple(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatalf("WriteTuple: %v", err)
	}
	allowed, token, err := engine.ContentChangeCheck(ctx, rebac.ContentChangeCheckRequest{TenantID: "acme", User: "user:alice", Relation: "viewer", Namespace: "document", ObjectID: "plan"})
	if err != nil || !allowed || token.TupleRevision == "" || token.ModelVersion == "" {
		t.Fatalf("ContentChangeCheck = %v, %#v, %v; want true, populated token, nil", allowed, token, err)
	}
	if _, _, err := models.WriteAuthorizationModelWithRevision(ctx, "acme", document, stored.Version); err != nil {
		t.Fatalf("WriteAuthorizationModelWithRevision(new version): %v", err)
	}
	historical, err := models.ReadAuthorizationModelAtRevision(ctx, "acme", stored.ID, token.TupleRevision)
	if err != nil || historical.Version != token.ModelVersion {
		t.Fatalf("model at token revision = %#v, %v; want version %q", historical, err, token.ModelVersion)
	}
	allowed, _, err = engine.CheckWithConsistency(ctx, token, "acme", "user:alice", "viewer", "document", "plan")
	if err != nil || !allowed {
		t.Fatalf("CheckWithConsistency = %v, %v; want true, nil", allowed, err)
	}
}

func model() rebac.AuthorizationModel {
	return rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
		"user": {},
		"group": {Relations: map[string]rebac.RelationDefinition{
			"member": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}, {Namespace: "group", Relation: "member"}}},
		}},
		"document": {Relations: map[string]rebac.RelationDefinition{
			"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}, {Namespace: "group", Relation: "member"}}},
		}},
	}}
}

func referenceModel() rebac.AuthorizationModel {
	return rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
		"user": {},
		"group": {Relations: map[string]rebac.RelationDefinition{
			"member": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}, {Namespace: "group", Relation: "member"}}},
		}},
		"folder": {Relations: map[string]rebac.RelationDefinition{
			"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
		}},
		"document": {Relations: map[string]rebac.RelationDefinition{
			"viewer":           {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}, {Namespace: "group", Relation: "member"}}, AllowWildcard: true},
			"owner":            {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
			"banned":           {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
			"parent":           {AllowedSubjects: []rebac.SubjectReference{{Namespace: "folder"}}},
			"can_edit":         {Rewrite: rebac.Rewrite{ComputedUserset: "owner"}},
			"inherited_viewer": {Rewrite: rebac.Rewrite{TupleToUserset: &rebac.TupleToUserset{Tupleset: "parent", ComputedUserset: "viewer"}}},
			"both":             {Rewrite: rebac.Rewrite{Intersection: []rebac.Rewrite{{ComputedUserset: "viewer"}, {ComputedUserset: "owner"}}}},
			"can_access":       {Rewrite: rebac.Rewrite{Union: []rebac.Rewrite{{ComputedUserset: "viewer"}, {ComputedUserset: "owner"}}}},
			"restricted":       {Rewrite: rebac.Rewrite{Exclusion: &rebac.Exclusion{Base: rebac.Rewrite{ComputedUserset: "viewer"}, Subtract: rebac.Rewrite{ComputedUserset: "banned"}}}},
		}},
	}}
}
