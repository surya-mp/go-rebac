package kv_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/conformance"
	"github.com/surya-mp/go-rebac/kv"
)

type hostBackend struct {
	database          *kv.Database
	snapshots, writes int
}

type flakyBackend struct {
	*hostBackend
	conflicts int
}

func (b *flakyBackend) Transaction(ctx context.Context) (kv.WriteTransaction, error) {
	tx, err := b.hostBackend.Transaction(ctx)
	if err != nil {
		return nil, err
	}
	return &flakyTransaction{WriteTransaction: tx, backend: b}, nil
}

type flakyTransaction struct {
	kv.WriteTransaction
	backend *flakyBackend
}

type modelWriteBackend struct {
	*hostBackend
	modelWrites, activeWrites int
}

func (b *modelWriteBackend) Transaction(ctx context.Context) (kv.WriteTransaction, error) {
	tx, err := b.hostBackend.Transaction(ctx)
	if err != nil {
		return nil, err
	}
	return &modelWriteTransaction{WriteTransaction: tx, backend: b}, nil
}

type modelWriteTransaction struct {
	kv.WriteTransaction
	backend *modelWriteBackend
}

func (t *modelWriteTransaction) Set(ctx context.Context, key string, value []byte) error {
	if strings.HasPrefix(key, "go-rebac/model/") {
		t.backend.modelWrites++
	}
	if strings.HasPrefix(key, "go-rebac/model-active/") {
		t.backend.activeWrites++
	}
	return t.WriteTransaction.Set(ctx, key, value)
}

func (t *flakyTransaction) Commit(ctx context.Context) (uint64, error) {
	if t.backend.conflicts > 0 {
		t.backend.conflicts--
		return 0, kv.ErrTransactionConflict
	}
	return t.WriteTransaction.Commit(ctx)
}

func (b *hostBackend) Snapshot(ctx context.Context) (kv.Reader, error) {
	b.snapshots++
	return b.database.NewSnapshot(ctx)
}

func (b *hostBackend) Transaction(ctx context.Context) (kv.WriteTransaction, error) {
	b.writes++
	return b.database.NewTransaction(ctx)
}

func TestReBACStoreUsesHostBackend(t *testing.T) {
	backend := &hostBackend{database: kv.New()}
	store := kv.NewReBACStore(backend)
	ctx := context.Background()
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	if err := store.WriteTuple(ctx, tuple); err != nil {
		t.Fatal(err)
	}
	if _, err := store.QueryTuples(ctx, tuple); err != nil {
		t.Fatal(err)
	}
	if backend.writes == 0 || backend.snapshots == 0 {
		t.Fatalf("backend usage = %d writes, %d snapshots; want both", backend.writes, backend.snapshots)
	}
}

func TestReBACStoreRetriesTransactionConflict(t *testing.T) {
	backend := &flakyBackend{hostBackend: &hostBackend{database: kv.New()}, conflicts: 1}
	store := kv.NewReBACStore(backend)
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	if revision, err := store.WriteTupleWithRevision(context.Background(), tuple); err != nil || revision != "1" {
		t.Fatalf("WriteTupleWithRevision() = %q, %v; want 1, nil", revision, err)
	}
	if backend.writes != 2 {
		t.Fatalf("transactions = %d; want retry", backend.writes)
	}
}

func TestReBACStoreConfiguresConflictRetriesAndKeyPrefix(t *testing.T) {
	backend := &flakyBackend{hostBackend: &hostBackend{database: kv.New()}, conflicts: 1}
	store := kv.NewReBACStoreWithOptions(backend, kv.ReBACStoreOptions{KeyPrefix: "application/authz", TransactionRetries: 1})
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	if _, err := store.WriteTupleWithRevision(context.Background(), tuple); err != nil {
		t.Fatal(err)
	}
	if backend.writes != 2 {
		t.Fatalf("transactions = %d; want one configured retry", backend.writes)
	}
	snapshot, err := backend.database.NewSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	keys, err := snapshot.Ascend(context.Background(), "")
	if err != nil || len(keys) == 0 || !strings.HasPrefix(keys[0], "application/authz/go-rebac/") {
		t.Fatalf("prefixed keys = %v, %v", keys, err)
	}
}

func TestReBACStoreStoresLiveTuplesAsForwardAndReverseRecords(t *testing.T) {
	database := kv.New()
	store := kv.NewReBACStore(database)
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	revision, err := store.WriteTupleWithRevision(context.Background(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := database.NewSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	raw, ok, err := snapshot.Get(context.Background(), "go-rebac/state")
	if err != nil || !ok {
		t.Fatalf("current state = %v, %v, %v", raw, ok, err)
	}
	var current struct {
		TupleLayout int                   `json:"tuple_layout"`
		Tuples      []rebac.RelationTuple `json:"tuples"`
	}
	if err := json.Unmarshal(raw, &current); err != nil || current.TupleLayout != 1 || len(current.Tuples) != 0 {
		t.Fatalf("current state = %#v, %v; want tuple layout without embedded tuples", current, err)
	}
	if keys, err := snapshot.Ascend(context.Background(), "go-rebac/tuple/"); err != nil || len(keys) != 1 {
		t.Fatalf("forward tuple keys = %v, %v; want one", keys, err)
	}
	if keys, err := snapshot.Ascend(context.Background(), "go-rebac/subject/"); err != nil || len(keys) != 1 {
		t.Fatalf("reverse tuple keys = %v, %v; want one", keys, err)
	}
	if keys, err := snapshot.Ascend(context.Background(), "go-rebac/resource/"); err != nil || len(keys) != 1 {
		t.Fatalf("resource candidate keys = %v, %v; want one", keys, err)
	}
	if candidates, err := store.LookupResourceCandidates(context.Background(), rebac.LookupResourcesRequest{TenantID: "acme", Namespace: "document"}); err != nil || len(candidates) != 1 || candidates[0] != "plan" {
		t.Fatalf("record-backed resource candidates = %v, %v; want [plan], nil", candidates, err)
	}
	if candidates, err := store.LookupSubjectCandidates(context.Background(), rebac.LookupSubjectsRequest{TenantID: "acme"}); err != nil || len(candidates) != 1 || candidates[0] != "user:alice" {
		t.Fatalf("record-backed subject candidates = %v, %v; want [user:alice], nil", candidates, err)
	}
	if _, ok, err := snapshot.Get(context.Background(), "go-rebac/state/"+string(revision)); err != nil || ok {
		t.Fatalf("whole-state history = exists:%v, err:%v; want absent", ok, err)
	}
	if _, ok, err := snapshot.Get(context.Background(), "go-rebac/revision/"+string(revision)); err != nil || !ok {
		t.Fatalf("revision marker = exists:%v, err:%v; want present", ok, err)
	}
	if keys, err := snapshot.Ascend(context.Background(), "go-rebac/tuple-history/"); err != nil || len(keys) != 1 {
		t.Fatalf("tuple history keys = %v, %v; want one", keys, err)
	}
}

func TestReBACStoreRebuildsSnapshotsFromRecordHistory(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	created, err := store.WriteTupleWithRevision(ctx, tuple)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := store.DeleteTupleWithRevision(ctx, tuple)
	if err != nil {
		t.Fatal(err)
	}
	before, _, closeBefore, err := store.SnapshotAt(ctx, created)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBefore()
	if tuples, err := before.QueryTuples(ctx, tuple); err != nil || len(tuples) != 1 {
		t.Fatalf("snapshot before delete = %v, %v; want tuple", tuples, err)
	}
	after, _, closeAfter, err := store.SnapshotAt(ctx, removed)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAfter()
	if tuples, err := after.QueryTuples(ctx, tuple); err != nil || len(tuples) != 0 {
		t.Fatalf("snapshot after delete = %v, %v; want none", tuples, err)
	}
}

func TestReBACStoreCompactsExpiredHistoryAtRecordCheckpoint(t *testing.T) {
	store := kv.NewReBACStoreWithOptions(kv.New(), kv.ReBACStoreOptions{HistoryRevisions: 2})
	ctx := context.Background()
	first, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "one", Relation: "viewer", User: "user:alice"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "two", Relation: "viewer", User: "user:alice"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "three", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.SnapshotAt(ctx, first); !errors.Is(err, rebac.ErrInvalidRevision) {
		t.Fatalf("expired SnapshotAt() error = %v; want ErrInvalidRevision", err)
	}
	reader, _, release, err := store.SnapshotAt(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if tuples, err := reader.QueryTuples(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document"}); err != nil || len(tuples) != 2 {
		t.Fatalf("retained snapshot = %v, %v; want two tuples", tuples, err)
	}
}

func TestReBACStoreCleansEmptyCandidateMarker(t *testing.T) {
	database := kv.New()
	store := kv.NewReBACStore(database)
	ctx := context.Background()
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	if _, err := store.WriteTupleWithRevision(ctx, tuple); err != nil {
		t.Fatal(err)
	}
	revision, err := store.DeleteTupleWithRevision(ctx, tuple)
	if err != nil {
		t.Fatal(err)
	}
	if candidates, err := store.LookupResourceCandidates(ctx, rebac.LookupResourcesRequest{TenantID: "acme", Namespace: "document"}); err != nil || len(candidates) != 0 {
		t.Fatalf("candidates after delete = %v, %v; want none", candidates, err)
	}
	if indexedAt, err := store.CandidateIndexRevision(ctx); err != nil || indexedAt != revision {
		t.Fatalf("CandidateIndexRevision() = %q, %v; want %q, nil", indexedAt, err, revision)
	}
}

func TestReBACStoreStoresModelVersionsAndActivePointerAsRecords(t *testing.T) {
	database := kv.New()
	store := kv.NewReBACStore(database)
	ctx := context.Background()
	document := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	stored, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", document, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ActivateAuthorizationModel(ctx, "acme", "access", "", stored.Version); err != nil {
		t.Fatal(err)
	}
	snapshot, err := database.NewSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	raw, ok, err := snapshot.Get(ctx, "go-rebac/state")
	if err != nil || !ok {
		t.Fatalf("current state = %v, %v, %v", raw, ok, err)
	}
	var current struct {
		ModelLayout  int                              `json:"model_layout"`
		Models       map[string][]rebac.ModelDocument `json:"models"`
		ActiveModels map[string]rebac.Revision        `json:"active_models"`
	}
	if err := json.Unmarshal(raw, &current); err != nil || current.ModelLayout != 1 || len(current.Models) != 0 || len(current.ActiveModels) != 0 {
		t.Fatalf("current state = %#v, %v; want model layout without embedded records", current, err)
	}
	if keys, err := snapshot.Ascend(ctx, "go-rebac/model/"); err != nil || len(keys) != 1 {
		t.Fatalf("model version keys = %v, %v; want one", keys, err)
	}
	if keys, err := snapshot.Ascend(ctx, "go-rebac/model-active/"); err != nil || len(keys) != 1 {
		t.Fatalf("active model keys = %v, %v; want one", keys, err)
	}
}

func TestReBACStoreWritesOnlyChangedModelRecords(t *testing.T) {
	backend := &modelWriteBackend{hostBackend: &hostBackend{database: kv.New()}}
	store := kv.NewReBACStore(backend)
	ctx := context.Background()
	document := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	stored, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", document, "")
	if err != nil {
		t.Fatal(err)
	}
	if backend.modelWrites != 1 || backend.activeWrites != 0 {
		t.Fatalf("initial model writes = %d/%d; want 1/0", backend.modelWrites, backend.activeWrites)
	}
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	if backend.modelWrites != 1 || backend.activeWrites != 0 {
		t.Fatalf("tuple mutation rewrote models = %d/%d; want 1/0", backend.modelWrites, backend.activeWrites)
	}
	if _, _, err := store.ActivateAuthorizationModel(ctx, "acme", stored.ID, "", stored.Version); err != nil {
		t.Fatal(err)
	}
	if backend.modelWrites != 1 || backend.activeWrites != 1 {
		t.Fatalf("activation writes = %d/%d; want 1/1", backend.modelWrites, backend.activeWrites)
	}
}

func TestReBACStore(t *testing.T) {
	conformance.Run(t, func(testing.TB) rebac.StorageEngine {
		return kv.NewReBACStore(kv.New())
	})
}

func TestReBACStoreConsistent(t *testing.T) {
	conformance.RunConsistent(t, func(testing.TB) (rebac.ConsistentStorage, rebac.RevisionedModelStorage) {
		store := kv.NewReBACStore(kv.New())
		return store, store
	})
}

func TestReBACStoreCapabilities(t *testing.T) {
	conformance.RunProduction(t, func(testing.TB) rebac.ProductionStorage {
		return kv.NewReBACStore(kv.New())
	})
	conformance.RunRevisioned(t, func(testing.TB) (rebac.StorageEngine, rebac.RevisionedStorage) {
		store := kv.NewReBACStore(kv.New())
		return store, store
	})
	conformance.RunMutations(t, func(testing.TB) (rebac.StorageEngine, rebac.MutationStorage) {
		store := kv.NewReBACStore(kv.New())
		return store, store
	})
	conformance.RunModelStorage(t, func(testing.TB) rebac.ModelStorage {
		return kv.NewReBACStore(kv.New())
	})
	conformance.RunActiveModels(t, func(testing.TB) rebac.ActiveModelStorage {
		return kv.NewReBACStore(kv.New())
	})
	conformance.RunCandidateReaders(t, func(testing.TB) (rebac.StorageEngine, rebac.ResourceCandidateReader, rebac.SubjectCandidateReader) {
		store := kv.NewReBACStore(kv.New())
		return store, store, store
	})
	conformance.RunWatches(t, func(testing.TB) (rebac.StorageEngine, rebac.WatchStorage) {
		store := kv.NewReBACStore(kv.New())
		return store, store
	})
}

func TestReBACStoreKeepsRevisionedCandidates(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	first, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "first", Relation: "viewer", User: "user:alice"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "second", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	candidates, indexedAt, err := store.LookupResourceCandidatesAt(ctx, first, rebac.LookupResourcesRequest{TenantID: "acme", Namespace: "document"})
	if err != nil || indexedAt != first || len(candidates) != 1 || candidates[0] != "first" {
		t.Fatalf("LookupResourceCandidatesAt() = %v, %q, %v; want [first], %q, nil", candidates, indexedAt, err, first)
	}
}

func TestReBACStoreWatchResumesAndFilters(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	first, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "first", Relation: "viewer", User: "user:alice"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "folder", ObjectID: "ignored", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "second", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, errs := store.WatchTuples(watchCtx, rebac.WatchRequest{TenantID: "acme", After: first, Namespaces: []string{"document"}})
	if event := nextEvent(t, events, errs); event.Changes[0].Tuple.ObjectID != "second" {
		t.Fatalf("replayed event = %+v; want document:second", event)
	}
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "third", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	if event := nextEvent(t, events, errs); event.TenantID != "acme" || event.Changes[0].Tuple.ObjectID != "third" {
		t.Fatalf("live event = %+v; want acme document:third", event)
	}
}

func TestReBACStoreGlobalWatchUsesOneEventPerCommitAndSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rebac.json")
	database, err := kv.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store := kv.NewReBACStore(database)
	ctx := context.Background()
	if _, err := store.Mutate(ctx, []rebac.TupleChange{
		{Operation: rebac.WriteOperation, Tuple: rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "one", Relation: "viewer", User: "user:alice"}},
		{Operation: rebac.WriteOperation, Tuple: rebac.RelationTuple{TenantID: "other", Namespace: "document", ObjectID: "two", Relation: "viewer", User: "user:bob"}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	reopened, err := kv.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, errs := kv.NewReBACStore(reopened).WatchAllTuples(watchCtx, rebac.GlobalWatchRequest{After: "0"})
	event := nextEvent(t, events, errs)
	if event.TenantID != "" || len(event.Changes) != 2 || event.Revision == "" {
		t.Fatalf("global event = %+v; want one two-tenant committed event", event)
	}
}

func TestOpenPersistsReBACStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rebac.json")
	database, err := kv.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store := kv.NewReBACStore(database)
	ctx := context.Background()
	if _, err := store.WriteTupleWithRevision(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
	reopened, err := kv.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tuples, err := kv.NewReBACStore(reopened).QueryTuples(ctx, rebac.RelationTuple{TenantID: "acme"})
	if err != nil || len(tuples) != 1 || tuples[0].ObjectID != "roadmap" {
		t.Fatalf("reopened tuples = %v, %v; want [roadmap], nil", tuples, err)
	}
}

func TestDeleteObject(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	for _, tuple := range []rebac.RelationTuple{
		{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "editor", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "other", Relation: "viewer", User: "user:alice"},
	} {
		if _, err := store.WriteTupleWithRevision(ctx, tuple); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.DeleteObject(ctx, "acme", "document", "roadmap"); err != nil {
		t.Fatal(err)
	}
	tuples, err := store.QueryTuples(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document"})
	if err != nil || len(tuples) != 1 || tuples[0].ObjectID != "other" {
		t.Fatalf("tuples after DeleteObject = %v, %v; want [other], nil", tuples, err)
	}
}

func TestCollectGarbageRemovesInboundReferences(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	for _, tuple := range []rebac.RelationTuple{
		{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:alice"},
		{TenantID: "acme", Namespace: "group", ObjectID: "ops", Relation: "member", User: "user:bob"},
		{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "group:eng#member"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:ops"},
	} {
		if _, err := store.WriteTupleWithRevision(ctx, tuple); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.DeleteObject(ctx, "acme", "group", "eng"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteObject(ctx, "acme", "group", "ops"); err != nil {
		t.Fatal(err)
	}
	if collected, err := store.CollectGarbage(ctx, 1); err != nil || collected != 1 {
		t.Fatalf("CollectGarbage() = %d, %v; want 1, nil", collected, err)
	}
	tuples, err := store.QueryTuples(ctx, rebac.RelationTuple{TenantID: "acme"})
	if err != nil || len(tuples) != 1 || tuples[0].ObjectID != "plan" {
		t.Fatalf("tuples after first CollectGarbage = %v, %v; want [plan], nil", tuples, err)
	}
	if collected, err := store.CollectGarbage(ctx, 1); err != nil || collected != 1 {
		t.Fatalf("second CollectGarbage() = %d, %v; want 1, nil", collected, err)
	}
	tuples, err = store.QueryTuples(ctx, rebac.RelationTuple{TenantID: "acme"})
	if err != nil || len(tuples) != 0 {
		t.Fatalf("tuples after second CollectGarbage = %v, %v; want none, nil", tuples, err)
	}
}

func TestAuthorizationModelCompareAndSet(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	model := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	stored, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, "")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Version == "" || stored.ParentVersion != "" || stored.CreatedAtUnixNano == 0 || stored.Checksum == "" {
		t.Fatalf("stored model metadata = %#v; want initial immutable metadata", stored)
	}
	if stored.State != rebac.ModelPublished {
		t.Fatalf("stored model state = %q; want published", stored.State)
	}
	if checksum, err := stored.ComputeChecksum(); err != nil || checksum != stored.Checksum {
		t.Fatalf("stored checksum = %q, %v; want %q, nil", checksum, err, stored.Checksum)
	}
	if _, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, ""); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("duplicate create error = %v; want ErrPreconditionFailed", err)
	}
	if _, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, "wrong"); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("stale update error = %v; want ErrPreconditionFailed", err)
	}
	updated, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, stored.Version)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version == stored.Version || updated.ParentVersion != stored.Version || updated.Checksum != stored.Checksum {
		t.Fatalf("updated model metadata = %#v; want a new version parented by %q", updated, stored.Version)
	}
	if historical, err := store.ReadAuthorizationModel(ctx, "acme", model.ID, stored.Version); err != nil || historical.Version != stored.Version || historical.ParentVersion != "" {
		t.Fatalf("historical model = %#v, %v; want immutable initial version", historical, err)
	}
	versions, err := store.ListAuthorizationModelVersions(ctx, "acme", model.ID)
	if err != nil || len(versions) != 2 || versions[0].Version != stored.Version || versions[1].Version != updated.Version {
		t.Fatalf("ListAuthorizationModelVersions() = %#v, %v; want ordered immutable history", versions, err)
	}
}

func TestAuthorizationModelActivationAndRollback(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	model := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	first, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, "")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, first.Version)
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := store.ActivateAuthorizationModel(ctx, "acme", model.ID, "", first.Version)
	if err != nil || active.Version != first.Version {
		t.Fatalf("initial activation = %#v, %v; want first version", active, err)
	}
	if _, _, err := store.ActivateAuthorizationModel(ctx, "acme", model.ID, "", second.Version); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("stale activation error = %v; want ErrPreconditionFailed", err)
	}
	if _, secondActiveRevision, err := store.ActivateAuthorizationModel(ctx, "acme", model.ID, first.Version, second.Version); err != nil {
		t.Fatal(err)
	} else if document, err := store.ReadAuthorizationModelAtRevision(ctx, "acme", model.ID, secondActiveRevision); err != nil || document.Version != second.Version {
		t.Fatalf("model at second activation = %#v, %v; want version %q", document, err, second.Version)
	}
	if active, err = store.ReadActiveAuthorizationModel(ctx, "acme", model.ID); err != nil || active.Version != second.Version {
		t.Fatalf("active model = %#v, %v; want second version", active, err)
	}
	if _, rollbackRevision, err := store.ActivateAuthorizationModel(ctx, "acme", model.ID, second.Version, first.Version); err != nil {
		t.Fatal(err)
	} else if document, err := store.ReadAuthorizationModelAtRevision(ctx, "acme", model.ID, rollbackRevision); err != nil || document.Version != first.Version {
		t.Fatalf("model at rollback = %#v, %v; want version %q", document, err, first.Version)
	}
	if active, err = store.ReadActiveAuthorizationModel(ctx, "acme", model.ID); err != nil || active.Version != first.Version {
		t.Fatalf("rolled back model = %#v, %v; want first version", active, err)
	}
}

func TestAuthorizationModelActivationRetriesConflict(t *testing.T) {
	backend := &flakyBackend{hostBackend: &hostBackend{database: kv.New()}}
	store := kv.NewReBACStore(backend)
	ctx := context.Background()
	model := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	stored, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, "")
	if err != nil {
		t.Fatal(err)
	}
	before := backend.writes
	backend.conflicts = 1
	active, _, err := store.ActivateAuthorizationModel(ctx, "acme", model.ID, "", stored.Version)
	if err != nil || active.Version != stored.Version || backend.writes != before+2 {
		t.Fatalf("ActivateAuthorizationModel() = %#v, %v; transactions=%d, want retry", active, err, backend.writes)
	}
}

func TestAuthorizationModelLifecycle(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	draft := rebac.ModelDocument{ID: "access", State: rebac.ModelDraft, Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}
	storedDraft, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", draft, "")
	if err != nil || storedDraft.State != rebac.ModelDraft {
		t.Fatalf("store draft = %#v, %v", storedDraft, err)
	}
	if _, _, err := store.ActivateAuthorizationModel(ctx, "acme", draft.ID, "", storedDraft.Version); !errors.Is(err, rebac.ErrModelNotPublished) {
		t.Fatalf("activate draft error = %v; want ErrModelNotPublished", err)
	}
	published := draft
	published.State = rebac.ModelPublished
	storedPublished, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", published, storedDraft.Version)
	if err != nil {
		t.Fatal(err)
	}
	if active, _, err := store.ActivateAuthorizationModel(ctx, "acme", draft.ID, "", storedPublished.Version); err != nil || active.State != rebac.ModelPublished {
		t.Fatalf("activate published = %#v, %v", active, err)
	}
}

func TestGrantReplacesTupleValidityWindow(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	ctx := context.Background()
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "roadmap", Relation: "viewer", User: "user:alice", NotBeforeUnixNano: 100, NotAfterUnixNano: 200}
	if _, err := store.WriteTupleWithRevision(ctx, tuple); err != nil {
		t.Fatal(err)
	}
	tuple.NotBeforeUnixNano, tuple.NotAfterUnixNano = 200, 300
	if _, err := store.WriteTupleWithRevision(ctx, tuple); err != nil {
		t.Fatal(err)
	}
	tuples, err := store.QueryTuples(ctx, rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "roadmap"})
	if err != nil || len(tuples) != 1 || tuples[0].NotBeforeUnixNano != 200 || tuples[0].NotAfterUnixNano != 300 {
		t.Fatalf("regranted tuple = %v, %v; want one tuple with [200,300)", tuples, err)
	}
}

func nextEvent(t *testing.T, events <-chan rebac.WatchEvent, errs <-chan error) rebac.WatchEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("watch closed")
		}
		return event
	case err := <-errs:
		t.Fatalf("watch error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}
	return rebac.WatchEvent{}
}
