package kv_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/conformance"
	"github.com/surya-mp/go-rebac/kv"
)

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
	if _, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, ""); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("duplicate create error = %v; want ErrPreconditionFailed", err)
	}
	if _, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, "wrong"); !errors.Is(err, rebac.ErrPreconditionFailed) {
		t.Fatalf("stale update error = %v; want ErrPreconditionFailed", err)
	}
	if _, _, err := store.WriteAuthorizationModelWithRevision(ctx, "acme", model, stored.Version); err != nil {
		t.Fatal(err)
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
