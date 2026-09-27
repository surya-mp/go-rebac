// Package conformance provides a portable baseline test suite for
// go-rebac StorageEngine implementations.
package conformance

import (
	"context"
	"testing"

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
