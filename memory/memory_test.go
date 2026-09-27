package memory

import (
	"context"
	"testing"

	"github.com/surya-mp/go-rebac"
)

func TestNewSupportsProductionStorage(t *testing.T) {
	store := New()
	engine, err := rebac.NewProductionEngine(store, rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
		"user":     {},
		"document": {Relations: map[string]rebac.RelationDefinition{"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.WriteTuple(context.Background(), rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
}
