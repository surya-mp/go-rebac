package rebac

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type staticModelStorage struct{ document ModelDocument }

func (s staticModelStorage) ReadAuthorizationModel(_ context.Context, tenantID, modelID string, version Revision) (ModelDocument, error) {
	if tenantID != "acme" || modelID != s.document.ID || (version != "" && version != s.document.Version) {
		return ModelDocument{}, ErrModelNotFound
	}
	return s.document, nil
}

func (s staticModelStorage) WriteAuthorizationModel(_ context.Context, _ string, document ModelDocument, _ Revision) (ModelDocument, error) {
	return document, nil
}

func (s staticModelStorage) ReadAuthorizationModelAtRevision(ctx context.Context, tenantID, modelID string, _ Revision) (ModelDocument, error) {
	return s.ReadAuthorizationModel(ctx, tenantID, modelID, "")
}

func (s staticModelStorage) WriteAuthorizationModelWithRevision(ctx context.Context, tenantID string, document ModelDocument, expected Revision) (ModelDocument, Revision, error) {
	stored, err := s.WriteAuthorizationModel(ctx, tenantID, document, expected)
	return stored, "42", err
}

func TestTupleSyntax(t *testing.T) {
	valid := RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "roadmap:v2", Relation: "viewer", User: "group:eng#member"}
	if err := valid.ValidateSyntax(); err != nil {
		t.Fatalf("ValidateSyntax() = %v", err)
	}
	for _, tuple := range []RelationTuple{
		{TenantID: "acme", Namespace: "Document", ObjectID: "plan", Relation: "viewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:eng#member#bad"},
		{TenantID: "acme", Namespace: "document", ObjectID: "with space", Relation: "viewer", User: "user:alice"},
	} {
		if !errors.Is(tuple.ValidateSyntax(), ErrInvalidTuple) {
			t.Fatalf("ValidateSyntax(%+v) did not reject invalid tuple", tuple)
		}
	}
}

func TestModelDocumentJSONAndCompile(t *testing.T) {
	document := ModelDocument{ID: "document_access", Model: AuthorizationModel{
		Namespaces: testModel().Namespaces,
		Caveats:    map[string]CaveatDefinition{"same_region": {}},
	}}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var restored ModelDocument
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	model, err := restored.Compile(map[string]CaveatDefinition{
		"same_region": {Evaluate: func(context.Context, CaveatContext, CaveatContext) (bool, error) { return true, nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine(&memoryStore{}, model); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Compile(nil); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("Compile() error = %v; want ErrInvalidModel", err)
	}
}

func TestProductionEngineLoadsSelectedModel(t *testing.T) {
	store := &indexedMemoryStore{revisionMemoryStore: &revisionMemoryStore{revision: "42"}}
	models := staticModelStorage{document: ModelDocument{ID: "document_access", Version: "7", Model: testModel()}}
	engine, document, err := NewProductionEngineFromModelStorage(context.Background(), store, models, ModelSelection{TenantID: "acme", ModelID: "document_access", Version: "7"}, nil)
	if err != nil || document.Version != "7" || engine == nil {
		t.Fatalf("NewProductionEngineFromModelStorage() = %v, %#v, %v", engine, document, err)
	}
	if _, err := engine.Check(context.Background(), "other", "user:alice", "viewer", "document", "plan"); !errors.Is(err, ErrModelTenant) {
		t.Fatalf("tenant-bound Check() error = %v; want ErrModelTenant", err)
	}
}
