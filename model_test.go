package rebac

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCompiledModelCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := newCompiledModelCache(2)
	first := compiledModelKey{modelID: "first"}
	second := compiledModelKey{modelID: "second"}
	third := compiledModelKey{modelID: "third"}
	cache.put(first, &CompiledModel{})
	cache.put(second, &CompiledModel{})
	if cache.get(first) == nil {
		t.Fatal("first cache entry missing")
	}
	cache.put(third, &CompiledModel{})
	if cache.get(second) != nil || cache.get(first) == nil || cache.get(third) == nil {
		t.Fatalf("cache contents = %#v; want first and third", cache.models)
	}
}

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
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:*"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:*#member"},
		{TenantID: "acme", Namespace: "document", ObjectID: strings.Repeat("a", 1025), Relation: "viewer", User: "user:alice"},
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice", Caveat: "approved", CaveatContext: CaveatContext{"value": strings.Repeat("a", maxCaveatContextSize)}},
	} {
		if !errors.Is(tuple.ValidateSyntax(), ErrInvalidTuple) {
			t.Fatalf("ValidateSyntax(%+v) did not reject invalid tuple", tuple)
		}
	}
	if err := testModel().ValidateTuple(valid); err != nil {
		t.Fatalf("ValidateTuple() = %v", err)
	}
}

func TestLintModel(t *testing.T) {
	model := testModel()
	document := model.Namespaces["document"]
	document.Relations["empty"] = RelationDefinition{}
	model.Namespaces["document"] = document

	issues := LintModel(model)
	var empty, recursive bool
	for _, issue := range issues {
		if issue.Path == "document#empty" && issue.Severity == LintWarning {
			empty = true
		}
		if issue.Path == "group#member" && issue.Severity == LintWarning {
			recursive = true
		}
	}
	if !empty || !recursive {
		t.Fatalf("LintModel() = %#v; want empty and recursive warnings", issues)
	}
	issues = LintModel(AuthorizationModel{})
	if len(issues) != 1 || issues[0].Severity != LintError {
		t.Fatalf("LintModel(invalid) = %#v; want one error", issues)
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
	restored.Checksum = "tampered"
	if err := restored.Validate(); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("tampered Validate() error = %v; want ErrInvalidModel", err)
	}
}

func TestModelDocumentStateValidation(t *testing.T) {
	document := ModelDocument{ID: "document_access", State: "unknown", Model: testModel()}
	if err := document.Validate(); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("invalid model state = %v; want ErrInvalidModel", err)
	}
	document.State = ModelDraft
	checksum, err := document.ComputeChecksum()
	if err != nil {
		t.Fatal(err)
	}
	document.Checksum, document.State = checksum, ModelPublished
	if err := document.Validate(); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("tampered model state = %v; want ErrInvalidModel", err)
	}
}

func TestModelRejectsOversizedDocument(t *testing.T) {
	relations := make(map[string]RelationDefinition, 70_000)
	for i := range 70_000 {
		relations[fmt.Sprintf("relation%05d", i)] = RelationDefinition{}
	}
	model := AuthorizationModel{Namespaces: map[string]NamespaceDefinition{
		"user":     {},
		"document": {Relations: relations},
	}}
	if err := model.Validate(); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("oversized model = %v; want ErrInvalidModel", err)
	}
}

func TestEngineCompilesImmutableModel(t *testing.T) {
	model := testModel()
	engine, err := NewEngine(&memoryStore{}, model)
	if err != nil {
		t.Fatal(err)
	}
	document := model.Namespaces["document"]
	document.Relations = nil
	model.Namespaces["document"] = document
	if err := engine.WriteTuple(context.Background(), RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatalf("WriteTuple() after caller mutation = %v", err)
	}
}

func TestCompileModelDependencies(t *testing.T) {
	compiled, err := CompileModel(testModel())
	if err != nil {
		t.Fatal(err)
	}
	if dependencies := compiled.Dependencies("document", "viewer"); !reflect.DeepEqual(dependencies, []string{"group#member"}) {
		t.Fatalf("document dependencies = %v; want group#member", dependencies)
	}
	if dependencies := compiled.Dependencies("group", "member"); !reflect.DeepEqual(dependencies, []string{"group#member"}) {
		t.Fatalf("group dependencies = %v; want group#member", dependencies)
	}
	if dependents := compiled.Dependents("group", "member"); !reflect.DeepEqual(dependents, []string{"document#viewer", "group#member"}) {
		t.Fatalf("group dependents = %v; want document#viewer, group#member", dependents)
	}
	engine, err := NewEngineWithCompiledModel(&memoryStore{}, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.WriteTuple(context.Background(), RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateModelTransition(t *testing.T) {
	old := testModel()
	compatible := testModel()
	document := compatible.Namespaces["document"]
	document.Relations["editor"] = RelationDefinition{AllowedSubjects: []SubjectReference{{Namespace: "user"}}}
	compatible.Namespaces["document"] = document
	if err := ValidateModelTransition(old, compatible); err != nil {
		t.Fatalf("compatible transition = %v", err)
	}
	incompatible := testModel()
	document = incompatible.Namespaces["document"]
	delete(document.Relations, "viewer")
	incompatible.Namespaces["document"] = document
	if err := ValidateModelTransition(old, incompatible); !errors.Is(err, ErrInvalidModelTransition) {
		t.Fatalf("removed relation transition = %v; want ErrInvalidModelTransition", err)
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
	observer := &testObserver{}
	if _, err := engine.WithObserver(observer).Check(context.Background(), "acme", "user:alice", "viewer", "document", "plan"); err != nil {
		t.Fatal(err)
	}
	if observer.lastCheck.ModelID != "document_access" || observer.lastCheck.ModelVersion != "7" {
		t.Fatalf("CheckEvent model = %#v; want document_access@7", observer.lastCheck)
	}
}
