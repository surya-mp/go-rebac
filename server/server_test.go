package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/kv"
)

type memoryStore []rebac.RelationTuple

func (s memoryStore) QueryTuples(_ context.Context, filter rebac.RelationTuple) ([]rebac.RelationTuple, error) {
	var result []rebac.RelationTuple
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

func (s *memoryStore) WriteTuple(_ context.Context, tuple rebac.RelationTuple) error {
	*s = append(*s, tuple)
	return nil
}

func (s *memoryStore) DeleteTuple(_ context.Context, tuple rebac.RelationTuple) error {
	for i, candidate := range *s {
		if candidate.TenantID == tuple.TenantID && candidate.Namespace == tuple.Namespace && candidate.ObjectID == tuple.ObjectID && candidate.Relation == tuple.Relation && candidate.User == tuple.User {
			*s = append((*s)[:i], (*s)[i+1:]...)
			break
		}
	}
	return nil
}

func TestHandlers(t *testing.T) {
	store := &memoryStore{}
	engine, err := rebac.NewEngine(store, rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
		"user": {},
		"document": {Relations: map[string]rebac.RelationDefinition{
			"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}

	body := `{"tuple":{"tenant_id":"acme","namespace":"document","object_id":"plan","relation":"viewer","user":"user:alice"}}`
	write := httptest.NewRecorder()
	server.AdminHandler().ServeHTTP(write, httptest.NewRequest(http.MethodPost, "/v1/tuples/write", strings.NewReader(body)))
	if write.Code != http.StatusOK {
		t.Fatalf("write status = %d: %s", write.Code, write.Body.String())
	}

	check := httptest.NewRecorder()
	server.Handler().ServeHTTP(check, httptest.NewRequest(http.MethodPost, "/v1/check", strings.NewReader(`{"tenant_id":"acme","namespace":"document","object_id":"plan","relation":"viewer","user":"user:alice"}`)))
	if check.Code != http.StatusOK {
		t.Fatalf("check status = %d: %s", check.Code, check.Body.String())
	}
	var response CheckResponse
	if err := json.Unmarshal(check.Body.Bytes(), &response); err != nil || !response.Allowed {
		t.Fatalf("check response = %#v, %v", response, err)
	}

	batch := httptest.NewRecorder()
	server.Handler().ServeHTTP(batch, httptest.NewRequest(http.MethodPost, "/v1/batch-check", strings.NewReader(`{"checks":[{"tenant_id":"acme","namespace":"document","object_id":"plan","relation":"viewer","user":"user:alice"},{"tenant_id":"acme","namespace":"document","object_id":"plan","relation":"viewer","user":"user:bob"}]}`)))
	var batchResponse BatchCheckResponse
	if batch.Code != http.StatusOK || json.Unmarshal(batch.Body.Bytes(), &batchResponse) != nil || len(batchResponse.Checks) != 2 || !batchResponse.Checks[0].Allowed || batchResponse.Checks[1].Allowed {
		t.Fatalf("batch response = %d: %s", batch.Code, batch.Body.String())
	}

	limited := server.WithRequestLimits(RequestLimits{MaxBatchSize: 1})
	tooLarge := httptest.NewRecorder()
	limited.Handler().ServeHTTP(tooLarge, httptest.NewRequest(http.MethodPost, "/v1/batch-check", strings.NewReader(`{"checks":[{},{}]}`)))
	if tooLarge.Code != http.StatusBadRequest {
		t.Fatalf("batch limit status = %d: %s", tooLarge.Code, tooLarge.Body.String())
	}
}

func TestClientUsesDataEndpoints(t *testing.T) {
	store := &memoryStore{}
	engine, err := rebac.NewEngine(store, rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
		"user":     {},
		"document": {Relations: map[string]rebac.RelationDefinition{"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/", handlers.Handler())
	mux.Handle("/v1/tuples/", handlers.AdminHandler())
	remote := httptest.NewServer(mux)
	defer remote.Close()
	client, err := NewClient(remote.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	if _, err := client.WriteTuple(context.Background(), tuple); err != nil {
		t.Fatal(err)
	}
	response, err := client.Check(context.Background(), CheckRequest{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"})
	if err != nil || !response.Allowed {
		t.Fatalf("Check() = %#v, %v", response, err)
	}
}

func TestClientUsesModelEndpoints(t *testing.T) {
	engine, err := rebac.NewEngine(&memoryStore{}, rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}})
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	handlers = handlers.WithModelStorage(kv.NewReBACStore(kv.New()))
	mux := http.NewServeMux()
	mux.Handle("POST /v1/models/read", handlers.Handler())
	mux.Handle("POST /v1/models/versions", handlers.Handler())
	mux.Handle("POST /v1/models/active", handlers.Handler())
	mux.Handle("POST /v1/models/write", handlers.AdminHandler())
	mux.Handle("POST /v1/models/activate", handlers.AdminHandler())
	remote := httptest.NewServer(mux)
	defer remote.Close()
	client, err := NewClient(remote.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := client.WriteModel(context.Background(), WriteModelRequest{TenantID: "acme", Document: rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{"user": {}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ActivateModel(context.Background(), ActivateModelRequest{TenantID: "acme", ModelID: "access", Version: stored.Version}); err != nil {
		t.Fatal(err)
	}
	active, err := client.ReadActiveModel(context.Background(), ReadModelRequest{TenantID: "acme", ModelID: "access"})
	if err != nil || active.Version != stored.Version {
		t.Fatalf("ReadActiveModel() = %#v, %v", active, err)
	}
}
