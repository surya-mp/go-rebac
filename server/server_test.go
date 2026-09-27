package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/surya-mp/go-rebac"
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
}
