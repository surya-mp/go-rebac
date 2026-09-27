package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	rebac "github.com/surya-mp/go-rebac"
)

func TestTupleWriteAndCheck(t *testing.T) {
	dir := t.TempDir()
	modelPath, tuplePath, storePath := filepath.Join(dir, "model.json"), filepath.Join(dir, "tuple.json"), filepath.Join(dir, "store.json")
	document := rebac.ModelDocument{ID: "access", Model: rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
		"user":     {},
		"document": {Relations: map[string]rebac.RelationDefinition{"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}}}},
	}}}
	tuple := rebac.RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	modelJSON, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	tupleJSON, err := json.Marshal(tuple)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(modelPath, modelJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tuplePath, tupleJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := run([]string{"tuple", "write", "--store", storePath, "--model", modelPath, "--tuple", tuplePath}, &output, &errors); code != 0 {
		t.Fatalf("tuple write = %d: %s", code, errors.String())
	}
	output.Reset()
	if code := run([]string{"check", "--store", storePath, "--model", modelPath, "--tenant", "acme", "--user", "user:alice", "--relation", "viewer", "--namespace", "document", "--object", "plan"}, &output, &errors); code != 0 {
		t.Fatalf("check = %d: %s", code, errors.String())
	}
	var response struct{ Allowed bool }
	if err := json.Unmarshal(output.Bytes(), &response); err != nil || !response.Allowed {
		t.Fatalf("check output = %q, %v; want allowed", output.String(), err)
	}
}
