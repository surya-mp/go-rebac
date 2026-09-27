package rebac

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkCheckDirect(b *testing.B) {
	benchmarkCheck(b, testModel(), memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}}, "plan")
}

func BenchmarkCheckNested(b *testing.B) {
	benchmarkCheck(b, testModel(), memoryStore{
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:eng#member"},
		{TenantID: "acme", Namespace: "group", ObjectID: "eng", Relation: "member", User: "user:alice"},
	}, "plan")
}

func BenchmarkCheckRewrite(b *testing.B) {
	for _, rewrite := range []struct {
		name    string
		rewrite Rewrite
	}{
		{"union", Rewrite{Union: []Rewrite{{This: true}, {ComputedUserset: "owner"}}}},
		{"intersection", Rewrite{Intersection: []Rewrite{{This: true}, {ComputedUserset: "owner"}}}},
		{"exclusion", Rewrite{Exclusion: &Exclusion{Base: Rewrite{This: true}, Subtract: Rewrite{ComputedUserset: "banned"}}}},
	} {
		b.Run(rewrite.name, func(b *testing.B) {
			model := testModel()
			document := model.Namespaces["document"]
			viewer := document.Relations["viewer"]
			viewer.Rewrite = rewrite.rewrite
			document.Relations["viewer"] = viewer
			document.Relations["owner"] = RelationDefinition{AllowedSubjects: []SubjectReference{{Namespace: "user"}}}
			document.Relations["banned"] = RelationDefinition{AllowedSubjects: []SubjectReference{{Namespace: "user"}}}
			model.Namespaces["document"] = document
			benchmarkCheck(b, model, memoryStore{
				{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"},
				{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "owner", User: "user:alice"},
			}, "plan")
		})
	}
}

func BenchmarkCheckTupleToUserset(b *testing.B) {
	model := AuthorizationModel{Namespaces: map[string]NamespaceDefinition{
		"user": {},
		"folder": {Relations: map[string]RelationDefinition{
			"viewer": {AllowedSubjects: []SubjectReference{{Namespace: "user"}}},
		}},
		"document": {Relations: map[string]RelationDefinition{
			"parent": {AllowedSubjects: []SubjectReference{{Namespace: "folder"}}},
			"viewer": {Rewrite: Rewrite{TupleToUserset: &TupleToUserset{Tupleset: "parent", ComputedUserset: "viewer"}}},
		}},
	}}
	benchmarkCheck(b, model, memoryStore{
		{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "parent", User: "folder:root"},
		{TenantID: "acme", Namespace: "folder", ObjectID: "root", Relation: "viewer", User: "user:alice"},
	}, "plan")
}

func BenchmarkCheckLargeGraph10K(b *testing.B) {
	benchmarkLargeGraph(b, 10_000)
}

func BenchmarkCheckLargeGraph100K(b *testing.B) {
	benchmarkLargeGraph(b, 100_000)
}

func benchmarkLargeGraph(b *testing.B, size int) {
	tuples := make(memoryStore, size)
	for i := range tuples {
		tuples[i] = RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "large", Relation: "viewer", User: "user:" + fmt.Sprint(i)}
	}
	tuples[len(tuples)-1].User = "user:alice"
	benchmarkCheck(b, testModel(), tuples, "large")
}

func BenchmarkLookupResources(b *testing.B) {
	store := &indexedMemoryStore{revisionMemoryStore: &revisionMemoryStore{memoryStore: memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}}, revision: "1"}, resources: []string{"plan"}}
	engine, err := NewEngine(store, testModel())
	if err != nil {
		b.Fatal(err)
	}
	request := LookupResourcesRequest{TenantID: "acme", User: "user:alice", Relation: "viewer", Namespace: "document"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := engine.LookupResources(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCheck(b *testing.B, model AuthorizationModel, store memoryStore, objectID string) {
	b.Helper()
	engine, err := NewEngine(&store, model)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		allowed, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", objectID)
		if err != nil || !allowed {
			b.Fatalf("Check() = %v, %v", allowed, err)
		}
	}
}
