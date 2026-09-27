package rebac

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/quick"
)

func TestMembershipChainProperty(t *testing.T) {
	err := quick.Check(func(length uint8) bool {
		length = length%12 + 1
		store := memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "group:0#member"}}
		for i := uint8(0); i < length; i++ {
			subject := "user:alice"
			if i+1 < length {
				subject = fmt.Sprintf("group:%d#member", i+1)
			}
			store = append(store, RelationTuple{TenantID: "acme", Namespace: "group", ObjectID: fmt.Sprint(i), Relation: "member", User: subject})
		}
		engine, err := NewEngine(&store, testModel())
		if err != nil {
			return false
		}
		allowed, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", "plan")
		return err == nil && allowed
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func FuzzParseUserset(f *testing.F) {
	f.Add("group:eng#member")
	f.Add("not a userset")
	f.Fuzz(func(t *testing.T, subject string) {
		namespace, objectID, relation, ok := parseUserset(subject)
		if ok && (namespace == "" || objectID == "" || relation == "") {
			t.Fatalf("accepted malformed userset %q", subject)
		}
	})
}

func FuzzValidateTuple(f *testing.F) {
	f.Add("acme", "document", "plan", "viewer", "user:alice", "")
	f.Add("acme", "document", "plan", "viewer", "group:eng#member", "")
	f.Fuzz(func(t *testing.T, tenant, namespace, objectID, relation, user, caveat string) {
		_ = (RelationTuple{TenantID: tenant, Namespace: namespace, ObjectID: objectID, Relation: relation, User: user, Caveat: caveat}).ValidateSyntax()
	})
}

func FuzzValidateModel(f *testing.F) {
	f.Add("document", "viewer", "user")
	f.Add("group", "member", "group")
	f.Fuzz(func(t *testing.T, namespace, relation, subjectNamespace string) {
		_ = (AuthorizationModel{Namespaces: map[string]NamespaceDefinition{
			namespace: {Relations: map[string]RelationDefinition{
				relation: {AllowedSubjects: []SubjectReference{{Namespace: subjectNamespace}}},
			}},
		}}).Validate()
	})
}

func FuzzTenantIsolation(f *testing.F) {
	f.Add("acme")
	f.Add("other")
	store := memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}}
	engine, err := NewEngine(&store, testModel())
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, tenant string) {
		allowed, err := engine.Check(context.Background(), tenant, "user:alice", "viewer", "document", "plan")
		if err == nil && allowed != (tenant == "acme") {
			t.Fatalf("Check(%q) = %v, nil; want %v", tenant, allowed, tenant == "acme")
		}
	})
}

func TestConcurrentChecks(t *testing.T) {
	store := memoryStore{{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}}
	engine, err := NewEngine(&store, testModel())
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				allowed, err := engine.Check(context.Background(), "acme", "user:alice", "viewer", "document", "plan")
				if err != nil || !allowed {
					t.Errorf("Check() = %v, %v", allowed, err)
					return
				}
			}
		}()
	}
	workers.Wait()
}
