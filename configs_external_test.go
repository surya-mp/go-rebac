package rebac_test

import (
	"context"
	"testing"

	"github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/kv"
)

func TestNamespaceConfigStoreProjectsImmutableModelVersions(t *testing.T) {
	store := kv.NewReBACStore(kv.New())
	configs := rebac.NewNamespaceConfigStore(store, "acme", "access")
	user, err := configs.WriteConfig(context.Background(), rebac.NamespaceConfig{Namespace: "user"}, "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := configs.WriteConfig(context.Background(), rebac.NamespaceConfig{Namespace: "document", Definition: rebac.NamespaceDefinition{Relations: map[string]rebac.RelationDefinition{
		"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
	}}}, user.Version)
	if err != nil {
		t.Fatal(err)
	}
	if document.ParentVersion != user.Version {
		t.Fatalf("parent version = %q; want %q", document.ParentVersion, user.Version)
	}
	versions, err := configs.ListConfigVersions(context.Background(), "user")
	if err != nil || len(versions) != 2 || versions[1].Version != document.Version {
		t.Fatalf("user config versions = %#v, %v", versions, err)
	}
	list, err := configs.ListConfigs(context.Background(), document.Version)
	if err != nil || len(list) != 2 || list[0].Namespace != "document" || list[1].Namespace != "user" {
		t.Fatalf("configs = %#v, %v", list, err)
	}
}
