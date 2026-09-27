# Five-minute getting started

This guide uses the simple, embedded path. It needs only a `StorageEngine` and
an immutable `AuthorizationModel`.

The code snippets that return an error assume they are inside an application
function with an `error` return value.

## 1. Define object types

The model is application code. A direct subject is `namespace:object_id`; a
userset is `namespace:object_id#relation`.

```go
model := rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
	"user": {},
	"group": {Relations: map[string]rebac.RelationDefinition{
		"member": {AllowedSubjects: []rebac.SubjectReference{
			{Namespace: "user"},
			{Namespace: "group", Relation: "member"},
		}},
	}},
	"document": {Relations: map[string]rebac.RelationDefinition{
		"viewer": {AllowedSubjects: []rebac.SubjectReference{
			{Namespace: "user"},
			{Namespace: "group", Relation: "member"},
		}},
	}},
}}
```

Call `model.Validate()` in tests or let `NewEngine` validate it at startup.
Never mutate a model map after passing it to a running engine.

## 2. Create storage

The included embedded store is enough to get started:

```go
store := kv.NewReBACStore(kv.New())
```

Import it alongside the core package:

```go
import (
	"context"
	"errors"

	rebac "github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/kv"
)
```

For durable local storage, create the database with `kv.Open("rebac.json")`.
Your application can instead implement the small `StorageEngine` interface
against its existing database:

`QueryTuples` receives a filter: every non-empty field is an exact match and
every empty field is a wildcard. Always enforce the tenant filter in storage as
well as relying on the engine's validation.

```go
type StorageEngine interface {
	QueryTuples(context.Context, rebac.RelationTuple) ([]rebac.RelationTuple, error)
	WriteTuple(context.Context, rebac.RelationTuple) error
	DeleteTuple(context.Context, rebac.RelationTuple) error
}
```

The application owns driver selection, pool configuration, migrations,
transactions, and close behavior. The core package imports no database driver.

## 3. Construct an engine and write relationships

```go
engine, err := rebac.NewEngine(store, model)
if err != nil {
	return err
}

ctx := context.Background()
if err := engine.WriteTuple(ctx, rebac.RelationTuple{
	TenantID: "acme", Namespace: "group", ObjectID: "engineering",
	Relation: "member", User: "user:alice",
}); err != nil {
	return err
}
if err := engine.WriteTuple(ctx, rebac.RelationTuple{
	TenantID: "acme", Namespace: "document", ObjectID: "roadmap",
	Relation: "viewer", User: "group:engineering#member",
}); err != nil {
	return err
}
```

`WriteTuple` validates tuple syntax and confirms that the active model allows
the relationship before storage is called.

## 4. Check access

```go
allowed, err := engine.Check(ctx, "acme", "user:alice", "viewer", "document", "roadmap")
if err != nil {
	return err
}
if !allowed {
	return errors.New("forbidden")
}
return nil
```

The engine follows group/userset edges, terminates cycles safely, and applies
depth and node ceilings. Use `WithLimits` to lower those ceilings for a
particular workload.

## Next steps

- Use [modeling.md](modeling.md) for inherited permissions and set algebra.
- Use [storage.md](storage.md) before implementing a durable store.
- Use [consistency.md](consistency.md) when a content update must never be
  checked against an ACL older than the content version.
