# go-rebac — Zanzibar-style ReBAC authorization for Go

[![CI](https://github.com/surya-mp/go-rebac/actions/workflows/ci.yml/badge.svg)](https://github.com/surya-mp/go-rebac/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/surya-mp/go-rebac.svg)](https://pkg.go.dev/github.com/surya-mp/go-rebac)

`go-rebac` is a Go library for relationship-based access control (ReBAC),
inspired by Google Zanzibar authorization concepts. Define an authorization
model in Go, store relationship tuples in your own datastore, and evaluate
tenant-safe authorization checks without running an external permissions
service.

It is useful when a plain RBAC role table is not enough: document sharing,
group membership, organization hierarchies, parent-folder permissions, and
other graph-shaped authorization rules.

## Features

- Direct relationships, nested usersets, computed usersets, tuple-to-userset,
  union, intersection, and exclusion.
- Tenant-scoped checks with cycle, depth, and node limits.
- Revision-pinned reads and atomic mutations when the storage supports them.
- Optional PostgreSQL storage, pagination, batched reads, tuple watches, and
  deterministic application-defined caveats.

## Install

```sh
go get github.com/surya-mp/go-rebac
```

The library does not choose, open, or close a PostgreSQL driver. Your
application owns its `*sql.DB` and passes it to `NewPostgresStorage`.

## Relationship tuple format

Every relationship has a tenant, resource namespace, object ID, relation, and
subject:

```go
rebac.RelationTuple{
	TenantID: "acme",
	Namespace: "document",
	ObjectID: "roadmap",
	Relation: "viewer",
	User: "group:engineering#member",
}
```

Direct subjects use `namespace:objectID`, such as `user:alice`. Nested
usersets use `namespace:objectID#relation`, such as
`group:engineering#member`. The engine follows userset edges recursively and
rejects graph cycles safely.

## Quick start

```go
model := rebac.AuthorizationModel{Namespaces: map[string]rebac.NamespaceDefinition{
	"user": {},
	"group": {Relations: map[string]rebac.RelationDefinition{
		"member": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
	}},
	"document": {Relations: map[string]rebac.RelationDefinition{
		"viewer": {AllowedSubjects: []rebac.SubjectReference{
			{Namespace: "user"},
			{Namespace: "group", Relation: "member"},
		}},
	}},
}}

engine, err := rebac.NewEngine(store, model)
if err != nil {
	return err
}

_ = engine.WriteTuple(ctx, rebac.RelationTuple{
	TenantID: "acme", Namespace: "group", ObjectID: "eng",
	Relation: "member", User: "user:alice",
})
_ = engine.WriteTuple(ctx, rebac.RelationTuple{
	TenantID: "acme", Namespace: "document", ObjectID: "roadmap",
	Relation: "viewer", User: "group:eng#member",
})

allowed, err := engine.Check(ctx, "acme", "user:alice", "viewer", "document", "roadmap")
```

`store` can be any implementation of `StorageEngine`. For PostgreSQL, create
the database handle in your application, apply `migrations/001_initial.sql`,
then use `rebac.NewPostgresStorage(db)`.

## Authorization models

The application owns the model. It declares valid namespaces, relations, and
subjects, so invalid tuples are rejected before storage. `Rewrite` supports
Zanzibar-style relation composition:

- `This` for direct tuples and nested usersets.
- `ComputedUserset` for another relation on the same object.
- `TupleToUserset` for a relation on a referenced object.
- `Union`, `Intersection`, and `Exclusion` for set algebra.

Keep model construction at application startup and treat it as immutable for
the lifetime of an engine.

## Consistency and operations

`CheckWithRevision` returns the opaque revision used for a decision.
`WriteTupleWithRevision` and `Mutate` return write revisions. Pass a revision
back to `CheckWithRevision` for a stable graph view. `Watch` streams committed
tuple changes so an application-owned cache can invalidate tenant entries.

The `Observer` interface is the integration point for tracing, metrics, and
audit systems. `CheckEvent` includes the final reason, duration, graph-node
count, and revision.

## Caveats and caching

Tuples can name an application-defined caveat and carry caveat parameters.
Pass request values through `CheckWithContext`; caveat evaluators must be
deterministic and must not perform I/O. Revision-pinned decisions can use a
`DecisionCache`; call `WatchCacheInvalidation` to invalidate tenant entries
after committed tuple changes.

## PostgreSQL

`PostgresStorage` uses `database/sql` with an application-provided connection
pool. Apply [migrations/001_initial.sql](migrations/001_initial.sql) through
your existing migration tool. The schema stores revisions and tombstones to
support snapshot reads, revision-pinned checks, atomic mutations, pagination,
and tuple change watches.

The application selects its database driver, pool settings, TLS policy, and
database lifecycle. This package does not open connections or bundle a driver.

## API guide

| Need | API |
| --- | --- |
| Check one permission | `Check` or `CheckWithContext` |
| Keep a decision stable | `CheckWithRevision` |
| Write one tuple | `WriteTupleWithRevision` |
| Change tuples atomically | `Mutate` |
| Page through tuples | `ReadTuples` or `ReadTuplesBatch` |
| Find resources or subjects | `LookupResources`, `LookupSubjects` |
| Inspect a relationship graph | `Expand` |
| Invalidate a cache | `Watch` or `WatchCacheInvalidation` |

For a concise API map, see [docs/api.md](docs/api.md). Exported Go comments
remain the canonical symbol documentation on pkg.go.dev.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

To run the PostgreSQL integration test, provide `REBAC_TEST_POSTGRES_DSN` and
link your chosen `database/sql` driver in the CI integration-test harness.

## Project keywords

Go authorization, ReBAC, relationship-based access control, Zanzibar,
authorization model, permission checks, RBAC migration, PostgreSQL, and tuple
graph evaluation.

## License

MIT. See [LICENSE](LICENSE).
