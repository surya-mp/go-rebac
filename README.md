# go-rebac

`go-rebac` is a Go implementation of relationship-based access control
(ReBAC), inspired by Zanzibar-style tuple graphs. It evaluates authorization
models defined by the application and stores relationship tuples through an
application-provided backend.

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

## Consistency and operations

`CheckWithRevision` returns the opaque revision used for a decision.
`WriteTupleWithRevision` and `Mutate` return write revisions. Pass a revision
back to `CheckWithRevision` for a stable graph view. `Watch` streams committed
tuple changes so an application-owned cache can invalidate tenant entries.

The `Observer` interface is the integration point for tracing, metrics, and
audit systems. `CheckEvent` includes the final reason, duration, graph-node
count, and revision.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

To run the PostgreSQL integration test, provide `REBAC_TEST_POSTGRES_DSN` and
link your chosen `database/sql` driver in the CI integration-test harness.

## License

MIT. See [LICENSE](LICENSE).
