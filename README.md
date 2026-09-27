# go-rebac — Zanzibar-style ReBAC authorization for Go

[![CI](https://github.com/surya-mp/go-rebac/actions/workflows/ci.yml/badge.svg)](https://github.com/surya-mp/go-rebac/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/surya-mp/go-rebac.svg)](https://pkg.go.dev/github.com/surya-mp/go-rebac)

`go-rebac` is a Go library for relationship-based access control (ReBAC),
inspired by Google Zanzibar authorization concepts. Define an authorization
model in Go, store relationship tuples in the included embedded store or your
own datastore, and evaluate tenant-safe authorization checks without running
an external permissions service.

It is useful when a plain RBAC role table is not enough: document sharing,
group membership, organization hierarchies, parent-folder permissions, and
other graph-shaped authorization rules.

## Features

- Direct relationships, nested usersets, computed usersets, tuple-to-userset,
  union, intersection, and exclusion.
- Tenant-scoped checks with cycle, depth, and node limits.
- Revision-pinned reads and atomic mutations when the storage supports them.
- Portable storage interfaces, backend conformance tests, and optional indexed
  lookup candidates.
- Optional Zanzibar-style at-least-as-fresh checks that pin the authorization
  model and tuple graph to one shared revision.
- Pagination, batched reads, tuple watches, and deterministic
  application-defined caveats.

## Install

```sh
go get github.com/surya-mp/go-rebac
```

For the zero-setup path, use `kv.NewReBACStore(kv.New())`. The library also
accepts a `StorageEngine` backed by your existing datastore.

## Documentation

Start with the [documentation index](docs/README.md). It includes guided
material for [modeling](docs/modeling.md), [storage implementations](docs/storage.md),
[content consistency](docs/consistency.md), [HTTP integration](docs/http.md),
and [testing and operations](docs/operations.md).
Release history is in [CHANGELOG.md](CHANGELOG.md).

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

The following body belongs in a function that returns `error`:

```go
import (
	"context"
	"errors"

	rebac "github.com/surya-mp/go-rebac"
	"github.com/surya-mp/go-rebac/kv"
)

ctx := context.Background()
store := kv.NewReBACStore(kv.New())
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
if err != nil {
	return err
}
if !allowed {
	return errors.New("forbidden")
}
return nil
```

`kv.New()` is an in-process store. For durable local storage, use
`database, err := kv.Open("rebac.json")` and pass `database` to
`kv.NewReBACStore`. It is a single-process store; do not share its file across
processes. A distributed database can implement `StorageEngine` and the
revisioned contracts when cross-process or cross-region consistency is needed.

## Authorization models

The application owns the model. It declares valid namespaces, relations, and
subjects, so invalid tuples are rejected before storage. `Rewrite` supports
Zanzibar-style relation composition:

- `This` for direct tuples and nested usersets.
- `ComputedUserset` for another relation on the same object.
- `TupleToUserset` for a relation on a referenced object.
- `Union`, `Intersection`, and `Exclusion` for set algebra.

Keep model construction immutable for the lifetime of an engine. For runtime
model configuration, persist a JSON `ModelDocument` through `ModelStorage`,
bind application caveats with `Compile`, and construct a new engine for that
model version. See [docs/model.md](docs/model.md) for the strict portable
grammar and model-storage contract.

For a production adapter, use `NewProductionEngine` (or
`NewProductionEngineFromModelStorage`). It requires revision-pinned reads,
atomic mutations, and both indexed lookup candidate capabilities. Lightweight
adapters can continue to use `NewEngine` for local tests and prototypes.

For Zanzibar-style content consistency, use
`NewConsistentEngineFromModelStorage`. Its storage implements
`ConsistentStorage`, and its model store implements `RevisionedModelStorage`.
Both use the same externally consistent revision sequence. Call
`ContentChangeCheck` before saving application content, save its
`ConsistencyToken` with that content, then pass it to `CheckWithConsistency`
when the content is read.

For tokens that cross an untrusted HTTP boundary, use `TokenCodec`; the
standard-library `NewHMACTokenCodec` provides authenticated, expiring tokens.
`conformance.RunConsistent` exercises the portable strict-storage protocol.
The bundled `kv` store satisfies that protocol inside one process; it does not
create global replication or external consistency on its own.

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

## API guide

| Need | API |
| --- | --- |
| Check one permission | `Check` or `CheckWithContext` |
| Keep a decision stable | `CheckWithRevision` |
| Check no older than content | `CheckWithConsistency` |
| Produce a content token | `ContentChangeCheck` |
| Write one tuple | `WriteTupleWithRevision` |
| Change tuples atomically | `Mutate` |
| Page through tuples | `ReadTuples` or `ReadTuplesBatch` |
| Find resources or subjects | `LookupResources`, `LookupSubjects` |
| Inspect a relationship graph | `Expand` |
| Invalidate a cache | `Watch` or `WatchCacheInvalidation` |

For a concise API map, see [docs/api.md](docs/api.md). Exported Go comments
remain the canonical symbol documentation on pkg.go.dev.

Storage authors can reuse `conformance.Run` in their own tests. Add
`ResourceCandidateReader` or `SubjectCandidateReader` only when native indexes
can return a complete candidate superset; the normal storage interface remains
the portability baseline.

## Optional HTTP service

The dependency-free [`server`](server) package exposes a configured Engine as
standard `net/http` data-plane and separately mounted admin handlers. It does
not open a database, authenticate callers, or start a listener, so applications
retain their existing driver, middleware, and deployment choices. See
[docs/server.md](docs/server.md) for routes and mounting guidance.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

## Project keywords

Go authorization, ReBAC, relationship-based access control, Zanzibar,
authorization model, permission checks, RBAC migration, and tuple graph
evaluation.

## License

MIT. See [LICENSE](LICENSE).
