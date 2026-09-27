# API guide

`go-rebac` keeps the authorization model in the application and tuple storage
behind `StorageEngine`. Use this page as an API map. Behavioral preconditions,
outcomes, and failure contracts are normative only in
[`SPEC.md` S6](../SPEC.md#s6-public-api-contracts); exported Go comments add
Go-level usage detail.

## Build an engine

```go
engine, err := rebac.NewEngine(store, model)
```

For a persisted, tenant-scoped model, use
`NewEngineFromModelStorage(ctx, store, models, selection, caveats)`. It loads
the selected version and returns an Engine bound to that tenant. Production
deployments use `NewProductionEngine` or
`NewProductionEngineFromModelStorage`, which require revisioned atomic storage
with at-least-fresh snapshots, snapshot-matched lookup indexes, and resumable
tenant/global change streams.

`NewConsistentEngineFromModelStorage` adds Zanzibar-style consistency without
choosing a database. It requires `ConsistentStorage` and
`RevisionedModelStorage` to share an externally consistent revision sequence.

`AuthorizationModel` declares namespaces, relations, allowed subjects, and
optional `Rewrite` expressions. Build the model once at application startup;
do not mutate it while the engine is in use.

`StorageEngine` is implemented by your application. It owns connection setup,
close, migrations, and database-driver choice. The same engine works with SQL,
key-value, document, and in-memory stores.

## Model and tuples

| Type | Purpose |
| --- | --- |
| `AuthorizationModel` | Namespaces and optional caveat definitions. |
| `NamespaceDefinition` | Relations for one object type. |
| `RelationDefinition` | Allowed subjects and a relationship rewrite. |
| `RelationTuple` | One tenant-scoped relationship edge. |
| `Rewrite` | Direct, computed, tuple-to-userset, union, intersection, or exclusion rule. |
| `ModelDocument` | JSON-serializable, versioned model configuration. |

Direct subjects use `namespace:objectID`; nested usersets use
`namespace:objectID#relation`.

`ModelStorage` is optional durable storage for model documents. `Compile`
binds non-serializable application caveat evaluators after a document is read.
The exact syntax and index contracts are in [model.md](model.md).

For config-first administration, `NewNamespaceConfigStore(models, tenant,
modelID)` projects `ReadConfig`, `ListConfigs`, `ListConfigVersions`, and
`WriteConfig` over the same immutable model documents. A config write creates a
new whole-model version; activate it explicitly when ready.

## Authorize

| Need | API |
| --- | --- |
| One decision | `Engine.Check` |
| Caveat-aware decision | `Engine.CheckWithContext` |
| Evaluate validity windows at a time | `Engine.CheckAt` or `Engine.CheckWithContextAt` |
| Stable revision-pinned decision | `Engine.CheckWithRevision` |
| Caveat-aware stable decision | `Engine.CheckWithRevisionAndContext` |
| At-least-as-fresh decision | `Engine.CheckWithConsistency` |
| Token for a content update | `Engine.ContentChangeCheck` |
| Decision telemetry | `Engine.WithObserver` and `Engine.Stats` |

Every decision receives a tenant ID. A check returns `false` for graph cycles
and obeys the configured depth and node limits. Use `WithLimits` to set lower
application-specific ceilings.

## Write and read tuples

| Need | API |
| --- | --- |
| Write/delete one tuple | `WriteTuple`, `DeleteTuple` |
| Receive mutation revision | `WriteTupleWithRevision`, `DeleteTupleWithRevision` |
| Atomic multi-tuple change | `Mutate` with `TupleChange` and `Precondition` |
| Delete every relationship on an object | `DeleteObject` (when storage supports it) |
| Page tuples | `ReadTuples` |
| Batch independent filters | `ReadTuplesBatch` |
| Find permitted resources | `LookupResources` |
| Find permitted subjects | `LookupSubjects` |
| Inspect relationship edges | `Expand` |
| Explain a privileged debugging decision | `Explain` |

Revision-aware storage returns an opaque `Revision`. Preserve it when a client
needs the same authorization graph across multiple reads or pages.

For scalable lookups, a storage adapter may implement
`ResourceCandidateReader` and `SubjectCandidateReader`. Both must return a
complete candidate superset; `Engine` still authorizes every result.
`LookupResourcesWithConsistency` and `LookupSubjectsWithConsistency` require
snapshot-matched candidates and reject a stale index.

Set `NotBeforeUnixNano` and/or `NotAfterUnixNano` on a tuple for a fixed
validity interval. Point-in-time checks and lookup requests use `AsOfUnixNano`;
ordinary checks deny interval-bearing tuples. A relation may opt into `user:*`
with `AllowWildcard`. See [modeling.md](modeling.md).

## Operate safely

`Watch` returns committed tuple changes after a revision. Use
`WatchCacheInvalidation` with an application-owned `DecisionCache` to clear
tenant entries after mutations. `ReplicaSet` selects only a region that can
serve the requested revision; it never silently uses a stale replica.

Application caveats are declared with `CaveatDefinition`. Their evaluators must
be deterministic and free of I/O because they run during authorization checks.

## HTTP integration

`github.com/surya-mp/go-rebac/server` provides optional standard-library HTTP
handlers over a configured Engine. Its read-only data plane and privileged
tuple-mutation plane are separate. `server.NewClient` supplies the matching
typed standard-library client. See [server.md](server.md).
