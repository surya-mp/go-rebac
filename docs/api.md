# API guide

`go-rebac` keeps the authorization model in the application and tuple storage
behind `StorageEngine`. Use this page as a map; exported Go comments remain the
authoritative API reference on pkg.go.dev.

## Build an engine

```go
engine, err := rebac.NewEngine(store, model)
```

`AuthorizationModel` declares namespaces, relations, allowed subjects, and
optional `Rewrite` expressions. Build the model once at application startup;
do not mutate it while the engine is in use.

`StorageEngine` is implemented by your datastore adapter. `PostgresStorage`
uses an application-owned `*sql.DB`; it never owns connection setup or close.

## Model and tuples

| Type | Purpose |
| --- | --- |
| `AuthorizationModel` | Namespaces and optional caveat definitions. |
| `NamespaceDefinition` | Relations for one object type. |
| `RelationDefinition` | Allowed subjects and a relationship rewrite. |
| `RelationTuple` | One tenant-scoped relationship edge. |
| `Rewrite` | Direct, computed, tuple-to-userset, union, intersection, or exclusion rule. |

Direct subjects use `namespace:objectID`; nested usersets use
`namespace:objectID#relation`.

## Authorize

| Need | API |
| --- | --- |
| One decision | `Engine.Check` |
| Caveat-aware decision | `Engine.CheckWithContext` |
| Stable revision-pinned decision | `Engine.CheckWithRevision` |
| Caveat-aware stable decision | `Engine.CheckWithRevisionAndContext` |
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
| Page tuples | `ReadTuples` |
| Batch independent filters | `ReadTuplesBatch` |
| Find permitted resources | `LookupResources` |
| Find permitted subjects | `LookupSubjects` |
| Inspect relationship edges | `Expand` |

Revision-aware storage returns an opaque `Revision`. Preserve it when a client
needs the same authorization graph across multiple reads or pages.

## Operate safely

`Watch` returns committed tuple changes after a revision. Use
`WatchCacheInvalidation` with an application-owned `DecisionCache` to clear
tenant entries after mutations. `ReplicaSet` selects only a region that can
serve the requested revision; it never silently uses a stale replica.

Application caveats are declared with `CaveatDefinition`. Their evaluators must
be deterministic and free of I/O because they run during authorization checks.
