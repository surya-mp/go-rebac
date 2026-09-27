# Testing and operations

## Test layers

Run the repository checks before changing the library:

```sh
go test ./...
go test -race ./...
go vet ./...
go test -run=^$ -fuzz=FuzzParseUserset -fuzztime=10s .
```

Run repeatable evaluator and lookup measurements with:

```sh
go test -run='^$' -bench='Benchmark(Check|Lookup)' -benchmem .
```

## Local diagnostics

`go run ./cmd/rebac` operates on a local `kv.Open` database and a model JSON
file. It is an administrative tool, not an authentication service:

```sh
go run ./cmd/rebac model validate --file model.json
go run ./cmd/rebac tuple write --store rebac.json --model model.json --tuple tuple.json
go run ./cmd/rebac check --store rebac.json --model model.json \
  --tenant acme --user user:alice --relation viewer --namespace document --object roadmap
go run ./cmd/rebac explain --store rebac.json --model model.json \
  --tenant acme --user user:alice --relation viewer --namespace document --object roadmap
```

Use `model lint`, `model publish`, `model versions`, `tuple delete`, `tuple
read`, `expand`, and `storage check` for the corresponding local operations.

Adapter authors should run `conformance.Run` for every backend and
`conformance.RunConsistent` for a backend that claims strict consistency. Add
database integration tests for transaction rollback, tenant isolation,
historical snapshot expiration, index lag, watch restart, and concurrent
mutations.

## Limits and hot objects

The default traversal ceilings are depth 50, 10,000 visited graph nodes,
100,000 tuples read, 1,000 storage calls, five seconds of evaluation time, and
100,000 expansion output items. Use `WithLimits` for depth and
nodes, or `WithEvaluationLimits` to lower any of them when untrusted clients
can create deeply nested relationships. Limit errors are availability signals:
do not convert them to an authorization allow.

`WithRequestCoalescing` coalesces identical concurrent `Check` calls within
one Go process. `WithDecisionCache` caches only revision-pinned decisions.
Keys include tenant, compiled model identity, model version, tuple revision,
request, and caveat context. Cache implementations are application-owned;
invalidate them through `Watch` or `WatchCacheInvalidation` after committed
changes.

## Observability

`WithObserver` emits a `CheckEvent` for every completed check and a
`MutationEvent` after tuple writes. Use `ObserverFuncs` to bridge these events
to a metrics or tracing SDK without making either dependency mandatory. Record
duration, result reason, model and tuple revisions, graph node count, storage
calls, tuples read, storage errors, and limit failures. Events never include
caveat context. `Stats` provides only process-local counters; it is not a
replacement for metrics aggregation.
`StorageCalls` and `TuplesRead` make per-decision storage amplification
visible without instrumenting a datastore adapter.

Do not log raw user IDs, object IDs, caveat values, or opaque tokens unless the
application's privacy policy explicitly permits it.

`Explain` is an explicit privileged debugging API. `WithDebugObserver` receives
its results separately from ordinary production events. It executes the same
bounded evaluator as `Check` and returns traversed relations, considered
tuples, branch outcomes, and the revision used. It deliberately omits caveat
context, but its tuple data is still sensitive and must not be exposed to
ordinary callers.

## Security checklist

- Derive tenant IDs from authenticated application context where possible.
- Keep tuple/model mutation endpoints separate from ordinary request traffic.
- Treat storage errors, expired revisions, and limit failures as deny/error,
  never as allow.
- Use deterministic caveats without network calls.
- Authenticate and expire consistency tokens when they leave trusted process
  memory.
- Restrict model changes and record a durable audit trail outside this library.

## Deployment boundary

The embedded engine is intentionally not a global control plane. A production
service may place it behind load balancing and implement shared caches,
distributed dispatch, index workers, replication, backups, and failover in the
chosen infrastructure. The interfaces in this module make those guarantees
visible to the evaluator; they do not create a database or replicate data.

For strict consistency, monitor model-read failures, token expiry, snapshot
retention, index watermark lag, and watch consumer lag. Any of these conditions
must fail closed or trigger a documented resynchronization path.
