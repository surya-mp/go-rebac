# Testing and operations

## Test layers

Run the repository checks before changing the library:

```sh
go test ./...
go test -race ./...
go vet ./...
go test -run=^$ -fuzz=FuzzParseUserset -fuzztime=10s .
```

Adapter authors should run `conformance.Run` for every backend and
`conformance.RunConsistent` for a backend that claims strict consistency. Add
database integration tests for transaction rollback, tenant isolation,
historical snapshot expiration, index lag, watch restart, and concurrent
mutations.

## Limits and hot objects

The default traversal ceilings are depth 50 and 10,000 visited graph nodes.
Use `WithLimits` to lower them when untrusted clients can create deeply nested
relationships. Limit errors are availability signals: do not convert them to
an authorization allow.

`WithRequestCoalescing` coalesces identical concurrent `Check` calls within
one Go process. `WithDecisionCache` caches only revision-pinned decisions.
Cache implementations are application-owned; invalidate them through `Watch`
or `WatchCacheInvalidation` after committed changes.

## Observability

`WithObserver` emits a `CheckEvent` for every completed check and a
`MutationEvent` after tuple writes. Record duration, result reason, graph node
count, revision, storage errors, and limit failures. `Stats` provides only
process-local counters; it is not a replacement for metrics aggregation.

Do not log raw user IDs, object IDs, caveat values, or opaque tokens unless the
application's privacy policy explicitly permits it.

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
