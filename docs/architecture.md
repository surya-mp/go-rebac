# Architecture

`go-rebac` evaluates an application-owned authorization model against
application-owned relationship storage. It is a library boundary, not a
database, identity provider, or authorization service deployment.

```text
Host application
├── authentication and tenant derivation
├── authorization model and caveat evaluators
├── content and relationship datastore
└── go-rebac Engine
    ├── model validation and immutable compilation
    ├── tuple validation and graph evaluation
    ├── revisions, consistency tokens, and limits
    └── optional lookups, expansion, watches, and HTTP handlers
```

## Data flow

1. The host authenticates a request and derives its tenant.
2. It builds an `Engine` from an application model or a versioned
   `ModelDocument` plus bound caveats.
3. The engine selects a tuple snapshot, validates every tuple returned by
   storage, and evaluates the relation graph fail-closed.
4. For content consistency, the host saves the returned `ConsistencyToken`
   with the content version and later supplies it to a consistent check.

## Storage capabilities

`StorageEngine` is the portable minimum. `RevisionedStorage`,
`MutationStorage`, candidate readers, and watches add stronger guarantees.
`ConsistentStorage` requires those capabilities to share one externally
ordered revision sequence. The included KV adapter implements the contracts in
one process; distributed ordering and replication belong to an external
datastore adapter.

See [storage.md](storage.md) for capability contracts and
[semantics.md](semantics.md) for evaluator behavior.
