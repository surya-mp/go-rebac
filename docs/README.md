# go-rebac documentation

`go-rebac` is an embedded, database-neutral authorization library. The `kv`
subpackage provides a zero-setup embedded store; applications own identity,
transport, and any external-database integration.

## Choose a path

| Need | Start here |
| --- | --- |
| Add ReBAC checks to one Go application | [Five-minute quickstart](getting-started.md) |
| Design namespaces, relations, and rewrites | [Modeling guide](modeling.md) |
| Read the normative behavioral contract | [Specification](../SPEC.md) |
| Learn the evaluator's conceptual model | [Authorization semantics guide](semantics.md) |
| Understand component ownership | [Architecture](architecture.md) |
| Review API compatibility rules | [API stability](api-stability.md) |
| Implement a database store | [Storage guide](storage.md) |
| Verify an adapter's guarantees | [Storage adapters](storage-adapters.md) |
| Migrate an external adapter safely | [Migration guide](migrations.md) |
| Prevent stale ACL checks after content changes | [Consistency guide](consistency.md) |
| Expose the optional HTTP handlers | [HTTP guide](http.md) |
| Test or operate a production integration | [Testing and operations](operations.md) |
| Review deployment security boundaries | [Threat model](security/threat-model.md) |
| Find an exported symbol | [API guide](api.md) |

## Ownership boundary

The package deliberately does **not** select an external database, identify a
caller, or start a server. Those are application decisions. The portable
`StorageEngine` contract supports external integrations; optional stronger
contracts make snapshot consistency and scalable lookups explicit when a
deployment needs them.
