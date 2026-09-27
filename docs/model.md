# Authorization model and tuple grammar

This document defines the portable `go-rebac` model format. Storage adapters
must preserve these values exactly; application code owns resource IDs and
caveat evaluation.

## Names and tuples

`namespace`, `relation`, `caveat`, and model IDs use 1–63-character lower
snake case:

```text
name = lower *( lower / digit / "_" )
```

An object ID and tenant ID are non-empty UTF-8 strings without whitespace,
control characters, or `#`. They are otherwise opaque, so IDs such as
`roadmap:v2` are valid.

```text
subject = namespace ":" object-id [ "#" relation ]
tuple   = tenant, namespace, object-id, relation, subject
```

Examples:

```text
user:alice@example.com
group:engineering#member
document:roadmap:v2#viewer
```

Call `RelationTuple.ValidateSyntax` before using a tuple outside an `Engine`.
`Engine.WriteTuple` additionally verifies that the relation and subject are
allowed by the active `AuthorizationModel`.

## Versioned models

`ModelDocument` is JSON-serializable and contains a model ID, opaque storage
version, namespaces, relations, allowed subjects, and rewrites. The optional
`ModelStorage` interface lets any datastore persist it with compare-and-set
semantics. An empty expected version creates a model; a later write must use
the version it read.

Caveat names are persisted but evaluators are deliberately not serialized.
After reading a document, call `Compile` with application-owned deterministic
evaluators, then pass the resulting model to `NewEngine`.

For the usual application workflow, `NewEngineFromModelStorage` performs the
read, version selection, caveat binding, and engine construction in one call.
Use `NewProductionEngineFromModelStorage` when the tuple store satisfies the
production consistency and indexed-lookup contract.

Storage implementations choose their own schema, transactions, and indexes
while preserving this portable model contract.

## Zanzibar-style consistency

`NewConsistentEngineFromModelStorage` is the strict optional path. It requires
one shared, externally consistent revision sequence for tuple writes and model
updates. `RevisionedModelStorage` requires model writes to return that global
revision. `ModelSnapshotStorage.ReadAuthorizationModelAtRevision` returns the
model effective at the selected tuple revision; returning a newer model is a
correctness error.

`ContentChangeCheck` returns a `ConsistencyToken`. Store that token with the
application content version and provide it to `CheckWithConsistency` later.
The store may select a newer snapshot, but never one older than the token.

## Indexed lookup contract

`ResourceCandidateReader` and `SubjectCandidateReader` are optional storage
capabilities. Their result must be a complete *superset* of possible answers.
The engine checks every candidate against the model and tuple graph before it
returns it, so an index can narrow work but never makes an authorization
decision. Candidate readers receive the requested, revision-pinned view.
Stores without these indexes use the portable scan fallback.

`NewProductionEngine` requires `ProductionStorage`: revision-pinned snapshots,
atomic mutations, and both candidate-reader interfaces. This keeps lightweight
stores useful for tests while making the stronger contract explicit for a
production deployment.
