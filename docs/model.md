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

An object ID and tenant ID are non-empty UTF-8 strings of at most 1,024 bytes
without whitespace, control characters, or `#`. They are otherwise opaque, so IDs such as
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

## Linting

`LintModel` provides deterministic, non-blocking diagnostics. Invalid models
produce one `error` issue; valid models may produce `warning` issues for a
relation that cannot grant and for direct recursion in an allowed userset or
rewrite. It deliberately does not warn about unreferenced relations: public
entry-point relations are often intentionally unreferenced.

## Compilation

`CompileModel` performs validation, normalization, and deep cloning once, then
returns an immutable `CompiledModel`. `NewEngineWithCompiledModel` can reuse it
across Engines. `CompiledModel.Dependencies` exposes sorted relation dependency
metadata, while `Dependents` exposes its reverse, without exposing mutable
model maps. `LintModel` warns about direct and indirect recursive dependencies.

## Versioned models

`ModelDocument` is JSON-serializable and contains a model ID, opaque storage
version, parent version, creation timestamp, content checksum, immutable state, namespaces,
relations, allowed subjects, and rewrites. The optional `ModelStorage`
interface lets any datastore persist it with compare-and-set semantics. An
empty expected version creates a model; a later write must use the version it
read. `kv.ReBACStore` assigns lifecycle metadata atomically and retains every
version. Its `ListAuthorizationModelVersions` method implements the optional
`ModelVersionLister` capability. `ComputeChecksum` verifies model content when
a checksum is present, including its immutable state (but not version or
timestamp metadata).

## Activation and rollback

`ActiveModelStorage` separates immutable model writes from activation.
`ActivateAuthorizationModel` compare-and-sets the active version; pass the
currently active version as `expectedActive` and the immutable target version
as `version`. Re-activate an older version with the expected current version
to roll back without rewriting history. `ValidateModelTransition` conservatively
rejects removals and rewrite changes that can invalidate live tuples.

Each version has state `draft`, `published`, or `deprecated`. An omitted state
is treated as `published` for compatibility. Create a new immutable published
version from a validated draft before activation; `kv.ReBACStore` rejects
activation of draft or deprecated versions. Deprecation does not mutate a
historical document or clear an existing active pointer.

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

`NewProductionEngine` requires `ProductionStorage`: atomic mutation,
revision-pinned and at-least-fresh snapshots, revision-matched candidate
indexes, and resumable tenant/global changelogs. This keeps lightweight stores
useful for tests while making the stronger contract explicit for production.
