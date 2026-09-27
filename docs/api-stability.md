# API stability and versioning

The module follows Go module semantic import versioning.

## Go compatibility

The supported baseline is Go 1.27.1, the current stable Go release. CI runs
the complete security suite on that exact version. A future release may drop a
Go version only in a minor release, with the change recorded in
`CHANGELOG.md`.

## Before v1

`v0.x` releases may change exported APIs when required to close authorization
correctness or security gaps. Such changes are documented in `CHANGELOG.md`.
Consumers that need a fixed contract should pin a release tag.

## v1 commitment

At v1, exported identifiers in the root `rebac`, `kv`, `server`, and
`conformance` packages are stable within a major version. Breaking changes
require a new module major version. Additive APIs and bug fixes remain minor
or patch releases as appropriate.

The evaluator's fail-closed behavior, tuple/model grammar, storage capability
contracts, and semantics document are part of that compatibility commitment.
Implementation details remain uncommitted unless explicitly exported.

## Public surface

| Package/symbol family | v1 status |
| --- | --- |
| Root model, tuple, engine, storage, revision, consistency, cache, token, and region APIs | Stable |
| `kv` storage APIs | Stable reference adapter |
| `conformance` capability suites | Stable adapter-test contract |
| `server` HTTP adapter | Stable optional adapter; applications supply authentication |
| `CompiledModel`, `LintModel`, and `Explain` | Experimental until their trace/diagnostic schema has one release cycle of production use |

The root package keeps evaluator state, model maps owned by compiled models,
and cache-key construction unexported. No exported API requires a caller to
depend on those implementation details. New implementation helpers belong in
an `internal` package if they cannot remain file-private.

## Deprecation

An API is deprecated through a Go doc comment and release notes before removal
in the next major version. Security fixes may change behavior immediately when
the prior behavior could grant unintended access.
