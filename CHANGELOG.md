# Changelog

## v0.3.0

- Complete Zanzibar-style evaluator hardening: formal semantics, strict model
  and tuple validation, bounded recursive checks and expansion, deterministic
  fail-closed decisions, explanations, and caveats.
- Add immutable compiled-model caching, versioned model lifecycle and rollback,
  a strongest-capability in-memory adapter, storage conformance suites, and
  tenant-safe decision-cache invalidation.
- Add the optional `/v1` HTTP batch-check endpoint, request limits, metrics and
  debug observer adapters, `rebac` local diagnostics CLI, benchmarks, security
  documentation, and signed SBOM release automation.

## v0.2.0

- Add the embedded `kv` relationship and model store, atomic mutations,
  revision-pinned reads, candidate indexes, resumable watches, and bounded
  deleted-object cleanup.
- Add versioned authorization models, caveats, validity windows, wildcard
  subjects, consistency tokens, conformance tests, and optional `net/http`
  handlers.
- Remove the PostgreSQL-specific adapter and migration. External datastores
  now implement the documented storage contracts directly.

## v0.1.3

- Last PostgreSQL-backed release.
