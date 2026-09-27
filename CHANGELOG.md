# Changelog

## Unreleased

- Consolidate GitHub Actions validation and tag-release automation into one
  workflow, while keeping signing and release permissions tag-scoped.

## v0.5.0

- Raise the supported Go baseline to 1.27.1 because earlier supported Go
  standard libraries have reachable `net/http`, `crypto/tls`, `net/url`, and
  filesystem vulnerabilities. CI and release builds use the patched baseline.

## v0.4.0

- Expand the normative specification with compact decision, snapshot, API, and
  invariant contracts in a top-level `SPEC.md`; add executable reference
  scenarios and a historical model-at-tuple-revision conformance assertion.
- Make `Expand` produce a canonical rewrite-expression tree, add a
  transport-neutral exact-snapshot check-dispatch boundary, and add a durable,
  resumable global tuple changelog to the KV adapter.
- Add config-first namespace model operations, configurable KV record prefixes
  and transaction conflict retries, model HTTP administration endpoints, and a
  matching typed HTTP client.

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
