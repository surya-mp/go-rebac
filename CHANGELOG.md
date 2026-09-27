# Changelog

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
