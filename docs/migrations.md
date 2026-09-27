# External adapter migrations

The bundled `kv` store has no schema migration surface. An external adapter
must publish its own ordered, immutable migrations before it claims production
support.

## Versioning

Name migrations with a fixed sequence such as `001_initial`,
`002_models`, and `003_indexes`. Record the applied version in the same
transactional database as the tuples and models. A release must state the
minimum and maximum compatible schema versions.

Never alter a migration after it has been released. Add a new migration for
every change, including an index change. Run migrations before deploying code
that requires them; mixed application versions must continue to work through
the documented upgrade window.

## Safe rollout

1. Back up tuples, model documents, active-model pointers, and revision state.
2. Apply additive schema changes and indexes first.
3. Deploy code that can read both old and new data.
4. Backfill asynchronously and verify counts and tenant isolation.
5. Remove old fields only in a later, explicitly incompatible release.

Downgrades are supported only when the adapter author supplies a tested reverse
migration. Otherwise restore the pre-migration backup; never hand-edit tuple
or revision records.

## Required indexes

An adapter's diagnostic command or startup check must verify at least these
indexes before enabling indexed lookups:

```text
(tenant_id, namespace, object_id, relation, user)
(tenant_id, namespace, relation, user, object_id)
```

It must also verify the unique tuple-identity constraint and any index used to
serve revision snapshots, model versions, or watches. Index verification is
database-specific, so `go-rebac` does not guess at a portable implementation;
the adapter must fail clearly or disable the optional capability when an index
is missing.
