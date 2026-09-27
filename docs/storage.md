# Storage integration guide

`go-rebac` has no required backend. Start with `StorageEngine`; implement
additional capabilities only when the application needs them. The normative
adapter contract is [`SPEC.md` S5–S8](../SPEC.md#s5-revisions-and-snapshots);
this document provides integration guidance.

## Baseline contract

`QueryTuples` must apply every populated field of `RelationTuple` exactly.
Return only live tuples from the requested tenant. `WriteTuple` should be
idempotent for an existing identical tuple; `DeleteTuple` should be idempotent
for an absent tuple. The engine validates model correctness, but storage must
still enforce tenant isolation and unique tuple identity.

Tuple identity includes `tenant_id`, `namespace`, `object_id`, `relation`,
`user`, caveat name, and caveat context. A practical relational design stores
the caveat context in canonical JSON and has indexes for both directions:

```text
(tenant_id, namespace, object_id, relation, user)  -- direct check
(tenant_id, namespace, relation, user, object_id)  -- reverse lookup
```

Do not concatenate query values into SQL. Use your driver's parameter binding
and map database cancellation to the provided `context.Context`.

## Capability ladder

| Capability | Interface | Why it exists |
| --- | --- | --- |
| Basic reads and writes | `StorageEngine` | Embedded checks and mutations |
| One stable read view | `SnapshotStorage` | Prevent mixed reads in one check |
| Historical snapshots | `RevisionedStorage` | Repeatable pages and decisions |
| Atomic changes | `MutationStorage` | Preconditions and all-or-nothing writes |
| Reverse candidates | `ResourceCandidateReader`, `SubjectCandidateReader` | Avoid broad scans |
| Tuple change stream | `WatchStorage` | Cache invalidation |
| Global tuple changelog | `GlobalWatchStorage` | Cross-tenant replication and indexing |
| Zanzibar-style checks | `ConsistentStorage` | At-least-fresh model/tuple snapshots |

`NewEngine` accepts the baseline contract. `NewProductionEngine` requires
atomic mutations, exact and at-least-fresh revision snapshots, revision-matched
candidate indexes, and resumable tenant/global changelogs. Implementing the Go
interfaces does not prove physical durability or external consistency: validate
those claims with the backend's own crash, replication, and failover tests.
`NewConsistentEngineFromModelStorage` requires the stricter last row plus a
revisioned model store.

## Revisions and transactions

A `Revision` is opaque to `go-rebac`; only the store may create or interpret
it. For `RevisionedStorage`:

- `SnapshotAt(ctx, "")` returns a current stable snapshot and its revision.
- `SnapshotAt(ctx, revision)` returns that retained snapshot or
  `ErrInvalidRevision` when it is unavailable.
- A mutation returns the shared revision committed by all of its changes.

`MutationStorage.Mutate` must evaluate every precondition and apply every
change in one transaction. Never implement it as a read followed by separate
writes outside a transaction.

## Indexed lookups

Candidate readers may return extra candidates, but may never omit an object or
subject that could be authorized. The engine re-checks every returned
candidate. For the strict consistency path, implement
`SnapshotResourceCandidateReader` and `SnapshotSubjectCandidateReader`; they
must report the **same revision** requested by the engine. A lagging index must
return an error rather than a partial answer.

## KV layout, retention, and watermarks

KV adapters should use a private, layout-versioned prefix and update forward
tuple, reverse-subject, model-version, active-model, revision, and candidate
records in one transaction. Retained exact revisions need per-record history;
before pruning it, write a complete checkpoint at the retention boundary.
Expired revisions must return `ErrInvalidRevision`.

`kv.ReBACStoreOptions.KeyPrefix` prepends an application-owned namespace to
the embedded store's keys. `TransactionRetries` and `RetryBackoff` bound
conflict retries; zero retries selects the default of three and a negative
retry count disables retries.

An asynchronous candidate index must expose its completed revision and reject
a strict lookup ahead of that watermark. `CandidateIndexWatermark` provides
this diagnostic for adapters whose current index is otherwise complete.

## Watches and retention

Implement `ResumableWatchStorage` for `ConsistentStorage`. Events must be
ordered after `WatchRequest.After`. A heartbeat has no changes but advances the
revision. If the requested revision is older than retained history, return
`ErrInvalidRevision`; clients then take a new snapshot and restart the watch.

Retention is a datastore policy. Document it, monitor lagging consumers, and
do not silently skip revisions.

`kv.ReBACStore` retains the latest 1,000 tuple events. A consumer that falls
behind receives `kv.ErrWatchOverflow`; resume from its last processed revision
or take a new snapshot if that revision has expired.

`GlobalWatchStorage.WatchAllTuples` is the cross-tenant counterpart. It emits
one event for the complete mutation, including changes for every tenant. Its
checkpoint is safe only after the consumer has processed that whole event.
The KV adapter persists its retained global changelog alongside tuple state;
the same 1,000-event retention limit applies.

## Deleted objects

`kv.ReBACStore.DeleteObject` removes an object's relationships and records a
tombstone. Periodically call `CollectGarbage(ctx, max)` to remove inbound
references to at most `max` tombstoned objects. This keeps deletion bounded;
object recreation clears its tombstone.

## Test the adapter

Use the portable suite in your adapter tests. `Run` includes the normative
reference scenarios (direct and nested relationships, wildcards, rewrites,
exclusion, deletion, and tenant isolation):

```go
func TestStore(t *testing.T) {
	conformance.Run(t, func(t testing.TB) rebac.StorageEngine {
		return newTestStore(t)
	})
}
```

For a strict implementation, use `conformance.RunConsistent` and provide both
the tuple store and `RevisionedModelStorage`. This verifies the public
protocol; database replication and external-consistency guarantees still need
database-specific integration and fault tests.

`RunConsistent` verifies both sides of snapshot coherence: a content token
contains a tuple and model view, and a later model write MUST NOT change the
model returned for that token's historical tuple revision.

Run `conformance.RunRevisioned`, `conformance.RunMutations`, and
`conformance.RunModelStorage` independently for each optional capability your
adapter claims. Also run `RunActiveModels`, `RunCandidateReaders`, and
`RunWatches` for those respective capabilities. `RunConsistent` remains the
full strict-protocol suite.

See [migrations.md](migrations.md) for versioning, rollout, backup, rollback,
and index-verification requirements for external adapters.
