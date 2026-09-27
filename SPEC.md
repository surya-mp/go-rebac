# go-rebac behavioral specification

This is the sole normative behavioral contract for `go-rebac`. If this file,
an exported comment, an example, or an implementation detail disagree, this
file wins. `MUST`, `MUST NOT`, and `MAY` are normative requirements.

Every semantic change MUST update the affected clause and its mapped
conformance scenario in the same change.

## S1. Authorization view and outcomes

An authorization view is `(M, T, r)`: immutable compiled model `M`, live
tenant-scoped tuple set `T`, and opaque snapshot revision `r`. A request is
`(tenant, subject, relation, namespace, object, caveat_context, as_of)`.

For one valid request, exactly one outcome exists:

| Outcome | Meaning |
| --- | --- |
| `true, nil` | A finite, model-valid witness grants the relation. |
| `false, nil` | No witness exists in the selected view. |
| `false, error` | The engine cannot establish an answer safely. |

`true, error` is forbidden. A witness is a finite chain of rewrite edges and
usable tuples from the requested relation to a matching direct subject or
matching wildcard. Missing tuples are absent witnesses, not errors.

## S2. Validity and tuple eligibility

Before a tuple contributes to a witness, the engine MUST establish all of the
following:

1. The tuple's tenant equals the request tenant.
2. Its namespace, object, relation, subject, caveat, and validity interval
   satisfy the compiled model and portable tuple grammar.
3. It is live in the selected snapshot.
4. It is active at `as_of`. A tuple interval is half-open:
   `not_before <= as_of < not_after`. Ordinary checks have no `as_of` and MUST
   deny interval-bearing tuples.
5. Its caveat is absent or evaluates to true with the tuple and request
   contexts. A caveat error is a decision error.

Tuples that fail eligibility because of time or a false caveat are ignored.
Malformed, cross-tenant, or model-invalid tuples returned by storage are
errors; an adapter MUST NOT silently use them.

Direct subjects are `namespace:object`. A userset is
`namespace:object#relation`. The only wildcard is `user:*`; it matches a
direct `user:<id>` check subject only when the resource relation opts into
wildcards. Wildcard check subjects, userset wildcards, and other wildcard
namespaces are invalid.

## S3. Evaluation rules

`Eval(subject, namespace:object#relation)` validates the request, selects the
relation definition from `M`, and evaluates exactly one rewrite.

| Rewrite | Required behavior |
| --- | --- |
| `This` | Read eligible tuples on the current relation. Allow for an exact subject, a matching `user:*`, or a userset whose recursive evaluation allows. |
| `ComputedUserset(x)` | Evaluate `x` on the same object with the same subject and view. |
| `TupleToUserset(t, x)` | Read eligible tuples on `t`; each target is a direct object. Evaluate `x` on every target. Allow if any target allows. |
| `Union(a, b, …)` | Evaluate children in declaration order. Allow on the first allow; deny if all children deny. |
| `Intersection(a, b, …)` | Evaluate children in declaration order. Allow only if all children allow; deny on the first denial. |
| `Exclusion(base, subtract)` | Evaluate `base`. If it denies, deny without evaluating `subtract`. Otherwise allow exactly if `subtract` denies. |

An error from a required child returns `false, error` for the whole decision.
Short-circuiting is permitted only after the table determines the result.
Declaration order MAY affect work and the first error, but MUST NOT affect an
error-free boolean result.

`Expand` and `Explain` inspect this same graph; neither defines an alternate
authorization algorithm or can override `Check`.

## S4. Recursion, determinism, and budgets

Recursive calls share one request context, tuple-reader memo, traversal stack,
and budgets. Re-entering a node already on the active stack returns
`ErrCycleDetected`; it never grants. Revisiting a node after it left the stack
is valid.

Depth, graph nodes, tuples read, storage calls, elapsed time, and expansion
output are independently bounded. Exceeding any bound returns its documented
error and denies. Context cancellation is equivalent to a decision error.

For identical `(M, T, r, request)`, the result MUST be identical. The engine
MUST NOT depend on map iteration, cache contents, or storage ordering for the
boolean decision.

## S5. Revisions and snapshots

Revisions are opaque datastore positions. A caller MUST preserve and return
them unchanged; only storage interprets them.

| Operation | Selected view | Contract |
| --- | --- | --- |
| `Check` | One current view when supported. | It makes no promise about a later read. |
| `CheckWithRevision(r)` | Exactly `r`; empty means current. | Returns the exact revision used. An unavailable non-empty revision errors. |
| `ReadTuples`, `Expand`, lookup with `Revision` | Exactly one view for the complete operation. | Results/pages carry that revision; clients reuse it for subsequent pages. |
| `CheckWithConsistency(minimum)` | Any view at least as fresh as `minimum`. | Returns the selected tuple and model versions in one token. |
| `ContentChangeCheck` | Any view at least as fresh as its input token. | Caller atomically persists the returned token with the authorized content version. |

At-least-fresh is not exact: a newer selected view MAY change an allow to a
deny or a deny to an allow; an older view is forbidden. Strict-consistency
adapters MUST use one externally ordered revision domain for tuple snapshots,
model reads, candidate indexes, and watches.

`ReadAuthorizationModelAtRevision` MUST return the model active at that tuple
revision. It MUST NOT return a model written after it. Activation and rollback
select immutable existing versions through compare-and-swap.

## S6. Public API contracts

| API | Preconditions | Success | Failure |
| --- | --- | --- | --- |
| `NewEngine`, `CompileModel` | Non-nil dependency and valid model. | Engine owns immutable compiled state. | Reject invalid model/dependency. |
| `Check*` | Valid tenant, subject, resource relation, and caveat context. | Allow only with an S1 witness. | `false, error`; never an implicit allow. |
| `WriteTuple*` | Canonically valid tuple allowed by model. | Idempotently persists its exact identity; returns revision where supported. | Reject before storage on validation/tenant failure. |
| `DeleteTuple*` | Same canonical identity as write. | Idempotently removes only that tuple. | Never removes a non-identical tuple. |
| `Mutate` | All changes and preconditions valid. | All requested changes become visible at one revision. | No requested change becomes visible if any precondition fails. |
| `ReadTuples*` | Tenant-scoped valid filter and cursor. | Only valid tuples from one view. | Reject invalid filter/cursor; never mix views. |
| `LookupResources*`, `LookupSubjects*` | Candidate reader provides a complete superset when used. | Every item independently passes canonical `Check`. | A stale strict index or candidate error fails. |
| `Expand*` | Valid relation node and selected view. | Bounded, canonical tree: a relation node contains exactly one rewrite node; operators preserve operand order; `this` and tuple-to-userset nodes carry only active source tuples. | Never grants access by itself. |
| `WatchAllTuples` | Storage exposes a retained global changelog. | One ordered event per committed tuple mutation, containing all tenants' changes in that mutation. | Expired checkpoints fail; a partial commit is never emitted. |
| `Explain` | Same as `Check`; caller is privileged. | Bounded canonical traversal trace. | Omits caveat context and returns errors fail-closed. |

Tuple validation is canonical: writes, deletes, mutations, read filters,
candidate results, and recursive reads MUST use the same grammar and model
assignment rules.

## S7. Security invariants

1. **Tenant isolation:** storage queries, caches, models, snapshots,
   candidates, and watches are tenant-scoped.
2. **Fail closed:** invalid input/data, caveat errors, storage failures,
   context cancellation, cycles, and budget exhaustion deny with error.
3. **View coherence:** one decision observes one model and tuple view. Strict
   APIs require snapshot-matched candidate indexes and model versions.
4. **Model immutability:** published versions do not change.
5. **Candidate safety:** indexes MAY over-return and MUST NOT under-return;
   they cannot independently grant access.
6. **Cache isolation:** cache identity includes tenant, model ID/version/hash,
   tuple revision, request, and caveat context. This applies to denied results
   as well as allowed results.

## S8. KV adapter layout and atomicity

This section applies to adapters that expose a key-value layout. It does not
require a particular database.

- Every adapter MUST use one private, layout-versioned key prefix. Adapters
  intended to share a backend between independent stores SHOULD make that
  prefix configurable.
- Live tuples MUST have a forward object/relation index and a reverse subject
  index. A tuple write or delete MUST update both in one transaction.
- Models use immutable version records and a separate active-version pointer.
  Publishing or activating a model MUST atomically update its record/pointer,
  revision marker, and any affected index watermark.
- Exact retained revisions use immutable per-record history plus revision
  markers. An adapter MAY compact history only by writing a complete per-record
  checkpoint before removing older history. It MUST return `ErrInvalidRevision`
  for expired revisions.
- Candidate indexes MAY over-return but MUST be complete at their reported
  watermark. Synchronous indexes report the write revision; asynchronous
  indexes MUST expose their watermark and reject strict reads ahead of it.
- A mutation MUST read and validate its current head inside the transaction.
  On a retryable conflict, the adapter MUST re-run the complete read/validate/
  write operation a bounded number of times; it MUST NOT replay stale writes.

## S9. Conformance mapping and proof obligations

The conformance suite is the executable companion to this specification.

| Clauses | Suite | Obligation |
| --- | --- | --- |
| S1–S4, S7.1–2 | `conformance.RunReferenceScenarios` | Positive witness rows prove reference completeness; missing, excluded, cross-tenant, and removed-witness rows prove reference soundness. |
| S5 exact snapshots | `conformance.RunRevisioned` | A historical revision remains readable after later mutation. |
| S5 model coherence | `conformance.RunConsistent` | A content token's tuple revision resolves the model version active then, even after a later model write. |
| S6 atomic writes | `conformance.RunMutations` | Failed preconditions expose no partial mutation. |
| S6 model lifecycle | `conformance.RunModelStorage`, `RunActiveModels` | Versions are immutable and activation is compare-and-swap. |
| S6/S7 candidates and watches | `RunCandidateReaders`, `RunWatches` | Known candidates are present and committed changes are observable. |
| S5/S8 single view | `RunSnapshotDiscipline` | A recursive revision-pinned Check opens one snapshot. |

`conformance.Run` includes the reference scenarios: direct relationships,
nested usersets, wildcard, computed userset, tuple-to-userset, union,
intersection, exclusion, missing tuples, tenant isolation, and witness
deletion. Adapters MUST run it. They MUST also run every capability suite for
each optional interface they advertise.

These are finite scenario proofs, not a proof of a database's replication,
durability, crash recovery, or operational timeout behavior. Production
adapters MUST add database-native tests for those properties.
