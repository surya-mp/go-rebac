# Authorization semantics

This document is the normative decision contract for `go-rebac`. For a fixed
model, tuple snapshot, request, caveat context, and validity time, a decision
is deterministic. An authorization error always returns `allowed == false`.

## Inputs and graph nodes

`Check` evaluates a request `(tenant, subject, relation, namespace, object)`
against the relation node `namespace:object#relation`. Every storage query is
scoped to that tenant. A tuple is usable only when it is syntactically valid,
allowed by the model, active at the requested time, and its caveat (if any)
returns true.

| Construct | Traversal | Result |
| --- | --- | --- |
| `This` | Read tuples on the current relation. | Grant on an exact subject match, a matching nested userset, or `user:*`. |
| Direct user | Compare the tuple subject to the request subject. | Equal subjects grant. |
| Userset | Follow `namespace:object#relation` in a tuple. | The tuple grants when the target relation grants. |
| `user:*` | Match a direct request subject in namespace `user`. | Grants every `user:<id>`; it never grants a userset. |
| `ComputedUserset` | Evaluate its named relation on the current object. | Target result. |
| `TupleToUserset` | Read direct-object tuples on `Tupleset`, then evaluate `ComputedUserset` on each referenced object. | Any matching target grants. |
| `Union` | Evaluate children in declaration order. | First grant grants; all denials deny. |
| `Intersection` | Evaluate children in declaration order. | Every child must grant. |
| `Exclusion` | Evaluate `Base`, then `Subtract` only when base grants. | Grant only when base grants and subtract denies. |

Rewrites nest recursively. The declared order is observable only for traversal
cost and the first error; it never changes a successful boolean result when no
error occurs. Missing tuples simply make that branch deny.

`Expand` is a privileged graph-inspection API, not an alternate decision
algorithm. Each output node carries its object, relation, rewrite, snapshot
revision, model identity/version, active direct tuples, and nested-userset
children. It applies the same tuple validation, caveat/time, tenant, cycle,
depth, node, tuple-read, storage-call, and cancellation rules as `Check`; its
separate output budget counts returned nodes and tuples.

`LookupResources` and `LookupSubjects` first obtain a complete candidate
superset, then verify each candidate with the same revision-pinned `Check`
evaluator. An empty point-in-time value is exactly ordinary `Check` semantics;
it is not exposed to caveats as a synthetic timestamp.

## Wildcards, caveats, and time

Wildcard support is opt-in per relation with `AllowWildcard`. The only
wildcard subject is `user:*`; `group:*`, userset wildcards, and wildcard check
subjects are invalid. A wildcard participates in `This` before surrounding
union, intersection, or exclusion operators, so ordinary rewrite rules apply
unchanged.

A caveat runs after a tuple has been found. False makes that tuple inapplicable;
an evaluator error fails the entire decision closed. A tuple validity interval
is half-open: `NotBeforeUnixNano <= asOfUnixNano < NotAfterUnixNano`. Ordinary
checks have no `asOf` time and therefore deny interval-bearing tuples.

Models are capped at 1 MiB encoded JSON. Tuple and request caveat contexts
must be JSON-serializable and are capped at 64 KiB encoded JSON. Oversized or
invalid input is rejected before storage or evaluation.

## Recursion and bounds

The evaluator keeps a path-local set of relation nodes. Revisiting an active
node is a cycle and returns `ErrCycleDetected`; it never grants. A repeated
node reached after its prior branch returns is evaluated normally. Depth and
node budgets are independent: exceeding either returns `ErrMaxDepthExceeded`
or `ErrMaxNodesExceeded`. These errors fail the whole decision closed.

## Failure and isolation rules

| Condition | Decision |
| --- | --- |
| Missing live tuple or inactive/deleted tuple | deny, no error |
| Invalid request, tuple, model, or model version | deny, error |
| Storage, snapshot, revision, or model-read error | deny, error |
| Caveat evaluator error | deny, error |
| Cycle, depth/node/tuple-read/storage-call/time/output limit | deny, error |
| Cross-tenant tuple returned by storage | deny, error |
| Tenant/model mismatch | deny, error |

Object deletion removes outgoing relationships. References to a deleted object
cannot grant while its target relations are empty; `kv.ReBACStore` can later
remove those inbound references with bounded garbage collection.

## Revisions and models

`CheckWithRevision` uses one tuple snapshot. The consistent APIs additionally
load the model effective at that same externally ordered revision. An empty
revision selects the latest snapshot supported by storage. Storage adapters
must not mix tuple/model revisions or return cross-tenant tuples.
