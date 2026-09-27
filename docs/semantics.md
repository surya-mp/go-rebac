# Authorization semantics guide

[`SPEC.md`](../SPEC.md) is the sole normative behavioral contract for
`go-rebac`. This guide is deliberately non-normative: use it to orient yourself
before reading the relevant numbered specification clause.

## Compact conceptual model

An authorization view combines an immutable model, tenant-scoped live tuples,
and a snapshot revision. A check asks whether a subject has one relation on one
object in that view. A successful check has a witness path through direct
tuples, usersets, and rewrite rules. No witness means a normal denial; a
failure to evaluate safely means denial plus error.

The complete decision rules are [S1–S4](../SPEC.md#s1-authorization-view-and-outcomes),
with tuple eligibility in [S2](../SPEC.md#s2-validity-and-tuple-eligibility).

## Reading a model

`This` evaluates tuples on a relation. A computed userset moves sideways to
another relation on the same object. Tuple-to-userset follows an object edge,
then checks a relation on the referenced object. Union, intersection, and
exclusion combine those checks. The exact short-circuit and error behavior is
defined by [S3](../SPEC.md#s3-evaluation-rules).

## Time, recursion, and views

Tuple caveats and validity intervals determine whether a tuple is usable.
Cycles and graph limits fail closed. Snapshot-pinned APIs use an exact view;
consistency-token APIs select a view no older than their input token. See
[S4](../SPEC.md#s4-recursion-determinism-and-budgets) and
[S5](../SPEC.md#s5-revisions-and-snapshots).

## Executable reference scenarios

`conformance.RunReferenceScenarios` exercises the direct, nested, wildcard,
computed, tuple-to-userset, set-algebra, missing-tuple, tenant, and
witness-removal cases. The scenario-to-clause mapping and its limits are in
[S8](../SPEC.md#s8-conformance-mapping-and-proof-obligations).
