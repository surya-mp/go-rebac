# Threat model

This is the library's security boundary. The normative authorization behavior
is in [`SPEC.md` S7](../../SPEC.md#s7-security-invariants).

## Evaluator guarantees

- A failed or incomplete check never grants access.
- Invalid requests, tuples, models, revisions, caveats, cycles, cancellations,
  and evaluation-limit failures return deny plus an error.
- Tuple reads are tenant-scoped. A tuple returned for another tenant fails the
  check with `ErrCrossTenantTuple`.
- Candidate readers may return extra values, but the engine re-checks every
  candidate at the selected snapshot. A stale strict index must return an
  error, not a partial answer.
- A consistent check uses one model/tuple revision only when the selected
  storage implements the required consistency contracts.

## Host responsibilities

- Authenticate callers and derive the tenant from trusted application state.
- Authorize tuple and model mutations before calling this package.
- Set request deadlines and choose evaluation limits for the workload.
- Use deterministic, side-effect-free caveat evaluators.
- Select a datastore whose transaction, snapshot, revision, index, and watch
  guarantees match the deployment. Run the relevant conformance suite against
  that adapter.
- Keep `Explain` output, raw identifiers, caveat values, and consistency
  tokens out of unprivileged logs and responses.

## Not provided

`go-rebac` does not provide identity, transport authentication, encryption,
database replication, a global clock, or protection from a compromised host or
trusted datastore. The embedded KV adapter is single-process; it is not an
externally consistent datastore.
