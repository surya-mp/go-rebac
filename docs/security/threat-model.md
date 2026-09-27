# Threat model

`go-rebac` is an authorization evaluator, not an identity provider, database,
or network policy boundary. A host application must authenticate callers,
derive tenant identity from that authentication, protect tuple/model mutation
paths, and select a storage adapter with guarantees matching its deployment.

## Security invariants

- A check error never grants access.
- Every tuple query is tenant-scoped; a cross-tenant tuple returned by storage
  fails the check with `ErrCrossTenantTuple`.
- Invalid requests, tuples, models, revisions, caveat errors, cycles, and
  evaluation-limit failures deny with an error.
- Candidate indexes may return extra values, never omit potential values; the
  engine re-checks every candidate against the selected snapshot.
- A consistent model/tuple decision requires storage with one externally
  ordered revision sequence. The embedded KV store provides this only inside
  one process.

## Threats and controls

| Threat | Required control |
| --- | --- |
| Caller chooses another tenant | Derive the tenant from authenticated application state; validate it on every API call. |
| Malformed tuple or model | Use `AuthorizationModel.ValidateTuple`, model validation, and model-storage CAS before persistence. |
| Recursive or enormous graph | Keep evaluation limits appropriate to the workload; defaults bound depth, nodes, tuples read, and storage calls. |
| Slow/cancelled request | Set a request deadline; the evaluator and tuple reads return context cancellation. |
| Stale ACL after content update | Persist a `ConsistencyToken` with content and use the consistent APIs with externally consistent storage. |
| Stale or incomplete candidate index | Use snapshot-matched candidate readers; stale indexes fail closed. |
| Caveat abuse | Bind only deterministic, side-effect-free evaluators; never execute untrusted caveat code. |
| Cache collision or stale cache | Include tenant, revision, model, request, and caveat context in cache identity; invalidate from committed watches. |
| Sensitive diagnostic data | Do not log raw IDs, caveat values, or tokens without an explicit privacy policy. |

## Out of scope

The package does not authenticate requests, authorize its HTTP admin routes,
encrypt storage, replicate data, operate a global clock, or protect a host
whose process memory or trusted storage adapter is compromised. These are host
and infrastructure responsibilities.

## Supply chain

CI runs `go vet`, Staticcheck, Govulncheck, race tests, and fuzz smoke tests.
Dependabot tracks Go modules and GitHub Actions. Tagged releases generate an
SPDX SBOM and sign it with a keyless Sigstore bundle; verify both before
adopting a release.

## Deployment review

Before exposing authorization decisions to untrusted traffic, verify tenant
derivation, request deadlines, mutation authorization, model change audit,
backup/recovery, storage snapshot guarantees, watch retention, and cache
invalidation. Run the adapter conformance suite and failure tests against the
actual datastore rather than relying on the embedded KV tests.
