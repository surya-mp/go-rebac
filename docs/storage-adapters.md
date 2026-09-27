# Storage adapters

`StorageEngine` is the portable minimum: tenant-scoped tuple reads, writes,
and deletes. Tuple reads must return only tuples matching the requested tenant;
the evaluator revalidates every returned tuple and denies on violations.

Production adapters should implement `ProductionStorage`: atomic mutation,
revisioned snapshots, and complete resource/subject candidate readers. To use
the Zanzibar-style consistency APIs, implement `ConsistentStorage` and make
tuple snapshots and model reads share the same ordered revision sequence.

Candidate readers may return extra candidates, but never omit a resource or
subject that can pass `Check` at the requested revision. The engine verifies
every candidate with the canonical evaluator.

Implement optional paging, batch reads, watches, and model-version listing
when the backing database supports them. Watches must be ordered and resumable
from a revision; snapshots must not observe writes after their revision.

Run `conformance.Run` for all adapters and `conformance.RunConsistent` before
claiming consistent-storage support. `memory.New()` is the strongest bundled
in-process reference implementation; it is useful for tests and benchmarks,
not durable production state.
