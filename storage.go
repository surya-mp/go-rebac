package rebac

import (
	"context"
	"errors"
)

var ErrSnapshotsUnsupported = errors.New("rebac: storage does not support consistent snapshots")

var (
	ErrRevisionsUnsupported   = errors.New("rebac: storage does not support revision tokens")
	ErrInvalidRevision        = errors.New("rebac: invalid revision")
	ErrMutationUnsupported    = errors.New("rebac: storage does not support atomic mutations")
	ErrPreconditionFailed     = errors.New("rebac: mutation precondition failed")
	ErrProductionStorage      = errors.New("rebac: production storage capabilities are required")
	ErrConsistencyUnsupported = errors.New("rebac: storage does not support at-least-as-fresh consistency")
	ErrIndexSnapshot          = errors.New("rebac: lookup index cannot serve the selected snapshot")
)

// Revision is an opaque datastore position returned by a checked read or
// mutation. Pass it back to CheckWithRevision to evaluate the same graph view.
type Revision string

// StorageEngine persists relationship tuples. In QueryTuples, empty filter
// fields are wildcards; non-empty fields must match exactly.
type StorageEngine interface {
	TupleReader
	WriteTuple(ctx context.Context, tuple RelationTuple) error
	DeleteTuple(ctx context.Context, tuple RelationTuple) error
}

// TupleReader is the read-only portion of a tuple store.
type TupleReader interface {
	QueryTuples(ctx context.Context, filter RelationTuple) ([]RelationTuple, error)
}

// BatchTupleReader can satisfy independent tuple filters in one storage call.
type BatchTupleReader interface {
	QueryTuplesBatch(ctx context.Context, filters []RelationTuple) ([][]RelationTuple, error)
}

// PagedTupleReader performs deterministic storage-backed tuple pagination.
// Offset is zero-based and hasMore reports whether another tuple exists.
type PagedTupleReader interface {
	QueryTuplesPage(ctx context.Context, filter RelationTuple, limit, offset int) (tuples []RelationTuple, hasMore bool, err error)
}

// ResourceCandidateReader uses a datastore's reverse indexes to return a
// complete superset of objects that may grant the request. Engine re-checks
// every candidate, so this interface cannot grant access by itself.
//
// Implement it when LookupResources must avoid a tenant-wide tuple scan.
type ResourceCandidateReader interface {
	LookupResourceCandidates(ctx context.Context, request LookupResourcesRequest) ([]string, error)
}

// SubjectCandidateReader uses datastore indexes to return a complete superset
// of direct subjects that may grant the request. Engine verifies each subject
// before returning it.
type SubjectCandidateReader interface {
	LookupSubjectCandidates(ctx context.Context, request LookupSubjectsRequest) ([]string, error)
}

// SnapshotResourceCandidateReader returns candidates indexed at exactly
// snapshot. Returning another revision makes the index unsafe for a
// revision-pinned authorization lookup.
type SnapshotResourceCandidateReader interface {
	LookupResourceCandidatesAt(ctx context.Context, snapshot Revision, request LookupResourcesRequest) (candidates []string, indexedAt Revision, err error)
}

// SnapshotSubjectCandidateReader is SnapshotResourceCandidateReader for the
// inverse lookup direction.
type SnapshotSubjectCandidateReader interface {
	LookupSubjectCandidatesAt(ctx context.Context, snapshot Revision, request LookupSubjectsRequest) (candidates []string, indexedAt Revision, err error)
}

// SnapshotStorage provides a consistent read view for an entire Check call.
// The caller must invoke the returned release function exactly once.
type SnapshotStorage interface {
	Snapshot(ctx context.Context) (TupleReader, func() error, error)
}

// RevisionedStorage supports stable, externally visible authorization views.
// Empty revision means "latest".
type RevisionedStorage interface {
	SnapshotAt(ctx context.Context, revision Revision) (TupleReader, Revision, func() error, error)
	WriteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error)
	DeleteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error)
}

// AtLeastFreshStorage selects one consistent snapshot no older than minimum.
// The returned revision may be newer than minimum. Implementations used for
// Zanzibar-style checks must derive revisions from a single externally
// consistent ordering shared by tuple and model writes.
type AtLeastFreshStorage interface {
	SnapshotAtLeast(ctx context.Context, minimum Revision) (TupleReader, Revision, func() error, error)
}

// TupleOperation is the requested mutation for one tuple.
type TupleOperation string

const (
	WriteOperation  TupleOperation = "write"
	DeleteOperation TupleOperation = "delete"
)

// TupleChange is one write or delete in an atomic mutation.
type TupleChange struct {
	Operation TupleOperation `json:"operation"`
	Tuple     RelationTuple  `json:"tuple"`
}

// Precondition asserts whether an exact, currently live tuple must exist
// before the mutation is applied.
type Precondition struct {
	Tuple     RelationTuple `json:"tuple"`
	MustExist bool          `json:"must_exist"`
}

// MutationStorage atomically applies changes and returns their shared revision.
type MutationStorage interface {
	Mutate(ctx context.Context, changes []TupleChange, preconditions []Precondition) (Revision, error)
}

// ObjectDeletionStorage atomically removes every relationship on one object.
// References to that object remain inert because its relation tuples are gone.
type ObjectDeletionStorage interface {
	DeleteObject(ctx context.Context, tenantID, namespace, objectID string) (Revision, error)
}

// WatchEvent reports tuple changes committed at one revision.
type WatchEvent struct {
	Revision  Revision      `json:"revision"`
	TenantID  string        `json:"tenant_id"`
	Changes   []TupleChange `json:"changes,omitempty"`
	Heartbeat bool          `json:"heartbeat,omitempty"`
}

// WatchStorage streams committed changes after a revision. Applications use it
// to invalidate their own caches; the library never owns a cache.
type WatchStorage interface {
	Watch(ctx context.Context, tenantID string, after Revision) (<-chan WatchEvent, <-chan error)
}

// WatchRequest resumes an ordered tuple change stream. A heartbeat advances
// Revision without carrying changes; a store must return ErrInvalidRevision
// when After has fallen outside its retained history.
type WatchRequest struct {
	TenantID   string   `json:"tenant_id"`
	After      Revision `json:"after,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
}

// ResumableWatchStorage is the strict Watch contract used by ConsistentStorage.
type ResumableWatchStorage interface {
	WatchTuples(ctx context.Context, request WatchRequest) (<-chan WatchEvent, <-chan error)
}

// ProductionStorage is the minimum contract for an adapter used by
// NewProductionEngine. It guarantees revision-pinned reads, atomic writes,
// and storage-backed candidates for both lookup directions.
//
// StorageEngine remains intentionally smaller for tests, prototypes, and
// applications that only need Check and tuple writes.
type ProductionStorage interface {
	StorageEngine
	RevisionedStorage
	MutationStorage
	ResourceCandidateReader
	SubjectCandidateReader
}

// ConsistentStorage is the stronger optional contract required by
// NewConsistentEngineFromModelStorage. It makes at-least-as-fresh snapshots
// and revision-matched lookup indexes mandatory; StorageEngine stays small so
// ordinary applications remain database agnostic.
type ConsistentStorage interface {
	ProductionStorage
	AtLeastFreshStorage
	SnapshotResourceCandidateReader
	SnapshotSubjectCandidateReader
	ResumableWatchStorage
}
