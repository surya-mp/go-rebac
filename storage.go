package rebac

import (
	"context"
	"errors"
)

var ErrSnapshotsUnsupported = errors.New("rebac: storage does not support consistent snapshots")

var (
	ErrRevisionsUnsupported = errors.New("rebac: storage does not support revision tokens")
	ErrInvalidRevision      = errors.New("rebac: invalid revision")
	ErrMutationUnsupported  = errors.New("rebac: storage does not support atomic mutations")
	ErrPreconditionFailed   = errors.New("rebac: mutation precondition failed")
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

// TupleOperation is the requested mutation for one tuple.
type TupleOperation string

const (
	WriteOperation  TupleOperation = "write"
	DeleteOperation TupleOperation = "delete"
)

// TupleChange is one write or delete in an atomic mutation.
type TupleChange struct {
	Operation TupleOperation
	Tuple     RelationTuple
}

// Precondition asserts whether an exact, currently live tuple must exist
// before the mutation is applied.
type Precondition struct {
	Tuple     RelationTuple
	MustExist bool
}

// MutationStorage atomically applies changes and returns their shared revision.
type MutationStorage interface {
	Mutate(ctx context.Context, changes []TupleChange, preconditions []Precondition) (Revision, error)
}

// WatchEvent reports tuple changes committed at one revision.
type WatchEvent struct {
	Revision Revision
	TenantID string
	Changes  []TupleChange
}

// WatchStorage streams committed changes after a revision. Applications use it
// to invalidate their own caches; the library never owns a cache.
type WatchStorage interface {
	Watch(ctx context.Context, tenantID string, after Revision) (<-chan WatchEvent, <-chan error)
}
