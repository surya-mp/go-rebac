package rebac

import (
	"context"
	"errors"
)

var ErrRegionUnavailable = errors.New("rebac: requested region is unavailable")

// RegionalStore is one replica capable of serving revision-pinned reads.
type RegionalStore struct {
	Name  string
	Store RevisionedStorage
}

// ReplicaSet routes writes to Primary and lets callers select a read replica
// only when that replica can serve the requested revision.
type ReplicaSet struct {
	Primary  StorageEngine
	Replicas map[string]RevisionedStorage
}

func NewReplicaSet(primary StorageEngine, replicas []RegionalStore) (*ReplicaSet, error) {
	if primary == nil {
		return nil, errors.New("rebac: storage engine is nil")
	}
	set := &ReplicaSet{Primary: primary, Replicas: make(map[string]RevisionedStorage, len(replicas))}
	for _, replica := range replicas {
		if replica.Name == "" || replica.Store == nil {
			return nil, ErrRegionUnavailable
		}
		set.Replicas[replica.Name] = replica.Store
	}
	return set, nil
}

// SnapshotAtRegion returns a revision-consistent read view from region. It
// never silently falls back to a stale replica.
func (s *ReplicaSet) SnapshotAtRegion(ctx context.Context, region string, revision Revision) (TupleReader, Revision, func() error, error) {
	store, ok := s.Replicas[region]
	if !ok {
		return nil, "", nil, ErrRegionUnavailable
	}
	return store.SnapshotAt(ctx, revision)
}

// PrimaryStore exposes the writer for engine construction and mutations.
func (s *ReplicaSet) PrimaryStore() StorageEngine { return s.Primary }
