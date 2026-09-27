package conformance

import (
	"context"
	"testing"

	"github.com/surya-mp/go-rebac"
)

type memoryStore []rebac.RelationTuple

func (s memoryStore) QueryTuples(_ context.Context, filter rebac.RelationTuple) ([]rebac.RelationTuple, error) {
	var result []rebac.RelationTuple
	for _, tuple := range s {
		if (filter.TenantID == "" || filter.TenantID == tuple.TenantID) &&
			(filter.Namespace == "" || filter.Namespace == tuple.Namespace) &&
			(filter.ObjectID == "" || filter.ObjectID == tuple.ObjectID) &&
			(filter.Relation == "" || filter.Relation == tuple.Relation) &&
			(filter.User == "" || filter.User == tuple.User) {
			result = append(result, tuple)
		}
	}
	return result, nil
}

func (s *memoryStore) WriteTuple(_ context.Context, tuple rebac.RelationTuple) error {
	*s = append(*s, tuple)
	return nil
}

func (s *memoryStore) DeleteTuple(_ context.Context, tuple rebac.RelationTuple) error {
	for i, candidate := range *s {
		if candidate.TenantID == tuple.TenantID && candidate.Namespace == tuple.Namespace && candidate.ObjectID == tuple.ObjectID && candidate.Relation == tuple.Relation && candidate.User == tuple.User {
			*s = append((*s)[:i], (*s)[i+1:]...)
			break
		}
	}
	return nil
}

func TestRun(t *testing.T) {
	Run(t, func(testing.TB) rebac.StorageEngine { return &memoryStore{} })
}

func TestReferenceScenarios(t *testing.T) {
	RunReferenceScenarios(t, func(testing.TB) rebac.StorageEngine { return &memoryStore{} })
}
