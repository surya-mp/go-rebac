package rebac

import (
	"context"
	"sync/atomic"
	"time"
)

// DecisionReason is a stable, low-cardinality check outcome for metrics and
// audit sinks. The original error remains available on CheckEvent.
type DecisionReason string

const (
	DecisionAllowed        DecisionReason = "allowed"
	DecisionDenied         DecisionReason = "denied"
	DecisionInvalidRequest DecisionReason = "invalid_request"
	DecisionLimitExceeded  DecisionReason = "limit_exceeded"
	DecisionStorageError   DecisionReason = "storage_error"
)

// CheckEvent is emitted after every authorization decision. Observer is an
// adapter point for tracing, metrics, and audit systems owned by the app.
type CheckEvent struct {
	TenantID, User, Relation, Namespace, ObjectID string
	Revision                                      Revision
	Allowed                                       bool
	Reason                                        DecisionReason
	Err                                           error
	Duration                                      time.Duration
	Nodes                                         int
}

// MutationEvent is emitted after each write, delete, or batch mutation.
type MutationEvent struct {
	Revision Revision
	Changes  int
	Err      error
}

// Observer receives completed events synchronously. Keep implementations
// non-blocking; the application can enqueue work if it needs asynchronous I/O.
type Observer interface {
	ObserveCheck(context.Context, CheckEvent)
	ObserveMutation(context.Context, MutationEvent)
}

// Stats is a lock-free process-local counter snapshot.
type Stats struct {
	Checks, Allowed, Denied, Errors, Mutations uint64
}

type engineStats struct {
	checks, allowed, denied, errors, mutations atomic.Uint64
}

func (s *engineStats) snapshot() Stats {
	return Stats{
		Checks: s.checks.Load(), Allowed: s.allowed.Load(), Denied: s.denied.Load(),
		Errors: s.errors.Load(), Mutations: s.mutations.Load(),
	}
}
