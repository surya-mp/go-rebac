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
	DecisionCycleDetected  DecisionReason = "cycle_detected"
	DecisionStorageError   DecisionReason = "storage_error"
)

// CheckEvent is emitted after every authorization decision. Observer is an
// adapter point for tracing, metrics, and audit systems owned by the app.
type CheckEvent struct {
	TenantID, User, Relation, Namespace, ObjectID string
	Revision                                      Revision
	ModelID                                       string
	ModelVersion                                  Revision
	Allowed                                       bool
	Reason                                        DecisionReason
	Err                                           error
	Duration                                      time.Duration
	Nodes, MaxDepth, Branches, Cycles             int
	StorageCalls, TuplesRead, CacheHits           int
	CacheHit                                      bool
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

// ObserverFuncs adapts ordinary functions to the production Observer contract.
// It keeps metrics and tracing integrations optional and dependency-free.
type ObserverFuncs struct {
	Check    func(context.Context, CheckEvent)
	Mutation func(context.Context, MutationEvent)
}

func (o ObserverFuncs) ObserveCheck(ctx context.Context, event CheckEvent) {
	if o.Check != nil {
		o.Check(ctx, event)
	}
}

func (o ObserverFuncs) ObserveMutation(ctx context.Context, event MutationEvent) {
	if o.Mutation != nil {
		o.Mutation(ctx, event)
	}
}

// DebugObserver receives privileged evaluation explanations separately from
// production events. Explanations reveal relationship tuples and must not be
// attached to normal metrics or audit sinks.
type DebugObserver interface {
	ObserveExplanation(context.Context, Explanation)
}

// DebugObserverFunc adapts a function to DebugObserver.
type DebugObserverFunc func(context.Context, Explanation)

func (f DebugObserverFunc) ObserveExplanation(ctx context.Context, explanation Explanation) {
	if f != nil {
		f(ctx, explanation)
	}
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
