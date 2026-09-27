package rebac

import (
	"context"
	"errors"
)

// ErrDispatch is returned when a dispatcher cannot provide a safe decision.
// Callers must treat it as deny; Engine never falls back to another snapshot.
var ErrDispatch = errors.New("rebac: check dispatch failed")

// CheckDispatchRequest is one exact-snapshot check that may be evaluated by a
// remote authorization worker. Empty Revision requests the worker's latest
// consistent view; a non-empty Revision requires that exact view.
type CheckDispatchRequest struct {
	TenantID      string
	User          string
	Relation      string
	Namespace     string
	ObjectID      string
	Revision      Revision
	CaveatContext CaveatContext
}

// CheckDispatchResult is the remote decision and the exact view it used.
type CheckDispatchResult struct {
	Allowed  bool
	Revision Revision
}

// CheckDispatcher routes top-level checks to another worker. The worker must
// apply the same model and snapshot contract as this Engine. Dispatchers are
// deliberately transport-neutral; HTTP, RPC, and in-process queues can all
// implement this boundary without coupling the evaluator to a network stack.
type CheckDispatcher interface {
	DispatchCheck(ctx context.Context, request CheckDispatchRequest) (CheckDispatchResult, error)
}

// WithCheckDispatcher returns a copy that dispatches top-level revisioned
// checks. Recursive subproblems remain local to the selected worker.
func (e *Engine) WithCheckDispatcher(dispatcher CheckDispatcher) *Engine {
	configured := *e
	configured.dispatcher = dispatcher
	return &configured
}
