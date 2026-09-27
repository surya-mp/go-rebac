package rebac

import "context"

// ExplainRequest is a privileged debugging request. Caveat context is used for
// evaluation but is never copied into the resulting explanation.
type ExplainRequest struct {
	TenantID, User, Relation, Namespace, ObjectID string
	Revision                                      Revision
	CaveatContext                                 CaveatContext
	AsOfUnixNano                                  int64
}

// Explanation is the bounded trace of one Check evaluation.
type Explanation struct {
	Allowed      bool
	Revision     Revision
	ModelID      string
	ModelVersion Revision
	Nodes        []ExplanationNode
}

// ExplanationNode is one traversed relation. A false Allowed value records a
// failed branch; Error is populated when that branch failed closed.
type ExplanationNode struct {
	User, Relation, Namespace, ObjectID string
	Rewrite                             Rewrite
	Tuples                              []ExplanationTuple
	Allowed                             bool
	Error                               string
}

// ExplanationTuple is a tuple considered by a relation node. Tuple caveat
// context is omitted to avoid copying sensitive application values.
type ExplanationTuple struct {
	Tuple   RelationTuple
	Applies bool
	Matched bool
}

type explainContextKey struct{}
type explainNodeContextKey struct{}

type explainTrace struct{ nodes []ExplanationNode }

func explainTraceFromContext(ctx context.Context) *explainTrace {
	trace, _ := ctx.Value(explainContextKey{}).(*explainTrace)
	return trace
}

func explainNodeFromContext(ctx context.Context) int {
	node, _ := ctx.Value(explainNodeContextKey{}).(int)
	return node
}

func (t *explainTrace) addNode(user, relation, namespace, objectID string, rewrite Rewrite) int {
	t.nodes = append(t.nodes, ExplanationNode{User: user, Relation: relation, Namespace: namespace, ObjectID: objectID, Rewrite: cloneRewrite(rewrite)})
	return len(t.nodes) - 1
}

func (t *explainTrace) finishNode(node int, allowed bool, err error) {
	if node < 0 || node >= len(t.nodes) {
		return
	}
	t.nodes[node].Allowed = allowed
	if err != nil {
		t.nodes[node].Error = err.Error()
	}
}

func (t *explainTrace) addTuple(node int, tuple RelationTuple) int {
	if node < 0 || node >= len(t.nodes) {
		return -1
	}
	tuple.CaveatContext = nil
	t.nodes[node].Tuples = append(t.nodes[node].Tuples, ExplanationTuple{Tuple: tuple})
	return len(t.nodes[node].Tuples) - 1
}

func (t *explainTrace) finishTuple(node, tuple int, applies, matched bool) {
	if node < 0 || node >= len(t.nodes) || tuple < 0 || tuple >= len(t.nodes[node].Tuples) {
		return
	}
	t.nodes[node].Tuples[tuple].Applies = applies
	t.nodes[node].Tuples[tuple].Matched = matched
}

// Explain evaluates the same graph as Check and returns its bounded traversal
// trace. Treat explanations as privileged debugging data because they reveal
// relationship tuples.
func (e *Engine) Explain(ctx context.Context, request ExplainRequest) (explanation Explanation, err error) {
	trace := &explainTrace{}
	ctx = context.WithValue(ctx, explainContextKey{}, trace)
	defer func() {
		if e != nil && e.debugObserver != nil {
			e.debugObserver.ObserveExplanation(ctx, explanation)
		}
	}()
	contextValues := request.CaveatContext
	if request.AsOfUnixNano != 0 {
		contextValues = withAsOf(contextValues, request.AsOfUnixNano)
	}
	explanation = Explanation{}
	if e != nil && e.modelSource != nil {
		if request.Revision != "" {
			return explanation, ErrConsistencyUnsupported
		}
		var token ConsistencyToken
		explanation.Allowed, token, err = e.CheckWithConsistencyAndContext(ctx, ConsistencyToken{}, contextValues, request.TenantID, request.User, request.Relation, request.Namespace, request.ObjectID)
		explanation.Revision, explanation.ModelID, explanation.ModelVersion = token.TupleRevision, token.ModelID, token.ModelVersion
	} else {
		explanation.Allowed, explanation.Revision, err = e.CheckWithRevisionAndContext(ctx, request.Revision, contextValues, request.TenantID, request.User, request.Relation, request.Namespace, request.ObjectID)
		if e != nil {
			explanation.ModelID, explanation.ModelVersion = e.modelID, e.modelVersion
		}
	}
	explanation.Nodes = trace.nodes
	return explanation, err
}
