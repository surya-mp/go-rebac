package rebac

// RelationTuple is one directed edge in the authorization graph.
//
// User is either a direct subject (for example, "user:alice") or a userset
// reference in the form "namespace:objectID#relation" (for example,
// "group:eng#member").
type RelationTuple struct {
	TenantID      string        `json:"tenant_id"`
	Namespace     string        `json:"namespace"`
	ObjectID      string        `json:"object_id"`
	Relation      string        `json:"relation"`
	User          string        `json:"user"`
	Caveat        string        `json:"caveat,omitempty"`
	CaveatContext CaveatContext `json:"caveat_context,omitempty"`
}
