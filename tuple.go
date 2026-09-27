package rebac

// RelationTuple is one directed edge in the authorization graph.
//
// User is either a direct subject (for example, "user:alice") or a userset
// reference in the form "namespace:objectID#relation" (for example,
// "group:eng#member"). Validity-window fields are metadata: writing the same
// tuple identity again replaces its window.
type RelationTuple struct {
	TenantID          string        `json:"tenant_id"`
	Namespace         string        `json:"namespace"`
	ObjectID          string        `json:"object_id"`
	Relation          string        `json:"relation"`
	User              string        `json:"user"`
	Caveat            string        `json:"caveat,omitempty"`
	CaveatContext     CaveatContext `json:"caveat_context,omitempty"`
	NotBeforeUnixNano int64         `json:"not_before_unix_nano,omitempty"`
	NotAfterUnixNano  int64         `json:"not_after_unix_nano,omitempty"`
}
