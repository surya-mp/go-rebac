// Package rebac implements relationship-based access control (ReBAC) for Go.
// It evaluates application-defined, Zanzibar-style relationship tuples without
// requiring an external authorization service.
//
// An application defines its object namespaces, relations, allowed subject
// types, and optional rewrites in AuthorizationModel. It then creates an Engine
// with a StorageEngine and supplies a tenant ID for every check and mutation.
// The package supports direct relationships, nested usersets, computed
// usersets, tuple-to-userset, union, intersection, exclusion, and optional
// application-defined caveats.
//
// Tuple strings use namespace:objectID for direct subjects and
// namespace:objectID#relation for usersets. For example,
// "group:engineering#member" allows a document relation to inherit group
// membership.
//
// Engine is safe for concurrent checks. StorageEngine implementations own
// connection management, durability, and database-driver selection. The core
// package never opens or closes a database connection.
//
// For stable decisions across calls, use CheckWithRevision and pass the
// returned Revision into later reads. Storage implementations that support
// revisions also provide atomic tuple mutations and change watches for
// application-owned cache invalidation.
//
// ModelDocument and ModelStorage provide optional, versioned serialization of
// application models. NewProductionEngine makes revision consistency, atomic
// mutations, and indexed lookups explicit for production storage. Storage
// authors can use the conformance package to exercise the portable contract.
// NewConsistentEngineFromModelStorage adds an optional Zanzibar-style protocol
// that evaluates a model and tuple graph from one at-least-as-fresh revision.
package rebac
