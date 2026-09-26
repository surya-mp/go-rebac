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
// connection management and durability. The PostgreSQL adapter accepts an
// application-owned *sql.DB; it never opens or closes a database connection.
// PostgreSQL schemas are versioned in migrations/.
//
// For stable decisions across calls, use CheckWithRevision and pass the
// returned Revision into later reads. Storage implementations that support
// revisions also provide atomic tuple mutations and change watches for
// application-owned cache invalidation.
package rebac
