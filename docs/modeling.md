# Authorization modeling guide

An `AuthorizationModel` declares object namespaces and their relations. The
model is a type system for tuples: a tuple that does not match an allowed
subject reference is rejected before it reaches storage.

## Subjects

| Form | Meaning |
| --- | --- |
| `user:alice` | A direct subject |
| `group:engineering#member` | Everyone in a userset |
| `folder:product` | A direct object reference for tuple-to-userset |

Names use lower snake case. Object and tenant IDs are opaque non-empty strings
without whitespace, control characters, or `#`.

## Direct relation

```go
"viewer": {AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}}},
```

This relation accepts only direct `user:*` tuples and has the default `This`
rewrite.

## Nested usersets

```go
"viewer": {AllowedSubjects: []rebac.SubjectReference{
	{Namespace: "user"},
	{Namespace: "group", Relation: "member"},
}},
```

Now `document:roadmap#viewer@group:engineering#member` grants every group
member access. Cycles fail the check closed with `ErrCycleDetected` rather than
looping forever.

## Rewrite operations

Exactly one rewrite operation is selected for a relation. A zero-value rewrite
means `This`.

| Rewrite | Meaning |
| --- | --- |
| `This` | Direct tuples and nested usersets on this relation |
| `ComputedUserset` | Another relation on the same object |
| `TupleToUserset` | Follow a relation to another object, then evaluate its relation |
| `Union` | Any child grants |
| `Intersection` | Every child grants |
| `Exclusion` | Base grants and subtract does not |

### Owner implies editor

```go
"editor": {
	AllowedSubjects: []rebac.SubjectReference{{Namespace: "user"}},
	Rewrite: rebac.Rewrite{Union: []rebac.Rewrite{
		{This: true},
		{ComputedUserset: "owner"},
	}},
},
```

### Folder permission inherited by a document

`parent` is a direct `folder:*` relation. `via_parent` follows it and evaluates
`folder#viewer` on the referenced folder.

```go
"via_parent": {Rewrite: rebac.Rewrite{TupleToUserset: &rebac.TupleToUserset{
	Tupleset: "parent", ComputedUserset: "viewer",
}}},
```

### Block list

```go
"access": {Rewrite: rebac.Rewrite{Exclusion: &rebac.Exclusion{
	Base: rebac.Rewrite{ComputedUserset: "editor"},
	Subtract: rebac.Rewrite{ComputedUserset: "blocked"},
}}},
```

## Caveats

Caveats attach deterministic request-time conditions to tuples. The evaluator
must not perform I/O or depend on mutable process state.

```go
model.Caveats = map[string]rebac.CaveatDefinition{
	"same_region": {Evaluate: func(_ context.Context, tuple, request rebac.CaveatContext) (bool, error) {
		return tuple["region"] == request["region"], nil
	}},
}
```

Write a tuple with `Caveat: "same_region"` and evaluate it with
`CheckWithContext`. Caveat code is application code and is intentionally not
serialized in `ModelDocument`; bind it again with `ModelDocument.Compile`.

## Time windows and public access

Set `NotBeforeUnixNano` and/or `NotAfterUnixNano` on a tuple for a fixed,
half-open validity window. `CheckAt`, `LookupResources`, and `LookupSubjects`
use `AsOfUnixNano`; ordinary `Check` denies interval-bearing tuples.

Set `AllowWildcard: true` on a relation to allow `user:*`. It grants that
relation to every direct `user:*` subject, never to usersets.

## Versioned models

Use `ModelDocument` and `ModelStorage` when model configuration is data rather
than static Go code. `Version` is an optimistic-concurrency value; use it as
the expected value for an update. For strict consistency, use
`RevisionedModelStorage` and load the model at the tuple snapshot revision.
