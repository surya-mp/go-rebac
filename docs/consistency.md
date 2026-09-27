# Consistency and content changes

Use the baseline engine when a current authorization result is enough. Use the
strict consistency path when authorization must respect causal ordering between
ACL changes and application-content versions.

## The content-token protocol

1. Before saving a new content version, call `ContentChangeCheck`.
2. If allowed, atomically persist the content version **and returned token** in
   the application's content store.
3. When reading that content, load its stored token and call
   `CheckWithConsistency` with it.
4. The store selects a snapshot at least as fresh as the token, loads the model
   effective at that same revision, and evaluates the graph there.

```go
allowed, token, err := engine.ContentChangeCheck(ctx, rebac.ContentChangeCheckRequest{
	TenantID: "acme", User: "user:alice", Relation: "editor",
	Namespace: "document", ObjectID: "roadmap",
})
if err != nil {
	return err
}
if !allowed {
	return errors.New("forbidden")
}
if err := contentStore.Save(ctx, roadmap, token); err != nil {
	return err
}

content, token := contentStore.Load(ctx, "roadmap")
allowed, _, err = engine.CheckWithConsistency(ctx, token,
	"acme", "user:alice", "viewer", "document", content.ID)
```

The application must supply the token from trusted content metadata, not accept
an arbitrary caller-provided older token as the authority for a content read.

## Strict construction

```go
engine, model, err := rebac.NewConsistentEngineFromModelStorage(
	ctx, tuples, models,
	rebac.ModelSelection{TenantID: "acme", ModelID: "document_access"},
	caveats,
)
```

`tuples` implements `ConsistentStorage`; `models` implements
`RevisionedModelStorage`. Both must use the same externally ordered revision
sequence. `ReadAuthorizationModelAtRevision` returns the configuration that
was effective at the chosen tuple snapshot—not a newer configuration.

## Opaque transport tokens

`ConsistencyToken` is the in-process value. For HTTP, cookies, queues, or
external content stores, use a `TokenCodec`:

```go
codec, err := rebac.NewHMACTokenCodec(secret, 24*time.Hour)
encoded, err := codec.Encode(token)
token, err = codec.Decode(encoded)
```

`HMACTokenCodec` authenticates a versioned token and can expire it. Keep its
key in application secret management; rotate keys by accepting old keys during
a migration, then issuing only new tokens. A token expiry requires the client
to obtain a new content-consistency token.

## Consistent reads and watches

`ReadTuplesWithConsistency`, `LookupResourcesWithConsistency`,
`LookupSubjectsWithConsistency`, and `ExpandWithConsistency` return both their
normal response and the selected `ConsistencyToken`.

Use `WatchWithConsistency` to resume a strict tuple watch. Persist the latest
event or heartbeat revision. If history has been garbage collected, take a new
snapshot and restart from that token.
