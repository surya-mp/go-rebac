# Optional HTTP service

`github.com/surya-mp/go-rebac/server` adapts an already configured
`*rebac.Engine` to standard `net/http` handlers. It does not open a database,
choose a driver, authenticate callers, or start a listener.

```go
engine, document, err := rebac.NewProductionEngineFromModelStorage(
	ctx, store, models,
	rebac.ModelSelection{TenantID: "acme", ModelID: "document_access"},
	caveats,
)
_ = document

api, err := server.New(engine)
if err != nil {
	return err
}

appMux.Handle("/authz/", http.StripPrefix("/authz", api.Handler()))
adminMux.Handle("/authz-admin/", http.StripPrefix("/authz-admin", api.AdminHandler()))
```

`Handler` is the data plane:

| Method and path | Operation |
| --- | --- |
| `GET /healthz` | Liveness response |
| `POST /v1/check` | Authorization check |
| `POST /v1/batch-check` | Check up to 100 independent relations |
| `POST /v1/check/consistent` | At-least-as-fresh authorization check |
| `POST /v1/content-change-check` | Produce a token for a content version |
| `POST /v1/tuples/read` | Read stored tuples |
| `POST /v1/lookup/resources` | Find permitted resources |
| `POST /v1/lookup/subjects` | Find permitted direct subjects |
| `POST /v1/expand` | Expand a relation graph |

`AdminHandler` is intentionally separate. Protect it with stricter application
authorization before exposing `POST /v1/tuples/write`,
`POST /v1/tuples/delete`, or `POST /v1/tuples/mutate`.

The handlers use JSON with the field names shown in the Go request types. A
single request body is limited to 1 MiB, requests time out after five seconds,
and unknown or trailing JSON is rejected. `WithRequestLimits` can lower the
body, batch, and timeout limits; engine graph limits still apply to every
batch entry. A persistent `rebacd` binary is not bundled because selecting and
linking a database driver is a deployment decision; applications can mount
these handlers directly or provide their own thin binary.

`server.NewWithTokenCodec` enables authenticated opaque tokens on the two
consistent endpoints. Clients send `opaque_token` and should persist the
returned value with their content version.

For complete route payloads and deployment guidance, see [http.md](http.md).

The embedded engine dispatches a request in-process. Cross-region routing,
request hedging, shared caches, and replica management belong to the host
service or deployment platform; they are deliberately not simulated here.
