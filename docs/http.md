# Optional HTTP handlers

The `server` package exposes a configured `*rebac.Engine` through standard
`net/http`. It does not listen on a port, authenticate callers, open a
database, or make authorization decisions about its own admin routes.

```go
api, err := server.New(engine)
if err != nil { return err }
appMux.Handle("/authz/", http.StripPrefix("/authz", api.Handler()))
adminMux.Handle("/authz-admin/", http.StripPrefix("/authz-admin", api.AdminHandler()))
```

Mount `AdminHandler` behind stricter application authorization than `Handler`.
Anyone able to write tuples can change permissions.

Use `api.WithModelStorage(models)` for model administration endpoints. The
matching typed client is constructed with `server.NewClient(baseURL, client)`;
the host configures authentication on that HTTP client's transport.

## Data-plane routes

| Route | Request | Response |
| --- | --- | --- |
| `GET /healthz` | none | `{"status":"ok"}` |
| `POST /v1/check` | `CheckRequest` | allowed + revision |
| `POST /v1/check/consistent` | `ConsistentCheckRequest` | allowed + token |
| `POST /v1/content-change-check` | `ContentChangeCheckRequest` | allowed + token |
| `POST /v1/tuples/read` | `ReadTuplesRequest` | `TuplePage` |
| `POST /v1/lookup/resources` | `LookupResourcesRequest` | `ResourcePage` |
| `POST /v1/lookup/subjects` | `LookupSubjectsRequest` | `SubjectPage` |
| `POST /v1/expand` | relation request | expansion + revision |

Tuple mutation endpoints are exposed only by `AdminHandler`:
`/v1/tuples/write`, `/v1/tuples/delete`, `/v1/tuples/mutate`, and
`/v1/objects/delete`.

With model storage configured, `Handler` serves `/v1/models/read`,
`/v1/models/versions`, and `/v1/models/active`; `AdminHandler` serves
`/v1/models/write` and `/v1/models/activate`.

## Consistent endpoint example

```json
{
  "tenant_id": "acme",
  "user": "user:alice",
  "relation": "viewer",
  "namespace": "document",
  "object_id": "roadmap",
  "opaque_token": "optional-token-stored-with-content"
}
```

Use `server.NewWithTokenCodec(engine, codec)` to enable `opaque_token`. The
response includes `opaque_token`; persist that value with the content version.
The structured `token` remains available for trusted in-process integrations.

## Safety behavior

Requests are limited to 1 MiB. Unknown and trailing JSON are rejected. Storage
errors map to `503`, mutation/revision conflicts map to `409`, and a consistent
endpoint mounted on a non-consistent engine returns `501`.

Add application middleware for authentication, tenant derivation, request IDs,
rate limits, request timeouts, CORS, tracing, and audit logging.
