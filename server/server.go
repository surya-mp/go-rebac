// Package server exposes a rebac.Engine through optional net/http handlers.
// It owns neither authentication nor datastore connections.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/surya-mp/go-rebac"
)

// Server adapts one configured Engine to data-plane and administration handlers.
type Server struct {
	engine       *rebac.Engine
	models       rebac.ModelStorage
	codec        rebac.TokenCodec
	maxBodyBytes int64
	maxBatchSize int
	timeout      time.Duration
}

const (
	defaultMaxBodyBytes = 1 << 20
	defaultMaxBatchSize = 100
	defaultTimeout      = 5 * time.Second
)

// RequestLimits bound work accepted by the optional HTTP adapter. Zero values
// retain the secure defaults.
type RequestLimits struct {
	MaxBodyBytes int64
	MaxBatchSize int
	Timeout      time.Duration
}

// New returns HTTP handlers for an already constructed engine.
func New(engine *rebac.Engine) (*Server, error) {
	if engine == nil {
		return nil, errors.New("rebac/server: engine is nil")
	}
	return &Server{engine: engine, maxBodyBytes: defaultMaxBodyBytes, maxBatchSize: defaultMaxBatchSize, timeout: defaultTimeout}, nil
}

// NewWithTokenCodec configures opaque, authenticated consistency tokens for
// the Zanzibar-style endpoints. The codec and its key remain application-owned.
func NewWithTokenCodec(engine *rebac.Engine, codec rebac.TokenCodec) (*Server, error) {
	if engine == nil || codec == nil {
		return nil, errors.New("rebac/server: engine and token codec are required")
	}
	return &Server{engine: engine, codec: codec, maxBodyBytes: defaultMaxBodyBytes, maxBatchSize: defaultMaxBatchSize, timeout: defaultTimeout}, nil
}

// WithModelStorage returns a copy exposing model administration endpoints.
// Mount AdminHandler behind separate, stricter application authorization.
func (s *Server) WithModelStorage(models rebac.ModelStorage) *Server {
	configured := *s
	configured.models = models
	return &configured
}

// WithRequestLimits returns a copy with explicit HTTP request ceilings.
func (s *Server) WithRequestLimits(limits RequestLimits) *Server {
	configured := *s
	if limits.MaxBodyBytes > 0 {
		configured.maxBodyBytes = limits.MaxBodyBytes
	}
	if limits.MaxBatchSize > 0 {
		configured.maxBatchSize = limits.MaxBatchSize
	}
	if limits.Timeout > 0 {
		configured.timeout = limits.Timeout
	}
	return &configured
}

// Handler exposes read-only authorization endpoints. Mount it behind the
// application's authentication middleware; this package does not authenticate
// callers or interpret their identity.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/check", s.check)
	mux.HandleFunc("POST /v1/batch-check", s.batchCheck)
	mux.HandleFunc("POST /v1/check/consistent", s.checkConsistent)
	mux.HandleFunc("POST /v1/content-change-check", s.contentChangeCheck)
	mux.HandleFunc("POST /v1/tuples/read", s.readTuples)
	mux.HandleFunc("POST /v1/lookup/resources", s.lookupResources)
	mux.HandleFunc("POST /v1/lookup/subjects", s.lookupSubjects)
	mux.HandleFunc("POST /v1/expand", s.expand)
	mux.HandleFunc("POST /v1/models/read", s.readModel)
	mux.HandleFunc("POST /v1/models/versions", s.listModelVersions)
	mux.HandleFunc("POST /v1/models/active", s.readActiveModel)
	return s.withTimeout(mux)
}

// AdminHandler exposes tuple mutation endpoints. Mount it separately behind
// stricter application authorization; granting it to ordinary callers lets
// them modify permissions.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tuples/write", s.writeTuple)
	mux.HandleFunc("POST /v1/tuples/delete", s.deleteTuple)
	mux.HandleFunc("POST /v1/tuples/mutate", s.mutate)
	mux.HandleFunc("POST /v1/objects/delete", s.deleteObject)
	mux.HandleFunc("POST /v1/models/write", s.writeModel)
	mux.HandleFunc("POST /v1/models/activate", s.activateModel)
	return s.withTimeout(mux)
}

// CheckRequest is the JSON payload for POST /v1/check.
type CheckRequest struct {
	TenantID      string              `json:"tenant_id"`
	User          string              `json:"user"`
	Relation      string              `json:"relation"`
	Namespace     string              `json:"namespace"`
	ObjectID      string              `json:"object_id"`
	Revision      rebac.Revision      `json:"revision,omitempty"`
	CaveatContext rebac.CaveatContext `json:"caveat_context,omitempty"`
	AsOfUnixNano  int64               `json:"as_of_unix_nano,omitempty"`
}

// CheckResponse is returned by POST /v1/check.
type CheckResponse struct {
	Allowed  bool           `json:"allowed"`
	Revision rebac.Revision `json:"revision,omitempty"`
}

// BatchCheckRequest contains independently evaluated authorization checks.
type BatchCheckRequest struct {
	Checks []CheckRequest `json:"checks"`
}

// BatchCheckResponse preserves the input check order.
type BatchCheckResponse struct {
	Checks []CheckResponse `json:"checks"`
}

// ConsistentCheckRequest adds an at-least-as-fresh token to a check.
type ConsistentCheckRequest struct {
	TenantID       string                 `json:"tenant_id"`
	User           string                 `json:"user"`
	Relation       string                 `json:"relation"`
	Namespace      string                 `json:"namespace"`
	ObjectID       string                 `json:"object_id"`
	CaveatContext  rebac.CaveatContext    `json:"caveat_context,omitempty"`
	AsOfUnixNano   int64                  `json:"as_of_unix_nano,omitempty"`
	AtLeastAsFresh rebac.ConsistencyToken `json:"at_least_as_fresh,omitempty"`
	OpaqueToken    string                 `json:"opaque_token,omitempty"`
}

// ConsistentCheckResponse includes the selected authorization view token.
type ConsistentCheckResponse struct {
	Allowed     bool                   `json:"allowed"`
	Token       rebac.ConsistencyToken `json:"token"`
	OpaqueToken string                 `json:"opaque_token,omitempty"`
}

type contentChangeCheckRequest struct {
	rebac.ContentChangeCheckRequest
	OpaqueToken string `json:"opaque_token,omitempty"`
}

type tupleRequest struct {
	Tuple rebac.RelationTuple `json:"tuple"`
}

type objectRequest struct {
	TenantID  string `json:"tenant_id"`
	Namespace string `json:"namespace"`
	ObjectID  string `json:"object_id"`
}

type mutateRequest struct {
	Changes       []rebac.TupleChange  `json:"changes"`
	Preconditions []rebac.Precondition `json:"preconditions,omitempty"`
}

type revisionResponse struct {
	Revision rebac.Revision `json:"revision,omitempty"`
}

// ExpandRequest identifies a relationship expression to expand.
type ExpandRequest struct {
	TenantID  string         `json:"tenant_id"`
	Revision  rebac.Revision `json:"revision,omitempty"`
	Relation  string         `json:"relation"`
	Namespace string         `json:"namespace"`
	ObjectID  string         `json:"object_id"`
}

// ExpandResponse is the canonical expansion tree and revision used.
type ExpandResponse struct {
	Expansion rebac.Expansion `json:"expansion"`
	Revision  rebac.Revision  `json:"revision,omitempty"`
}

// ReadModelRequest identifies one authorization model version. An empty
// Version selects the latest stored version.
type ReadModelRequest struct {
	TenantID string         `json:"tenant_id"`
	ModelID  string         `json:"model_id"`
	Version  rebac.Revision `json:"version,omitempty"`
}

// WriteModelRequest creates an immutable model version using optimistic CAS.
type WriteModelRequest struct {
	TenantID string              `json:"tenant_id"`
	Document rebac.ModelDocument `json:"document"`
	Expected rebac.Revision      `json:"expected,omitempty"`
}

// ActivateModelRequest atomically moves the active model pointer.
type ActivateModelRequest struct {
	TenantID       string         `json:"tenant_id"`
	ModelID        string         `json:"model_id"`
	ExpectedActive rebac.Revision `json:"expected_active,omitempty"`
	Version        rebac.Revision `json:"version"`
}

// ActivateModelResponse contains the selected document and its global
// authorization revision.
type ActivateModelResponse struct {
	Document rebac.ModelDocument `json:"document"`
	Revision rebac.Revision      `json:"revision"`
}

var errModelStorageNotConfigured = errors.New("rebac/server: model storage is not configured")

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) check(w http.ResponseWriter, r *http.Request) {
	var request CheckRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	response, err := s.evaluateCheck(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) batchCheck(w http.ResponseWriter, r *http.Request) {
	var request BatchCheckRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if len(request.Checks) == 0 || len(request.Checks) > s.maxBatchSize {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid batch size"})
		return
	}
	response := BatchCheckResponse{Checks: make([]CheckResponse, 0, len(request.Checks))}
	for _, check := range request.Checks {
		result, err := s.evaluateCheck(r.Context(), check)
		if err != nil {
			writeError(w, err)
			return
		}
		response.Checks = append(response.Checks, result)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) evaluateCheck(ctx context.Context, request CheckRequest) (CheckResponse, error) {
	allowed, revision, err := s.engine.CheckWithRevisionAt(ctx, request.Revision, request.CaveatContext, request.AsOfUnixNano, request.TenantID, request.User, request.Relation, request.Namespace, request.ObjectID)
	return CheckResponse{Allowed: allowed, Revision: revision}, err
}

func (s *Server) checkConsistent(w http.ResponseWriter, r *http.Request) {
	var request ConsistentCheckRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	minimum, err := s.decodeToken(request.AtLeastAsFresh, request.OpaqueToken)
	if err != nil {
		writeError(w, err)
		return
	}
	allowed, token, err := s.engine.CheckWithConsistencyAt(r.Context(), minimum, request.CaveatContext, request.AsOfUnixNano, request.TenantID, request.User, request.Relation, request.Namespace, request.ObjectID)
	if err != nil {
		writeError(w, err)
		return
	}
	s.writeConsistentResponse(w, allowed, token)
}

func (s *Server) contentChangeCheck(w http.ResponseWriter, r *http.Request) {
	var request contentChangeCheckRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	minimum, err := s.decodeToken(request.AtLeastAsFresh, request.OpaqueToken)
	if err != nil {
		writeError(w, err)
		return
	}
	request.AtLeastAsFresh = minimum
	allowed, token, err := s.engine.ContentChangeCheck(r.Context(), request.ContentChangeCheckRequest)
	if err != nil {
		writeError(w, err)
		return
	}
	s.writeConsistentResponse(w, allowed, token)
}

func (s *Server) decodeToken(token rebac.ConsistencyToken, encoded string) (rebac.ConsistencyToken, error) {
	if encoded == "" {
		return token, nil
	}
	if s.codec == nil {
		return rebac.ConsistencyToken{}, rebac.ErrInvalidRevision
	}
	return s.codec.Decode(encoded)
}

func (s *Server) writeConsistentResponse(w http.ResponseWriter, allowed bool, token rebac.ConsistencyToken) {
	response := ConsistentCheckResponse{Allowed: allowed, Token: token}
	if s.codec != nil {
		encoded, err := s.codec.Encode(token)
		if err != nil {
			writeError(w, err)
			return
		}
		response.OpaqueToken = encoded
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) readTuples(w http.ResponseWriter, r *http.Request) {
	var request rebac.ReadTuplesRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	page, err := s.engine.ReadTuples(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) lookupResources(w http.ResponseWriter, r *http.Request) {
	var request rebac.LookupResourcesRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	page, err := s.engine.LookupResources(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) lookupSubjects(w http.ResponseWriter, r *http.Request) {
	var request rebac.LookupSubjectsRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	page, err := s.engine.LookupSubjects(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) expand(w http.ResponseWriter, r *http.Request) {
	var request ExpandRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	expansion, revision, err := s.engine.Expand(r.Context(), request.Revision, request.TenantID, request.Relation, request.Namespace, request.ObjectID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ExpandResponse{Expansion: expansion, Revision: revision})
}

func (s *Server) modelStorage() (rebac.ModelStorage, error) {
	if s.models == nil {
		return nil, errModelStorageNotConfigured
	}
	return s.models, nil
}

func (s *Server) readModel(w http.ResponseWriter, r *http.Request) {
	var request ReadModelRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	models, err := s.modelStorage()
	if err == nil {
		var document rebac.ModelDocument
		document, err = models.ReadAuthorizationModel(r.Context(), request.TenantID, request.ModelID, request.Version)
		if err == nil {
			writeJSON(w, http.StatusOK, document)
			return
		}
	}
	writeError(w, err)
}

func (s *Server) listModelVersions(w http.ResponseWriter, r *http.Request) {
	var request ReadModelRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	models, err := s.modelStorage()
	if err == nil {
		lister, ok := models.(rebac.ModelVersionLister)
		if !ok {
			err = errModelStorageNotConfigured
		} else {
			var documents []rebac.ModelDocument
			documents, err = lister.ListAuthorizationModelVersions(r.Context(), request.TenantID, request.ModelID)
			if err == nil {
				writeJSON(w, http.StatusOK, documents)
				return
			}
		}
	}
	writeError(w, err)
}

func (s *Server) readActiveModel(w http.ResponseWriter, r *http.Request) {
	var request ReadModelRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	models, err := s.modelStorage()
	if err == nil {
		active, ok := models.(rebac.ActiveModelStorage)
		if !ok {
			err = errModelStorageNotConfigured
		} else {
			var document rebac.ModelDocument
			document, err = active.ReadActiveAuthorizationModel(r.Context(), request.TenantID, request.ModelID)
			if err == nil {
				writeJSON(w, http.StatusOK, document)
				return
			}
		}
	}
	writeError(w, err)
}

func (s *Server) writeTuple(w http.ResponseWriter, r *http.Request) {
	var request tupleRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	revision, err := s.engine.WriteTupleWithRevision(r.Context(), request.Tuple)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisionResponse{Revision: revision})
}

func (s *Server) deleteTuple(w http.ResponseWriter, r *http.Request) {
	var request tupleRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	revision, err := s.engine.DeleteTupleWithRevision(r.Context(), request.Tuple)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisionResponse{Revision: revision})
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	var request objectRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	revision, err := s.engine.DeleteObject(r.Context(), request.TenantID, request.Namespace, request.ObjectID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisionResponse{Revision: revision})
}

func (s *Server) mutate(w http.ResponseWriter, r *http.Request) {
	var request mutateRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	revision, err := s.engine.Mutate(r.Context(), request.Changes, request.Preconditions)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisionResponse{Revision: revision})
}

func (s *Server) writeModel(w http.ResponseWriter, r *http.Request) {
	var request WriteModelRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	models, err := s.modelStorage()
	if err == nil {
		var document rebac.ModelDocument
		document, err = models.WriteAuthorizationModel(r.Context(), request.TenantID, request.Document, request.Expected)
		if err == nil {
			writeJSON(w, http.StatusOK, document)
			return
		}
	}
	writeError(w, err)
}

func (s *Server) activateModel(w http.ResponseWriter, r *http.Request) {
	var request ActivateModelRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	models, err := s.modelStorage()
	if err == nil {
		active, ok := models.(rebac.ActiveModelStorage)
		if !ok {
			err = errModelStorageNotConfigured
		} else {
			var document rebac.ModelDocument
			var revision rebac.Revision
			document, revision, err = active.ActivateAuthorizationModel(r.Context(), request.TenantID, request.ModelID, request.ExpectedActive, request.Version)
			if err == nil {
				writeJSON(w, http.StatusOK, ActivateModelResponse{Document: document, Revision: revision})
				return
			}
		}
	}
	writeError(w, err)
}

func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	limit := s.maxBodyBytes
	if limit <= 0 {
		limit = defaultMaxBodyBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return false
	}
	return true
}

func (s *Server) withTimeout(handler http.Handler) http.Handler {
	if s.timeout <= 0 {
		return handler
	}
	return http.TimeoutHandler(handler, s.timeout, `{"error":"request timed out"}`)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	message := err.Error()
	switch {
	case errors.Is(err, rebac.ErrPreconditionFailed), errors.Is(err, rebac.ErrInvalidRevision):
		status = http.StatusConflict
	case errors.Is(err, rebac.ErrConsistencyUnsupported):
		status = http.StatusNotImplemented
	case errors.Is(err, errModelStorageNotConfigured):
		status = http.StatusNotImplemented
	case errors.Is(err, rebac.ErrStorage):
		status = http.StatusServiceUnavailable
		message = "storage unavailable"
	case errors.Is(err, rebac.ErrModelTenant):
		status = http.StatusForbidden
	}
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
