// Package server exposes a rebac.Engine through optional net/http handlers.
// It owns neither authentication nor datastore connections.
package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/surya-mp/go-rebac"
)

// Server adapts one configured Engine to data-plane and administration handlers.
type Server struct {
	engine *rebac.Engine
	codec  rebac.TokenCodec
}

// New returns HTTP handlers for an already constructed engine.
func New(engine *rebac.Engine) (*Server, error) {
	if engine == nil {
		return nil, errors.New("rebac/server: engine is nil")
	}
	return &Server{engine: engine}, nil
}

// NewWithTokenCodec configures opaque, authenticated consistency tokens for
// the Zanzibar-style endpoints. The codec and its key remain application-owned.
func NewWithTokenCodec(engine *rebac.Engine, codec rebac.TokenCodec) (*Server, error) {
	if engine == nil || codec == nil {
		return nil, errors.New("rebac/server: engine and token codec are required")
	}
	return &Server{engine: engine, codec: codec}, nil
}

// Handler exposes read-only authorization endpoints. Mount it behind the
// application's authentication middleware; this package does not authenticate
// callers or interpret their identity.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/check", s.check)
	mux.HandleFunc("POST /v1/check/consistent", s.checkConsistent)
	mux.HandleFunc("POST /v1/content-change-check", s.contentChangeCheck)
	mux.HandleFunc("POST /v1/tuples/read", s.readTuples)
	mux.HandleFunc("POST /v1/lookup/resources", s.lookupResources)
	mux.HandleFunc("POST /v1/lookup/subjects", s.lookupSubjects)
	mux.HandleFunc("POST /v1/expand", s.expand)
	return mux
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
	return mux
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

type expandRequest struct {
	TenantID  string         `json:"tenant_id"`
	Revision  rebac.Revision `json:"revision,omitempty"`
	Relation  string         `json:"relation"`
	Namespace string         `json:"namespace"`
	ObjectID  string         `json:"object_id"`
}

type expandResponse struct {
	Expansion rebac.Expansion `json:"expansion"`
	Revision  rebac.Revision  `json:"revision,omitempty"`
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) check(w http.ResponseWriter, r *http.Request) {
	var request CheckRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	allowed, revision, err := s.engine.CheckWithRevisionAt(r.Context(), request.Revision, request.CaveatContext, request.AsOfUnixNano, request.TenantID, request.User, request.Relation, request.Namespace, request.ObjectID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, CheckResponse{Allowed: allowed, Revision: revision})
}

func (s *Server) checkConsistent(w http.ResponseWriter, r *http.Request) {
	var request ConsistentCheckRequest
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
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
	var request expandRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	expansion, revision, err := s.engine.Expand(r.Context(), request.Revision, request.TenantID, request.Relation, request.Namespace, request.ObjectID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, expandResponse{Expansion: expansion, Revision: revision})
}

func (s *Server) writeTuple(w http.ResponseWriter, r *http.Request) {
	var request tupleRequest
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
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
	if !decodeJSON(w, r, &request) {
		return
	}
	revision, err := s.engine.Mutate(r.Context(), request.Changes, request.Preconditions)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisionResponse{Revision: revision})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
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

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	message := err.Error()
	switch {
	case errors.Is(err, rebac.ErrPreconditionFailed), errors.Is(err, rebac.ErrInvalidRevision):
		status = http.StatusConflict
	case errors.Is(err, rebac.ErrConsistencyUnsupported):
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
