package rebac

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type compiledModelKey struct {
	tenantID, modelID, version, checksum string
}

type compiledModelCache struct {
	mu     sync.Mutex
	max    int
	models map[compiledModelKey]*CompiledModel
	order  []compiledModelKey
}

const defaultCompiledModelCacheSize = 128

func newCompiledModelCache(max int) *compiledModelCache {
	return &compiledModelCache{max: max, models: make(map[compiledModelKey]*CompiledModel)}
}

func (c *compiledModelCache) get(key compiledModelKey) *CompiledModel {
	c.mu.Lock()
	defer c.mu.Unlock()
	model := c.models[key]
	if model != nil {
		c.touch(key)
	}
	return model
}

func (c *compiledModelCache) put(key compiledModelKey, model *CompiledModel) *CompiledModel {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached := c.models[key]; cached != nil {
		c.touch(key)
		return cached
	}
	if c.max > 0 && len(c.order) == c.max {
		delete(c.models, c.order[0])
		c.order = c.order[1:]
	}
	c.models[key] = model
	c.order = append(c.order, key)
	return model
}

func (c *compiledModelCache) touch(key compiledModelKey) {
	for index, existing := range c.order {
		if existing == key {
			copy(c.order[index:], c.order[index+1:])
			c.order[len(c.order)-1] = key
			return
		}
	}
}

func (e *Engine) compiledModelForDocument(tenantID string, document ModelDocument) (*CompiledModel, error) {
	checksum, err := document.ComputeChecksum()
	if err != nil {
		return nil, err
	}
	key := compiledModelKey{tenantID: tenantID, modelID: document.ID, version: string(document.Version), checksum: checksum}
	if e.compiledModels != nil {
		if cached := e.compiledModels.get(key); cached != nil {
			return cached, nil
		}
	}
	model, err := document.Compile(e.caveats)
	if err != nil {
		return nil, err
	}
	compiled, err := CompileModel(model)
	if err != nil {
		return nil, err
	}
	if e.compiledModels == nil {
		return compiled, nil
	}
	return e.compiledModels.put(key, compiled), nil
}

var (
	ErrModelNotFound          = errors.New("rebac: authorization model not found")
	ErrInvalidModelTransition = errors.New("rebac: incompatible authorization model transition")
	ErrModelNotPublished      = errors.New("rebac: authorization model is not published")
)

// ModelState is the immutable lifecycle state recorded with one model version.
type ModelState string

const (
	ModelDraft      ModelState = "draft"
	ModelPublished  ModelState = "published"
	ModelDeprecated ModelState = "deprecated"
)

// ModelDocument is the serializable, versioned form of an authorization
// model. Caveat names are stored; their Go evaluators remain application code
// and are bound with Compile after loading.
type ModelDocument struct {
	ID                string             `json:"id"`
	Version           Revision           `json:"version"`
	ParentVersion     Revision           `json:"parent_version,omitempty"`
	CreatedAtUnixNano int64              `json:"created_at_unix_nano,omitempty"`
	Checksum          string             `json:"checksum,omitempty"`
	State             ModelState         `json:"state,omitempty"`
	Model             AuthorizationModel `json:"model"`
}

// ModelSelection identifies the tenant-scoped model and optional historical
// version to load. An empty Version selects the latest stored model.
type ModelSelection struct {
	TenantID string
	ModelID  string
	Version  Revision
}

// Validate verifies a persisted model document without requiring caveat code.
func (d ModelDocument) Validate() error {
	if !validName(d.ID) {
		return fmt.Errorf("%w: invalid model ID %q", ErrInvalidModel, d.ID)
	}
	if d.State != "" && d.State != ModelDraft && d.State != ModelPublished && d.State != ModelDeprecated {
		return fmt.Errorf("%w: invalid model state %q", ErrInvalidModel, d.State)
	}
	if err := d.Model.Validate(); err != nil {
		return err
	}
	if d.Checksum != "" {
		checksum, err := d.ComputeChecksum()
		if err != nil {
			return err
		}
		if d.Checksum != checksum {
			return fmt.Errorf("%w: model checksum mismatch", ErrInvalidModel)
		}
	}
	return nil
}

// ComputeChecksum returns the SHA-256 checksum of this document's stable model
// content and immutable lifecycle state. Version and timestamps are excluded.
func (d ModelDocument) ComputeChecksum() (string, error) {
	payload := struct {
		ID    string             `json:"id"`
		State ModelState         `json:"state,omitempty"`
		Model AuthorizationModel `json:"model"`
	}{ID: d.ID, State: d.State, Model: d.Model}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateModelTransition rejects changes that can invalidate live tuples or
// alter an existing relation's meaning. Additive namespaces, relations,
// subjects, and caveats are allowed.
func ValidateModelTransition(old, next AuthorizationModel) error {
	if err := old.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	for namespace, oldDefinition := range old.Namespaces {
		nextDefinition, ok := next.Namespaces[namespace]
		if !ok {
			return fmt.Errorf("%w: removed namespace %q", ErrInvalidModelTransition, namespace)
		}
		for relation, oldRelation := range oldDefinition.Relations {
			nextRelation, ok := nextDefinition.Relations[relation]
			if !ok {
				return fmt.Errorf("%w: removed relation %s#%s", ErrInvalidModelTransition, namespace, relation)
			}
			if oldRelation.AllowWildcard && !nextRelation.AllowWildcard {
				return fmt.Errorf("%w: disabled wildcard on %s#%s", ErrInvalidModelTransition, namespace, relation)
			}
			for _, subject := range oldRelation.AllowedSubjects {
				if !containsSubject(nextRelation.AllowedSubjects, subject) {
					return fmt.Errorf("%w: removed subject %s#%s from %s#%s", ErrInvalidModelTransition, subject.Namespace, subject.Relation, namespace, relation)
				}
			}
			if !reflect.DeepEqual(oldRelation.Rewrite, nextRelation.Rewrite) {
				return fmt.Errorf("%w: changed rewrite on %s#%s", ErrInvalidModelTransition, namespace, relation)
			}
		}
	}
	for name := range old.Caveats {
		if _, ok := next.Caveats[name]; !ok {
			return fmt.Errorf("%w: removed caveat %q", ErrInvalidModelTransition, name)
		}
	}
	return nil
}

func containsSubject(subjects []SubjectReference, target SubjectReference) bool {
	for _, subject := range subjects {
		if subject == target {
			return true
		}
	}
	return false
}

// Compile binds application-owned caveat evaluators to a loaded document.
func (d ModelDocument) Compile(caveats map[string]CaveatDefinition) (AuthorizationModel, error) {
	if err := d.Validate(); err != nil {
		return AuthorizationModel{}, err
	}
	bound := make(map[string]CaveatDefinition, len(d.Model.Caveats))
	for name := range d.Model.Caveats {
		definition, ok := caveats[name]
		if !ok || definition.Evaluate == nil {
			return AuthorizationModel{}, fmt.Errorf("%w: caveat %q is not bound", ErrInvalidModel, name)
		}
		bound[name] = definition
	}
	d.Model.Caveats = bound
	return d.Model, nil
}

// ModelStorage is optional durable storage for versioned authorization-model
// documents. expected is the version the caller read; an empty expected means
// create only. Implementations must reject a stale expected version with
// ErrPreconditionFailed.
type ModelStorage interface {
	ReadAuthorizationModel(ctx context.Context, tenantID, modelID string, version Revision) (ModelDocument, error)
	WriteAuthorizationModel(ctx context.Context, tenantID string, document ModelDocument, expected Revision) (ModelDocument, error)
}

// ModelVersionLister lists immutable versions in creation order. It is an
// optional read capability; ModelStorage remains the portable baseline.
type ModelVersionLister interface {
	ListAuthorizationModelVersions(ctx context.Context, tenantID, modelID string) ([]ModelDocument, error)
}

// ActiveModelStorage atomically selects one immutable model version for a
// tenant/model pair. expectedActive is the version currently active; empty
// means that no version may be active yet.
type ActiveModelStorage interface {
	ModelStorage
	ReadActiveAuthorizationModel(ctx context.Context, tenantID, modelID string) (ModelDocument, error)
	ActivateAuthorizationModel(ctx context.Context, tenantID, modelID string, expectedActive, version Revision) (ModelDocument, Revision, error)
}

// ModelSnapshotStorage resolves the authorization model effective at a tuple
// snapshot. Its revision sequence must be the same externally consistent
// sequence used by ConsistentStorage; it must never return a model written
// after revision.
type ModelSnapshotStorage interface {
	ModelStorage
	ReadAuthorizationModelAtRevision(ctx context.Context, tenantID, modelID string, revision Revision) (ModelDocument, error)
}

// RevisionedModelStorage commits model updates into the same revision sequence
// used for tuple snapshots. expected is the model-document version, while the
// returned revision is the global authorization revision.
type RevisionedModelStorage interface {
	ModelSnapshotStorage
	WriteAuthorizationModelWithRevision(ctx context.Context, tenantID string, document ModelDocument, expected Revision) (stored ModelDocument, revision Revision, err error)
}

// NewEngineFromModelStorage loads a selected model, binds application caveat
// code, and returns a normal Engine pinned to that model version. Applications
// choose when to reload by constructing a new Engine with a newer selection.
func NewEngineFromModelStorage(ctx context.Context, store StorageEngine, models ModelStorage, selection ModelSelection, caveats map[string]CaveatDefinition) (*Engine, ModelDocument, error) {
	if models == nil {
		return nil, ModelDocument{}, ErrModelNotFound
	}
	if !validObjectID(selection.TenantID) {
		return nil, ModelDocument{}, ErrTenantRequired
	}
	if !validName(selection.ModelID) {
		return nil, ModelDocument{}, fmt.Errorf("%w: invalid model ID %q", ErrInvalidModel, selection.ModelID)
	}
	document, err := models.ReadAuthorizationModel(ctx, selection.TenantID, selection.ModelID, selection.Version)
	if err != nil {
		return nil, ModelDocument{}, err
	}
	if document.ID != selection.ModelID {
		return nil, ModelDocument{}, ErrModelNotFound
	}
	model, err := document.Compile(caveats)
	if err != nil {
		return nil, ModelDocument{}, err
	}
	engine, err := NewEngine(store, model)
	if err != nil {
		return nil, ModelDocument{}, err
	}
	engine.modelTenant = selection.TenantID
	engine.modelID = selection.ModelID
	engine.modelVersion = document.Version
	if checksum, err := document.ComputeChecksum(); err == nil && engine.compiledModels != nil {
		key := compiledModelKey{tenantID: selection.TenantID, modelID: document.ID, version: string(document.Version), checksum: checksum}
		engine.compiledModel = engine.compiledModels.put(key, engine.compiledModel)
	}
	return engine, document, nil
}

// NewProductionEngineFromModelStorage is NewEngineFromModelStorage with the
// mandatory production storage contract.
func NewProductionEngineFromModelStorage(ctx context.Context, store ProductionStorage, models ModelStorage, selection ModelSelection, caveats map[string]CaveatDefinition) (*Engine, ModelDocument, error) {
	if store == nil {
		return nil, ModelDocument{}, ErrProductionStorage
	}
	return NewEngineFromModelStorage(ctx, store, models, selection, caveats)
}

// NewConsistentEngineFromModelStorage constructs an Engine whose normal Check
// path selects a tuple snapshot at least as fresh as the caller's token and
// loads the model valid at that same snapshot. It requires revisioned model
// writes so model changes participate in the shared revision sequence.
func NewConsistentEngineFromModelStorage(ctx context.Context, store ConsistentStorage, models RevisionedModelStorage, selection ModelSelection, caveats map[string]CaveatDefinition) (*Engine, ModelDocument, error) {
	if store == nil || models == nil {
		return nil, ModelDocument{}, ErrConsistencyUnsupported
	}
	engine, document, err := NewEngineFromModelStorage(ctx, store, models, selection, caveats)
	if err != nil {
		return nil, ModelDocument{}, err
	}
	engine.modelID = selection.ModelID
	engine.modelSource = models
	engine.caveats = caveats
	return engine, document, nil
}
