package rebac

import (
	"context"
	"errors"
	"fmt"
)

var ErrModelNotFound = errors.New("rebac: authorization model not found")

// ModelDocument is the serializable, versioned form of an authorization
// model. Caveat names are stored; their Go evaluators remain application code
// and are bound with Compile after loading.
type ModelDocument struct {
	ID      string             `json:"id"`
	Version Revision           `json:"version"`
	Model   AuthorizationModel `json:"model"`
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
	return d.Model.Validate()
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
