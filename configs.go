package rebac

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrNamespaceConfigNotFound reports a namespace absent from a model version.
var ErrNamespaceConfigNotFound = errors.New("rebac: namespace configuration not found")

// NamespaceConfig is one namespace projected from an immutable model document.
// Its version fields always refer to the containing model document.
type NamespaceConfig struct {
	Namespace         string              `json:"namespace"`
	Definition        NamespaceDefinition `json:"definition"`
	ModelID           string              `json:"model_id"`
	Version           Revision            `json:"version"`
	ParentVersion     Revision            `json:"parent_version,omitempty"`
	CreatedAtUnixNano int64               `json:"created_at_unix_nano,omitempty"`
	State             ModelState          `json:"state,omitempty"`
}

// NamespaceConfigStore provides config-first namespace operations while
// retaining ModelDocument as the single atomic persistence format.
type NamespaceConfigStore struct {
	Models   ModelStorage
	TenantID string
	ModelID  string
}

// NewNamespaceConfigStore returns a namespace-scoped view of one tenant model.
func NewNamespaceConfigStore(models ModelStorage, tenantID, modelID string) NamespaceConfigStore {
	return NamespaceConfigStore{Models: models, TenantID: tenantID, ModelID: modelID}
}

func (s NamespaceConfigStore) validate() error {
	if s.Models == nil || !validObjectID(s.TenantID) || !validName(s.ModelID) {
		return ErrInvalidModel
	}
	return nil
}

func namespaceConfig(document ModelDocument, namespace string) (NamespaceConfig, error) {
	definition, ok := document.Model.Namespaces[namespace]
	if !ok {
		return NamespaceConfig{}, fmt.Errorf("%w: %s", ErrNamespaceConfigNotFound, namespace)
	}
	return NamespaceConfig{Namespace: namespace, Definition: definition, ModelID: document.ID, Version: document.Version, ParentVersion: document.ParentVersion, CreatedAtUnixNano: document.CreatedAtUnixNano, State: document.State}, nil
}

// ReadConfig reads one namespace configuration at version; an empty version
// selects the latest stored model document.
func (s NamespaceConfigStore) ReadConfig(ctx context.Context, namespace string, version Revision) (NamespaceConfig, error) {
	if err := s.validate(); err != nil {
		return NamespaceConfig{}, err
	}
	if !validName(namespace) {
		return NamespaceConfig{}, ErrInvalidModel
	}
	document, err := s.Models.ReadAuthorizationModel(ctx, s.TenantID, s.ModelID, version)
	if err != nil {
		return NamespaceConfig{}, err
	}
	return namespaceConfig(document, namespace)
}

// ReadActiveConfig reads one namespace from the currently active model.
func (s NamespaceConfigStore) ReadActiveConfig(ctx context.Context, namespace string) (NamespaceConfig, error) {
	if err := s.validate(); err != nil {
		return NamespaceConfig{}, err
	}
	if !validName(namespace) {
		return NamespaceConfig{}, ErrInvalidModel
	}
	active, ok := s.Models.(ActiveModelStorage)
	if !ok {
		return NamespaceConfig{}, ErrModelNotFound
	}
	document, err := active.ReadActiveAuthorizationModel(ctx, s.TenantID, s.ModelID)
	if err != nil {
		return NamespaceConfig{}, err
	}
	return namespaceConfig(document, namespace)
}

// ListConfigs lists namespace configurations in deterministic name order.
func (s NamespaceConfigStore) ListConfigs(ctx context.Context, version Revision) ([]NamespaceConfig, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	document, err := s.Models.ReadAuthorizationModel(ctx, s.TenantID, s.ModelID, version)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(document.Model.Namespaces))
	for namespace := range document.Model.Namespaces {
		names = append(names, namespace)
	}
	sort.Strings(names)
	configs := make([]NamespaceConfig, 0, len(names))
	for _, namespace := range names {
		config, err := namespaceConfig(document, namespace)
		if err != nil {
			return nil, err
		}
		configs = append(configs, config)
	}
	return configs, nil
}

// ListConfigVersions lists every model version containing namespace, in
// creation order. It requires the optional ModelVersionLister capability.
func (s NamespaceConfigStore) ListConfigVersions(ctx context.Context, namespace string) ([]NamespaceConfig, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if !validName(namespace) {
		return nil, ErrInvalidModel
	}
	lister, ok := s.Models.(ModelVersionLister)
	if !ok {
		return nil, ErrModelNotFound
	}
	documents, err := lister.ListAuthorizationModelVersions(ctx, s.TenantID, s.ModelID)
	if err != nil {
		return nil, err
	}
	configs := make([]NamespaceConfig, 0, len(documents))
	for _, document := range documents {
		config, err := namespaceConfig(document, namespace)
		if errors.Is(err, ErrNamespaceConfigNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		configs = append(configs, config)
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNamespaceConfigNotFound, namespace)
	}
	return configs, nil
}

// WriteConfig creates a new immutable containing model version with config
// updated. expected is the containing model version last read; empty creates
// the first model only. The new version is not activated automatically.
func (s NamespaceConfigStore) WriteConfig(ctx context.Context, config NamespaceConfig, expected Revision) (NamespaceConfig, error) {
	if err := s.validate(); err != nil {
		return NamespaceConfig{}, err
	}
	if !validName(config.Namespace) {
		return NamespaceConfig{}, ErrInvalidModel
	}
	document, err := s.Models.ReadAuthorizationModel(ctx, s.TenantID, s.ModelID, "")
	if errors.Is(err, ErrModelNotFound) {
		if expected != "" {
			return NamespaceConfig{}, ErrPreconditionFailed
		}
		document = ModelDocument{ID: s.ModelID, Model: AuthorizationModel{Namespaces: make(map[string]NamespaceDefinition)}}
	} else if err != nil {
		return NamespaceConfig{}, err
	}
	namespaces := make(map[string]NamespaceDefinition, len(document.Model.Namespaces)+1)
	for name, definition := range document.Model.Namespaces {
		namespaces[name] = definition
	}
	document.Model.Namespaces = namespaces
	document.Model.Namespaces[config.Namespace] = config.Definition
	// Versions, parent links, timestamps, and checksums are assigned by the
	// single model writer. Retaining the previous checksum would reject this
	// intentionally changed document before it can be versioned.
	document.Version = ""
	document.ParentVersion = ""
	document.CreatedAtUnixNano = 0
	document.Checksum = ""
	stored, err := s.Models.WriteAuthorizationModel(ctx, s.TenantID, document, expected)
	if err != nil {
		return NamespaceConfig{}, err
	}
	return namespaceConfig(stored, config.Namespace)
}
