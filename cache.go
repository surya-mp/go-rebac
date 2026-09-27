package rebac

import (
	"context"
	"encoding/json"
	"sync"
)

// DecisionCache stores revision-pinned decisions. Distributed cache backends
// implement this interface; the library never assumes a particular service.
type DecisionCache interface {
	Get(ctx context.Context, key string) (allowed bool, found bool, err error)
	Set(ctx context.Context, key string, allowed bool) error
	InvalidateTenant(ctx context.Context, tenantID string) error
}

// MemoryDecisionCache is a small single-process implementation suitable for
// tests and development. Production deployments can provide a shared backend.
type MemoryDecisionCache struct {
	mu      sync.RWMutex
	entries map[string]bool
}

func NewMemoryDecisionCache() *MemoryDecisionCache {
	return &MemoryDecisionCache{entries: make(map[string]bool)}
}

func (c *MemoryDecisionCache) Get(_ context.Context, key string) (bool, bool, error) {
	c.mu.RLock()
	allowed, found := c.entries[key]
	c.mu.RUnlock()
	return allowed, found, nil
}

func (c *MemoryDecisionCache) Set(_ context.Context, key string, allowed bool) error {
	c.mu.Lock()
	c.entries[key] = allowed
	c.mu.Unlock()
	return nil
}

func (c *MemoryDecisionCache) InvalidateTenant(_ context.Context, tenantID string) error {
	c.mu.Lock()
	for key := range c.entries {
		if len(key) > len(tenantID) && key[:len(tenantID)+1] == tenantID+"\x00" {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
	return nil
}

// WatchCacheInvalidation invalidates a tenant after every committed change.
// It returns the underlying watch channels so the application can persist the
// latest revision and restart the watcher after failures.
func (e *Engine) WatchCacheInvalidation(ctx context.Context, tenantID string, after Revision) (<-chan Revision, <-chan error) {
	revisions := make(chan Revision)
	errorsCh := make(chan error, 1)
	if e == nil || e.cache == nil {
		errorsCh <- ErrInvalidRequest
		close(revisions)
		close(errorsCh)
		return revisions, errorsCh
	}
	events, watchErrors := e.Watch(ctx, tenantID, after)
	go func() {
		defer close(revisions)
		defer close(errorsCh)
		for events != nil || watchErrors != nil {
			select {
			case event, ok := <-events:
				if !ok {
					events = nil
					continue
				}
				if err := e.cache.InvalidateTenant(ctx, event.TenantID); err != nil {
					errorsCh <- err
					return
				}
				select {
				case revisions <- event.Revision:
				case <-ctx.Done():
					return
				}
			case err, ok := <-watchErrors:
				if !ok {
					watchErrors = nil
					continue
				}
				if err != nil {
					errorsCh <- err
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return revisions, errorsCh
}

func decisionCacheKey(modelID string, modelVersion Revision, modelHash string, revision Revision, tenantID, user, relation, namespace, objectID string, caveatContext CaveatContext) (string, bool) {
	if revision == "" {
		return "", false
	}
	context, err := json.Marshal(caveatContext)
	if err != nil {
		return "", false
	}
	return tenantID + "\x00" + modelID + "\x00" + string(modelVersion) + "\x00" + modelHash + "\x00" + string(revision) + "\x00" + user + "\x00" + relation + "\x00" + namespace + "\x00" + objectID + "\x00" + string(context), true
}
