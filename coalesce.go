package rebac

import (
	"context"
	"sync"
)

type checkFlight struct {
	done    chan struct{}
	allowed bool
	err     error
}

type checkCoalescer struct {
	mu      sync.Mutex
	flights map[string]*checkFlight
}

func (c *checkCoalescer) do(ctx context.Context, key string, fn func() (bool, error)) (bool, error) {
	c.mu.Lock()
	if flight := c.flights[key]; flight != nil {
		c.mu.Unlock()
		select {
		case <-flight.done:
			return flight.allowed, flight.err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	flight := &checkFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.mu.Unlock()

	flight.allowed, flight.err = fn()
	c.mu.Lock()
	delete(c.flights, key)
	close(flight.done)
	c.mu.Unlock()
	return flight.allowed, flight.err
}

// WithRequestCoalescing returns an Engine that coalesces identical concurrent
// Check calls in this process. Distributed dispatch remains a host-service
// concern, but this removes duplicate local graph traversals for hot objects.
func (e *Engine) WithRequestCoalescing() *Engine {
	if e == nil {
		return nil
	}
	configured := *e
	if configured.coalescer == nil {
		configured.coalescer = &checkCoalescer{flights: make(map[string]*checkFlight)}
	}
	return &configured
}

func checkFlightKey(tenantID, user, relation, namespace, objectID string) string {
	return tenantID + "\x00" + user + "\x00" + relation + "\x00" + namespace + "\x00" + objectID
}
