// Package kv provides a small transactional key-value database for go-rebac.
// It is intentionally dependency-free and owns no network listener.
package kv

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var ErrClosed = errors.New("rebac/kv: transaction is closed")

// Database stores byte values behind snapshot and transaction handles.
type Database struct {
	mu       sync.RWMutex
	values   map[string][]byte
	revision uint64
	path     string
}

// New creates an in-process KV database. Persistence belongs to a later
// durable backend; this constructor performs no I/O.
func New() *Database { return &Database{values: make(map[string][]byte)} }

// Open loads or creates a durable KV database at path. The caller controls its
// lifecycle; no background process or network listener is started.
func Open(path string) (*Database, error) {
	if path == "" {
		return nil, errors.New("rebac/kv: database path is required")
	}
	database := New()
	database.path = path
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return database, nil
	}
	if err != nil {
		return nil, err
	}
	var state struct {
		Values   map[string][]byte `json:"values"`
		Revision uint64            `json:"revision"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	if state.Values != nil {
		database.values = state.Values
	}
	database.revision = state.Revision
	return database, nil
}

// Snapshot is a stable read view.
type Snapshot struct {
	values   map[string][]byte
	revision uint64
	closed   bool
}

// NewSnapshot creates a point-in-time read view.
func (d *Database) NewSnapshot(ctx context.Context) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.RLock()
	values, revision := clone(d.values), d.revision
	d.mu.RUnlock()
	return &Snapshot{values: values, revision: revision}, nil
}

// Revision identifies the snapshot's database view.
func (s *Snapshot) Revision() uint64 { return s.revision }

// Get returns a copy of the stored value and whether it exists.
func (s *Snapshot) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.closed {
		return nil, false, ErrClosed
	}
	v, ok := s.values[key]
	return append([]byte(nil), v...), ok, nil
}

// Ascend returns a sorted snapshot of keys matching prefix.
func (s *Snapshot) Ascend(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	keys := make([]string, 0)
	for key := range s.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// Close releases a snapshot.
func (s *Snapshot) Close() error { s.closed = true; return nil }

// Transaction stages atomic changes over a snapshot.
type Transaction struct {
	*Snapshot
	db     *Database
	writes map[string]*[]byte
}

// NewTransaction starts a write transaction.
func (d *Database) NewTransaction(ctx context.Context) (*Transaction, error) {
	snapshot, err := d.NewSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &Transaction{Snapshot: snapshot, db: d, writes: make(map[string]*[]byte)}, nil
}

func (t *Transaction) Set(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.closed {
		return ErrClosed
	}
	v := append([]byte(nil), value...)
	t.writes[key] = &v
	return nil
}

func (t *Transaction) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.closed {
		return ErrClosed
	}
	t.writes[key] = nil
	return nil
}

// Commit applies all staged writes atomically. It rejects a stale snapshot.
func (t *Transaction) Commit(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if t.closed {
		return 0, ErrClosed
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.db.revision != t.revision {
		return 0, errors.New("rebac/kv: transaction conflict")
	}
	next := clone(t.db.values)
	for key, value := range t.writes {
		if value == nil {
			delete(next, key)
		} else {
			next[key] = append([]byte(nil), (*value)...)
		}
	}
	if err := t.db.persistLocked(next, t.db.revision+1); err != nil {
		return 0, err
	}
	t.db.values = next
	t.db.revision++
	t.closed = true
	return t.db.revision, nil
}

func (d *Database) persistLocked(values map[string][]byte, revision uint64) error {
	if d.path == "" {
		return nil
	}
	state := struct {
		Values   map[string][]byte `json:"values"`
		Revision uint64            `json:"revision"`
	}{Values: values, Revision: revision}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(d.path), 0o755); err != nil {
		return err
	}
	temporary := d.path + ".tmp"
	if err := os.WriteFile(temporary, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, d.path)
}

func clone(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}
