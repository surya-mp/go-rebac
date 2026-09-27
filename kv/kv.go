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

var (
	ErrClosed              = errors.New("rebac/kv: transaction is closed")
	ErrTransactionConflict = errors.New("rebac/kv: transaction conflict")
)

// Reader is one point-in-time KV read view. Host-backed implementations must
// return a stable view for its lifetime.
type Reader interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Ascend(ctx context.Context, prefix string) ([]string, error)
	Close() error
}

// WriteTransaction is one atomic read-modify-write KV operation.
type WriteTransaction interface {
	Reader
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Commit(ctx context.Context) (uint64, error)
	Rollback(ctx context.Context) error
}

// Backend is the host-supplied KV contract used by ReBACStore. It deliberately
// mirrors only the snapshot and transaction guarantees the store needs.
type Backend interface {
	Snapshot(ctx context.Context) (Reader, error)
	Transaction(ctx context.Context) (WriteTransaction, error)
}

// prefixedBackend isolates one ReBACStore's records inside an application
// supplied backend. Keys returned to the store never include prefix.
type prefixedBackend struct {
	Backend
	prefix string
}

func withPrefix(backend Backend, prefix string) Backend {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return backend
	}
	return prefixedBackend{Backend: backend, prefix: prefix + "/"}
}

func (b prefixedBackend) Snapshot(ctx context.Context) (Reader, error) {
	reader, err := b.Backend.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return prefixedReader{Reader: reader, prefix: b.prefix}, nil
}

func (b prefixedBackend) Transaction(ctx context.Context) (WriteTransaction, error) {
	tx, err := b.Backend.Transaction(ctx)
	if err != nil {
		return nil, err
	}
	return prefixedTransaction{WriteTransaction: tx, prefix: b.prefix}, nil
}

type prefixedReader struct {
	Reader
	prefix string
}

func (r prefixedReader) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return r.Reader.Get(ctx, r.prefix+key)
}

func (r prefixedReader) Ascend(ctx context.Context, prefix string) ([]string, error) {
	keys, err := r.Reader.Ascend(ctx, r.prefix+prefix)
	if err != nil {
		return nil, err
	}
	for index := range keys {
		keys[index] = strings.TrimPrefix(keys[index], r.prefix)
	}
	return keys, nil
}

type prefixedTransaction struct {
	WriteTransaction
	prefix string
}

func (t prefixedTransaction) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return t.WriteTransaction.Get(ctx, t.prefix+key)
}

func (t prefixedTransaction) Ascend(ctx context.Context, prefix string) ([]string, error) {
	keys, err := t.WriteTransaction.Ascend(ctx, t.prefix+prefix)
	if err != nil {
		return nil, err
	}
	for index := range keys {
		keys[index] = strings.TrimPrefix(keys[index], t.prefix)
	}
	return keys, nil
}

func (t prefixedTransaction) Set(ctx context.Context, key string, value []byte) error {
	return t.WriteTransaction.Set(ctx, t.prefix+key, value)
}

func (t prefixedTransaction) Delete(ctx context.Context, key string) error {
	return t.WriteTransaction.Delete(ctx, t.prefix+key)
}

// Database stores byte values behind snapshot and transaction handles.
type Database struct {
	mu       sync.RWMutex
	values   map[string][]byte
	revision uint64
	path     string
}

var _ Backend = (*Database)(nil)

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

// Snapshot satisfies Backend for the bundled in-memory/file implementation.
func (d *Database) Snapshot(ctx context.Context) (Reader, error) { return d.NewSnapshot(ctx) }

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

// Transaction satisfies Backend for the bundled in-memory/file implementation.
func (d *Database) Transaction(ctx context.Context) (WriteTransaction, error) {
	return d.NewTransaction(ctx)
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
		t.closed = true
		return 0, ErrTransactionConflict
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

// Rollback discards staged writes.
func (t *Transaction) Rollback(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.closed {
		return ErrClosed
	}
	t.closed = true
	return nil
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
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, d.path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(d.path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func clone(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}
