// Package memory provides the in-process reference storage adapter.
package memory

import "github.com/surya-mp/go-rebac/kv"

// New returns an in-memory store implementing every capability supported by
// the bundled KV adapter. It is suitable for tests, examples, and benchmarks.
func New() *kv.ReBACStore {
	return kv.NewReBACStore(kv.New())
}
