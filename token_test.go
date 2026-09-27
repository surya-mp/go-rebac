package rebac

import (
	"errors"
	"testing"
	"time"
)

func TestHMACTokenCodec(t *testing.T) {
	codec, err := NewHMACTokenCodec([]byte("01234567890123456789012345678901"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token := ConsistencyToken{TenantID: "acme", ModelID: "document_access", ModelVersion: "7", TupleRevision: "42"}
	encoded, err := codec.Encode(token)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil || decoded != token {
		t.Fatalf("Decode() = %#v, %v", decoded, err)
	}
	if _, err := codec.Decode(encoded + "x"); !errors.Is(err, ErrInvalidRevision) {
		t.Fatalf("tampered token error = %v; want ErrInvalidRevision", err)
	}
}
