package rebac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// TokenCodec serializes a ConsistencyToken for an untrusted transport or
// content store. Applications should keep decoded tokens inside trusted code.
type TokenCodec interface {
	Encode(ConsistencyToken) (string, error)
	Decode(string) (ConsistencyToken, error)
}

// HMACTokenCodec produces versioned, authenticated opaque tokens without a
// database dependency. The application owns key rotation and codec lifetime.
type HMACTokenCodec struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

type tokenEnvelope struct {
	Version uint8            `json:"v"`
	Expires int64            `json:"exp,omitempty"`
	Token   ConsistencyToken `json:"token"`
}

// NewHMACTokenCodec creates a codec. ttl of zero disables expiry.
func NewHMACTokenCodec(key []byte, ttl time.Duration) (*HMACTokenCodec, error) {
	if len(key) < 32 || ttl < 0 {
		return nil, errors.New("rebac: token key must be at least 32 bytes and ttl cannot be negative")
	}
	return &HMACTokenCodec{key: append([]byte(nil), key...), ttl: ttl, now: time.Now}, nil
}

func (c *HMACTokenCodec) Encode(token ConsistencyToken) (string, error) {
	if c == nil || len(c.key) < 32 || token.empty() {
		return "", ErrInvalidRevision
	}
	envelope := tokenEnvelope{Version: 1, Token: token}
	if c.ttl > 0 {
		envelope.Expires = c.now().Add(c.ttl).Unix()
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (c *HMACTokenCodec) Decode(encoded string) (ConsistencyToken, error) {
	if c == nil || len(c.key) < 32 {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	payload, signature, ok := strings.Cut(encoded, ".")
	if !ok || payload == "" || signature == "" || strings.Contains(signature, ".") {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(payload))
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	var envelope tokenEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Version != 1 || envelope.Token.empty() {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	if envelope.Expires != 0 && !c.now().Before(time.Unix(envelope.Expires, 0)) {
		return ConsistencyToken{}, ErrInvalidRevision
	}
	return envelope.Token, nil
}
