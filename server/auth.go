package server

import (
	"crypto/cipher"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// With AuthRedisACL, a client logs in with the Flight handshake and HTTP
// Basic credentials, which are a Redis ACL user's. The server checks them
// with PING and returns a bearer token for the client to send with every
// call. Each call then runs as that Redis user, so Redis's ACLs decide what
// it may read and change.
//
// A token is the identity and an expiry time sealed with AES-256-GCM. The
// server keeps no session state, so any server with the same token secret
// accepts it. Tokens contain the Redis password, encrypted: keep the
// secret secret, and use TLS.

const tokenAAD = "redis-arrow-flight token v1"

type tokenSealer struct {
	aead cipher.AEAD
}

// newTokenSealer uses the community implementation; see the build-profile contract.
func newTokenSealer(secret string) (*tokenSealer, error) {
	return nil, errFeatureUnavailable
}

type tokenClaims struct {
	identity
	Expires int64 `json:"e"` // unix seconds
}

func (s *tokenSealer) seal(id identity, expires time.Time) (string, error) {
	return "", errFeatureUnavailable
}

var errBadToken = errors.New("invalid or expired token")

func (s *tokenSealer) open(token string, now time.Time) (identity, error) {
	return identity{}, errFeatureUnavailable
}

// aclValidator is the flight.BasicAuthValidator for AuthRedisACL.
type aclValidator struct {
	uri    string
	sealer *tokenSealer
	ttl    time.Duration
	now    func() time.Time
}

// Validate uses the community implementation; see the build-profile contract.
func (v *aclValidator) Validate(username, password string) (string, error) {
	return "", errFeatureUnavailable
}

// IsValid returns the identity a bearer token carries.
func (v *aclValidator) IsValid(token string) (any, error) {
	id, err := v.sealer.open(token, v.now())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return id, nil
}
