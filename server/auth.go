package server

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// newTokenSealer derives the key from secret, or makes a random one when
// secret is empty.
func newTokenSealer(secret string) (*tokenSealer, error) {
	var key [32]byte
	if secret == "" {
		if _, err := rand.Read(key[:]); err != nil {
			return nil, err
		}
	} else {
		key = sha256.Sum256([]byte(secret))
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &tokenSealer{aead: aead}, nil
}

type tokenClaims struct {
	identity
	Expires int64 `json:"e"` // unix seconds
}

func (s *tokenSealer) seal(id identity, expires time.Time) (string, error) {
	plain, err := json.Marshal(tokenClaims{identity: id, Expires: expires.Unix()})
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(plain)+s.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(s.aead.Seal(nonce, nonce, plain, []byte(tokenAAD))), nil
}

var errBadToken = errors.New("invalid or expired token")

func (s *tokenSealer) open(token string, now time.Time) (identity, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < s.aead.NonceSize() {
		return identity{}, errBadToken
	}
	nonce, sealed := raw[:s.aead.NonceSize()], raw[s.aead.NonceSize():]
	plain, err := s.aead.Open(nil, nonce, sealed, []byte(tokenAAD))
	if err != nil {
		return identity{}, errBadToken
	}
	var c tokenClaims
	if err := json.Unmarshal(plain, &c); err != nil || now.Unix() >= c.Expires {
		return identity{}, errBadToken
	}
	return c.identity, nil
}

// aclValidator is the flight.BasicAuthValidator for AuthRedisACL.
type aclValidator struct {
	uri    string
	sealer *tokenSealer
	ttl    time.Duration
	now    func() time.Time
}

// Validate checks a username and password against Redis and returns a
// bearer token.
func (v *aclValidator) Validate(username, password string) (string, error) {
	if username == "" {
		username = "default"
	}
	id := identity{Username: username, Password: password}
	opts, err := clientOptions(v.uri, id)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	opts.MaxRetries = -1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := goredis.NewClient(opts)
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "WRONGPASS") || strings.Contains(msg, "NOAUTH") ||
			strings.Contains(msg, "NOPERM") || strings.Contains(msg, "invalid username-password") {
			return "", status.Error(codes.Unauthenticated, "invalid Redis username or password")
		}
		return "", status.Errorf(codes.Unavailable, "failed to reach Redis to check the credentials: %v", err)
	}
	token, err := v.sealer.seal(id, v.now().Add(v.ttl))
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to make a token: %v", err)
	}
	return token, nil
}

// IsValid returns the identity a bearer token carries.
func (v *aclValidator) IsValid(token string) (any, error) {
	id, err := v.sealer.open(token, v.now())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return id, nil
}
