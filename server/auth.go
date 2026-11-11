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
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Redis ACL login exchanges credentials over verified TLS for an expiring,
// authenticated-encrypted bearer token. Each request still uses that principal's
// Redis credentials; the token and snapshot never replace Redis authorization.
const tokenAAD = "redis-arrow-flight token v1"
const authTimeout = 5 * time.Second

type tokenSealer struct{ aead cipher.AEAD }

func newTokenSealer(secret string) (*tokenSealer, error) {
	var key [32]byte
	if secret == "" {
		if _, err := rand.Read(key[:]); err != nil {
			return nil, err
		}
	} else {
		if len(secret) < 32 {
			return nil, errors.New("token secret must contain at least 32 bytes")
		}
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
	Expires int64 `json:"e"`
}

func (s *tokenSealer) seal(id identity, expires time.Time) (string, error) {
	raw, err := json.Marshal(tokenClaims{identity: id, Expires: expires.Unix()})
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	encrypted := s.aead.Seal(nonce, nonce, raw, []byte(tokenAAD))
	return base64.RawURLEncoding.EncodeToString(encrypted), nil
}

var errBadToken = errors.New("invalid or expired token")

func (s *tokenSealer) open(token string, now time.Time) (identity, error) {
	if len(token) > 16384 {
		return identity{}, errBadToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	n := s.aead.NonceSize()
	if err != nil || len(raw) < n+s.aead.Overhead() {
		return identity{}, errBadToken
	}
	plaintext, err := s.aead.Open(nil, raw[:n], raw[n:], []byte(tokenAAD))
	if err != nil {
		return identity{}, errBadToken
	}
	var claims tokenClaims
	if json.Unmarshal(plaintext, &claims) != nil || claims.Expires <= now.Unix() || claims.Username == "" || claims.Password == "" {
		return identity{}, errBadToken
	}
	return claims.identity, nil
}

type aclValidator struct {
	uri    string
	caFile string
	sealer *tokenSealer
	ttl    time.Duration
	now    func() time.Time
}

func (v *aclValidator) Validate(username, password string) (string, error) {
	if username == "" || password == "" {
		return "", status.Error(codes.Unauthenticated, "Redis username and password are required")
	}
	id := identity{Username: username, Password: password}
	opts, err := clientOptionsWithCA(v.uri, id, v.caFile)
	if err != nil || opts.TLSConfig == nil {
		return "", status.Error(codes.Unavailable, "verified Redis TLS is required")
	}
	opts.MaxRetries = -1
	opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout = authTimeout, authTimeout, authTimeout
	opts.ContextTimeoutEnabled = true
	client := goredis.NewClient(opts)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), authTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		switch status.Code(toStatus(err)) {
		case codes.Unauthenticated, codes.PermissionDenied:
			return "", status.Error(codes.Unauthenticated, "Redis authentication failed")
		default:
			return "", status.Error(codes.Unavailable, "Redis authentication service unavailable")
		}
	}
	token, err := v.sealer.seal(id, v.now().Add(v.ttl))
	if err != nil {
		return "", status.Error(codes.Internal, "could not issue authentication token")
	}
	return token, nil
}

func (v *aclValidator) IsValid(token string) (any, error) {
	id, err := v.sealer.open(token, v.now())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, errBadToken.Error())
	}
	return id, nil
}
