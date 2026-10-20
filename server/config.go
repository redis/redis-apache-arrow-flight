// Package server is an Apache Arrow Flight and Flight SQL server for Redis.
//
// Two kinds of requests are served on one gRPC endpoint:
//
//   - Flight SQL commands (CMD descriptors and protobuf tickets) run through
//     the Redis ADBC driver (github.com/adbc-drivers/redis/go), which stores
//     tables as one HASH per row with a RediSearch index and translates SQL
//     into FT.AGGREGATE pipelines.
//   - Plain Flight requests on PATH descriptors upload and download named
//     datasets, stored in Redis as Arrow IPC streams (see datasets.go).
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// Version is the server's version, set at build time with
// -ldflags "-X github.com/alberttwong/redis-apache-arrow-flight/server.Version=…".
var Version = "dev"

// ServerName is reported as the Flight SQL server name.
const ServerName = "Redis Arrow Flight SQL Server"

// AuthMode selects how clients authenticate.
type AuthMode string

const (
	// AuthNone serves every client with the credentials of Config.RedisURI.
	AuthNone AuthMode = "none"
	// AuthRedisACL has clients log in with HTTP Basic credentials (the Flight
	// handshake), checks them against Redis, and runs each client's requests
	// as that Redis ACL user.
	AuthRedisACL AuthMode = "redis-acl"
)

// Config configures a Server. The zero value of each field means its default.
type Config struct {
	// Listen is the address to listen on (default "localhost:8815").
	Listen string
	// RedisURI is redis://[user:pass@]host:port/db, or rediss:// for TLS
	// (default "redis://localhost:6379/0"). With AuthRedisACL, its host,
	// database and TLS settings are used with each client's credentials.
	RedisURI string

	// Auth selects client authentication (default AuthNone).
	Auth AuthMode
	// TokenSecret derives the key that seals bearer tokens. Servers that share
	// it accept each other's tokens. When empty, a random key is used, so
	// tokens last only as long as the process.
	TokenSecret string
	// TokenTTL is how long a bearer token is valid (default 12h).
	TokenTTL time.Duration

	// TLSCertFile and TLSKeyFile enable TLS on the gRPC endpoint.
	TLSCertFile string
	TLSKeyFile  string

	// KeyPrefix prefixes the server's own Redis keys: datasets, and
	// statement results too big for their ticket (default "arrowflight:").
	KeyPrefix string
	// DatasetTTL expires uploaded datasets; 0 keeps them until dropped.
	DatasetTTL time.Duration
	// DatasetEndpointBatches, if set, splits a dataset's FlightInfo into one
	// endpoint per this many batches, which clients can read in parallel.
	// Clients must then read every endpoint. 0 gives one endpoint.
	DatasetEndpointBatches int

	// PoolMaxIdle is the number of idle ADBC connections kept per user
	// (default 8). PoolMaxOpen caps the connections in use at once per user
	// (default 32).
	PoolMaxIdle int
	PoolMaxOpen int
	// TenantIdleTimeout closes a user's connections after this long without
	// requests (default 10m).
	TenantIdleTimeout time.Duration

	// MaxStreams is the number of query results a user can have streaming
	// from Redis at once (default 8). Each holds a pooled connection until
	// it is read; further queries are read whole before they are sent
	// (see ResultMemory). At most PoolMaxOpen-1 stream, so a user always
	// has a connection left for other requests (none stream with a pool
	// of 1).
	MaxStreams int
	// StreamOverflow selects what happens when an eligible scan has no stream
	// slot: "buffer" (default, within ResultMemory) or "reject". Reject returns
	// RESOURCE_EXHAUSTED before executing the scan instead of materializing it.
	StreamOverflow string
	// MaxTableStreams is the number of query results that can stream from
	// one table at once, across all users of the server (default 32). Each
	// holds a cursor on the table's index, and RediSearch allows an index
	// search-index-cursor-limit of them (128 by default), shared with other
	// clients and servers. On standalone Redis, results also stop
	// streaming once the index has three quarters of its cursors open,
	// counting every client (FT.INFO); a cluster doesn't report the count.
	// Further queries on the table are read whole (see ResultMemory).
	MaxTableStreams int
	// ResultMemory is the most memory, in bytes, that results which could
	// have streamed but found no stream free (MaxStreams, MaxTableStreams)
	// take at once, across the server, while they are read whole and sent
	// (default 1 GiB). A result that doesn't fit fails with
	// RESOURCE_EXHAUSTED.
	ResultMemory int
	// StreamStallTimeout ends a streaming result whose client takes longer
	// than this to read a batch (default 2m), since Redis deletes a cursor
	// left unread for 5 minutes.
	StreamStallTimeout time.Duration

	// DisableSessions turns off Flight SQL sessions (cookies, a dedicated
	// connection per session, and the session options).
	DisableSessions bool
	// SessionIdleTimeout closes a session after this long without requests
	// (default 10m).
	SessionIdleTimeout time.Duration

	// MaxMsgSize is the largest gRPC message the server accepts
	// (default 64 MiB).
	MaxMsgSize int

	// ShutdownTimeout is how long Shutdown lets running calls finish before
	// it cancels them (default 25s, under Kubernetes' default 30s grace
	// period).
	ShutdownTimeout time.Duration

	// MetricsListen is the address ListenAndServe serves Prometheus metrics
	// on, at /metrics, over plain HTTP without authentication (default
	// none: off).
	MetricsListen string

	// ADBCOptions are passed to the Redis ADBC driver's database, for
	// example adbc.redis.aggregate_pushdown or adbc.redis.default_schema.
	ADBCOptions map[string]string

	Logger    *slog.Logger
	Allocator memory.Allocator

	// redisHook, if set, is added to every go-redis client the server opens
	// for requests (tests use it to make Redis replies fail).
	redisHook goredis.Hook
	// healthInterval is how often the health service pings Redis (default
	// healthInterval; tests shorten it).
	healthInterval time.Duration
}

func (c *Config) setDefaults() {
	if c.StreamOverflow == "" {
		c.StreamOverflow = "buffer"
	}
	if c.Listen == "" {
		c.Listen = "localhost:8815"
	}
	if c.RedisURI == "" {
		c.RedisURI = "redis://localhost:6379/0"
	}
	if c.Auth == "" {
		c.Auth = AuthNone
	}
	if c.TokenTTL == 0 {
		c.TokenTTL = 12 * time.Hour
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "arrowflight:"
	}
	if c.PoolMaxIdle == 0 {
		c.PoolMaxIdle = 8
	}
	if c.PoolMaxOpen == 0 {
		c.PoolMaxOpen = 32
	}
	if c.TenantIdleTimeout == 0 {
		c.TenantIdleTimeout = 10 * time.Minute
	}
	if c.MaxStreams == 0 {
		c.MaxStreams = 8
	}
	if c.MaxTableStreams == 0 {
		c.MaxTableStreams = 32
	}
	if c.ResultMemory == 0 {
		c.ResultMemory = 1 << 30
	}
	if c.StreamStallTimeout == 0 {
		c.StreamStallTimeout = 2 * time.Minute
	}
	if c.SessionIdleTimeout == 0 {
		c.SessionIdleTimeout = 10 * time.Minute
	}
	if c.MaxMsgSize == 0 {
		c.MaxMsgSize = 64 << 20
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 25 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Allocator == nil {
		c.Allocator = memory.DefaultAllocator
	}
	if c.healthInterval == 0 {
		c.healthInterval = healthInterval
	}
}

func (c *Config) validate() error {
	if c.StreamOverflow != "buffer" && c.StreamOverflow != "reject" {
		return fmt.Errorf("invalid StreamOverflow %q (want buffer or reject)", c.StreamOverflow)
	}
	switch c.Auth {
	case AuthNone, AuthRedisACL:
	default:
		return fmt.Errorf("invalid auth mode %q (want %q or %q)", c.Auth, AuthNone, AuthRedisACL)
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("TLS needs both a certificate and a key file")
	}
	if c.DatasetEndpointBatches < 0 || c.MaxStreams < 0 || c.MaxTableStreams < 0 || c.ResultMemory < 0 {
		return errors.New("DatasetEndpointBatches, MaxStreams, MaxTableStreams and ResultMemory must not be negative")
	}
	if c.PoolMaxIdle < 0 || c.PoolMaxOpen < 1 {
		return errors.New("the pool needs PoolMaxIdle >= 0 and PoolMaxOpen >= 1")
	}
	if c.TokenTTL < 0 || c.DatasetTTL < 0 || c.TenantIdleTimeout < 0 || c.SessionIdleTimeout < 0 ||
		c.StreamStallTimeout < 0 || c.ShutdownTimeout < 0 {
		return errors.New("durations must not be negative")
	}
	if c.MetricsListen != "" {
		if _, _, err := net.SplitHostPort(c.MetricsListen); err != nil {
			return fmt.Errorf("invalid metrics address %q: %w", c.MetricsListen, err)
		}
	}
	for _, k := range []string{"uri", "username", "password"} {
		if _, ok := c.ADBCOptions[k]; ok {
			return fmt.Errorf("set the Redis URI and credentials with RedisURI, not the ADBC option %q", k)
		}
	}
	return nil
}
