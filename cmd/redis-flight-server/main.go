// Command redis-flight-server serves Redis over Apache Arrow Flight and
// Flight SQL. Every flag can also be set with the environment variable
// shown in its help.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alberttwong/redis-apache-arrow-flight/server"
)

// optionsFlag collects repeated key=value flags.
type optionsFlag map[string]string

func (o optionsFlag) String() string { return "" }

func (o optionsFlag) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("want key=value, got %q", v)
	}
	o[k] = val
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "redis-flight-server:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("redis-flight-server", flag.ContinueOnError)
	var (
		cfg      server.Config
		sessions bool
		auth     string
		logLevel string
		version  bool
		check    bool
		adbcOpts = optionsFlag{}
	)
	str := func(p *string, name, env, def, usage string) {
		fs.StringVar(p, name, envOr(env, def), fmt.Sprintf("%s (env %s)", usage, env))
	}
	// Secrets come from the environment after parsing, so -h doesn't show
	// them as defaults.
	secrets := map[string]struct {
		p   *string
		env string
	}{}
	secret := func(p *string, name, env, def, usage string) {
		fs.StringVar(p, name, def, fmt.Sprintf("%s (env %s)", usage, env))
		secrets[name] = struct {
			p   *string
			env string
		}{p, env}
	}
	dur := func(p *time.Duration, name, env string, def time.Duration, usage string) {
		v, err := time.ParseDuration(envOr(env, def.String()))
		if err != nil {
			v = def
		}
		fs.DurationVar(p, name, v, fmt.Sprintf("%s (env %s)", usage, env))
	}
	num := func(p *int, name, env string, def int, usage string) {
		v, err := strconv.Atoi(envOr(env, strconv.Itoa(def)))
		if err != nil {
			v = def
		}
		fs.IntVar(p, name, v, fmt.Sprintf("%s (env %s)", usage, env))
	}

	str(&cfg.Listen, "listen", "FLIGHT_LISTEN", "localhost:8815", "address to listen on")
	secret(&cfg.RedisURI, "redis-uri", "REDIS_URI", "redis://localhost:6379/0", "Redis URI: redis://[user:pass@]host:port/db, or rediss:// for TLS")
	str(&auth, "auth", "FLIGHT_AUTH", "none", `client authentication: "none", or "redis-acl" (Basic auth checked against Redis ACL users)`)
	secret(&cfg.TokenSecret, "token-secret", "FLIGHT_TOKEN_SECRET", "", "secret that seals bearer tokens; servers that share it accept each other's tokens (default: random per process)")
	dur(&cfg.TokenTTL, "token-ttl", "FLIGHT_TOKEN_TTL", 12*time.Hour, "how long a bearer token is valid")
	str(&cfg.TLSCertFile, "tls-cert", "FLIGHT_TLS_CERT", "", "TLS certificate file (PEM); enables TLS with -tls-key")
	str(&cfg.TLSKeyFile, "tls-key", "FLIGHT_TLS_KEY", "", "TLS private key file (PEM)")
	str(&cfg.KeyPrefix, "key-prefix", "FLIGHT_KEY_PREFIX", "arrowflight:", "prefix of the Redis keys that store datasets")
	dur(&cfg.DatasetTTL, "dataset-ttl", "FLIGHT_DATASET_TTL", 0, "expire uploaded datasets after this long (0: keep until dropped)")
	num(&cfg.DatasetEndpointBatches, "dataset-endpoint-batches", "FLIGHT_DATASET_ENDPOINT_BATCHES", 0,
		"split a dataset's FlightInfo into one endpoint per this many batches (0: one endpoint)")
	num(&cfg.PoolMaxIdle, "pool-max-idle", "FLIGHT_POOL_MAX_IDLE", 8, "idle ADBC connections kept per Redis user")
	num(&cfg.PoolMaxOpen, "pool-max-open", "FLIGHT_POOL_MAX_OPEN", 32, "ADBC connections in use at once per Redis user")
	num(&cfg.MaxStreams, "max-streams", "FLIGHT_MAX_STREAMS", 8,
		"query results a Redis user can stream from Redis at once, at most -pool-max-open minus 1; others are read whole first, within -result-memory")
	str(&cfg.StreamOverflow, "stream-overflow", "FLIGHT_STREAM_OVERFLOW", "buffer",
		"when an eligible scan has no free stream: buffer within -result-memory, or reject with RESOURCE_EXHAUSTED")
	num(&cfg.MaxTableStreams, "max-table-streams", "FLIGHT_MAX_TABLE_STREAMS", 32,
		"query results that can stream from one table at once, across all users; keep well under Redis's search-index-cursor-limit (128)")
	num(&cfg.ResultMemory, "result-memory", "FLIGHT_RESULT_MEMORY", 1<<30,
		"bytes of memory that query results read whole for want of a stream (-max-streams, -max-table-streams) may take at once, "+
			"across the server; a result that doesn't fit fails with RESOURCE_EXHAUSTED")
	dur(&cfg.StreamStallTimeout, "stream-stall-timeout", "FLIGHT_STREAM_STALL_TIMEOUT", 2*time.Minute,
		"end a streaming result whose client takes longer than this to read a batch")
	fs.BoolVar(&sessions, "sessions", envOr("FLIGHT_SESSIONS", "true") != "false",
		"Flight SQL sessions: a client that keeps the session cookie gets its own connection, so SET and temporary tables last (env FLIGHT_SESSIONS)")
	dur(&cfg.SessionIdleTimeout, "session-idle-timeout", "FLIGHT_SESSION_IDLE_TIMEOUT", 10*time.Minute, "close a session after this long without requests")
	num(&cfg.MaxMsgSize, "max-msg-size", "FLIGHT_MAX_MSG_SIZE", 64<<20, "largest gRPC message accepted, in bytes")
	dur(&cfg.ShutdownTimeout, "shutdown-timeout", "FLIGHT_SHUTDOWN_TIMEOUT", 25*time.Second,
		"on SIGTERM or SIGINT, let running calls finish for this long, then cancel them")
	str(&cfg.MetricsListen, "metrics-listen", "FLIGHT_METRICS_LISTEN", "",
		"address to serve Prometheus metrics on, at /metrics over plain HTTP without authentication (default: off)")
	str(&logLevel, "log-level", "FLIGHT_LOG_LEVEL", "info", "debug, info, warn or error")
	fs.Var(adbcOpts, "adbc-option", "Redis ADBC driver database option key=value, repeatable (env FLIGHT_ADBC_OPTIONS, comma-separated)")
	fs.BoolVar(&version, "version", false, "print the version and exit")
	fs.BoolVar(&check, "healthcheck", false,
		"check the server at -listen with the gRPC health service and exit: 0 if it is serving, 1 if not (for a Docker HEALTHCHECK)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if version {
		fmt.Println(server.Version)
		return nil
	}
	if check {
		return checkHealth(cfg.Listen, cfg.TLSCertFile)
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	for name, s := range secrets {
		if v, ok := os.LookupEnv(s.env); ok && !set[name] {
			*s.p = v
		}
	}
	if env := os.Getenv("FLIGHT_ADBC_OPTIONS"); env != "" {
		for _, kv := range strings.Split(env, ",") {
			k, _, _ := strings.Cut(kv, "=")
			if _, set := adbcOpts[k]; set {
				continue // the flag wins
			}
			if err := adbcOpts.Set(strings.TrimSpace(kv)); err != nil {
				return fmt.Errorf("FLIGHT_ADBC_OPTIONS: %w", err)
			}
		}
	}
	cfg.Auth = server.AuthMode(auth)
	cfg.DisableSessions = !sessions
	cfg.ADBCOptions = adbcOpts

	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("invalid log level %q", logLevel)
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	srv, err := server.New(cfg)
	if err != nil {
		return err
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	select {
	case err := <-served:
		srv.Shutdown()
		return err
	case s := <-sigs:
		// A second signal exits at once.
		signal.Stop(sigs)
		cfg.Logger.Info("shutting down", "signal", s.String(), "timeout", cfg.ShutdownTimeout)
	}
	// Serve returns before Shutdown has closed the sessions (and their
	// temporary tables) and the Redis connections: exit once it is done.
	srv.Shutdown()
	if err := <-served; err != nil {
		return err
	}
	cfg.Logger.Info("shut down")
	return nil
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
