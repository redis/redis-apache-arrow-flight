package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Server is the Arrow Flight server.
type Server struct {
	cfg      Config
	log      *slog.Logger
	tenants  *tenantCache
	sessions *sessionStore // nil when sessions are off
	grpc     *grpc.Server
	health   *redisHealth
	// stopDrain ends the health service's Watch streams at Shutdown.
	stopDrain context.CancelFunc
	shutdown  sync.Once
}

// New makes a Server from cfg. It opens ADBC connections only when
// requests come in, and pings Redis in the background for its health
// status, so it starts even if Redis is down.
func New(cfg Config) (*Server, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if _, err := clientOptions(cfg.RedisURI, identity{}); err != nil {
		return nil, err
	}

	// Health checks skip logging (probes call often), authentication and
	// sessions.
	middleware := []flight.ServerMiddleware{recoverMiddleware(cfg.Logger), skipHealth(logMiddleware(cfg.Logger))}
	if cfg.Auth == AuthRedisACL {
		sealer, err := newTokenSealer(cfg.TokenSecret)
		if err != nil {
			return nil, err
		}
		if cfg.TokenSecret == "" {
			cfg.Logger.Warn("no token secret is set: tokens are valid only until this server restarts, " +
				"and other servers won't accept them")
		}
		v := &aclValidator{uri: cfg.RedisURI, sealer: sealer, ttl: cfg.TokenTTL, now: time.Now}
		middleware = append(middleware, skipHealth(flight.CreateServerBasicAuthMiddleware(v)))
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxMsgSize),
		// So Shutdown, once Stop has cancelled the calls still running, waits
		// for them to give back their connections and cursors.
		grpc.WaitForHandlers(true),
		// Ping idle connections, so a client that has vanished doesn't hold
		// a call (and its connection and cursor) until TCP gives up.
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Minute, Timeout: 20 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
	if cfg.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load the TLS certificate: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})))
	}

	tenants := newTenantCache(&cfg)
	var sessions *sessionStore
	if !cfg.DisableSessions {
		// After authentication: a session belongs to who made it.
		sessions = newSessionStore(tenants, cfg.SessionIdleTimeout, cfg.Logger)
		middleware = append(middleware, skipHealth(sessions.middleware()))
	}
	sql, err := newSQLServer(tenants, cfg.Allocator, cfg.KeyPrefix)
	if err != nil {
		if sessions != nil {
			sessions.close()
		}
		tenants.close()
		return nil, err
	}
	health, err := newRedisHealth(&cfg)
	if err != nil {
		if sessions != nil {
			sessions.close()
		}
		tenants.close()
		return nil, err
	}
	drain, stopDrain := context.WithCancel(context.Background())
	middleware = append(middleware, flight.ServerMiddleware{Stream: healthDrain(drain)})
	r := &router{
		sql: flightsql.NewFlightServerWithAllocator(sql, cfg.Allocator),
		ds: &datasets{tenants: tenants, prefix: cfg.KeyPrefix, ttl: cfg.DatasetTTL,
			endpointBatches: int64(cfg.DatasetEndpointBatches), alloc: cfg.Allocator},
	}
	// The gRPC server is built as flight.NewServerWithMiddleware would, but
	// kept, so Shutdown can Stop it. (Its interceptors for a
	// BaseFlightServer auth handler do nothing here: the router has none.)
	var unary []grpc.UnaryServerInterceptor
	var stream []grpc.StreamServerInterceptor
	for _, m := range middleware {
		if m.Unary != nil {
			unary = append(unary, m.Unary)
		}
		if m.Stream != nil {
			stream = append(stream, m.Stream)
		}
	}
	opts = append(opts, grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))
	gs := grpc.NewServer(opts...)
	flight.RegisterFlightServiceServer(gs, r)
	healthpb.RegisterHealthServer(gs, health.hs)
	return &Server{cfg: cfg, log: cfg.Logger, tenants: tenants, sessions: sessions, grpc: gs,
		health: health, stopDrain: stopDrain}, nil
}

// Serve serves on lis. Once Shutdown has stopped the gRPC server, Serve
// returns, before Shutdown has closed the sessions and the Redis
// connections: wait for Shutdown to return before exiting.
func (s *Server) Serve(lis net.Listener) error {
	s.log.Info("serving Arrow Flight", "addr", lis.Addr().String(), "redis", redactURI(s.cfg.RedisURI),
		"auth", string(s.cfg.Auth), "tls", s.cfg.TLSCertFile != "", "sessions", !s.cfg.DisableSessions)
	return s.grpc.Serve(lis)
}

// ListenAndServe listens on Config.Listen and serves, as Serve does.
func (s *Server) ListenAndServe() error {
	lis, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	return s.Serve(lis)
}

// Shutdown stops accepting calls and lets running ones finish, for up to
// Config.ShutdownTimeout; then it cancels the rest and waits for them to
// release what they hold (a streaming result's cursor and connection).
// Last, it closes the sessions, which drops their temporary tables, and
// the Redis connections. Health checks report NOT_SERVING from the start.
// Later calls wait for the first to finish.
func (s *Server) Shutdown() {
	s.shutdown.Do(func() {
		s.health.shutdown()
		s.stopDrain()
		stopped := make(chan struct{})
		go func() {
			s.grpc.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(s.cfg.ShutdownTimeout)
		defer timer.Stop()
		select {
		case <-stopped:
		case <-timer.C:
			s.log.Warn("calls still running at the shutdown timeout are cancelled", "timeout", s.cfg.ShutdownTimeout)
			// Stop closes the connections, which cancels the calls' contexts
			// and ends sends blocked on a client that doesn't read.
			s.grpc.Stop()
			<-stopped
		}
		if s.sessions != nil {
			s.sessions.close()
		}
		s.tenants.close()
	})
}

// redactURI hides a URI's password for logs.
func redactURI(uri string) string {
	opts, err := clientOptions(uri, identity{})
	if err != nil {
		return "(invalid)"
	}
	scheme := "redis"
	if opts.TLSConfig != nil {
		scheme = "rediss"
	}
	return fmt.Sprintf("%s://%s/%d", scheme, opts.Addr, opts.DB)
}

// recoverMiddleware turns a panic in a handler into an Internal error
// instead of a crash.
func recoverMiddleware(log *slog.Logger) flight.ServerMiddleware {
	recovered := func(method string, err *error) {
		if p := recover(); p != nil {
			log.Error("panic in handler", "method", method, "panic", p, "stack", string(debug.Stack()))
			*err = status.Errorf(codes.Internal, "internal error: %v", p)
		}
	}
	return flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (_ any, err error) {
			defer recovered(info.FullMethod, &err)
			return h(ctx, req)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
			defer recovered(info.FullMethod, &err)
			return h(srv, ss)
		},
	}
}

// logMiddleware logs each call: at debug level when it succeeds, at info
// level for client errors, and at warn level for server errors.
func logMiddleware(log *slog.Logger) flight.ServerMiddleware {
	done := func(ctx context.Context, method string, start time.Time, err error) {
		code := status.Code(err)
		level := slog.LevelDebug
		switch code {
		case codes.OK, codes.Canceled:
		case codes.Internal, codes.Unknown, codes.Unavailable, codes.DataLoss:
			level = slog.LevelWarn
		default:
			level = slog.LevelInfo
		}
		attrs := []any{"method", method, "code", code.String(), "duration", time.Since(start)}
		if err != nil {
			attrs = append(attrs, "error", status.Convert(err).Message())
		}
		log.Log(ctx, level, "call", attrs...)
	}
	return flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			start := time.Now()
			resp, err := h(ctx, req)
			done(ctx, info.FullMethod, start, err)
			return resp, err
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			start := time.Now()
			err := h(srv, ss)
			done(ss.Context(), info.FullMethod, start, err)
			return err
		},
	}
}
