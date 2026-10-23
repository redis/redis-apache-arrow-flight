package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

// Server is the Arrow Flight server.
type Server struct {
	cfg      Config
	log      *slog.Logger
	tenants  *tenantCache
	sessions *sessionStore // nil when sessions are off
	grpc     *grpc.Server
	health   *redisHealth
	metrics  *metrics
	// metricsHTTP serves metrics, with ServeMetrics.
	metricsHTTP *http.Server
	// stopDrain ends the health service's Watch streams at Shutdown.
	stopDrain context.CancelFunc
	shutdown  sync.Once
}

// New uses the community implementation; see the build-profile contract.
func New(cfg Config) (*Server, error) {
	cfg.setDefaults()
	if !enterpriseBuild && (cfg.Auth != AuthNone || cfg.TLSCertFile != "" || cfg.TLSKeyFile != "") {
		return nil, errFeatureUnavailable
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if _, err := clientOptions(cfg.RedisURI, identity{}); err != nil {
		return nil, err
	}
	m := newMetrics()
	middleware := []flight.ServerMiddleware{skipHealth(logMiddleware(cfg.Logger, m)), recoverMiddleware(cfg.Logger)}
	if cfg.Auth == AuthRedisACL {
		sealer, err := newTokenSealer(cfg.TokenSecret)
		if err != nil {
			return nil, err
		}
		if cfg.TokenSecret == "" {
			cfg.Logger.Warn("no token secret is set: tokens are valid only until this server restarts, " + "and other servers won't accept them")
		}
		v := &aclValidator{uri: cfg.RedisURI, sealer: sealer, ttl: cfg.TokenTTL, now: time.Now}
		middleware = append(middleware, skipHealth(flight.CreateServerBasicAuthMiddleware(v)))
	}
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(cfg.MaxMsgSize), grpc.WaitForHandlers(true), grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Minute, Timeout: 20 * time.Second}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true})}
	tenants := newTenantCache(&cfg)
	tenants.metrics = m
	var sessions *sessionStore
	if !cfg.DisableSessions {
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
	m.watch(tenants, sessions, sql.results, health)
	drain, stopDrain := context.WithCancel(context.Background())
	middleware = append(middleware, flight.ServerMiddleware{Stream: healthDrain(drain)})
	r := &router{sql: flightsql.NewFlightServerWithAllocator(sql, cfg.Allocator), ds: &datasets{tenants: tenants, prefix: cfg.KeyPrefix, ttl: cfg.DatasetTTL, endpointBatches: int64(cfg.DatasetEndpointBatches), alloc: cfg.Allocator}}
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
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.handler())
	return &Server{cfg: cfg, log: cfg.Logger, tenants: tenants, sessions: sessions, grpc: gs, health: health, metrics: m, stopDrain: stopDrain, metricsHTTP: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}}, nil
}

// MetricsHandler serves the server's Prometheus metrics, for a server of
// your own (ListenAndServe serves them on Config.MetricsListen).
func (s *Server) MetricsHandler() http.Handler { return s.metrics.handler() }

// ServeMetrics serves the Prometheus metrics at /metrics on lis, over plain
// HTTP without authentication, until Shutdown.
func (s *Server) ServeMetrics(lis net.Listener) error {
	s.log.Info("serving metrics", "addr", lis.Addr().String())
	if err := s.metricsHTTP.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Serve serves on lis. Once Shutdown has stopped the gRPC server, Serve
// returns, before Shutdown has closed the sessions and the Redis
// connections: wait for Shutdown to return before exiting.
func (s *Server) Serve(lis net.Listener) error {
	s.log.Info("serving Arrow Flight", "addr", lis.Addr().String(), "redis", redactURI(s.cfg.RedisURI),
		"auth", string(s.cfg.Auth), "tls", s.cfg.TLSCertFile != "", "sessions", !s.cfg.DisableSessions)
	return s.grpc.Serve(lis)
}

// ListenAndServe listens on Config.Listen and serves, as Serve does, and
// serves metrics on Config.MetricsListen, if set (see ServeMetrics).
func (s *Server) ListenAndServe() error {
	lis, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	if s.cfg.MetricsListen != "" {
		mlis, err := net.Listen("tcp", s.cfg.MetricsListen)
		if err != nil {
			_ = lis.Close()
			return fmt.Errorf("metrics: %w", err)
		}
		go func() {
			if err := s.ServeMetrics(mlis); err != nil {
				s.log.Error("serving metrics failed", "error", err)
			}
		}()
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
		// Last, so the drain can be watched.
		_ = s.metricsHTTP.Close()
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

// logMiddleware uses the community implementation; see the build-profile contract.
func logMiddleware(log *slog.Logger, m *metrics) flight.ServerMiddleware {
	return flight.ServerMiddleware{}
}
