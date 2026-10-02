package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// Server is the Arrow Flight server.
type Server struct {
	cfg     Config
	log     *slog.Logger
	tenants *tenantCache
	flight  flight.Server
}

// New makes a Server from cfg. It connects to Redis only when requests
// come in, so it starts even if Redis is down.
func New(cfg Config) (*Server, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if _, err := clientOptions(cfg.RedisURI, identity{}); err != nil {
		return nil, err
	}

	middleware := []flight.ServerMiddleware{recoverMiddleware(cfg.Logger), logMiddleware(cfg.Logger)}
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
		middleware = append(middleware, flight.CreateServerBasicAuthMiddleware(v))
	}

	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(cfg.MaxMsgSize)}
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
	sql, err := newSQLServer(tenants, cfg.Allocator, cfg.KeyPrefix)
	if err != nil {
		tenants.close()
		return nil, err
	}
	r := &router{
		sql: flightsql.NewFlightServerWithAllocator(sql, cfg.Allocator),
		ds:  &datasets{tenants: tenants, prefix: cfg.KeyPrefix, ttl: cfg.DatasetTTL, alloc: cfg.Allocator},
	}
	fs := flight.NewServerWithMiddleware(middleware, opts...)
	fs.RegisterFlightService(r)
	return &Server{cfg: cfg, log: cfg.Logger, tenants: tenants, flight: fs}, nil
}

// Serve serves on lis until Shutdown.
func (s *Server) Serve(lis net.Listener) error {
	s.flight.InitListener(lis)
	s.log.Info("serving Arrow Flight", "addr", lis.Addr().String(), "redis", redactURI(s.cfg.RedisURI),
		"auth", string(s.cfg.Auth), "tls", s.cfg.TLSCertFile != "")
	return s.flight.Serve()
}

// ListenAndServe listens on Config.Listen and serves until Shutdown.
func (s *Server) ListenAndServe() error {
	lis, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	return s.Serve(lis)
}

// Shutdown stops accepting calls, waits for running ones, and closes the
// Redis connections.
func (s *Server) Shutdown() {
	s.flight.Shutdown()
	s.tenants.close()
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
