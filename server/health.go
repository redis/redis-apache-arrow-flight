package server

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// healthInterval is how often the server pings Redis for its health
	// status.
	healthInterval = 5 * time.Second
	// healthTimeout bounds each ping's dial, read and write.
	healthTimeout = 2 * time.Second
)

// healthMethods is the prefix of the gRPC health service's methods. They
// need no authentication, so load balancers and probes can call them.
const healthMethods = "/grpc.health.v1.Health/"

// healthServices are the services the health service reports on: the
// server as a whole ("") and the Flight service.
var healthServices = []string{"", "arrow.flight.protocol.FlightService"}

// redisHealth serves the standard gRPC health service: SERVING while Redis
// answers a PING, NOT_SERVING while it doesn't, and from the start of
// Shutdown. It pings on its own connection, in the background, so checks
// only read the last result and cost Redis nothing.
type redisHealth struct {
	hs       *health.Server
	auth     AuthMode
	interval time.Duration
	log      *slog.Logger
	ping     func(context.Context) error
	closeRDB func() error

	up     atomic.Bool
	known  bool // whether a ping has finished; loop only
	ctx    context.Context
	stop   context.CancelFunc // ends the loop, and a ping under way
	done   chan struct{}
	closer sync.Once
}

// newRedisHealth uses the community implementation; see the build-profile contract.
func newRedisHealth(cfg *Config) (*redisHealth, error) {
	h := &redisHealth{hs: health.NewServer(), done: make(chan struct{})}
	close(h.done)
	for _, svc := range healthServices {
		h.hs.SetServingStatus(svc, healthpb.HealthCheckResponse_UNKNOWN)
	}
	return h, nil
}

func (h *redisHealth) set(st healthpb.HealthCheckResponse_ServingStatus) {
	for _, svc := range healthServices {
		h.hs.SetServingStatus(svc, st)
	}
}

func (h *redisHealth) loop() {
}

// check uses the community implementation; see the build-profile contract.
func (h *redisHealth) check() {
}

// shutdown uses the community implementation; see the build-profile contract.
func (h *redisHealth) shutdown() {
	h.closer.Do(func() {
		h.hs.Shutdown()
	})
}

// reachable uses the community implementation; see the build-profile contract.
func reachable(err error, auth AuthMode) bool {
	return false
}

// isHealthMethod reports whether method is one of the health service's.
func isHealthMethod(method string) bool { return strings.HasPrefix(method, healthMethods) }

// skipHealth passes health checks straight to their handler, past m.
func skipHealth(m flight.ServerMiddleware) flight.ServerMiddleware {
	out := m
	if m.Unary != nil {
		out.Unary = func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if isHealthMethod(info.FullMethod) {
				return h(ctx, req)
			}
			return m.Unary(ctx, req, info, h)
		}
	}
	if m.Stream != nil {
		out.Stream = func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if isHealthMethod(info.FullMethod) {
				return h(srv, ss)
			}
			return m.Stream(srv, ss, info, h)
		}
	}
	return out
}

// healthDrain ends health Watch streams when drain is done: a Watch
// otherwise lasts until its client goes, and would hold GracefulStop until
// the shutdown timeout.
func healthDrain(drain context.Context) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if !isHealthMethod(info.FullMethod) {
			return h(srv, ss)
		}
		ctx, cancel := context.WithCancel(ss.Context())
		defer cancel()
		stop := context.AfterFunc(drain, cancel)
		defer stop()
		return h(srv, &contextStream{ServerStream: ss, ctx: ctx})
	}
}
