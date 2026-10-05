package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

// newRedisHealth starts pinging the Redis of cfg.RedisURI, with its
// credentials if it has any. A cluster's seed node stands for the cluster.
func newRedisHealth(cfg *Config) (*redisHealth, error) {
	opts, err := clientOptions(cfg.RedisURI, identity{})
	if err != nil {
		return nil, err
	}
	opts.PoolSize = 1
	opts.MaxRetries = -1
	opts.DialerRetries = 1 // the next check is the retry
	opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout = healthTimeout, healthTimeout, healthTimeout
	rdb := goredis.NewClient(opts)
	ctx, stop := context.WithCancel(context.Background())
	h := &redisHealth{
		hs:       health.NewServer(),
		auth:     cfg.Auth,
		interval: cfg.healthInterval,
		log:      cfg.Logger,
		ping:     func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		closeRDB: rdb.Close,
		ctx:      ctx,
		stop:     stop,
		done:     make(chan struct{}),
	}
	h.set(healthpb.HealthCheckResponse_NOT_SERVING)
	go h.loop()
	return h, nil
}

func (h *redisHealth) set(st healthpb.HealthCheckResponse_ServingStatus) {
	for _, svc := range healthServices {
		h.hs.SetServingStatus(svc, st)
	}
}

func (h *redisHealth) loop() {
	defer close(h.done)
	tick := time.NewTicker(h.interval)
	defer tick.Stop()
	for {
		h.check()
		select {
		case <-h.ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// check pings Redis once and sets the status, logging when it changes.
func (h *redisHealth) check() {
	ctx, cancel := context.WithTimeout(h.ctx, 2*healthTimeout)
	err := h.ping(ctx)
	cancel()
	if h.ctx.Err() != nil {
		return // shutting down
	}
	up := reachable(err, h.auth)
	was, known := h.up.Swap(up), h.known
	h.known = true
	switch {
	case !up && (was || !known):
		h.log.Warn("Redis is unreachable: health checks report NOT_SERVING", "error", err)
	case up && !was && known:
		h.log.Info("Redis is reachable again: health checks report SERVING")
	}
	if up {
		h.set(healthpb.HealthCheckResponse_SERVING)
	} else {
		h.set(healthpb.HealthCheckResponse_NOT_SERVING)
	}
}

// shutdown reports NOT_SERVING from now on, and stops pinging.
func (h *redisHealth) shutdown() {
	h.closer.Do(func() {
		h.hs.Shutdown()
		h.stop()
		<-h.done
		_ = h.closeRDB()
	})
}

// reachable reports whether a PING's error leaves Redis able to serve
// requests. Any reply from Redis shows it is up, but under AuthNone the
// server's own credentials are what every request uses, so their being
// refused means no request can succeed. Under AuthRedisACL each client
// brings its own.
func reachable(err error, auth AuthMode) bool {
	if err == nil {
		return true
	}
	var rerr goredis.Error
	if !errors.As(err, &rerr) {
		return false // a dial error, a timeout, a closed client
	}
	msg := rerr.Error()
	switch {
	case strings.HasPrefix(msg, "NOPERM"):
		return true
	case strings.HasPrefix(msg, "NOAUTH"), strings.HasPrefix(msg, "WRONGPASS"):
		return auth == AuthRedisACL
	}
	return false // LOADING, BUSY, MASTERDOWN, CLUSTERDOWN, …
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
