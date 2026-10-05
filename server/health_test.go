package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// redisErr is an error reply from Redis, as go-redis returns it.
type redisErr string

func (e redisErr) Error() string { return string(e) }
func (redisErr) RedisError()     {}

func TestReachable(t *testing.T) {
	for _, tc := range []struct {
		err      error
		none     bool // reachable under AuthNone
		redisACL bool // and under AuthRedisACL
	}{
		{nil, true, true},
		{redisErr("NOAUTH Authentication required."), false, true},
		{redisErr("WRONGPASS invalid username-password pair or user is disabled."), false, true},
		{redisErr("NOPERM User u has no permissions to run the 'ping' command"), true, true},
		{redisErr("LOADING Redis is loading the dataset in memory"), false, false},
		{redisErr("CLUSTERDOWN The cluster is down"), false, false},
		{errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), false, false},
		{context.DeadlineExceeded, false, false},
	} {
		if got := reachable(tc.err, AuthNone); got != tc.none {
			t.Errorf("reachable(%v, none) = %v, want %v", tc.err, got, tc.none)
		}
		if got := reachable(tc.err, AuthRedisACL); got != tc.redisACL {
			t.Errorf("reachable(%v, redis-acl) = %v, want %v", tc.err, got, tc.redisACL)
		}
	}
}

// fakeRedis answers PING with reply (a RESP line such as "+PONG" or
// "-NOAUTH …"), HELLO with an error (so go-redis falls back to RESP2), and
// anything else with +OK.
type fakeRedis struct {
	lis   net.Listener
	mu    sync.Mutex
	reply string
	conns []net.Conn
}

func startFakeRedis(t *testing.T, reply string) *fakeRedis {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRedis{lis: lis, reply: reply}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			f.mu.Unlock()
			go f.serve(c)
		}
	}()
	t.Cleanup(f.close)
	return f
}

func (f *fakeRedis) uri() string { return "redis://" + f.lis.Addr().String() + "/0" }

// close stops listening and drops the connections, as a Redis that goes
// down would.
func (f *fakeRedis) close() {
	_ = f.lis.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
}

func (f *fakeRedis) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		reply := "+OK"
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			reply = "-ERR unknown command 'HELLO'"
		case "PING":
			f.mu.Lock()
			reply = f.reply
			f.mu.Unlock()
		}
		if _, err := io.WriteString(c, reply+"\r\n"); err != nil {
			return
		}
	}
}

// readCommand reads one RESP array of bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("bad command %q", line)
	}
	args := make([]string, n)
	for i := range args {
		if line, err = r.ReadString('\n'); err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
		if err != nil {
			return nil, fmt.Errorf("bad argument %q", line)
		}
		b := make([]byte, size+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		args[i] = string(b[:size])
	}
	return args, nil
}

func checkStatus(t *testing.T, h *redisHealth) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	res, err := h.hs.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetStatus()
}

func waitStatus(t *testing.T, h *redisHealth, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for checkStatus(t, h) != want {
		if time.Now().After(deadline) {
			t.Fatalf("the status didn't become %v", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The status follows Redis's answers to PING.
func TestRedisHealth(t *testing.T) {
	start := func(t *testing.T, f *fakeRedis, auth AuthMode) *redisHealth {
		t.Helper()
		h, err := newRedisHealth(&Config{RedisURI: f.uri(), Auth: auth, Logger: testLogger(t), healthInterval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(h.shutdown)
		return h
	}

	t.Run("serving while Redis answers, not once it's gone", func(t *testing.T) {
		f := startFakeRedis(t, "+PONG")
		h := start(t, f, AuthNone)
		waitStatus(t, h, healthpb.HealthCheckResponse_SERVING)
		if !h.up.Load() {
			t.Error("up is false while serving")
		}
		f.close()
		waitStatus(t, h, healthpb.HealthCheckResponse_NOT_SERVING)
	})

	t.Run("an authentication error is Redis answering, under redis-acl", func(t *testing.T) {
		f := startFakeRedis(t, "-NOAUTH Authentication required.")
		h := start(t, f, AuthRedisACL)
		waitStatus(t, h, healthpb.HealthCheckResponse_SERVING)
	})

	t.Run("but not under none, where the server's credentials are every request's", func(t *testing.T) {
		f := startFakeRedis(t, "-NOAUTH Authentication required.")
		h := start(t, f, AuthNone)
		time.Sleep(100 * time.Millisecond) // several pings
		if got := checkStatus(t, h); got != healthpb.HealthCheckResponse_NOT_SERVING {
			t.Errorf("status = %v, want NOT_SERVING", got)
		}
		f.mu.Lock()
		f.reply = "+PONG"
		f.mu.Unlock()
		waitStatus(t, h, healthpb.HealthCheckResponse_SERVING)
	})

	t.Run("NOT_SERVING after shutdown", func(t *testing.T) {
		f := startFakeRedis(t, "+PONG")
		h := start(t, f, AuthNone)
		waitStatus(t, h, healthpb.HealthCheckResponse_SERVING)
		h.shutdown()
		if got := checkStatus(t, h); got != healthpb.HealthCheckResponse_NOT_SERVING {
			t.Errorf("status = %v, want NOT_SERVING", got)
		}
		h.shutdown() // again: a no-op
	})
}

// Health checks need no token and get no session cookie; other calls are
// unchanged.
func TestHealthCheckSkipsAuthAndSessions(t *testing.T) {
	const down = "redis://127.0.0.1:1/0" // nothing listens on port 1

	t.Run("redis-acl", func(t *testing.T) {
		cc := newTestServer(t, Config{RedisURI: down, Auth: AuthRedisACL})
		res, err := healthpb.NewHealthClient(cc).Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("Check without a token: %v", err)
		}
		if res.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
			t.Errorf("status = %v with Redis down, want NOT_SERVING", res.GetStatus())
		}
		// The Flight service still needs a token.
		_, err = flight.NewFlightServiceClient(cc).GetFlightInfo(context.Background(), &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"x"}})
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("GetFlightInfo without a token: %v, want Unauthenticated", err)
		}
	})

	t.Run("no session cookie", func(t *testing.T) {
		cc := newTestServer(t, Config{RedisURI: down})
		var md metadata.MD
		if _, err := healthpb.NewHealthClient(cc).Check(context.Background(), &healthpb.HealthCheckRequest{}, grpc.Header(&md)); err != nil {
			t.Fatal(err)
		}
		if c := md.Get("set-cookie"); len(c) != 0 {
			t.Errorf("a health check got a cookie: %q", c)
		}
		actions, err := flight.NewFlightServiceClient(cc).ListActions(context.Background(), &flight.Empty{})
		if err != nil {
			t.Fatal(err)
		}
		if md, err = actions.Header(); err != nil {
			t.Fatal(err)
		}
		if c := md.Get("set-cookie"); len(c) == 0 {
			t.Error("ListActions got no session cookie")
		}
	})

	t.Run("unknown service", func(t *testing.T) {
		cc := newTestServer(t, Config{RedisURI: down})
		_, err := healthpb.NewHealthClient(cc).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "nope"})
		if status.Code(err) != codes.NotFound {
			t.Errorf("Check(nope): %v, want NotFound", err)
		}
	})
}

// A client watching the health status doesn't hold Shutdown until its
// timeout.
func TestHealthWatchDoesNotHoldShutdown(t *testing.T) {
	srv, lis := startServer(t, Config{RedisURI: "redis://127.0.0.1:1/0", ShutdownTimeout: 10 * time.Second})
	cc := dialTestServer(t, lis)
	w, err := healthpb.NewHealthClient(cc).Watch(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Recv(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	srv.Shutdown()
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Shutdown took %v with a Watch open", d)
	}
	for {
		if _, err := w.Recv(); err != nil {
			break // the Watch ended
		}
	}
}
