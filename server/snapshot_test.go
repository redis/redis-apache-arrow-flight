package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type snapshotTestConn struct {
	adbc.ConnectionWithContext
	adbc.GetSetOptionsWithContext
	view                 readSnapshot
	failGet, failAcquire error
	closed               atomic.Int32
	retention            string
	queries              int
}

func (c *snapshotTestConn) GetOption(_ context.Context, key string) (string, error) {
	if c.failGet != nil {
		return "", c.failGet
	}
	if key != redis.OptionStringReadSnapshotStatus {
		return "", errors.New("unexpected option")
	}
	raw, err := json.Marshal(c.view)
	return string(raw), err
}
func (c *snapshotTestConn) SetOption(ctx context.Context, key, value string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch key {
	case redis.OptionIntReadSnapshotRetention:
		c.retention = value
	case redis.OptionStringReadSnapshotAction:
		if c.failAcquire != nil {
			return c.failAcquire
		}
		if value != "acquire" {
			return errors.New("unexpected action")
		}
		c.view = readSnapshot{State: "active", Generation: "native-diagnostic-only", Deadline: time.Now().Add(2 * time.Minute).UTC()}
	default:
		return errors.New("unexpected option")
	}
	return nil
}
func (c *snapshotTestConn) Close(context.Context) error { c.closed.Add(1); return nil }
func (c *snapshotTestConn) NewStatement(context.Context) (adbc.StatementWithContext, error) {
	c.queries++
	return &snapshotTestStmt{}, nil
}

type snapshotTestStmt struct{ adbc.StatementWithContext }

func (*snapshotTestStmt) Close(context.Context) error                     { return nil }
func (*snapshotTestStmt) SetOption(context.Context, string, string) error { return nil }
func (*snapshotTestStmt) SetSqlQuery(context.Context, string) error       { return nil }
func (*snapshotTestStmt) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 1, 2}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return rdr, -1, err
}

// Exercise the real Arrow auth context adapter, without implementing or replacing
// the enterprise authentication boundary.
type snapshotTestAuth struct{ id identity }

func (a snapshotTestAuth) Validate(string, string) (string, error) { return "", nil }
func (a snapshotTestAuth) IsValid(string) (any, error)             { return a.id, nil }

func snapshotHarness(t *testing.T) (*sqlServer, *tenant, *session, *snapshotTestConn, context.Context) {
	t.Helper()
	id := identity{Username: "snapshot-owner", Password: "test-only"}
	cfg := &Config{Auth: AuthRedisACL, TLSCertFile: "test", TLSKeyFile: "test", StreamStallTimeout: time.Second}
	cfg.setDefaults()
	conn := &snapshotTestConn{}
	tn := &tenant{streams: make(chan struct{}, 2)}
	cache := &tenantCache{cfg: cfg, tenants: map[string]*tenant{id.key(): tn}, creating: map[string]chan struct{}{}}
	st := &sessionStore{tenants: cache, idle: time.Hour, log: slog.New(slog.NewTextHandler(io.Discard, nil)), sessions: map[string]*session{}}
	sess := st.get(newSessionToken(), id.key(), true)
	// Install the already-open dedicated connection. A drop must never reopen it.
	sess.conn, sess.release = conn, func() {}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer test"))
	middleware := flight.CreateServerBasicAuthMiddleware(snapshotTestAuth{id})
	_, err := middleware.Unary(ctx, nil, &grpc.UnaryServerInfo{}, func(authed context.Context, _ any) (any, error) { ctx = authed; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, sessionCtxKey{}, &sessionInfo{store: st, owner: id.key(), token: sess.token, sess: sess})
	t.Cleanup(func() { st.remove(sess) })
	return &sqlServer{tenants: cache}, tn, sess, conn, ctx
}
func snapshotCommand(action string) *flight.SetSessionOptionsRequest {
	return &flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{snapshotOption: stringOption(`{"action":"` + action + `"}`)}}
}
func acquireSnapshot(t *testing.T, srv *sqlServer, sess *session, ctx context.Context) context.Context {
	t.Helper()
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("acquire")); err != nil {
		t.Fatal(err)
	}
	return metadata.NewIncomingContext(ctx, metadata.Pairs(snapshotHeader, sess.snapshot.Handle))
}

func TestReadSnapshotAffinityAndRelease(t *testing.T) {
	srv, tn, sess, conn, ctx := snapshotHarness(t)
	ctx = acquireSnapshot(t, srv, sess, ctx)
	if conn.retention != "900000" {
		t.Fatalf("retention=%q", conn.retention)
	}
	if sess.snapshot.Handle == sess.snapshot.Generation {
		t.Fatal("diagnostic generation became an attachment token")
	}
	for range 4 {
		if err := tn.withConn(ctx, "", func(c adbc.ConnectionWithContext) error {
			if c != conn {
				t.Fatal("used unrelated connection")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var h statementHandle
	if err := bindSnapshotHandle(ctx, &h); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("release")); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("release")); err != nil {
		t.Fatal(err)
	}
	if conn.closed.Load() != 1 {
		t.Fatalf("connection closed %d times", conn.closed.Load())
	}
	if err := tn.withConn(ctx, "", func(adbc.ConnectionWithContext) error { t.Fatal("read after release"); return nil }); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if err := validateSnapshotHandle(context.Background(), h); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("acquire")); err == nil {
		t.Fatal("reacquired a different view in the same session")
	}
}

func TestReadSnapshotGuard(t *testing.T) {
	srv, _, sess, _, ctx := snapshotHarness(t)
	bound := acquireSnapshot(t, srv, sess, ctx)
	if err := snapshotGuard(bound); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]context.Context{
		"missing header":   ctx,
		"missing cookie":   metadata.NewIncomingContext(context.Background(), metadata.Pairs(snapshotHeader, sess.snapshot.Handle)),
		"wrong handle":     metadata.NewIncomingContext(ctx, metadata.Pairs(snapshotHeader, newSessionToken())),
		"duplicate header": metadata.NewIncomingContext(ctx, metadata.Pairs(snapshotHeader, sess.snapshot.Handle, snapshotHeader, sess.snapshot.Handle)),
	} {
		t.Run(name, func(t *testing.T) {
			if snapshotGuard(bad) == nil {
				t.Fatal("accepted unrelated snapshot request")
			}
		})
	}
	other := sess.store.get(newSessionToken(), "another-owner", true)
	bad := context.WithValue(bound, sessionCtxKey{}, &sessionInfo{sess: other})
	if snapshotGuard(bad) == nil {
		t.Fatal("accepted another identity's handle")
	}
	// The real cookie store never gives another principal this session.
	if got, err := sess.store.resume(sess.token, "another-owner"); got != nil || err != nil {
		t.Fatalf("resume=%v %v", got, err)
	}
}

func TestReadSnapshotLossIsTerminal(t *testing.T) {
	for _, failure := range []string{"expiry", "lease loss", "native unbound", "changed generation", "metadata failure"} {
		t.Run(failure, func(t *testing.T) {
			srv, tn, sess, conn, ctx := snapshotHarness(t)
			ctx = acquireSnapshot(t, srv, sess, ctx)
			switch failure {
			case "expiry":
				sess.snapshot.Deadline = time.Now().Add(-time.Second)
			case "lease loss":
				conn.failGet = errors.New("authority unavailable")
			case "native unbound":
				conn.view.State = "unbound"
			case "changed generation":
				conn.view.Generation = "newer"
			case "metadata failure":
				if err := tn.withConn(ctx, "", func(adbc.ConnectionWithContext) error { return adbc.Error{Code: adbc.StatusUnauthorized} }); err == nil {
					t.Fatal("ignored metadata failure")
				}
			}
			if err := snapshotGuard(ctx); err == nil {
				t.Fatal("lost view accepted")
			}
			conn.failGet = nil
			if err := tn.withConn(ctx, "", func(adbc.ConnectionWithContext) error { t.Fatal("fell back to live reads"); return nil }); err == nil {
				t.Fatal("lost binding revived")
			}
			if conn.closed.Load() != 1 {
				t.Fatalf("connection not released: %d", conn.closed.Load())
			}
		})
	}
}

func TestReadSnapshotAcquireFailure(t *testing.T) {
	srv, tn, sess, conn, ctx := snapshotHarness(t)
	conn.failAcquire = adbc.Error{Code: adbc.StatusNotImplemented}
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("acquire")); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	if conn.closed.Load() != 1 {
		t.Fatal("failed acquire leaked connection")
	}
	if err := tn.withConn(ctx, "", func(adbc.ConnectionWithContext) error { t.Fatal("read live after failed acquire"); return nil }); err == nil {
		t.Fatal("failed acquire allowed fallback")
	}
	if sess.snapshot.State != "failed" {
		t.Fatal("acquire not terminal")
	}
}

func TestReadSnapshotStreamCleanup(t *testing.T) {
	for _, failure := range []string{"success", "cancel", "stall", "expiry"} {
		t.Run(failure, func(t *testing.T) {
			srv, tn, sess, conn, ctx := snapshotHarness(t)
			srv.tenants.cfg.StreamStallTimeout = 20 * time.Millisecond
			ctx = acquireSnapshot(t, srv, sess, ctx)
			if failure == "expiry" {
				deadline := time.Now().Add(100 * time.Millisecond).UTC()
				sess.snapshot.Deadline, conn.view.Deadline = deadline, deadline
				srv.tenants.cfg.StreamStallTimeout = time.Second
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			_, ch, err := srv.snapshotQuery(ctx, tn, sess, statementHandle{Query: "SELECT v FROM t"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := srv.SetSessionOptions(ctx, snapshotCommand("release")); status.Code(err) != codes.ResourceExhausted {
				t.Fatal("release raced reader", err)
			}
			if failure == "cancel" {
				cancel()
			}
			if failure == "stall" {
				time.Sleep(50 * time.Millisecond)
			}
			if failure == "expiry" {
				time.Sleep(150 * time.Millisecond)
			}
			sawErr := false
			for chunk := range ch {
				if chunk.Data != nil {
					chunk.Data.Release()
				}
				if chunk.Err != nil {
					sawErr = true
				}
			}
			sess.mu.Lock()
			state := sess.snapshot.State
			sess.mu.Unlock()
			if len(tn.streams) != 0 {
				t.Fatal("stream token leaked")
			}
			if failure == "success" {
				if state != "active" || conn.closed.Load() != 0 {
					t.Fatal("successful read lost binding")
				}
			} else if state != "failed" || conn.closed.Load() != 1 {
				t.Fatalf("failed stream retained connection: %s", state)
			}
			if (failure == "stall" || failure == "expiry") && !sawErr {
				t.Fatal("failed stream succeeded with missing rows")
			}
		})
	}
}

func TestReadSnapshotReaderChecksBufferedExpiry(t *testing.T) {
	srv, _, sess, _, ctx := snapshotHarness(t)
	ctx = acquireSnapshot(t, srv, sess, ctx)
	native, _, _ := new(snapshotTestStmt).ExecuteQuery(ctx)
	defer native.Release()
	rdr := &snapshotReader{RecordReader: native, ctx: ctx, sess: sess}
	sess.snapshot.Deadline = time.Now().Add(-time.Second)
	if rdr.Next() || rdr.Err() == nil {
		t.Fatal("buffered reader handed out expired data")
	}
}

func TestReadSnapshotUnsupportedDeployment(t *testing.T) {
	srv, _, _, conn, ctx := snapshotHarness(t)
	srv.tenants.cfg.Auth = AuthNone
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("acquire")); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	if conn.retention != "" {
		t.Fatal("native acquire on unauthenticated deployment")
	}
}

func TestReadSnapshotInvalidCommandHasNoEffects(t *testing.T) {
	for _, value := range []string{
		`{"action":"attach"}`, `{"action":"acquire","retention_ms":0}`,
		`{"action":"acquire","retention_ms":119999}`, `{"action":"acquire","retention_ms":3600001}`,
		`{"action":"acquire","generation":"external"}`, `{"action":"acquire"} {}`,
	} {
		t.Run(value, func(t *testing.T) {
			srv, _, sess, conn, ctx := snapshotHarness(t)
			req := &flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{snapshotOption: stringOption(value)}}
			if _, err := srv.SetSessionOptions(ctx, req); status.Code(err) != codes.InvalidArgument {
				t.Fatal(err)
			}
			if sess.snapshot != nil || conn.retention != "" {
				t.Fatal("invalid command changed native state")
			}
		})
	}
}

func TestReadSnapshotAcquireCancellationIsTerminal(t *testing.T) {
	srv, tn, sess, conn, ctx := snapshotHarness(t)
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := srv.SetSessionOptions(ctx, snapshotCommand("acquire")); err == nil {
		t.Fatal("acquisition ignored cancellation")
	}
	if conn.closed.Load() != 1 {
		t.Fatal("cancelled acquire leaked native connection")
	}
	if err := tn.withConn(context.WithoutCancel(ctx), "", func(adbc.ConnectionWithContext) error { t.Fatal("live fallback after cancellation"); return nil }); err == nil {
		t.Fatal("cancelled binding revived")
	}
	if sess.snapshot.State != "failed" {
		t.Fatal("cancelled acquire not terminal")
	}
}
