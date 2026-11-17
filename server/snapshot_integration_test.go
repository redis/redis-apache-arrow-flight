package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/redis-apache-arrow-flight/internal/integrationtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// These tests use the real native driver and secured Software backend. The
// existing snapshot/security unit tests deliberately use native test doubles.
func liveSnapshotGateway(t *testing.T, idle time.Duration, options ...map[string]string) (*Server, string, *x509.CertPool) {
	t.Helper()
	cert, key, _, roots := securityCertificate(t)
	cfg := Config{RedisURI: redisURI(t), Auth: AuthRedisACL, TLSCertFile: cert, TLSKeyFile: key, SessionIdleTimeout: idle, StreamStallTimeout: time.Second}
	if len(options) > 0 {
		cfg.ADBCOptions = options[0]
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		srv.Shutdown()
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Shutdown)
	return srv, lis.Addr().String(), roots
}

func liveSnapshotClient(t *testing.T, address string, roots *x509.CertPool, username, password string) (*flightsql.Client, context.Context) {
	t.Helper()
	client, err := flight.NewClientWithMiddlewareCtx(context.Background(), address, nil, []flight.ClientMiddleware{flight.CreateClientMiddleware(flight.NewCookieMiddleware())}, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	authed, err := client.AuthenticateBasicToken(ctx, username, password)
	if err != nil {
		t.Fatal("Flight authentication failed")
	}
	return &flightsql.Client{Client: client, Alloc: memory.DefaultAllocator}, authed
}

func acquireLiveSnapshot(t *testing.T, c *flightsql.Client, ctx context.Context) (context.Context, readSnapshot, string) {
	t.Helper()
	var headers metadata.MD
	result, err := c.SetSessionOptions(ctx, snapshotCommand("acquire"), grpc.Header(&headers))
	if err != nil || len(result.Errors) > 0 {
		t.Fatal("real snapshot acquisition failed", err)
	}
	opts, err := c.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var view readSnapshot
	if err := json.Unmarshal([]byte(opts.SessionOptions[snapshotStatusOption].GetStringValue()), &view); err != nil || view.State != "active" {
		t.Fatal("snapshot is not active", err)
	}
	return metadata.AppendToOutgoingContext(ctx, snapshotHeader, view.Handle), view, sessionToken(headers)
}

func nativeSnapshotPins(t *testing.T, admin goredis.UniversalClient) map[string]bool {
	t.Helper()
	fields, err := admin.HKeys(context.Background(), "adbc:{meta}:snapshot:catalog").Result()
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]bool{}
	for _, field := range fields {
		if strings.HasPrefix(field, "p:") {
			pins[field] = true
		}
	}
	return pins
}
func newNativeSnapshotPin(t *testing.T, admin goredis.UniversalClient, before map[string]bool) string {
	t.Helper()
	var added []string
	for pin := range nativeSnapshotPins(t, admin) {
		if !before[pin] {
			added = append(added, pin)
		}
	}
	if len(added) != 1 {
		t.Fatalf("expected exactly one dedicated native snapshot pin, got %d", len(added))
	}
	return added[0]
}
func awaitNativeSnapshotRelease(t *testing.T, admin goredis.UniversalClient, pin string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		exists, err := admin.HExists(context.Background(), "adbc:{meta}:snapshot:catalog", pin).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("native snapshot pin was not released")
		}
	}
}
func requireSnapshotReadFailure(t *testing.T, c *flightsql.Client, ctx context.Context) {
	t.Helper()
	for range 2 {
		info, err := c.Execute(ctx, "SELECT 1")
		if err == nil {
			r, e := c.DoGet(ctx, info.Endpoint[0].Ticket)
			if e == nil {
				for r.Next() {
				}
				e = r.Err()
				r.Release()
			}
			err = e
		}
		if err == nil {
			t.Fatal("lost snapshot silently fell back to a live read")
		}
	}
}

func TestIntegrationSnapshotLeaseLoss(t *testing.T) {
	uri := redisURI(t)
	admin := adminClient(t, uri)
	_, address, roots := liveSnapshotGateway(t, time.Minute)
	u, _ := url.Parse(uri)
	password, _ := u.User.Password()
	c, ctx := liveSnapshotClient(t, address, roots, u.User.Username(), password)
	before := nativeSnapshotPins(t, admin)
	bound, _, _ := acquireLiveSnapshot(t, c, ctx)
	pin := newNativeSnapshotPin(t, admin, before)
	// Deliberate fault injection, restricted to this test's newly acquired pin.
	if err := admin.HDel(ctx, "adbc:{meta}:snapshot:catalog", pin).Err(); err != nil {
		t.Fatal(err)
	}
	requireSnapshotReadFailure(t, c, bound)
	opts, err := c.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var view readSnapshot
	if err := json.Unmarshal([]byte(opts.SessionOptions[snapshotStatusOption].GetStringValue()), &view); err != nil || view.State != "failed" {
		t.Fatal("lease loss did not terminate the attempt", err)
	}
}

func TestIntegrationSnapshotDisconnectAndIdleExpiry(t *testing.T) {
	uri := redisURI(t)
	admin := adminClient(t, uri)
	_, address, roots := liveSnapshotGateway(t, 200*time.Millisecond)
	u, _ := url.Parse(uri)
	password, _ := u.User.Password()
	c, ctx := liveSnapshotClient(t, address, roots, u.User.Username(), password)
	before := nativeSnapshotPins(t, admin)
	bound, _, cookie := acquireLiveSnapshot(t, c, ctx)
	pin := newNativeSnapshotPin(t, admin, before)
	if cookie == "" {
		t.Fatal("no owning session cookie")
	}
	// Close the transport without CloseSession, as after abrupt process death.
	c.Client.Close()
	awaitNativeSnapshotRelease(t, admin, pin)
	replacement, replacementCtx := liveSnapshotClient(t, address, roots, u.User.Username(), password)
	handle, _ := metadata.FromOutgoingContext(bound)
	lost := metadata.AppendToOutgoingContext(withSession(replacementCtx, cookie), snapshotHeader, handle.Get(snapshotHeader)[0])
	requireSnapshotReadFailure(t, replacement, lost)
}

func TestIntegrationSnapshotIndependentPrincipal(t *testing.T) {
	uri := redisURI(t)
	admin := adminClient(t, uri)
	_, address, roots := liveSnapshotGateway(t, time.Minute)
	u, _ := url.Parse(uri)
	password, _ := u.User.Password()
	otherName, otherPassword := uniqueName("it_flight_snapshot_principal"), "snapshot-test-password"
	other := integrationtest.NewACLUser(t, otherName, "on", ">"+otherPassword, "+@all", "~*")
	owner, ownerCtx := liveSnapshotClient(t, address, roots, otherName, otherPassword)
	bound, _, cookie := acquireLiveSnapshot(t, owner, ownerCtx)
	if cookie == "" {
		t.Fatal("no session cookie")
	}
	stranger, strangerCtx := liveSnapshotClient(t, address, roots, u.User.Username(), password)
	md, _ := metadata.FromOutgoingContext(bound)
	stolen := metadata.AppendToOutgoingContext(withSession(strangerCtx, cookie), snapshotHeader, md.Get(snapshotHeader)[0])
	requireSnapshotReadFailure(t, stranger, stolen)
	// Revocation is performed through Software's supported management API. It
	// invalidates an already acquired native view, rather than just a new login.
	other.Set("-get", "-ft.search", "-ft.aggregate", "-hget", "-hmget", "-hgetall")
	requireSnapshotReadFailure(t, owner, bound)
	// Administrative cleanup must not depend on the revoked user's permissions.
	// Native lease/idle expiry remains the fallback if connection close is denied.
	_ = admin
}

func TestIntegrationSnapshotSQLGrantRevocation(t *testing.T) {
	uri := redisURI(t)
	name, password := uniqueName("it_flight_snapshot_grant"), "snapshot-grant-test-password"
	user := integrationtest.NewACLUser(t, name, "on", ">"+password, "+@all", "~*")
	rbac := user.SnapshotRBACOptions()
	_, address, roots := liveSnapshotGateway(t, time.Minute, map[string]string{"adbc.redis.rbac": rbac})
	u, _ := url.Parse(uri)
	secret, _ := u.User.Password()
	admin, adminCtx := liveSnapshotClient(t, address, roots, u.User.Username(), secret)
	table := uniqueName("it_flight_snapshot_grants")
	update(t, adminCtx, admin, "CREATE TABLE "+table+"(id BIGINT)")
	defer func() { _, _ = admin.ExecuteUpdate(context.WithoutCancel(adminCtx), "DROP TABLE IF EXISTS "+table) }()
	update(t, adminCtx, admin, "INSERT INTO "+table+" VALUES(42)")
	update(t, adminCtx, admin, "GRANT SELECT ON "+table+" TO "+name)
	probe, probeCtx := liveSnapshotClient(t, address, roots, name, password)
	statement := "SELECT id FROM " + table
	// Management grants propagate asynchronously. Only reads are polled; no SQL
	// write is replayed. Do not start the snapshot until the grant is effective.
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		info, err := probe.Execute(probeCtx, statement)
		if err == nil {
			fetch(t, probeCtx, probe, info)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SQL SELECT grant did not propagate")
		}
	}
	reader, readerCtx := liveSnapshotClient(t, address, roots, name, password)
	bound, _, _ := acquireLiveSnapshot(t, reader, readerCtx)
	wantRows(t, query(t, bound, reader, statement), "42")
	update(t, adminCtx, admin, "REVOKE SELECT ON "+table+" FROM "+name)
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		info, err := probe.Execute(probeCtx, statement)
		if err == nil {
			r, e := probe.DoGet(probeCtx, info.Endpoint[0].Ticket)
			if e == nil {
				for r.Next() {
				}
				e = r.Err()
				r.Release()
			}
			err = e
		}
		if err != nil {
			if status.Code(err) != codes.PermissionDenied {
				t.Fatal("unexpected error while observing grant revocation", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SQL SELECT revocation did not propagate")
		}
	}
	for attempt := range 2 {
		info, err := reader.Execute(bound, statement)
		if err == nil {
			r, e := reader.DoGet(bound, info.Endpoint[0].Ticket)
			if e == nil {
				for r.Next() {
				}
				e = r.Err()
				r.Release()
			}
			err = e
		}
		if err == nil {
			t.Fatal("retained snapshot bypassed revoked SQL SELECT grant")
		}
		if attempt == 0 && status.Code(err) != codes.PermissionDenied {
			t.Fatal("snapshot did not surface the revoked SELECT grant", err)
		}
	}
}

func TestIntegrationSnapshotReaderCleanup(t *testing.T) {
	uri := redisURI(t)
	admin := adminClient(t, uri)
	setup := newClients(t, Config{RedisURI: uri}).sql
	table := uniqueName("it_flight_snapshot_cancel")
	dropTable(t, setup, table)
	ingestRows(t, setup, table, streamRows, 2000)
	_, address, roots := liveSnapshotGateway(t, time.Minute)
	u, _ := url.Parse(uri)
	password, _ := u.User.Password()
	for _, mode := range []string{"pending_cancel", "active_cancel", "early_release"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx := liveSnapshotClient(t, address, roots, u.User.Username(), password)
			before := nativeSnapshotPins(t, admin)
			bound, _, _ := acquireLiveSnapshot(t, c, ctx)
			pin := newNativeSnapshotPin(t, admin, before)
			info, err := c.Execute(bound, "SELECT id,label FROM "+table)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "pending_cancel" {
				result, err := c.CancelFlightInfo(bound, &flight.CancelFlightInfoRequest{Info: info})
				if err != nil || result.Status != flight.CancelStatusCancelled {
					t.Fatal("pending cancellation failed", err)
				}
				r, err := c.DoGet(bound, info.Endpoint[0].Ticket)
				if err == nil {
					for r.Next() {
					}
					err = r.Err()
					r.Release()
				}
				if err == nil {
					t.Fatal("cancelled snapshot ticket remained readable")
				}
				if _, err := c.SetSessionOptions(bound, snapshotCommand("release")); err != nil {
					t.Fatal(err)
				}
			} else {
				active, cancel := context.WithCancel(bound)
				defer cancel()
				r, err := c.DoGet(active, info.Endpoint[0].Ticket)
				if err != nil {
					t.Fatal(err)
				}
				if !r.Next() {
					err := r.Err()
					r.Release()
					t.Fatal("no first batch", err)
				}
				if r.RecordBatch().NumRows() >= streamRows {
					r.Release()
					t.Fatal("fixture was not an unread multi-batch stream")
				}
				if mode == "active_cancel" {
					cancel()
				}
				r.Release()
				// Releasing the Go reader alone does not close the transport. The gateway
				// stall timeout must still terminate the abandoned native reader/pin.
			}
			awaitNativeSnapshotRelease(t, admin, pin)
			requireSnapshotReadFailure(t, c, bound)
		})
	}
}
