package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// cookieClient connects to lis with arrow-go's cookie middleware, which
// keeps the session cookie as JDBC does.
func cookieClient(t *testing.T, lis *bufconn.Listener) *flightsql.Client {
	t.Helper()
	fc, err := flight.NewClientWithMiddlewareCtx(context.Background(), "passthrough:///bufconn", nil,
		[]flight.ClientMiddleware{flight.CreateClientMiddleware(flight.NewCookieMiddleware())}, testDialOptions(lis)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fc.Close() })
	return &flightsql.Client{Client: fc, Alloc: memory.DefaultAllocator}
}

func plainClient(t *testing.T, lis *bufconn.Listener) *flightsql.Client {
	return &flightsql.Client{Client: flight.NewClientFromConn(dialTestServer(t, lis), nil), Alloc: memory.DefaultAllocator}
}

// sessionToken returns the session cookie a response set.
func sessionToken(md metadata.MD) string {
	for _, v := range md.Get("set-cookie") {
		if c, err := http.ParseSetCookie(v); err == nil && c.Name == sessionCookie && c.Value != "" {
			return c.Value
		}
	}
	return ""
}

// withSession sends token as the session cookie, by hand.
func withSession(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "cookie", sessionCookie+"="+token)
}

// newSessionToken asks the server for a session cookie.
func askSessionToken(t *testing.T, ctx context.Context, c *flightsql.Client) string {
	t.Helper()
	var md metadata.MD
	if _, err := c.Execute(ctx, "SELECT 1", grpc.Header(&md)); err != nil {
		t.Fatal(err)
	}
	token := sessionToken(md)
	if token == "" {
		t.Fatalf("no session cookie in %v", md)
	}
	return token
}

func failsMissing(t *testing.T, err error, what string) {
	t.Helper()
	if code := status.Code(err); code != codes.NotFound && code != codes.InvalidArgument {
		t.Errorf("%s: err = %v, want NotFound or InvalidArgument", what, err)
	}
}

func stringOption(v string) *flight.SessionOptionValue {
	o, _ := flight.NewSessionOptionValue(v)
	return &o
}

func TestIntegrationSessions(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	lis := startTestServer(t, Config{RedisURI: uri})
	plain := plainClient(t, lis)

	schema, tbl := uniqueName("it_flight_sess_s"), uniqueName("it_flight_sess_t")
	dropTable(t, plain, schema+"."+tbl)
	update(t, ctx, plain, "CREATE TABLE "+schema+"."+tbl+" (id BIGINT)")
	update(t, ctx, plain, "INSERT INTO "+schema+"."+tbl+" VALUES (1), (2)")

	t.Run("SET lasts with cookies", func(t *testing.T) {
		c := cookieClient(t, lis)
		query(t, ctx, c, "SELECT 1") // gets the cookie
		update(t, ctx, c, "SET search_path TO "+schema)
		wantRows(t, query(t, ctx, c, "SELECT COUNT(*) FROM "+tbl), "2")

		// Without cookies, every request is on its own.
		update(t, ctx, plain, "SET search_path TO "+schema)
		_, err := plain.Execute(ctx, "SELECT COUNT(*) FROM "+tbl)
		failsMissing(t, err, "unqualified SELECT after SET, without cookies")
	})

	t.Run("temporary tables last", func(t *testing.T) {
		c := cookieClient(t, lis)
		query(t, ctx, c, "SELECT 1")
		name := uniqueName("it_flight_sess_tmp")
		update(t, ctx, c, "CREATE TEMP TABLE "+name+" AS SELECT 7 AS a")
		wantRows(t, query(t, ctx, c, "SELECT a FROM "+name), "7")

		_, err := plain.Execute(ctx, "SELECT a FROM "+name)
		failsMissing(t, err, "another client reading the temp table")
	})

	t.Run("temporary ingest", func(t *testing.T) {
		name := uniqueName("it_flight_sess_ingest")
		ingest := func(c *flightsql.Client) (int64, error) {
			rec := int64Record(t, []string{"n"}, []int64{1, 2, 3})
			rdr, _ := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
			rec.Release()
			defer rdr.Release()
			return c.ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{Table: name, Temporary: true,
				TableDefinitionOptions: &flightsql.TableDefinitionOptions{
					IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
					IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionFail}})
		}
		if _, err := ingest(plain); status.Code(err) != codes.InvalidArgument {
			t.Errorf("temporary ingest without a session: err = %v, want InvalidArgument", err)
		}
		c := cookieClient(t, lis)
		query(t, ctx, c, "SELECT 1")
		if n, err := ingest(c); err != nil || n != 3 {
			t.Fatalf("temporary ingest in a session = %d, %v", n, err)
		}
		wantRows(t, query(t, ctx, c, "SELECT SUM(n) FROM "+name), "6")
	})

	t.Run("session options", func(t *testing.T) {
		c := cookieClient(t, lis)
		res, err := c.Client.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{
			"schema": stringOption(schema), "timezone": stringOption("Asia/Tokyo"),
			"colour": stringOption("blue"),
		}})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Errors) != 1 || res.Errors["colour"].GetValue() != flight.SetSessionOptionsResultErrorInvalidName {
			t.Errorf("SetSessionOptions errors = %v, want colour: InvalidName", res.Errors)
		}
		wantRows(t, query(t, ctx, c, "SELECT COUNT(*) FROM "+tbl), "2")
		wantRows(t, query(t, ctx, c, "SHOW TimeZone"), "Asia/Tokyo")

		got, err := c.Client.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]string{"catalog": "redis", "schema": schema, "timezone": "Asia/Tokyo"} {
			if v := got.SessionOptions[name].GetStringValue(); v != want {
				t.Errorf("GetSessionOptions %s = %q, want %q", name, v, want)
			}
		}

		res, err = c.Client.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{
			"schema": stringOption(uniqueName("it_flight_no_schema")), "catalog": stringOption("elsewhere"),
		}})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"schema", "catalog"} {
			if res.Errors[name].GetValue() != flight.SetSessionOptionsResultErrorInvalidValue {
				t.Errorf("setting a missing %s: errors = %v, want InvalidValue", name, res.Errors)
			}
		}

		// Erasing goes back to the defaults.
		res, err = c.Client.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{
			"timezone": {}, "schema": {},
		}})
		if err != nil || len(res.Errors) != 0 {
			t.Fatalf("erasing timezone and schema = %v, %v", res, err)
		}
		wantRows(t, query(t, ctx, c, "SHOW TimeZone"), "UTC")
		_, err = c.Execute(ctx, "SELECT COUNT(*) FROM "+tbl)
		failsMissing(t, err, "unqualified SELECT after erasing schema")

		// Without a session, the defaults.
		got, err = plain.Client.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
		if err != nil || got.SessionOptions["schema"].GetStringValue() != "public" {
			t.Errorf("GetSessionOptions without a session = %v, %v; want schema public", got, err)
		}
	})

	t.Run("CloseSession", func(t *testing.T) {
		token := askSessionToken(t, ctx, plain)
		sctx := withSession(ctx, token)
		name := uniqueName("it_flight_sess_close")
		update(t, sctx, plain, "CREATE TEMP TABLE "+name+" AS SELECT 1 AS a")
		wantRows(t, query(t, sctx, plain, "SELECT a FROM "+name), "1")
		res, err := plain.Client.CloseSession(sctx, &flight.CloseSessionRequest{})
		if err != nil || res.GetStatus() != flight.CloseSessionResultClosed {
			t.Fatalf("CloseSession = %v, %v", res, err)
		}
		// The same cookie now starts a new, empty session.
		_, err = plain.Execute(sctx, "SELECT a FROM "+name)
		failsMissing(t, err, "the temp table after CloseSession")
	})

	t.Run("idle sessions close", func(t *testing.T) {
		lis := startTestServer(t, Config{RedisURI: uri, SessionIdleTimeout: 200 * time.Millisecond})
		c := plainClient(t, lis)
		sctx := withSession(ctx, askSessionToken(t, ctx, c))
		name := uniqueName("it_flight_sess_idle")
		update(t, sctx, c, "CREATE TEMP TABLE "+name+" AS SELECT 1 AS a")
		wantRows(t, query(t, sctx, c, "SELECT a FROM "+name), "1")
		time.Sleep(700 * time.Millisecond)
		_, err := c.Execute(sctx, "SELECT a FROM "+name)
		failsMissing(t, err, "the temp table of an idle session")
	})

	t.Run("sessions off", func(t *testing.T) {
		lis := startTestServer(t, Config{RedisURI: uri, DisableSessions: true})
		c := plainClient(t, lis)
		var md metadata.MD
		if _, err := c.Execute(ctx, "SELECT 1", grpc.Header(&md)); err != nil {
			t.Fatal(err)
		}
		if token := sessionToken(md); token != "" {
			t.Errorf("a server without sessions set a session cookie")
		}
		sctx := withSession(ctx, newSessionToken())
		update(t, sctx, c, "SET search_path TO "+schema)
		_, err := c.Execute(sctx, "SELECT COUNT(*) FROM "+tbl)
		failsMissing(t, err, "unqualified SELECT after SET, sessions off")
		if _, err := c.Client.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{}); status.Code(err) != codes.Unimplemented {
			t.Errorf("SetSessionOptions with sessions off: err = %v, want Unimplemented", err)
		}
	})
}

// A session belongs to the Redis user that made it: another user's
// request with its cookie gets a session of its own.
func TestIntegrationSessionOwner(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	admin := adminClient(t, uri)
	alice, bob := uniqueName("it_flight_alice"), uniqueName("it_flight_bob")
	for _, user := range []string{alice, bob} {
		if err := eachNode(admin, func(ctx context.Context, c *goredis.Client) error {
			return c.Do(ctx, "ACL", "SETUSER", user, "on", ">pw", "resetchannels", "~*", "+@all").Err()
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = eachNode(admin, func(ctx context.Context, c *goredis.Client) error {
				return c.Do(ctx, "ACL", "DELUSER", user).Err()
			})
		})
	}
	lis := startTestServer(t, Config{RedisURI: uri, Auth: AuthRedisACL, TokenSecret: "sessions"})
	c := plainClient(t, lis)
	actx, err := c.Client.AuthenticateBasicToken(ctx, alice, "pw")
	if err != nil {
		t.Fatal(err)
	}
	bctx, err := c.Client.AuthenticateBasicToken(ctx, bob, "pw")
	if err != nil {
		t.Fatal(err)
	}
	token := askSessionToken(t, actx, c)
	name := uniqueName("it_flight_sess_owner")
	update(t, withSession(actx, token), c, "CREATE TEMP TABLE "+name+" AS SELECT 1 AS a")
	wantRows(t, query(t, withSession(actx, token), c, "SELECT a FROM "+name), "1")

	var md metadata.MD
	_, err = c.Execute(withSession(bctx, token), "SELECT a FROM "+name, grpc.Header(&md))
	failsMissing(t, err, "another user's request with the session cookie")
	if got := sessionToken(md); got == "" || got == token {
		t.Errorf("bob got set-cookie %q, want a session of his own", got)
	}
	if !strings.Contains(alice, "alice") {
		t.Fatal("unreachable")
	}
}

// A session's scans stream on a pooled connection with only the settings
// in scanOptions carried over: of the driver's SET parameters, only
// search_path and TimeZone change what a scan returns. This fails when the
// driver gains a parameter, so that someone decides whether it does too.
func TestIntegrationSessionSettings(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	// Each is accepted with one value only, or only shown.
	known := map[string]bool{"search_path": true, "timezone": true, "client_encoding": true,
		"application_name": true, "standard_conforming_strings": true, "statement_timeout": true,
		"lock_timeout": true, "idle_in_transaction_session_timeout": true, "extra_float_digits": true,
		"datestyle": true, "intervalstyle": true}
	rows := query(t, context.Background(), c, "SHOW ALL")
	if len(rows) == 0 {
		t.Fatal("SHOW ALL returned nothing")
	}
	for _, row := range rows {
		name, _, _ := strings.Cut(row, "|")
		if !known[strings.ToLower(name)] {
			t.Errorf("the driver has a new setting, %q: if it can change what a scan returns, add it to scanOptions "+
				"(and sessionOptions); then add it here", name)
		}
	}
}
