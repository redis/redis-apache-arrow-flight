package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestTokenSealer(t *testing.T) {
	s, err := newTokenSealer("secret")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	id := identity{Username: "alice", Password: "p@ss"}
	tok, err := s.seal(id, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(tok, "p@ss") || strings.Contains(tok, "alice") {
		t.Fatalf("token shows the credentials: %s", tok)
	}
	got, err := s.open(tok, now)
	if err != nil || got != id {
		t.Fatalf("open = %+v, %v; want %+v", got, err, id)
	}
	if _, err := s.open(tok, now.Add(time.Hour)); !errors.Is(err, errBadToken) {
		t.Errorf("expired token: err = %v", err)
	}
	other, _ := newTokenSealer("other secret")
	if _, err := other.open(tok, now); !errors.Is(err, errBadToken) {
		t.Errorf("token from another secret: err = %v", err)
	}
	same, _ := newTokenSealer("secret")
	if _, err := same.open(tok, now); err != nil {
		t.Errorf("token from a server with the same secret: %v", err)
	}
	tampered := []byte(tok)
	tampered[len(tampered)/2] ^= 1
	if _, err := s.open(string(tampered), now); !errors.Is(err, errBadToken) {
		t.Errorf("tampered token: err = %v", err)
	}
	if _, err := s.open("not base64!", now); !errors.Is(err, errBadToken) {
		t.Errorf("garbage token: err = %v", err)
	}
}

func TestChangesSession(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT 1":                                       false,
		"SELECT * FROM t WHERE a = $1":                   false,
		"UPDATE t SET a = 1 WHERE b = 2":                 false,
		"CREATE TABLE t (a INTEGER)":                     false,
		"CREATE VIEW v AS SELECT 1 AS a":                 false,
		"INSERT INTO t VALUES (1)":                       false,
		"SET search_path = other":                        true,
		"SET TIME ZONE 'America/New_York'":               true,
		"RESET ALL":                                      true,
		"BEGIN":                                          true,
		"SELECT 1; COMMIT":                               true,
		"CREATE TEMP TABLE t (a INTEGER)":                true,
		"CREATE TEMPORARY VIEW v AS SELECT 1 AS a":       true,
		"CREATE TEMP TABLE t AS SELECT 1 AS a":           true,
		"this is not SQL":                                false,
		"":                                               false,
		"SELECT 1; SET application_name = 'x'; SELECT 2": true,
		// pg_temp makes a temporary object without TEMP, in any case.
		"CREATE TABLE pg_temp.t (a INTEGER)":      true,
		"CREATE TABLE PG_TEMP.t (a INTEGER)":      true,
		"CREATE VIEW pg_temp.v AS SELECT 1 AS a":  true,
		"CREATE TABLE pg_temp.t AS SELECT 1 AS a": true,
		"CREATE TABLE public.t (a INTEGER)":       false,
		"CREATE TABLE pg_temp_x.t (a INTEGER)":    false,
	} {
		if got := changesSession(sql); got != want {
			t.Errorf("changesSession(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestStreamTokens(t *testing.T) {
	for _, tc := range []struct{ maxStreams, poolMaxOpen, want int }{
		{8, 32, 8},
		{8, 9, 8},
		{8, 8, 7},
		{8, 2, 1},
		{8, 1, 0}, // a pool of 1 never streams
	} {
		cfg := &Config{MaxStreams: tc.maxStreams, PoolMaxOpen: tc.poolMaxOpen}
		if got := streamTokens(cfg); got != tc.want {
			t.Errorf("streamTokens(MaxStreams %d, PoolMaxOpen %d) = %d, want %d",
				tc.maxStreams, tc.poolMaxOpen, got, tc.want)
		}
	}
}

func TestStreamTable(t *testing.T) {
	for sql, want := range map[string]string{
		"SELECT * FROM t": "public.t",
		"select id from Orders where id > 3 order by id": "public.orders",
		"SELECT id FROM Sales.Orders LIMIT 10":           "sales.orders",
		`SELECT id FROM "Mixed"`:                         "public.mixed",
		"SELECT o.id FROM t AS o WHERE o.id = $1":        "public.t",
		// The driver doesn't stream these.
		"SELECT 1":                                  "",
		"SELECT * FROM a JOIN b ON a.id = b.id":     "",
		"SELECT * FROM a, b":                        "",
		"WITH c AS (SELECT 1 AS a) SELECT * FROM c": "",
		"SELECT * FROM (SELECT id FROM t) AS d":     "",
		"SELECT id FROM a UNION SELECT id FROM b":   "",
		"SELECT * FROM t; SELECT * FROM t":          "",
		"INSERT INTO t VALUES (1)":                  "",
		"this is not SQL":                           "",
		"":                                          "",
	} {
		got, ok := streamTable(sql, "public")
		if ok != (want != "") || got != want {
			t.Errorf("streamTable(%q) = %q, %v; want %q", sql, got, ok, want)
		}
	}
	if got, _ := streamTable("SELECT * FROM t", "sales"); got != "sales.t" {
		t.Errorf("with default schema sales: %q", got)
	}
}

func TestTableStreams(t *testing.T) {
	ts := newTableStreams(2)
	if !ts.try("public.a") || !ts.try("public.a") {
		t.Fatal("refused a stream under the cap")
	}
	if ts.try("public.a") {
		t.Error("allowed a stream over the cap")
	}
	if !ts.try("public.b") {
		t.Error("another table's streams counted against public.a")
	}
	ts.end("public.a")
	if !ts.try("public.a") {
		t.Error("an ended stream wasn't freed")
	}
	ts.end("public.a")
	ts.end("public.a")
	ts.end("public.b")
	if len(ts.n) != 0 {
		t.Errorf("counts left after every stream ended: %v", ts.n)
	}
}

func TestCursorLimitStatus(t *testing.T) {
	// The driver wraps Redis's reply as an I/O error.
	err := adbc.Error{Code: adbc.StatusIO,
		Msg: "FT.AGGREGATE failed: INDEX_CURSOR_LIMIT of 128 has been reached for an index"}
	if got := toStatus(err); status.Code(got) != codes.ResourceExhausted {
		t.Errorf("toStatus = %v, want ResourceExhausted", got)
	}
	if !reusable("SELECT * FROM t", err) {
		t.Error("an index out of cursors should keep the connection")
	}
}

func TestReusable(t *testing.T) {
	stmtErr := adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "syntax error"}
	ioErr := adbc.Error{Code: adbc.StatusIO, Msg: "connection reset"}
	if !reusable("SELECT 1", nil) || !reusable("SELECT 1", stmtErr) {
		t.Error("a statement error should keep the connection")
	}
	if reusable("SELECT 1", ioErr) || reusable("SELECT 1", context.Canceled) {
		t.Error("an I/O error or cancellation should close the connection")
	}
	if reusable("SET search_path = x", nil) {
		t.Error("SET should close the connection")
	}
}

func TestConnectStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{errors.New("WRONGPASS invalid username-password pair or user is disabled."), codes.Unauthenticated},
		{errors.New("NOAUTH Authentication required."), codes.Unauthenticated},
		{errors.New("NOPERM User u has no permissions to run the 'ping' command"), codes.PermissionDenied},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), codes.Unavailable},
		{errors.New("i/o timeout"), codes.Unavailable},
	} {
		got := connectStatus(tc.err)
		if status.Code(got) != tc.want || !strings.Contains(status.Convert(got).Message(), tc.err.Error()) {
			t.Errorf("connectStatus(%q) = %v, want %v with the cause", tc.err, got, tc.want)
		}
	}
}

// fakeDB and fakeConn stand in for the driver; only what the pool calls is
// implemented.
type fakeDB struct {
	adbc.DatabaseWithContext
	conns []*fakeConn
}

func (d *fakeDB) Open(context.Context) (adbc.ConnectionWithContext, error) {
	c := &fakeConn{}
	d.conns = append(d.conns, c)
	return c, nil
}

type fakeConn struct {
	adbc.ConnectionWithContext
	closed bool
}

func (c *fakeConn) Close(context.Context) error {
	c.closed = true
	return nil
}

func TestWithConnReleasesOnPanic(t *testing.T) {
	db := &fakeDB{}
	tn := &tenant{pool: newConnPool(db, 1, 1)}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("withConn didn't pass the panic on")
			}
		}()
		_ = tn.withConn(context.Background(), "SELECT 1", func(adbc.ConnectionWithContext) error {
			panic("a driver bug")
		})
	}()
	if len(db.conns) != 1 || !db.conns[0].closed {
		t.Error("the connection of a call that panicked wasn't closed")
	}
	// The pool has one slot: it must be free again.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tn.withConn(ctx, "SELECT 1", func(adbc.ConnectionWithContext) error { return nil }); err != nil {
		t.Errorf("withConn after a panic: %v", err)
	}
}

func TestAcquireAfterPanicInOpen(t *testing.T) {
	cfg := Config{Logger: testLogger(t)}
	cfg.setDefaults()
	c := newTenantCache(&cfg)
	defer c.close()
	errDown := errors.New("redis is down")
	calls := 0
	c.open = func(context.Context, identity) (*tenant, error) {
		calls++
		if calls == 1 {
			panic("a bug while connecting")
		}
		return nil, errDown
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("acquire didn't pass the panic on")
			}
		}()
		_, _, _ = c.acquire(context.Background())
	}()
	// The next request must connect again, not wait for the one that panicked.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := c.acquire(ctx); !errors.Is(err, errDown) {
		t.Errorf("acquire after a panic: err = %v, want %v", err, errDown)
	}
	if calls != 2 {
		t.Errorf("open was called %d times, want 2", calls)
	}
}

func TestIngestMode(t *testing.T) {
	const (
		create   = flightsql.TableDefinitionOptionsTableNotExistOptionCreate
		failNE   = flightsql.TableDefinitionOptionsTableNotExistOptionFail
		failEx   = flightsql.TableDefinitionOptionsTableExistsOptionFail
		appendEx = flightsql.TableDefinitionOptionsTableExistsOptionAppend
		replace  = flightsql.TableDefinitionOptionsTableExistsOptionReplace
	)
	for _, tc := range []struct {
		opts      *flightsql.TableDefinitionOptions
		mode      string
		mustExist bool
		fails     bool
	}{
		{opts: nil, mode: adbc.OptionValueIngestModeCreate},
		{opts: &flightsql.TableDefinitionOptions{IfNotExist: create, IfExists: failEx}, mode: adbc.OptionValueIngestModeCreate},
		{opts: &flightsql.TableDefinitionOptions{IfNotExist: create, IfExists: appendEx}, mode: adbc.OptionValueIngestModeCreateAppend},
		{opts: &flightsql.TableDefinitionOptions{IfNotExist: create, IfExists: replace}, mode: adbc.OptionValueIngestModeReplace},
		{opts: &flightsql.TableDefinitionOptions{IfNotExist: failNE, IfExists: appendEx}, mode: adbc.OptionValueIngestModeAppend},
		{opts: &flightsql.TableDefinitionOptions{IfNotExist: failNE, IfExists: replace}, mode: adbc.OptionValueIngestModeReplace, mustExist: true},
		{opts: &flightsql.TableDefinitionOptions{IfNotExist: failNE, IfExists: failEx}, fails: true},
	} {
		mode, mustExist, err := ingestMode(tc.opts)
		if tc.fails {
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("%v: err = %v, want InvalidArgument", tc.opts, err)
			}
			continue
		}
		if err != nil || mode != tc.mode || mustExist != tc.mustExist {
			t.Errorf("%v: got %q, %v, %v; want %q, %v", tc.opts, mode, mustExist, err, tc.mode, tc.mustExist)
		}
	}
}

func TestFlattenObjects(t *testing.T) {
	const objs = `[
	  {"catalog_name": "redis", "catalog_db_schemas": [
	    {"db_schema_name": "sales", "db_schema_tables": [
	      {"table_name": "orders", "table_type": "TABLE", "table_columns": null, "table_constraints": null},
	      {"table_name": "big_orders", "table_type": "VIEW", "table_columns": null, "table_constraints": null}
	    ]},
	    {"db_schema_name": "public", "db_schema_tables": []},
	    {"db_schema_name": "empty", "db_schema_tables": null}
	  ]},
	  {"catalog_name": "other", "catalog_db_schemas": null}
	]`
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, adbc.GetObjectsSchema, strings.NewReader(objs))
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	rdr, _ := array.NewRecordReader(adbc.GetObjectsSchema, []arrow.RecordBatch{rec})
	defer rdr.Release()
	got, err := flattenObjects(rdr)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"other", "redis"}; !equal(got.catalogs, want) {
		t.Errorf("catalogs = %v, want %v", got.catalogs, want)
	}
	var schemas, tables []string
	for _, s := range got.schemas {
		schemas = append(schemas, *s.catalog+"."+s.schema)
	}
	for _, tr := range got.tables {
		tables = append(tables, *tr.catalog+"."+*tr.schema+"."+tr.name+":"+tr.kind)
	}
	if want := []string{"redis.empty", "redis.public", "redis.sales"}; !equal(schemas, want) {
		t.Errorf("schemas = %v, want %v", schemas, want)
	}
	if want := []string{"redis.sales.big_orders:VIEW", "redis.sales.orders:TABLE"}; !equal(tables, want) {
		t.Errorf("tables = %v, want %v", tables, want)
	}
}

func equal(a, b []string) bool { return strings.Join(a, "|") == strings.Join(b, "|") }

func TestChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(mem)
	const n = 1_000_000 // 8 MB
	for i := range n {
		b.Append(int64(i))
	}
	col := b.NewArray()
	b.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, n)
	col.Release()
	parts := chunks(rec)
	rec.Release()
	if len(parts) < 4 {
		t.Fatalf("got %d chunks, want at least 4", len(parts))
	}
	var rows int64
	for _, p := range parts {
		if p.NumRows()*8 > maxChunkBytes {
			t.Errorf("chunk of %d rows is over %d bytes", p.NumRows(), maxChunkBytes)
		}
		if first := p.Column(0).(*array.Int64).Value(0); first != rows {
			t.Errorf("chunk starts at %d, want %d", first, rows)
		}
		rows += p.NumRows()
		p.Release()
	}
	if rows != n {
		t.Errorf("chunks have %d rows, want %d", rows, n)
	}
}

func TestDatasetKeys(t *testing.T) {
	d := &datasets{prefix: "af:"}
	for _, name := range []string{"sales", "a/b/c", "with {braces}", "odd*?[]\\ name", "ünïcödé"} {
		k := d.keys(name)
		if strings.Count(k.meta, "{") != 1 || strings.Count(k.meta, "}") != 1 {
			t.Errorf("%q: key %q has extra braces, which would break its hash tag", name, k.meta)
		}
		if got, ok := d.nameFromKey(k.meta); !ok || got != name {
			t.Errorf("nameFromKey(%q) = %q, %v; want %q", k.meta, got, ok, name)
		}
		if _, ok := d.nameFromKey(k.seq); ok {
			t.Errorf("nameFromKey(%q) accepted a seq key", k.seq)
		}
	}
	if got := escapeGlob(`a*b?[c]\`); got != `a\*b\?\[c\]\\` {
		t.Errorf("escapeGlob = %q", got)
	}
}

func TestStatementHandle(t *testing.T) {
	h := statementHandle{Query: "SELECT $1", Params: []byte{0, 1, 2, 0xff}}
	got, err := decodeHandle(h.encode())
	if err != nil || got.Query != h.Query || string(got.Params) != string(h.Params) {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	if _, err := decodeHandle([]byte("garbage")); status.Code(err) != codes.InvalidArgument {
		t.Errorf("garbage handle: err = %v", err)
	}
}

// newTestServer starts a Server on an in-memory listener and returns a
// connection to it.
func newTestServer(t *testing.T, cfg Config) *grpc.ClientConn {
	t.Helper()
	return dialTestServer(t, startTestServer(t, cfg))
}

// startTestServer starts a Server on an in-memory listener.
func startTestServer(t *testing.T, cfg Config) *bufconn.Listener {
	t.Helper()
	_, lis := startServer(t, cfg)
	return lis
}

// startServer is startTestServer that also returns the Server.
func startServer(t *testing.T, cfg Config) (*Server, *bufconn.Listener) {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = testLogger(t)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Shutdown)
	return srv, lis
}

func testDialOptions(lis *bufconn.Listener) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 << 20)),
	}
}

func dialTestServer(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///bufconn", testDialOptions(lis)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func TestShutdownDrainsThenCancels(t *testing.T) {
	// Nothing here reaches Redis: a DoPut waits for its first message.
	const timeout = 500 * time.Millisecond
	srv, lis := startServer(t, Config{RedisURI: "redis://127.0.0.1:1/0", ShutdownTimeout: timeout})
	client := flight.NewFlightServiceClient(dialTestServer(t, lis))
	ctx := context.Background()
	open := func() flight.FlightService_DoPutClient {
		stream, err := client.DoPut(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	finishing, hung := open(), open()
	time.Sleep(200 * time.Millisecond) // for both calls to reach the server

	start := time.Now()
	done := make(chan struct{})
	go func() {
		srv.Shutdown()
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)

	// A call running at shutdown still finishes.
	if err := finishing.Send(&flight.FlightData{DataHeader: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := finishing.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a call that finished during shutdown: err = %v, want its own InvalidArgument", err)
	}
	// A new one is refused.
	if stream, err := client.DoPut(ctx); err == nil {
		if _, err = stream.Recv(); status.Code(err) != codes.Unavailable {
			t.Errorf("a call made during shutdown: err = %v, want Unavailable", err)
		}
	}
	// One that doesn't finish is cancelled at the timeout.
	select {
	case <-done:
	case <-time.After(timeout + 5*time.Second):
		t.Fatal("Shutdown didn't return after its timeout")
	}
	if d := time.Since(start); d < timeout {
		t.Errorf("Shutdown returned after %v, before its timeout, with a call running", d)
	}
	if _, err := hung.Recv(); err == nil || status.Code(err) == codes.InvalidArgument {
		t.Errorf("the call still running at the timeout: err = %v, want it cut off", err)
	}
}

func TestDoPutNeedsDescriptor(t *testing.T) {
	// Nothing here reaches Redis.
	cc := newTestServer(t, Config{RedisURI: "redis://127.0.0.1:1/0"})
	client := flight.NewFlightServiceClient(cc)
	ctx := context.Background()

	put := func(msgs ...*flight.FlightData) error {
		stream, err := client.DoPut(ctx)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if err := stream.Send(m); err != nil {
				return err
			}
		}
		if err := stream.CloseSend(); err != nil {
			return err
		}
		_, err = stream.Recv()
		return err
	}
	if err := put(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty DoPut: err = %v, want InvalidArgument", err)
	}
	if err := put(&flight.FlightData{DataHeader: []byte{1}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("DoPut without a descriptor: err = %v, want InvalidArgument", err)
	}
	if err := put(&flight.FlightData{FlightDescriptor: &flight.FlightDescriptor{Type: flight.DescriptorPATH}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("DoPut with an empty path: err = %v, want InvalidArgument", err)
	}

	// Garbage tickets are refused, whichever handler they reach.
	for _, tkt := range [][]byte{[]byte("{not json"), []byte("garbage")} {
		stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: tkt})
		if err == nil {
			_, err = stream.Recv()
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("DoGet(%q): err = %v, want InvalidArgument", tkt, err)
		}
	}

	// The action list has Flight SQL's actions and the dataset one.
	actions, err := client.ListActions(ctx, &flight.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for {
		a, err := actions.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, a.Type)
	}
	joined := strings.Join(types, ",")
	if !strings.Contains(joined, actionDropDataset) || !strings.Contains(joined, flightsql.CreatePreparedStatementActionType) {
		t.Errorf("actions = %v", types)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []Config{
		{Auth: "kerberos"},
		{TLSCertFile: "cert.pem"},
		{ADBCOptions: map[string]string{"uri": "redis://elsewhere"}},
		{RedisURI: "http://not-redis"},
		{MaxTableStreams: -1},
		{ShutdownTimeout: -time.Second},
	} {
		cfg.Logger = testLogger(t)
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
}

// Each SqlInfo value is in the union member FlightSql.proto gives its type
// (the JDBC driver reads values as that type).
func TestSqlInfoTypes(t *testing.T) {
	cc := newTestServer(t, Config{RedisURI: "redis://127.0.0.1:1/0"}) // SqlInfo needs no Redis
	c := &flightsql.Client{Client: flight.NewClientFromConn(cc, nil), Alloc: memory.DefaultAllocator}
	ctx := context.Background()
	const str, boolean, int32Value = 0, 1, 3 // the union's type codes
	want := map[flightsql.SqlInfo]arrow.UnionTypeCode{
		flightsql.SqlInfoFlightSqlServerName: str, flightsql.SqlInfoFlightSqlServerVersion: str,
		flightsql.SqlInfoFlightSqlServerArrowVersion: str, flightsql.SqlInfoIdentifierQuoteChar: str,
		flightsql.SqlInfoFlightSqlServerReadOnly: boolean, flightsql.SqlInfoFlightSqlServerSql: boolean,
		flightsql.SqlInfoFlightSqlServerSubstrait: boolean, flightsql.SqlInfoFlightSqlServerCancel: boolean,
		flightsql.SqlInfoFlightSqlServerBulkIngestion: boolean, flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported: boolean,
		flightsql.SqlInfoDDLCatalog: boolean, flightsql.SqlInfoDDLSchema: boolean, flightsql.SqlInfoDDLTable: boolean,
		flightsql.SqlInfoAllTablesAreASelectable: boolean, flightsql.SqlInfoTransactionsSupported: boolean,
		flightsql.SqlInfoFlightSqlServerTransaction: int32Value, flightsql.SqlInfoFlightSqlServerStatementTimeout: int32Value,
		flightsql.SqlInfoFlightSqlServerTransactionTimeout: int32Value, flightsql.SqlInfoNullOrdering: int32Value,
	}
	info, err := c.GetSqlInfo(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	seen := 0
	for rdr.Next() {
		rec := rdr.RecordBatch()
		names := rec.Column(0).(*array.Uint32)
		values := rec.Column(1).(*array.DenseUnion)
		for i := range int(rec.NumRows()) {
			id := flightsql.SqlInfo(names.Value(i))
			code, ok := want[id]
			if !ok {
				t.Errorf("SqlInfo %v isn't checked", id)
				continue
			}
			seen++
			if got := values.TypeCode(i); got != code {
				t.Errorf("SqlInfo %v is union member %d, want %d", id, got, code)
			}
		}
	}
	if seen != len(want) {
		t.Errorf("got %d SqlInfo values, want %d", seen, len(want))
	}
}
