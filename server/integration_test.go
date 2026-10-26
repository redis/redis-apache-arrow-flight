package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The integration tests need a Redis 8 with the Query Engine, for example
// `docker compose up -d --wait redis`, and REDIS_URI pointing at it. They
// create tables, keys and ACL users named it_flight_* and remove them.

func redisURI(t testing.TB) string {
	t.Helper()
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run the integration tests")
	}
	return uri
}

type clients struct {
	sql *flightsql.Client
	fl  flight.Client
}

func newClients(t testing.TB, cfg Config) clients {
	t.Helper()
	cc := newTestServer(t, cfg)
	fc := flight.NewClientFromConn(cc, nil)
	return clients{sql: &flightsql.Client{Client: fc, Alloc: memory.DefaultAllocator}, fl: fc}
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()%1_000_000_000_000)
}

// fetch reads every endpoint of info, checks the streams' schema against
// info's, and returns the rows as "a|b|c" strings.
func fetch(t *testing.T, ctx context.Context, c *flightsql.Client, info *flight.FlightInfo) (*arrow.Schema, []string) {
	t.Helper()
	var want *arrow.Schema
	if len(info.Schema) > 0 {
		s, err := flight.DeserializeSchema(info.Schema, memory.DefaultAllocator)
		if err != nil {
			t.Fatal(err)
		}
		want = s
	}
	var (
		rows   []string
		schema *arrow.Schema
	)
	for _, ep := range info.Endpoint {
		rdr, err := c.DoGet(ctx, ep.Ticket)
		if err != nil {
			t.Fatalf("DoGet: %v", err)
		}
		schema = rdr.Schema()
		if want != nil && !schema.Equal(want) {
			t.Errorf("stream schema %s differs from FlightInfo schema %s", schema, want)
		}
		for rdr.Next() {
			rows = append(rows, rowStrings(rdr.RecordBatch())...)
		}
		if err := rdr.Err(); err != nil {
			t.Fatalf("reading stream: %v", err)
		}
		rdr.Release()
	}
	return schema, rows
}

func rowStrings(rec arrow.RecordBatch) []string {
	out := make([]string, rec.NumRows())
	for i := range out {
		cells := make([]string, rec.NumCols())
		for j, col := range rec.Columns() {
			cells[j] = col.ValueStr(i)
		}
		out[i] = strings.Join(cells, "|")
	}
	return out
}

func query(t *testing.T, ctx context.Context, c *flightsql.Client, sql string) []string {
	t.Helper()
	info, err := c.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	_, rows := fetch(t, ctx, c, info)
	return rows
}

func update(t *testing.T, ctx context.Context, c *flightsql.Client, sql string) int64 {
	t.Helper()
	n, err := c.ExecuteUpdate(ctx, sql)
	if err != nil {
		t.Fatalf("ExecuteUpdate(%q): %v", sql, err)
	}
	return n
}

func wantRows(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("rows = %q\nwant   %q", got, want)
	}
}

func dropTable(t *testing.T, c *flightsql.Client, name string) {
	t.Cleanup(func() {
		_, _ = c.ExecuteUpdate(context.Background(), "DROP TABLE IF EXISTS "+name)
	})
}

func TestIntegrationSQL(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_sql")
	dropTable(t, c, tbl)

	update(t, ctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, name VARCHAR, score DOUBLE PRECISION)")
	if n := update(t, ctx, c, "INSERT INTO "+tbl+" VALUES (1, 'a', 1.5), (2, 'b', NULL), (3, 'c', 3.25)"); n != 3 {
		t.Errorf("INSERT changed %d rows, want 3", n)
	}
	wantRows(t, query(t, ctx, c, "SELECT id, name, score FROM "+tbl+" ORDER BY id"),
		"1|a|1.5", "2|b|(null)", "3|c|3.25")
	wantRows(t, query(t, ctx, c, "SELECT COUNT(*) AS n, SUM(score) AS total FROM "+tbl), "3|4.75")
	wantRows(t, query(t, ctx, c, "SELECT name FROM "+tbl+" WHERE score > 2 ORDER BY id"), "c")

	// A schema-changing script runs, and its connection isn't reused.
	wantRows(t, query(t, ctx, c, "SET TIME ZONE 'America/New_York'; SHOW TimeZone"), "America/New_York")
	for range 3 {
		wantRows(t, query(t, ctx, c, "SHOW TimeZone"), "UTC")
	}

	// Errors keep the driver's message and get a fitting code.
	_, err := c.Execute(ctx, "SELECT * FROM it_flight_no_such_table")
	if code := status.Code(err); code != codes.NotFound && code != codes.InvalidArgument {
		t.Errorf("missing table: err = %v, want NotFound or InvalidArgument", err)
	}
	_, err = c.Execute(ctx, "SELEKT 1")
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("syntax error: err = %v, want InvalidArgument", err)
	}
	_, err = c.ExecuteUpdate(ctx, "INSERT INTO "+tbl+" (id) VALUES (NULL)")
	if err == nil {
		t.Error("NULL into a NOT NULL column succeeded")
	}

	// A DDL statement through Execute: an empty result, then the effect.
	view := tbl + "_v"
	t.Cleanup(func() { _, _ = c.ExecuteUpdate(context.Background(), "DROP VIEW IF EXISTS "+view) })
	wantRows(t, query(t, ctx, c, "CREATE VIEW "+view+" AS SELECT id FROM "+tbl+" WHERE id >= 2"))
	wantRows(t, query(t, ctx, c, "SELECT COUNT(*) FROM "+view), "2")
}

// Statements that change data run once, when their FlightInfo is asked
// for, whether or not the stream is read, and however often.
func TestIntegrationWritesRunOnce(t *testing.T) {
	cl := newClients(t, Config{RedisURI: redisURI(t), KeyPrefix: "it_flight_res:"})
	c := cl.sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_once")
	dropTable(t, c, tbl)
	update(t, ctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, label VARCHAR)")

	// Not reading the stream at all.
	if _, err := c.Execute(ctx, "INSERT INTO "+tbl+" VALUES (1, 'one'), (2, 'two')"); err != nil {
		t.Fatal(err)
	}
	wantRows(t, query(t, ctx, c, "SELECT COUNT(*) FROM "+tbl), "2")

	// Reading a RETURNING result twice.
	info, err := c.Execute(ctx, "INSERT INTO "+tbl+" VALUES (3, 'three') RETURNING id, label")
	if err != nil {
		t.Fatal(err)
	}
	if info.TotalRecords != 1 {
		t.Errorf("TotalRecords = %d, want 1", info.TotalRecords)
	}
	for range 2 {
		_, rows := fetch(t, ctx, c, info)
		wantRows(t, rows, "3|three")
	}
	wantRows(t, query(t, ctx, c, "SELECT COUNT(*) FROM "+tbl), "3")

	// A prepared INSERT through Execute, as DB-API cursors run it.
	ins, err := c.Prepare(ctx, "INSERT INTO "+tbl+" VALUES ($1, 'p')")
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Close(ctx)
	p := int64Record(t, []string{"id"}, []int64{4})
	ins.SetParameters(p)
	p.Release()
	if _, err := ins.Execute(ctx); err != nil {
		t.Fatal(err)
	}
	wantRows(t, query(t, ctx, c, "SELECT label FROM "+tbl+" WHERE id = 4"), "p")

	// A result too big for a ticket is kept in Redis for a while.
	const n = 60_000
	labels := make([]string, n)
	for i := range labels {
		labels[i] = fmt.Sprintf("a fairly long label for row %06d", i)
	}
	lb := array.NewStringBuilder(memory.DefaultAllocator)
	lb.AppendValues(labels, nil)
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	ib.AppendValues(sequence(n), nil)
	ids, lab := ib.NewArray(), lb.NewArray()
	ib.Release()
	lb.Release()
	big := uniqueName("it_flight_big")
	dropTable(t, c, big)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "label", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{ids, lab}, n)
	ids.Release()
	lab.Release()
	rdr, _ := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	rec.Release()
	if _, err := c.ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{Table: big, TableDefinitionOptions: &flightsql.TableDefinitionOptions{
		IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate, IfExists: flightsql.TableDefinitionOptionsTableExistsOptionFail}}); err != nil {
		t.Fatal(err)
	}
	rdr.Release()
	info, err = c.Execute(ctx, "UPDATE "+big+" SET label = UPPER(label) RETURNING id, label")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Endpoint[0].Ticket.Ticket) > maxInlineBytes {
		t.Errorf("ticket of %d bytes", len(info.Endpoint[0].Ticket.Ticket))
	}
	_, rows := fetch(t, ctx, c, info)
	slices.Sort(rows)
	if len(rows) != n || rows[0] != "0|A FAIRLY LONG LABEL FOR ROW 000000" {
		t.Errorf("RETURNING: %d rows, first %q", len(rows), rows[0])
	}
	admin := adminClient(t, redisURI(t))
	keys := keysMatching(t, admin, "it_flight_res:result:*")
	if len(keys) != 1 {
		t.Errorf("result keys = %v, want one", keys)
	}
	for _, k := range keys {
		if ttl := admin.TTL(ctx, k).Val(); ttl <= 0 || ttl > resultTTL {
			t.Errorf("result key TTL = %v", ttl)
		}
		admin.Del(ctx, k)
	}
}

// Temporary tables and views stay with the request that made them, however
// they are made: a pool of one connection would hand a leaked one to the
// next request.
func TestIntegrationTempObjectsDontLeak(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t), PoolMaxIdle: 1, PoolMaxOpen: 1}).sql
	ctx := context.Background()
	for _, create := range []string{
		"CREATE TABLE pg_temp.%s (id BIGINT)",
		"CREATE TABLE PG_TEMP.%s AS SELECT 1 AS id",
		"CREATE VIEW pg_temp.%s AS SELECT 1 AS id",
	} {
		name := uniqueName("it_flight_tmp")
		update(t, ctx, c, fmt.Sprintf(create, name))
		_, err := c.Execute(ctx, "SELECT * FROM "+name)
		if code := status.Code(err); code != codes.NotFound && code != codes.InvalidArgument {
			t.Errorf("%s, then SELECT from it in another request: err = %v, want NotFound or InvalidArgument",
				fmt.Sprintf(create, name), err)
		}
	}

	// Bulk ingest into a temporary table is refused, however it is asked for.
	pgTemp := "pg_temp"
	for desc, opts := range map[string]*flightsql.ExecuteIngestOpts{
		"schema pg_temp":               {Schema: &pgTemp},
		"adbc.ingest.temporary":        {Options: map[string]string{"adbc.ingest.temporary": "true"}},
		"adbc.ingest.target_db_schema": {Options: map[string]string{"adbc.ingest.target_db_schema": "pg_temp"}},
	} {
		opts.Table = uniqueName("it_flight_tmp_ingest")
		dropTable(t, c, opts.Table)
		opts.TableDefinitionOptions = &flightsql.TableDefinitionOptions{
			IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
			IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionFail,
		}
		rec := int64Record(t, []string{"id"}, []int64{1})
		rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
		rec.Release()
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.ExecuteIngest(ctx, rdr, opts)
		rdr.Release()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("ingest with %s: err = %v, want InvalidArgument", desc, err)
		}
	}
}

// Prepared statements report their parameters' types (the JDBC driver
// sizes a statement's parameters from them), and are planned with them.
func TestIntegrationPreparedTypes(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_ptypes")
	dropTable(t, c, tbl)
	// DDL reports 0 rows, not the driver's -1 (unknown): JDBC takes -1 for
	// a query.
	if n := update(t, ctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, name VARCHAR, qty INTEGER)"); n != 0 {
		t.Errorf("CREATE TABLE changed %d rows, want 0", n)
	}
	types := func(s *arrow.Schema) string {
		if s == nil {
			return "none"
		}
		out := make([]string, s.NumFields())
		for i, f := range s.Fields() {
			out[i] = f.Type.String()
		}
		return "[" + strings.Join(out, " ") + "]"
	}
	for _, tc := range []struct{ sql, params, dataset string }{
		{"SELECT name FROM " + tbl + " WHERE id = $1", "[int64]", "[utf8]"},
		{"INSERT INTO " + tbl + " VALUES (?, ?, ?)", "[int64 utf8 int32]", "[]"},
		// Planned with $1's type, so the column has one.
		{"SELECT $1 AS x FROM " + tbl + " WHERE id = $1", "[int64]", "[int64]"},
		{"SELECT $1 AS x", "[null]", "none"},
		{"SELECT 1 AS one", "[]", "[int32]"},
		{"SELECT CAST(1 AS BIGINT) AS one", "[]", "[int64]"},
	} {
		p, err := c.Prepare(ctx, tc.sql)
		if err != nil {
			t.Errorf("Prepare(%q): %v", tc.sql, err)
			continue
		}
		if got := types(p.ParameterSchema()); got != tc.params {
			t.Errorf("%s: parameters %s, want %s", tc.sql, got, tc.params)
		}
		if got := types(p.DatasetSchema()); got != tc.dataset {
			t.Errorf("%s: dataset %s, want %s", tc.sql, got, tc.dataset)
		}
		_ = p.Close(ctx)
	}
}

// A prepared script that changes things isn't planned before it runs, so
// its last statement may use what the ones before it make or set.
func TestIntegrationPreparedScripts(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	execute := func(t *testing.T, p *flightsql.PreparedStatement, id int64) []string {
		t.Helper()
		params := int64Record(t, []string{"id"}, []int64{id})
		p.SetParameters(params)
		params.Release()
		info, err := p.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		_, rows := fetch(t, ctx, c, info)
		return rows
	}

	t.Run("create, then read", func(t *testing.T) {
		tbl := uniqueName("it_flight_script")
		dropTable(t, c, tbl)
		p, err := c.Prepare(ctx, "CREATE TABLE "+tbl+" (id BIGINT); INSERT INTO "+tbl+" VALUES ($1); SELECT id FROM "+tbl)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		defer p.Close(ctx)
		if p.DatasetSchema() != nil {
			t.Errorf("dataset schema = %s, want none before the script runs", p.DatasetSchema())
		}
		if _, err := p.GetSchema(ctx); status.Code(err) != codes.Unimplemented {
			t.Errorf("GetSchema: err = %v, want Unimplemented", err)
		}
		wantRows(t, execute(t, p, 7), "7")
	})

	t.Run("set search_path, then read", func(t *testing.T) {
		schema, tbl := uniqueName("it_flight_s"), uniqueName("it_flight_t")
		dropTable(t, c, schema+"."+tbl)
		update(t, ctx, c, "CREATE TABLE "+schema+"."+tbl+" (id BIGINT)")
		update(t, ctx, c, "INSERT INTO "+schema+"."+tbl+" VALUES (1), (2)")
		p, err := c.Prepare(ctx, "SET search_path TO "+schema+"; SELECT id FROM "+tbl+" WHERE id = $1")
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		defer p.Close(ctx)
		wantRows(t, execute(t, p, 2), "2")
	})
}

// A bound parameter batch with columns but no rows runs the statement zero
// times; only a batch without columns means "no parameters".
func TestIntegrationZeroRowParams(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_zero")
	dropTable(t, c, tbl)
	update(t, ctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, qty BIGINT)")
	update(t, ctx, c, "INSERT INTO "+tbl+" VALUES (1, 10)")
	count := func(t *testing.T) []string { return query(t, ctx, c, "SELECT COUNT(*) FROM "+tbl) }
	prepare := func(t *testing.T, sql string) *flightsql.PreparedStatement {
		t.Helper()
		p, err := c.Prepare(ctx, sql)
		if err != nil {
			t.Fatalf("Prepare(%q): %v", sql, err)
		}
		t.Cleanup(func() { _ = p.Close(context.Background()) })
		return p
	}
	bind := func(p *flightsql.PreparedStatement, names []string, cols ...[]int64) {
		rec := int64Record(t, names, cols...)
		p.SetParameters(rec)
		rec.Release()
	}
	// The arrow-go client sends a zero-row batch given to SetParameters as
	// "no parameters" (no columns), so a bind of no rows goes through a
	// reader, as ADBC's BindStream does: its columns, and no batches.
	bindNoRows := func(p *flightsql.PreparedStatement, names ...string) {
		cols := make([][]int64, len(names))
		for i := range cols {
			cols[i] = []int64{}
		}
		rec := int64Record(t, names, cols...)
		rdr, err := array.NewRecordReader(rec.Schema(), nil)
		rec.Release()
		if err != nil {
			t.Fatal(err)
		}
		p.SetRecordReader(rdr)
		rdr.Release()
	}

	t.Run("update", func(t *testing.T) {
		p := prepare(t, "INSERT INTO "+tbl+" VALUES ($1, $2)")
		bindNoRows(p, "id", "qty")
		if n, err := p.ExecuteUpdate(ctx); err != nil || n != 0 {
			t.Errorf("ExecuteUpdate with no rows = %d, %v; want 0", n, err)
		}
		wantRows(t, count(t), "1")
	})

	t.Run("update after a query bind", func(t *testing.T) {
		p := prepare(t, "INSERT INTO "+tbl+" VALUES ($1, 20)")
		bind(p, []string{"id"}, []int64{2})
		// Run as a query: it inserts once, and the handle keeps {2}.
		if _, err := p.Execute(ctx); err != nil {
			t.Fatal(err)
		}
		wantRows(t, count(t), "2")
		bindNoRows(p, "id")
		if n, err := p.ExecuteUpdate(ctx); err != nil || n != 0 {
			t.Errorf("ExecuteUpdate with no rows = %d, %v; want 0", n, err)
		}
		wantRows(t, count(t), "2")
	})

	t.Run("query", func(t *testing.T) {
		p := prepare(t, "SELECT qty FROM "+tbl+" WHERE id = $1")
		bindNoRows(p, "id")
		info, err := p.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		schema, rows := fetch(t, ctx, c, info)
		wantRows(t, rows)
		if schema.NumFields() != 1 || schema.Field(0).Name != "qty" {
			t.Errorf("result schema = %s, want qty", schema)
		}
	})
}

func int64Record(t *testing.T, names []string, cols ...[]int64) arrow.RecordBatch {
	t.Helper()
	fields := make([]arrow.Field, len(names))
	arrs := make([]arrow.Array, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.AppendValues(cols[i], nil)
		arrs[i] = b.NewArray()
		b.Release()
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, int64(len(cols[0])))
	for _, a := range arrs {
		a.Release()
	}
	return rec
}

func TestIntegrationPrepared(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_prep")
	dropTable(t, c, tbl)
	update(t, ctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, qty BIGINT)")
	update(t, ctx, c, "INSERT INTO "+tbl+" VALUES (1, 10), (2, 20), (3, 30)")

	// A query with a parameter, bound twice.
	q, err := c.Prepare(ctx, "SELECT qty FROM "+tbl+" WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close(ctx)
	if isUpdate, ok := q.IsUpdate(); ok && isUpdate {
		t.Error("a SELECT was prepared as an update")
	}
	if q.DatasetSchema() == nil || q.DatasetSchema().Field(0).Name != "qty" {
		t.Errorf("dataset schema = %v", q.DatasetSchema())
	}
	for id, want := range map[int64]string{2: "20", 3: "30"} {
		p := int64Record(t, []string{"id"}, []int64{id})
		q.SetParameters(p)
		p.Release()
		info, err := q.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, rows := fetch(t, ctx, c, info)
		wantRows(t, rows, want)
	}

	// A prepared update run once per row of its parameters.
	ins, err := c.Prepare(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Close(ctx)
	if isUpdate, ok := ins.IsUpdate(); !ok || !isUpdate {
		t.Errorf("IsUpdate = %v, %v; want true", isUpdate, ok)
	}
	p := int64Record(t, []string{"id", "qty"}, []int64{4, 5}, []int64{40, 50})
	ins.SetParameters(p)
	p.Release()
	if n, err := ins.ExecuteUpdate(ctx); err != nil || n != 2 {
		t.Errorf("prepared INSERT = %d, %v; want 2", n, err)
	}

	// A prepared update without parameters (the client sends an empty
	// batch, which must not run it zero times).
	up, err := c.Prepare(ctx, "UPDATE "+tbl+" SET qty = 0 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close(ctx)
	if n, err := up.ExecuteUpdate(ctx); err != nil || n != 1 {
		t.Errorf("prepared UPDATE = %d, %v; want 1", n, err)
	}

	// A prepared query without parameters.
	cnt, err := c.Prepare(ctx, "SELECT COUNT(*) AS n, SUM(qty) AS total FROM "+tbl)
	if err != nil {
		t.Fatal(err)
	}
	defer cnt.Close(ctx)
	info, err := cnt.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, rows := fetch(t, ctx, c, info)
	wantRows(t, rows, "5|140")

	if _, err := c.Prepare(ctx, "SELEKT"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("preparing bad SQL: err = %v, want InvalidArgument", err)
	}
}

func TestIntegrationIngest(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_ingest")
	dropTable(t, c, tbl)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	makeReader := func(start, n int) array.RecordReader {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		var recs []arrow.RecordBatch
		for i := start; i < start+n; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(i))
			b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row %d", i))
			b.Field(2).(*array.Float64Builder).Append(float64(i) / 2)
			if (i-start+1)%250 == 0 || i == start+n-1 {
				recs = append(recs, b.NewRecordBatch())
			}
		}
		rdr, _ := array.NewRecordReader(schema, recs)
		releaseAll(recs)
		return rdr
	}
	ingest := func(rdr array.RecordReader, ifNotExist flightsql.TableDefinitionOptionsTableNotExistOption,
		ifExists flightsql.TableDefinitionOptionsTableExistsOption, temporary bool) (int64, error) {
		defer rdr.Release()
		return c.ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{
			Table:                  tbl,
			Temporary:              temporary,
			TableDefinitionOptions: &flightsql.TableDefinitionOptions{IfNotExist: ifNotExist, IfExists: ifExists},
			Options:                map[string]string{"adbc.redis.ingest.index_columns": "id,value"},
		})
	}
	const (
		create   = flightsql.TableDefinitionOptionsTableNotExistOptionCreate
		failNE   = flightsql.TableDefinitionOptionsTableNotExistOptionFail
		failEx   = flightsql.TableDefinitionOptionsTableExistsOptionFail
		appendEx = flightsql.TableDefinitionOptionsTableExistsOptionAppend
		replace  = flightsql.TableDefinitionOptionsTableExistsOptionReplace
	)

	if _, err := ingest(makeReader(0, 10), failNE, replace, false); status.Code(err) != codes.NotFound {
		t.Errorf("replace of a missing table that must exist: err = %v, want NotFound", err)
	}
	if n, err := ingest(makeReader(0, 1000), create, failEx, false); err != nil || n != 1000 {
		t.Fatalf("create ingest = %d, %v; want 1000", n, err)
	}
	if _, err := ingest(makeReader(0, 10), create, failEx, false); status.Code(err) != codes.AlreadyExists {
		t.Errorf("create ingest into an existing table: err = %v, want AlreadyExists", err)
	}
	if n, err := ingest(makeReader(1000, 500), failNE, appendEx, false); err != nil || n != 500 {
		t.Errorf("append ingest = %d, %v; want 500", n, err)
	}
	wantRows(t, query(t, ctx, c, "SELECT COUNT(*), MIN(id), MAX(id), SUM(value) FROM "+tbl), "1500|0|1499|562125")
	wantRows(t, query(t, ctx, c, "SELECT label FROM "+tbl+" WHERE id = 1234"), "row 1234")
	if _, err := ingest(makeReader(0, 10), create, appendEx, true); status.Code(err) != codes.InvalidArgument {
		t.Errorf("temporary ingest: err = %v, want InvalidArgument", err)
	}
	if n, err := ingest(makeReader(0, 250), failNE, replace, false); err != nil || n != 250 {
		t.Errorf("replace ingest = %d, %v; want 250", n, err)
	}
	wantRows(t, query(t, ctx, c, "SELECT COUNT(*) FROM "+tbl), "250")
}

func TestIntegrationMetadata(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	tbl := uniqueName("it_flight_meta")
	dropTable(t, c, tbl)
	update(t, ctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, name VARCHAR)")

	info, err := c.GetCatalogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, rows := fetch(t, ctx, c, info)
	wantRows(t, rows, "redis")

	info, err = c.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, rows = fetch(t, ctx, c, info); !slices.Contains(rows, "redis|public") {
		t.Errorf("schemas %q have no public", rows)
	}

	info, err = c.GetTables(ctx, &flightsql.GetTablesOpts{TableNameFilterPattern: &tbl, IncludeSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	var found int
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := range int(rec.NumRows()) {
			found++
			if got := rec.Column(2).ValueStr(i) + "|" + rec.Column(3).ValueStr(i); got != tbl+"|TABLE" {
				t.Errorf("table row = %s", got)
			}
			ts, err := flight.DeserializeSchema(rec.Column(4).(*array.Binary).Value(i), memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			if ts.NumFields() != 2 || ts.Field(0).Name != "id" || ts.Field(1).Name != "name" {
				t.Errorf("table schema = %s", ts)
			}
		}
	}
	if found != 1 {
		t.Errorf("GetTables found %d tables, want 1", found)
	}

	info, err = c.GetTableTypes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, rows = fetch(t, ctx, c, info)
	wantRows(t, rows, "TABLE", "VIEW")

	info, err = c.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerName, flightsql.SqlInfoFlightSqlServerBulkIngestion})
	if err != nil {
		t.Fatal(err)
	}
	if _, rows = fetch(t, ctx, c, info); len(rows) != 2 || !strings.Contains(strings.Join(rows, " "), ServerName) {
		t.Errorf("SqlInfo rows = %q", rows)
	}

	info, err = c.GetPrimaryKeys(ctx, flightsql.TableRef{Table: tbl})
	if err != nil {
		t.Fatal(err)
	}
	_, rows = fetch(t, ctx, c, info)
	wantRows(t, rows)
}

// putDataset uploads recs as a dataset and returns the server's summary.
func putDataset(t *testing.T, ctx context.Context, fc flight.Client, desc *flight.FlightDescriptor, schema *arrow.Schema, recs ...arrow.RecordBatch) datasetMeta {
	t.Helper()
	m, err := tryPutDataset(ctx, fc, desc, schema, recs...)
	if err != nil {
		t.Fatalf("DoPut: %v", err)
	}
	return m
}

func tryPutDataset(ctx context.Context, fc flight.Client, desc *flight.FlightDescriptor, schema *arrow.Schema, recs ...arrow.RecordBatch) (datasetMeta, error) {
	var m datasetMeta
	stream, err := fc.DoPut(ctx)
	if err != nil {
		return m, err
	}
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	w.SetFlightDescriptor(desc)
	for _, rec := range recs {
		if err := w.Write(rec); err != nil {
			return m, err
		}
	}
	// With no batches, Close sends the schema alone.
	if err := w.Close(); err != nil {
		return m, err
	}
	if err := stream.CloseSend(); err != nil {
		return m, err
	}
	res, err := stream.Recv()
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(res.AppMetadata, &m); err != nil {
		return m, err
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return m, fmt.Errorf("DoPut ended with %v", err)
	}
	return m, nil
}

// putListGeneration stores recs as a new generation of dataset name the way
// servers did before the HASH layout, a LIST, and makes it current, as an
// older replica would.
func putListGeneration(t testing.TB, rdb goredis.UniversalClient, prefix, name string, schema *arrow.Schema, recs ...arrow.RecordBatch) datasetMeta {
	t.Helper()
	ctx := context.Background()
	mem := memory.DefaultAllocator
	d := &datasets{prefix: prefix, alloc: mem}
	keys := d.keys(name)
	gen, err := rdb.Incr(ctx, keys.seq).Result()
	if err != nil {
		t.Fatal(err)
	}
	m := datasetMeta{Name: name, Generation: gen, schema: flight.SerializeSchema(schema, mem)}
	head, err := writeIPC(schema, nil, mem)
	if err != nil {
		t.Fatal(err)
	}
	vals := []any{head}
	for _, rec := range recs {
		if rec.NumRows() == 0 {
			continue
		}
		for _, part := range chunks(rec) {
			b, err := writeIPC(schema, []arrow.RecordBatch{part}, mem)
			m.Rows += part.NumRows()
			part.Release()
			if err != nil {
				t.Fatal(err)
			}
			vals = append(vals, b)
			m.Batches++
			m.Bytes += int64(len(b))
		}
	}
	if err := rdb.RPush(ctx, keys.gen(gen), vals...).Err(); err != nil {
		t.Fatal(err)
	}
	m.Updated = time.Now().UTC()
	if _, err := d.commit(ctx, rdb, keys, m); err != nil {
		t.Fatal(err)
	}
	return m
}

func getDataset(t *testing.T, ctx context.Context, fc flight.Client, tkt []byte) (*arrow.Schema, []string, int, error) {
	t.Helper()
	stream, err := fc.DoGet(ctx, &flight.Ticket{Ticket: tkt})
	if err != nil {
		return nil, nil, 0, err
	}
	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rdr.Release()
	var (
		rows    []string
		batches int
	)
	for rdr.Next() {
		batches++
		rows = append(rows, rowStrings(rdr.RecordBatch())...)
	}
	return rdr.Schema(), rows, batches, rdr.Err()
}

func TestIntegrationDatasets(t *testing.T) {
	uri := redisURI(t)
	cl := newClients(t, Config{RedisURI: uri, KeyPrefix: "it_flight_ds:"})
	fc := cl.fl
	ctx := context.Background()
	base := uniqueName("it_flight")
	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{base, "events {v1}"}}
	name := base + "/events {v1}"
	t.Cleanup(func() {
		admin := adminClient(t, uri)
		deleteKeys(admin, keysMatching(t, admin, "it_flight_ds:*"))
	})

	// Nested and dictionary types, which SQL tables don't have, round-trip.
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "point", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64}), Nullable: true},
		{Name: "kind", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
	}, nil)
	rec1, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(`[
		{"id": 1, "tags": ["a", "b"], "point": {"x": 1, "y": 2}, "kind": "click"},
		{"id": 2, "tags": null, "point": null, "kind": "view"}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer rec1.Release()
	rec2, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(`[
		{"id": 3, "tags": [], "point": {"x": -1.5, "y": 0}, "kind": "click"}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer rec2.Release()

	m := putDataset(t, ctx, fc, desc, schema, rec1, rec2)
	if m.Name != name || m.Rows != 3 || m.Batches != 2 || m.Generation < 1 {
		t.Errorf("upload summary = %+v", m)
	}

	info, err := fc.GetFlightInfo(ctx, desc)
	if err != nil {
		t.Fatal(err)
	}
	if info.TotalRecords != 3 || info.TotalBytes != m.Bytes {
		t.Errorf("FlightInfo records %d, bytes %d; want 3, %d", info.TotalRecords, info.TotalBytes, m.Bytes)
	}
	infoSchema, err := flight.DeserializeSchema(info.Schema, mem)
	if err != nil || !infoSchema.Equal(schema) {
		t.Errorf("FlightInfo schema = %v, %v; want %s", infoSchema, err, schema)
	}
	sr, err := fc.GetSchema(ctx, desc)
	if err != nil || string(sr.Schema) != string(info.Schema) {
		t.Errorf("GetSchema = %v, %v", sr, err)
	}
	// A dataset is complete: PollInfo without a descriptor to poll again.
	pi, err := fc.PollFlightInfo(ctx, desc)
	if err != nil {
		t.Fatalf("PollFlightInfo: %v", err)
	}
	if pi.FlightDescriptor != nil || pi.Info.GetTotalRecords() != 3 {
		t.Errorf("PollFlightInfo = descriptor %v, %d records; want no descriptor (complete), 3 records",
			pi.FlightDescriptor, pi.Info.GetTotalRecords())
	}
	oldTicket := info.Endpoint[0].Ticket.Ticket
	gotSchema, rows, batches, err := getDataset(t, ctx, fc, oldTicket)
	if err != nil {
		t.Fatal(err)
	}
	if !gotSchema.Equal(schema) {
		t.Errorf("stream schema = %s", gotSchema)
	}
	if batches != 2 {
		t.Errorf("got %d batches, want 2", batches)
	}
	wantRows(t, rows, `1|["a","b"]|{"x":1,"y":2}|click`, `2|(null)|(null)|view`, `3|[]|{"x":-1.5,"y":0}|click`)
	// It is stored by column, and read by column: nested and dictionary
	// columns too.
	admin := adminClient(t, uri)
	genKey := (&datasets{prefix: "it_flight_ds:"}).keys(name).gen(m.Generation)
	if typ, err := admin.Type(ctx, genKey).Result(); err != nil || typ != "hash" {
		t.Errorf("TYPE %s = %q, %v; want hash", genKey, typ, err)
	}
	projected, _ := json.Marshal(datasetTicket{Dataset: name, Generation: m.Generation, Columns: []string{"kind", "point"}})
	if _, rows, _, err := getDataset(t, ctx, fc, projected); err != nil {
		t.Error(err)
	} else {
		wantRows(t, rows, `click|{"x":1,"y":2}`, `view|(null)`, `click|{"x":-1.5,"y":0}`)
	}

	// ListFlights finds it by name prefix.
	list, err := fc.ListFlights(ctx, &flight.Criteria{Expression: []byte(base)})
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for {
		fi, err := list.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		listed = append(listed, strings.Join(fi.FlightDescriptor.Path, "/"))
	}
	wantRows(t, listed, name)

	// Replacing it: the new generation is served, and a ticket for the old
	// one still reads the old data during the grace period.
	big := int64Record(t, []string{"n"}, sequence(600_000)) // 4.8 MB: stored in chunks
	defer big.Release()
	m2 := putDataset(t, ctx, fc, desc, big.Schema(), big)
	if m2.Generation != m.Generation+1 || m2.Rows != 600_000 || m2.Batches < 3 {
		t.Errorf("replacement summary = %+v", m2)
	}
	_, rows, _, err = getDataset(t, ctx, fc, oldTicket)
	if err != nil || len(rows) != 3 {
		t.Errorf("old ticket after replace: %d rows, %v", len(rows), err)
	}
	latest, _ := json.Marshal(datasetTicket{Dataset: name})
	gotSchema, rows, batches, err = getDataset(t, ctx, fc, latest)
	if err != nil {
		t.Fatal(err)
	}
	if !gotSchema.Equal(big.Schema()) || len(rows) != 600_000 || rows[599_999] != "599999" || batches < 3 {
		t.Errorf("latest: schema %s, %d rows in %d batches", gotSchema, len(rows), batches)
	}

	// A dataset with a schema and no rows.
	emptyDesc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{base + "_empty"}}
	if m := putDataset(t, ctx, fc, emptyDesc, schema); m.Rows != 0 || m.Batches != 0 {
		t.Errorf("empty upload summary = %+v", m)
	}
	info, err = fc.GetFlightInfo(ctx, emptyDesc)
	if err != nil {
		t.Fatal(err)
	}
	gotSchema, rows, _, err = getDataset(t, ctx, fc, info.Endpoint[0].Ticket.Ticket)
	if err != nil || !gotSchema.Equal(schema) || len(rows) != 0 {
		t.Errorf("empty dataset: schema %v, %d rows, %v", gotSchema, len(rows), err)
	}

	// Dropping: then it's gone, for old tickets too.
	drop := func(body string) map[string]any {
		stream, err := fc.DoAction(ctx, &flight.Action{Type: actionDropDataset, Body: []byte(body)})
		if err != nil {
			t.Fatal(err)
		}
		res, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		_ = json.Unmarshal(res.Body, &out)
		return out
	}
	if out := drop(name); out["dropped"] != true {
		t.Errorf("drop = %v", out)
	}
	if out := drop(fmt.Sprintf(`{"dataset": %q}`, base+"_empty")); out["dropped"] != true {
		t.Errorf("drop by JSON = %v", out)
	}
	if out := drop(name); out["dropped"] != false {
		t.Errorf("second drop = %v", out)
	}
	if _, err := fc.GetFlightInfo(ctx, desc); status.Code(err) != codes.NotFound {
		t.Errorf("GetFlightInfo after drop: err = %v, want NotFound", err)
	}
	if _, _, _, err := getDataset(t, ctx, fc, oldTicket); status.Code(err) != codes.NotFound {
		t.Errorf("old ticket after drop: err = %v, want NotFound", err)
	}

	// An upload without a schema is refused.
	stream, err := fc.DoPut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&flight.FlightData{FlightDescriptor: desc})
	_ = stream.CloseSend()
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("upload without a schema: err = %v, want InvalidArgument", err)
	}
}

// commitHook makes the reply to the dataset commit script fail once, the
// way it does when the client goes away: after the script ran (ran), or
// instead of running it.
type commitHook struct {
	ran   bool
	fired atomic.Bool
}

func (h *commitHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *commitHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

// isCommitScript reports whether cmd runs the dataset commit script, which
// has 4 keys (the drop script has 2).
func isCommitScript(cmd goredis.Cmder) bool {
	name, args := strings.ToLower(cmd.Name()), cmd.Args()
	return (name == "evalsha" || name == "eval") && len(args) > 2 && fmt.Sprint(args[2]) == "4"
}

func (h *commitHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if !isCommitScript(cmd) || h.fired.Load() {
			return next(ctx, cmd)
		}
		if h.ran {
			if err := next(ctx, cmd); err != nil {
				return err // NOSCRIPT, before go-redis falls back to EVAL
			}
		}
		h.fired.Store(true)
		cmd.SetErr(context.Canceled)
		return context.Canceled
	}
}

func datasetDesc(name string) *flight.FlightDescriptor {
	return &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{name}}
}

// readDataset returns a dataset's current summary and rows.
func readDataset(t *testing.T, ctx context.Context, fc flight.Client, desc *flight.FlightDescriptor) (datasetMeta, []string) {
	t.Helper()
	info, err := fc.GetFlightInfo(ctx, desc)
	if err != nil {
		t.Fatalf("GetFlightInfo: %v", err)
	}
	var m datasetMeta
	if err := json.Unmarshal(info.AppMetadata, &m); err != nil {
		t.Fatal(err)
	}
	_, rows, _, err := getDataset(t, ctx, fc, info.Endpoint[0].Ticket.Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	return m, rows
}

// An upload whose commit reply is lost keeps its data when the commit
// went through, and leaves the dataset as it was when it didn't.
func TestIntegrationUploadCommitOutcome(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	const prefix = "it_flight_commit:"
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, prefix+"*")) })
	rec := int64Record(t, []string{"n"}, []int64{1, 2, 3})
	defer rec.Release()

	t.Run("reply lost after the commit ran", func(t *testing.T) {
		hook := &commitHook{ran: true}
		fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, redisHook: hook}).fl
		desc := datasetDesc(uniqueName("it_flight_lost_reply"))
		m := putDataset(t, ctx, fc, desc, rec.Schema(), rec)
		if !hook.fired.Load() {
			t.Fatal("the commit reply wasn't failed")
		}
		got, rows := readDataset(t, ctx, fc, desc)
		if got.Generation != m.Generation {
			t.Errorf("current generation %d, want the upload's %d", got.Generation, m.Generation)
		}
		wantRows(t, rows, "1", "2", "3")
	})

	t.Run("commit never ran", func(t *testing.T) {
		name := uniqueName("it_flight_no_commit")
		desc := datasetDesc(name)
		plain := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix}).fl
		first := putDataset(t, ctx, plain, desc, rec.Schema(), rec)

		hook := &commitHook{ran: false}
		fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, redisHook: hook}).fl
		other := int64Record(t, []string{"n"}, []int64{9})
		defer other.Release()
		if _, err := tryPutDataset(ctx, fc, desc, other.Schema(), other); err == nil {
			t.Error("an upload whose commit failed succeeded")
		}
		got, rows := readDataset(t, ctx, plain, desc)
		if got.Generation != first.Generation {
			t.Errorf("current generation %d, want the first upload's %d", got.Generation, first.Generation)
		}
		wantRows(t, rows, "1", "2", "3")
		// The failed upload's generation expires like a replaced one.
		key := prefix + "ds:{" + name + "}:g" + fmt.Sprint(first.Generation+1)
		if ttl := admin.PTTL(ctx, key).Val(); ttl <= 0 || ttl > replacedGrace {
			t.Errorf("the failed upload's generation %s has TTL %v, want at most %v", key, ttl, replacedGrace)
		}
	})
}

// Dataset tickets can name columns and a range of batches, and a server
// can split a dataset into several endpoints.
func TestIntegrationDatasetReads(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	const prefix = "it_flight_reads:"
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, prefix+"*")) })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil)
	var recs []arrow.RecordBatch
	for b := range 10 {
		bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for i := b * 100; i < (b+1)*100; i++ {
			bld.Field(0).(*array.Int64Builder).Append(int64(i))
			bld.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("r%d", i))
		}
		recs = append(recs, bld.NewRecordBatch())
		bld.Release()
	}
	defer releaseAll(recs)
	fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix}).fl
	// The same reads of a generation stored by column, and of one stored
	// as a LIST by an earlier server.
	for _, layout := range []string{"hash", "list"} {
		t.Run(layout, func(t *testing.T) {
			name := uniqueName("it_flight_reads")
			var m datasetMeta
			if layout == "hash" {
				m = putDataset(t, ctx, fc, datasetDesc(name), schema, recs...)
			} else {
				m = putListGeneration(t, admin, prefix, name, schema, recs...)
			}
			if m.Batches != 10 {
				t.Fatalf("stored %d batches, want 10", m.Batches)
			}
			read := func(fc flight.Client, tk map[string]any) (*arrow.Schema, []string, int, error) {
				t.Helper()
				tk["dataset"] = name
				b, _ := json.Marshal(tk)
				return getDataset(t, ctx, fc, b)
			}
			names := func(s *arrow.Schema) string {
				var out []string
				for _, f := range s.Fields() {
					out = append(out, f.Name)
				}
				return strings.Join(out, ",")
			}

			s, rows, _, err := read(fc, map[string]any{"columns": []string{"label", "n"}})
			if err != nil {
				t.Fatal(err)
			}
			if names(s) != "label,n" || len(rows) != 1000 || rows[0] != "r0|0" {
				t.Errorf("projection: columns %s, %d rows, first %q", names(s), len(rows), rows[0])
			}
			if s, rows, _, err = read(fc, map[string]any{"columns": []string{}}); err != nil || s.NumFields() != 0 || len(rows) != 1000 {
				t.Errorf("no columns: %v, %d rows, %v; want no fields, 1000 rows", s, len(rows), err)
			}
			for _, cols := range [][]string{{"nope"}, {"n", "n"}} {
				if _, _, _, err := read(fc, map[string]any{"columns": cols}); status.Code(err) != codes.InvalidArgument {
					t.Errorf("columns %v: err = %v, want InvalidArgument", cols, err)
				}
			}

			_, rows, batches, err := read(fc, map[string]any{"batches": []int{2, 5}})
			if err != nil || len(rows) != 300 || rows[0] != "200|r200" || rows[299] != "499|r499" || batches != 3 {
				t.Errorf("batches [2, 5): %d rows in %d batches, %v", len(rows), batches, err)
			}
			if _, rows, _, err = read(fc, map[string]any{"batches": []int{8, 99}}); err != nil || len(rows) != 200 || rows[0] != "800|r800" {
				t.Errorf("batches [8, 99): %d rows, %v", len(rows), err)
			}
			s, rows, _, err = read(fc, map[string]any{"batches": []int{5, 5}})
			if err != nil || len(rows) != 0 || names(s) != "n,label" {
				t.Errorf("batches [5, 5): schema %v, %d rows, %v", s, len(rows), err)
			}
			for _, r := range [][]int{{-1, 2}, {3, 2}, {1}} {
				if _, _, _, err := read(fc, map[string]any{"batches": r}); status.Code(err) != codes.InvalidArgument {
					t.Errorf("batches %v: err = %v, want InvalidArgument", r, err)
				}
			}
			if _, rows, _, err = read(fc, map[string]any{"columns": []string{"n"}, "batches": []int{9, 10}}); err != nil ||
				len(rows) != 100 || rows[0] != "900" || rows[99] != "999" {
				t.Errorf("column n, batches [9, 10): %d rows, %v", len(rows), err)
			}

			// Enterprise splits endpoints into 4, 4 and 2 batches. Community
			// keeps all ten batches in one endpoint, even when splitting is requested.
			fc4 := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, DatasetEndpointBatches: 4}).fl
			info, err := fc4.GetFlightInfo(ctx, datasetDesc(name))
			if err != nil {
				t.Fatal(err)
			}
			wantBatches := []int{10}
			if FeatureCapabilities()["optimization"] {
				wantBatches = []int{4, 4, 2}
			}
			if len(info.Endpoint) != len(wantBatches) {
				t.Fatalf("got %d endpoints, want %d", len(info.Endpoint), len(wantBatches))
			}
			var all []string
			for i, ep := range info.Endpoint {
				_, rows, batches, err := getDataset(t, ctx, fc4, ep.Ticket.Ticket)
				if err != nil {
					t.Fatal(err)
				}
				if want := wantBatches[i]; batches != want {
					t.Errorf("endpoint %d has %d batches, want %d", i, batches, want)
				}
				all = append(all, rows...)
			}
			if len(all) != 1000 || all[0] != "0|r0" || all[999] != "999|r999" {
				t.Errorf("all endpoints: %d rows", len(all))
			}
		})
	}
}

// lrangeHook records the element ranges of the LRANGEs on keys containing
// match.
type lrangeHook struct {
	match  string
	mu     sync.Mutex
	ranges [][2]int64
}

func (h *lrangeHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *lrangeHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (h *lrangeHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if args := cmd.Args(); cmd.Name() == "lrange" && len(args) == 4 && strings.Contains(fmt.Sprint(args[1]), h.match) {
			start, _ := args[2].(int64)
			stop, _ := args[3].(int64)
			h.mu.Lock()
			h.ranges = append(h.ranges, [2]int64{start, stop})
			h.mu.Unlock()
		}
		return next(ctx, cmd)
	}
}

// Generations stored by column and as LISTs (by earlier servers, as during
// a rolling upgrade) replace each other, and each stays readable as such.
func TestIntegrationDatasetLayouts(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	const prefix = "it_flight_layouts:"
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, prefix+"*")) })
	fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix}).fl
	ds := &datasets{prefix: prefix}
	name := uniqueName("it_flight_layouts")
	keys := ds.keys(name)
	rec := func(base int64) arrow.RecordBatch {
		r := int64Record(t, []string{"a", "b"}, []int64{base, base + 1}, []int64{-base, -base - 1})
		t.Cleanup(r.Release)
		return r
	}
	read := func(gen int64, columns ...string) ([]string, error) {
		t.Helper()
		tkt, _ := json.Marshal(datasetTicket{Dataset: name, Generation: gen, Columns: columns})
		_, rows, _, err := getDataset(t, ctx, fc, tkt)
		return rows, err
	}
	wantType := func(gen int64, want string) {
		t.Helper()
		if typ, err := admin.Type(ctx, keys.gen(gen)).Result(); err != nil || typ != want {
			t.Errorf("generation %d is a %q, %v; want %s", gen, typ, err, want)
		}
	}
	wantGrace := func(gen int64) {
		t.Helper()
		if ttl, err := admin.PTTL(ctx, keys.gen(gen)).Result(); err != nil || ttl <= 0 || ttl > replacedGrace {
			t.Errorf("replaced generation %d expires in %v, %v; want within %v", gen, ttl, err, replacedGrace)
		}
	}
	r1, r2, r3 := rec(10), rec(20), rec(30)

	g1 := putListGeneration(t, admin, prefix, name, r1.Schema(), r1).Generation
	wantType(g1, "list")
	if rows, err := read(0, "b"); err != nil {
		t.Fatal(err)
	} else {
		wantRows(t, rows, "-10", "-11")
	}

	g2 := putDataset(t, ctx, fc, datasetDesc(name), r2.Schema(), r2).Generation
	wantType(g2, "hash")
	wantGrace(g1)
	if rows, err := read(0); err != nil {
		t.Fatal(err)
	} else {
		wantRows(t, rows, "20|-20", "21|-21")
	}
	if rows, err := read(g1); err != nil {
		t.Fatal(err)
	} else {
		wantRows(t, rows, "10|-10", "11|-11")
	}
	// What a server before the HASH layout gets for it.
	if err := admin.LLen(ctx, keys.gen(g2)).Err(); !isWrongType(err) {
		t.Errorf("LLEN of a HASH generation: %v, want WRONGTYPE", err)
	}

	// An earlier server's upload replaces it, and it expires like any.
	g3 := putListGeneration(t, admin, prefix, name, r3.Schema(), r3).Generation
	wantType(g3, "list")
	wantGrace(g2)
	if rows, err := read(0, "a"); err != nil {
		t.Fatal(err)
	} else {
		wantRows(t, rows, "30", "31")
	}
	if rows, err := read(g2, "b", "a"); err != nil {
		t.Fatal(err)
	} else {
		wantRows(t, rows, "-20|20", "-21|21")
	}

	// A drop deletes the current generation, and old tickets are gone.
	drop, err := fc.DoAction(ctx, &flight.Action{Type: actionDropDataset, Body: []byte(name)})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := drop.Recv(); err == nil; _, err = drop.Recv() {
	}
	if n, err := admin.Exists(ctx, keys.meta, keys.gen(g3)).Result(); err != nil || n != 0 {
		t.Errorf("after the drop, %d of the dataset's keys are left, %v", n, err)
	}
	if _, err := read(g2); status.Code(err) != codes.NotFound {
		t.Errorf("an old ticket after the drop: %v, want NotFound", err)
	}

	// A generation stored by column that isn't whole.
	g4 := putDataset(t, ctx, fc, datasetDesc(name), r2.Schema(), r2).Generation
	if err := admin.HSet(ctx, keys.gen(g4), "layout", "9").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(0); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("an unknown layout: %v, want FailedPrecondition", err)
	}
	if err := admin.HSet(ctx, keys.gen(g4), "layout", layoutColumns).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.HDel(ctx, keys.gen(g4), "0:1").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(0, "a"); err != nil {
		t.Errorf("a column that is there: %v", err)
	}
	if _, err := read(0, "b"); status.Code(err) != codes.DataLoss {
		t.Errorf("a missing column: %v, want DataLoss", err)
	}
	if err := admin.HDel(ctx, keys.gen(g4), "batches").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(0); status.Code(err) != codes.NotFound {
		t.Errorf("a generation without a batch count: %v, want NotFound", err)
	}
}

// hmgetHook records the fields of the HMGETs on keys containing match, and
// the bytes of the values they returned.
type hmgetHook struct {
	match  string
	mu     sync.Mutex
	fields [][]string
	bytes  int64
}

func (h *hmgetHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *hmgetHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (h *hmgetHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if args := cmd.Args(); cmd.Name() == "hmget" && len(args) > 2 && strings.Contains(fmt.Sprint(args[1]), h.match) {
			fields := make([]string, 0, len(args)-2)
			for _, a := range args[2:] {
				fields = append(fields, fmt.Sprint(a))
			}
			var n int64
			if sc, ok := cmd.(*goredis.SliceCmd); ok {
				for _, v := range sc.Val() {
					if s, ok := v.(string); ok {
						n += int64(len(s))
					}
				}
			}
			h.mu.Lock()
			h.fields = append(h.fields, fields)
			h.bytes += n
			h.mu.Unlock()
		}
		return err
	}
}

// A dataset read takes about readPageBytes of the columns it reads per
// HMGET (LRANGE, for a LIST generation), so it holds a few large batches at
// a time, and still many small ones; and it takes only those columns.
func TestIntegrationDatasetReadPages(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	const prefix = "it_flight_pages:"
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, prefix+"*")) })
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil)
	// records makes batches of rows rows with labels of pad bytes.
	records := func(batches, rows, pad int) []arrow.RecordBatch {
		fill := strings.Repeat("x", pad)
		var recs []arrow.RecordBatch
		for b := range batches {
			bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			for i := b * rows; i < (b+1)*rows; i++ {
				bld.Field(0).(*array.Int64Builder).Append(int64(i))
				bld.Field(1).(*array.StringBuilder).Append(fill)
			}
			recs = append(recs, bld.NewRecordBatch())
			bld.Release()
		}
		t.Cleanup(func() { releaseAll(recs) })
		return recs
	}
	upload := func(recs []arrow.RecordBatch) string {
		t.Helper()
		name := uniqueName("it_flight_pages")
		fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix}).fl
		if m := putDataset(t, ctx, fc, datasetDesc(name), schema, recs...); m.Batches != int64(len(recs)) {
			t.Fatalf("stored %d batches, want %d", m.Batches, len(recs))
		}
		return name
	}
	// read reads a dataset with a ticket's columns, and checks its n column
	// counts up from 0.
	read := func(name string, rows int, hook goredis.Hook, columns ...string) {
		t.Helper()
		fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, redisHook: hook}).fl
		tkt, _ := json.Marshal(datasetTicket{Dataset: name, Columns: columns})
		stream, err := fc.DoGet(ctx, &flight.Ticket{Ticket: tkt})
		if err != nil {
			t.Fatal(err)
		}
		rdr, err := flight.NewRecordReader(stream)
		if err != nil {
			t.Fatal(err)
		}
		defer rdr.Release()
		var n int64
		for rdr.Next() {
			col := rdr.RecordBatch().Column(0).(*array.Int64)
			for i := range col.Len() {
				if col.Value(i) != n {
					t.Fatalf("row %d has n = %d", n, col.Value(i))
				}
				n++
			}
		}
		if err := rdr.Err(); err != nil || n != int64(rows) {
			t.Fatalf("read %d rows, want %d: %v", n, rows, err)
		}
	}
	// pages returns the batches each HMGET after the first (the
	// generation's schema) asked for, checking they cover the batches in
	// order, and the columns it asked for of each.
	pages := func(hook *hmgetHook, cols string) []int {
		t.Helper()
		var lens []int
		next := 0
		for _, fields := range hook.fields[1:] {
			batches := map[string]bool{}
			for _, f := range fields {
				if f == "batches" {
					continue // asked with every page
				}
				b, c, _ := strings.Cut(f, ":")
				if !strings.Contains(","+cols+",", ","+c+",") {
					t.Fatalf("HMGET of field %s, want only columns %s", f, cols)
				}
				batches[b] = true
			}
			for range batches {
				if !batches[strconv.Itoa(next)] {
					t.Fatalf("HMGETs %v don't cover the batches in order", hook.fields)
				}
				next++
			}
			lens = append(lens, len(batches))
		}
		return lens
	}

	// About 1.5 MiB each: 4 batches (sized for 2 MiB ones), then 5 per page
	// (8 MiB / 1.5 MiB).
	large := records(12, 1000, 1500)
	name := upload(large)
	all := &hmgetHook{match: name}
	read(name, 12_000, all)
	if lens := pages(all, "0,1"); !slices.Equal(lens, []int{4, 5, 3}) {
		t.Errorf("batches of 1.5 MiB were read %v at a time, want [4 5 3]", lens)
	}
	// Their n column alone: 4 batches, then the rest, and a small part of
	// the bytes.
	one := &hmgetHook{match: name}
	read(name, 12_000, one, "n")
	if lens := pages(one, "0"); !slices.Equal(lens, []int{4, 8}) {
		t.Errorf("one column of batches of 1.5 MiB was read %v batches at a time, want [4 8]", lens)
	}
	if one.bytes*50 > all.bytes {
		t.Errorf("reading one column took %d bytes from Redis, reading both %d", one.bytes, all.bytes)
	}
	// No columns: their row counts, 4 batches, then the rest at once.
	none := &hmgetHook{match: name}
	fcNone := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, redisHook: none}).fl
	tkt, _ := json.Marshal(map[string]any{"dataset": name, "columns": []string{}})
	if _, rows, _, err := getDataset(t, ctx, fcNone, tkt); err != nil || len(rows) != 12_000 {
		t.Fatalf("no columns: %d rows, %v", len(rows), err)
	}
	if lens := pages(none, "rows"); !slices.Equal(lens, []int{4, 8}) {
		t.Errorf("no columns of 12 batches were read %v batches at a time, want [4 8]", lens)
	}
	// Small batches: 4, then the rest at once.
	small := upload(records(10, 100, 10))
	hook := &hmgetHook{match: small}
	read(small, 1000, hook)
	if lens := pages(hook, "0,1"); !slices.Equal(lens, []int{4, 6}) {
		t.Errorf("small batches were read %v at a time, want [4 6]", lens)
	}

	// A LIST generation, stored by an earlier server: 4 elements (the
	// schema and 3 batches), then 5 batches per LRANGE.
	listName := uniqueName("it_flight_pages")
	putListGeneration(t, admin, prefix, listName, schema, large...)
	lhook := &lrangeHook{match: listName}
	read(listName, 12_000, lhook)
	var lens []int64
	next := int64(0)
	for _, r := range lhook.ranges {
		if r[0] != next {
			t.Fatalf("LRANGEs %v don't cover the list in order", lhook.ranges)
		}
		lens, next = append(lens, r[1]-r[0]+1), r[1]+1
	}
	if !slices.Equal(lens, []int64{4, 5, 4}) {
		t.Errorf("a LIST generation of batches of 1.5 MiB was read %v elements at a time, want [4 5 4]", lens)
	}
}

// holdCommitHook holds the first dataset commit until release is closed,
// after closing held.
type holdCommitHook struct {
	held, release chan struct{}
	once          sync.Once
}

func (h *holdCommitHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *holdCommitHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (h *holdCommitHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if isCommitScript(cmd) {
			h.once.Do(func() {
				close(h.held)
				<-h.release
			})
		}
		return next(ctx, cmd)
	}
}

// Of two uploads of one dataset, the one started later wins, even if the
// earlier one finishes last.
func TestIntegrationUploadOrder(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	const prefix = "it_flight_order:"
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, prefix+"*")) })

	hook := &holdCommitHook{held: make(chan struct{}), release: make(chan struct{})}
	slow := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, redisHook: hook}).fl
	fast := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix}).fl
	name := uniqueName("it_flight_order")
	desc := datasetDesc(name)
	older := int64Record(t, []string{"n"}, []int64{1})
	defer older.Release()
	newer := int64Record(t, []string{"n"}, []int64{2})
	defer newer.Release()

	type result struct {
		m   datasetMeta
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := tryPutDataset(ctx, slow, desc, older.Schema(), older)
		done <- result{m, err}
	}()
	select {
	case <-hook.held: // the older upload has its generation, and waits to commit
	case <-time.After(10 * time.Second):
		t.Fatal("the older upload never reached its commit")
	}
	m := putDataset(t, ctx, fast, desc, newer.Schema(), newer)
	close(hook.release)
	var r result
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the older upload never finished")
	}
	if r.err != nil {
		t.Fatalf("older upload: %v", r.err)
	}
	if !r.m.Superseded || r.m.Generation >= m.Generation {
		t.Errorf("older upload = generation %d, superseded %v; want below %d, superseded", r.m.Generation, r.m.Superseded, m.Generation)
	}
	got, rows := readDataset(t, ctx, fast, desc)
	if got.Generation != m.Generation {
		t.Errorf("current generation %d, want the newer upload's %d", got.Generation, m.Generation)
	}
	wantRows(t, rows, "2")
	key := prefix + "ds:{" + name + "}:g" + fmt.Sprint(r.m.Generation)
	if ttl := admin.PTTL(ctx, key).Val(); ttl <= 0 || ttl > replacedGrace {
		t.Errorf("the superseded upload's generation %s has TTL %v, want at most %v", key, ttl, replacedGrace)
	}
}

// With --dataset-ttl, a dataset's keys expire but its generation counter
// doesn't, so generation numbers never repeat.
func TestIntegrationDatasetTTLKeepsSeq(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	const prefix = "it_flight_ttl:"
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, prefix+"*")) })
	fc := newClients(t, Config{RedisURI: uri, KeyPrefix: prefix, DatasetTTL: time.Hour}).fl
	name := uniqueName("it_flight_ttl")
	rec := int64Record(t, []string{"n"}, []int64{1})
	defer rec.Release()
	m := putDataset(t, ctx, fc, datasetDesc(name), rec.Schema(), rec)

	base := prefix + "ds:{" + name + "}"
	for _, key := range []string{base, base + ":g" + fmt.Sprint(m.Generation)} {
		if ttl := admin.PTTL(ctx, key).Val(); ttl <= 0 || ttl > time.Hour {
			t.Errorf("%s has TTL %v, want at most 1h", key, ttl)
		}
	}
	if n := admin.Exists(ctx, base+":seq").Val(); n != 1 {
		t.Fatalf("%s:seq is missing", base)
	}
	if ttl := admin.PTTL(ctx, base+":seq").Val(); ttl > 0 {
		t.Errorf("%s:seq has TTL %v, want none", base, ttl)
	}
}

func sequence(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}

func adminClient(t testing.TB, uri string) goredis.UniversalClient {
	t.Helper()
	opts, err := clientOptions(uri, identity{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := newRedisClient(context.Background(), opts, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// eachNode runs fn on every node of a cluster (ACL users are per node), or
// on the one server.
func eachNode(admin goredis.UniversalClient, fn func(context.Context, *goredis.Client) error) error {
	ctx := context.Background()
	switch c := admin.(type) {
	case *goredis.ClusterClient:
		return c.ForEachShard(ctx, fn)
	case *goredis.Client:
		return fn(ctx, c)
	}
	return fmt.Errorf("unsupported client %T", admin)
}

// keysMatching scans every primary for keys matching pattern.
func keysMatching(t testing.TB, admin goredis.UniversalClient, pattern string) []string {
	t.Helper()
	var (
		mu   sync.Mutex
		keys []string
	)
	scan := func(ctx context.Context, c *goredis.Client) error {
		it := c.Scan(ctx, 0, pattern, 1000).Iterator()
		for it.Next(ctx) {
			mu.Lock()
			keys = append(keys, it.Val())
			mu.Unlock()
		}
		return it.Err()
	}
	var err error
	if c, ok := admin.(*goredis.ClusterClient); ok {
		err = c.ForEachMaster(context.Background(), scan)
	} else {
		err = scan(context.Background(), admin.(*goredis.Client))
	}
	if err != nil {
		t.Errorf("SCAN %s: %v", pattern, err)
	}
	return keys
}

// deleteKeys deletes keys one per command, so keys in different cluster
// slots can be deleted together.
func deleteKeys(admin goredis.UniversalClient, keys []string) {
	ctx := context.Background()
	_, _ = admin.Pipelined(ctx, func(p goredis.Pipeliner) error {
		for _, k := range keys {
			p.Del(ctx, k)
		}
		return nil
	})
}

// A token whose Redis credentials stopped working is refused for that
// reason, not as Redis being down, so the client logs in again instead of
// retrying.
