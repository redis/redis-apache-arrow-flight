package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
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

func redisURI(t *testing.T) string {
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

func newClients(t *testing.T, cfg Config) clients {
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
		// The failed upload's list expires like a replaced generation.
		key := prefix + "ds:{" + name + "}:g" + fmt.Sprint(first.Generation+1)
		if ttl := admin.PTTL(ctx, key).Val(); ttl <= 0 || ttl > replacedGrace {
			t.Errorf("the failed upload's list %s has TTL %v, want at most %v", key, ttl, replacedGrace)
		}
	})
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
		t.Errorf("the superseded upload's list %s has TTL %v, want at most %v", key, ttl, replacedGrace)
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

func adminClient(t *testing.T, uri string) goredis.UniversalClient {
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
func keysMatching(t *testing.T, admin goredis.UniversalClient, pattern string) []string {
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
func TestIntegrationStaleCredentials(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	admin := adminClient(t, uri)
	user := uniqueName("it_flight_stale")
	acl := func(rules ...string) {
		t.Helper()
		args := []any{"ACL", "SETUSER", user}
		for _, r := range rules {
			args = append(args, r)
		}
		if err := eachNode(admin, func(ctx context.Context, c *goredis.Client) error {
			return c.Do(ctx, args...).Err()
		}); err != nil {
			t.Fatalf("ACL SETUSER %s %v: %v", user, rules, err)
		}
	}
	acl("on", ">old-pw", "resetchannels", "~*", "+@all")
	t.Cleanup(func() {
		_ = eachNode(admin, func(ctx context.Context, c *goredis.Client) error {
			return c.Do(ctx, "ACL", "DELUSER", user).Err()
		})
	})

	cl := newClients(t, Config{RedisURI: uri, Auth: AuthRedisACL, TokenSecret: "stale"})
	tctx, err := cl.fl.AuthenticateBasicToken(ctx, user, "old-pw")
	if err != nil {
		t.Fatal(err)
	}
	// Logging in doesn't connect the user's requests, and a failed connect
	// isn't kept, so each request below connects with the token's
	// credentials.
	for _, step := range []struct {
		change []string
		want   codes.Code
	}{
		{[]string{"resetpass", ">new-pw"}, codes.Unauthenticated},
		{[]string{"resetpass", ">old-pw", "-ping"}, codes.PermissionDenied},
		{[]string{"+ping", "off"}, codes.Unauthenticated},
	} {
		acl(step.change...)
		if _, err := cl.sql.Execute(tctx, "SELECT 1"); status.Code(err) != step.want {
			t.Errorf("after ACL SETUSER %v: err = %v, want %v", step.change, err, step.want)
		}
	}
}

func TestIntegrationRedisACL(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	admin := adminClient(t, uri)
	writer, reader := uniqueName("it_flight_w"), uniqueName("it_flight_r")
	// The reader's rules are the driver's read-only list, plus what reading
	// datasets needs.
	readRules := strings.Fields("on >reader-pw resetchannels %R~adbc:* %R~public:* %R~arrowflight:* " +
		"+ping +info +ft._list +ft.aggregate +ft.cursor +ft.search +ft.profile +get +exists +hget +hmget " +
		"+hgetall +smembers +sismember +llen +lrange +scan +cluster|slots +command")
	for user, rules := range map[string][]string{
		writer: strings.Fields("on >writer-pw resetchannels ~* +@all"),
		reader: readRules,
	} {
		args := []any{"ACL", "SETUSER", user}
		for _, r := range rules {
			args = append(args, r)
		}
		if err := eachNode(admin, func(ctx context.Context, c *goredis.Client) error {
			return c.Do(ctx, args...).Err()
		}); err != nil {
			t.Fatalf("ACL SETUSER %s: %v", user, err)
		}
		t.Cleanup(func() {
			_ = eachNode(admin, func(ctx context.Context, c *goredis.Client) error {
				return c.Do(ctx, "ACL", "DELUSER", user).Err()
			})
		})
	}

	cfg := Config{RedisURI: uri, Auth: AuthRedisACL, TokenSecret: "integration"}
	cl := newClients(t, cfg)
	c := cl.sql

	if _, err := c.Execute(ctx, "SELECT 1"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("call without a token: err = %v, want Unauthenticated", err)
	}
	if _, err := cl.fl.AuthenticateBasicToken(ctx, writer, "wrong"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong password: err = %v, want Unauthenticated", err)
	}
	wctx, err := cl.fl.AuthenticateBasicToken(ctx, writer, "writer-pw")
	if err != nil {
		t.Fatal(err)
	}
	rctx, err := cl.fl.AuthenticateBasicToken(ctx, reader, "reader-pw")
	if err != nil {
		t.Fatal(err)
	}

	tbl := uniqueName("it_flight_acl")
	t.Cleanup(func() { _, _ = c.ExecuteUpdate(wctx, "DROP TABLE IF EXISTS "+tbl) })
	update(t, wctx, c, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL)")
	update(t, wctx, c, "INSERT INTO "+tbl+" VALUES (1), (2)")
	wantRows(t, query(t, rctx, c, "SELECT COUNT(*) FROM "+tbl), "2")
	if _, err := c.ExecuteUpdate(rctx, "INSERT INTO "+tbl+" VALUES (3)"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("reader INSERT: err = %v, want PermissionDenied", err)
	}

	// The reader can't upload a dataset either.
	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{tbl}}
	stream, err := cl.fl.DoPut(rctx)
	if err != nil {
		t.Fatal(err)
	}
	rec := int64Record(t, []string{"n"}, []int64{1})
	defer rec.Release()
	w := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
	w.SetFlightDescriptor(desc)
	_ = w.Write(rec)
	_ = w.Close()
	_ = stream.CloseSend()
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Errorf("reader upload: err = %v, want PermissionDenied", err)
	}

	// The writer can, and the reader can then read and list it.
	t.Cleanup(func() {
		deleteKeys(admin, keysMatching(t, admin, "arrowflight:ds:{"+tbl+"}*"))
	})
	putDataset(t, wctx, cl.fl, desc, rec.Schema(), rec)
	info, err := cl.fl.GetFlightInfo(rctx, desc)
	if err != nil {
		t.Fatalf("reader GetFlightInfo: %v", err)
	}
	if _, rows, _, err := getDataset(t, rctx, cl.fl, info.Endpoint[0].Ticket.Ticket); err != nil || !slices.Equal(rows, []string{"1"}) {
		t.Errorf("reader DoGet = %q, %v", rows, err)
	}
	list, err := cl.fl.ListFlights(rctx, &flight.Criteria{Expression: []byte(tbl)})
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := list.Recv(); err != nil || fi.TotalRecords != 1 {
		t.Errorf("reader ListFlights = %v, %v", fi, err)
	}
	drop, err := cl.fl.DoAction(rctx, &flight.Action{Type: actionDropDataset, Body: []byte(tbl)})
	if err == nil {
		_, err = drop.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("reader drop: err = %v, want PermissionDenied", err)
	}

	// Another server with the same secret takes the writer's token.
	other := newClients(t, cfg).sql
	wantRows(t, query(t, wctx, other, "SELECT COUNT(*) FROM "+tbl), "2")
	// One with another secret doesn't.
	cfg.TokenSecret = "different"
	stranger := newClients(t, cfg).sql
	if _, err := stranger.Execute(wctx, "SELECT 1"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("token on a server with another secret: err = %v, want Unauthenticated", err)
	}
}
