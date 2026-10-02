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
	keys, _ := admin.Keys(ctx, "it_flight_res:result:*").Result()
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
	stream, err := fc.DoPut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	w.SetFlightDescriptor(desc)
	for _, rec := range recs {
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
	}
	// With no batches, Close sends the schema alone.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	res, err := stream.Recv()
	if err != nil {
		t.Fatalf("DoPut: %v", err)
	}
	var m datasetMeta
	if err := json.Unmarshal(res.AppMetadata, &m); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("DoPut ended with %v", err)
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
		keys, _ := admin.Keys(context.Background(), "it_flight_ds:*").Result()
		if len(keys) > 0 {
			admin.Del(context.Background(), keys...)
		}
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

func TestIntegrationRedisACL(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	admin := adminClient(t, uri)
	writer, reader := uniqueName("it_flight_w"), uniqueName("it_flight_r")
	// The reader's rules are the driver's read-only list, plus what reading
	// datasets needs.
	readRules := strings.Fields("on >reader-pw resetchannels %R~adbc:* %R~public:* %R~arrowflight:* " +
		"+ping +info +ft._list +ft.aggregate +ft.cursor +ft.search +ft.profile +get +exists +hget +hmget " +
		"+hgetall +smembers +sismember +llen +lrange +scan")
	for user, rules := range map[string][]string{
		writer: strings.Fields("on >writer-pw resetchannels ~* +@all"),
		reader: readRules,
	} {
		args := []any{"ACL", "SETUSER", user}
		for _, r := range rules {
			args = append(args, r)
		}
		if err := admin.Do(ctx, args...).Err(); err != nil {
			t.Fatalf("ACL SETUSER %s: %v", user, err)
		}
		t.Cleanup(func() { admin.Do(context.Background(), "ACL", "DELUSER", user) })
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
		keys, _ := admin.Keys(context.Background(), "arrowflight:ds:{"+tbl+"}*").Result()
		if len(keys) > 0 {
			admin.Del(context.Background(), keys...)
		}
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
