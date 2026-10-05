package server

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamRows spans three of the driver's cursor pages (10,000 rows), so a
// streaming result keeps its cursor open after its first batch.
const streamRows = 25_000

// streamConfig makes the driver stream results in batches of 500 rows.
func streamConfig(uri string) Config {
	return Config{RedisURI: uri, ADBCOptions: map[string]string{"adbc.redis.stream_batch_rows": "500"}}
}

// ingestRows creates table with n rows: id 0…n-1, and a label of pad bytes.
func ingestRows(t *testing.T, c *flightsql.Client, table string, n, pad int) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	fill := strings.Repeat("x", pad)
	for i := range n {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("%d%s", i, fill))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if _, err := c.ExecuteIngest(context.Background(), rdr, &flightsql.ExecuteIngestOpts{Table: table,
		TableDefinitionOptions: &flightsql.TableDefinitionOptions{
			IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
			IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionFail,
		}}); err != nil {
		t.Fatal(err)
	}
}

// openRead runs sql and opens its stream.
func openRead(t *testing.T, ctx context.Context, c *flightsql.Client, sql string) *flight.Reader {
	t.Helper()
	info, err := c.Execute(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rdr.Release)
	return rdr
}

// readIDs reads the rest of a stream's first column.
func readIDs(rdr *flight.Reader) ([]int64, error) {
	var ids []int64
	for rdr.Next() {
		col := rdr.RecordBatch().Column(0).(*array.Int64)
		for i := range col.Len() {
			ids = append(ids, col.Value(i))
		}
	}
	return ids, rdr.Err()
}

// cursors returns the number of open cursors on a table's index, or -1 on
// a cluster, whose coordinator FT.INFO doesn't count.
func cursors(t *testing.T, admin goredis.UniversalClient, table string) int64 {
	t.Helper()
	if _, ok := admin.(*goredis.ClusterClient); ok {
		return -1
	}
	reply, err := admin.Do(context.Background(), "FT.INFO", "idx:public:"+table).Result()
	if err != nil {
		t.Fatal(err)
	}
	stats, ok := replyField(reply, "cursor_stats")
	if !ok {
		t.Fatal("no cursor_stats in FT.INFO")
	}
	n, _ := replyField(stats, "index_total")
	return n.(int64)
}

// replyField reads a field of a RESP2 (flat list) or RESP3 (map) reply.
func replyField(reply any, name string) (any, bool) {
	switch r := reply.(type) {
	case map[any]any:
		v, ok := r[name]
		return v, ok
	case []any:
		for i := 0; i+1 < len(r); i += 2 {
			if r[i] == name {
				return r[i+1], true
			}
		}
	}
	return nil, false
}

// waitCursors waits for a table's index to have n open cursors (on
// standalone Redis; on a cluster it can't tell).
func waitCursors(t *testing.T, admin goredis.UniversalClient, table string, n int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got := cursors(t, admin, table)
		if got == -1 || got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has %d open cursors, want %d", table, got, n)
		}
	}
}

// connFree reports whether a query gets a connection within a short time.
func connFree(t *testing.T, c *flightsql.Client) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	info, err := c.Execute(ctx, "SELECT 1")
	if err == nil {
		var rdr *flight.Reader
		if rdr, err = c.DoGet(ctx, info.Endpoint[0].Ticket); err == nil {
			_, err = readIDs(rdr)
			rdr.Release()
		}
	}
	if err != nil && status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("SELECT 1: %v", err)
	}
	return err == nil
}

// waitStreamFree waits until a scan of table streams with a connection
// free beside it, which shows a stream that ended gave back its connection
// and its stream. On a cluster, where cursors can't be counted, it only
// checks the connection.
func waitStreamFree(t *testing.T, c *flightsql.Client, admin goredis.UniversalClient, table string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		ctx, cancel := context.WithCancel(context.Background())
		rdr := openRead(t, ctx, c, "SELECT id FROM "+table)
		if !rdr.Next() {
			cancel()
			t.Fatal(rdr.Err())
		}
		streamed := cursors(t, admin, table) != 0
		free := connFree(t, c)
		cancel()
		waitCursors(t, admin, table, 0)
		if streamed && free {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after a stream ended, a scan streamed: %v, and a connection was free beside it: %v", streamed, free)
		}
	}
}

// openCursors opens n cursors on a table's index, as other clients would,
// and deletes them when the test ends.
func openCursors(t *testing.T, admin goredis.UniversalClient, table string, n int) {
	t.Helper()
	ctx := context.Background()
	index := "idx:public:" + table
	var ids []int64
	t.Cleanup(func() {
		for _, id := range ids {
			_ = admin.Do(ctx, "FT.CURSOR", "DEL", index, id).Err()
		}
	})
	for range n {
		reply, err := admin.Do(ctx, "FT.AGGREGATE", index, "*", "WITHCURSOR", "COUNT", 1).Slice()
		if err != nil {
			t.Fatalf("opening cursor %d of %d: %v", len(ids)+1, n, err)
		}
		id, ok := reply[len(reply)-1].(int64)
		if !ok || id == 0 {
			t.Fatalf("FT.AGGREGATE WITHCURSOR = %v: no cursor left open", reply)
		}
		ids = append(ids, id)
	}
}

func TestIntegrationStreamResults(t *testing.T) {
	uri := redisURI(t)
	admin := adminClient(t, uri)
	setup := newClients(t, Config{RedisURI: uri}).sql
	table := uniqueName("it_flight_stream")
	dropTable(t, setup, table)
	ingestRows(t, setup, table, streamRows, 0)
	scan := "SELECT id FROM " + table

	t.Run("a stream holds a connection until it is read, and leaves the user one", func(t *testing.T) {
		cfg := streamConfig(uri)
		cfg.PoolMaxOpen = 2
		c := newClients(t, cfg).sql
		rdr := openRead(t, context.Background(), c, scan)
		if !rdr.Next() || rdr.RecordBatch().NumRows() != 500 {
			t.Fatalf("first batch: %v", rdr.Err())
		}
		open := cursors(t, admin, table)
		if open == 0 {
			t.Error("no cursor is open after the first batch")
		}
		if !connFree(t, c) {
			t.Error("a stream took the user's last connection")
		}
		// That was the user's one stream (PoolMaxOpen-1), so this is whole.
		whole := openRead(t, context.Background(), c, scan)
		if !whole.Next() {
			t.Fatal(whole.Err())
		}
		first := int(whole.RecordBatch().NumRows())
		if n := cursors(t, admin, table); n > open {
			t.Errorf("a second result streamed: %d cursors open, was %d", n, open)
		}
		ids, err := readIDs(rdr)
		if err != nil {
			t.Fatal(err)
		}
		if ids = append(sequence(500), ids...); !slices.Equal(ids, sequence(streamRows)) {
			t.Errorf("got %d ids, not 0…%d in order", len(ids), streamRows-1)
		}
		if ids, err := readIDs(whole); err != nil || first+len(ids) != streamRows {
			t.Errorf("the whole result: %d rows, %v", first+len(ids), err)
		}
		waitCursors(t, admin, table, 0)
		waitStreamFree(t, c, admin, table)
	})

	t.Run("with a pool of 1, results are read whole", func(t *testing.T) {
		cfg := streamConfig(uri)
		cfg.PoolMaxOpen = 1
		c := newClients(t, cfg).sql
		rdr := openRead(t, context.Background(), c, scan)
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		first := int(rdr.RecordBatch().NumRows())
		if n := cursors(t, admin, table); n > 0 {
			t.Errorf("%d cursors are open with a pool of 1", n)
		}
		if !connFree(t, c) {
			t.Error("a result kept the only connection")
		}
		if ids, err := readIDs(rdr); err != nil || first+len(ids) != streamRows {
			t.Errorf("%d rows, %v", first+len(ids), err)
		}
	})

	t.Run("a cancelled read frees the connection", func(t *testing.T) {
		cfg := streamConfig(uri)
		cfg.PoolMaxOpen = 2
		c := newClients(t, cfg).sql
		ctx, cancel := context.WithCancel(context.Background())
		rdr := openRead(t, ctx, c, scan)
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		cancel()
		waitCursors(t, admin, table, 0)
		waitStreamFree(t, c, admin, table)
	})

	t.Run("beyond MaxStreams, results are read whole", func(t *testing.T) {
		cfg := streamConfig(uri)
		cfg.PoolMaxOpen, cfg.MaxStreams = 4, 1
		c := newClients(t, cfg).sql
		var readers []*flight.Reader
		var firsts []int
		for range 2 {
			rdr := openRead(t, context.Background(), c, scan)
			if !rdr.Next() {
				t.Fatal(rdr.Err())
			}
			readers, firsts = append(readers, rdr), append(firsts, int(rdr.RecordBatch().NumRows()))
		}
		if n := cursors(t, admin, table); n > 1 {
			t.Errorf("%d cursors are open with MaxStreams 1", n)
		}
		for i, rdr := range readers {
			if ids, err := readIDs(rdr); err != nil || firsts[i]+len(ids) != streamRows {
				t.Errorf("result %d: %d rows, %v", i+1, firsts[i]+len(ids), err)
			}
		}
	})

	t.Run("beyond MaxTableStreams, results are read whole", func(t *testing.T) {
		other := uniqueName("it_flight_stream_other")
		dropTable(t, setup, other)
		ingestRows(t, setup, other, streamRows, 0)
		cfg := streamConfig(uri)
		cfg.MaxTableStreams = 1
		c := newClients(t, cfg).sql
		var readers []*flight.Reader
		var firsts []int
		for _, sql := range []string{scan, scan, "SELECT id FROM " + other} {
			rdr := openRead(t, context.Background(), c, sql)
			if !rdr.Next() {
				t.Fatal(rdr.Err())
			}
			readers, firsts = append(readers, rdr), append(firsts, int(rdr.RecordBatch().NumRows()))
		}
		if n := cursors(t, admin, table); n > 1 {
			t.Errorf("%d cursors are open on %s with MaxTableStreams 1", n, table)
		}
		if n := cursors(t, admin, other); n == 0 {
			t.Errorf("another table's result didn't stream")
		}
		for i, rdr := range readers {
			if ids, err := readIDs(rdr); err != nil || firsts[i]+len(ids) != streamRows {
				t.Errorf("result %d: %d rows, %v", i+1, firsts[i]+len(ids), err)
			}
		}
	})

	t.Run("an index out of cursors is RESOURCE_EXHAUSTED", func(t *testing.T) {
		if _, ok := admin.(*goredis.ClusterClient); ok {
			t.Skip("a cluster's shards count their own cursors")
		}
		ctx := context.Background()
		limit := 128
		if cfg, err := admin.ConfigGet(ctx, "search-index-cursor-limit").Result(); err == nil {
			if v, err := strconv.Atoi(cfg["search-index-cursor-limit"]); err == nil {
				limit = v
			}
		}
		full := uniqueName("it_flight_stream_full")
		dropTable(t, setup, full)
		ingestRows(t, setup, full, 10, 0)
		openCursors(t, admin, full, limit)
		c := newClients(t, streamConfig(uri)).sql
		info, err := c.Execute(ctx, "SELECT id FROM "+full)
		if err == nil {
			var rdr *flight.Reader
			if rdr, err = c.DoGet(ctx, info.Endpoint[0].Ticket); err == nil {
				_, err = readIDs(rdr)
				rdr.Release()
			}
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Errorf("a query on an index with %d cursors open: err = %v, want ResourceExhausted", limit, err)
		}
	})

	t.Run("a client that stalls gets an error", func(t *testing.T) {
		wide := uniqueName("it_flight_stream_wide")
		dropTable(t, setup, wide)
		ingestRows(t, setup, wide, streamRows, 200) // more than gRPC buffers
		cfg := streamConfig(uri)
		cfg.PoolMaxOpen, cfg.StreamStallTimeout = 2, 300*time.Millisecond
		c := newClients(t, cfg).sql
		rdr := openRead(t, context.Background(), c, "SELECT id, label FROM "+wide)
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		time.Sleep(time.Second)
		ids, err := readIDs(rdr)
		if status.Code(err) != codes.DeadlineExceeded || len(ids)+500 >= streamRows {
			t.Errorf("after stalling: %d more ids, err = %v; want DeadlineExceeded", len(ids), err)
		}
		waitCursors(t, admin, wide, 0)
		waitStreamFree(t, c, admin, wide)
	})

	t.Run("in a session, results are read whole", func(t *testing.T) {
		c := cookieClient(t, startTestServer(t, streamConfig(uri)))
		ctx := context.Background()
		query(t, ctx, c, "SELECT 1") // the session starts with the next request
		rdr := openRead(t, ctx, c, scan)
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		first := int(rdr.RecordBatch().NumRows())
		if n := cursors(t, admin, table); n > 0 {
			t.Errorf("%d cursors are open for a session's result", n)
		}
		// The session's connection is free for its next statement.
		wantRows(t, query(t, ctx, c, "SELECT 2"), "2")
		if ids, err := readIDs(rdr); err != nil || first+len(ids) != streamRows {
			t.Errorf("%d rows, %v", first+len(ids), err)
		}
	})
}
