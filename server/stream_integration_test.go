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

// tryQuery runs sql and reads its result's first column.
func tryQuery(ctx context.Context, c *flightsql.Client, sql string) ([]int64, error) {
	info, err := c.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	return readIDs(rdr)
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

// cursorLimit is Redis's search-index-cursor-limit.
func cursorLimit(admin goredis.UniversalClient) int {
	limit := 128
	if cfg, err := admin.ConfigGet(context.Background(), "search-index-cursor-limit").Result(); err == nil {
		if v, err := strconv.Atoi(cfg["search-index-cursor-limit"]); err == nil {
			limit = v
		}
	}
	return limit
}

// openCursors opens n cursors on a table's index, as other clients would.
// It returns a function that deletes them, which also runs when the test
// ends.
func openCursors(t *testing.T, admin goredis.UniversalClient, table string, n int) func() {
	t.Helper()
	ctx := context.Background()
	index := "idx:public:" + table
	var ids []int64
	del := func() {
		for _, id := range ids {
			_ = admin.Do(ctx, "FT.CURSOR", "DEL", index, id).Err()
		}
		ids = nil
	}
	t.Cleanup(del)
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
	return del
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

	t.Run("results read whole for want of a stream take at most ResultMemory", func(t *testing.T) {
		wide := uniqueName("it_flight_stream_budget")
		dropTable(t, setup, wide)
		ingestRows(t, setup, wide, streamRows, 200) // about 5.4 MB, more than gRPC buffers
		wideScan := "SELECT id, label FROM " + wide
		ctx := context.Background()
		cfg := streamConfig(uri)
		cfg.PoolMaxOpen, cfg.ResultMemory = 2, 8<<20 // one stream; room for one result read whole
		c := newClients(t, cfg).sql

		stream := openRead(t, ctx, c, scan) // the user's one stream
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		held := openRead(t, ctx, c, wideScan) // read whole, and left unread
		if !held.Next() {
			t.Fatal(held.Err())
		}
		first := int(held.RecordBatch().NumRows())
		if n := cursors(t, admin, wide); n > 0 {
			t.Errorf("a result read whole kept %d cursors open", n)
		}
		_, err := tryQuery(ctx, c, wideScan)
		if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "-result-memory") {
			t.Errorf("a second result past ResultMemory: err = %v, want ResourceExhausted", err)
		}
		if n := cursors(t, admin, wide); n > 0 {
			t.Errorf("a result refused for ResultMemory left %d cursors open", n)
		}
		if !connFree(t, c) {
			t.Error("a result refused for ResultMemory kept its connection")
		}

		// Read to the end, the held result gives its memory back.
		if ids, err := readIDs(held); err != nil || first+len(ids) != streamRows {
			t.Fatalf("the held result: %d rows, %v", first+len(ids), err)
		}
		if ids, err := tryQuery(ctx, c, wideScan); err != nil || len(ids) != streamRows {
			t.Errorf("after the held result was read: %d rows, %v", len(ids), err)
		}
		if ids, err := readIDs(stream); err != nil || len(ids)+500 != streamRows {
			t.Errorf("the stream: %d rows, %v", len(ids)+500, err)
		}
	})

	t.Run("the server finds a table's index, also after a rename", func(t *testing.T) {
		ctx := context.Background()
		check := func(name string) {
			t.Helper()
			index, err := tableIndex(ctx, admin, streamSource{"public", name})
			if err != nil {
				t.Fatalf("tableIndex(%s): %v", name, err)
			}
			if err := admin.Do(ctx, "FT.INFO", index).Err(); err != nil {
				t.Errorf("FT.INFO %s, the index of %s: %v", index, name, err)
			}
		}
		check(table)
		old, renamed := uniqueName("it_flight_stream_old"), uniqueName("it_flight_stream_renamed")
		dropTable(t, setup, old)
		dropTable(t, setup, renamed)
		ingestRows(t, setup, old, 10, 0)
		update(t, ctx, setup, "ALTER TABLE "+old+" RENAME TO "+renamed)
		check(renamed)
	})

	t.Run("streams stop at three quarters of the index's cursors, counting other clients", func(t *testing.T) {
		if _, ok := admin.(*goredis.ClusterClient); ok {
			t.Skip("a cluster's FT.INFO doesn't count cursors")
		}
		ctx := context.Background()
		waitCursors(t, admin, table, 0)
		limit := cursorLimit(admin)
		others := limit - limit/4 - 1 // one short of three quarters
		closeOthers := openCursors(t, admin, table, others)
		c := newClients(t, streamConfig(uri)).sql
		var readers []*flight.Reader
		var firsts []int
		for i := range 2 {
			rdr := openRead(t, ctx, c, scan)
			if !rdr.Next() {
				t.Fatal(rdr.Err())
			}
			readers, firsts = append(readers, rdr), append(firsts, int(rdr.RecordBatch().NumRows()))
			// The first streams, to three quarters; the second is read whole.
			if n := cursors(t, admin, table); n != int64(others+1) {
				t.Errorf("after scan %d, with %d of %d cursors open to other clients: %d open, want %d",
					i+1, others, limit, n, others+1)
			}
		}
		for i, rdr := range readers {
			if ids, err := readIDs(rdr); err != nil || firsts[i]+len(ids) != streamRows {
				t.Errorf("result %d: %d rows, %v", i+1, firsts[i]+len(ids), err)
			}
		}

		// Once the other client's cursors are gone, and the count is read
		// again, results stream again.
		closeOthers()
		waitCursors(t, admin, table, 0)
		time.Sleep(cursorCheckInterval)
		rdr := openRead(t, ctx, c, scan)
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		if n := cursors(t, admin, table); n != 1 {
			t.Errorf("after the other client's cursors were closed, a scan left %d cursors open, want 1", n)
		}
		if _, err := readIDs(rdr); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("an index out of cursors is RESOURCE_EXHAUSTED", func(t *testing.T) {
		if _, ok := admin.(*goredis.ClusterClient); ok {
			t.Skip("a cluster's shards count their own cursors")
		}
		ctx := context.Background()
		limit := cursorLimit(admin)
		full := uniqueName("it_flight_stream_full")
		dropTable(t, setup, full)
		ingestRows(t, setup, full, 10, 0)
		openCursors(t, admin, full, limit)
		c := newClients(t, streamConfig(uri)).sql
		if _, err := tryQuery(ctx, c, "SELECT id FROM "+full); status.Code(err) != codes.ResourceExhausted {
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
