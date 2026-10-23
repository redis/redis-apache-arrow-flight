package server

import (
	"context"
	"fmt"

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
		switch col := rdr.RecordBatch().Column(0).(type) {
		case *array.Int64:
			for i := range col.Len() {
				ids = append(ids, col.Value(i))
			}
		case *array.Int32: // SQL integer literals now have PostgreSQL's INTEGER type.
			for i := range col.Len() {
				ids = append(ids, int64(col.Value(i)))
			}
		default:
			return nil, fmt.Errorf("expected integer IDs, got %s", col.DataType())
		}
	}
	return ids, rdr.Err()
}

// cursors returns the number of open cursors on a public table's index,
// or -1 on a cluster, whose coordinator FT.INFO doesn't count.
func cursors(t *testing.T, admin goredis.UniversalClient, table string) int64 {
	t.Helper()
	return schemaCursors(t, admin, "public", table)
}

// schemaCursors is cursors for a table in schema.
func schemaCursors(t *testing.T, admin goredis.UniversalClient, schema, table string) int64 {
	t.Helper()
	if _, ok := admin.(*goredis.ClusterClient); ok {
		return -1
	}
	reply, err := admin.Do(context.Background(), "FT.INFO", "idx:"+schema+":"+table).Result()
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
	waitSchemaCursors(t, admin, "public", table, n)
}

// waitSchemaCursors is waitCursors for a table in schema.
func waitSchemaCursors(t *testing.T, admin goredis.UniversalClient, schema, table string, n int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		got := schemaCursors(t, admin, schema, table)
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

// metricsClient starts a server with cfg, and returns a client of it and a
// function that reads one of its metrics.
func metricsClient(t *testing.T, cfg Config) (*flightsql.Client, func(series string) float64) {
	t.Helper()
	srv, lis := startServer(t, cfg)
	return plainClient(t, lis), func(series string) float64 {
		t.Helper()
		return metricValue(t, scrape(t, srv.MetricsHandler()), series)
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
