package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/redis-apache-arrow-flight/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestMain runs the command itself when a test starts the test binary as
// the server (see startCommand).
func TestMain(m *testing.M) {
	if os.Getenv("REDIS_FLIGHT_SERVER_TEST_MAIN") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// command is the server running as a process.
type command struct {
	cmd    *exec.Cmd
	addr   string
	exited chan struct{}
	err    error // once exited is closed
	// metricsAddr gets the metrics' address, if it serves them.
	metricsAddr chan string
}

var (
	servingAddr = regexp.MustCompile(`msg="serving Arrow Flight" addr=(\S+)`)
	metricsAddr = regexp.MustCompile(`msg="serving metrics" addr=(\S+)`)
)

// startCommand runs the command on a free port, with env added to the
// environment, and waits for it to serve. Its log goes to the test's.
func startCommand(t *testing.T, env []string, args ...string) *command {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-listen", "127.0.0.1:0"}, args...)...)
	cmd.Env = append(append(os.Environ(), "REDIS_FLIGHT_SERVER_TEST_MAIN=1"), env...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &command{cmd: cmd, exited: make(chan struct{}), metricsAddr: make(chan string, 1)}
	addr := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			t.Log(sc.Text())
			if m := servingAddr.FindStringSubmatch(sc.Text()); m != nil {
				addr <- m[1]
			}
			if m := metricsAddr.FindStringSubmatch(sc.Text()); m != nil {
				c.metricsAddr <- m[1]
			}
		}
		// Wait closes stderr, so it comes after the last read.
		c.err = cmd.Wait()
		close(c.exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-c.exited
	})
	select {
	case c.addr = <-addr:
	case <-c.exited:
		t.Fatalf("the server exited before serving: %v", c.err)
	case <-time.After(10 * time.Second):
		t.Fatal("the server didn't start serving")
	}
	return c
}

// wait waits for the process to exit and returns its error.
func (c *command) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-c.exited:
		return c.err
	case <-time.After(timeout):
		t.Fatalf("the server didn't exit within %v", timeout)
		return nil
	}
}

func dial(t *testing.T, addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(addr, append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// openPut opens a DoPut.
func openPut(t *testing.T, client flight.FlightServiceClient) flight.FlightService_DoPutClient {
	t.Helper()
	stream, err := client.DoPut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

// On SIGTERM the command lets running calls finish, cancels those still
// running at -shutdown-timeout, and exits once that is done.
func TestSIGTERM(t *testing.T) {
	// Nothing here reaches Redis: a DoPut waits for its first message.
	const timeout = time.Second
	c := startCommand(t, []string{"REDIS_URI=redis://127.0.0.1:1/0"}, "-shutdown-timeout", timeout.String())
	client := flight.NewFlightServiceClient(dial(t, c.addr))
	finishing, hung := openPut(t, client), openPut(t, client)
	time.Sleep(200 * time.Millisecond) // for both calls to reach the server

	start := time.Now()
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := finishing.Send(&flight.FlightData{DataHeader: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := finishing.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a call that finished after SIGTERM: err = %v, want its own InvalidArgument", err)
	}
	if err := c.wait(t, timeout+10*time.Second); err != nil {
		t.Errorf("the server exited with %v, want 0", err)
	}
	if d := time.Since(start); d < timeout {
		t.Errorf("the server exited %v after SIGTERM, before -shutdown-timeout, with a call running", d)
	}
	if _, err := hung.Recv(); err == nil || status.Code(err) == codes.InvalidArgument {
		t.Errorf("the call still running at the timeout: err = %v, want it cut off", err)
	}
}

// helperServer starts a server in this process, for setting up and
// checking what the command leaves in Redis.
func helperServer(t *testing.T, uri string) flight.Client {
	t.Helper()
	srv, err := server.New(server.Config{RedisURI: uri, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Shutdown)
	return flight.NewClientFromConn(dial(t, lis.Addr().String()), nil)
}

func sqlClient(fc flight.Client) *flightsql.Client {
	return &flightsql.Client{Client: fc, Alloc: memory.DefaultAllocator}
}

// rows makes a batch of ids from..to-1, each with a label of pad bytes.
func rows(from, to, pad int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	fill := strings.Repeat("x", pad)
	for i := from; i < to; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(fill)
	}
	return b.NewRecordBatch()
}

// read reads the rest of a stream and returns its row count.
func read(rdr *flight.Reader) (int, error) {
	n := 0
	for rdr.Next() {
		n += int(rdr.RecordBatch().NumRows())
	}
	return n, rdr.Err()
}

// cursors returns the number of open cursors on an index, or -1 on a
// cluster, whose shards count their own.
func cursors(t *testing.T, admin *goredis.Client, index string) int64 {
	t.Helper()
	if info, _ := admin.Info(context.Background(), "cluster").Result(); strings.Contains(info, "cluster_enabled:1") {
		return -1
	}
	reply, err := admin.Do(context.Background(), "FT.INFO", index).Result()
	if err != nil {
		t.Fatal(err)
	}
	field := func(reply any, name string) any {
		switch r := reply.(type) {
		case map[any]any:
			return r[name]
		case []any:
			for i := 0; i+1 < len(r); i += 2 {
				if r[i] == name {
					return r[i+1]
				}
			}
		}
		return nil
	}
	n, _ := field(field(reply, "cursor_stats"), "index_total").(int64)
	return n
}

// tempIndexes returns the indexes of temporary tables named name.
func tempIndexes(t *testing.T, admin *goredis.Client, name string) []string {
	t.Helper()
	all, err := admin.Do(context.Background(), "FT._LIST").StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	var temp []string
	for _, idx := range all {
		if strings.HasPrefix(idx, "idx:pg_temp_") && strings.HasSuffix(idx, ":"+name) {
			temp = append(temp, idx)
		}
	}
	return temp
}

// On SIGTERM, an upload under way finishes, and a stream still unread at
// -shutdown-timeout is cancelled. The command then deletes the stream's
// cursor and drops its session's temporary table before it exits.
func TestIntegrationSIGTERM(t *testing.T) {
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run the integration tests")
	}
	ctx := context.Background()
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	admin := goredis.NewClient(opts)
	t.Cleanup(func() { _ = admin.Close() })

	helper := helperServer(t, uri)
	id := time.Now().UnixNano() % 1_000_000_000_000
	table, temp, dataset := fmt.Sprintf("it_flight_sigterm_%d", id), fmt.Sprintf("it_flight_sigterm_tmp_%d", id),
		fmt.Sprintf("it_flight_sigterm_%d", id)
	t.Cleanup(func() {
		_, _ = sqlClient(helper).ExecuteUpdate(ctx, "DROP TABLE IF EXISTS "+table)
		if stream, err := helper.DoAction(ctx, &flight.Action{Type: "drop_dataset", Body: []byte(dataset)}); err == nil {
			for _, err := stream.Recv(); err == nil; _, err = stream.Recv() {
			}
		}
	})
	// Wide rows, more than gRPC buffers, so the stream keeps its cursor.
	const tableRows = 25_000
	rec := rows(0, tableRows, 2000)
	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	_, err = sqlClient(helper).ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{Table: table,
		TableDefinitionOptions: &flightsql.TableDefinitionOptions{
			IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
			IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionFail,
		}})
	rdr.Release()
	if err != nil {
		t.Fatal(err)
	}

	c := startCommand(t, nil, "-shutdown-timeout", "2s", "-adbc-option", "adbc.redis.stream_batch_rows=500")

	// A session with a temporary table.
	sfc, err := flight.NewClientWithMiddlewareCtx(ctx, c.addr, nil,
		[]flight.ClientMiddleware{flight.CreateClientMiddleware(flight.NewCookieMiddleware())},
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sfc.Close() })
	sess := sqlClient(sfc)
	query := func(sql string) int {
		t.Helper()
		info, err := sess.Execute(ctx, sql)
		if err != nil {
			t.Fatal(err)
		}
		rdr, err := sess.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			t.Fatal(err)
		}
		defer rdr.Release()
		n, err := read(rdr)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	query("SELECT 1") // gets the session cookie
	if _, err := sess.ExecuteUpdate(ctx, "CREATE TEMP TABLE "+temp+" AS SELECT 1 AS a"); err != nil {
		t.Fatal(err)
	}
	if got := tempIndexes(t, admin, temp); len(got) != 1 {
		t.Fatalf("the session's temporary table has indexes %v, want one", got)
	}

	// A result larger than the transport buffers, with its first batch read.
	// Community reads SQL whole; enterprise additionally holds a Redis cursor.
	fc := flight.NewClientFromConn(dial(t, c.addr), nil)
	info, err := sqlClient(fc).Execute(ctx, "SELECT id, label FROM "+table)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := fc.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	result, err := flight.NewRecordReader(stream)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if !result.Next() {
		t.Fatal(result.Err())
	}
	index := "idx:public:" + table
	if n := cursors(t, admin, index); server.FeatureCapabilities()["optimization"] && n == 0 {
		t.Fatal("the result isn't streaming: no cursor is open")
	}

	// An upload with its first batch sent.
	put, err := fc.DoPut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := rows(0, 100, 0)
	defer first.Release()
	w := flight.NewRecordWriter(put, ipc.WithSchema(first.Schema()))
	w.SetFlightDescriptor(&flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{dataset}})
	if err := w.Write(first); err != nil {
		t.Fatal(err)
	}

	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	// The upload finishes.
	second := rows(100, 200, 0)
	defer second.Release()
	if err := w.Write(second); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := put.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := put.Recv(); err != nil {
		t.Errorf("the upload under way at SIGTERM: %v", err)
	}
	if _, err := put.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("the upload ended with %v", err)
	}

	// The stream is cancelled at the timeout, and the server exits once it
	// has cleaned up.
	if err := c.wait(t, 15*time.Second); err != nil {
		t.Errorf("the server exited with %v, want 0", err)
	}
	if n := cursors(t, admin, index); n > 0 {
		t.Errorf("%d cursors are open after the server exited", n)
	}
	if got := tempIndexes(t, admin, temp); len(got) > 0 {
		t.Errorf("the session's temporary table is left after the server exited: %v", got)
	}
	if n, err := read(result); err == nil || 500+n >= tableRows {
		t.Errorf("the stream left unread got %d more rows, err = %v; want it cut off", n, err)
	}

	// The upload was kept.
	dinfo, err := helper.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{dataset}})
	if err != nil {
		t.Fatal(err)
	}
	dstream, err := helper.DoGet(ctx, dinfo.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	drdr, err := flight.NewRecordReader(dstream)
	if err != nil {
		t.Fatal(err)
	}
	defer drdr.Release()
	if got, err := read(drdr); err != nil || got != 200 {
		t.Errorf("the dataset uploaded during shutdown has %d rows, %v; want 200", got, err)
	}
}

// -metrics-listen serves the Prometheus metrics.
