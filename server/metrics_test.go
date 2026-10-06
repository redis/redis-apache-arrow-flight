package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// scrape returns what h serves at /metrics.
func scrape(t testing.TB, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: %d %s", rec.Code, rec.Body)
	}
	return rec.Body.String()
}

// metricValue returns a series' value in a scrape (labels in name order,
// as Prometheus writes them), or fails if there is none.
func metricValue(t testing.TB, text, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("%s: %v", line, err)
			}
			return f
		}
	}
	t.Fatalf("no %s in the metrics", series)
	return 0
}

const unreachableRedis = "redis://127.0.0.1:1/0" // nothing listens on port 1

// Calls are counted and timed by method and code, health checks aren't,
// and the gauges are there from the start.
func TestMetricsCalls(t *testing.T) {
	ctx := context.Background()
	srv, lis := startServer(t, Config{RedisURI: unreachableRedis})
	cc := dialTestServer(t, lis)
	fc := flight.NewFlightServiceClient(cc)
	actions, err := fc.ListActions(ctx, &flight.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := actions.Recv(); err == nil; _, err = actions.Recv() {
	}
	if _, err := fc.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorPATH}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetFlightInfo without a path: %v", err)
	}
	if _, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}

	text := scrape(t, srv.MetricsHandler())
	for series, want := range map[string]float64{
		`redis_flight_calls_total{code="OK",method="ListActions"}`:                1,
		`redis_flight_calls_total{code="InvalidArgument",method="GetFlightInfo"}`: 1,
		`redis_flight_call_duration_seconds_count{method="ListActions"}`:          1,
		`redis_flight_queries_read_whole_total{reason="user_limit"}`:              0,
		`redis_flight_queries_streamed_total`:                                     0,
		`redis_flight_query_streams_open`:                                         0,
		`redis_flight_pool_connections{state="in_use"}`:                           0,
		`redis_flight_tenants_open`:                                               0,
		`redis_flight_sessions_open`:                                              0,
		`redis_flight_redis_up`:                                                   0,
		`redis_flight_build_info{version="` + Version + `"}`:                      1,
	} {
		if got := metricValue(t, text, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
	if strings.Contains(text, `method="Check"`) {
		t.Error("a health check was counted")
	}
	if !strings.Contains(text, "\ngo_goroutines ") {
		t.Error("no Go runtime metrics")
	}
}

// Under redis-acl, calls are counted before authentication.
func TestMetricsCountUnauthenticated(t *testing.T) {
	srv, lis := startServer(t, Config{RedisURI: unreachableRedis, Auth: AuthRedisACL})
	fc := flight.NewFlightServiceClient(dialTestServer(t, lis))
	if _, err := fc.GetFlightInfo(context.Background(), &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"x"}}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("GetFlightInfo without a token: %v", err)
	}
	if n := metricValue(t, scrape(t, srv.MetricsHandler()), `redis_flight_calls_total{code="Unauthenticated",method="GetFlightInfo"}`); n != 1 {
		t.Errorf("unauthenticated calls: %v, want 1", n)
	}
}

// Only requests that wait for a connection are timed.
func TestPoolWaitMetric(t *testing.T) {
	ctx := context.Background()
	m := newMetrics()
	pool := newConnPool(&fakeDB{}, 1, 1)
	pool.metrics = m
	c, err := pool.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := metricValue(t, scrape(t, m.handler()), "redis_flight_pool_wait_seconds_count"); n != 0 {
		t.Fatalf("a get that didn't wait was counted: %v", n)
	}
	got := make(chan adbc.ConnectionWithContext)
	go func() {
		c, _ := pool.get(ctx)
		got <- c
	}()
	time.Sleep(50 * time.Millisecond)
	pool.put(c, true)
	pool.put(<-got, true)
	text := scrape(t, m.handler())
	if n := metricValue(t, text, "redis_flight_pool_wait_seconds_count"); n != 1 {
		t.Errorf("waits = %v, want 1", n)
	}
	if s := metricValue(t, text, "redis_flight_pool_wait_seconds_sum"); s < 0.04 {
		t.Errorf("wait time = %vs, want about 0.05", s)
	}
}

// ServeMetrics serves until Shutdown, which ends it without an error.
func TestServeMetrics(t *testing.T) {
	srv, _ := startServer(t, Config{RedisURI: unreachableRedis})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.ServeMetrics(lis) }()
	url := "http://" + lis.Addr().String() + "/metrics"
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK || !strings.Contains(string(body), "redis_flight_build_info") {
		t.Errorf("GET /metrics: %d\n%s", res.StatusCode, body)
	}
	srv.Shutdown()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("ServeMetrics after Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeMetrics didn't return after Shutdown")
	}
	if res, err := http.Get(url); err == nil {
		res.Body.Close()
		t.Error("metrics are still served after Shutdown")
	}
}

// The metrics follow streamed and read-whole results, sessions and
// datasets.
func TestIntegrationMetrics(t *testing.T) {
	uri := redisURI(t)
	ctx := context.Background()
	cfg := streamConfig(uri)
	cfg.PoolMaxOpen = 2 // one stream token
	cfg.KeyPrefix = "it_flight_metrics:"
	srv, lis := startServer(t, cfg)
	c := plainClient(t, lis)
	admin := adminClient(t, uri)
	t.Cleanup(func() { deleteKeys(admin, keysMatching(t, admin, cfg.KeyPrefix+"*")) })
	table := uniqueName("it_flight_metrics")
	dropTable(t, c, table)
	ingestRows(t, c, table, streamRows, 200) // more than gRPC buffers: streams stay open until read
	scan := "SELECT id, label FROM " + table
	metric := func(series string) float64 {
		t.Helper()
		return metricValue(t, scrape(t, srv.MetricsHandler()), series)
	}
	want := func(series string, v float64) {
		t.Helper()
		if got := metric(series); got != v {
			t.Errorf("%s = %v, want %v", series, got, v)
		}
	}

	streamed := openRead(t, ctx, c, scan)
	if !streamed.Next() {
		t.Fatal(streamed.Err())
	}
	want("redis_flight_queries_streamed_total", 1)
	want("redis_flight_query_streams_open", 1)
	want("redis_flight_tenants_open", 1)
	if n := metric(`redis_flight_pool_connections{state="in_use"}`); n < 1 {
		t.Errorf("pool connections in use = %v with a result streaming", n)
	}
	whole := openRead(t, ctx, c, scan) // no stream token left
	if !whole.Next() {
		t.Fatal(whole.Err())
	}
	want(`redis_flight_queries_read_whole_total{reason="user_limit"}`, 1)
	for _, rdr := range []*flight.Reader{streamed, whole} {
		if _, err := readIDs(rdr); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(5 * time.Second); metric("redis_flight_query_streams_open") != 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("query_streams_open didn't go back to 0")
		}
	}
	if _, err := tryQuery(ctx, c, "WITH x AS (SELECT id FROM "+table+") SELECT id FROM x"); err != nil {
		t.Fatal(err)
	}
	want(`redis_flight_queries_read_whole_total{reason="query"}`, 1)

	// A session with a temporary table reads on its connection.
	sc := cookieClient(t, lis)
	query(t, ctx, sc, "SELECT 1")
	update(t, ctx, sc, "CREATE TEMP TABLE it_flight_metrics_tmp AS SELECT 1 AS id")
	if _, err := tryQuery(ctx, sc, scan); err != nil {
		t.Fatal(err)
	}
	want(`redis_flight_queries_read_whole_total{reason="session"}`, 1)
	want("redis_flight_sessions_open", 1)
	want("redis_flight_session_connections", 1)

	// Datasets: rows and bytes, in and out.
	rec := int64Record(t, []string{"n"}, sequence(100))
	defer rec.Release()
	fc := flight.NewClientFromConn(dialTestServer(t, lis), nil)
	name := uniqueName("it_flight_metrics")
	m := putDataset(t, ctx, fc, datasetDesc(name), rec.Schema(), rec)
	tkt := []byte(`{"dataset": "` + name + `"}`)
	if _, rows, _, err := getDataset(t, ctx, fc, tkt); err != nil || len(rows) != 100 {
		t.Fatalf("read %d rows, %v", len(rows), err)
	}
	want("redis_flight_dataset_rows_written_total", 100)
	want("redis_flight_dataset_bytes_written_total", float64(m.Bytes))
	want("redis_flight_dataset_rows_read_total", 100)
	if n := metric("redis_flight_dataset_bytes_read_total"); n < float64(m.Bytes) {
		t.Errorf("dataset_bytes_read_total = %v, want at least %d", n, m.Bytes)
	}
	want("redis_flight_redis_up", 1)
}
