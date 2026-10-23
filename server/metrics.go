package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"net/http"
	"time"
)

// metricsPrefix starts every metric's name.
const metricsPrefix = "redis_flight_"

// wholeReason is why a query's result was read whole into memory before
// it was sent, rather than streamed from Redis.
type wholeReason string

const (
	// wholeQuery: the driver doesn't stream the query (a join, a CTE, an
	// aggregate, …), or read its result whole.
	wholeQuery wholeReason = "query"
	// wholeSession: it ran on its session's own connection, since the
	// session may have temporary objects (or its settings couldn't be
	// carried to a pooled connection).
	wholeSession wholeReason = "session"
	// wholeUserLimit: the Redis user had MaxStreams results streaming
	// (or PoolMaxOpen minus 1).
	wholeUserLimit wholeReason = "user_limit"
	// wholeTableLimit: the table had MaxTableStreams results streaming.
	wholeTableLimit wholeReason = "table_limit"
	// wholeIndexCursors: the table's index had three quarters of its
	// cursors open (see indexCursors).
	wholeIndexCursors wholeReason = "index_cursors"
)

var wholeReasons = []wholeReason{wholeQuery, wholeSession, wholeUserLimit, wholeTableLimit, wholeIndexCursors}

// metrics are the server's Prometheus metrics, in a registry of its own.
// Labels never name a user or a table: there can be many of each. The
// event methods do nothing on a nil *metrics.
type metrics struct {
	reg *prometheus.Registry

	calls         *prometheus.CounterVec
	callSeconds   *prometheus.HistogramVec
	streamed      prometheus.Counter
	readWhole     *prometheus.CounterVec
	streamsOpen   prometheus.Gauge
	stalled       prometheus.Counter
	memoryRefused prometheus.Counter
	cursorLimit   prometheus.Counter
	poolWait      prometheus.Histogram
	sessionsFull  prometheus.Counter
	sessionConns  prometheus.Gauge
	dsRowsWritten prometheus.Counter
	dsBytesWrite  prometheus.Counter
	dsRowsRead    prometheus.Counter
	dsBytesRead   prometheus.Counter
}

func newMetrics() *metrics {
	return &metrics{}
}

// watch uses the community implementation; see the build-profile contract.
func (m *metrics) watch(tenants *tenantCache, sessions *sessionStore, results *resultBudget, health *redisHealth) {
}

func (m *metrics) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
}

// shortMethod uses the community implementation; see the build-profile contract.
func shortMethod(full string) string {
	var zero0 string
	return zero0
}

func (m *metrics) call(method string, code codes.Code, d time.Duration) {
}

func (m *metrics) queryReadWhole(why wholeReason) {
}

func (m *metrics) streamStarted() {
}

func (m *metrics) streamEnded() {
}

func (m *metrics) resultStalled() {
}

func (m *metrics) resultMemoryRefused() {
}

func (m *metrics) cursorLimitError() {
}

func (m *metrics) poolWaited(d time.Duration) {
}

func (m *metrics) sessionStoreFull() {
}

func (m *metrics) sessionConn(delta float64) {
}

func (m *metrics) datasetWritten(rows, bytes int64) {
}

func (m *metrics) datasetRead(rows, bytes int64) {
}

// stateCollector reports what the server holds at each scrape.
type stateCollector struct {
	tenants  *tenantCache
	sessions *sessionStore // nil when sessions are off
	results  *resultBudget
	health   *redisHealth
}

var (
	poolConnsDesc = prometheus.NewDesc(metricsPrefix+"pool_connections",
		"Pooled ADBC connections, across Redis users: in_use (including ones being opened), and idle.",
		[]string{"state"}, nil)
	tenantsDesc = prometheus.NewDesc(metricsPrefix+"tenants_open",
		"Redis users with an ADBC database, connection pool and Redis client open.", nil, nil)
	sessionsDesc = prometheus.NewDesc(metricsPrefix+"sessions_open",
		"Flight SQL sessions the server keeps.", nil, nil)
	resultMemoryDesc = prometheus.NewDesc(metricsPrefix+"result_memory_bytes",
		"Memory held by query results read whole for want of a stream (counted against -result-memory).", nil, nil)
	redisUpDesc = prometheus.NewDesc(metricsPrefix+"redis_up",
		"1 while Redis answers the health check's PING, else 0.", nil, nil)
)

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
}
