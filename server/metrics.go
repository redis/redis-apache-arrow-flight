package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
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
	reg := prometheus.NewRegistry()
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	name := func(s string) string { return metricsPrefix + s }
	m := &metrics{
		reg: reg,
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{Name: name("calls_total"),
			Help: "Calls finished, by method and gRPC code."}, []string{"method", "code"}),
		callSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name("call_duration_seconds"),
			Help:    "How long calls took, by method. DoGet and DoPut are timed until their stream ends.",
			Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 60, 300, 1800}}, []string{"method"}),
		streamed: prometheus.NewCounter(prometheus.CounterOpts{Name: name("queries_streamed_total"),
			Help: "Query results streamed from Redis as they were sent."}),
		readWhole: prometheus.NewCounterVec(prometheus.CounterOpts{Name: name("queries_read_whole_total"),
			Help: "Query results read whole into memory before they were sent, by why: query (the driver doesn't " +
				"stream it, or returned it whole), session (a session that may have temporary objects), user_limit (-max-streams), " +
				"table_limit (-max-table-streams), index_cursors (the index near its cursor limit)."},
			[]string{"reason"}),
		streamsOpen: prometheus.NewGauge(prometheus.GaugeOpts{Name: name("query_streams_open"),
			Help: "Query results streaming from Redis now, each with a connection and a cursor."}),
		stalled: prometheus.NewCounter(prometheus.CounterOpts{Name: name("query_results_stalled_total"),
			Help: "Query results ended because the client took longer than -stream-stall-timeout to read a batch."}),
		memoryRefused: prometheus.NewCounter(prometheus.CounterOpts{Name: name("result_memory_refusals_total"),
			Help: "Query results refused with RESOURCE_EXHAUSTED for not fitting in -result-memory."}),
		cursorLimit: prometheus.NewCounter(prometheus.CounterOpts{Name: name("index_cursor_limit_errors_total"),
			Help: "Calls that failed because a table's index had no cursor left (INDEX_CURSOR_LIMIT)."}),
		poolWait: prometheus.NewHistogram(prometheus.HistogramOpts{Name: name("pool_wait_seconds"),
			Help:    "How long requests that found -pool-max-open connections in use waited for one.",
			Buckets: []float64{.001, .01, .1, .5, 1, 5, 30}}),
		sessionsFull: prometheus.NewCounter(prometheus.CounterOpts{Name: name("sessions_full_total"),
			Help: "Requests that used the pool because the server had as many sessions as it keeps."}),
		sessionConns: prometheus.NewGauge(prometheus.GaugeOpts{Name: name("session_connections"),
			Help: "ADBC connections open for sessions (outside the pool)."}),
		dsRowsWritten: prometheus.NewCounter(prometheus.CounterOpts{Name: name("dataset_rows_written_total"),
			Help: "Rows of dataset uploads committed."}),
		dsBytesWrite: prometheus.NewCounter(prometheus.CounterOpts{Name: name("dataset_bytes_written_total"),
			Help: "Arrow IPC bytes of dataset uploads committed."}),
		dsRowsRead: prometheus.NewCounter(prometheus.CounterOpts{Name: name("dataset_rows_read_total"),
			Help: "Rows of datasets sent."}),
		dsBytesRead: prometheus.NewCounter(prometheus.CounterOpts{Name: name("dataset_bytes_read_total"),
			Help: "Arrow IPC bytes of datasets read from Redis."}),
	}
	for _, c := range []prometheus.Collector{m.calls, m.callSeconds, m.streamed, m.readWhole, m.streamsOpen,
		m.stalled, m.memoryRefused, m.cursorLimit, m.poolWait, m.sessionsFull, m.sessionConns,
		m.dsRowsWritten, m.dsBytesWrite, m.dsRowsRead, m.dsBytesRead} {
		f(c)
	}
	for _, r := range wholeReasons {
		m.readWhole.WithLabelValues(string(r))
	}
	f(collectors.NewGoCollector())
	f(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name("build_info"), Help: "The server's version.",
		ConstLabels: prometheus.Labels{"version": Version}}, func() float64 { return 1 }))
	return m
}

// watch adds the gauges read from the server's state at each scrape.
func (m *metrics) watch(tenants *tenantCache, sessions *sessionStore, results *resultBudget, health *redisHealth) {
	m.reg.MustRegister(&stateCollector{tenants: tenants, sessions: sessions, results: results, health: health})
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// shortMethod is a gRPC method's name without its service.
func shortMethod(full string) string {
	return full[strings.LastIndexByte(full, '/')+1:]
}

func (m *metrics) call(method string, code codes.Code, d time.Duration) {
	if m == nil {
		return
	}
	method = shortMethod(method)
	m.calls.WithLabelValues(method, code.String()).Inc()
	m.callSeconds.WithLabelValues(method).Observe(d.Seconds())
}

func (m *metrics) queryReadWhole(why wholeReason) {
	if m != nil {
		m.readWhole.WithLabelValues(string(why)).Inc()
	}
}

func (m *metrics) streamStarted() {
	if m != nil {
		m.streamed.Inc()
		m.streamsOpen.Inc()
	}
}

func (m *metrics) streamEnded() {
	if m != nil {
		m.streamsOpen.Dec()
	}
}

func (m *metrics) resultStalled() {
	if m != nil {
		m.stalled.Inc()
	}
}

func (m *metrics) resultMemoryRefused() {
	if m != nil {
		m.memoryRefused.Inc()
	}
}

func (m *metrics) cursorLimitError() {
	if m != nil {
		m.cursorLimit.Inc()
	}
}

func (m *metrics) poolWaited(d time.Duration) {
	if m != nil {
		m.poolWait.Observe(d.Seconds())
	}
}

func (m *metrics) sessionStoreFull() {
	if m != nil {
		m.sessionsFull.Inc()
	}
}

func (m *metrics) sessionConn(delta float64) {
	if m != nil {
		m.sessionConns.Add(delta)
	}
}

func (m *metrics) datasetWritten(rows, bytes int64) {
	if m != nil {
		m.dsRowsWritten.Add(float64(rows))
		m.dsBytesWrite.Add(float64(bytes))
	}
}

func (m *metrics) datasetRead(rows, bytes int64) {
	if m != nil {
		m.dsRowsRead.Add(float64(rows))
		m.dsBytesRead.Add(float64(bytes))
	}
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
	prometheus.DescribeByCollect(c, ch)
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	tenants, inUse, idle := c.tenants.poolStats()
	ch <- prometheus.MustNewConstMetric(poolConnsDesc, prometheus.GaugeValue, float64(inUse), "in_use")
	ch <- prometheus.MustNewConstMetric(poolConnsDesc, prometheus.GaugeValue, float64(idle), "idle")
	ch <- prometheus.MustNewConstMetric(tenantsDesc, prometheus.GaugeValue, float64(tenants))
	if c.sessions != nil {
		ch <- prometheus.MustNewConstMetric(sessionsDesc, prometheus.GaugeValue, float64(c.sessions.count()))
	}
	ch <- prometheus.MustNewConstMetric(resultMemoryDesc, prometheus.GaugeValue, float64(c.results.used.Load()))
	up := 0.0
	if c.health.up.Load() {
		up = 1
	}
	ch <- prometheus.MustNewConstMetric(redisUpDesc, prometheus.GaugeValue, up)
}
