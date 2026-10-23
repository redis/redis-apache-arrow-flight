package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
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

// Under redis-acl, calls are counted before authentication.

// Only requests that wait for a connection are timed.

// ServeMetrics serves until Shutdown, which ends it without an error.

// The metrics follow streamed and read-whole results, sessions and
// datasets.
