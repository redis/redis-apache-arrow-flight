package server

import (
	"log/slog"
	"os"
	"strings"
	"testing"
)

// testLogger logs to the test's log, at debug level with
// FLIGHT_TEST_DEBUG=1.
func testLogger(t testing.TB) *slog.Logger {
	level := slog.LevelInfo
	if os.Getenv("FLIGHT_TEST_DEBUG") != "" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: level}))
}

type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
