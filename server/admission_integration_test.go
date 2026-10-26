package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIntegrationStreamOverflowReject(t *testing.T) {
	if !FeatureCapabilities()["optimization"] {
		t.Skip("stream admission requires the enterprise optimization implementation")
	}
	uri := redisURI(t)
	ctx := context.Background()
	setup := newClients(t, Config{RedisURI: uri}).sql
	table := uniqueName("it_flight_admission")
	dropTable(t, setup, table)
	ingestRows(t, setup, table, streamRows, 200)
	cfg := streamConfig(uri)
	cfg.StreamOverflow = "reject"
	cfg.MaxStreams = 1
	cfg.MaxTableStreams = 1
	c := newClients(t, cfg).sql
	first := openRead(t, ctx, c, "SELECT id,label FROM "+table)
	if !first.Next() {
		t.Fatal(first.Err())
	}
	// A failed query in the overflow path would normally reach the driver and
	// fail conversion. Admission must reject it before executing that scan.
	if _, err := tryQuery(ctx, c, "SELECT CAST(label AS BIGINT) FROM "+table); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("overflow did not reject before execution: %v", err)
	}
	if _, err := readIDs(first); err != nil {
		t.Fatal(err)
	}
	first.Release()
	if ids, err := tryQuery(ctx, c, "SELECT id FROM "+table); err != nil || len(ids) != streamRows {
		t.Fatalf("capacity not reusable: rows=%d, err=%v", len(ids), err)
	}
	cfg.PoolMaxOpen = 1
	noStreams := newClients(t, cfg).sql
	if _, err := tryQuery(ctx, noStreams, "SELECT id FROM "+table); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("pool of one: %v", err)
	}
	// Schema-only and non-scan operations do not need a stream slot.
	wantRows(t, query(t, ctx, noStreams, "SELECT 1"), "1")
}

// Community queries use basic connections and buffered results, so paid stream
// limits must not reject otherwise valid SQL or hide the driver's SQL errors.
func TestIntegrationCommunityBufferedQueries(t *testing.T) {
	if FeatureCapabilities()["optimization"] {
		t.Skip("community buffered-query contract")
	}
	uri := redisURI(t)
	ctx := context.Background()
	cfg := streamConfig(uri)
	cfg.StreamOverflow = "reject"
	cfg.MaxStreams, cfg.MaxTableStreams, cfg.PoolMaxOpen = 1, 1, 1
	c := newClients(t, cfg).sql
	table := uniqueName("it_flight_buffered")
	dropTable(t, c, table)
	ingestRows(t, c, table, streamRows, 200)
	first := openRead(t, ctx, c, "SELECT id,label FROM "+table)
	if !first.Next() {
		t.Fatal(first.Err())
	}
	if ids, err := tryQuery(ctx, c, "SELECT id FROM "+table); err != nil || len(ids) != streamRows {
		t.Fatalf("basic connection query: rows=%d, err=%v", len(ids), err)
	}
	if _, err := tryQuery(ctx, c, "SELECT CAST(label AS BIGINT) FROM "+table); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("buffered query must preserve the SQL conversion error: %v", err)
	}
	n := int(first.RecordBatch().NumRows())
	ids, err := readIDs(first)
	if err != nil || n+len(ids) != streamRows {
		t.Fatalf("first buffered result: rows=%d, err=%v", n+len(ids), err)
	}
	waitCursors(t, adminClient(t, uri), table, 0)
}
