package server

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFlightTypeInfo(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	// Type metadata must be available even without a reachable data backend.
	c := newClients(t, Config{RedisURI: "redis://127.0.0.1:1", Allocator: alloc}).sql
	ctx := context.Background()
	for _, tc := range []struct {
		code *int32
		rows int
	}{
		{nil, 18}, {ptr(int32(4)), 1}, {ptr(int32(92)), 2}, {ptr(int32(123456)), 0},
	} {
		info, err := c.GetXdbcTypeInfo(ctx, tc.code)
		if err != nil {
			t.Fatal(err)
		}
		rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			t.Fatal(err)
		}
		if !rdr.Schema().Equal(schema_ref.XdbcTypeInfo) {
			t.Errorf("schema: %s", rdr.Schema())
		}
		count := 0
		var previous int32 = -1000
		for rdr.Next() {
			rec := rdr.RecordBatch()
			codes := rec.Column(1).(*array.Int32)
			for i := 0; i < int(rec.NumRows()); i++ {
				code := codes.Value(i)
				if code < previous {
					t.Error("types are not sorted by data_type")
				}
				previous = code
				if tc.code != nil && code != *tc.code {
					t.Errorf("filter %d returned %d", *tc.code, code)
				}
				for _, field := range []int{0, 1, 6, 7, 8, 10, 15} {
					if rec.Column(field).IsNull(i) {
						t.Errorf("required field %d is NULL", field)
					}
				}
				if code == 92 && rec.Column(16).(*array.Int32).Value(i) != 2 {
					t.Error("TIME datetime_subcode is not 2")
				}
				count++
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil || count != tc.rows {
			t.Errorf("type info: rows=%d want=%d, err=%v", count, tc.rows, err)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestFlightUnsupportedCapabilities(t *testing.T) {
	clients := newClients(t, Config{RedisURI: "redis://127.0.0.1:1"})
	ctx := context.Background()
	exchange, err := clients.fl.DoExchange(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = exchange.CloseSend()
	_, err = exchange.Recv()
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "DoPut") {
		t.Fatalf("DoExchange: %v", err)
	}
	if _, err := clients.sql.BeginTransaction(ctx); status.Code(err) != codes.Unimplemented {
		t.Errorf("BeginTransaction: %v", err)
	}
	if _, err := clients.sql.ExecuteSubstrait(ctx, flightsql.SubstraitPlan{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("Substrait: %v", err)
	}
}

func TestIntegrationTypeInfoDDL(t *testing.T) {
	c := newClients(t, Config{RedisURI: redisURI(t)}).sql
	ctx := context.Background()
	info, err := c.GetXdbcTypeInfo(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	var columns []string
	for rdr.Next() {
		names := rdr.RecordBatch().Column(0).(*array.String)
		for i := 0; i < names.Len(); i++ {
			columns = append(columns, "c"+strings.ReplaceAll(names.Value(i), " ", "_")+" "+names.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	table := uniqueName("it_flight_typeinfo")
	dropTable(t, c, table)
	update(t, ctx, c, "CREATE TABLE "+table+" ("+strings.Join(columns, ",")+")")
	info, err = c.Execute(ctx, "SELECT * FROM "+table)
	if err != nil {
		t.Fatal(err)
	}
	schema, rows := fetch(t, ctx, c, info)
	if schema.NumFields() != 18 || len(rows) != 0 {
		t.Errorf("reported types could not create an empty 18-column table: %s, %v", schema, rows)
	}
}
