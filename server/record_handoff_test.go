package server

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type failingDoGet struct {
	flight.FlightService_DoGetServer
}

func (*failingDoGet) Context() context.Context      { return context.Background() }
func (*failingDoGet) Send(*flight.FlightData) error { return errors.New("injected transport failure") }

type handoffSQL struct {
	flightsql.BaseServer
	record arrow.RecordBatch
}

func (s *handoffSQL) DoGetStatement(ctx context.Context, _ flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	owner := ctx.Value(handoffKey{}).(*recordHandoffs)
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: owner.wrap(s.record)}
	close(ch)
	return s.record.Schema(), ch, nil
}

// Exercise the real SDK's write-error path, including its deferred stream drain.
// The batch currently being serialized must be reclaimed after the writer exits.
func TestDoGetFailedSendReleasesCurrentBatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	builder := array.NewInt64Builder(alloc)
	builder.AppendValues([]int64{1, 2, 3}, nil)
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	values.Release()
	adapter := flightsql.NewFlightServer(&handoffSQL{record: record})
	route := &router{sql: adapter}
	envelope, err := anypb.New(&pb.TicketStatementQuery{StatementHandle: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := route.DoGet(&flight.Ticket{Ticket: data}, &failingDoGet{}); err == nil {
		t.Fatal("expected transport failure")
	}
}
