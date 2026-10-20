package server

import (
	"context"
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// router is the registered Flight service. Requests on PATH descriptors,
// dataset tickets (JSON, starting with '{') and dataset actions go to the
// dataset store; everything else goes to the Flight SQL server, whose
// commands and tickets are protobuf Any messages (which start with 0x0a).
//
// It embeds BaseFlightServer for Handshake, which must succeed for the
// Basic-auth middleware to hand out tokens, and DoExchange (unsupported).
type router struct {
	flight.BaseFlightServer
	sql flight.FlightServer
	ds  *datasets
}

func isDataset(desc *flight.FlightDescriptor) bool {
	return desc != nil && desc.Type == flight.DescriptorPATH
}

func (r *router) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if isDataset(desc) {
		return r.ds.GetFlightInfo(ctx, desc)
	}
	return r.sql.GetFlightInfo(ctx, desc)
}

func (r *router) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	if isDataset(desc) {
		info, err := r.ds.GetFlightInfo(ctx, desc)
		if err != nil {
			return nil, err
		}
		// A dataset is always complete. A descriptor in PollInfo would tell
		// the client to poll again ("if unset, the query is complete").
		return &flight.PollInfo{Info: info}, nil
	}
	return r.sql.PollFlightInfo(ctx, desc)
}

func (r *router) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if isDataset(desc) {
		return r.ds.GetSchema(ctx, desc)
	}
	return r.sql.GetSchema(ctx, desc)
}

func (r *router) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	if len(tkt.GetTicket()) > 0 && tkt.Ticket[0] == '{' {
		return r.ds.DoGet(tkt, stream)
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	handoffs := &recordHandoffs{}
	ctx = context.WithValue(ctx, handoffKey{}, handoffs)
	defer handoffs.releaseAll()
	err := r.sql.DoGet(tkt, &cancelOnSendError{FlightService_DoGetServer: stream, ctx: ctx, cancel: cancel})
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// The Flight SQL server returns the context's error as it is,
		// which gRPC would report as Unknown.
		err = status.FromContextError(err).Err()
	}
	return err
}

// cancelOnSendError cancels a DoGet's context once a message fails to
// send. After a failed send, the Flight SQL server drains the result
// before it returns, while the call's context is still live; cancelling
// it stops the result being read from Redis to the end for nothing.
type cancelOnSendError struct {
	flight.FlightService_DoGetServer
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *cancelOnSendError) Context() context.Context { return s.ctx }

func (s *cancelOnSendError) Send(d *flight.FlightData) error {
	err := s.FlightService_DoGetServer.Send(d)
	if err != nil {
		s.cancel()
	}
	return err
}

// DoPut reads the first message to see where the stream goes, and replays
// it to the handler.
func (r *router) DoPut(stream flight.FlightService_DoPutServer) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "DoPut got no messages")
	}
	if err != nil {
		return err
	}
	if first.FlightDescriptor == nil {
		return status.Error(codes.InvalidArgument, "the first DoPut message needs a FlightDescriptor")
	}
	if isDataset(first.FlightDescriptor) {
		return r.ds.DoPut(stream, first)
	}
	return r.sql.DoPut(&replayStream{FlightService_DoPutServer: stream, first: first})
}

func (r *router) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	return r.ds.ListFlights(criteria, stream)
}

func (r *router) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action.GetType() == actionDropDataset {
		return r.ds.Drop(action, stream)
	}
	return r.sql.DoAction(action, stream)
}

func (r *router) ListActions(e *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	if err := r.sql.ListActions(e, stream); err != nil {
		return err
	}
	return stream.Send(&flight.ActionType{
		Type:        actionDropDataset,
		Description: `Drop a dataset. Body: its name (the path segments joined by "/"), or {"dataset": "<name>"}.`,
	})
}

// DoExchange is intentionally deferred: this service has no bidirectional
// transformation contract. Clients upload with DoPut and fetch with DoGet.
func (r *router) DoExchange(flight.FlightService_DoExchangeServer) error {
	return status.Error(codes.Unimplemented, "DoExchange is not supported: upload datasets with DoPut and download them with DoGet")
}
