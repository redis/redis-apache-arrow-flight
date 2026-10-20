package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// A cancellation record is Redis-backed so a non-session query can be fetched
// and cancelled on different replicas. It contains no SQL, bound values or
// credentials. Pending/completed records expire after resultTTL; active readers
// renew that lease. Old tickets without a cancellation ID retain their behavior.
const queryPollInterval = 250 * time.Millisecond

func queryOwner(t *tenant) string {
	// Bind to the authenticated principal, not a password-derived value.
	// A valid replacement credential for the same user can still cancel its work.
	hash := sha256.Sum256([]byte("redis-flight-query-owner-v1\x00" + t.id.Username))
	return hex.EncodeToString(hash[:])
}

func (s *sqlServer) queryKey(id string) (string, error) {
	if len(id) != 32 {
		return "", status.Error(codes.InvalidArgument, "invalid query cancellation ID")
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", status.Error(codes.InvalidArgument, "invalid query cancellation ID")
	}
	return s.keyPrefix + "query:{" + id + "}", nil
}

func (s *sqlServer) registerQuery(ctx context.Context, t *tenant, h *statementHandle) error {
	h.CancelID = newSessionToken()
	key, _ := s.queryKey(h.CancelID)
	encoded, _ := json.Marshal(map[string]any{"owner": queryOwner(t), "state": "pending", "active": 0})
	// SET with EX is one command: an ACL failure cannot leave a hash behind
	// without its expiry. GET also checks the read permission needed by polling.
	err := toStatus(t.rdb.Eval(ctx, `redis.call('GET',KEYS[1]); return redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2])`,
		[]string{key}, string(encoded), int(resultTTL/time.Second)).Err())
	if status.Code(err) == codes.PermissionDenied {
		// Read-only identities must not lose SQL access merely because they
		// cannot write cancellation metadata. Such a ticket is not cancellable.
		h.CancelID = ""
		s.tenants.cfg.Logger.Debug("query cancellation unavailable for this Redis identity")
		return nil
	}
	return err
}

// State transitions are single-key Lua operations, including owner checks.
// Repeated DoGet reads share an ID and cancellation ends all active replays.
const queryStateScript = `
local raw=redis.call('GET',KEYS[1])
if not raw then return -1 end
local record=cjson.decode(raw)
if record.owner ~= ARGV[1] then return -2 end
local op=ARGV[2]
if op == 'begin' then
  if record.state == 'cancelled' then return -3 end
  record.state='active'; record.active=record.active+1
elseif op == 'finish' then
  record.active=math.max(0,record.active-1)
  if record.active == 0 and record.state ~= 'cancelled' then record.state='complete' end
elseif op == 'cancel' then
  if record.state == 'complete' and record.active == 0 then return 3 end
  if record.state ~= 'cancelled' then
    record.state='cancelled'
    redis.call('SET',KEYS[1],cjson.encode(record),'EX',ARGV[3])
  end
  if record.active > 0 then return 2 else return 1 end
elseif record.state == 'cancelled' then return -3 end
redis.call('SET',KEYS[1],cjson.encode(record),'EX',ARGV[3])
return 0`

func (s *sqlServer) queryState(ctx context.Context, t *tenant, id, op string) (int64, error) {
	key, err := s.queryKey(id)
	if err != nil {
		return 0, err
	}
	n, err := t.rdb.Eval(ctx, queryStateScript, []string{key}, queryOwner(t), op, int(resultTTL/time.Second)).Int64()
	if err != nil {
		return 0, toStatus(err)
	}
	switch n {
	case -1:
		return 0, status.Error(codes.NotFound, "the query ticket is unknown or has expired; execute the query again")
	case -2:
		return 0, status.Error(codes.PermissionDenied, "the query belongs to a different Redis identity")
	case -3:
		return 0, status.Error(codes.Canceled, "the query was explicitly cancelled")
	}
	return n, nil
}

func (s *sqlServer) CancelFlightInfo(ctx context.Context, req *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error) {
	result := flight.CancelStatusNotCancellable
	info := req.GetInfo()
	if info == nil {
		return flight.CancelFlightInfoResult{Status: result}, status.Error(codes.InvalidArgument, "cancel requires FlightInfo")
	}
	if isDataset(info.FlightDescriptor) {
		return flight.CancelFlightInfoResult{Status: result}, nil
	}
	if len(info.Endpoint) != 1 {
		return flight.CancelFlightInfoResult{Status: result}, status.Error(codes.InvalidArgument, "cancel expects one SQL query endpoint")
	}
	var envelope anypb.Any
	if err := proto.Unmarshal(info.Endpoint[0].GetTicket().GetTicket(), &envelope); err != nil {
		return flight.CancelFlightInfoResult{Status: result}, status.Error(codes.InvalidArgument, "invalid query ticket")
	}
	var ticket pb.TicketStatementQuery
	if !envelope.MessageIs(&ticket) {
		return flight.CancelFlightInfoResult{Status: result}, nil
	} // metadata, not an executable query
	if err := envelope.UnmarshalTo(&ticket); err != nil {
		return flight.CancelFlightInfoResult{Status: result}, status.Error(codes.InvalidArgument, "invalid query ticket")
	}
	h, err := decodeHandle(ticket.StatementHandle)
	if err != nil {
		return flight.CancelFlightInfoResult{Status: result}, err
	}
	if h.CancelID == "" || h.Result != nil || h.ResultKey != "" {
		return flight.CancelFlightInfoResult{Status: result}, nil
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return flight.CancelFlightInfoResult{Status: result}, err
	}
	defer done()
	n, err := s.queryState(ctx, t, h.CancelID, "cancel")
	if err != nil {
		return flight.CancelFlightInfoResult{Status: result}, err
	}
	result = flight.CancelStatus(n)
	return flight.CancelFlightInfoResult{Status: result}, nil
}

// executeCancellableQuery ties the execution context to the shared cancellation
// record and keeps the tenant alive through final cleanup, even for a result
// whose connection was already returned to its pool.
func (s *sqlServer) executeCancellableQuery(parent context.Context, h statementHandle) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if h.CancelID == "" {
		return s.executeQuery(parent, h)
	}
	t, done, err := s.tenants.acquire(parent)
	if err != nil {
		return nil, nil, err
	}
	if _, err = s.queryState(parent, t, h.CancelID, "begin"); err != nil {
		done()
		return nil, nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	var stallOnce sync.Once
	report := func() { stallOnce.Do(s.tenants.metrics.resultStalled) }
	ctx = context.WithValue(ctx, stalledKey{}, report)
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		tick := time.NewTicker(queryPollInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if _, err := s.queryState(ctx, t, h.CancelID, "poll"); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	finish := func() {
		cancel(context.Canceled)
		<-watched
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_, err := s.queryState(cleanup, t, h.CancelID, "finish")
		if err != nil {
			s.tenants.cfg.Logger.Debug("query cancellation record cleanup", "error", err)
		}
		done()
	}
	handedOff := false
	defer func() {
		if !handedOff {
			finish()
		}
	}()
	schema, input, err := s.executeQuery(ctx, h)
	if err != nil {
		if ctx.Err() != nil {
			err = toStatus(context.Cause(ctx))
		}
		return nil, nil, err
	}
	output := make(chan flight.StreamChunk, 1)
	handoffs, _ := parent.Value(handoffKey{}).(*recordHandoffs)
	handedOff = true
	go func() {
		defer close(output)
		defer finish()
		for chunk := range input {
			if ctx.Err() != nil {
				if chunk.Data != nil {
					chunk.Data.Release()
				}
				continue // drain to release the original reader and pool lease
			}
			var released <-chan struct{}
			if chunk.Data != nil {
				record := handoffs.wrap(chunk.Data)
				chunk.Data, released = record, record.released
			}
			timer := time.NewTimer(s.tenants.cfg.StreamStallTimeout)
			sent := false
			select {
			case output <- chunk:
				sent = true
			case <-ctx.Done():
			case <-timer.C:
				report()
				cancel(status.Error(codes.DeadlineExceeded, "the client stopped reading the query result (--stream-stall-timeout)"))
			}
			if !sent && chunk.Data != nil {
				chunk.Data.Release()
			}
			// Pull the next batch only after the adapter has finished this
			// one. A stopped transport must not keep the control lease alive.
			if sent && released != nil {
				select {
				case <-released:
				case <-ctx.Done():
				case <-timer.C:
					report()
					cancel(status.Error(codes.DeadlineExceeded, "the client stopped reading the query result (--stream-stall-timeout)"))
				}
			}
			timer.Stop()
		}
		if ctx.Err() != nil {
			// Replace a queued batch with the terminal cancellation error.
			// Only this goroutine writes output, so its one slot is available.
			select {
			case queued := <-output:
				if queued.Data != nil {
					queued.Data.Release()
				}
			default:
			}
			if parent.Err() == nil {
				output <- flight.StreamChunk{Err: toStatus(context.Cause(ctx))}
			}
		}
	}()
	return schema, output, nil
}
