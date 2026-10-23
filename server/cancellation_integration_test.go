package server

import (
	"context"
	"encoding/json"

	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func cancellationHandle(t *testing.T, info *flight.FlightInfo) statementHandle {
	t.Helper()
	var envelope anypb.Any
	if err := proto.Unmarshal(info.Endpoint[0].Ticket.Ticket, &envelope); err != nil {
		t.Fatal(err)
	}
	var ticket pb.TicketStatementQuery
	if err := envelope.UnmarshalTo(&ticket); err != nil {
		t.Fatal(err)
	}
	h, err := decodeHandle(ticket.StatementHandle)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestIntegrationQueryCancellation(t *testing.T) {
	uri := redisURI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	cfg := streamConfig(uri)
	cfg.DisableSessions = true
	cfg.Allocator = alloc
	first := newClients(t, cfg).sql
	second := newClients(t, cfg).sql
	admin := adminClient(t, uri)
	table := uniqueName("it_flight_cancel")
	dropTable(t, first, table)
	ingestRows(t, first, table, streamRows, 200) // more than gRPC can buffer
	query := "SELECT id,label FROM " + table
	execute := func() *flight.FlightInfo {
		t.Helper()
		info, err := first.Execute(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		return info
	}
	cleanup := func(info *flight.FlightInfo) {
		t.Helper()
		h := cancellationHandle(t, info)
		t.Cleanup(func() { _ = admin.Del(context.Background(), "arrowflight:query:{"+h.CancelID+"}").Err() })
	}
	t.Run("before fetching and repeated cancellation", func(t *testing.T) {
		info := execute()
		cleanup(info)
		for range 2 {
			result, err := second.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
			if err != nil || result.Status != flight.CancelStatusCancelled {
				t.Fatalf("cancel=%v, %v", result, err)
			}
		}
		if _, err := second.DoGet(ctx, info.Endpoint[0].Ticket); status.Code(err) != codes.Canceled {
			t.Fatalf("fetch cancelled ticket: %v", err)
		}
	})
	t.Run("active stream across replicas frees resources", func(t *testing.T) {
		info := execute()
		cleanup(info)
		rdr, err := second.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			t.Fatal(err)
		}
		defer rdr.Release()
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		result, err := first.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
		if err != nil || result.Status != flight.CancelStatusCancelling {
			t.Fatalf("active cancel=%v, %v", result, err)
		}
		for rdr.Next() {
		}
		if status.Code(rdr.Err()) != codes.Canceled {
			t.Fatalf("cancelled stream: %v", rdr.Err())
		}
		waitCursors(t, admin, table, 0)
		h := cancellationHandle(t, info)
		deadline := time.Now().Add(3 * time.Second)
		for {
			active, err := queryActive(ctx, admin, "arrowflight:query:{"+h.CancelID+"}")
			if err != nil {
				t.Fatal(err)
			}
			if active == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d query leases still active", active)
			}
			time.Sleep(10 * time.Millisecond)
		}
		result, err = first.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
		if err != nil || result.Status != flight.CancelStatusCancelled {
			t.Fatalf("settled cancel=%v, %v", result, err)
		}
	})
	t.Run("completed query and write result are not cancellable", func(t *testing.T) {
		info, err := first.Execute(ctx, "SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		cleanup(info)
		fetch(t, ctx, first, info)
		// The action may race the final cleanup; retry only while it is still active.
		deadline := time.Now().Add(3 * time.Second)
		h := cancellationHandle(t, info)
		for {
			active, _ := queryActive(ctx, admin, "arrowflight:query:{"+h.CancelID+"}")
			if active == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("query did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
		result, err := second.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
		if err != nil || result.Status != flight.CancelStatusNotCancellable {
			t.Fatalf("completed cancel=%v, %v", result, err)
		}
		info, err = first.Execute(ctx, "SET TIME ZONE 'UTC'")
		if err != nil {
			t.Fatal(err)
		}
		result, err = second.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
		if err != nil || result.Status != flight.CancelStatusNotCancellable {
			t.Fatalf("write cancel=%v, %v", result, err)
		}
	})
	t.Run("expiry and ownership", func(t *testing.T) {
		info := execute()
		cleanup(info)
		h := cancellationHandle(t, info)
		key := "arrowflight:query:{" + h.CancelID + "}"
		raw, err := admin.Get(ctx, key).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		record["owner"] = "another-identity"
		raw, _ = json.Marshal(record)
		if err := admin.Set(ctx, key, raw, resultTTL).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := second.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unauthorized cancel: %v", err)
		}
		if _, err := second.DoGet(ctx, info.Endpoint[0].Ticket); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unauthorized fetch: %v", err)
		}
		_ = admin.Del(ctx, key).Err()
		if _, err := second.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info}); status.Code(err) != codes.NotFound {
			t.Fatalf("expired cancel: %v", err)
		}
	})
	t.Run("completion races cancellation", func(t *testing.T) {
		for range 10 {
			info, err := first.Execute(ctx, "SELECT 1")
			if err != nil {
				t.Fatal(err)
			}
			cleanup(info)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := second.DoGet(ctx, info.Endpoint[0].Ticket)
				if err == nil {
					for r.Next() {
					}
					err = r.Err()
					r.Release()
				}
				if err != nil && status.Code(err) != codes.Canceled {
					t.Error(err)
				}
			}()
			result, err := first.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
			if err != nil || result.Status == flight.CancelStatusUnspecified {
				t.Errorf("racing cancel=%v, %v", result, err)
			}
			wg.Wait()
		}
	})
}

func queryActive(ctx context.Context, admin goredis.UniversalClient, key string) (int, error) {
	raw, err := admin.Get(ctx, key).Bytes()
	if err != nil {
		return 0, err
	}
	var record struct{ Active int }
	err = json.Unmarshal(raw, &record)
	return record.Active, err
}
