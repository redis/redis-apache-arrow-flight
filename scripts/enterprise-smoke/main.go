// Command enterprise-smoke verifies SQL writes through a running Flight endpoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func check(address string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := flightsql.NewClientCtx(ctx, address, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer client.Close()
	table := fmt.Sprintf("it_flight_env_sql_%d", time.Now().UnixNano())
	if _, err = client.ExecuteUpdate(ctx, "CREATE TABLE "+table+" (id BIGINT, label TEXT)"); err != nil {
		return err
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, dropErr := client.ExecuteUpdate(cleanup, "DROP TABLE IF EXISTS "+table); dropErr != nil && err == nil {
			err = fmt.Errorf("fixture cleanup: %w", dropErr)
		}
	}()
	if n, err := client.ExecuteUpdate(ctx, "INSERT INTO "+table+" VALUES (1,'before')"); err != nil || n != 1 {
		return fmt.Errorf("insert count %d: %v", n, err)
	}
	if n, err := client.ExecuteUpdate(ctx, "UPDATE "+table+" SET label='after' WHERE id=1"); err != nil || n != 1 {
		return fmt.Errorf("update count %d: %v", n, err)
	}
	info, err := client.Execute(ctx, "SELECT id,label FROM "+table)
	if err != nil {
		return err
	}
	rows := int64(0)
	for _, ep := range info.Endpoint {
		reader, err := client.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}
		for reader.Next() {
			record := reader.RecordBatch()
			for i := 0; i < int(record.NumRows()); i++ {
				ids, okID := record.Column(0).(*array.Int64)
				labels, okLabel := record.Column(1).(*array.String)
				if !okID || !okLabel || ids.IsNull(i) || labels.IsNull(i) || ids.Value(i) != 1 || labels.Value(i) != "after" {
					reader.Release()
					return fmt.Errorf("SQL write readback did not match expected row")
				}
				rows++
			}
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return err
		}
	}
	if rows != 1 {
		return fmt.Errorf("read %d rows, want 1", rows)
	}
	return nil
}

func main() {
	address := flag.String("addr", "flight:8815", "Flight address in the isolated test network")
	flag.Parse()
	if err := check(*address); err != nil {
		fmt.Fprintln(os.Stderr, "Flight SQL smoke failed:", err)
		os.Exit(1)
	}
	fmt.Println("Flight SQL create/insert/update/readback passed; fixture removed")
}
