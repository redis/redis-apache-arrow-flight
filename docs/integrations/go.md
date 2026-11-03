# Go

[All integration guides](README.md). Use Arrow Go's Flight SQL client for SQL
and its native Flight client for named datasets. Both paths have recorded
passing integration/race coverage in the [compatibility matrix](../client-compatibility.md).

## Install and query SQL

Follow the [server and sample table setup](README.md). In a separate Go module,
install the repository's recorded Arrow version:

```sh
go mod init example.com/flight-demo
go get github.com/apache/arrow-go/v18@v18.8.0
```

Save as `main.go` and run `go mod tidy`, then `go run .`:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    if err := run(); err != nil { log.Fatal(err) }
}

func run() error {
    ctx := context.Background()
    client, err := flightsql.NewClient("localhost:8815", nil, nil,
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil { return err }
    defer client.Close()
    info, err := client.Execute(ctx, "SELECT * FROM flight_tool_sample ORDER BY id")
    if err != nil { return err }
    for _, ep := range info.Endpoint {
        reader, err := client.DoGet(ctx, ep.Ticket)
        if err != nil { return err }
        for reader.Next() { fmt.Println(reader.RecordBatch()) }
        err = reader.Err()
        reader.Release()
        if err != nil { return err }
    }
    return nil
}
```

Expect three rows across the returned record batches. Consume all endpoints,
check stream errors and release readers. Use `ExecuteUpdate` for DDL/DML and
`Prepare` for bound parameters. SQL is autocommit; transactions are unsupported.

## Native datasets and validation

Create a native client with `flight.NewClientWithMiddleware` from
`github.com/apache/arrow-go/v18/arrow/flight`. Use path descriptors for named
datasets, Arrow IPC over `DoPut`/`DoGet`, and consume upload acknowledgements.
The [dataset tests](../../server/integration_test.go) show complete upload,
download, selection and cleanup flows; the [dataset reference](../data-access.md#datasets)
defines descriptors, tickets and actions. Native datasets do not appear as SQL tables.

From this repository, `make test-integration` and `make test-cluster` run race
suites against an exported authenticated `rediss://` URI and the qualified
Enterprise composition; see [integration setup](../sdlc/testing.md). These tests
create tables, keys and ACL users; use a disposable backend. Without `REDIS_URI`, backend tests skip.
Remove the shared sample table with the [fixture cleanup](README.md#optional-sample-sql-table).

## Write and verify through Go Flight SQL

In the `run()` function above, after client creation and before its SELECT,
add this block. It writes a unique table, checks the returned value and drops
its fixture. Add `"github.com/google/uuid"` to imports and run `go mod tidy`.
This illustrative snippet has not been rerun against a live endpoint.

```go
name := "guide_" + uuid.NewString()
name = "\"" + name + "\"" // Quote the UUID's hyphens as part of the identifier.
if _, err := client.ExecuteUpdate(ctx, "CREATE TABLE " + name + " (id BIGINT, amount DOUBLE)"); err != nil {
    return err
}
defer func() {
    if _, err := client.ExecuteUpdate(ctx, "DROP TABLE " + name); err != nil { log.Print(err) }
}()
if _, err := client.ExecuteUpdate(ctx, "INSERT INTO " + name + " VALUES (1, 2.5)"); err != nil {
    return err
}
written, err := client.Execute(ctx, "SELECT amount FROM " + name)
if err != nil { return err }
count := int64(0)
for _, ep := range written.Endpoint {
    reader, err := client.DoGet(ctx, ep.Ticket)
    if err != nil { return err }
    for reader.Next() {
        batch := reader.RecordBatch()
        amounts, ok := batch.Column(0).(*array.Float64)
        if !ok { reader.Release(); return fmt.Errorf("expected DOUBLE amount") }
        for i := 0; i < amounts.Len(); i++ {
            if amounts.IsNull(i) || amounts.Value(i) != 2.5 {
                reader.Release(); return fmt.Errorf("write/read-back mismatch")
            }
        }
        count += batch.NumRows()
    }
    err = reader.Err()
    reader.Release()
    if err != nil { return err }
}
if count != 1 { return fmt.Errorf("expected one written row, got %d", count) }
```

Also add `"github.com/apache/arrow-go/v18/arrow/array"` to the imports for the
typed DOUBLE read-back check.
