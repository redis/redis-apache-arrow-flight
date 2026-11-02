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

From this repository, `make test-integration` and `make test-cluster` start the
Compose backends and run race suites. Use disposable backends: these tests
create tables, keys and ACL users. Without `REDIS_URI`, backend tests skip.
Remove the shared sample table with the [fixture cleanup](README.md#optional-sample-sql-table).
