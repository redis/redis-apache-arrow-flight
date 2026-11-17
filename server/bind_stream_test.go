package server

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Match ADBC's BindStream contract: success retains a separate reference;
// refusal leaves the caller's reference untouched.
type bindingStatement struct {
	adbc.StatementWithContext
	params  array.RecordReader
	bindErr error
}

func (*bindingStatement) SetOption(context.Context, string, string) error { return nil }
func (*bindingStatement) SetSqlQuery(context.Context, string) error       { return nil }
func (s *bindingStatement) BindStream(_ context.Context, rdr array.RecordReader) error {
	if s.bindErr != nil {
		return s.bindErr
	}
	rdr.Retain()
	s.params = rdr
	return nil
}
func (s *bindingStatement) Close(context.Context) error {
	if s.params != nil {
		s.params.Release()
		s.params = nil
	}
	return nil
}
func (s *bindingStatement) ExecuteUpdate(context.Context) (int64, error) {
	var rows int64
	for s.params.Next() {
		rows += s.params.RecordBatch().NumRows()
	}
	return rows, s.params.Err()
}

type bindingConnection struct {
	adbc.ConnectionWithContext
	stmt *bindingStatement
}

func (c *bindingConnection) NewStatement(context.Context) (adbc.StatementWithContext, error) {
	return c.stmt, nil
}
func (*bindingConnection) Close(context.Context) error { return nil }

type bindingDatabase struct {
	adbc.DatabaseWithContext
	conn *bindingConnection
}

func (d *bindingDatabase) Open(context.Context) (adbc.ConnectionWithContext, error) {
	return d.conn, nil
}

// Only RecordReader methods are used by the ingest handler and fake statement.
type bindingMessageReader struct {
	flight.MessageReader
	array.RecordReader
}

func (r bindingMessageReader) Retain()                        { r.RecordReader.Retain() }
func (r bindingMessageReader) Release()                       { r.RecordReader.Release() }
func (r bindingMessageReader) Schema() *arrow.Schema          { return r.RecordReader.Schema() }
func (r bindingMessageReader) Next() bool                     { return r.RecordReader.Next() }
func (r bindingMessageReader) RecordBatch() arrow.RecordBatch { return r.RecordReader.RecordBatch() }
func (r bindingMessageReader) Record() arrow.RecordBatch      { return r.RecordReader.RecordBatch() }
func (r bindingMessageReader) Err() error                     { return r.RecordReader.Err() }

type bindingIngest struct{ pb.CommandStatementIngest }

func (*bindingIngest) GetTableDefinitionOptions() *flightsql.TableDefinitionOptions { return nil }

func TestBindStreamOwnership(t *testing.T) {
	for _, path := range []string{"handle", "update", "ingest"} {
		for _, refused := range []bool{false, true} {
			name := path + "/success"
			if refused {
				name = path + "/refused"
			}
			t.Run(name, func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
				b := array.NewRecordBuilder(alloc, schema)
				b.Field(0).(*array.Int64Builder).Append(42)
				rec := b.NewRecordBatch()
				b.Release()
				rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
				rec.Release()
				if err != nil {
					t.Fatal(err)
				}
				stmt := &bindingStatement{}
				if refused {
					stmt.bindErr = errors.New("binding refused")
				}
				conn := &bindingConnection{stmt: stmt}
				tn := &tenant{pool: newConnPool(&bindingDatabase{conn: conn}, 1, 1)}
				cfg := &Config{}
				cfg.setDefaults()
				srv := &sqlServer{tenants: &tenantCache{cfg: cfg, tenants: map[string]*tenant{(identity{}).key(): tn}}}
				srv.Alloc = alloc
				ctx := context.Background()
				var rows int64
				switch path {
				case "handle":
					params, e := writeIPC(schema, []arrow.RecordBatch{rec}, alloc)
					rdr.Release()
					if e != nil {
						t.Fatal(e)
					}
					var bound adbc.StatementWithContext
					bound, err = srv.newStatement(ctx, conn, statementHandle{Query: "SELECT $1", Params: params}, false)
					if err == nil {
						rows, err = bound.ExecuteUpdate(ctx)
						_ = bound.Close(ctx)
					}
				case "update":
					rows, err = srv.executeUpdate(ctx, statementHandle{Query: "INSERT INTO t VALUES ($1)"}, rdr)
				case "ingest":
					rows, err = srv.DoPutCommandStatementIngest(ctx, &bindingIngest{pb.CommandStatementIngest{Table: "t"}}, bindingMessageReader{RecordReader: rdr})
					// The Flight SQL adapter releases its reference after the handler.
					if !refused && rdr.Schema() != schema {
						t.Fatal("handler released caller's reader")
					}
					rdr.Release()
				}
				if refused && err == nil {
					t.Fatal("expected binding refusal")
				}
				if !refused && (err != nil || rows != 1) {
					t.Fatalf("execution = %d rows, %v; want 1 row", rows, err)
				}
			})
		}
	}
}
