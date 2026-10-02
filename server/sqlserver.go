package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"time"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sqlServer implements Flight SQL on the Redis ADBC driver.
//
// It keeps no per-statement state. A statement's ticket, and a prepared
// statement's handle, carry the SQL and its bound parameters (as an Arrow
// IPC stream), so any replica can serve any request and nothing needs
// cleaning up. A bind returns the new handle, as Flight SQL's stateless
// prepared statements do (DoPutPreparedStatementResult).
//
// A query that only reads (SELECT, SHOW) runs when its ticket is read. Any
// other statement runs when its FlightInfo is asked for, since a client
// may never read the stream of an INSERT or a CREATE TABLE (the ADBC
// driver cancels it), and reading a stream twice must not run it twice;
// its ticket then carries the result.
type sqlServer struct {
	flightsql.BaseServer
	tenants   *tenantCache
	keyPrefix string
}

const (
	// maxInlineBytes caps what a ticket carries (bound parameters, or a
	// result). Tickets travel in FlightInfo replies, which gRPC clients
	// refuse over 4 MiB by default.
	maxInlineBytes = 1 << 20
	// resultTTL is how long a result too big for its ticket stays in Redis.
	resultTTL = 10 * time.Minute
)

var errNoTransactions = status.Error(codes.InvalidArgument,
	"transactions are not supported: the Redis driver runs every statement in autocommit mode")

func newSQLServer(tenants *tenantCache, alloc memory.Allocator, keyPrefix string) (*sqlServer, error) {
	s := &sqlServer{tenants: tenants, keyPrefix: keyPrefix}
	s.Alloc = alloc
	info := map[flightsql.SqlInfo]any{
		flightsql.SqlInfoFlightSqlServerName:                        ServerName,
		flightsql.SqlInfoFlightSqlServerVersion:                     Version,
		flightsql.SqlInfoFlightSqlServerArrowVersion:                arrow.PkgVersion,
		flightsql.SqlInfoFlightSqlServerReadOnly:                    false,
		flightsql.SqlInfoFlightSqlServerSql:                         true,
		flightsql.SqlInfoFlightSqlServerSubstrait:                   false,
		flightsql.SqlInfoFlightSqlServerTransaction:                 int32(flightsql.SqlTransactionNone),
		flightsql.SqlInfoFlightSqlServerCancel:                      false,
		flightsql.SqlInfoFlightSqlServerBulkIngestion:               true,
		flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported: false,
		flightsql.SqlInfoFlightSqlServerStatementTimeout:            int32(0),
		flightsql.SqlInfoFlightSqlServerTransactionTimeout:          int32(0),
		flightsql.SqlInfoDDLCatalog:                                 false,
		flightsql.SqlInfoDDLSchema:                                  false,
		flightsql.SqlInfoDDLTable:                                   true,
		flightsql.SqlInfoIdentifierQuoteChar:                        `"`,
		flightsql.SqlInfoAllTablesAreASelectable:                    true,
		flightsql.SqlInfoNullOrdering:                               int64(flightsql.SqlNullOrderingSortAtEnd),
		flightsql.SqlInfoTransactionsSupported:                      false,
	}
	for id, v := range info {
		if err := s.RegisterSqlInfo(id, v); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// statementHandle is a statement ticket's handle, and a prepared
// statement's: the SQL and the bound parameters. The ticket of a statement
// that already ran has its result instead: Result, or ResultKey, the
// Redis key that holds it.
type statementHandle struct {
	Query     string `json:"q,omitempty"`
	Params    []byte `json:"p,omitempty"` // an Arrow IPC stream
	Result    []byte `json:"r,omitempty"` // an Arrow IPC stream
	ResultKey string `json:"k,omitempty"`
}

func (h statementHandle) encode() []byte {
	b, _ := json.Marshal(h) // a string and bytes always marshal
	return b
}

func decodeHandle(b []byte) (statementHandle, error) {
	var h statementHandle
	if err := json.Unmarshal(b, &h); err != nil {
		return h, status.Errorf(codes.InvalidArgument, "invalid statement handle: %v", err)
	}
	return h, nil
}

// newStatement makes a statement on conn for h's query with h's parameters
// bound.
func (s *sqlServer) newStatement(ctx context.Context, conn adbc.ConnectionWithContext, h statementHandle) (adbc.StatementWithContext, error) {
	stmt, err := conn.NewStatement(ctx)
	if err != nil {
		return nil, err
	}
	if err := stmt.SetSqlQuery(ctx, h.Query); err != nil {
		_ = stmt.Close(ctx)
		return nil, err
	}
	if len(h.Params) > 0 {
		schema, recs, err := readIPC(h.Params, s.Alloc)
		if err != nil {
			_ = stmt.Close(ctx)
			return nil, status.Errorf(codes.InvalidArgument, "invalid parameters in statement handle: %v", err)
		}
		rdr, err := array.NewRecordReader(schema, recs)
		releaseAll(recs)
		if err != nil {
			_ = stmt.Close(ctx)
			return nil, err
		}
		// The statement takes ownership of the reader.
		if err := stmt.BindStream(ctx, rdr); err != nil {
			_ = stmt.Close(ctx)
			return nil, err
		}
	}
	return stmt, nil
}

// planSchema returns the schema of h's result without running it. It is
// empty (no fields) for a statement that returns no rows.
func (s *sqlServer) planSchema(ctx context.Context, t *tenant, h statementHandle) (*arrow.Schema, error) {
	var schema *arrow.Schema
	err := t.withConn(ctx, "", func(conn adbc.ConnectionWithContext) error {
		stmt, err := s.newStatement(ctx, conn, h)
		if err != nil {
			return err
		}
		defer stmt.Close(ctx)
		es, ok := stmt.(adbc.StatementExecuteSchema)
		if !ok {
			return adbc.Error{Code: adbc.StatusNotImplemented, Msg: "ExecuteSchema is not supported"}
		}
		schema, err = es.ExecuteSchema(ctx)
		return err
	})
	return schema, err
}

// infoSchema is planSchema for a FlightInfo: when the driver can't plan the
// statement, the schema is left out and the client reads it from the
// stream. A schema that may be wrong is never sent, since ADBC clients
// check that the stream's schema matches.
func (s *sqlServer) infoSchema(ctx context.Context, t *tenant, h statementHandle) (*arrow.Schema, error) {
	schema, err := s.planSchema(ctx, t, h)
	if err != nil {
		if st := toStatus(err); status.Code(st) != codes.Unimplemented {
			return nil, st
		}
		return nil, nil
	}
	return schema, nil
}

func (s *sqlServer) flightInfo(desc *flight.FlightDescriptor, schema *arrow.Schema, ticket []byte) *flight.FlightInfo {
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
	if schema != nil {
		info.Schema = flight.SerializeSchema(schema, s.Alloc)
	}
	return info
}

// readOnly reports whether running a script has no effect besides its
// result, so it can run when its stream is read, and again if read again.
func readOnly(parsed []redis.ParsedStmt) bool {
	for _, ps := range parsed {
		switch ps.Stmt.(type) {
		case *redis.SelectStmt, *redis.ShowStmt:
		default:
			return false
		}
	}
	return len(parsed) > 0
}

// plannedOnRun reports whether a script can be planned only by running it:
// it has several statements and changes things, so its last statement may
// use a table or setting that an earlier one makes, and the driver plans
// only the last statement, on a fresh session.
func plannedOnRun(parsed []redis.ParsedStmt) bool {
	return len(parsed) > 1 && !readOnly(parsed)
}

var errPlannedOnRun = status.Error(codes.Unimplemented,
	"the result schema of a script that changes things is known only when it runs: read it from its result")

func (s *sqlServer) statementInfo(ctx context.Context, h statementHandle, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	parsed, err := redis.ParseScript(h.Query)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if !readOnly(parsed) {
		return s.runNow(ctx, t, h, desc)
	}
	schema, err := s.infoSchema(ctx, t, h)
	if err != nil {
		return nil, err
	}
	tkt, err := flightsql.CreateStatementQueryTicket(h.encode())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to make a ticket: %v", err)
	}
	return s.flightInfo(desc, schema, tkt), nil
}

func (s *sqlServer) statementSchema(ctx context.Context, h statementHandle) (*flight.SchemaResult, error) {
	if parsed, err := redis.ParseScript(h.Query); err == nil && plannedOnRun(parsed) {
		return nil, errPlannedOnRun
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	schema, err := s.planSchema(ctx, t, h)
	if err != nil {
		return nil, toStatus(err)
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.Alloc)}, nil
}

// runNow runs a statement that changes something, and returns a
// FlightInfo whose ticket carries the result.
func (s *sqlServer) runNow(ctx context.Context, t *tenant, h statementHandle, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var rdr array.RecordReader
	err := t.withConn(ctx, h.Query, func(conn adbc.ConnectionWithContext) error {
		stmt, err := s.newStatement(ctx, conn, h)
		if err != nil {
			return err
		}
		defer stmt.Close(ctx)
		rdr, _, err = stmt.ExecuteQuery(ctx)
		return err
	})
	if err != nil {
		return nil, toStatus(err)
	}
	defer rdr.Release()
	schema := rdr.Schema()
	recs, rows, err := readAll(rdr)
	if err != nil {
		return nil, toStatus(err)
	}
	b, err := writeIPC(schema, recs, s.Alloc)
	releaseAll(recs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to encode the result: %v", err)
	}
	var tk statementHandle
	if len(b) <= maxInlineBytes {
		tk.Result = b
	} else {
		tk.ResultKey = s.keyPrefix + "result:{" + rand.Text() + "}"
		if err := t.rdb.Set(ctx, tk.ResultKey, b, resultTTL).Err(); err != nil {
			return nil, status.Errorf(status.Code(toStatus(err)),
				"the statement ran, but its result could not be kept for reading: %v", err)
		}
	}
	tkt, err := flightsql.CreateStatementQueryTicket(tk.encode())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to make a ticket: %v", err)
	}
	info := s.flightInfo(desc, schema, tkt)
	if schema.NumFields() > 0 {
		info.TotalRecords = rows // RETURNING rows; unknown (-1) otherwise
	}
	return info, nil
}

// streamResult streams a result a ticket carries.
func (s *sqlServer) streamResult(ctx context.Context, h statementHandle) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	b := h.Result
	if b == nil {
		if !strings.HasPrefix(h.ResultKey, s.keyPrefix+"result:") {
			return nil, nil, status.Error(codes.InvalidArgument, "invalid result key in ticket")
		}
		t, done, err := s.tenants.acquire(ctx)
		if err != nil {
			return nil, nil, err
		}
		defer done()
		b, err = t.rdb.Get(ctx, h.ResultKey).Bytes()
		if errors.Is(err, goredis.Nil) {
			return nil, nil, status.Errorf(codes.NotFound,
				"the statement's result was kept for %s and has expired", resultTTL)
		}
		if err != nil {
			return nil, nil, toStatus(err)
		}
	}
	schema, recs, err := readIPC(b, s.Alloc)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "invalid result in ticket: %v", err)
	}
	rdr, err := array.NewRecordReader(schema, recs)
	releaseAll(recs)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "invalid result: %v", err)
	}
	return schema, streamRecords(ctx, rdr), nil
}

// executeQuery runs h and streams its result. The driver holds the whole
// result in memory, so the connection goes back to the pool before
// streaming starts.
func (s *sqlServer) executeQuery(ctx context.Context, h statementHandle) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer done()
	var rdr array.RecordReader
	err = t.withConn(ctx, h.Query, func(conn adbc.ConnectionWithContext) error {
		stmt, err := s.newStatement(ctx, conn, h)
		if err != nil {
			return err
		}
		defer stmt.Close(ctx)
		rdr, _, err = stmt.ExecuteQuery(ctx)
		return err
	})
	if err != nil {
		return nil, nil, toStatus(err)
	}
	return rdr.Schema(), streamRecords(ctx, rdr), nil
}

// executeUpdate runs h, with params bound instead of h's parameters if
// params is not nil, and returns the number of rows changed (-1 if
// unknown). It takes ownership of params.
func (s *sqlServer) executeUpdate(ctx context.Context, h statementHandle, params array.RecordReader) (int64, error) {
	if params != nil {
		h.Params = nil
		defer func() {
			if params != nil {
				params.Release()
			}
		}()
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	var n int64
	err = t.withConn(ctx, h.Query, func(conn adbc.ConnectionWithContext) error {
		stmt, err := s.newStatement(ctx, conn, h)
		if err != nil {
			return err
		}
		defer stmt.Close(ctx)
		if params != nil {
			// The statement now owns params, even if binding fails.
			p := params
			params = nil
			if err := stmt.BindStream(ctx, p); err != nil {
				return err
			}
		}
		n, err = stmt.ExecuteUpdate(ctx)
		return err
	})
	return n, toStatus(err)
}

// ---- statements ----

func (s *sqlServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return nil, errNoTransactions
	}
	return s.statementInfo(ctx, statementHandle{Query: cmd.GetQuery()}, desc)
}

func (s *sqlServer) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return nil, errNoTransactions
	}
	return s.statementSchema(ctx, statementHandle{Query: cmd.GetQuery()})
}

func (s *sqlServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	h, err := decodeHandle(tkt.GetStatementHandle())
	if err != nil {
		return nil, nil, err
	}
	if h.Result != nil || h.ResultKey != "" {
		return s.streamResult(ctx, h)
	}
	return s.executeQuery(ctx, h)
}

func (s *sqlServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return 0, errNoTransactions
	}
	return s.executeUpdate(ctx, statementHandle{Query: cmd.GetQuery()}, nil)
}

// ---- prepared statements ----

func (s *sqlServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	var res flightsql.ActionCreatePreparedStatementResult
	if len(req.GetTransactionId()) > 0 {
		return res, errNoTransactions
	}
	h := statementHandle{Query: req.GetQuery()}
	parsed, err := redis.ParseScript(h.Query)
	if err != nil {
		return res, status.Error(codes.InvalidArgument, err.Error())
	}
	res.Handle = h.encode()
	// Planning such a script would fail on what its earlier statements
	// make; the client reads its schema from the result instead.
	if plannedOnRun(parsed) {
		return res, nil
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return res, err
	}
	defer done()
	schema, err := s.infoSchema(ctx, t, h)
	if err != nil {
		return res, err
	}
	// Without bound values the driver types parameters as NULL, so a result
	// column computed from a parameter has no real type yet.
	if schema != nil && numParams(parsed) > 0 && hasNullField(schema) {
		schema = nil
	}
	res.DatasetSchema = schema
	if schema != nil {
		isUpdate := schema.NumFields() == 0
		res.IsUpdate = &isUpdate
	}
	// ParameterSchema stays nil: the driver can't describe parameters, and
	// an empty schema would tell clients there are none.
	return res, nil
}

func numParams(parsed []redis.ParsedStmt) int {
	n := 0
	for _, ps := range parsed {
		n = max(n, ps.NumParams)
	}
	return n
}

func hasNullField(schema *arrow.Schema) bool {
	for _, f := range schema.Fields() {
		if f.Type.ID() == arrow.NULL {
			return true
		}
	}
	return false
}

func (s *sqlServer) ClosePreparedStatement(context.Context, flightsql.ActionClosePreparedStatementRequest) error {
	return nil // the handle is the whole statement
}

// DoPutPreparedStatementQuery binds parameters for a query, returning the
// handle that carries them.
func (s *sqlServer) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) ([]byte, error) {
	h, err := decodeHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	schema, recs, bound, err := readParams(rdr)
	if err != nil {
		return nil, err
	}
	defer releaseAll(recs)
	h.Params = nil
	if bound {
		// With no rows too: the query then runs zero times.
		b, err := writeIPC(schema, recs, s.Alloc)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to encode the parameters: %v", err)
		}
		if len(b) > maxInlineBytes {
			return nil, status.Errorf(codes.InvalidArgument,
				"the parameters of a query take %d bytes, more than the %d a prepared query can hold; "+
					"bind fewer rows, or use a prepared update", len(b), maxInlineBytes)
		}
		h.Params = b
	}
	return h.encode(), nil
}

// readParams reads bound parameters. bound is false when the stream has no
// schema or no columns: clients send that for a statement without
// parameters. A schema with columns binds even with no rows, and the
// statement then runs zero times. recs are the batches that have rows.
func readParams(rdr flight.MessageReader) (schema *arrow.Schema, recs []arrow.RecordBatch, bound bool, err error) {
	schema = rdr.Schema()
	if schema == nil {
		if err := rdr.Err(); err != nil {
			return nil, nil, false, status.Errorf(codes.InvalidArgument, "failed to read the parameters: %v", err)
		}
		return nil, nil, false, nil
	}
	recs, _, err = readAll(rdr)
	if err != nil {
		return nil, nil, false, status.Error(codes.InvalidArgument, err.Error())
	}
	if schema.NumFields() == 0 {
		releaseAll(recs)
		return schema, nil, false, nil
	}
	return schema, recs, true, nil
}

func (s *sqlServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	h, err := decodeHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	return s.statementInfo(ctx, h, desc)
}

func (s *sqlServer) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	h, err := decodeHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	return s.statementSchema(ctx, h)
}

func (s *sqlServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	h, err := decodeHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, nil, err
	}
	return s.executeQuery(ctx, h)
}

// DoPutPreparedStatementUpdate runs a prepared update once per row of the
// uploaded parameters (zero times for none), or with the parameters
// already in the handle when the upload has no columns.
func (s *sqlServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	h, err := decodeHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return 0, err
	}
	schema, recs, bound, err := readParams(rdr)
	if err != nil {
		return 0, err
	}
	var params array.RecordReader
	if bound {
		params, err = array.NewRecordReader(schema, recs)
		releaseAll(recs)
		if err != nil {
			return 0, status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
		}
	}
	return s.executeUpdate(ctx, h, params)
}

// CloseSession succeeds: the server keeps no sessions, and the ADBC Flight
// SQL driver closes one when its connection closes.
func (s *sqlServer) CloseSession(context.Context, *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

// ---- bulk ingest ----

func (s *sqlServer) DoPutCommandStatementIngest(ctx context.Context, cmd flightsql.StatementIngest, rdr flight.MessageReader) (int64, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return 0, errNoTransactions
	}
	if cmd.GetTemporary() || isTempSchemaName(cmd.GetSchema()) {
		return 0, status.Error(codes.InvalidArgument,
			"temporary tables are not supported: they belong to one connection, and each Flight request may use another")
	}
	// The command's fields say where and how to ingest; the same settings as
	// ADBC options could override them (and make the table temporary).
	for k := range cmd.GetOptions() {
		if strings.HasPrefix(k, "adbc.ingest.") {
			return 0, status.Errorf(codes.InvalidArgument,
				"option %q is not accepted: set the ingest target, mode and temporary with the command's own fields", k)
		}
	}
	table := cmd.GetTable()
	if table == "" {
		return 0, status.Error(codes.InvalidArgument, "bulk ingest needs a table name")
	}
	mode, mustExist, err := ingestMode(cmd.GetTableDefinitionOptions())
	if err != nil {
		return 0, err
	}
	if rdr.Schema() == nil {
		if err := rdr.Err(); err != nil {
			return 0, status.Errorf(codes.InvalidArgument, "failed to read the uploaded data: %v", err)
		}
		return 0, status.Error(codes.InvalidArgument, "bulk ingest needs an Arrow schema and record batches")
	}
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	var n int64
	err = t.withConn(ctx, "", func(conn adbc.ConnectionWithContext) error {
		if mustExist {
			var catalog, dbSchema *string
			if c := cmd.GetCatalog(); c != "" {
				catalog = &c
			}
			if sc := cmd.GetSchema(); sc != "" {
				dbSchema = &sc
			}
			if _, err := conn.GetTableSchema(ctx, catalog, dbSchema, table); err != nil {
				return err
			}
		}
		stmt, err := conn.NewStatement(ctx)
		if err != nil {
			return err
		}
		defer stmt.Close(ctx)
		opts := [][2]string{
			{adbc.OptionKeyIngestTargetTable, table},
			{adbc.OptionKeyIngestMode, mode},
		}
		if sc := cmd.GetSchema(); sc != "" {
			opts = append(opts, [2]string{adbc.OptionValueIngestTargetDBSchema, sc})
		}
		if c := cmd.GetCatalog(); c != "" {
			opts = append(opts, [2]string{adbc.OptionValueIngestTargetCatalog, c})
		}
		for k, v := range cmd.GetOptions() {
			opts = append(opts, [2]string{k, v})
		}
		for _, o := range opts {
			if err := stmt.SetOption(ctx, o[0], o[1]); err != nil {
				return err
			}
		}
		// The statement releases what it binds; the Flight SQL handler
		// releases rdr too.
		rdr.Retain()
		if err := stmt.BindStream(ctx, rdr); err != nil {
			return err
		}
		n, err = stmt.ExecuteUpdate(ctx)
		return err
	})
	return n, toStatus(err)
}

// ingestMode maps Flight SQL's table definition options to an ADBC ingest
// mode. mustExist is set when the table has to exist already (ADBC's
// replace mode creates a missing table). Unspecified options mean create
// the table, and fail if it exists, as ADBC's default mode does.
func ingestMode(opts *flightsql.TableDefinitionOptions) (mode string, mustExist bool, err error) {
	notExist := flightsql.TableDefinitionOptionsTableNotExistOptionCreate
	exists := flightsql.TableDefinitionOptionsTableExistsOptionFail
	if opts != nil {
		if opts.IfNotExist != flightsql.TableDefinitionOptionsTableNotExistOptionUnspecified {
			notExist = opts.IfNotExist
		}
		if opts.IfExists != flightsql.TableDefinitionOptionsTableExistsOptionUnspecified {
			exists = opts.IfExists
		}
	}
	create := notExist == flightsql.TableDefinitionOptionsTableNotExistOptionCreate
	switch exists {
	case flightsql.TableDefinitionOptionsTableExistsOptionFail:
		if create {
			return adbc.OptionValueIngestModeCreate, false, nil
		}
		return "", false, status.Error(codes.InvalidArgument,
			"bulk ingest that fails both when the table exists and when it doesn't can never succeed")
	case flightsql.TableDefinitionOptionsTableExistsOptionAppend:
		if create {
			return adbc.OptionValueIngestModeCreateAppend, false, nil
		}
		return adbc.OptionValueIngestModeAppend, false, nil
	case flightsql.TableDefinitionOptionsTableExistsOptionReplace:
		return adbc.OptionValueIngestModeReplace, !create, nil
	}
	return "", false, status.Errorf(codes.InvalidArgument, "unknown if_exists option %v", exists)
}
