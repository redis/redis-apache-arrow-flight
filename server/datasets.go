package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Datasets are named Arrow tables uploaded with DoPut on a PATH descriptor
// and stored in Redis as Arrow IPC, unchanged and unindexed: the fast path
// for whole tables (dataframes, feature snapshots, query results), with any
// Arrow type. Use SQL (Flight SQL) to filter or aggregate.
//
// A dataset's name is its descriptor's path segments joined by "/". Its
// keys share one hash tag, so they live in one cluster slot:
//
//	<prefix>ds:{<name>}        HASH  gen, schema, rows, batches, bytes, updated
//	<prefix>ds:{<name>}:seq    STRING  the last generation number
//	<prefix>ds:{<name>}:g<N>   HASH  generation N, by column:
//	                                 layout  "2"
//	                                 schema  an IPC stream with just the schema
//	                                 batches the number of batches (written last)
//	                                 <b>:rows  batch b's row count
//	                                 <b>:<c>   an IPC stream of batch b's
//	                                           column c (by field index) alone
//
// Generations stored before the HASH layout are a LIST at the same key: an
// IPC stream with just the schema, then one IPC stream per batch. They stay
// readable; the key's type tells the layouts apart. (<name> is
// URL-path-escaped, so braces can't end the hash tag.)
//
// An upload writes a new generation and then switches the HASH to it
// atomically, so readers never see a partial upload. The replaced
// generation expires after a grace period, letting reads already under way
// finish. A ticket names the generation its FlightInfo described, and can
// name columns and a range of batches to read: a read takes only those
// columns' streams from Redis.

const (
	actionDropDataset = "drop_dataset"

	maxDatasetName = 1024
	// uploadTTL expires the keys of an upload that never finishes.
	uploadTTL = time.Hour
	// replacedGrace is how long a replaced generation stays readable.
	replacedGrace = 5 * time.Minute
	// commitTimeout bounds an upload's commit, which runs even if the
	// client has gone.
	commitTimeout = 30 * time.Second
	// A read takes up to readPage batches, and about readPageBytes of the
	// columns it reads, per command (see pageLen).
	readPage      = 64
	readPageBytes = 8 << 20
	// An upload writes every flushBatches batches, or about flushBytes.
	flushBatches = 64
	flushBytes   = 8 << 20

	// layoutColumns is the layout field of a generation stored by column.
	layoutColumns = "2"
)

// pageLen is the number of batches a read takes next, given the largest
// batch it has read so far (of the columns it reads): about readPageBytes
// of them, from 1 to readPage.
func pageLen(largest int64) int64 {
	if largest <= 0 {
		return readPage
	}
	return min(max(readPageBytes/largest, 1), readPage)
}

type datasets struct {
	tenants *tenantCache
	prefix  string
	ttl     time.Duration
	// endpointBatches, if set, is the number of batches per endpoint.
	endpointBatches int64
	alloc           memory.Allocator
}

type datasetKeys struct{ meta, seq string }

func (d *datasets) keys(name string) datasetKeys {
	meta := d.prefix + "ds:{" + url.PathEscape(name) + "}"
	return datasetKeys{meta: meta, seq: meta + ":seq"}
}

func (k datasetKeys) gen(n int64) string { return k.meta + ":g" + strconv.FormatInt(n, 10) }

// nameFromKey is the reverse of keys(name).meta.
func (d *datasets) nameFromKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, d.prefix+"ds:{")
	if !ok || !strings.HasSuffix(rest, "}") {
		return "", false
	}
	name, err := url.PathUnescape(strings.TrimSuffix(rest, "}"))
	return name, err == nil
}

func datasetName(desc *flight.FlightDescriptor) (string, error) {
	if desc == nil || len(desc.Path) == 0 {
		return "", status.Error(codes.InvalidArgument, "a dataset descriptor needs a path")
	}
	if slices.Contains(desc.Path, "") {
		return "", status.Error(codes.InvalidArgument, "dataset path segments must not be empty")
	}
	name := strings.Join(desc.Path, "/")
	if len(name) > maxDatasetName {
		return "", status.Errorf(codes.InvalidArgument, "dataset names are limited to %d bytes", maxDatasetName)
	}
	return name, nil
}

// datasetTicket is a dataset's DoGet ticket. Generation 0 means the
// current one, so a client can write {"dataset": "name"} itself.
type datasetTicket struct {
	Dataset    string `json:"dataset"`
	Generation int64  `json:"generation,omitempty"`
	// Columns, if set, reads just these columns, in this order.
	Columns []string `json:"columns,omitempty"`
	// Batches, if set, reads the stored batches [start, end), counted from
	// 0. An end past the last batch reads to the last.
	Batches []int64 `json:"batches,omitempty"`
}

// batchRange returns the ticket's batches, [lo, hi), of a generation of n.
func (dt datasetTicket) batchRange(n int64) (lo, hi int64) {
	if dt.Batches == nil {
		return 0, n
	}
	hi = min(dt.Batches[1], n)
	return min(dt.Batches[0], hi), hi
}

func (dt datasetTicket) validate() error {
	if dt.Dataset == "" {
		return status.Error(codes.InvalidArgument, `invalid dataset ticket: want {"dataset": "<name>"}`)
	}
	if b := dt.Batches; b != nil && (len(b) != 2 || b[0] < 0 || b[1] < b[0]) {
		return status.Error(codes.InvalidArgument, `invalid dataset ticket: want "batches": [start, end] with 0 <= start <= end`)
	}
	return nil
}

// projection returns the indices in schema of the named columns, and the
// schema of just those, or nil for all the columns.
func projection(schema *arrow.Schema, names []string) ([]int, *arrow.Schema, error) {
	if names == nil {
		return nil, schema, nil
	}
	idx := make([]int, len(names))
	fields := make([]arrow.Field, len(names))
	seen := map[string]bool{}
	for i, name := range names {
		switch at := schema.FieldIndices(name); {
		case seen[name]:
			return nil, nil, status.Errorf(codes.InvalidArgument, "column %q is named twice", name)
		case len(at) == 0:
			return nil, nil, status.Errorf(codes.InvalidArgument, "the dataset has no column %q", name)
		case len(at) > 1:
			return nil, nil, status.Errorf(codes.InvalidArgument, "the dataset has more than one column %q", name)
		default:
			seen[name] = true
			idx[i], fields[i] = at[0], schema.Field(at[0])
		}
	}
	md := schema.Metadata()
	return idx, arrow.NewSchema(fields, &md), nil
}

type datasetMeta struct {
	Name       string    `json:"dataset"`
	Generation int64     `json:"generation"`
	Rows       int64     `json:"rows"`
	Batches    int64     `json:"batches"`
	Bytes      int64     `json:"bytes"`
	Updated    time.Time `json:"updated"`
	// Superseded is set in an upload's reply when a newer upload of the
	// dataset finished first.
	Superseded bool `json:"superseded,omitempty"`
	schema     []byte
}

func parseMeta(name string, h map[string]string) (datasetMeta, bool) {
	m := datasetMeta{Name: name, schema: []byte(h["schema"])}
	var err error
	if m.Generation, err = strconv.ParseInt(h["gen"], 10, 64); err != nil || m.Generation <= 0 {
		return m, false
	}
	m.Rows, _ = strconv.ParseInt(h["rows"], 10, 64)
	m.Batches, _ = strconv.ParseInt(h["batches"], 10, 64)
	m.Bytes, _ = strconv.ParseInt(h["bytes"], 10, 64)
	if ms, err := strconv.ParseInt(h["updated"], 10, 64); err == nil {
		m.Updated = time.UnixMilli(ms).UTC()
	}
	return m, true
}

func (d *datasets) readMeta(ctx context.Context, rdb goredis.UniversalClient, name string) (datasetMeta, error) {
	h, err := rdb.HGetAll(ctx, d.keys(name).meta).Result()
	if err != nil {
		return datasetMeta{}, toStatus(err)
	}
	m, ok := parseMeta(name, h)
	if !ok {
		return m, status.Errorf(codes.NotFound, "dataset %q not found", name)
	}
	return m, nil
}

// flightInfo describes a dataset: one endpoint, or with endpointBatches
// set, one per that many batches, in order.
func (d *datasets) flightInfo(m datasetMeta) *flight.FlightInfo {
	endpoint := func(dt datasetTicket) *flight.FlightEndpoint {
		tkt, _ := json.Marshal(dt)
		return &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: tkt}}
	}
	dt := datasetTicket{Dataset: m.Name, Generation: m.Generation}
	var endpoints []*flight.FlightEndpoint
	if n := d.endpointBatches; n > 0 && m.Batches > n {
		for start := int64(0); start < m.Batches; start += n {
			dt.Batches = []int64{start, min(start+n, m.Batches)}
			endpoints = append(endpoints, endpoint(dt))
		}
	} else {
		endpoints = []*flight.FlightEndpoint{endpoint(dt)}
	}
	appMeta, _ := json.Marshal(m)
	return &flight.FlightInfo{
		Schema:           m.schema,
		FlightDescriptor: &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: strings.Split(m.Name, "/")},
		Endpoint:         endpoints,
		TotalRecords:     m.Rows,
		TotalBytes:       m.Bytes,
		Ordered:          len(endpoints) > 1,
		AppMetadata:      appMeta,
	}
}

func (d *datasets) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	name, err := datasetName(desc)
	if err != nil {
		return nil, err
	}
	t, done, err := d.tenants.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	m, err := d.readMeta(ctx, t.rdb, name)
	if err != nil {
		return nil, err
	}
	return d.flightInfo(m), nil
}

func (d *datasets) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	info, err := d.GetFlightInfo(ctx, desc)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: info.Schema}, nil
}

// ---- upload ----

// genWriter writes a generation's HASH in pipelined HSETs, every
// flushBatches batches or about flushBytes, keeping the upload's expiry on
// the key.
type genWriter struct {
	rdb     goredis.UniversalClient
	key     string
	pending []any // field, value, …
	batches int
	size    int
}

func (w *genWriter) set(field string, v any) { w.pending = append(w.pending, field, v) }

// addBatch adds batch b: its row count, and its columns' IPC streams.
func (w *genWriter) addBatch(ctx context.Context, b, rows int64, cols [][]byte) error {
	prefix := strconv.FormatInt(b, 10) + ":"
	w.set(prefix+"rows", rows)
	for c, v := range cols {
		w.set(prefix+strconv.Itoa(c), v)
		w.size += len(v)
	}
	if w.batches++; w.batches >= flushBatches || w.size >= flushBytes {
		return w.flush(ctx)
	}
	return nil
}

func (w *genWriter) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	_, err := w.rdb.Pipelined(ctx, func(p goredis.Pipeliner) error {
		p.HSet(ctx, w.key, w.pending...)
		p.PExpire(ctx, w.key, uploadTTL)
		return nil
	})
	w.pending, w.batches, w.size = w.pending[:0], 0, 0
	return err
}

// columnSchemas are the schemas of schema's columns alone.
func columnSchemas(schema *arrow.Schema) []*arrow.Schema {
	out := make([]*arrow.Schema, schema.NumFields())
	for c := range out {
		out[c] = arrow.NewSchema([]arrow.Field{schema.Field(c)}, nil)
	}
	return out
}

// encodeColumns encodes each column of rec as an IPC stream of its own
// (schemas[c], from columnSchemas), with the column's dictionaries.
func encodeColumns(schemas []*arrow.Schema, rec arrow.RecordBatch, mem memory.Allocator) ([][]byte, error) {
	out := make([][]byte, len(schemas))
	for c, s := range schemas {
		col := array.NewRecordBatch(s, []arrow.Array{rec.Column(c)}, rec.NumRows())
		b, err := writeIPC(s, []arrow.RecordBatch{col}, mem)
		col.Release()
		if err != nil {
			return nil, err
		}
		out[c] = b
	}
	return out, nil
}

// assemble makes a batch of schema from the IPC streams of its columns, or
// for a schema without columns, from its row count. It checks they fit
// together. size is their encoded size.
func assemble(schema *arrow.Schema, vals []string, mem memory.Allocator) (rec arrow.RecordBatch, size int64, err error) {
	if schema.NumFields() == 0 {
		rows, err := strconv.ParseInt(vals[0], 10, 64)
		if err != nil || rows < 0 {
			return nil, 0, fmt.Errorf("invalid row count %q", vals[0])
		}
		return array.NewRecordBatch(schema, nil, rows), 0, nil
	}
	arrs := make([]arrow.Array, len(vals))
	defer func() {
		for _, a := range arrs {
			if a != nil {
				a.Release()
			}
		}
	}()
	rows := -1
	for i, v := range vals {
		size += int64(len(v))
		_, recs, err := readIPC([]byte(v), mem)
		if err != nil {
			return nil, 0, err
		}
		f := schema.Field(i)
		switch {
		case len(recs) != 1 || recs[0].NumCols() != 1:
			err = fmt.Errorf("column %q isn't one batch of one column", f.Name)
		case !arrow.TypeEqual(recs[0].Column(0).DataType(), f.Type):
			err = fmt.Errorf("column %q is %s, not %s", f.Name, recs[0].Column(0).DataType(), f.Type)
		case rows >= 0 && int(recs[0].NumRows()) != rows:
			err = fmt.Errorf("column %q has %d rows, not %d", f.Name, recs[0].NumRows(), rows)
		default:
			rows = int(recs[0].NumRows())
			arrs[i] = recs[0].Column(0)
			arrs[i].Retain()
		}
		releaseAll(recs)
		if err != nil {
			return nil, 0, err
		}
	}
	return array.NewRecordBatch(schema, arrs, int64(rows)), size, nil
}

// commitScript switches a dataset to a new generation if its current one
// is still the expected one (ARGV[1], "" for none), and returns 1. It
// returns 0 if the current one changed meanwhile (read it and try again),
// and -1 if the current one is newer: an upload that started later
// finished first, so this one is replaced at once, and its generation
// expires like a replaced one. Generations only go up, and the seq key
// never expires, so a generation number never names two uploads.
//
//	KEYS: meta, new generation, seq, old generation (or the new one again)
//	ARGV: expected gen, new gen, schema, rows, batches, bytes, updated (unix ms),
//	      grace (ms), ttl (ms, 0 for none)
var commitScript = goredis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'gen') or ''
if cur ~= ARGV[1] then return 0 end
if cur ~= '' and tonumber(cur) >= tonumber(ARGV[2]) then
  redis.call('PEXPIRE', KEYS[2], ARGV[8])
  return -1
end
redis.call('HSET', KEYS[1], 'gen', ARGV[2], 'schema', ARGV[3], 'rows', ARGV[4],
  'batches', ARGV[5], 'bytes', ARGV[6], 'updated', ARGV[7])
local ttl = tonumber(ARGV[9])
for i = 1, 2 do
  if ttl > 0 then redis.call('PEXPIRE', KEYS[i], ttl) else redis.call('PERSIST', KEYS[i]) end
end
redis.call('PERSIST', KEYS[3])
if ARGV[1] ~= '' and KEYS[4] ~= KEYS[2] then redis.call('PEXPIRE', KEYS[4], ARGV[8]) end
return 1
`)

// dropScript deletes a dataset if its generation is still ARGV[1], and
// returns 1; 0 if it changed, -1 if there is no dataset. The seq key stays,
// so generation numbers never repeat (an old ticket can't read new data).
//
//	KEYS: meta, generation ARGV[1]
var dropScript = goredis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'gen')
if not cur then return -1 end
if cur ~= ARGV[1] then return 0 end
redis.call('UNLINK', KEYS[1], KEYS[2])
return 1
`)

// DoPut stores an uploaded stream as a dataset's new generation. first is
// the stream's first message, already read.
func (d *datasets) DoPut(stream flight.FlightService_DoPutServer, first *flight.FlightData) error {
	ctx := stream.Context()
	name, err := datasetName(first.FlightDescriptor)
	if err != nil {
		return err
	}
	t, done, err := d.tenants.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()

	rdr, err := flight.NewRecordReader(&replayStream{FlightService_DoPutServer: stream, first: first}, ipc.WithAllocator(d.alloc))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "a dataset upload needs an Arrow schema: %v", err)
	}
	defer rdr.Release()
	schema := rdr.Schema()

	keys := d.keys(name)
	gen, err := t.rdb.Incr(ctx, keys.seq).Result()
	if err != nil {
		return toStatus(err)
	}
	m := datasetMeta{Name: name, Generation: gen, schema: flight.SerializeSchema(schema, d.alloc)}
	w := &genWriter{rdb: t.rdb, key: keys.gen(gen)}
	abort := func(err error) error {
		_ = t.rdb.Del(context.WithoutCancel(ctx), w.key).Err()
		return err
	}

	head, err := writeIPC(schema, nil, d.alloc)
	if err != nil {
		return abort(status.Errorf(codes.Internal, "failed to encode the schema: %v", err))
	}
	w.set("layout", layoutColumns)
	w.set("schema", head)
	colSchemas := columnSchemas(schema)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if rec.NumRows() == 0 {
			continue
		}
		parts := chunks(rec)
		for i, part := range parts {
			cols, err := encodeColumns(colSchemas, part, d.alloc)
			rows := part.NumRows()
			part.Release()
			if err == nil {
				err = w.addBatch(ctx, m.Batches, rows, cols)
			}
			if err != nil {
				releaseAll(parts[i+1:])
				return abort(toStatus(err))
			}
			m.Rows += rows
			m.Batches++
			for _, b := range cols {
				m.Bytes += int64(len(b))
			}
		}
	}
	if err := rdr.Err(); err != nil {
		return abort(status.Errorf(codes.InvalidArgument, "failed to read the upload: %v", err))
	}
	// Last: a generation with a batch count is complete.
	w.set("batches", m.Batches)
	if err := w.flush(ctx); err != nil {
		return abort(toStatus(err))
	}
	if err := ctx.Err(); err != nil {
		return abort(toStatus(err)) // gone before the commit started
	}
	// Once the commit script may have run, its outcome is Redis's, so the
	// client going away mustn't interrupt it. And when its reply is an error,
	// the new generation is deleted only if Redis says it isn't current.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	m.Updated = time.Now().UTC()
	superseded, err := d.commit(cctx, t.rdb, keys, m)
	if err != nil {
		current, cerr := d.isCurrent(cctx, t.rdb, keys, gen)
		switch {
		case cerr != nil:
			// Unknown. If it wasn't committed, the upload expiry removes it.
			return status.Errorf(status.Code(err), "could not confirm whether the upload was committed: %v", err)
		case !current:
			// It may have been committed and then replaced already, so it
			// expires like a replaced generation rather than at once.
			_ = t.rdb.PExpire(cctx, w.key, replacedGrace).Err()
			return err
		}
		// The commit went through, and only its reply was lost.
	}
	m.Superseded = superseded

	res, _ := json.Marshal(m)
	return stream.Send(&flight.PutResult{AppMetadata: res})
}

// isCurrent reports whether gen is the dataset's current generation.
func (d *datasets) isCurrent(ctx context.Context, rdb goredis.UniversalClient, keys datasetKeys, gen int64) (bool, error) {
	cur, err := rdb.HGet(ctx, keys.meta, "gen").Result()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return cur == strconv.FormatInt(gen, 10), nil
}

// commit makes m's generation the dataset's current one. superseded is
// set when a newer upload had already committed, so this one was replaced
// at once (see commitScript).
func (d *datasets) commit(ctx context.Context, rdb goredis.UniversalClient, keys datasetKeys, m datasetMeta) (superseded bool, err error) {
	for range 10 {
		cur, err := rdb.HGet(ctx, keys.meta, "gen").Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return false, toStatus(err)
		}
		old := keys.gen(m.Generation)
		if cur != "" {
			n, err := strconv.ParseInt(cur, 10, 64)
			if err != nil {
				return false, status.Errorf(codes.Internal, "dataset %q has an invalid generation %q", m.Name, cur)
			}
			old = keys.gen(n)
		}
		res, err := commitScript.Run(ctx, rdb,
			[]string{keys.meta, keys.gen(m.Generation), keys.seq, old},
			cur, m.Generation, m.schema, m.Rows, m.Batches, m.Bytes, m.Updated.UnixMilli(),
			replacedGrace.Milliseconds(), d.ttl.Milliseconds(),
		).Int()
		if err != nil {
			return false, toStatus(err)
		}
		switch res {
		case 1:
			return false, nil
		case -1:
			return true, nil
		}
	}
	return false, status.Errorf(codes.Aborted, "dataset %q kept changing during the upload; try again", m.Name)
}

// replayStream returns a message already read from a DoPut stream before
// the rest of the stream.
type replayStream struct {
	flight.FlightService_DoPutServer
	first *flight.FlightData
}

func (s *replayStream) Recv() (*flight.FlightData, error) {
	if s.first != nil {
		d := s.first
		s.first = nil
		return d, nil
	}
	return s.FlightService_DoPutServer.Recv()
}

// ---- download ----

func (d *datasets) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	ctx := stream.Context()
	var dt datasetTicket
	if err := json.Unmarshal(tkt.Ticket, &dt); err != nil {
		return status.Error(codes.InvalidArgument, `invalid dataset ticket: want {"dataset": "<name>"}`)
	}
	if err := dt.validate(); err != nil {
		return err
	}
	t, done, err := d.tenants.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()

	// A dropped dataset is gone even for tickets naming a replaced
	// generation that hasn't expired yet.
	m, err := d.readMeta(ctx, t.rdb, dt.Dataset)
	if err != nil {
		return err
	}
	gen := dt.Generation
	if gen == 0 {
		gen = m.Generation
	} else if gen > m.Generation {
		return status.Errorf(codes.NotFound, "dataset %q generation %d not found", dt.Dataset, gen)
	}
	key := d.keys(dt.Dataset).gen(gen)
	gone := func() error {
		if m, err := d.readMeta(ctx, t.rdb, dt.Dataset); err != nil {
			return err
		} else if m.Generation != gen {
			return status.Errorf(codes.Aborted,
				"dataset %q was replaced since its FlightInfo was read; get it again", dt.Dataset)
		}
		return status.Errorf(codes.NotFound, "dataset %q generation %d not found", dt.Dataset, gen)
	}
	r := &datasetRead{rdb: t.rdb, key: key, dt: dt, gen: gen, gone: gone, alloc: d.alloc}
	head, err := t.rdb.HMGet(ctx, key, "layout", "schema", "batches").Result()
	switch {
	case isWrongType(err):
		return r.list(ctx, stream) // stored before the HASH layout
	case err != nil:
		return toStatus(err)
	case head[1] == nil || head[2] == nil:
		return gone() // expired, dropped, or an upload that didn't finish
	case head[0] != layoutColumns:
		return status.Errorf(codes.FailedPrecondition,
			"dataset %q generation %d has layout %v, which this server can't read", dt.Dataset, gen, head[0])
	}
	n, err := strconv.ParseInt(head[2].(string), 10, 64)
	if err != nil || n < 0 {
		return status.Errorf(codes.DataLoss, "dataset %q generation %d has an invalid batch count %v", dt.Dataset, gen, head[2])
	}
	return r.columns(ctx, stream, head[1].(string), n)
}

// isWrongType reports whether err is Redis refusing a command for the
// key's type.
func isWrongType(err error) bool {
	var rerr goredis.Error
	return errors.As(err, &rerr) && strings.HasPrefix(rerr.Error(), "WRONGTYPE")
}

// datasetRead is a DoGet of a generation.
type datasetRead struct {
	rdb   goredis.UniversalClient
	key   string
	dt    datasetTicket
	gen   int64
	gone  func() error // the error for a generation no longer there
	alloc memory.Allocator
}

func (r *datasetRead) corrupt(err error) error {
	return status.Errorf(codes.DataLoss, "dataset %q generation %d is corrupt: %v", r.dt.Dataset, r.gen, err)
}

// columns reads a generation stored by column: the ticket's columns of its
// batches, about readPageBytes of them per HMGET. head is its schema.
func (r *datasetRead) columns(ctx context.Context, stream flight.FlightService_DoGetServer, head string, n int64) error {
	s, _, err := readIPC([]byte(head), r.alloc)
	if err != nil {
		return r.corrupt(err)
	}
	cols, schema, err := projection(s, r.dt.Columns)
	if err != nil {
		return err
	}
	if cols == nil {
		cols = make([]int, s.NumFields())
		for c := range cols {
			cols[c] = c
		}
	}
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(r.alloc))
	defer w.Close()
	// Fields per batch: its columns, or with none, its row count.
	per := max(len(cols), 1)
	// largest is the largest batch read so far, of these columns, which
	// sizes the next page. Until one has been read, pages are sized for
	// batches of maxChunkBytes (see list).
	var largest int64
	lo, hi := r.dt.batchRange(n)
	for start := lo; start < hi; {
		size := largest
		if size == 0 {
			size = maxChunkBytes
		}
		end := min(start+pageLen(size), hi)
		// The batch count last, which tells a generation that has expired
		// from one missing a column.
		fields := make([]string, 0, int(end-start)*per+1)
		for b := start; b < end; b++ {
			prefix := strconv.FormatInt(b, 10) + ":"
			if len(cols) == 0 {
				fields = append(fields, prefix+"rows")
			}
			for _, c := range cols {
				fields = append(fields, prefix+strconv.Itoa(c))
			}
		}
		vals, err := r.rdb.HMGet(ctx, r.key, append(fields, "batches")...).Result()
		if err != nil {
			return toStatus(err)
		}
		if vals[len(vals)-1] == nil {
			return r.gone() // expired while being read
		}
		strs := make([]string, len(fields))
		for i := range strs {
			var ok bool
			if strs[i], ok = vals[i].(string); !ok {
				return r.corrupt(fmt.Errorf("it has no field %s", fields[i]))
			}
		}
		for i := 0; i < len(strs); i += per {
			rec, size, err := assemble(schema, strs[i:i+per], r.alloc)
			if err != nil {
				return r.corrupt(err)
			}
			largest = max(largest, size)
			err = w.Write(rec)
			rec.Release()
			if err != nil {
				return err
			}
		}
		start = end
	}
	return nil
}

// list reads a generation stored as a LIST, before the HASH layout: an
// element per batch, after one with the schema alone, read whole and then
// projected.
func (r *datasetRead) list(ctx context.Context, stream flight.FlightService_DoGetServer) error {
	n, err := r.rdb.LLen(ctx, r.key).Result()
	if err != nil {
		return toStatus(err)
	}
	if n == 0 {
		return r.gone()
	}
	var (
		w      *flight.Writer
		cols   []int
		schema *arrow.Schema
	)
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	// send writes a list element: the schema first, then batches.
	send := func(v string) error {
		s, recs, err := readIPC([]byte(v), r.alloc)
		if err != nil {
			return r.corrupt(err)
		}
		defer releaseAll(recs)
		if w == nil {
			if cols, schema, err = projection(s, r.dt.Columns); err != nil {
				return err
			}
			w = flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(r.alloc))
		}
		for _, rec := range recs {
			if cols != nil {
				arrs := make([]arrow.Array, len(cols))
				for i, c := range cols {
					arrs[i] = rec.Column(c)
				}
				rec = array.NewRecordBatch(schema, arrs, rec.NumRows())
				defer rec.Release()
			}
			if err := w.Write(rec); err != nil {
				return err
			}
		}
		return nil
	}
	// largest is the largest batch read so far, which sizes the next page.
	// Until one has been read, pages are sized for batches of
	// maxChunkBytes, the most an upload stores in one unless a row is
	// larger. A batch much larger than those before it can make one page
	// go over readPageBytes.
	var largest int64
	// read sends the list elements [from, to).
	read := func(from, to int64) error {
		for start := from; start < to; {
			size := largest
			if size == 0 {
				size = maxChunkBytes
			}
			end := min(start+pageLen(size), to)
			vals, err := r.rdb.LRange(ctx, r.key, start, end-1).Result()
			if err != nil {
				return toStatus(err)
			}
			if int64(len(vals)) < end-start {
				return r.gone() // expired while being read
			}
			for i, v := range vals {
				if start+int64(i) > 0 { // element 0 is the schema alone
					largest = max(largest, int64(len(v)))
				}
				if err := send(v); err != nil {
					return err
				}
			}
			start = end
		}
		return nil
	}
	// Element 1+b is batch b.
	lo, hi := r.dt.batchRange(n - 1)
	if lo == 0 {
		return read(0, 1+hi) // the schema and the batches together
	}
	if err := read(0, 1); err != nil {
		return err
	}
	return read(1+lo, 1+hi)
}

// ---- listing and dropping ----

// ListFlights lists the datasets whose names start with the criteria's
// expression, if any.
func (d *datasets) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	ctx := stream.Context()
	t, done, err := d.tenants.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()
	prefix := string(criteria.GetExpression())
	pattern := escapeGlob(d.prefix+"ds:{"+url.PathEscape(prefix)) + "*}"

	var (
		mu   sync.Mutex
		keys []string
	)
	scan := func(ctx context.Context, c *goredis.Client) error {
		it := c.Scan(ctx, 0, pattern, 1000).Iterator()
		for it.Next(ctx) {
			mu.Lock()
			keys = append(keys, it.Val())
			mu.Unlock()
		}
		return it.Err()
	}
	switch c := t.rdb.(type) {
	case *goredis.ClusterClient:
		err = c.ForEachMaster(ctx, scan)
	case *goredis.Client:
		err = scan(ctx, c)
	default:
		err = fmt.Errorf("unsupported Redis client %T", t.rdb)
	}
	if err != nil {
		return toStatus(err)
	}
	slices.Sort(keys)

	for chunk := range slices.Chunk(keys, 100) {
		cmds := make([]*goredis.MapStringStringCmd, len(chunk))
		_, err := t.rdb.Pipelined(ctx, func(p goredis.Pipeliner) error {
			for i, k := range chunk {
				cmds[i] = p.HGetAll(ctx, k)
			}
			return nil
		})
		if err != nil {
			return toStatus(err)
		}
		for i, k := range chunk {
			name, ok := d.nameFromKey(k)
			if !ok || !strings.HasPrefix(name, prefix) {
				continue
			}
			m, ok := parseMeta(name, cmds[i].Val())
			if !ok {
				continue
			}
			if err := stream.Send(d.flightInfo(m)); err != nil {
				return err
			}
		}
	}
	return nil
}

func escapeGlob(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(`*?[]\`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Drop handles the drop_dataset action. Its body is the dataset's name, or
// {"dataset": "<name>"}.
func (d *datasets) Drop(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()
	name := string(action.Body)
	if strings.HasPrefix(strings.TrimSpace(name), "{") {
		var dt datasetTicket
		if err := json.Unmarshal(action.Body, &dt); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid %s body: %v", actionDropDataset, err)
		}
		name = dt.Dataset
	}
	if name == "" {
		return status.Errorf(codes.InvalidArgument, "%s needs the dataset's name", actionDropDataset)
	}
	t, done, err := d.tenants.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()
	keys := d.keys(name)
	dropped := false
	for attempt := 0; ; attempt++ {
		cur, err := t.rdb.HGet(ctx, keys.meta, "gen").Result()
		if errors.Is(err, goredis.Nil) {
			break
		}
		if err != nil {
			return toStatus(err)
		}
		gen, _ := strconv.ParseInt(cur, 10, 64)
		res, err := dropScript.Run(ctx, t.rdb, []string{keys.meta, keys.gen(gen)}, cur).Int()
		if err != nil {
			return toStatus(err)
		}
		if res == 1 {
			dropped = true
			break
		}
		if res == -1 {
			break
		}
		if attempt == 10 {
			return status.Errorf(codes.Aborted, "dataset %q kept changing; try again", name)
		}
	}
	body, _ := json.Marshal(map[string]any{"dataset": name, "dropped": dropped})
	return stream.Send(&flight.Result{Body: body})
}
