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
//	<prefix>ds:{<name>}:g<N>   LIST  generation N: an IPC stream with just the
//	                                 schema, then one IPC stream per batch
//
// (<name> is URL-path-escaped, so braces can't end the hash tag.) An upload
// writes a new generation and then switches the HASH to it atomically, so
// readers never see a partial upload. The replaced generation expires after
// a grace period, letting reads already under way finish. A ticket names
// the generation its FlightInfo described.

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
	readPage      = 64
	flushValues   = 64
	flushBytes    = 8 << 20
)

type datasets struct {
	tenants *tenantCache
	prefix  string
	ttl     time.Duration
	alloc   memory.Allocator
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

func (d *datasets) flightInfo(m datasetMeta) *flight.FlightInfo {
	tkt, _ := json.Marshal(datasetTicket{Dataset: m.Name, Generation: m.Generation})
	appMeta, _ := json.Marshal(m)
	return &flight.FlightInfo{
		Schema:           m.schema,
		FlightDescriptor: &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: strings.Split(m.Name, "/")},
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		TotalRecords:     m.Rows,
		TotalBytes:       m.Bytes,
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

// listWriter appends values to a LIST in pipelined batches, keeping the
// upload's expiry on the key.
type listWriter struct {
	rdb     goredis.UniversalClient
	key     string
	pending []any
	size    int
}

func (w *listWriter) add(ctx context.Context, v []byte) error {
	w.pending = append(w.pending, v)
	w.size += len(v)
	if len(w.pending) >= flushValues || w.size >= flushBytes {
		return w.flush(ctx)
	}
	return nil
}

func (w *listWriter) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	_, err := w.rdb.Pipelined(ctx, func(p goredis.Pipeliner) error {
		p.RPush(ctx, w.key, w.pending...)
		p.PExpire(ctx, w.key, uploadTTL)
		return nil
	})
	w.pending, w.size = w.pending[:0], 0
	return err
}

// commitScript switches a dataset to a new generation if its current one
// is still the expected one (ARGV[1], "" for none), and returns 1. It
// returns 0 if the current one changed meanwhile (read it and try again),
// and -1 if the current one is newer: an upload that started later
// finished first, so this one is replaced at once, and its list expires
// like a replaced generation. Generations only go up, and the seq key
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
	w := &listWriter{rdb: t.rdb, key: keys.gen(gen)}
	abort := func(err error) error {
		_ = t.rdb.Del(context.WithoutCancel(ctx), w.key).Err()
		return err
	}

	head, err := writeIPC(schema, nil, d.alloc)
	if err != nil {
		return abort(status.Errorf(codes.Internal, "failed to encode the schema: %v", err))
	}
	if err := w.add(ctx, head); err != nil {
		return abort(toStatus(err))
	}
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if rec.NumRows() == 0 {
			continue
		}
		parts := chunks(rec)
		for i, part := range parts {
			b, err := writeIPC(schema, []arrow.RecordBatch{part}, d.alloc)
			m.Rows += part.NumRows()
			part.Release()
			if err == nil {
				err = w.add(ctx, b)
			}
			if err != nil {
				releaseAll(parts[i+1:])
				return abort(toStatus(err))
			}
			m.Batches++
			m.Bytes += int64(len(b))
		}
	}
	if err := rdr.Err(); err != nil {
		return abort(status.Errorf(codes.InvalidArgument, "failed to read the upload: %v", err))
	}
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
	if err := json.Unmarshal(tkt.Ticket, &dt); err != nil || dt.Dataset == "" {
		return status.Error(codes.InvalidArgument, `invalid dataset ticket: want {"dataset": "<name>"}`)
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
	n, err := t.rdb.LLen(ctx, key).Result()
	if err != nil {
		return toStatus(err)
	}
	gone := func() error {
		if m, err := d.readMeta(ctx, t.rdb, dt.Dataset); err != nil {
			return err
		} else if m.Generation != gen {
			return status.Errorf(codes.Aborted,
				"dataset %q was replaced since its FlightInfo was read; get it again", dt.Dataset)
		}
		return status.Errorf(codes.NotFound, "dataset %q generation %d not found", dt.Dataset, gen)
	}
	if n == 0 {
		return gone()
	}

	var w *flight.Writer
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	for start := int64(0); start < n; start += readPage {
		vals, err := t.rdb.LRange(ctx, key, start, min(start+readPage, n)-1).Result()
		if err != nil {
			return toStatus(err)
		}
		if int64(len(vals)) < min(readPage, n-start) {
			return gone() // expired while being read
		}
		for i, v := range vals {
			schema, recs, err := readIPC([]byte(v), d.alloc)
			if err != nil {
				return status.Errorf(codes.DataLoss, "dataset %q has a corrupt batch: %v", dt.Dataset, err)
			}
			if start == 0 && i == 0 {
				w = flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(d.alloc))
			}
			for j, rec := range recs {
				if err := w.Write(rec); err != nil {
					releaseAll(recs[j:])
					return err
				}
				rec.Release()
			}
		}
	}
	return nil
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
