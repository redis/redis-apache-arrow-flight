package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// Benchmarks of dataset uploads and reads through the server, against the
// Redis at REDIS_URI (CI doesn't run them):
//
//	REDIS_URI=redis://localhost:6379/0 go test ./server -run '^$' -bench Dataset -benchtime 10x
//
// Besides time and the client's allocations, each reports Redis's cost per
// operation from INFO (bytes it sent and received, CPU time, commands), and
// the most Arrow memory the server held at once.

// benchDatasets are 100,000 rows of 10 columns: num10 all numbers (5 int64,
// 5 float64), mixed10 4 int64, 2 float64, 3 strings and a dictionary.
var benchDatasets = sync.OnceValue(func() map[string]arrow.RecordBatch {
	const rows = 100_000
	num := []arrow.Field{}
	for i := range 5 {
		num = append(num, arrow.Field{Name: fmt.Sprintf("i%d", i), Type: arrow.PrimitiveTypes.Int64})
	}
	for i := range 5 {
		num = append(num, arrow.Field{Name: fmt.Sprintf("f%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	nb := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(num, nil))
	defer nb.Release()
	for r := range rows {
		for i := range 5 {
			nb.Field(i).(*array.Int64Builder).Append(int64(r*7 + i))
			nb.Field(5 + i).(*array.Float64Builder).Append(float64(r) * 1.5 * float64(i+1))
		}
	}

	mixed := arrow.NewSchema([]arrow.Field{
		{Name: "i0", Type: arrow.PrimitiveTypes.Int64}, {Name: "i1", Type: arrow.PrimitiveTypes.Int64},
		{Name: "i2", Type: arrow.PrimitiveTypes.Int64}, {Name: "i3", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f0", Type: arrow.PrimitiveTypes.Float64}, {Name: "f1", Type: arrow.PrimitiveTypes.Float64},
		{Name: "s0", Type: arrow.BinaryTypes.String}, {Name: "s1", Type: arrow.BinaryTypes.String},
		{Name: "s2", Type: arrow.BinaryTypes.String},
		{Name: "kind", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
	}, nil)
	mb := array.NewRecordBuilder(memory.DefaultAllocator, mixed)
	defer mb.Release()
	kinds := []string{"click", "view", "purchase", "scroll"}
	for r := range rows {
		for i := range 4 {
			mb.Field(i).(*array.Int64Builder).Append(int64(r*4 + i))
		}
		mb.Field(4).(*array.Float64Builder).Append(float64(r) / 3)
		mb.Field(5).(*array.Float64Builder).Append(float64(r) * 2.5)
		mb.Field(6).(*array.StringBuilder).Append(fmt.Sprintf("user-%010d@example.com/%d", r, r%97))
		mb.Field(7).(*array.StringBuilder).Append(fmt.Sprintf("sess-%08x", r))
		mb.Field(8).(*array.StringBuilder).Append(fmt.Sprintf("page/%d/%d", r%1000, r))
		_ = mb.Field(9).(*array.BinaryDictionaryBuilder).AppendString(kinds[r%4])
	}
	return map[string]arrow.RecordBatch{"num10": nb.NewRecordBatch(), "mixed10": mb.NewRecordBatch()}
})

// peakAllocator tracks the most memory allocated through it at once.
type peakAllocator struct {
	memory.Allocator
	cur, peak atomic.Int64
}

func (p *peakAllocator) add(n int64) {
	c := p.cur.Add(n)
	for pk := p.peak.Load(); c > pk && !p.peak.CompareAndSwap(pk, c); pk = p.peak.Load() {
	}
}

func (p *peakAllocator) Allocate(n int) []byte {
	p.add(int64(n))
	return p.Allocator.Allocate(n)
}

func (p *peakAllocator) Reallocate(n int, b []byte) []byte {
	p.add(int64(n - len(b)))
	return p.Allocator.Reallocate(n, b)
}

func (p *peakAllocator) Free(b []byte) {
	p.add(-int64(len(b)))
	p.Allocator.Free(b)
}

// redisCost is what Redis reports having done, summed over its masters.
type redisCost struct {
	out, in, cmds int64
	cpu           float64 // seconds
}

func readRedisCost(b testing.TB, admin goredis.UniversalClient) redisCost {
	b.Helper()
	var (
		mu  sync.Mutex
		sum redisCost
	)
	one := func(ctx context.Context, c *goredis.Client) error {
		info, err := c.Info(ctx, "stats", "cpu").Result()
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, line := range strings.Split(info, "\r\n") {
			k, v, _ := strings.Cut(line, ":")
			n, _ := strconv.ParseInt(v, 10, 64)
			switch k {
			case "total_net_output_bytes":
				sum.out += n
			case "total_net_input_bytes":
				sum.in += n
			case "total_commands_processed":
				sum.cmds += n
			case "used_cpu_user", "used_cpu_sys":
				f, _ := strconv.ParseFloat(v, 64)
				sum.cpu += f
			}
		}
		return nil
	}
	ctx := context.Background()
	var err error
	if cc, ok := admin.(*goredis.ClusterClient); ok {
		err = cc.ForEachMaster(ctx, one)
	} else {
		err = one(ctx, admin.(*goredis.Client))
	}
	if err != nil {
		b.Fatal(err)
	}
	return sum
}

func reportRedisCost(b *testing.B, before, after redisCost, mem *peakAllocator) {
	n := float64(b.N)
	b.ReportMetric(float64(after.out-before.out)/n, "redis-out-B/op")
	b.ReportMetric(float64(after.in-before.in)/n, "redis-in-B/op")
	b.ReportMetric((after.cpu-before.cpu)*1e6/n, "redis-cpu-µs/op")
	b.ReportMetric(float64(after.cmds-before.cmds)/n, "redis-cmds/op")
	b.ReportMetric(float64(mem.peak.Load()), "peak-arrow-B")
}

// drainDataset reads a ticket's stream, and returns its rows.
func drainDataset(b testing.TB, ctx context.Context, fc flight.Client, tkt []byte) int64 {
	b.Helper()
	stream, err := fc.DoGet(ctx, &flight.Ticket{Ticket: tkt})
	if err != nil {
		b.Fatal(err)
	}
	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		b.Fatal(err)
	}
	defer rdr.Release()
	var n int64
	for rdr.Next() {
		n += rdr.RecordBatch().NumRows()
	}
	if err := rdr.Err(); err != nil {
		b.Fatal(err)
	}
	return n
}

func benchDatasetServer(b *testing.B) (flight.Client, goredis.UniversalClient, *peakAllocator, string) {
	uri := redisURI(b)
	prefix := uniqueName("it_flight_bench") + ":"
	mem := &peakAllocator{Allocator: memory.DefaultAllocator}
	cfg := Config{RedisURI: uri, KeyPrefix: prefix, Allocator: mem}
	fc := newClients(b, cfg).fl
	admin := adminClient(b, uri)
	b.Cleanup(func() { deleteKeys(admin, keysMatching(b, admin, prefix+"*")) })
	return fc, admin, mem, prefix
}

func BenchmarkDatasetWrite(b *testing.B) {
	fc, admin, mem, _ := benchDatasetServer(b)
	ctx := context.Background()
	for _, name := range []string{"num10", "mixed10"} {
		rec := benchDatasets()[name]
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"bench", name}}
			mem.peak.Store(mem.cur.Load())
			before := readRedisCost(b, admin)
			for range b.N {
				if _, err := tryPutDataset(ctx, fc, desc, rec.Schema(), rec); err != nil {
					b.Fatal(err)
				}
			}
			reportRedisCost(b, before, readRedisCost(b, admin), mem)
		})
	}
}

func BenchmarkDatasetRead(b *testing.B) {
	fc, admin, mem, _ := benchDatasetServer(b)
	ctx := context.Background()
	for _, name := range []string{"num10", "mixed10"} {
		rec := benchDatasets()[name]
		desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"bench", name}}
		if _, err := tryPutDataset(ctx, fc, desc, rec.Schema(), rec); err != nil {
			b.Fatal(err)
		}
		for _, read := range []struct {
			name    string
			columns []string
		}{{"all", nil}, {"1col", []string{"i0"}}} {
			tkt, _ := json.Marshal(datasetTicket{Dataset: "bench/" + name, Columns: read.columns})
			b.Run(name+"/"+read.name, func(b *testing.B) {
				b.ReportAllocs()
				mem.peak.Store(mem.cur.Load())
				before := readRedisCost(b, admin)
				for range b.N {
					if n := drainDataset(b, ctx, fc, tkt); n != rec.NumRows() {
						b.Fatalf("read %d rows, want %d", n, rec.NumRows())
					}
				}
				reportRedisCost(b, before, readRedisCost(b, admin), mem)
			})
		}
	}
}
