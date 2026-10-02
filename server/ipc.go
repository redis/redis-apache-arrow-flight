package server

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// maxChunkBytes is the most data the server puts in one record batch it
// sends. gRPC clients refuse messages over 4 MiB by default (Go, Java), and
// the driver returns up to 65,536 rows per batch, so larger batches are
// sliced.
const maxChunkBytes = 2 << 20

// writeIPC encodes schema and recs as one Arrow IPC stream.
func writeIPC(schema *arrow.Schema, recs []arrow.RecordBatch, mem memory.Allocator) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	for _, rec := range recs {
		if err := w.Write(rec); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// readIPC decodes an Arrow IPC stream. The caller releases the batches.
func readIPC(b []byte, mem memory.Allocator) (*arrow.Schema, []arrow.RecordBatch, error) {
	r, err := ipc.NewReader(bytes.NewReader(b), ipc.WithAllocator(mem))
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()
	var recs []arrow.RecordBatch
	for r.Next() {
		rec := r.RecordBatch()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := r.Err(); err != nil {
		releaseAll(recs)
		return nil, nil, err
	}
	return r.Schema(), recs, nil
}

func releaseAll(recs []arrow.RecordBatch) {
	for _, r := range recs {
		r.Release()
	}
}

// chunks slices rec into batches of about maxChunkBytes. Each returned
// batch is retained; the caller releases them.
func chunks(rec arrow.RecordBatch) []arrow.RecordBatch {
	rows := rec.NumRows()
	size := recordSize(rec)
	if rows <= 1 || size <= maxChunkBytes {
		rec.Retain()
		return []arrow.RecordBatch{rec}
	}
	per := max(rows*maxChunkBytes/size, 1)
	out := make([]arrow.RecordBatch, 0, (rows+per-1)/per)
	for start := int64(0); start < rows; start += per {
		out = append(out, rec.NewSlice(start, min(start+per, rows)))
	}
	return out
}

// recordSize is the size of rec's buffers. A sliced array counts its whole
// buffers, so slices are overestimated, which only makes chunks smaller.
func recordSize(rec arrow.RecordBatch) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += dataSize(col.Data())
	}
	return n
}

func dataSize(d arrow.ArrayData) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += dataSize(c)
	}
	// Dictionary() is a nil *Data, not a nil interface, on other types.
	if d.DataType().ID() == arrow.DICTIONARY {
		n += dataSize(d.Dictionary())
	}
	return n
}

// streamRecords sends rdr's batches, sliced by chunks, on a channel for
// the Flight SQL DoGet handlers, and releases rdr when done.
func streamRecords(ctx context.Context, rdr array.RecordReader) <-chan flight.StreamChunk {
	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		defer rdr.Release()
		send := func(c flight.StreamChunk) bool {
			select {
			case ch <- c:
				return true
			case <-ctx.Done():
				if c.Data != nil {
					c.Data.Release()
				}
				return false
			}
		}
		for rdr.Next() {
			parts := chunks(rdr.RecordBatch())
			for i, part := range parts {
				if !send(flight.StreamChunk{Data: part}) {
					releaseAll(parts[i+1:])
					return
				}
			}
		}
		if err := rdr.Err(); err != nil {
			send(flight.StreamChunk{Err: toStatus(err)})
		}
	}()
	return ch
}

// readAll drains rdr into memory, keeping batches that have rows. The
// caller releases them.
func readAll(rdr array.RecordReader) ([]arrow.RecordBatch, int64, error) {
	var (
		recs []arrow.RecordBatch
		rows int64
	)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if rec.NumRows() == 0 {
			continue
		}
		rec.Retain()
		recs = append(recs, rec)
		rows += rec.NumRows()
	}
	if err := rdr.Err(); err != nil {
		releaseAll(recs)
		return nil, 0, fmt.Errorf("failed to read record batches: %w", err)
	}
	return recs, rows, nil
}
