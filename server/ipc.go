package server

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
func recordSize(rec arrow.RecordBatch) int64 { return recordBytes(rec, (*memory.Buffer).Len) }

// recordMemory is the memory rec's buffers hold, which can be more than
// their size: builders grow buffers ahead of the data.
func recordMemory(rec arrow.RecordBatch) int64 { return recordBytes(rec, (*memory.Buffer).Cap) }

func recordBytes(rec arrow.RecordBatch, size func(*memory.Buffer) int) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += dataBytes(col.Data(), size)
	}
	return n
}

func dataBytes(d arrow.ArrayData, size func(*memory.Buffer) int) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(size(b))
		}
	}
	for _, c := range d.Children() {
		n += dataBytes(c, size)
	}
	// Dictionary() is a nil *Data, not a nil interface, on other types.
	if d.DataType().ID() == arrow.DICTIONARY {
		n += dataBytes(d.Dictionary(), size)
	}
	return n
}

// liveResult is a result that holds something until it has been sent: a
// connection and a cursor, for one streaming from Redis as it is read, or
// a share of ResultMemory, for one read whole for want of a stream (see
// executeQuery).
type liveResult struct {
	// stall is how long the client may take to read a batch, and stalled,
	// if set, is called when one takes longer.
	stall   time.Duration
	stalled func()
	// done is called once the result is released, with its error: nil if
	// it was read to the end, or the client went away or stalled.
	done func(error)
}

// streamRecords sends rdr's batches, sliced by chunks, on a channel for
// the Flight SQL DoGet handlers, and releases rdr when done. For a live
// result, a client that takes longer than live.stall to read a batch gets
// an error instead of the rest, and the result is released at once.
func streamRecords(ctx context.Context, rdr array.RecordReader, live *liveResult) <-chan flight.StreamChunk {
	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		var resultErr error
		released := false
		release := func() {
			if !released {
				released = true
				rdr.Release()
				if live != nil {
					live.done(resultErr)
				}
			}
		}
		defer release()
		var stall *time.Timer
		var stalled <-chan time.Time // nil (never) unless live
		if live != nil {
			stall = time.NewTimer(live.stall)
			defer stall.Stop()
			stalled = stall.C
		}
		// send sends c, unless the client goes away or stalls.
		send := func(c flight.StreamChunk) (ok, stuck bool) {
			select {
			case ch <- c:
				return true, false
			case <-ctx.Done():
			case <-stalled:
				stuck = true
			}
			if c.Data != nil {
				c.Data.Release()
			}
			return false, stuck
		}
		// sendErr sends an error, however long the client takes.
		sendErr := func(err error) {
			select {
			case ch <- flight.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
		}
		for rdr.Next() {
			if stall != nil {
				stall.Reset(live.stall)
			}
			parts := chunks(rdr.RecordBatch())
			for i, part := range parts {
				if ok, stuck := send(flight.StreamChunk{Data: part}); !ok {
					releaseAll(parts[i+1:])
					if stuck {
						if live.stalled != nil {
							live.stalled()
						}
						// Free what the result holds now, and tell the
						// client, if it comes back, that rows are missing.
						release()
						sendErr(status.Errorf(codes.DeadlineExceeded, "the client took longer than %s to read a "+
							"batch of the result, so the server stopped sending it (--stream-stall-timeout)", live.stall))
					}
					return
				}
			}
		}
		if err := rdr.Err(); err != nil {
			if ctx.Err() == nil {
				resultErr = err // not the client going away
			}
			release()
			sendErr(toStatus(err))
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
