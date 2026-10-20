package server

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

type handoffKey struct{}
type stalledKey struct{}

// recordHandoffs owns batches handed to the Flight SQL adapter until it releases
// them. Its DoGet defer also covers a transport write error: the adapter drains
// subsequent batches but does not release the batch whose write failed.
type recordHandoffs struct {
	mu      sync.Mutex
	records map[*handoffRecord]struct{}
}

func (h *recordHandoffs) wrap(rec arrow.RecordBatch) *handoffRecord {
	r := &handoffRecord{RecordBatch: rec, refs: 1, released: make(chan struct{}), owner: h}
	if h != nil {
		h.mu.Lock()
		if h.records == nil {
			h.records = make(map[*handoffRecord]struct{})
		}
		h.records[r] = struct{}{}
		h.mu.Unlock()
	}
	return r
}

func (h *recordHandoffs) releaseAll() {
	h.mu.Lock()
	pending := make([]*handoffRecord, 0, len(h.records))
	for r := range h.records {
		pending = append(pending, r)
	}
	h.mu.Unlock()
	// The adapter and its writer have returned; no serializer can still use these.
	for _, r := range pending {
		r.release(true)
	}
}

type handoffRecord struct {
	arrow.RecordBatch
	mu       sync.Mutex
	refs     int
	released chan struct{}
	owner    *recordHandoffs
}

func (r *handoffRecord) Retain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		panic("retain after batch release")
	}
	r.RecordBatch.Retain()
	r.refs++
}
func (r *handoffRecord) Release() { r.release(false) }
func (r *handoffRecord) release(all bool) {
	r.mu.Lock()
	if r.refs == 0 {
		r.mu.Unlock()
		return
	}
	n := 1
	if all {
		n = r.refs
	}
	for range n {
		r.RecordBatch.Release()
	}
	r.refs -= n
	finished := r.refs == 0
	if finished {
		close(r.released)
	}
	r.mu.Unlock()
	if finished && r.owner != nil {
		r.owner.mu.Lock()
		delete(r.owner.records, r)
		r.owner.mu.Unlock()
	}
}

func reportStall(ctx context.Context, fallback func()) {
	if report, ok := ctx.Value(stalledKey{}).(func()); ok {
		report()
	} else if fallback != nil {
		fallback()
	}
}
