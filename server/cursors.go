package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	// cursorCheckInterval is how long the server goes by an index's count
	// of open cursors before it reads the count again.
	cursorCheckInterval = time.Second
	// maxCursorCounts is the number of tables whose counts are kept before
	// those not read for a minute are dropped.
	maxCursorCounts = 1000
)

// indexCursors stops results from streaming from a table once its index
// has three quarters of its cursors open, counting every client: other
// servers, other tools, and the results read whole, each of which holds a
// cursor while it is read. RediSearch refuses every query on an index once
// it has search-index-cursor-limit of them open (128 by default), so the
// last quarter is left for short reads.
//
// The count comes from FT.INFO, read at most once per cursorCheckInterval
// per table, plus the streams allowed since. A stream that has ended counts
// until the next read. A cluster's FT.INFO doesn't count cursors, so there,
// and wherever FT.INFO fails, only MaxTableStreams applies.
type indexCursors struct {
	now   func() time.Time
	fetch func(ctx context.Context, rdb goredis.UniversalClient, src streamSource) (open, capacity int64, err error)

	mu     sync.Mutex
	counts map[streamSource]*cursorCount
}

// cursorCount is what indexCursors knows of one table's index.
type cursorCount struct {
	mu       sync.Mutex
	at       time.Time // when open and capacity were read
	known    bool      // whether FT.INFO reported them
	open     int64
	capacity int64
	started  int64 // the streams allowed since
}

func newIndexCursors() *indexCursors {
	return &indexCursors{now: time.Now, fetch: fetchCursors, counts: map[streamSource]*cursorCount{}}
}

// allow reports whether a result may stream from src, and counts it if so.
// Requests for one table wait for each other's FT.INFO rather than each
// sending one.
func (ic *indexCursors) allow(ctx context.Context, t *tenant, src streamSource) bool {
	if _, ok := t.rdb.(*goredis.ClusterClient); ok {
		return true
	}
	c := ic.count(src)
	c.mu.Lock()
	defer c.mu.Unlock()
	if now := ic.now(); now.Sub(c.at) >= cursorCheckInterval {
		open, capacity, err := ic.fetch(ctx, t.rdb, src)
		c.at, c.known, c.open, c.capacity, c.started = now, err == nil && capacity > 0, open, capacity, 0
	}
	if !c.known {
		return true
	}
	if c.open+c.started >= c.capacity-c.capacity/4 {
		return false
	}
	c.started++
	return true
}

// count returns src's count, first dropping the counts not read for a
// minute if there are maxCursorCounts.
func (ic *indexCursors) count(src streamSource) *cursorCount {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if c, ok := ic.counts[src]; ok {
		return c
	}
	if len(ic.counts) >= maxCursorCounts {
		stale := ic.now().Add(-time.Minute)
		for k, c := range ic.counts {
			if c.mu.TryLock() {
				if c.at.Before(stale) {
					delete(ic.counts, k)
				}
				c.mu.Unlock()
			}
		}
	}
	c := &cursorCount{}
	ic.counts[src] = c
	return c
}

// fetchCursors reads how many cursors src's index has open, and how many it
// allows, from FT.INFO.
func fetchCursors(ctx context.Context, rdb goredis.UniversalClient, src streamSource) (open, capacity int64, err error) {
	index, err := tableIndex(ctx, rdb, src)
	if err != nil {
		return 0, 0, err
	}
	reply, err := rdb.Do(ctx, "FT.INFO", index).Result()
	if err != nil {
		return 0, 0, err
	}
	stats, ok := replyField(reply, "cursor_stats")
	if !ok {
		return 0, 0, fmt.Errorf("FT.INFO %s has no cursor_stats", index)
	}
	open, ok1 := replyInt(stats, "index_total")
	capacity, ok2 := replyInt(stats, "index_capacity")
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("FT.INFO %s has no cursor counts", index)
	}
	return open, capacity, nil
}

// tableIndex returns the name of src's search index, from the table's
// metadata as the Redis ADBC driver keeps it (its store.go: metaKey, and
// tableMeta.IndexName, which adopted and rekeyed tables set). Other
// tables' indexes are named after them (indexName).
func tableIndex(ctx context.Context, rdb goredis.UniversalClient, src streamSource) (string, error) {
	suffix := escapeKeyPart(src.schema) + ":" + escapeKeyPart(src.name)
	raw, err := rdb.Get(ctx, "adbc:{meta}:table:"+suffix).Result()
	if errors.Is(err, goredis.Nil) {
		return "", fmt.Errorf("table %s.%s has no metadata", src.schema, src.name)
	}
	if err != nil {
		return "", err
	}
	var meta struct {
		IndexName string `json:"index_name"`
	}
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return "", err
	}
	if meta.IndexName != "" {
		return meta.IndexName, nil
	}
	return "idx:" + suffix, nil
}

// escapeKeyPart escapes a schema or table name for a key, as the driver
// does (store.go).
func escapeKeyPart(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' || c == ':' || c == '{' || c == '}' || c == '*' || c == '?' || c == '[' || c == ']' || c == '\\' || c <= ' ' || c == 0x7f {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// replyField reads a field of a RESP2 (flat list) or RESP3 (map) reply.
func replyField(reply any, name string) (any, bool) {
	switch r := reply.(type) {
	case map[any]any:
		v, ok := r[name]
		return v, ok
	case []any:
		for i := 0; i+1 < len(r); i += 2 {
			if r[i] == name {
				return r[i+1], true
			}
		}
	}
	return nil, false
}

// replyInt reads a number field of a reply.
func replyInt(reply any, name string) (int64, bool) {
	v, ok := replyField(reply, name)
	if !ok {
		return 0, false
	}
	switch v := v.(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	return 0, false
}
