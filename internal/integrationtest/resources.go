package integrationtest

// Protocol layout: Redis ADBC v0.0.24 snapshot_metadata_cow.go and store.go
// (Apache-2.0, ADBC Drivers Contributors).
// Read-only inspection of the pinned native driver's storage protocol, for
// backend resource assertions only. Never use diagnostic metadata to attach a
// client to a snapshot. Unknown formats and broken descriptors fail explicitly.
import (
	"context"
	"errors"
	"slices"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/redis-apache-arrow-flight/internal/nativecatalog"
)

// TableIndexes resolves all physical indexes referenced by a current table,
// including its immutable segments. pg_temp searches registered session schemas;
// fixture names must be unique. An absent logical table returns no indexes;
// malformed metadata, unavailable authority and protocol mismatches are errors.
func TableIndexes(ctx context.Context, rdb goredis.UniversalClient, schema, table string) ([]string, error) {
	schemas := []string{schema}
	if schema == "pg_temp" {
		var err error
		schemas, err = nativecatalog.Members(ctx, rdb, "snapshot_temp_schemas").StringSlice()
		if errors.Is(err, goredis.Nil) {
			err = nil
		}
		if err != nil {
			return nil, err
		}
	}
	var indexes []string
	for _, physicalSchema := range schemas {
		if schema == "pg_temp" && !strings.HasPrefix(physicalSchema, "pg_temp_") {
			return nil, errors.New("invalid native temporary schema")
		}
		descriptor, err := nativecatalog.ReadTable(ctx, rdb, physicalSchema, table)
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		resolved := descriptor.Indexes()
		indexes = append(indexes, resolved...)
	}
	slices.Sort(indexes)
	return slices.Compact(indexes), nil
}
