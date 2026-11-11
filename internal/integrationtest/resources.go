package integrationtest

// Protocol layout: Redis ADBC v0.0.24 snapshot_metadata_cow.go and store.go
// (Apache-2.0, ADBC Drivers Contributors).
// Read-only inspection of the pinned native driver's storage protocol, for
// backend resource assertions only. Never use diagnostic metadata to attach a
// client to a snapshot. Unknown formats and broken descriptors fail explicitly.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

const tableDescriptorScript = `
local catalog=KEYS[1]
if redis.call('HGET',catalog,'format')~='17' then return redis.error_reply('test inspector requires native storage protocol 17') end
local root=redis.call('HGET',catalog,'root')
if not root then return redis.error_reply('missing native catalog root') end
local prefix='adbc:{meta}:snapshot:17:'
local bucket=redis.call('HGET',prefix..'metadata:root:'..root,ARGV[1])
if not bucket then return false end
local owner=redis.call('HGET',prefix..'metadata:bucket:'..bucket..':'..ARGV[1],ARGV[2])
if not owner then return false end
local key=prefix..'workspace:'..owner..':'..ARGV[2]
if ARGV[3]=='SMEMBERS' then return redis.call('SMEMBERS',key) end
local raw=redis.call('GET',key)
if not raw then return redis.error_reply('native table descriptor missing') end
return raw`

func keyPart(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if strings.ContainsRune("%:{}*?[]\\", rune(c)) || c <= ' ' || c == 0x7f {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// TableIndexes resolves all physical indexes referenced by a current table,
// including its immutable segments. pg_temp searches registered session schemas;
// fixture names must be unique. An absent logical table returns no indexes;
// malformed metadata, unavailable authority and protocol mismatches are errors.
func TableIndexes(ctx context.Context, rdb goredis.UniversalClient, schema, table string) ([]string, error) {
	schemas := []string{schema}
	if schema == "pg_temp" {
		var err error
		schemas, err = nativeMetadata(ctx, rdb, "snapshot_temp_schemas", "SMEMBERS").StringSlice()
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
		suffix := "table:" + keyPart(physicalSchema) + ":" + keyPart(table)
		raw, err := nativeMetadata(ctx, rdb, suffix, "GET").Text()
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		resolved, err := descriptorIndexes(raw, physicalSchema, table)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, resolved...)
	}
	slices.Sort(indexes)
	return slices.Compact(indexes), nil
}

func descriptorIndexes(raw, schema, table string) ([]string, error) {
	var meta struct {
		Schema   string `json:"schema"`
		Name     string `json:"name"`
		Index    string `json:"index_name"`
		Segments []struct {
			Index string `json:"index"`
		} `json:"snapshot_segments"`
	}
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return nil, err
	}
	if meta.Schema != schema || meta.Name != table || meta.Index == "" {
		return nil, errors.New("invalid native table descriptor identity or index")
	}
	indexes := []string{meta.Index}
	for _, seg := range meta.Segments {
		if seg.Index == "" {
			return nil, errors.New("native segment has no index")
		}
		indexes = append(indexes, seg.Index)
	}
	return indexes, nil
}

func nativeMetadata(ctx context.Context, rdb goredis.UniversalClient, suffix, command string) *goredis.Cmd {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(suffix))
	slot := fmt.Sprintf("%02x", hash.Sum32()&255)
	return rdb.Eval(ctx, tableDescriptorScript, []string{"adbc:{meta}:snapshot:catalog"}, slot, suffix, command)
}
