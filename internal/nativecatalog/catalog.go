// Package nativecatalog reads committed Redis ADBC protocol-17 metadata.
// This is read-only inspection, never authority to attach to a snapshot.
// Protocol layout follows Redis ADBC snapshot_metadata_cow.go/store.go
// (Apache-2.0, ADBC Drivers Contributors).
package nativecatalog

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

const metadataScript = `
local catalog=KEYS[1]
if redis.call('HGET',catalog,'format')~='17' then return redis.error_reply('native catalog inspection requires storage protocol 17') end
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

func KeyPart(s string) string {
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

// Members resolves a committed metadata set under the same atomic root lookup.
func Members(ctx context.Context, rdb goredis.UniversalClient, suffix string) *goredis.Cmd {
	return read(ctx, rdb, suffix, "SMEMBERS")
}

// Table is the physical index inventory of one committed managed table.
type Table struct {
	Schema   string `json:"schema"`
	Name     string `json:"name"`
	Index    string `json:"index_name"`
	Segments []struct {
		Index string `json:"index"`
	} `json:"snapshot_segments"`
}

func ParseTable(raw, schema, name string) (Table, error) {
	var table Table
	if err := json.Unmarshal([]byte(raw), &table); err != nil {
		return table, err
	}
	if table.Schema != schema || table.Name != name || table.Index == "" {
		return table, errors.New("invalid native table descriptor identity or index")
	}
	for _, segment := range table.Segments {
		if segment.Index == "" {
			return table, errors.New("native segment has no index")
		}
	}
	return table, nil
}

// ReadTable keeps root, metadata bucket and descriptor ownership consistent.
// Missing tables return redis.Nil; unknown formats and broken metadata fail.
func ReadTable(ctx context.Context, rdb goredis.UniversalClient, schema, name string) (Table, error) {
	raw, err := read(ctx, rdb, "table:"+KeyPart(schema)+":"+KeyPart(name), "GET").Text()
	if err != nil {
		return Table{}, err
	}
	return ParseTable(raw, schema, name)
}

func (table Table) Indexes() []string {
	indexes := []string{table.Index}
	for _, segment := range table.Segments {
		indexes = append(indexes, segment.Index)
	}
	slices.Sort(indexes)
	return slices.Compact(indexes)
}

func read(ctx context.Context, rdb goredis.UniversalClient, suffix, command string) *goredis.Cmd {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(suffix))
	slot := fmt.Sprintf("%02x", hash.Sum32()&255)
	return rdb.Eval(ctx, metadataScript, []string{"adbc:{meta}:snapshot:catalog"}, slot, suffix, command)
}
