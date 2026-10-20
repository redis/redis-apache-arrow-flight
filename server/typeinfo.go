package server

import (
	"context"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
)

// XDBC describes canonical, creatable scalar types, not every SQL alias.
// Unknown/unbounded sizes and scales stay NULL rather than claiming a limit.
// Keep this list aligned with the pinned driver's SQL types; the integration
// test creates a table with every reported type to detect dependency drift.
type sqlTypeInfo struct {
	name         string
	code         pb.XdbcDataType
	size, radix  int32
	text, quoted bool
	params       []string
}

var sqlTypes = []sqlTypeInfo{
	{name: "BOOLEAN", code: pb.XdbcDataType_XDBC_BIT},
	{name: "SMALLINT", code: pb.XdbcDataType_XDBC_SMALLINT, size: 5, radix: 10},
	{name: "INTEGER", code: pb.XdbcDataType_XDBC_INTEGER, size: 10, radix: 10},
	{name: "BIGINT", code: pb.XdbcDataType_XDBC_BIGINT, size: 19, radix: 10},
	{name: "REAL", code: pb.XdbcDataType_XDBC_REAL, size: 7, radix: 2},
	{name: "DOUBLE PRECISION", code: pb.XdbcDataType_XDBC_DOUBLE, size: 15, radix: 2},
	{name: "NUMERIC", code: pb.XdbcDataType_XDBC_NUMERIC, size: 76, radix: 10, params: []string{"precision", "scale"}},
	{name: "DECIMAL", code: pb.XdbcDataType_XDBC_DECIMAL, size: 76, radix: 10, params: []string{"precision", "scale"}},
	{name: "CHAR", code: pb.XdbcDataType_XDBC_CHAR, text: true, quoted: true, params: []string{"length"}},
	{name: "VARCHAR", code: pb.XdbcDataType_XDBC_VARCHAR, text: true, quoted: true, params: []string{"length"}},
	{name: "TEXT", code: pb.XdbcDataType_XDBC_LONGVARCHAR, text: true, quoted: true},
	{name: "BYTEA", code: pb.XdbcDataType_XDBC_VARBINARY},
	{name: "DATE", code: pb.XdbcDataType_XDBC_DATE, quoted: true},
	{name: "TIME", code: pb.XdbcDataType_XDBC_TIME, quoted: true, params: []string{"precision"}},
	{name: "TIME WITH TIME ZONE", code: pb.XdbcDataType_XDBC_TIME, quoted: true, params: []string{"precision"}},
	{name: "TIMESTAMP", code: pb.XdbcDataType_XDBC_TIMESTAMP, quoted: true, params: []string{"precision"}},
	{name: "TIMESTAMP WITH TIME ZONE", code: pb.XdbcDataType_XDBC_TIMESTAMP, quoted: true, params: []string{"precision"}},
	{name: "INTERVAL", code: pb.XdbcDataType_XDBC_INTERVAL, quoted: true},
}

func (s *sqlServer) GetFlightInfoXdbcTypeInfo(_ context.Context, _ flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.XdbcTypeInfo), nil
}

func (s *sqlServer) DoGetXdbcTypeInfo(_ context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	types := slices.Clone(sqlTypes)
	slices.SortFunc(types, func(a, b sqlTypeInfo) int {
		if a.code < b.code {
			return -1
		}
		if a.code > b.code {
			return 1
		}
		return 0
	})
	b := array.NewRecordBuilder(s.Alloc, schema_ref.XdbcTypeInfo)
	defer b.Release()
	for _, typ := range types {
		if filter := cmd.GetDataType(); filter != nil && *filter != int32(typ.code) {
			continue
		}
		b.Field(0).(*array.StringBuilder).Append(typ.name)
		b.Field(1).(*array.Int32Builder).Append(int32(typ.code))
		appendTypeInt(b.Field(2), typ.size)
		for _, field := range []int{3, 4} {
			if typ.quoted {
				b.Field(field).(*array.StringBuilder).Append("'")
			} else {
				b.Field(field).AppendNull()
			}
		}
		params := b.Field(5).(*array.ListBuilder)
		params.Append(true)
		params.ValueBuilder().(*array.StringBuilder).AppendValues(typ.params, nil)
		b.Field(6).(*array.Int32Builder).Append(int32(pb.Nullable_NULLABILITY_NULLABLE))
		b.Field(7).(*array.BooleanBuilder).Append(typ.text)
		searchable := pb.Searchable_SEARCHABLE_BASIC
		if typ.text {
			searchable = pb.Searchable_SEARCHABLE_FULL
		}
		b.Field(8).(*array.Int32Builder).Append(int32(searchable))
		if typ.radix > 0 {
			b.Field(9).(*array.BooleanBuilder).Append(false)
		} else {
			b.Field(9).AppendNull()
		}
		b.Field(10).(*array.BooleanBuilder).Append(false)
		b.Field(11).(*array.BooleanBuilder).Append(false)
		b.Field(12).(*array.StringBuilder).Append(typ.name)
		b.Field(13).AppendNull()
		b.Field(14).AppendNull()
		code := typ.code
		var subcode int32
		if code >= pb.XdbcDataType_XDBC_DATE && code <= pb.XdbcDataType_XDBC_TIMESTAMP {
			subcode = int32(code) - 90
			code = pb.XdbcDataType_XDBC_DATETIME
		}
		b.Field(15).(*array.Int32Builder).Append(int32(code))
		appendTypeInt(b.Field(16), subcode)
		appendTypeInt(b.Field(17), typ.radix)
		b.Field(18).AppendNull()
	}
	return schema_ref.XdbcTypeInfo, oneBatch(b.NewRecordBatch()), nil
}

func appendTypeInt(b array.Builder, v int32) {
	if v == 0 {
		b.AppendNull()
	} else {
		b.(*array.Int32Builder).Append(v)
	}
}
