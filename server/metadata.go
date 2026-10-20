package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

// The catalog commands read the driver's ADBC GetObjects, whose nested
// catalogs → db schemas → tables lists are flattened into Flight SQL's
// tables. Their tickets are the command itself.

type schemaRow struct {
	catalog *string
	schema  string
}

type tableRow struct {
	catalog, schema *string
	name, kind      string
}

type objects struct {
	catalogs []string
	schemas  []schemaRow
	tables   []tableRow
}

// flattenObjects reads a GetObjects result (adbc.GetObjectsSchema).
func flattenObjects(rdr array.RecordReader) (objects, error) {
	var out objects
	for rdr.Next() {
		rec := rdr.RecordBatch()
		cats, err := column[*array.String](rec, "catalog_name")
		if err != nil {
			return out, err
		}
		dbs, err := column[*array.List](rec, "catalog_db_schemas")
		if err != nil {
			return out, err
		}
		schemas, err := listStruct(dbs)
		if err != nil {
			return out, err
		}
		schemaNames, err := field[*array.String](schemas, "db_schema_name")
		if err != nil {
			return out, err
		}
		tableLists, err := field[*array.List](schemas, "db_schema_tables")
		if err != nil {
			return out, err
		}
		tables, err := listStruct(tableLists)
		if err != nil {
			return out, err
		}
		tableNames, err := field[*array.String](tables, "table_name")
		if err != nil {
			return out, err
		}
		tableTypes, err := field[*array.String](tables, "table_type")
		if err != nil {
			return out, err
		}
		for i := range int(rec.NumRows()) {
			cat := optString(cats, i)
			if cat != nil {
				out.catalogs = append(out.catalogs, *cat)
			}
			if dbs.IsNull(i) {
				continue
			}
			start, end := dbs.ValueOffsets(i)
			for j := int(start); j < int(end); j++ {
				sc := optString(schemaNames, j)
				if sc == nil {
					continue
				}
				out.schemas = append(out.schemas, schemaRow{catalog: cat, schema: *sc})
				if tableLists.IsNull(j) {
					continue
				}
				ts, te := tableLists.ValueOffsets(j)
				for k := int(ts); k < int(te); k++ {
					out.tables = append(out.tables, tableRow{
						catalog: cat, schema: sc,
						name: tableNames.Value(k), kind: tableTypes.Value(k),
					})
				}
			}
		}
	}
	if err := rdr.Err(); err != nil {
		return out, err
	}
	slices.Sort(out.catalogs)
	slices.SortFunc(out.schemas, func(a, b schemaRow) int {
		return cmp.Or(cmpOpt(a.catalog, b.catalog), cmp.Compare(a.schema, b.schema))
	})
	slices.SortFunc(out.tables, func(a, b tableRow) int {
		return cmp.Or(cmpOpt(a.catalog, b.catalog), cmpOpt(a.schema, b.schema), cmp.Compare(a.name, b.name))
	})
	return out, nil
}

func column[T arrow.Array](rec arrow.RecordBatch, name string) (T, error) {
	var zero T
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		return zero, fmt.Errorf("GetObjects result has no %s column", name)
	}
	col, ok := rec.Column(idx[0]).(T)
	if !ok {
		return zero, fmt.Errorf("GetObjects column %s has type %s", name, rec.Column(idx[0]).DataType())
	}
	return col, nil
}

func listStruct(l *array.List) (*array.Struct, error) {
	s, ok := l.ListValues().(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("GetObjects list holds %s, not a struct", l.ListValues().DataType())
	}
	return s, nil
}

func field[T arrow.Array](s *array.Struct, name string) (T, error) {
	var zero T
	idx, ok := s.DataType().(*arrow.StructType).FieldIdx(name)
	if !ok {
		return zero, fmt.Errorf("GetObjects struct has no %s field", name)
	}
	f, ok := s.Field(idx).(T)
	if !ok {
		return zero, fmt.Errorf("GetObjects field %s has type %s", name, s.Field(idx).DataType())
	}
	return f, nil
}

func optString(a *array.String, i int) *string {
	if a.IsNull(i) {
		return nil
	}
	v := a.Value(i)
	return &v
}

// cmpOpt orders nil before every string.
func cmpOpt(a, b *string) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return cmp.Compare(*a, *b)
}

func appendOpt(b *array.StringBuilder, v *string) {
	if v == nil {
		b.AppendNull()
	} else {
		b.Append(*v)
	}
}

// getObjects runs GetObjects on a pooled connection, and then more, if
// given, on the same connection.
func (s *sqlServer) getObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, table *string, tableTypes []string,
	more func(context.Context, adbc.ConnectionWithContext, objects) error) (objects, error) {
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return objects{}, err
	}
	defer done()
	var out objects
	err = t.withConn(ctx, "", func(conn adbc.ConnectionWithContext) error {
		rdr, err := conn.GetObjects(ctx, depth, catalog, dbSchema, table, nil, tableTypes)
		if err != nil {
			return err
		}
		defer rdr.Release()
		if out, err = flattenObjects(rdr); err != nil {
			return err
		}
		if more != nil {
			return more(ctx, conn, out)
		}
		return nil
	})
	return out, toStatus(err)
}

func (s *sqlServer) commandInfo(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return s.flightInfo(desc, schema, desc.Cmd)
}

// oneBatch streams a single record batch.
func oneBatch(rec arrow.RecordBatch) <-chan flight.StreamChunk {
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: rec}
	close(ch)
	return ch
}

func (s *sqlServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.Catalogs), nil
}

func (s *sqlServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	objs, err := s.getObjects(ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	b := array.NewRecordBuilder(s.Alloc, schema_ref.Catalogs)
	defer b.Release()
	names := b.Field(0).(*array.StringBuilder)
	for _, c := range objs.catalogs {
		names.Append(c)
	}
	return schema_ref.Catalogs, oneBatch(b.NewRecordBatch()), nil
}

func (s *sqlServer) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.DBSchemas), nil
}

func (s *sqlServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	objs, err := s.getObjects(ctx, adbc.ObjectDepthDBSchemas, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern(), nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	b := array.NewRecordBuilder(s.Alloc, schema_ref.DBSchemas)
	defer b.Release()
	cats := b.Field(0).(*array.StringBuilder)
	names := b.Field(1).(*array.StringBuilder)
	for _, sc := range objs.schemas {
		appendOpt(cats, sc.catalog)
		names.Append(sc.schema)
	}
	return schema_ref.DBSchemas, oneBatch(b.NewRecordBatch()), nil
}

func tablesSchema(includeSchema bool) *arrow.Schema {
	if includeSchema {
		return schema_ref.TablesWithIncludedSchema
	}
	return schema_ref.Tables
}

func (s *sqlServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, tablesSchema(cmd.GetIncludeSchema())), nil
}

func (s *sqlServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := tablesSchema(cmd.GetIncludeSchema())
	var tableSchemas [][]byte
	more := func(ctx context.Context, conn adbc.ConnectionWithContext, objs objects) error {
		if !cmd.GetIncludeSchema() {
			return nil
		}
		tableSchemas = make([][]byte, len(objs.tables))
		for i, tr := range objs.tables {
			ts, err := conn.GetTableSchema(ctx, tr.catalog, tr.schema, tr.name)
			if err != nil {
				var ae adbc.Error
				if errors.As(err, &ae) && ae.Code == adbc.StatusNotFound {
					continue // dropped since it was listed
				}
				return err
			}
			tableSchemas[i] = flight.SerializeSchema(ts, s.Alloc)
		}
		return nil
	}
	objs, err := s.getObjects(ctx, adbc.ObjectDepthTables, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern(),
		cmd.GetTableNameFilterPattern(), cmd.GetTableTypes(), more)
	if err != nil {
		return nil, nil, err
	}
	b := array.NewRecordBuilder(s.Alloc, schema)
	defer b.Release()
	cats := b.Field(0).(*array.StringBuilder)
	schemas := b.Field(1).(*array.StringBuilder)
	names := b.Field(2).(*array.StringBuilder)
	kinds := b.Field(3).(*array.StringBuilder)
	for i, tr := range objs.tables {
		if tableSchemas != nil && tableSchemas[i] == nil {
			continue
		}
		appendOpt(cats, tr.catalog)
		appendOpt(schemas, tr.schema)
		names.Append(tr.name)
		kinds.Append(tr.kind)
		if tableSchemas != nil {
			b.Field(4).(*array.BinaryBuilder).Append(tableSchemas[i])
		}
	}
	return schema, oneBatch(b.NewRecordBatch()), nil
}

func (s *sqlServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.TableTypes), nil
}

func (s *sqlServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	t, done, err := s.tenants.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer done()
	var kinds []string
	err = t.withConn(ctx, "", func(conn adbc.ConnectionWithContext) error {
		rdr, err := conn.GetTableTypes(ctx)
		if err != nil {
			return err
		}
		defer rdr.Release()
		for rdr.Next() {
			col, err := column[*array.String](rdr.RecordBatch(), "table_type")
			if err != nil {
				return err
			}
			for i := range col.Len() {
				kinds = append(kinds, col.Value(i))
			}
		}
		return rdr.Err()
	})
	if err != nil {
		return nil, nil, toStatus(err)
	}
	slices.Sort(kinds)
	b := array.NewRecordBuilder(s.Alloc, schema_ref.TableTypes)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues(kinds, nil)
	return schema_ref.TableTypes, oneBatch(b.NewRecordBatch()), nil
}

// The driver has no primary/foreign-key constraints, so key lookups answer with no rows rather
// than an error, which JDBC tools would show.

func (s *sqlServer) emptyResult(schema *arrow.Schema) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	b := array.NewRecordBuilder(s.Alloc, schema)
	defer b.Release()
	return schema, oneBatch(b.NewRecordBatch()), nil
}

func (s *sqlServer) GetFlightInfoPrimaryKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.PrimaryKeys), nil
}

func (s *sqlServer) DoGetPrimaryKeys(context.Context, flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.emptyResult(schema_ref.PrimaryKeys)
}

func (s *sqlServer) GetFlightInfoExportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.ExportedKeys), nil
}

func (s *sqlServer) DoGetExportedKeys(context.Context, flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.emptyResult(schema_ref.ExportedKeys)
}

func (s *sqlServer) GetFlightInfoImportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.ImportedKeys), nil
}

func (s *sqlServer) DoGetImportedKeys(context.Context, flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.emptyResult(schema_ref.ImportedKeys)
}

func (s *sqlServer) GetFlightInfoCrossReference(_ context.Context, _ flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.commandInfo(desc, schema_ref.CrossReference), nil
}

func (s *sqlServer) DoGetCrossReference(context.Context, flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.emptyResult(schema_ref.CrossReference)
}
