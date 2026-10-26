# Flight endpoint capabilities

This project is a Flight SQL and Flight RPC endpoint backed by Redis. Both interfaces share one gRPC service. Protocol support is distinct from validation of a particular BI application.

| Interface / operation | Behavior | Evidence |
| --- | --- | --- |
| Flight SQL statements and prepared statements | Query/update, parameter binding, schema discovery; prepared clients must use the rebound handle | integration_test.go, JDBC smoke |
| Flight SQL ingestion | Create, append, replace, create-or-append; autocommit | TestIntegrationIngest |
| Catalogs, schemas, tables, table types | Standard Arrow schemas; optional serialized table schema | TestIntegrationMetadata |
| GetXdbcTypeInfo | Canonical scalar SQL types, ordered by data_type; exact optional type filter; unknown filter returns an empty standard-schema stream. Unbounded/unknown size or scale is NULL. SQL aliases and RECORD are not separate entries. | TestFlightTypeInfo, TestIntegrationTypeInfoDDL |
| GetSqlInfo | Reports server identity, SQL/ingest support, typed capability values and autocommit limitations | unit_test.go and TestIntegrationMetadata |
| Primary/foreign keys | Standard-schema empty results, because the backend has no such constraints | metadata.go |
| Sessions | Cookie sessions for settings and temporary objects; unknown state returns ABORTED before execution and a recovery cookie | session_integration_test.go |
| Flight RPC datasets | PATH GetFlightInfo/GetSchema/PollFlightInfo, DoPut/DoGet, ListFlights, drop_dataset; nested Arrow types, columns/batch selection | dataset integration tests, Python quickstart |
| Query cancellation | CancelFlightInfo and translated legacy CancelQuery; pending/active read-only tickets, identity checks, shared Redis state, cooperative cleanup; completed/write/non-cancellable tickets return NOT_CANCELLABLE | cancellation_integration_test.go |
| DoExchange | Deferred; returns UNIMPLEMENTED and directs clients to DoPut/DoGet | TestFlightUnsupportedCapabilities |
| Transactions/savepoints | Out of scope until the Redis ADBC backend supplies transactional guarantees. BeginTransaction returns UNIMPLEMENTED; transaction IDs on statements/ingestion are rejected. GetSqlInfo advertises no transactions and no ingestion transactions. | sqlserver.go, TestFlightUnsupportedCapabilities |
| Substrait | Deferred until the backend has a supported plan execution contract. Commands return UNIMPLEMENTED and support is advertised as false. | TestFlightUnsupportedCapabilities |

DoExchange is not needed for the defined park/fetch dataset workflow. No application-specific bidirectional transform is promised. A future proposal must specify descriptor and schemas, authorization, backpressure and cancellation for an actual consumer before adding that method.

The [JDBC schema limitation](https://github.com/redis/redis-apache-arrow-flight/issues/17) still applies to statements whose result schema is unknown when prepared. This matrix does not imply every SQL query streams or that Tableau/Power BI work without compatibility testing.
