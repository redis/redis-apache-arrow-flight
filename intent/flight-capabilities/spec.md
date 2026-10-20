# Specification

Implement GetXdbcTypeInfo with Apache's fixed Arrow schema, optional exact type-code filtering, sorted type codes, canonical scalar SQL names, and NULL for unknown/unbounded metadata. Validate reported types by creating and querying a real Redis-backed table. Preserve correct autocommit/transaction/Substrait capability reporting.

Defer DoExchange until a consumer supplies a bidirectional operation contract. Explicitly return UNIMPLEMENTED with the supported DoPut/DoGet alternative and verify it through the Flight client. No backend transaction or Substrait support is invented.
