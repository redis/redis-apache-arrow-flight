# Specification

Use standard Flight session RPCs with documented Redis option extensions and an
explicit attempt header. Bind before discovery, retain exactly one owned native
connection, serialize readers and never copy a binding to a pooled connection.
Native generation IDs are diagnostic; random attempt handles require the owned
cookie plus the current authenticated identity. Require TLS/ACL configuration;
the minimum TLS/ACL security baseline is core under the requester's follow-up
instruction; see [security decision](security.md).

Fix lifetime at 2–60 minutes. Make failed acquire, metadata/read failures,
cancellation, expiry and authority loss terminal. Release closes the dedicated
connection; post-release reads must fail. Preserve native ACL/grant checks,
managed catalog/hash-slot boundary and admission limits. Cancel/control and
reader release must free native/Arrow/session/stream resources.

Pin a completed native release containing the API and document storage upgrade
constraints. Add actual Python ADBC Flight SQL tests and publish the exact
protocol, versions, cleanup/reconnect behavior and qualification limitations.
Connector activation requires independently recorded transport validation.

Commercial boundary: the requester's follow-up explicitly authorizes basic
Flight TLS/ACL login and verified Redis connectivity in core. Other paid features
retain their existing boundaries. Enterprise composition must be requalified.
