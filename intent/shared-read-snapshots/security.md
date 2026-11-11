# Minimum security scope and requester decision

Date: 2026-11-11. Requester: current issue #65 chat. Decision: after reviewing the
minimum required TLS/authentication capabilities, the requester instructed
**"do the minimum"**. This explicitly authorizes implementation of the described
basic security baseline in this checkout, superseding the earlier blanket public
security-stub boundary for this scope. No additional packaging approval is inferred.

Core scope:
- Flight TLS with certificate/key loading and certificate/hostname verification
  on the client; Redis ACL login requires encrypted transport on both legs.
- Validate client Redis username/password with a bounded TLS PING. Preserve that
  identity's credentials through tenants, native ADBC and gateway Redis clients.
- AES-256-GCM bearer tokens with random nonces, expiry, authenticated ciphertext,
  a random process key by default and minimum 32-byte configured secrets.
- Identical explicit Redis CA trust for login, gateway clients and native ADBC;
  refuse plaintext credential URIs and skip_verify=true.
- Existing session/snapshot ownership, native ACL/grant enforcement and terminal
  failure handling. No plaintext/anonymous fallback for snapshot mode.

Community and enterprise both expose this baseline. Monitoring, optimization,
automation, backup, Active-Active and unimplemented advanced security retain their
previous packaging/implementation boundaries. No private code/import was copied.
Single-instance/sticky-session deployment is sufficient. Mutual TLS, OAuth/SSO,
certificate automation and durable snapshot resume are outside this request.

Evidence: security_test.go exercises real verified TLS to Flight and a TLS RESP
authentication fixture, valid/invalid credentials, token tampering/expiry/wrong
keys, untrusted certificates, hostname mismatch and cross-principal snapshot use.
Native SQL is a test double in the composed TLS/ownership case. Full Redis
Enterprise, Python ADBC/Airbyte and grant-revocation qualification remain separate
requirements. An unrelated running Redis container was observed read-only; it was
not modified or assumed disposable.

This requester scope decision is not a production deployment/release approval.
