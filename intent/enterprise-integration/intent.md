# Redis Enterprise integration requirement

Date: 2026-11-01. Requester: repository user. Status: implementation in review.

All integration testing must use a Redis Enterprise cluster with TLS enabled
and a username and password. Local plaintext standalone/OSS Cluster runs no
longer qualify. Preserve the community/enterprise runtime boundary.

Recommendation: private enterprise composition CI with a protected GitHub
Environment for credentials and trusted network/CA configuration. User asked
for our recommendation; no private workflow, secrets or branch protection
configuration is available in this checkout.
