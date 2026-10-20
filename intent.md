# Intent: make the existing project usable throughout an AI-assisted SDLC

Status: draft for maintainer review. Requested by: repository user in this session.
Prepared: 2026-10-20. Baseline: `27bbc3e63ed1fdfe22c7018cc723b50f1681ed50`.
Source request: inspect this project and extract its context and data into Markdown following [the AI-native SDLC playbook](https://claude.com/resources/articles/the-ai-native-sdlc-playbook).

## Project motivation

Requester-provided context, 2026-10-20: there is no easy path for the intended users to stream large, many-row result sets from Redis to remote clients. The product goal is to make that transfer straightforward and efficient for analytical applications, dataframe users, and data pipelines. This describes the problem motivating the project, rather than a claim that Redis lacks every form of streaming or that the existing server has no streaming support.

Arrow Flight RPC provides a transport designed for high-throughput Arrow data transfers over a network. Using Arrow record batches gives this project a way to reduce the row-oriented conversion and transfer overhead associated with traditional analytical database access paths. Flight SQL supplies SQL access over that transport; plain Flight supplies named dataset transfers. See the [Apache Arrow Flight documentation](https://arrow.apache.org/docs/format/Flight.html).

The requester supplied [What is Apache Arrow Flight, Flight SQL & ADBC?](https://dipankar-tnt.medium.com/what-is-apache-arrow-flight-flight-sql-adbc-a076511122ac) as background on this motivation. It is contextual reading; the protocol description above is grounded in Apache's documentation. This repository has not established a measured speedup over Redis client APIs, ODBC, or JDBC. Its Flight SQL JDBC client remains a supported access path: the performance goal concerns the underlying data transfer, not excluding applications that use the JDBC API.

### Product outcome and proof still needed

Clients should be able to consume large Redis-backed results as Arrow batches without application-specific row-by-row transfer plumbing. Preserve incremental delivery for queries that can stream, column/batch selection for datasets, and cleanup when clients disconnect or stall. The current implementation already supports these paths; some SQL queries still materialize results before sending them. See [current behavior](docs/sdlc/context.md#data-contracts).

Define representative row counts, row widths/types, concurrency, network conditions, and client workflows before setting performance acceptance thresholds. Measure throughput, time to first batch, end-to-end transfer time, and peak server/client memory against an agreed baseline. These are proposed measurements, not existing benchmark results or promises that every SQL query streams.

## Problem

The project has a detailed README, Go tests, client examples, and build/release workflows, but no checked-in intent/spec/plan chain or concise agent/review instructions. A contributor must reconstruct storage, identity, resource, and deployment constraints before making a change.

## Desired outcome

Give contributors a factual project baseline and a linked set of documents they can use to propose, implement, verify, review, release, and investigate changes, with the large-result transfer goal above as product context. Preserve the distinction between implemented behavior, unverified claims, and proposed process. The acceptance criteria below apply to this documentation change; product performance targets remain to be agreed.

## Users and systems

Maintainers, contributors, coding agents, reviewers, and operators of the Go Flight server. The relevant systems are Redis, the Redis ADBC driver, Arrow Flight/Flight SQL clients, Docker Compose, and GitHub Actions.

## Scope and constraints

- Extract evidence from tracked source, tests, examples, and workflows at the baseline commit.
- Include requester-provided product motivation with its date and provenance; use primary protocol documentation for technical background.
- Keep runtime behavior, dependency versions, release configuration, and existing README guidance intact.
- Include architecture, storage contracts, command prerequisites, review risks, and release/incident guidance.
- Do not invent customer demand, performance results, incidents, service objectives, production environments, named owners, or approvals.
- Document automation gaps; installing hooks, integrations, monitoring, or paid model evaluations is outside this documentation change.

## Acceptance criteria

1. Root intent, spec, plan, agent context, and review files exist and link to supporting evidence.
2. All six lifecycle stages have a practical entry point in the documentation index.
3. Commands match the Makefile/workflows; current storage/security constraints are explicit.
4. Proposed controls and unknown operational data are labeled; verification reports what was actually checked.

## Open questions

- Who accepts product intent, technical specifications, and release risk for this repository?
- What production topology, availability/latency objectives, and rollback artifacts exist?
- Should future lifecycle automation use Claude, Codex, or another runner, and what tools/budget may it use?
- Which large-result workloads and comparison paths represent users' transfer problem, and what throughput, latency, and memory targets would demonstrate success?

These answers are needed for operational automation, not for extracting this documentation baseline.
