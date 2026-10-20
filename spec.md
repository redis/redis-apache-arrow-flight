# Specification: project context and SDLC documentation

Status: draft; no maintainer approval recorded. Prepared: 2026-10-20.
Input: [intent.md](intent.md). Implementation steps: [plan.md](plan.md).
This specifies the documentation change; the current runtime is described in [context.md](docs/sdlc/context.md).

## Deliverables

| File | Required content | Acceptance evidence |
| --- | --- | --- |
| `CLAUDE.md` | Concise commands, component map, critical constraints | Every command/path is present in repository sources |
| `REVIEW.md` | Project-specific review passes, finding severity, review record | Covers identity, writes, dataset generations, resource cleanup, and compatibility |
| `intent.md`, `spec.md`, `plan.md` | Linked request, requirements, ordered work and proof | Explicit draft/approval state and source baseline |
| `docs/sdlc/README.md` | Lifecycle entry points, artifact ownership, future-change convention | Links resolve and current/proposed controls are distinguished |
| `docs/sdlc/context.md` | Architecture, versions, defaults, storage contracts, evidence inventory | Local source links support factual assertions |
| `docs/sdlc/testing.md` | Local/CI checks, behavior-to-test mapping, proposed agent evaluation cases | No invented test/lint targets or benchmark results |
| `docs/sdlc/operations.md` | Existing release flow, deployment checks, diagnosis, rollback constraints | Follows release workflow, health/metrics code and storage compatibility |
| `docs/sdlc/incident-template.md` | Evidence and follow-up record for future incidents | Unfilled fields are explicitly placeholders, not an invented incident |

Add a discoverable link from the existing README.

## Content rules

1. Use repository-relative links so documents remain portable in GitHub and local checkouts.
2. Identify baseline SHA and extraction date. Tests in source prove coverage exists, not that this checkout passed a run.
3. Treat executable code/configuration as the source for implemented behavior; report conflicts with prose instead of silently turning desired behavior into facts.
4. Use Git and PR records for actual decisions. No named approval or external tracker is assumed.
5. Redact secrets and user data. Sample dataset names and metrics are examples, not observed production data.
6. Keep future feature intent/spec/plan together under `intent/<change-id>/` to avoid overwriting this adoption record.

## Design decisions and concerns

- Keep the README as the user-facing manual; add an engineering workflow layer with source links.
- Keep agent startup context short and place detailed extraction in supporting documents.
- Describe security and deployment checks as proposed review guidance unless deterministic enforcement is visible in existing files.
- Do not add vendor settings, hooks, executable skills, eval jobs, or deployment credentials as part of a Markdown extraction.
- Highest-risk documentation mistakes are claiming all SQL streams, all memory is budgeted, SQL and datasets share a namespace/interface, or rollback to an older reader is always safe.

## Completion criteria

All intent acceptance criteria are met; Markdown local file links resolve, referenced test names/commands are checked against sources, and `git diff --check` passes. Runtime tests are not required for this documentation-only diff. Runtime verification, if performed, must be reported separately with skips and environment limitations.
