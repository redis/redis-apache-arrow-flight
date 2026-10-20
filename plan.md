# Plan: extract project context and establish SDLC artifacts

Status: prepared for review; documentation scope only. Prepared: 2026-10-20.
Inputs: [intent.md](intent.md), [spec.md](spec.md).
Baseline: `27bbc3e63ed1fdfe22c7018cc723b50f1681ed50`.

## Work and files

1. Read README, `go.mod`, Makefile, Dockerfile, Compose, workflows, server/command source, tests, and client examples. Record evidence and unknowns in `docs/sdlc/context.md`.
2. Write root `intent.md`, `spec.md`, and this plan for the documentation request. Mark approvals accurately.
3. Add concise `CLAUDE.md` and project-specific `REVIEW.md` grounded in identity, SQL, dataset, and lifecycle code.
4. Add `docs/sdlc/testing.md`, `operations.md`, and `incident-template.md`; distinguish existing automation from proposed follow-up.
5. Connect the six lifecycle stages in `docs/sdlc/README.md` and link the index from root README.
6. Validate links, source paths, test identifiers, commands, and whitespace. Review the diff for accidental runtime/configuration changes.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Reconstructing past intent as an approved product decision | Label runtime material as an extracted baseline and process material as draft |
| Overstating test coverage or tool compatibility | Link actual test functions and retain README's untested-tool caveats |
| Unsafe integration commands against a real database | Require disposable Redis and document fixture mutations |
| Misleading operational claims | Leave owner, SLO, alert, production and approval details unresolved |
| Documentation drifts from implementation | Update corresponding documents in behavior-changing PRs |

## Proof and handoff

- Required: local file links resolve; referenced paths and test identifiers exist; documented Make targets exist; `git diff --check` is clean.
- Inspect `git status` and diff to confirm only Markdown changed.
- No build, Redis test, release, or publication is implied by completion of documentation checks.
- Maintainer review resolves ownership and acceptance; proposed operational automation needs its own change-specific intent/spec/plan.

## Reversal

Remove the added Markdown files and README link to reverse this documentation change. No Redis data or runtime settings are changed.
