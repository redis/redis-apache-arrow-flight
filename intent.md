# Intent: make the existing project usable throughout an AI-assisted SDLC

Status: draft for maintainer review. Requested by: repository user in this session.
Prepared: 2026-10-20. Baseline: `27bbc3e63ed1fdfe22c7018cc723b50f1681ed50`.
Source request: inspect this project and extract its context and data into Markdown following [the AI-native SDLC playbook](https://claude.com/resources/articles/the-ai-native-sdlc-playbook).

## Problem

The project has a detailed README, Go tests, client examples, and build/release workflows, but no checked-in intent/spec/plan chain or concise agent/review instructions. A contributor must reconstruct storage, identity, resource, and deployment constraints before making a change.

## Desired outcome

Give contributors a factual project baseline and a linked set of documents they can use to propose, implement, verify, review, release, and investigate changes. Preserve the distinction between implemented behavior, unverified claims, and proposed process.

## Users and systems

Maintainers, contributors, coding agents, reviewers, and operators of the Go Flight server. The relevant systems are Redis, the Redis ADBC driver, Arrow Flight/Flight SQL clients, Docker Compose, and GitHub Actions.

## Scope and constraints

- Extract evidence from tracked source, tests, examples, and workflows at the baseline commit.
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

These answers are needed for operational automation, not for extracting this documentation baseline.
