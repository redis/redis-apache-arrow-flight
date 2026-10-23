# Project SDLC documentation

Prepared 2026-10-20 from commit `27bbc3e63ed1fdfe22c7018cc723b50f1681ed50`.

This repository adapts the artifact handoffs in [Anthropic's AI-native SDLC playbook](https://claude.com/resources/articles/the-ai-native-sdlc-playbook): intent, specification, implementation plan, code and tests, review, then operational feedback. The project-specific content below comes from this repository; it is not evidence that the playbook's automation has been installed.

## Start here

| Stage | Read/use | Current state |
| --- | --- | --- |
| Plan | [Root intent](../../intent.md) | Documents this extraction request; draft |
| Design | [Root specification](../../spec.md), [project context](context.md) | Documentation requirements and extracted runtime baseline |
| Build | [Root plan](../../plan.md), [CLAUDE.md](../../CLAUDE.md) | Documentation work plan and concise agent context |
| Test | [Testing and evaluation](testing.md) | Existing executable checks and proposed agent evaluation cases |
| Deploy | [REVIEW.md](../../REVIEW.md), [operations](operations.md) | Proposed review policy; existing release workflow described |
| Maintain | [Operations](operations.md), [incident template](incident-template.md) | Available signals and a future incident record format |

## Records and ownership

The root intent/spec/plan describe **this documentation change**, not an invented pending product feature. For a new change, create `intent/<change-id>/intent.md`, `spec.md`, and `plan.md`; link those exact files in its PR. Record a source SHA, date, requester, status, unresolved questions, acceptance evidence, and actual reviewer decision. For incidents, keep the incident record alongside those files and link the corrective intent.

Proposed roles are requester/maintainer for intent, technical maintainer for spec/plan, reviewer for change assessment, and operator for deployment and incident decisions. Named people and approval rules remain unassigned. A document marked draft is not approved by its existence.

Use the repository as the working source of truth for these Markdown artifacts. Git history identifies revisions once committed; PR records hold actual review decisions. No commit, approval, deployment, or external tracker record is created by writing these files.

## Existing controls and gaps

Proposed product work: [optional Active-Active control plane](../active-active-control-plane.md),
with [intent](../../intent/active-active-control-plane/intent.md),
[specification](../../intent/active-active-control-plane/spec.md),
[plan](../../intent/active-active-control-plane/plan.md) and
[evidence](../../intent/active-active-control-plane/evidence.md). This is a design,
not an available runtime feature.

Existing: format/vet checks, standalone and cluster race tests, client smoke tests, release artifact verification, runtime health and metrics. See [testing](testing.md) and [operations](operations.md).

Not found in the inspected repository: Claude/Codex hook configuration, executable policy skills, agent eval workflows, production deployment manifests, alert rules, scheduled diagnosis, incident history, or recorded SLO baselines. Repository-hosted branch protections and environment settings were not inspected. Markdown guidance is advisory.

Candidate follow-up work is to assign owners, establish deployment/rollback evidence, approve representative agent evaluation tasks, and then design deterministic automation with its own tests. No schedule or external integration is enabled here.

## Commercial feature classification

Requestor policy: monitoring, optimization, automation, backup and security are
paid-feature categories. Any matching feature/change requires a recorded
commercial review before implementation/release. Apply the
[commercial feature review gate](../active-active-control-plane.md#commercial-feature-review-gate)
to both native driver and Flight work, including overlaps with existing behavior.
No agent classification substitutes for a requestor/maintainer packaging decision.

Community/enterprise build contract: public interfaces and stubs compile without
commercial dependencies; enterprise composition supplies full implementations.
See the feature boundary linked above for availability/error behavior. Code
extraction and build-profile qualification remain pending.
