# Incident: <short description>

Status: TEMPLATE — no incident is asserted by this file.
Copy to the change's artifact directory before filling it in.

## Record

- Incident ID / issue link: <record>
- Owner / investigator: <name>
- Started / detected / recovered: <timestamps with timezone>
- Environment and affected clients: <known values>
- Server version / source SHA / image digest: <known values>
- Related deployment or configuration change: <link or unknown>

## Observed impact and evidence

Describe affected operations, user impact, duration, and scope. Link sanitized logs, metrics with query windows, gRPC codes, client errors, and reproduction steps. Mark unavailable data as unknown. Do not include credentials, bearer tokens, private keys, or customer rows.

## Timeline and diagnosis

| Time | Observation or action | Actor and evidence |
| --- | --- | --- |
| <timestamp> | <observed fact or action> | <actor/link> |

Separate confirmed cause from hypotheses. Check identity/session behavior, Redis/Search availability, cursor/pool pressure, result memory, dataset generation/layout, and recent changes as applicable.

## Mitigation and recovery

- Decision and authorization: <actual decision record>
- Action and exact artifact/configuration: <what changed>
- Compatibility and data consequences: <including dataset rollback constraints>
- Recovery proof: <affected operation succeeds; relevant metrics over a stated window>
- Remaining impact or uncertainty: <details>

## Corrective intent and regression evidence

- Intent / spec / plan: <links in this change directory>
- Regression test and observed failure before fix: <evidence or reason unavailable>
- Fix / review / verification / release: <links and results>
- Agent evaluation case, if applicable: <proposed case or runner result>
- Follow-up owner and due date: <agreed values>
- Closure decision: <actual decision, person, date>
