# Specification

Input: [intent.md](intent.md). Evidence: [plan.md](plan.md).

Verify the latest stable release and default branch against upstream. Select the release commit, resolve its Go submodule pseudo-version, and pin it with verified checksums. Require the upstream minimum Go version in the module and explicit container pins. Document the selected version and permanent update policy.

Acceptance: race-enabled community unit tests, vet, static server build, and whitespace checks pass. Report backend integration skips and storage compatibility limits explicitly; no deployment or migration is authorized by this update.
