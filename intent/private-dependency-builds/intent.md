# Intent: reproducible private dependency builds

Requested 2026-10-20 while working on issue #58. Baseline: `4120689`.
The merged driver update exposed a real hosted CI failure fetching the private fork. Provide authenticated, reproducible builds without storing credentials in Git, logs, images or permanent Git configuration.

See [spec.md](spec.md) and [plan.md](plan.md). No deployment or release is authorized by this implementation.
