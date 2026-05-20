# How To Read This Repository

Before reading implementation details, distinguish release orientation, current truth, and historical material.

The root [README](../../../README.md) is the public project entrance for the v3r1 release. The introduction docs explain the same shape in more detail. The numbered foundation documents remain the current design authority.

## First Contact

Recommended order:

1. [Root README](../../../README.md)
2. [what-is-commonground.md](what-is-commonground.md)
3. [../01-constitution.md](../01-constitution.md)
4. [../02-three-plane-model.md](../02-three-plane-model.md)
5. [../03-design-review-principles.md](../03-design-review-principles.md)
6. [../cg-history.md](../cg-history.md)
7. [../guides/open-source-quickstart.md](../guides/open-source-quickstart.md)

## Preparing To Change Implementation

Start from the durable foundation:

1. `01-constitution.md`
2. `02-three-plane-model.md`
3. `03-design-review-principles.md`

Then inspect code and tests directly. Background material can explain context, but current contracts come from the active docs, implementation, and executable tests.

## Release And Runtime Surface

For release status and public setup, read:

1. [../release-notes.md](../release-notes.md)
2. [../guides/open-source-quickstart.md](../guides/open-source-quickstart.md)
3. [../guides/agent-integration-scenarios.md](../guides/agent-integration-scenarios.md)
4. [../reference/cli.md](../reference/cli.md)
5. [../reference/http.md](../reference/http.md)

## Historical Material Rule

Historical material can explain why a design changed, but it does not override the current foundation documents or executable tests.

The v3r1 reading of the history is that CommonGround moved from stronger orchestration assumptions toward a small constitutional Ledger Kernel: public facts, durable work boundaries, explicit authority, and causal lineage first; higher-level memory abstraction and product surfaces above that foundation.
