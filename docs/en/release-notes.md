# Release Notes

Status: Initial Open Source Preview (`v3r1`).

## Current Support Statement

v3r1 is the first public preview line for CommonGround Kernel. It focuses on the Ledger Kernel: Agent identity, Turn lifecycle, claim fencing, Turn-owned semantic records, causal lineage, projection reads, CLI workflows, and PostgreSQL-backed local development.

Supported at this stage:

- PostgreSQL-backed local development and test execution.
- `uv`-based setup.
- CommonGround Service through `cg service run`.
- Single-port local development through `cg local run`.
- Separated local service surfaces through `cg service run` and `cg admission run`.
- CLI-first operation for dispatch, turn inspection, project observation, worker lifecycle, profiles, reports, setup, and admission service entrypoints.
- BYOA work-memory reporting and BYOA conversation-worker examples.
- NanoBot integration fixtures as optional advanced demos.

## v3r1 Naming

The open-source preview line is v3r1. The current service/API implementation version is `v3r1`, used by `CommonGround/service/` and the client code in `CommonGround/agent_client/`.

Treat `v3r1` as the current preview API prefix. It is not a long-term compatibility promise.

## Preview Boundaries

- The open-source first-run path is `cg local run` plus BYOA examples, not NanoBot integration fixtures.
- Current work knowledge is ledger-native; higher-level memory APIs are outside the v3r1 kernel surface.
- The `v3r1` runtime surface covers local development, PostgreSQL-backed verification, and separated local service entrypoints.

## Release Checks

Use the source-checkout release/test workflow documented in [CONTRIBUTING.md](../../CONTRIBUTING.md) before publishing a public release candidate.

## PyPI Publish

- Package versions are synchronized into `VERSION`, `pyproject.toml`, and `uv.lock` before release.
- Release tags still use canonical PEP 440 Git tags that match `v<version>`, such as `v3.1.0` or `v3.1.1rc1`.
- Non-canonical semantic tags such as `v3-preview` or route labels such as `v3r1` are not valid package release tags and are rejected by the publish workflow.
- Maintainers manually start the release-preparation workflow to generate a Draft PR for version metadata changes.
- After that PR is merged into `main`, the publish workflow creates the release tag from the merged commit, verifies the artifact version matches the tag, smoke-tests both wheel and sdist, and then publishes `commonground-kernel` with the `PYPI_API_TOKEN` environment secret.
- Store `PYPI_API_TOKEN` as a GitHub Actions environment secret on the `pypi` environment, and use that environment's required reviewers and tag restrictions to gate release access.
