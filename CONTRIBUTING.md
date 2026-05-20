# Contributing

Thanks for contributing to CommonGround.

## Before You Start

Read the current truth documents:

1. [Constitution](docs/en/01-constitution.md)
2. [Three-Plane Model](docs/en/02-three-plane-model.md)
3. [Design Review Principles](docs/en/03-design-review-principles.md)

## Development Setup

```bash
git submodule update --init --recursive
uv sync --extra server
```

The `server` extra is required for the local HTTP services and PostgreSQL-backed paths.

Use a local PostgreSQL database for full tests:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME uv run -m scripts.setup.reset_db
```

## Checks

Run the closest focused tests for your change. For release-facing changes, use:

```bash
git diff --check
uv run python -m compileall CommonGround Integrations tests scripts
uv run --with pytest --with packaging python -m pytest tests/test_doc_hygiene.py tests/test_cg_cli.py tests/test_cg_skill_assets.py tests/test_release_tag_version.py -q
```

Full PostgreSQL-backed tests require `PG_DSN`:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME uv run --with pytest python -m pytest tests -q
```

## PyPI Release Setup

The release preparation workflow opens a Draft PR that synchronizes `VERSION`, `pyproject.toml`, and `uv.lock` to a target release version.

After that PR is merged into `main`, the publish workflow creates the release tag from the merged commit, publishes `commonground-kernel` to PyPI, and syncs the GitHub Release.

For GitHub Actions, store the PyPI project-scoped token as the `PYPI_API_TOKEN` secret on the `pypi` environment, not as a plain repository secret. Configure the `pypi` environment with tag restrictions and required reviewers before enabling public package releases.

## Pull Requests

- Keep changes scoped.
- Update docs when behavior, commands, environment variables, or public contracts change.
- Do not include local paths, real credentials, generated runtime output, or personal database names.
- Do not reintroduce historical documents into the active docs tree unless the task explicitly asks for a current rewrite.
- Explain authority and lifecycle implications for kernel, service, CLI, and integration changes.
