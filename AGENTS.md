# Repository Guidelines

## Repository Role

`CommonGround/` is the active implementation repository for CommonGround.

Primary implementation paths:

- `CommonGround/`: kernel, truth, service, SDK, and CLI code.
- `Integrations/nanobot/`: external runtime, provision, and leaf worker integration.
- `tests/`: regression coverage and executable contracts.
- `docs/`: current architecture and design documentation.

## Documentation Priority

When working in this repository, read these documents first:

1. `docs/en/01-constitution.md` or `docs/zh/01-constitution.md`
2. `docs/en/02-three-plane-model.md` or `docs/zh/02-three-plane-model.md`
3. `docs/en/03-design-review-principles.md` or `docs/zh/03-design-review-principles.md`
4. `docs/en/introduction/` or `docs/zh/introduction/`

The English and Chinese documentation trees are parallel current surfaces. Background material can explain design history, but it does not override the current docs or code.

## Code Structure

Important paths:

- `CommonGround/contracts/`
- `CommonGround/kernel/`
- `CommonGround/infra/`
- `CommonGround/sdk/`
- `CommonGround/adapters/`
- `CommonGround/service/`
- `Integrations/nanobot/`
- `tests/`

## Common Commands

Setup:

```bash
git submodule update --init --recursive
uv sync
```

Reset the database:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME uv run -m scripts.setup.reset_db
```

Run the service:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME uv run cg service run
```

Compile check:

```bash
uv run python -m compileall CommonGround Integrations tests
```

Full tests:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME uv run --with pytest python -m pytest tests -q
```

Focused mainline tests:

```bash
uv run --with pytest python -m pytest tests/test_agent_only_service.py tests/test_cg_cli.py tests/test_leaf_worker_main.py tests/test_nanobot_bridge.py -q
```

## Testing Notes

The pytest configuration creates isolated test databases per worker and resets each database per test. Parallel file execution should be safe when tests use `tests/conftest.py` and `tests/pg_support.py`.

Custom scripts can still reintroduce shared database races if they reuse a manually written `PG_DSN`.

## Implementation Boundaries

Review the constitution and design principles before changing high-risk areas:

- truth schema;
- registration and admission contracts;
- lifecycle state transitions;
- Turn claim, resume, and stop fencing;
- provision role discoverability.

Provisioning has three separate layers:

- `AgentSnapshot.role` and `AgentSnapshot.description`: Agent truth.
- `public_metadata.turn_offers[]`: canonical discoverability projection.
- NanoBot `RolePolicy`: integration-local business interpretation.

Do not promote discoverability metadata into kernel authority.

## Versioning Rules

- `v3r1` is the active service route prefix, API version marker, and protocol version for the current line.
- The first package release that matches `v3r1` is `v3.1.0`.
- Release tags must use canonical PEP 440 Git tags in the form `v<major>.<minor>.<patch>`, for example `v3.1.0` or `v3.1.1rc1`.
- Patch-only implementation changes that do not change the `v3r1` protocol contract increment the third release segment: `v3.1.1`, `v3.1.2`, and so on.
- A protocol/API upgrade from `v3r1` to `v3r2` increments the second release segment and resets the patch segment, so the first release in that line is `v3.2.0`.
- Do not use route labels such as `v3r1` or semantic labels such as `v3-preview` as package release tags.

## Git Notes

- `CG-Cardbox/` is a submodule. Check its status before editing it.
- Do not reset user changes in the submodule.
- If a task only changes the main repository, do not include submodule changes accidentally.
