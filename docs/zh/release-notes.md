# 发布说明

状态：Initial Open Source Preview（`v3r1`）。

## 当前支持声明

v3r1 是 CommonGround Kernel 的第一条公开 preview 版本线。它聚焦 Ledger Kernel：Agent identity、Turn lifecycle、claim fencing、Turn-owned semantic records、causal lineage、projection reads、CLI workflows，以及 PostgreSQL-backed local development。

当前阶段支持：

- PostgreSQL-backed local development 与 test execution。
- 基于 `uv` 的 setup。
- 通过 `cg service run` 运行 CommonGround Service。
- 通过 `cg local run` 运行 single-port local development 路径。
- 通过 `cg service run` 与 `cg admission run` 分离运行本地 service surfaces。
- CLI-first operation，覆盖 dispatch、Turn inspect、project observation、worker lifecycle、profile、report、setup 与 admission service entrypoint。
- BYOA work-memory reporting 与 BYOA conversation-worker examples。
- NanoBot integration fixtures 作为 optional advanced demos。

## v3r1 命名

当前开源 preview 版本线是 v3r1。当前 service/API 实现版本是 `v3r1`，由 `CommonGround/service/` 与 `CommonGround/agent_client/` 客户端代码使用。

请把 `v3r1` 理解为当前 preview API prefix。它不是长期兼容承诺。

## Preview Boundaries

- 开源 first-run 路径是 `cg local run` 加 BYOA examples，不是 NanoBot integration fixtures。
- 当前 work knowledge 是 ledger-native；更高层 memory API 不属于 v3r1 kernel surface。
- `v3r1` runtime surface 覆盖 local development、PostgreSQL-backed verification，以及本地分离 service entrypoints。

## 发布检查

发布 public release candidate 前，请使用 [CONTRIBUTING.md](../../CONTRIBUTING.md) 中记录的源码 checkout release/test workflow。

## PyPI 发布

- 包版本会先同步到 `VERSION`、`pyproject.toml` 与 `uv.lock`。
- release tag 仍使用符合 canonical PEP 440 的 Git tag，格式是 `v<version>`，例如 `v3.1.0` 或 `v3.1.1rc1`。
- `v3-preview` 这类语义 tag，或 `v3r1` 这类 route label，都不是合法的 package release tag；publish workflow 会直接拒绝。
- 维护者需要手动启动 release-preparation workflow，生成同步版本元数据的 Draft PR。
- 该 PR 合并到 `main` 后，publish workflow 会从合并后的提交创建 release tag，校验产物版本与 tag 一致，对 wheel 和 sdist 做 smoke test，然后使用 `PYPI_API_TOKEN` environment secret 发布 `commonground-kernel`。
- 请把 `PYPI_API_TOKEN` 存放在 GitHub Actions 的 `pypi` environment secret 中，并通过该 environment 的 required reviewers 与 tag restrictions 收口 release 权限。
