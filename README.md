# CommonGround Kernel

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-supported-336791.svg)](https://www.postgresql.org/)
[![Release: v3r1--preview](https://img.shields.io/badge/release-v3r1--preview-orange.svg)](docs/en/release-notes.md)
[![API: v3r1](https://img.shields.io/badge/API-v3r1-555555.svg)](docs/en/reference/http.md)
[![Discord](https://img.shields.io/badge/Discord-Join%20Community-7289DA.svg)](https://discord.com/invite/intelligentinternet)

[Chinese](README_CN.md) | [English](README.md)

![CommonGround Kernel banner](docs/assets/commonground-kernel-banner.png)

> No agent works alone for long. Real work crosses people, tools, services, runtimes, and other agents.

**CommonGround Kernel v3r1 preview** is an open-source ground layer for real human-agent and multi-agent work. It keeps agent work durable, public, and ready for the next human, agent, tool, or external runtime that picks it up.

CommonGround turns agent work into durable common ground. It is not just durable logging. It is the retained public substrate that lets humans and agents rebuild common ground across handoffs, pauses, and changing runtimes. It keeps the shared task state, handoff facts, boundary inputs and outputs, evidence, and recovery context that future participants need to continue the work.

It is also a small **constitutional ledger kernel**: it defines the minimum public facts, work boundaries, semantic ownership, and causal relationships needed for independent participants to cooperate without being absorbed into one central runtime.

**Independent agents at the edges. Durable public work records at the kernel.**

---

## What Is CommonGround?

CommonGround is the shared work substrate underneath agent runtimes, memory systems, and orchestration frameworks.

Here, **common ground** means the public basis for continued collaboration: the shared task state, shared handoff facts, shared reasons and evidence fit for later recovery, and shared boundary inputs and outputs that make work inspectable and resumable after the original session disappears.

It preserves the public record of work:

- what was requested;
- what was handed off;
- what came back;
- what was delivered;
- what future work can rely on.

It does not try to own every workflow, runtime, memory system, collaboration surface, or business decision. Instead, CommonGround preserves the minimum durable facts that independent humans, agents, tools, services, and external runtimes need in order to coordinate.

In the v3r1 preview, that foundation is exposed as the **Ledger Kernel**: a small kernel for Agent identity, Turn lifecycle, Turn-owned public semantics, work records, claim fencing, causal lineage, and pull-first inspection.

## Why It Exists

AI agents are moving from answering questions to participating in real work.

But real work rarely fits inside one prompt. It crosses people, agents, tools, services, and time. An agent researches a market. Another drafts a strategy memo. A human reviews what happened later. A coding agent picks up part of the work. A future agent tries to reuse the result.

When that work only lives in chat windows, temporary sessions, tool logs, and private runtime context, every next participant starts from fragments.

The deeper question is not only whether an agent can produce a good answer, but whether that work can become part of shared memory, reusable experience, review, and the next round of work.

CommonGround gives that work a durable shared record, so later humans and agents can inspect it, recover it, audit it, reuse it, and build on it.

## First Principles

CommonGround follows one first principle:

> Assume nothing beyond what constraints demand.

That principle keeps the kernel small. CommonGround should not freeze a current product shape, runtime design, topology preference, memory strategy, or orchestration style into permanent system ontology.

The v3r1 preview kernel is organized around a few constitutional commitments:

- **Agent is the stable logical executor**: it may be backed by an LLM, human, service, script, external runtime, organization process, or hybrid system.
- **Turn is the minimum durable work boundary**: a unit of work, delegation, interaction, or recovery loop, not merely a chat reply or temporary session.
- **Turn-owned semantics preserve public work facts**: inputs, selected observations, process records, handoffs, deliverables, termination reasons, and artifact references belong to the Turn that owns them.
- **Causality must be explicit enough to recover work**: parent/child work, completion, observation, and absorption should remain inspectable after sessions, runtimes, or notifications disappear.
- **Stored history is not automatic private memory or business effect**: higher layers may interpret, summarize, absorb, or reuse records, but the kernel does not make those judgments by default.

## From Earlier Previews To v3r1 Preview

Earlier CommonGround previews emphasized orchestration, swarm-like execution, and multi-agent collaboration.

The v3r1 preview keeps that direction, but shifts the center of gravity.

Orchestration matters because execution is where collaboration becomes real. Every time work is delegated, resumed, completed, or handed off across agents, it creates boundaries that should survive the session and the orchestrator that produced them.

The foundation became more precise because the vision became larger. CommonGround does not need to own every workflow, runtime, or memory system; it needs to preserve the public facts of work that let independent participants coordinate without collapsing into one central system.

The commons is the destination. The kernel is the first durable ground.

## What The Kernel Provides

CommonGround Kernel currently provides:

- **Agent identity**: a stable logical executor, backed by an LLM, human, service, script, external runtime, organization process, or hybrid system.
- **Turn boundaries**: durable work boundaries for tasks, delegations, interactions, and recovery loops.
- **Turn-owned public semantics**: input, selected observations, handoffs, process records, final deliverables, termination reasons, and artifact references owned by the Turn.
- **Claim fencing and lifecycle**: claim, heartbeat, renew, suspend, resume, stop, finish, and lease-expiry handling.
- **Causal lineage**: parent/child Turn relationships, child completion, parent observation, and explicit absorption.
- **Ledger and feed**: durable public facts for pull-first recovery, inspection, audit, and projection.
- **Local developer surface**: CLI, HTTP service, Admin Service admission flow, BYOA examples, and NanoBot reference fixtures.

## What CommonGround Is Not

CommonGround Kernel is intentionally focused.
It is not a full agent memory product, an agent-private memory system, a chain-of-thought recorder, an agent runtime, a central scheduler, an automatic orchestration framework, a Slack bot release, or a replacement for your existing agent framework.

Memory systems, orchestration frameworks, review surfaces, knowledge distillation pipelines, shared workspaces, routines, and playbooks can be built on top of CommonGround records. They are not the kernel itself.

CommonGround is **memory-ready, not memory-complete**.

## Breaking Change From v1

CommonGround Kernel v3r1 is not backward-compatible with the earlier public v1 preview. The package layout, runtime assumptions, local setup path, and API surface have changed. Existing v1 integrations should treat v3r1 as a new preview line, not an in-place upgrade.

The historical v1 source remains available on the [`legacy/v1`](https://github.com/Intelligent-Internet/CommonGround/tree/legacy/v1) branch. Existing v1 tags, including `v1r4` and `v1r4-hotfix`, remain unchanged.

## Current Release Scope

Status: **Initial Open Source Preview - v3r1**

Current service/API implementation: **v3r1**

The v3r1 preview focuses on the Ledger Kernel and a local-first developer path.

Supported in this preview:

- PostgreSQL-backed local development and test execution.
- `uv`-based setup.
- Single-port local development with `cg local run`.
- Separated service entrypoints with `cg service run` and `cg admission run`.
- CLI-first workflows for setup, profiles, dispatch, Turn inspection, project observation, reports, worker lifecycle, and admission.
- BYOA work-memory reporting.
- BYOA conversation-worker examples.
- NanoBot integration fixtures as optional advanced references.

`v3r1` is the active preview API prefix used by the service and client code. It is not a long-term compatibility promise.

## Install From PyPI

When a public package release is available:

```bash
uv tool install commonground-kernel
cg --version
```

For local service commands such as `cg local run`, install the server extra:

```bash
uv tool install 'commonground-kernel[server]'
```

## Quickstart

This first loop is intentionally small:

1. run the kernel locally;
2. submit a public work report;
3. inspect the retained work record.

### Prerequisites

- Python `>=3.13`
- `uv`
- PostgreSQL with a writable local database

### 1. Install

```bash
uv tool install 'commonground-kernel[server]'
```

### 2. Prepare The Database

Create a PostgreSQL database, then point `PG_DSN` at it:

```bash
export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

### 3. Seed The Local Project

```bash
cg setup project seed --default-local
cg setup project status --default-local

cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8000
```

This prepares the default local `cg-demo` project and writes local CLI configuration.

### 4. Run The Local Bundle

In a long-running terminal:

```bash
cg local run --project-id cg-demo --host 127.0.0.1 --port 8000
```

This serves:

- CommonGround Service API at `/v3r1`
- Admin Service admission API at `/admin/v1`

Check liveness:

```bash
curl http://127.0.0.1:8000/healthz
```

### 5. Submit A Public Work Report

In another terminal:

```bash
cat > report.json <<'EOF'
{
  "kind": "agent_work_memory_report_manifest.v1",
  "request_id": "local-agent-report-001",
  "summary": "Local work completed and reported.",
  "records": [
    {
      "role": "summary",
      "payload": {
        "summary": "Completed the local task and retained public evidence."
      }
    }
  ]
}
EOF

cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Agent"

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file report.json
```

On success, the CLI prints a JSON envelope. Copy `result.turn.turn_id`.

### 6. Inspect The Retained Turn

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

You should see a closed `turn.work_memory_report.v1` Turn with the submitted manifest, public work record, and final payload.

The full guide is in [Open Source Quickstart](docs/en/guides/open-source-quickstart.md).
If you want a source checkout and development workflow, use [CONTRIBUTING.md](CONTRIBUTING.md).

## Agent Integration Paths

Choose the smallest integration path that matches how your agent should relate to CommonGround.

| Goal | Receives CommonGround-assigned work | Reports public work records | Needs NanoBot | Typical entry |
| --- | --- | --- | --- | --- |
| A local agent finished work and only needs to publish selected public records | No | Yes | No | `cg profile ensure-agent` + `cg report work-memory` |
| An external runtime should receive and complete `turn.conversation.v1` work | Yes | Optional | No | `cg admission invite create` + `cg agent join` |
| A NanoBot gateway or companion should manage claims, context, child dispatch, resume, and final absorption | Yes | Optional | Currently yes | `examples/nanobot/` |

Start with [Agent Integration Scenarios](docs/en/guides/agent-integration-scenarios.md), then choose:

- [BYOA Work-Memory Reporter](docs/en/guides/byoa-work-memory-reporter.md)
- [BYOA Conversation Worker](docs/en/guides/byoa-conversation-worker.md)
- NanoBot advanced runtime fixtures under `examples/nanobot/`

NanoBot is useful as a reference integration, but it is not required for the first two BYOA paths.

## Architecture Overview

```mermaid
flowchart LR
    subgraph External["External humans, agents, tools, and runtimes"]
        CLI["cg CLI"]
        BYOA["BYOA agents"]
        Worker["External workers"]
        NanoBot["NanoBot reference fixtures"]
    end

    subgraph Surface["CommonGround service surface"]
        HTTP["HTTP API /v3r1"]
        Admin["Admin Service /admin/v1"]
        SDK["SDK helpers and adapters"]
    end

    subgraph Kernel["CommonGround Kernel"]
        Agents["Agent identity"]
        Turns["Turn lifecycle"]
        Semantics["Turn-owned semantics"]
        Claims["Claim fencing"]
        Lineage["Causal lineage"]
        Feed["Ledger and feed"]
    end

    subgraph Truth["Durable truth"]
        PG[("PostgreSQL")]
        CardBox[("CardBox schema")]
    end

    CLI --> HTTP
    CLI --> Admin
    BYOA --> HTTP
    Worker --> HTTP
    NanoBot --> HTTP

    HTTP --> SDK
    Admin --> SDK
    SDK --> Kernel
    Kernel --> PG
    Kernel --> CardBox
```

Push notifications, dashboards, summaries, and projections can make work easier to see. They do not replace the correctness baseline: later readers must be able to return to durable facts.

## Repository Layout

```text
CommonGround/          Kernel, contracts, infra adapters, SDK helpers, service surfaces, and CLI
Integrations/nanobot/  Optional advanced runtime / companion / provisioning integration
examples/              BYOA, work-memory reporter, skill, and NanoBot examples
docs/                  Current architecture, guides, reference docs, and design materials
tests/                 Regression coverage and executable contracts
CG-Cardbox/            CardBox submodule consumed by the PostgreSQL schema reset path
```

## Documentation

Start here:

- [Docs index](docs/index.md)
- [English docs](docs/en/index.md)
- [Chinese docs](docs/zh/index.md)

Core reading:

1. [What Is CommonGround Kernel?](docs/en/introduction/what-is-commonground.md)
2. [Open Source Quickstart](docs/en/guides/open-source-quickstart.md)
3. [Agent Integration Scenarios](docs/en/guides/agent-integration-scenarios.md)
4. [Release Notes](docs/en/release-notes.md)
5. [CLI Reference](docs/en/reference/cli.md)
6. [HTTP Reference](docs/en/reference/http.md)
7. [Environment Variables](docs/en/reference/environment.md)

Design references:

1. [Constitution](docs/en/01-constitution.md)
2. [Three-Plane Model](docs/en/02-three-plane-model.md)
3. [Design Review Principles](docs/en/03-design-review-principles.md)

## Development And Tests

For source checkout, development setup, and release/test workflows, read [CONTRIBUTING.md](CONTRIBUTING.md).

## Security And Credentials

CommonGround Agent credentials are bearer secrets.

Do not paste real tokens, provider API keys, private DSNs, local token files, workstation paths, or generated credential output into prompts, issues, PRs, logs, docs, manifests, or test fixtures.

Use placeholders in public material:

```text
<agent_credential_token>
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

Do not report suspected vulnerabilities in public issues. Read [SECURITY.md](SECURITY.md) for the private reporting path.

## What Comes Next

Durable work records are not the destination. They are the raw material.

CommonGround records can become the foundation for:

- memory and review;
- knowledge distillation;
- search and dossier surfaces;
- shared workspaces;
- routines and playbooks;
- orchestration layers;
- organizational learning.

Those higher-level layers should be built on stable public work facts, not on fragile session fragments.

## Contributing

The future of agent collaboration should be shared, inspectable, recoverable, and built in public.

Community contributions are welcome.

Before opening a PR:

- read [CONTRIBUTING.md](CONTRIBUTING.md);
- keep changes scoped;
- update docs when commands, behavior, public contracts, or environment variables change;
- avoid committing local paths, generated runtime output, real credentials, private DSNs, or personal database names;
- treat kernel, lifecycle, semantic-record, service, and truth-schema changes as high-risk design work.

## Community And Support

- Discord: [Join our Discord community](https://discord.com/invite/intelligentinternet)
- Issues: [Open a GitHub issue](https://github.com/Intelligent-Internet/CommonGround/issues)
- Security: read [SECURITY.md](SECURITY.md) before reporting vulnerabilities

## License

CommonGround Kernel is licensed under the [Apache License 2.0](LICENSE).
