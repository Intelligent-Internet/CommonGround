# What Is CommonGround Kernel?

CommonGround Kernel is a constitutional ledger kernel for human-agent and multi-Agent collaboration.

It is built on this first principle:

> Assume nothing beyond what constraints demand.

It is not primarily about how to call a model. It addresses a more basic problem:

> When multiple Agents, humans, tools, and external runtimes work together on real tasks, which work facts must be stably preserved so work can be handed off, recovered, audited, understood later, reused, and learned from?

CommonGround's answer is:

> Use Agent, Turn, and Turn-owned semantics to establish public facts, so real work moves from a temporary session into a traceable, auditable, explainable, reusable work record.

Short version:

> CommonGround turns agent work into durable common ground.

The v3r1 open-source line releases the Ledger Kernel first. The current service/API implementation version is `v3r1`.

## Why CommonGround Is Needed

The hard part of multi-Agent collaboration is usually not whether the system can send the next message. The hard part is that facts scatter quickly once work crosses multiple subjects.

Common questions include:

- Who is the stable executor?
- Where is the boundary of one task?
- Who has authority to advance, suspend, resume, or end the work?
- What exactly did one Agent hand to another?
- What exactly did a child Agent return as deliverable?
- Did the parent absorb the child result, and when and how?
- Where are the key intermediate judgments, observations, validations, failures, and artifacts retained?
- How can a later human or Agent audit the work?
- How can the organization distill reusable knowledge from this real work?

If these questions are maintained only through runtime memory, chat logs, process logs, notifications, shared work directories, or a central scheduler, the system quickly loses recoverability and explainability.

CommonGround's goal is to converge the least replaceable and most truth-sensitive parts of collaboration into a small, stable set of public fact objects.

## First Principle

CommonGround should assume nothing beyond what constraints demand.

This principle explains why the v3r1 kernel is deliberately small. CommonGround does not assume it owns every Agent, runtime, scheduler, memory, topology, workflow, or business decision. It only models the minimum durable facts needed for collaboration to remain recoverable, auditable, and explainable.

The kernel treats Agent identity, Turn boundaries, Turn-owned semantics, authority boundaries, lifecycle, and causal lineage as constitutional concerns. Other concerns should live in explicit upper layers unless sustained constraints force them into the kernel.

## Minimal Model

CommonGround Kernel has three minimal legal objects:

- **Agent**: a stable logical executor. It may be backed by an LLM, human, service, script, external runtime, or hybrid system, but in CommonGround it is first a formal subject that can be delegated to, held responsible, and audited.
- **Turn**: the minimum durable work boundary. A Turn expresses a clear unit of work, delegation, interaction, or recovery loop, not a synchronous reply or temporary session.
- **Complete semantics**: the semantic-fact boundary formally owned by a Turn. It carries the Turn's input, actively absorbed observations, public process records, final deliverable, and termination reason.

Together:

> Agent bears responsibility, Turn carries work, complete semantics preserves the public facts of that work.

## Turn Is The Work Container

In CommonGround, a Turn is not an ordinary request/response.

A Turn is the formal container for work. Around one Turn, the system can stably answer:

- Where did this work come from?
- Which Agent was it assigned to?
- What lifecycle state is it in?
- Who currently holds the execution claim?
- Which child Turns did it derive?
- Which inputs and public records has it received?
- What did it finally deliver?
- Why did it fail, stop, or complete?

When a task crosses multiple Agents, each child is also its own Turn. A child has its own input, process records, final deliverable, and lifecycle. The parent can only observe child facts, then a legal subject in the parent's control boundary decides whether to absorb them.

This avoids an implicit shared session pool and prevents children from silently rewriting parent work facts.

## Ledger Kernel Today, Memory Abstraction Next

CommonGround is not only about finishing tasks. v3r1 open-sources the Ledger Kernel first.

Real work naturally produces material that an organization may reuse later:

- key context;
- the reason for choosing an Agent;
- constraints passed during handoff;
- results returned by children;
- observations adopted by the parent;
- execution checkpoints;
- validation results;
- artifact references;
- failure paths;
- final deliverables.

CommonGround's long-term direction is to retain these public work materials around Turns so current Agents, later Agents, humans, LLM-enabled Agents, external systems, and Knowledge Team can read, audit, interpret, summarize, and distill them.

The boundary matters:

- Agent-boundary I/O, such as root input, dispatch input, child deliverable, and parent final deliverable, must be durably retained.
- Agent-internal public work-process knowledge, such as observations, judgments, validation, failures, and handoff rationale, should be reported to the Turn on a best-effort basis under Agent behavior guidance.
- Agent-private memory, runtime-local scratchpads, token-level reasoning, and chain-of-thought are not CommonGround truth by default.
- Knowledge interpretation and distillation belong to humans, LLM-enabled Agents, external systems, projections, and Knowledge Team, not the kernel.

CommonGround is therefore not a memory dump for all history.

It provides:

> Turn-owned, traceable, auditable, explainable public work facts.

These facts can become raw material for Agent self-review, other Agents learning similar tasks, organizational knowledge distillation, routines, playbooks, cases, and evals. They do not gain formal effect merely because they were stored.

This is memory-ready, not memory-complete. Higher-level memory abstraction, search, dossier surfaces, routines, playbooks, and learning workflows can be built on top of durable facts, but they are not kernel truth by default.

## What CommonGround Is Responsible For

The CommonGround Kernel stores and coordinates the minimum public facts:

- Agent identity;
- Turn birth, lifecycle, and terminal outcome;
- claim, heartbeat, and fencing;
- Turn-owned semantic records;
- dispatch and child lineage;
- final result;
- ledger, feed, and pull-first observation basis.

It ensures these facts can be recovered, observed, and audited.

Around the kernel, upper layers can build:

- projections and dashboards;
- Agent directory;
- Turn offers;
- runtime companions;
- management portal;
- Knowledge Team;
- search, index, and dossier surfaces;
- organization learning substrate.

These upper layers must not define kernel truth in reverse.

## What CommonGround Is Not

CommonGround Kernel is explicitly not:

- an Agent brain;
- an Agent-private memory system;
- an LLM reasoning recorder;
- a runtime, container, or workspace manager;
- a PMO or central scheduler;
- an automatic routing engine;
- a message bus;
- an event system whose correctness depends on push notifications;
- a knowledge interpretation or automatic promotion engine.

The kernel does not reason for Agents, make strategy judgments for the organization, or grant authorization, identity continuity, contract effect, or policy effect merely because a record was persisted.

Any material that needs machine-authoritative effect must be explicitly modeled.

## A Typical Workflow

A simple CommonGround workflow looks like:

1. An external request enters the system and becomes a root Turn.
2. The target Agent claims the Turn.
3. The Agent reads Turn context and advances work.
4. If collaboration is needed, the parent Agent explicitly dispatches a child Turn.
5. The child Agent claims the child Turn, completes its own work, and writes a final deliverable.
6. The parent observes the child result through durable feed, lineage, or snapshot.
7. The parent decides whether to absorb the child result and continues work.
8. The parent finishes the root Turn and writes a terminal result.
9. Humans, Agents, or Knowledge Team later read closed Turns for audit, interpretation, retrospective analysis, or knowledge distillation.

The important point is not who received a notification.

> Even if notifications are lost, runtimes restart, or processes move, the system can return from durable facts to correct work judgment.

## What Is In This Repository

The current v3r1 mainline contains:

- `CommonGround/`: contracts, kernel, infra, SDK, adapters, and service implementation.
- `Integrations/nanobot/`: the main external runtime / companion / dynamic provisioning integration.
- `examples/`: BYOA, work-memory reporter, skill, and NanoBot integration examples.
- `tests/`: the real regression entrypoint.
- `docs/`: constitution, three-plane model, design review, Turn work-knowledge vision, design docs, and guides.
- `CG-Cardbox/`: CardBox submodule consumed by the PostgreSQL schema reset path.

The current mainline includes:

- agent-only execution model;
- canonical dispatch;
- claim, heartbeat, and finish;
- suspend and resume;
- child dispatch, lineage, and parent observation;
- Turn-owned semantic records;
- projection read surface;
- NanoBot external runtime integration;
- multi-hop local-only orchestration demo.

## Continue Reading

For a first pass, read:

1. [how-to-read-this-repo.md](how-to-read-this-repo.md)
2. [../01-constitution.md](../01-constitution.md)
3. [../02-three-plane-model.md](../02-three-plane-model.md)
4. [../03-design-review-principles.md](../03-design-review-principles.md)
5. [../cg-history.md](../cg-history.md)
6. [../release-notes.md](../release-notes.md)

If you remember only one sentence:

> CommonGround Kernel turns real multi-Agent work into durable public facts and reusable work knowledge, while assuming nothing beyond what constraints demand.
