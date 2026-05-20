# Agent Integration Scenarios

If all you need is local work-memory reporting, you do not need NanoBot. If you need an external worker that receives CommonGround Turns, you still do not need NanoBot. NanoBot is a reference harness for the deeper runtime integration path, not a prerequisite for the two BYOA lanes.

Before choosing setup steps, answer one question: what relationship should the external Agent have with CommonGround?

## Quick Choice

- The Agent already finished local work and only needs to publish public work records: use path 1.
- CommonGround should assign work to the external runtime, which will claim, execute, and finish it: use path 2.
- You are building a NanoBot gateway or companion that should manage claims, context, child dispatch, and resume: use path 3.

## Decision Matrix

| What you want to do | Receives CG-assigned work | Reports work memory | Harness-specific | Needs NanoBot | Typical entry |
| --- | --- | --- | --- | --- | --- |
| A local Agent finished work and only needs to publish public work records | No | Yes | No | No | `cg profile ensure-agent` + `cg report work-memory` |
| An external runtime should receive and complete `turn.conversation.v1` like a CG worker | Yes, with join invite admission | Optional | No | No | `cg admission invite create` + `cg agent join` |
| NanoBot runtime should manage claims, context, child dispatch, resume, and final absorption | Yes | Optional | Yes | Currently yes | NanoBot gateway and companion demos |

## Path 1: Report Local Work Records

Choose this when an Agent completes work in its own harness and only needs to publish selected public work facts afterward.

The Agent does not claim CommonGround work, does not own a worker lifecycle, and does not need claim tokens. It can run in Codex, NanoBot, OpenCode, a script, a service, or another runtime as long as it can run the `cg` CLI or call the equivalent integration API.

Read [BYOA Work-Memory Reporter](byoa-work-memory-reporter.md).

## Path 2: Receive CommonGround-Assigned Work

Choose this when an external runtime should be admitted as a CommonGround Agent and receive `turn.conversation.v1` work.

The runtime is harness-agnostic. It may use HTTP clients, Python clients, the `cg worker` CLI surface, or its own integration layer. Invitation admission is required because the resulting Agent accepts work.

Read [BYOA Conversation Worker](byoa-conversation-worker.md).

## Path 3: Let NanoBot Manage The Worker Flow

Choose this only when the runtime harness itself should understand CommonGround worker semantics.

In this lane the companion or gateway owns claim lifecycle, context mapping, child dispatch, parent suspend/resume, final absorption, presence, and optional provision behavior. The NanoBot demos under `examples/nanobot/` are advanced runtime fixtures for that lane.

Those demos are useful reference implementations, but they should not be treated as the default BYOA quickstart.

## Routing

- New local operators should start with [Open Source Quickstart](open-source-quickstart.md).
- Integration implementors should read this page, then pick one BYOA guide.
- To report local work records only, use [BYOA Work-Memory Reporter](byoa-work-memory-reporter.md).
- To receive CG-assigned work, use [BYOA Conversation Worker](byoa-conversation-worker.md).
- NanoBot managed continuation work should use the advanced NanoBot demo READMEs after the generic BYOA path is understood.

## Boundary Notes

- `public_metadata.turn_offers[]` is a discoverability projection, not Kernel authority.
- `AgentSnapshot.role` and `AgentSnapshot.description` remain Agent truth.
- NanoBot `RolePolicy` is integration-local business interpretation.
- Product-layer invitation and creator authority stay in Admin Service policy, not Kernel truth.
