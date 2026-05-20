# CG Reference Skill

This directory contains the runtime-neutral reference `cg` skill package.

## Purpose

- `SKILL.md` is the installable skill payload.
- This file is the canonical prompt-facing `cg` skill text for the repository.
- The skill defines how prompt-level agents should use the `cg` CLI.
- The skill does not own worker lifecycle, claims, or heartbeats.
- The current reference skill covers safe requester/read commands, `cg report work-memory`, and selected read-only `cg project ...` discovery commands.

## Source Of Truth

- Keep prompt-facing `cg` command policy authoritative here.
- `.agents/skills/cg/SKILL.md` is a repo-local copy and must stay byte-for-byte aligned with this file.
- Demo runners should install this skill into a workspace at setup time instead of tracking their own demo-local `skills/cg/SKILL.md` copy.
- `cg worker` policy is separate and remains in `.agents/skills/cg-worker/SKILL.md`.

## Runtime Expectations

- `cg` must be available on `PATH`.
- Agent runtimes should load this skill as-is or copy it into their own skill directory.
- Runtime-controlled lifecycle code should use `cg worker` or direct Python APIs instead of this skill.

## Runtime Installation

Any prompt-facing runtime should load this skill by copying this directory to the active skill workspace:

- `<workspace>/skills/cg/`

If `cg` is installed in the CommonGround virtualenv, add that virtualenv's `bin` directory to the runtime command path so the `cg` binary is visible to the agent runtime.

For NanoBot as one optional harness, the config fragment under `examples/nanobot/cg_skill_minimal/` shows the same path setup. Generic BYOA work-memory reporting does not require NanoBot.
