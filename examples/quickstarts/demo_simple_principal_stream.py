"""
Demo: minimal principal streaming example.

Goals:
- No external tools (no web_search / openapi / etc).
- Demonstrate Inbox + Wakeup flow.
- Print core NATS streaming chunks: cg.v1r3.{project}.{channel}.str.agent.{agent_id}.chunk
- Wait for completion event: cg.v1r3.{project}.{channel}.evt.agent.{agent_id}.task

Notes:
- Worker only emits streaming chunks for LLM delta.content (chunk_type="content").
- If the model returns empty tool_calls, no chunk may be visible.
- Completion depends on built-in tool `submit_result` (the profile prompt below requires it).
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, UTC
from typing import Any, Dict, Optional

import uuid6

from core.utp_protocol import Card, FieldsSchemaContent, JsonContent, TextContent
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore
from scripts.utils.config import load_toml_config, require_cardbox_dsn
from services.pmo.agent_lifecycle import CreateAgentSpec, ensure_agent_ready

async def _create_profile_box(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    model: str,
    provider: str,
    temperature: float,
    stream: bool,
) -> str:
    profile_card_id = uuid6.uuid7().hex
    profile = Card(
        card_id=profile_card_id,
        project_id=project_id,
        type="sys.profile",
        content=JsonContent(data={
            "name": "PrincipalStreamDebug",
            "worker_target": "worker_generic",
            "tags": ["principal"],
            "description": "Minimal principal config for streaming debug without external tools.",
            "delegation_policy": {"target_tags": [], "target_profiles": []},
            "allowed_tools": [],
            "llm_config": {
                "save_reasoning_content": True,
                "model": model,
                "temperature": temperature,
                "stream": stream,
                "provider_options": {
                    "custom_llm_provider": provider,
                    "thinking": {"type": "enabled", "budget_tokens": 1024},
                },
            },
            "system_prompt_template": (
                "You are a principal agent.\n"
                "Answer the user question directly.\n"
                "Do not call any external tools.\n"
                "After answering, call submit_result exactly once to end this turn.\n"
                "In submit_result, fill required fields with plain text only.\n"
            ),
        }),
        created_at=datetime.now(UTC),
        author_id="user_demo",
        metadata={"role": "system"},
    )
    await cardbox.save_card(profile)
    profile_box_id = await cardbox.save_box([profile_card_id], project_id=project_id)
    return str(profile_box_id)


async def _create_context_box(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    question: str,
    fields_mode: str,
) -> str:
    instruction_card_id = uuid6.uuid7().hex
    instruction = Card(
        card_id=instruction_card_id,
        project_id=project_id,
        type="task.instruction",
        content=TextContent(text=question),
        created_at=datetime.now(UTC),
        author_id="user_demo",
        metadata={"role": "user"},
    )
    await cardbox.save_card(instruction)

    fields_card_id: Optional[str] = None
    if fields_mode != "none":
        if fields_mode == "minimal":
            required = [{"name": "output", "description": "Primary output (text) directly consumable"}]
        else:
            required = [
                {"name": "summary", "description": "Brief summary of task result (text)"},
                {"name": "output", "description": "Primary output (text) directly consumable"},
                {"name": "sources", "description": "References/sources (optional, text)"},
            ]

        # Ensure submit_result field definitions align with validation requirements.
        fields_card_id = uuid6.uuid7().hex
        fields = Card(
            card_id=fields_card_id,
            project_id=project_id,
            type="task.result_fields",
            content=FieldsSchemaContent(fields=required),
            created_at=datetime.now(UTC),
            author_id="user_demo",
            metadata={"role": "system"},
        )
        await cardbox.save_card(fields)

    card_ids = [instruction_card_id]
    if fields_card_id:
        card_ids.append(fields_card_id)
    box_id = await cardbox.save_box(card_ids, project_id=project_id)
    return str(box_id)


async def _create_followup_context_box(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    previous_context_box_id: str,
    output_box_id: str,
    continue_text: str = "continue",
) -> str:
    continue_card_id = uuid6.uuid7().hex
    continue_card = Card(
        card_id=continue_card_id,
        project_id=project_id,
        type="task.instruction",
        content=TextContent(text=continue_text),
        created_at=datetime.now(UTC),
        author_id="user_demo",
        metadata={"role": "user"},
    )
    await cardbox.save_card(continue_card)

    merged_ids: list[str] = []

    prev_ctx = await cardbox.get_box(previous_context_box_id, project_id=project_id)
    if prev_ctx and prev_ctx.card_ids:
        merged_ids.extend([str(cid) for cid in prev_ctx.card_ids])

    out_box = await cardbox.get_box(output_box_id, project_id=project_id)
    if out_box and out_box.card_ids:
        out_ids = [str(cid) for cid in out_box.card_ids]
        out_cards = await cardbox.get_cards(out_ids, project_id=project_id)
        out_card_map = {str(getattr(c, "card_id", "")): c for c in out_cards or []}
        skipped_deliverables = 0
        for cid in out_ids:
            card = out_card_map.get(cid)
            if card and getattr(card, "type", "") == "task.deliverable":
                skipped_deliverables += 1
                continue
            merged_ids.append(cid)
        if skipped_deliverables:
            print(f"[demo] followup context filtered out {skipped_deliverables} task.deliverable cards")

    merged_ids.append(continue_card_id)

    deduped_ids: list[str] = []
    seen: set[str] = set()
    for cid in merged_ids:
        if cid in seen:
            continue
        seen.add(cid)
        deduped_ids.append(cid)

    box_id = await cardbox.save_box(deduped_ids, project_id=project_id)
    return str(box_id)


async def _run_demo(
    *,
    agent_id: str,
    project_id: str,
    channel_id: str,
    question: str,
    model: str,
    provider: str,
    temperature: float,
    stream: bool,
    timeout_seconds: int,
    fields_mode: str,
) -> None:
    cfg = load_toml_config()
    cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
    dsn = require_cardbox_dsn(cfg, error="cardbox.postgres_dsn missing in config.toml")

    cardbox = CardBoxClient(config=cardbox_cfg)
    await cardbox.init()
    nats = NATSClient(config=(cfg.get("nats", {}) if isinstance(cfg, dict) else {}))
    await nats.connect()

    # Prepare context and output box
    profile_box_id = await _create_profile_box(
        cardbox,
        project_id=project_id,
        model=model,
        provider=provider,
        temperature=temperature,
        stream=stream,
    )
    output_box_id = await cardbox.save_box([], project_id=project_id)

    # Dispatch via AgentDispatcher (Inbox + Wakeup)
    state = StateStore(dsn)
    resource_store = ResourceStore(dsn)
    execution_store = ExecutionStore(dsn)
    await state.open()
    await resource_store.open()
    await execution_store.open()

    headers, _, trace_id = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
    headers = ensure_recursion_depth(headers, default_depth=0)

    # Ensure this agent exists in roster with worker_target to avoid worker_target_missing.
    await ensure_agent_ready(
        resource_store=resource_store,
        state_store=state,
        project_id=project_id,
        agent_id=agent_id,
        spec=CreateAgentSpec(
            profile_box_id=profile_box_id,
            worker_target="worker_generic",
            active_channel_id=channel_id,
            display_name=agent_id,
            owner_agent_id="system",
            tags=["principal"],
            metadata={"source": "demo_simple_principal_stream"},
        ),
        channel_id=channel_id,
        trace_id=trace_id,
    )

    dispatcher = AgentDispatcher(
        resource_store=resource_store,
        state_store=state,
        cardbox=cardbox,
        nats=nats,
    )
    current_turn_id: Optional[str] = None
    print(f"[demo] trace_id={trace_id} traceparent={headers.get('traceparent')}")

    done = asyncio.Event()
    stream_subject = f"cg.v1r3.{project_id}.{channel_id}.str.agent.{agent_id}.chunk"
    task_subject = f"cg.v1r3.{project_id}.{channel_id}.evt.agent.{agent_id}.task"
    step_subject = f"cg.v1r3.{project_id}.{channel_id}.evt.agent.{agent_id}.step"

    async def on_chunk(msg) -> None:
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception:
            return
        if not current_turn_id or data.get("agent_turn_id") != current_turn_id:
            return
        content = str(data.get("content") or "")
        if content:
            print(content, end="", flush=True)

    async def on_task(msg) -> None:
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception:
            return
        if not current_turn_id or data.get("agent_turn_id") != current_turn_id:
            return
        print("\n[demo] Task event: ", json.dumps(data, ensure_ascii=False, indent=2))
        done.set()

    # Core subscription (stream is core; task evt is also visible via core).
    await nats.subscribe_core(stream_subject, on_chunk)
    await nats.subscribe_core(task_subject, on_task)
    async def on_turn(msg) -> None:
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception:
            return
        if not current_turn_id or data.get("agent_turn_id") != current_turn_id:
            return
        phase = data.get("phase")
        if phase in ("completed", "failed"):
            print("\n[demo] Turn event: ", json.dumps(data, ensure_ascii=False, indent=2))

    await nats.subscribe_core(step_subject, on_turn)

    print(f"[demo] Streaming subject: {stream_subject}")
    print(f"[demo] output_box_id: {output_box_id}")

    async def _dispatch_turn(turn_idx: int, *, context_box_id: str, turn_question: str) -> str:
        nonlocal current_turn_id
        context_box = await cardbox.get_box(context_box_id, project_id=project_id)
        context_count = len(context_box.card_ids or []) if context_box else 0
        dispatch_result = await dispatcher.dispatch(
            DispatchRequest(
                project_id=project_id,
                channel_id=channel_id,
                agent_id=agent_id,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=str(output_box_id),
                trace_id=trace_id,
                headers=headers,
            )
        )
        if dispatch_result.status not in ("accepted", "pending") or not dispatch_result.agent_turn_id:
            raise RuntimeError(
                f"Failed to dispatch turn #{turn_idx}: {dispatch_result.status} ({dispatch_result.error_code})"
            )
        current_turn_id = dispatch_result.agent_turn_id
        done.clear()
        print(
            f"\n[demo] Turn #{turn_idx} dispatched agent_id={agent_id} run={current_turn_id} "
            f'question="{turn_question}" context_box_id={context_box_id} context_cards={context_count}'
        )
        await asyncio.wait_for(done.wait(), timeout=timeout_seconds)
        print(f"[demo] Turn #{turn_idx} completed, run={current_turn_id}")
        return current_turn_id

    try:
        first_context_box_id = await _create_context_box(
            cardbox,
            project_id=project_id,
            question=question,
            fields_mode=fields_mode,
        )
        await _dispatch_turn(1, context_box_id=first_context_box_id, turn_question=question)

        second_context_box_id = await _create_followup_context_box(
            cardbox,
            project_id=project_id,
            previous_context_box_id=first_context_box_id,
            output_box_id=str(output_box_id),
            continue_text="continue",
        )
        await _dispatch_turn(2, context_box_id=second_context_box_id, turn_question="continue")
    except asyncio.TimeoutError:
        print(f"\n[demo] Timeout while waiting for evt.agent.*.task (run={current_turn_id})")
    finally:
        await execution_store.close()
        await resource_store.close()
        await state.close(timeout=30.0)
        await nats.close()
        await cardbox.close(timeout=30.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal principal streaming demo without external tools.")
    parser.add_argument("--agent", dest="agent_id", default="principal_stream_debug_01")
    parser.add_argument("--project", dest="project_id", default="proj_mvp_001")
    parser.add_argument("--channel", dest="channel_id", default="public")
    parser.add_argument("--question", required=True, help="Question for the principal agent to answer.")
    parser.add_argument("--model", default="gemini/gemini-2.5-flash")
    parser.add_argument("--provider", default="gemini", help="LiteLLM provider (custom_llm_provider).")
    parser.add_argument("--temperature", type=float, default=1.0)
    parser.add_argument("--no-stream", action="store_true", help="Disable streaming output in llm_config.")
    parser.add_argument(
        "--fields-mode",
        choices=["full", "minimal", "none"],
        default="full",
        help="Validation mode for task.result_fields. Supported: minimal/none (mock-model-friendly).",
    )
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    asyncio.run(
        _run_demo(
            agent_id=args.agent_id,
            project_id=args.project_id,
            channel_id=args.channel_id,
            question=args.question,
            model=args.model,
            provider=args.provider,
            temperature=args.temperature,
            stream=not args.no_stream,
            timeout_seconds=args.timeout,
            fields_mode=args.fields_mode,
        )
    )


if __name__ == "__main__":
    main()
