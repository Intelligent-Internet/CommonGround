"""
Demo: fork_join map-reduce Word Count (online LLM + deterministic tool)

This demo shows:
- principal agent function-calls `fork_join`
- mapper agents call external `word_count` tool in map mode
- principal calls `word_count` tool in reduce mode, then submit_result

Requirements:
- NATS + Postgres + PMO + agent_worker are running
- `uv run -m services.tools.word_count` is running
- LLM model/provider credentials are configured for online inference
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

import uuid6

from core.headers import ensure_recursion_depth
from core.subject import format_subject
from core.trace import ensure_trace_headers
from core.utp_protocol import Card, FieldsSchemaContent, JsonContent, TextContent
from core.utils import safe_str, set_loop_policy
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.project_bootstrap import ensure_project, ensure_schema, seed_tools
from infra.stores import ExecutionStore, ResourceStore, StateStore
from scripts.utils.config import api_base_url, load_toml_config, require_cardbox_dsn
from services.pmo.agent_lifecycle import CreateAgentSpec, ensure_agent_ready

set_loop_policy()

REPO_ROOT = Path(__file__).resolve().parents[2]


async def _upload_yaml_to_api(*, api_url: str, endpoint: str, yaml_path: Path) -> None:
    if not yaml_path.exists():
        raise RuntimeError(f"yaml file not found: {yaml_path}")
    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as client:
        with yaml_path.open("rb") as fh:
            files = {"file": (yaml_path.name, fh, "application/x-yaml")}
            resp = await client.post(endpoint, files=files)
        resp.raise_for_status()


async def _resolve_profile_box_id(*, api_url: str, project_id: str, profile_name: str) -> str:
    encoded = quote(profile_name, safe="")
    async with httpx.AsyncClient(base_url=api_url, timeout=10.0) as client:
        resp = await client.get(f"/projects/{project_id}/profiles/{encoded}")
        if resp.status_code == 404:
            raise RuntimeError(f"profile not found via api: project={project_id} name={profile_name!r}")
        resp.raise_for_status()
        data = resp.json() or {}
        box_id = data.get("profile_box_id")
        if not isinstance(box_id, str) or not box_id:
            raise RuntimeError("profile_box_id missing in api response")
        return box_id


async def _resolve_tool_json(*, api_url: str, project_id: str, tool_name: str) -> Dict[str, Any]:
    encoded = quote(tool_name, safe="")
    async with httpx.AsyncClient(base_url=api_url, timeout=10.0) as client:
        resp = await client.get(f"/projects/{project_id}/tools/{encoded}")
        if resp.status_code == 404:
            raise RuntimeError(f"tool not found via api: project={project_id} name={tool_name!r}")
        resp.raise_for_status()
        data = resp.json() or {}
        return data if isinstance(data, dict) else {}


async def _ensure_profiles_via_api(
    *,
    api_url: str,
    project_id: str,
    principal_profile_yaml: Path,
    mapper_profile_yaml: Path,
) -> None:
    await _upload_yaml_to_api(
        api_url=api_url,
        endpoint=f"/projects/{project_id}/profiles",
        yaml_path=principal_profile_yaml,
    )
    await _upload_yaml_to_api(
        api_url=api_url,
        endpoint=f"/projects/{project_id}/profiles",
        yaml_path=mapper_profile_yaml,
    )


async def _ensure_tool_via_api(
    *,
    api_url: str,
    project_id: str,
    tool_yaml: Path,
) -> None:
    await _upload_yaml_to_api(
        api_url=api_url,
        endpoint=f"/projects/{project_id}/tools",
        yaml_path=tool_yaml,
    )


def _chunk_text(
    *,
    raw_text: str,
    words_per_chunk: int,
    max_chunks: int,
) -> List[Dict[str, Any]]:
    words = [w for w in str(raw_text).split() if w]
    if not words:
        raise RuntimeError("input text is empty after tokenization")

    chunks: List[Dict[str, Any]] = []
    chunk_words = max(1, int(words_per_chunk))
    chunk_cap = max(1, int(max_chunks))

    for idx in range(chunk_cap):
        start = idx * chunk_words
        if start >= len(words):
            break
        end = len(words) if idx == chunk_cap - 1 else min(start + chunk_words, len(words))
        piece = " ".join(words[start:end]).strip()
        if not piece:
            continue
        chunks.append({"chunk_index": idx, "text": piece})
    if not chunks:
        raise RuntimeError("failed to build chunks from input text")
    return chunks


def _build_principal_instruction(
    *,
    chunks: List[Dict[str, Any]],
    mapper_profile_name: str,
    map_top_k: int,
    final_top_k: int,
    fork_join_deadline_seconds: int,
) -> str:
    chunk_json = json.dumps(chunks, ensure_ascii=False, indent=2)
    return (
        "Run a map-reduce word count over CHUNKS_JSON.\n\n"
        "CHUNKS_JSON:\n"
        f"{chunk_json}\n\n"
        "Mandatory fork_join plan:\n"
        "- Call fork_join exactly once.\n"
        "- Create one task per chunk, preserving chunk order.\n"
        f"- Include deadline_seconds={int(fork_join_deadline_seconds)} in the fork_join call.\n"
        f"- For every task use target_strategy=\"new\" and target_ref=\"{mapper_profile_name}\".\n"
        "- Do not invent target_ref aliases (forbidden: chunk_0_result, Associate_WordCount_Mapper_0).\n"
        "- For each task instruction, use this exact template:\n"
        "  WORD_COUNT_MAP\n"
        "  chunk_index=<chunk_index>\n"
        f"  map_top_k={int(map_top_k)}\n"
        "  text=<chunk text>\n\n"
        "After resume:\n"
        "- Never call fork_join again.\n"
        "- Parse each successful task summary JSON and collect counts_map items.\n"
        "- If counts_map-bearing items is empty, DO NOT call word_count reduce.\n"
        "- In that empty-items case, immediately submit_result with status=\"failed\" and clear reason "
        "(e.g., fork_join timeout or no successful map outputs).\n"
        "- Call word_count exactly once in reduce mode (no retry):\n"
        "  mode=reduce\n"
        "  items=<array of counts_map-bearing objects>\n"
        f"  top_k={int(final_top_k)}\n"
        f"- Use the word_count reduce result as final ranking (top {int(final_top_k)}).\n"
        "- Immediately submit_result after reduce; do not call any extra tools.\n"
        "- If some tasks failed, still provide a partial result.\n\n"
        "For final submit_result:\n"
        "- summary: short sentence including status and total chunks.\n"
        "- output: JSON string with keys "
        "{\"top_words\": [{\"word\": str, \"count\": int}], "
        "\"counts_map\": {\"word\": int}, \"total_unique_words\": int, \"chunks\": int, \"status\": str}\n"
        "- sources: empty string\n"
    )


async def _create_context_box(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    instruction: str,
) -> str:
    instruction_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="task.instruction",
        content=TextContent(text=instruction),
        created_at=datetime.now(UTC),
        author_id="demo.word_count",
        metadata={"role": "user"},
    )
    await cardbox.save_card(instruction_card)

    result_fields_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="task.result_fields",
        content=FieldsSchemaContent(
            fields=[
                {"name": "summary", "description": "short status summary"},
                {"name": "output", "description": "final word-count json"},
                {"name": "sources", "description": "optional sources"},
            ]
        ),
        created_at=datetime.now(UTC),
        author_id="demo.word_count",
        metadata={"role": "system"},
    )
    await cardbox.save_card(result_fields_card)

    context_box_id = await cardbox.save_box(
        [instruction_card.card_id, result_fields_card.card_id],
        project_id=project_id,
    )
    return str(context_box_id)


async def _load_deliverable(
    *,
    cardbox: CardBoxClient,
    project_id: str,
    deliverable_card_id: Optional[str],
    output_box_id: Optional[str],
) -> Optional[Card]:
    if deliverable_card_id:
        cards = await cardbox.get_cards([str(deliverable_card_id)], project_id=project_id)
        if cards:
            return cards[0]
    if not output_box_id:
        return None
    box = await cardbox.get_box(str(output_box_id), project_id=project_id)
    if not box or not box.card_ids:
        return None
    cards = await cardbox.get_cards(list(box.card_ids), project_id=project_id)
    for card in reversed(cards):
        if getattr(card, "type", "") == "task.deliverable":
            return card
    return None


def _render_deliverable_content(card: Card) -> str:
    content = getattr(card, "content", None)
    if isinstance(content, TextContent):
        return content.text
    if isinstance(content, JsonContent):
        data = content.data
        if isinstance(data, dict) and isinstance(data.get("fields"), list):
            fields = data.get("fields") or []
            output_field = next(
                (
                    f for f in fields
                    if isinstance(f, dict) and safe_str(f.get("name")).strip().lower() == "output"
                ),
                None,
            )
            if isinstance(output_field, dict):
                return safe_str(output_field.get("value"))
        return json.dumps(data, ensure_ascii=False, indent=2)
    try:
        return json.dumps(content, ensure_ascii=False, indent=2)
    except Exception:  # noqa: BLE001
        return str(content)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a minimal fork_join map-reduce Word Count demo (online LLM)."
    )
    parser.add_argument("--project", default="proj_demo_word_count")
    parser.add_argument("--channel", default="public")
    parser.add_argument("--project-title", default="Word Count Demo")
    parser.add_argument("--owner-id", default="user_demo")
    parser.add_argument("--agent-id", default="principal_word_count_demo")
    parser.add_argument("--principal-profile-name", default="Principal_WordCount")
    parser.add_argument("--mapper-profile-name", default="Associate_WordCount_Mapper")
    parser.add_argument("--word-count-tool-name", default="word_count")
    parser.add_argument("--api-url", default=None)
    parser.add_argument(
        "--principal-profile-yaml",
        default=str(REPO_ROOT / "examples" / "profiles" / "principal_word_count.yaml"),
    )
    parser.add_argument(
        "--mapper-profile-yaml",
        default=str(REPO_ROOT / "examples" / "profiles" / "associate_word_count_mapper.yaml"),
    )
    parser.add_argument(
        "--word-count-tool-yaml",
        default=str(REPO_ROOT / "examples" / "tools" / "word_count_tool.yaml"),
    )
    parser.add_argument(
        "--no-ensure-profiles",
        action="store_true",
        help="Do not upload demo profile YAMLs before running.",
    )
    parser.add_argument(
        "--no-ensure-word-count-tool",
        action="store_true",
        help="Do not upload word_count tool YAML before running.",
    )
    parser.add_argument("--words-per-chunk", type=int, default=80)
    parser.add_argument("--max-chunks", type=int, default=4)
    parser.add_argument("--map-top-k", type=int, default=20)
    parser.add_argument("--final-top-k", type=int, default=20)
    parser.add_argument("--fork-join-deadline-seconds", type=int, default=120)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="run schema init SQL before demo (normally not needed on initialized envs)",
    )
    parser.add_argument(
        "--text-file",
        default=None,
        help="Path to input text file for word count.",
    )
    parser.add_argument(
        "text",
        nargs="?",
        default=None,
        help="Inline input text (optional; use --text-file for file input).",
    )
    args = parser.parse_args()

    raw_text = ""
    text_file = safe_str(args.text_file)
    inline_text = args.text if isinstance(args.text, str) else None
    if text_file:
        file_path = Path(text_file)
        if not file_path.exists() or not file_path.is_file():
            raise RuntimeError(f"text file not found: {file_path}")
        raw_text = file_path.read_text(encoding="utf-8")
    elif inline_text and inline_text.strip():
        raw_text = inline_text
    else:
        raise RuntimeError("provide either --text-file <path> or inline text")

    chunks = _chunk_text(
        raw_text=raw_text,
        words_per_chunk=max(1, int(args.words_per_chunk)),
        max_chunks=max(1, int(args.max_chunks)),
    )
    print(f"[demo] chunked text into {len(chunks)} chunk(s)")

    cfg = load_toml_config()
    if not cfg:
        raise RuntimeError("config.toml not found")
    dsn = require_cardbox_dsn(cfg, error="cardbox.postgres_dsn missing in config.toml")
    resolved_api_url = str(args.api_url or api_base_url(cfg))

    cardbox = CardBoxClient(config=cfg.get("cardbox", {}))
    nats = NATSClient(config=cfg.get("nats", {}))
    state_store = StateStore(dsn)
    resource_store = ResourceStore(dsn)
    execution_store = ExecutionStore(dsn)
    await cardbox.init()
    await nats.connect()
    await state_store.open()
    await resource_store.open()
    await execution_store.open()

    try:
        if args.ensure_schema:
            print("[demo] ensuring schema...")
            await ensure_schema(state_store.pool)

        await ensure_project(
            state_store.pool,
            project_id=str(args.project),
            title=str(args.project_title),
            owner_id=str(args.owner_id),
        )
        await seed_tools(state_store.pool, str(args.project))
        print("[demo] project ready and tools bootstrapped")

        principal_yaml = Path(str(args.principal_profile_yaml))
        mapper_yaml = Path(str(args.mapper_profile_yaml))
        word_count_tool_yaml = Path(str(args.word_count_tool_yaml))
        if not args.no_ensure_profiles:
            print(
                "[demo] uploading profiles via API: "
                f"principal={principal_yaml} mapper={mapper_yaml}"
            )
            await _ensure_profiles_via_api(
                api_url=resolved_api_url,
                project_id=str(args.project),
                principal_profile_yaml=principal_yaml,
                mapper_profile_yaml=mapper_yaml,
            )
        if not args.no_ensure_word_count_tool:
            print(f"[demo] uploading tool via API: word_count={word_count_tool_yaml}")
            await _ensure_tool_via_api(
                api_url=resolved_api_url,
                project_id=str(args.project),
                tool_yaml=word_count_tool_yaml,
            )

        principal_profile_box_id = await _resolve_profile_box_id(
            api_url=resolved_api_url,
            project_id=str(args.project),
            profile_name=str(args.principal_profile_name),
        )
        mapper_profile_box_id = await _resolve_profile_box_id(
            api_url=resolved_api_url,
            project_id=str(args.project),
            profile_name=str(args.mapper_profile_name),
        )
        word_count_tool = await _resolve_tool_json(
            api_url=resolved_api_url,
            project_id=str(args.project),
            tool_name=str(args.word_count_tool_name),
        )
        tool_subject = safe_str(word_count_tool.get("target_subject"))
        if not tool_subject:
            tool_subject = "(missing target_subject)"
        print(
            "[demo] profiles ready: "
            f"principal={args.principal_profile_name}({principal_profile_box_id}) "
            f"mapper={args.mapper_profile_name}({mapper_profile_box_id})"
        )
        print(
            f"[demo] tool ready: name={args.word_count_tool_name} "
            f"subject={tool_subject}"
        )

        instruction = _build_principal_instruction(
            chunks=chunks,
            mapper_profile_name=str(args.mapper_profile_name),
            map_top_k=max(1, int(args.map_top_k)),
            final_top_k=max(1, int(args.final_top_k)),
            fork_join_deadline_seconds=max(10, int(args.fork_join_deadline_seconds)),
        )
        context_box_id = await _create_context_box(
            cardbox,
            project_id=str(args.project),
            instruction=instruction,
        )
        output_box_id = str(await cardbox.save_box([], project_id=str(args.project)))

        headers, _, trace_id = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
        headers = ensure_recursion_depth(headers, default_depth=0)

        await ensure_agent_ready(
            resource_store=resource_store,
            state_store=state_store,
            project_id=str(args.project),
            agent_id=str(args.agent_id),
            spec=CreateAgentSpec(
                profile_box_id=str(principal_profile_box_id),
                worker_target="worker_generic",
                active_channel_id=str(args.channel),
                display_name=str(args.agent_id),
                owner_agent_id=str(args.owner_id),
                tags=["principal"],
                metadata={
                    "demo": "fork_join_word_count",
                    "profile_name": str(args.principal_profile_name),
                },
            ),
            channel_id=str(args.channel),
            trace_id=trace_id,
        )

        dispatcher = AgentDispatcher(
            resource_store=resource_store,
            state_store=state_store,
            cardbox=cardbox,
            nats=nats,
        )

        done = asyncio.Event()
        agent_turn_id: Optional[str] = None
        task_payload: Dict[str, Any] = {}

        stream_subject = format_subject(
            str(args.project),
            str(args.channel),
            "str",
            "agent",
            str(args.agent_id),
            "chunk",
        )
        task_subject = format_subject(
            str(args.project),
            str(args.channel),
            "evt",
            "agent",
            str(args.agent_id),
            "task",
        )

        async def _on_chunk(msg) -> None:
            if not agent_turn_id:
                return
            try:
                data = json.loads(msg.data.decode("utf-8"))
            except Exception:  # noqa: BLE001
                return
            if safe_str(data.get("agent_turn_id")) != agent_turn_id:
                return
            content = safe_str(data.get("content"))
            if content:
                print(content, end="", flush=True)

        async def _on_task(msg) -> None:
            nonlocal task_payload
            if not agent_turn_id:
                return
            try:
                data = json.loads(msg.data.decode("utf-8"))
            except Exception:  # noqa: BLE001
                return
            if safe_str(data.get("agent_turn_id")) != agent_turn_id:
                return
            task_payload = data if isinstance(data, dict) else {}
            done.set()

        await nats.subscribe_core(stream_subject, _on_chunk)
        await nats.subscribe_core(task_subject, _on_task)

        print("[demo] dispatching principal turn...")
        dispatch_result = await dispatcher.dispatch(
            DispatchRequest(
                project_id=str(args.project),
                channel_id=str(args.channel),
                agent_id=str(args.agent_id),
                profile_box_id=str(principal_profile_box_id),
                context_box_id=str(context_box_id),
                output_box_id=str(output_box_id),
                trace_id=trace_id,
                headers=headers,
            )
        )
        if dispatch_result.status not in ("accepted", "pending") or not dispatch_result.agent_turn_id:
            raise RuntimeError(
                f"dispatch failed: status={dispatch_result.status} error={dispatch_result.error_code}"
            )
        agent_turn_id = safe_str(dispatch_result.agent_turn_id)
        print(f"[demo] turn started: agent_turn_id={agent_turn_id} trace_id={trace_id}")

        try:
            await asyncio.wait_for(done.wait(), timeout=float(args.timeout_seconds))
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"timed out after {args.timeout_seconds}s waiting for task completion") from exc

        print("\n[demo] task event:", json.dumps(task_payload, ensure_ascii=False, indent=2))
        deliverable = await _load_deliverable(
            cardbox=cardbox,
            project_id=str(args.project),
            deliverable_card_id=safe_str(task_payload.get("deliverable_card_id")) or None,
            output_box_id=safe_str(task_payload.get("output_box_id")) or output_box_id,
        )
        if not deliverable:
            raise RuntimeError("task completed but deliverable card was not found")

        print("\n[demo] final deliverable:\n")
        print(_render_deliverable_content(deliverable))

    finally:
        await execution_store.close()
        await resource_store.close()
        await state_store.close()
        await nats.close()
        await cardbox.close(timeout=30.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
