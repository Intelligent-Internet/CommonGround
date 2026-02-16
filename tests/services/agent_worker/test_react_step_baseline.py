import asyncio
import time
from types import SimpleNamespace

import pytest

from core.llm import LLMConfig
from core.status import STATUS_FAILED, STATUS_SUCCESS
from core.utp_protocol import TextContent
from services.agent_worker.deliverable_utils import ensure_terminal_deliverable
from services.agent_worker.models import AgentTurnWorkItem
from services.agent_worker.react_step import ReactStepProcessor
import services.agent_worker.react_step as react_step_module


class _DummyStateStore:
    def __init__(self, *, update_result=True):
        self.update_result = update_result
        self.update_calls = []
        self.finish_calls = []

    async def update(self, **kwargs):
        self.update_calls.append(kwargs)
        return self.update_result

    async def finish_turn_idle(self, **kwargs):
        self.finish_calls.append(kwargs)
        return self.update_result

    async def fetch(self, project_id, agent_id):
        return SimpleNamespace(
            status="dispatched",
            active_agent_turn_id="turn_1",
            turn_epoch=1,
            profile_box_id="profile_1",
            context_box_id="context_1",
            output_box_id="output_1",
            parent_step_id="parent_1",
            trace_id="trace_1",
        )

    async def claim_turn_resume_ledgers(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        lease_owner,
        lease_seconds=30.0,
        limit=50,
    ):
        return []

    async def record_turn_resume_ledger(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id,
        payload=None,
        conn=None,
    ):
        return True


class _DummyStepStore:
    def __init__(self):
        self.update_calls = []

    async def update_step(self, **kwargs):
        self.update_calls.append(kwargs)

    async def insert_step(self, **kwargs):
        self.update_calls.append(kwargs)

class _DummySkillStore:
    async def list_skills(self, *, project_id: str, limit: int = 200, offset: int = 0):
        return []


class _DummyCardBox:
    def __init__(self):
        self.boxes = {}
        self.cards = {}
        self.saved_cards = []
        self.append_calls = []
        self.adapter = object()

    async def get_box(self, box_id, *, project_id, conn=None):
        _ = conn
        return self.boxes.get(str(box_id))

    async def get_cards(self, card_ids, *, project_id, conn=None):
        _ = conn
        return [self.cards[cid] for cid in card_ids if cid in self.cards]

    async def save_card(self, card, conn=None):
        _ = conn
        self.saved_cards.append(card)
        self.cards[card.card_id] = card

    async def save_box(self, card_ids, *, project_id, conn=None):
        _ = conn
        return "request_box_1"

    async def ensure_box_id(self, *, project_id, box_id, conn=None):
        _ = conn
        return str(box_id) if box_id is not None else "output_1"

    async def append_to_box(self, box_id, card_ids, *, project_id, conn=None):
        _ = conn
        key = str(box_id)
        box = self.boxes.get(key)
        if box is None:
            box = SimpleNamespace(card_ids=[])
            self.boxes[key] = box
        box.card_ids.extend(list(card_ids))
        self.append_calls.append((key, list(card_ids), project_id))
        return key


class _DummyNATS:
    def __init__(self):
        self.events = []

    async def publish_event(self, subject, payload, headers=None):
        self.events.append((subject, payload, headers))


def _make_processor(*, cardbox=None, state_store=None, step_store=None, nats=None, max_steps=8) -> ReactStepProcessor:
    return ReactStepProcessor(
        state_store=state_store or _DummyStateStore(),
        step_store=step_store or _DummyStepStore(),
        resource_store=SimpleNamespace(),
        resource_loader=SimpleNamespace(),
        skill_store=_DummySkillStore(),
        cardbox=cardbox or _DummyCardBox(),
        assembler=SimpleNamespace(),
        llm_wrapper=SimpleNamespace(),
        action_handler=SimpleNamespace(),
        nats=nats or _DummyNATS(),
        queue=asyncio.Queue(),
        execution_store=None,
        suspend_timeout_seconds=30.0,
        max_steps=max_steps,
    )


def _make_item() -> AgentTurnWorkItem:
    return AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id="profile_1",
        context_box_id="context_1",
        output_box_id="output_1",
        turn_epoch=1,
        headers={"CG-Recursion-Depth": "0"},
        parent_step_id="parent_1",
        trace_id="trace_1",
    )


def test_normalize_tool_args_parses_json_and_mutates_function_args() -> None:
    tool_call = {"function": {"arguments": "{\"x\": 1}"}}
    parsed = ReactStepProcessor._normalize_tool_args(tool_call)
    assert parsed == {"x": 1}
    assert tool_call["function"]["arguments"] == {"x": 1}


def test_sanitize_provider_options_removes_sensitive_keys() -> None:
    cleaned = ReactStepProcessor._sanitize_provider_options(
        {
            "api_key": "secret",
            "token": "secret2",
            "custom_llm_provider": "openai",
            "temperature": 0.1,
        }
    )
    assert cleaned == {"custom_llm_provider": "openai", "temperature": 0.1}


def test_build_stream_start_metadata_redacts_sensitive_llm_fields() -> None:
    processor = _make_processor()
    item = _make_item()
    profile = SimpleNamespace(
        card_id="profile_card_1",
        name="Planner",
        display_name="Planner Agent",
        tags=["planner", "core"],
        worker_target="agent_worker",
        llm_config=LLMConfig(
            model="gpt-4o-mini",
            api_key="should_not_leak",
            provider_options={"api_key": "remove", "custom_llm_provider": "openai"},
        ),
    )
    metadata = processor._build_stream_start_metadata(item, profile)
    assert metadata["agent_id"] == "agent_1"
    assert metadata["profile_id"] == "profile_card_1"
    assert metadata["llm_config"]["model"] == "gpt-4o-mini"
    assert "api_key" not in metadata["llm_config"]
    assert metadata["llm_config"]["provider_options"] == {"custom_llm_provider": "openai"}


def test_card_metadata_includes_lineage_and_extra() -> None:
    processor = _make_processor()
    item = _make_item()
    metadata = processor._card_metadata(
        item,
        step_id="step_1",
        role="assistant",
        tool_call_id="call_1",
        extra={"k": "v"},
    )
    assert metadata["agent_turn_id"] == "turn_1"
    assert metadata["step_id"] == "step_1"
    assert metadata["role"] == "assistant"
    assert metadata["tool_call_id"] == "call_1"
    assert metadata["trace_id"] == "trace_1"
    assert metadata["parent_step_id"] == "parent_1"
    assert metadata["k"] == "v"


@pytest.mark.asyncio
async def test_persist_thought_card_writes_tool_call_ids_to_step() -> None:
    step_store = _DummyStepStore()
    cardbox = _DummyCardBox()
    processor = _make_processor(cardbox=cardbox, step_store=step_store)
    item = _make_item()
    ctx = react_step_module.TurnContext(
        item=item,
        start_ts=time.monotonic(),
        state=SimpleNamespace(),
    )
    ctx.step_id = "step_1"
    ctx.resp = {
        "content": "thinking",
        "tool_calls": [
            {"id": "tc_1", "function": {"name": "tool_a", "arguments": {}}},
            {"id": "tc_2", "function": {"name": "tool_b", "arguments": {}}},
        ],
    }

    tool_calls = await processor._persist_thought_card(ctx)

    assert [tool_call.get("id") for tool_call in tool_calls] == ["tc_1", "tc_2"]
    assert any(
        call.get("project_id") == "proj_1"
        and call.get("agent_id") == "agent_1"
        and call.get("step_id") == "step_1"
        and call.get("tool_call_ids") == ["tc_1", "tc_2"]
        for call in step_store.update_calls
    )


@pytest.mark.asyncio
async def test_persist_thought_card_saves_reasoning_content_when_enabled() -> None:
    cardbox = _DummyCardBox()
    processor = _make_processor(cardbox=cardbox)
    item = _make_item()
    ctx = react_step_module.TurnContext(
        item=item,
        start_ts=time.monotonic(),
        state=SimpleNamespace(),
    )
    ctx.step_id = "step_1"
    ctx.profile = SimpleNamespace(llm_config=LLMConfig(model="gpt-4o-mini", save_reasoning_content=True))
    ctx.resp = {
        "content": "thinking",
        "reasoning_content": "internal-chain",
        "tool_calls": [],
    }

    await processor._persist_thought_card(ctx)

    assert len(cardbox.saved_cards) == 1
    saved = cardbox.saved_cards[0]
    assert saved.metadata.get("reasoning_content") == "internal-chain"


@pytest.mark.asyncio
async def test_persist_thought_card_saves_reasoning_content_by_default() -> None:
    cardbox = _DummyCardBox()
    processor = _make_processor(cardbox=cardbox)
    item = _make_item()
    ctx = react_step_module.TurnContext(
        item=item,
        start_ts=time.monotonic(),
        state=SimpleNamespace(),
    )
    ctx.step_id = "step_1"
    ctx.profile = SimpleNamespace(llm_config=LLMConfig(model="gpt-4o-mini"))
    ctx.resp = {
        "content": "thinking",
        "reasoning_content": "internal-chain-default",
        "tool_calls": [],
    }

    await processor._persist_thought_card(ctx)

    assert len(cardbox.saved_cards) == 1
    saved = cardbox.saved_cards[0]
    assert saved.metadata.get("reasoning_content") == "internal-chain-default"


@pytest.mark.asyncio
async def test_persist_thought_card_skips_reasoning_content_when_disabled() -> None:
    cardbox = _DummyCardBox()
    processor = _make_processor(cardbox=cardbox)
    item = _make_item()
    ctx = react_step_module.TurnContext(
        item=item,
        start_ts=time.monotonic(),
        state=SimpleNamespace(),
    )
    ctx.step_id = "step_1"
    ctx.profile = SimpleNamespace(llm_config=LLMConfig(model="gpt-4o-mini", save_reasoning_content=False))
    ctx.resp = {
        "content": "thinking",
        "reasoning_content": "internal-chain",
        "tool_calls": [],
    }

    await processor._persist_thought_card(ctx)

    assert len(cardbox.saved_cards) == 1
    saved = cardbox.saved_cards[0]
    assert "reasoning_content" not in saved.metadata


def test_apply_step_budget_controls_warns_when_two_steps_left() -> None:
    processor = _make_processor(max_steps=3)
    item = _make_item()
    item.step_count = 1
    messages = [{"role": "user", "content": "hi"}]
    tools = [
        {"type": "function", "function": {"name": "search"}},
        {"type": "function", "function": {"name": "submit_result"}},
    ]

    out_messages, out_tools = processor._apply_step_budget_controls(
        item=item,
        messages=messages,
        tools=tools,
    )

    assert out_tools == tools
    assert len(out_messages) == 2
    assert out_messages[-1]["role"] == "system"
    assert "2 steps remaining" in out_messages[-1]["content"]


def test_apply_step_budget_controls_final_step_restricts_to_submit_result() -> None:
    processor = _make_processor(max_steps=3)
    item = _make_item()
    item.step_count = 2
    messages = [{"role": "user", "content": "hi"}]
    tools = [
        {"type": "function", "function": {"name": "search"}},
        {"type": "function", "function": {"name": "submit_result"}},
    ]

    out_messages, out_tools = processor._apply_step_budget_controls(
        item=item,
        messages=messages,
        tools=tools,
    )

    assert isinstance(out_tools, list)
    assert len(out_tools) == 1
    assert out_tools[0]["function"]["name"] == "submit_result"
    assert out_messages[-1]["role"] == "system"
    assert "final step" in out_messages[-1]["content"]
    assert "ONLY tool call" in out_messages[-1]["content"]


def test_apply_step_budget_controls_final_step_without_submit_result_keeps_tools() -> None:
    processor = _make_processor(max_steps=3)
    item = _make_item()
    item.step_count = 2
    messages = [{"role": "user", "content": "hi"}]
    tools = [{"type": "function", "function": {"name": "search"}}]

    out_messages, out_tools = processor._apply_step_budget_controls(
        item=item,
        messages=messages,
        tools=tools,
    )

    assert out_tools == tools
    assert out_messages[-1]["role"] == "system"
    assert "No submit_result tool is available" in out_messages[-1]["content"]


@pytest.mark.asyncio
async def test_ensure_terminal_deliverable_reuses_existing_deliverable() -> None:
    cardbox = _DummyCardBox()
    cardbox.boxes["output_1"] = SimpleNamespace(card_ids=["d1"])
    cardbox.cards["d1"] = SimpleNamespace(card_id="d1", type="task.deliverable")
    processor = _make_processor(cardbox=cardbox)
    item = _make_item()
    card_id = await ensure_terminal_deliverable(
        cardbox=cardbox,
        item=item,
        step_id="step_1",
        terminal_status=STATUS_SUCCESS,
        reason="done",
        card_metadata_builder=processor._card_metadata,
        logger=react_step_module.logger,
    )
    assert card_id == "d1"
    assert cardbox.saved_cards == []


@pytest.mark.asyncio
async def test_ensure_terminal_deliverable_creates_fallback_when_missing() -> None:
    cardbox = _DummyCardBox()
    cardbox.boxes["output_1"] = SimpleNamespace(card_ids=["t1", "r1", "r2"])
    cardbox.cards["t1"] = SimpleNamespace(
        card_id="t1",
        type="agent.thought",
        content=TextContent(text="last thought"),
    )
    cardbox.cards["r1"] = SimpleNamespace(card_id="r1", type="tool.result")
    cardbox.cards["r2"] = SimpleNamespace(card_id="r2", type="tool.result")
    processor = _make_processor(cardbox=cardbox)
    item = _make_item()
    card_id = await ensure_terminal_deliverable(
        cardbox=cardbox,
        item=item,
        step_id="step_1",
        terminal_status=STATUS_FAILED,
        reason="tool_failed",
        card_metadata_builder=processor._card_metadata,
        logger=react_step_module.logger,
    )
    assert card_id is not None
    assert len(cardbox.saved_cards) == 1
    saved = cardbox.saved_cards[0]
    assert saved.type == "task.deliverable"
    assert saved.metadata["partial"] is True
    payload = getattr(saved.content, "data", {})
    assert isinstance(payload, dict)
    assert payload.get("kind") == "fallback_deliverable"
    assert payload.get("partial") is True
    assert "Collected tool results: 2" in str(payload.get("message"))
    diagnostics = payload.get("diagnostics") or {}
    assert diagnostics.get("tool_result_count") == 2
    assert diagnostics.get("last_agent_thought") == "last thought"


@pytest.mark.asyncio
async def test_complete_turn_emits_success_events_and_task(monkeypatch) -> None:
    state_store = _DummyStateStore(update_result=True)
    step_store = _DummyStepStore()
    nats = _DummyNATS()
    processor = _make_processor(state_store=state_store, step_store=step_store, nats=nats)
    item = _make_item()

    wakeups = []

    async def _fake_wakeup(**kwargs):
        wakeups.append(True)

    monkeypatch.setattr(processor.turn_finisher, "idle_wakeup_fn", _fake_wakeup)

    await processor._complete_turn(
        item,
        "step_1",
        start_ts=time.monotonic() - 0.01,
        deliverable_card_id="deliver_1",
        new_card_ids=[],
    )
    assert state_store.finish_calls[0]["expect_agent_turn_id"] == "turn_1"
    assert state_store.finish_calls[0]["last_output_box_id"] == "output_1"
    assert step_store.update_calls[0]["status"] == "completed"
    statuses = [payload.get("status") for _, payload, _ in nats.events if isinstance(payload, dict)]
    phases = [payload.get("phase") for _, payload, _ in nats.events if isinstance(payload, dict)]
    assert "idle" in statuses
    assert STATUS_SUCCESS in statuses
    assert "completed" in phases
    assert wakeups == [True]


@pytest.mark.asyncio
async def test_fail_turn_emits_failure_events_and_task(monkeypatch) -> None:
    state_store = _DummyStateStore(update_result=True)
    step_store = _DummyStepStore()
    nats = _DummyNATS()
    processor = _make_processor(state_store=state_store, step_store=step_store, nats=nats)
    item = _make_item()

    wakeups = []

    async def _fake_wakeup(**kwargs):
        wakeups.append(True)

    monkeypatch.setattr(processor.turn_finisher, "idle_wakeup_fn", _fake_wakeup)

    await processor._fail_turn(
        item,
        "step_1",
        "boom",
        error_code="E_FAIL",
        deliverable_card_id="deliver_1",
        new_card_ids=[],
    )
    assert state_store.finish_calls[0]["expect_agent_turn_id"] == "turn_1"
    assert state_store.finish_calls[0]["last_output_box_id"] == "output_1"
    assert step_store.update_calls[0]["status"] == STATUS_FAILED
    statuses = [payload.get("status") for _, payload, _ in nats.events if isinstance(payload, dict)]
    phases = [payload.get("phase") for _, payload, _ in nats.events if isinstance(payload, dict)]
    assert "idle" in statuses
    assert STATUS_FAILED in statuses
    assert "failed" in phases
    assert wakeups == [True]


@pytest.mark.asyncio
async def test_process_step_must_end_with_submit_result_without_submit_tool_spec(monkeypatch) -> None:
    cardbox = _DummyCardBox()
    state_store = _DummyStateStore(update_result=True)
    step_store = _DummyStepStore()
    processor = _make_processor(
        cardbox=cardbox,
        state_store=state_store,
        step_store=step_store,
        nats=_DummyNATS(),
    )
    item = _make_item()

    profile = SimpleNamespace(
        card_id="profile_1",
        name="Planner",
        display_name="Planner",
        tags=[],
        worker_target="agent_worker",
        allowed_tools=["search_only"],
        allowed_internal_tools=[],
        must_end_with=["submit_result"],
        llm_config=LLMConfig(model="gpt-4o-mini", stream=False),
    )

    async def _fake_load_profile(project_id, agent_id, profile_box_id):
        return profile

    async def _fake_fetch_tools_for_project_model(project_id):
        return []

    async def _fake_chat(request, on_chunk=None):
        return {"content": "hello", "tool_calls": []}

    async def _fake_assemble(**kwargs):
        return SimpleNamespace(card_ids=[])

    async def _fake_emit_state(*args, **kwargs):
        return None

    queued = []

    def _fake_enqueue_next_step(work_item):
        queued.append(work_item)

    class _DummyContextEngine:
        def __init__(self, *args, **kwargs):
            pass

        async def to_api(self, request_box, *, modifier=None):
            api_req = {"messages": [{"role": "user", "content": "hi"}], "tools": []}
            if modifier is not None:
                api_req = modifier(api_req)
            return api_req, None

    monkeypatch.setattr(processor.resource_loader, "load_profile", _fake_load_profile, raising=False)
    monkeypatch.setattr(
        processor.resource_store,
        "fetch_tools_for_project_model",
        _fake_fetch_tools_for_project_model,
        raising=False,
    )
    monkeypatch.setattr(processor.llm_wrapper, "chat", _fake_chat, raising=False)
    monkeypatch.setattr(processor.assembler, "assemble", _fake_assemble, raising=False)
    monkeypatch.setattr(processor, "_emit_state_event", _fake_emit_state)
    monkeypatch.setattr(processor, "_enqueue_next_step", _fake_enqueue_next_step)
    monkeypatch.setattr(react_step_module, "ContextEngine", _DummyContextEngine)

    await processor.process_step(item)

    assert len(queued) == 1
    saved_types = [getattr(card, "type", None) for card in cardbox.saved_cards]
    assert "sys.must_end_with_required" in saved_types
