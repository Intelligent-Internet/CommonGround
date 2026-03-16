from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.cg_context import CGContext
from core.errors import BadRequestError
from services.pmo.handover import HandoverPacker
from services.pmo.internal_handlers.orchestration_primitives import validate_box_ids_exist


def _ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_parent",
        agent_turn_id="turn_parent",
        turn_epoch=1,
        step_id="step_parent",
    )


class _DummyCardBox:
    def __init__(self) -> None:
        self.boxes = {
            "box_context": SimpleNamespace(card_ids=["c_ctx"]),
            "box_output": SimpleNamespace(card_ids=["c_out"]),
            "box_clone": SimpleNamespace(card_ids=["c_clone"]),
            "profile_delegatee": SimpleNamespace(card_ids=[]),
        }
        self.saved_cards = []

    async def get_box(self, box_id, *, project_id, conn=None):
        _ = project_id, conn
        return self.boxes.get(box_id)

    async def get_cards(self, card_ids, *, project_id, conn=None):
        _ = card_ids, project_id, conn
        return []

    async def save_card(self, card, conn=None):
        _ = conn
        self.saved_cards.append(card)

    async def save_box(self, card_ids, *, project_id, conn=None):
        _ = project_id, conn
        return "box_packed"


class _DummyResourceStore:
    async def find_profile_by_name(self, project_id, profile_name, conn=None):
        _ = project_id, conn
        if profile_name != "delegatee":
            return None
        return {"profile_box_id": "profile_delegatee"}


class _DummyStateStore:
    async def fetch(self, ctx, conn=None):
        _ = ctx, conn
        return SimpleNamespace(
            profile_box_id="profile_parent",
            context_box_id="box_context",
            output_box_id="box_output",
        )

    async def fetch_pointers(self, ctx, conn=None):
        _ = ctx, conn
        return SimpleNamespace(
            memory_context_box_id=None,
            last_output_box_id="box_output",
        )


@pytest.mark.asyncio
async def test_validate_box_ids_exist_rejects_unauthorized_box() -> None:
    deps = SimpleNamespace(
        state_store=_DummyStateStore(),
        cardbox=_DummyCardBox(),
    )

    with pytest.raises(BadRequestError, match="unauthorized box_id"):
        await validate_box_ids_exist(
            deps=deps,
            ctx=_ctx(),
            box_ids=["box_other"],
        )


@pytest.mark.asyncio
async def test_validate_box_ids_exist_allows_clone_output_when_explicitly_authorized() -> None:
    deps = SimpleNamespace(
        state_store=_DummyStateStore(),
        cardbox=_DummyCardBox(),
    )

    result = await validate_box_ids_exist(
        deps=deps,
        ctx=_ctx(),
        box_ids=["box_context", "box_clone"],
        authorized_box_ids=["box_clone"],
    )

    assert result == ["box_context", "box_clone"]


@pytest.mark.asyncio
async def test_handover_pack_context_rejects_unlisted_inherit_box() -> None:
    packer = HandoverPacker(cardbox=_DummyCardBox(), resource_store=_DummyResourceStore())

    with pytest.raises(BadRequestError, match="unauthorized box_id"):
        await packer.pack_context(
            ctx=_ctx(),
            tool_suffix="delegate_async",
            arguments={
                "profile_name": "delegatee",
                "instruction": "do work",
                "input_box_ids": ["box_clone"],
            },
            handover={
                "target_profile_config": {"profile_name": "delegatee"},
                "authorized_inherit_box_ids": ["box_context"],
                "context_packing_config": {
                    "pack_arguments": [{"arg_key": "instruction", "as_card_type": "task.instruction"}],
                    "inherit_context": {
                        "include_boxes_from_args": ["input_box_ids"],
                        "include_parent": True,
                    },
                },
            },
        )
