from __future__ import annotations

import pytest

from examples.quickstarts.demo_principal_fullflow import _resolve_child_trace_id


def test_resolve_child_trace_prefers_head_when_consistent() -> None:
    child_trace = _resolve_child_trace_id(
        parent_trace_id="11111111-1111-1111-1111-111111111111",
        head_trace_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        turns=[{"trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],
    )
    assert child_trace == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def test_resolve_child_trace_falls_back_to_first_turn() -> None:
    child_trace = _resolve_child_trace_id(
        parent_trace_id="11111111-1111-1111-1111-111111111111",
        head_trace_id=None,
        turns=[{"trace_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}],
    )
    assert child_trace == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def test_resolve_child_trace_rejects_head_turn_mismatch() -> None:
    with pytest.raises(RuntimeError, match="child trace source mismatch"):
        _resolve_child_trace_id(
            parent_trace_id="11111111-1111-1111-1111-111111111111",
            head_trace_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            turns=[{"trace_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}],
        )


def test_resolve_child_trace_rejects_missing_trace() -> None:
    with pytest.raises(RuntimeError, match="child trace_id missing"):
        _resolve_child_trace_id(
            parent_trace_id="11111111-1111-1111-1111-111111111111",
            head_trace_id=None,
            turns=[],
        )
