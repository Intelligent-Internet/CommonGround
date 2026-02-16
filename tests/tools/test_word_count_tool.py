from __future__ import annotations

import pytest

from core.errors import BadRequestError
from services.tools.word_count import map_word_count, reduce_word_count


def test_map_word_count_normalizes_and_sorts_deterministically() -> None:
    result = map_word_count(
        text="Banana apple banana, cherry apple! banana.",
        top_k=10,
    )

    assert result["mode"] == "map"
    assert result["total_tokens"] == 6
    assert result["unique_tokens"] == 3
    assert result["counts"] == [
        {"word": "banana", "count": 3},
        {"word": "apple", "count": 2},
        {"word": "cherry", "count": 1},
    ]
    assert result["counts_map"] == {"banana": 3, "apple": 2, "cherry": 1}


def test_map_word_count_tie_breaker_is_word_asc() -> None:
    result = map_word_count(text="b a c c b a", top_k=10)

    assert result["counts"] == [
        {"word": "a", "count": 2},
        {"word": "b", "count": 2},
        {"word": "c", "count": 2},
    ]


def test_reduce_word_count_merges_counts_map_and_counts() -> None:
    result = reduce_word_count(
        items=[
            {"counts_map": {"apple": 2, "banana": 1}},
            {"counts": [{"word": "banana", "count": 3}, {"word": "cherry", "count": 2}]},
            {"counts_map": {"apple": 1, "cherry": 1}},
        ],
        top_k=10,
    )

    assert result["mode"] == "reduce"
    assert result["input_items"] == 3
    assert result["unique_tokens"] == 3
    assert result["counts"] == [
        {"word": "banana", "count": 4},
        {"word": "apple", "count": 3},
        {"word": "cherry", "count": 3},
    ]
    assert result["counts_map"] == {"banana": 4, "apple": 3, "cherry": 3}


def test_reduce_word_count_requires_list_items() -> None:
    with pytest.raises(BadRequestError):
        reduce_word_count(items={"counts_map": {"x": 1}}, top_k=10)  # type: ignore[arg-type]
