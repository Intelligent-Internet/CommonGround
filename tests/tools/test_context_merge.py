from services.tools.context_merge import ContextMergeTool


def test_build_concat_card_text_uses_index_not_card_id() -> None:
    text = ContextMergeTool._build_concat_card_text(
        content_text="hello world",
        source_index=2,
    )
    assert text == "--- Source 2 ---\nhello world"
    assert "card_" not in text


def test_build_concat_card_text_normalizes_non_positive_index() -> None:
    text = ContextMergeTool._build_concat_card_text(
        content_text="content",
        source_index=0,
    )
    assert text.startswith("--- Source 1 ---")
