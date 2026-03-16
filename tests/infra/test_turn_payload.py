from infra.l1_helpers.turn_payload import prepare_turn_payload


def test_prepare_turn_payload_dehydrates_control_fields() -> None:
    payload = prepare_turn_payload(
        profile_box_id="profile_1",
        context_box_id="context_1",
        output_box_id="output_1",
        display_name="Alice",
        runtime_config={"temperature": 0.2},
        metadata={"note": "x"},
    )

    assert payload == {
        "profile_box_id": "profile_1",
        "context_box_id": "context_1",
        "output_box_id": "output_1",
        "display_name": "Alice",
        "runtime_config": {"temperature": 0.2},
        "metadata": {"note": "x"},
    }
    assert "agent_id" not in payload
    assert "agent_turn_id" not in payload
    assert "turn_epoch" not in payload


def test_prepare_turn_payload_defaults_runtime_config() -> None:
    payload = prepare_turn_payload(
        profile_box_id="profile_1",
        context_box_id="context_1",
        output_box_id="output_1",
    )
    assert payload["runtime_config"] == {}
