from __future__ import annotations

import json
from pathlib import Path

from scripts.demo.run_multihop_agent_handoff_e2e import (
    _build_summary,
    _demo_public_metadata_by_agent,
    _prepare_payload_file,
    _replace_payload_placeholders,
    _resolve_provider_settings,
    _runtime_provider_api_base,
    _semantic_records_from_context,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_ROOT = REPO_ROOT / "examples" / "nanobot" / "multihop_agent_handoff_demo"


def _all_object_keys(payload: object) -> set[str]:
    keys: set[str] = set()
    if isinstance(payload, dict):
        keys.update(payload.keys())
        for value in payload.values():
            keys.update(_all_object_keys(value))
    elif isinstance(payload, list):
        for item in payload:
            keys.update(_all_object_keys(item))
    return keys


def test_multihop_runner_resolves_provider_defaults_from_nanobot_config(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "agents": {
                    "defaults": {
                        "provider": "azure_openai",
                        "model": "gpt-5.4-nano",
                    }
                },
                "providers": {
                    "azure_openai": {
                        "apiKey": "test-key",
                        "apiBase": "https://example.openai.azure.com/openai/v1/",
                    },
                    "azureOpenai": {
                        "apiKey": "",
                        "apiBase": None,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    resolved = _resolve_provider_settings(
        provider_arg=None,
        model_arg=None,
        api_key_arg=None,
        api_base_arg=None,
        config_path=config_path,
    )

    assert resolved.provider == "azure_openai"
    assert resolved.model == "gpt-5.4-nano"
    assert resolved.api_key == "test-key"
    assert resolved.api_base == "https://example.openai.azure.com/openai/v1/"


def test_multihop_runner_strips_azure_responses_suffix_for_runtime_config() -> None:
    assert (
        _runtime_provider_api_base("azure_openai", "https://example.openai.azure.com/openai/v1/")
        == "https://example.openai.azure.com"
    )
    assert _runtime_provider_api_base("azure_openai", "https://example.openai.azure.com") == "https://example.openai.azure.com"
    assert _runtime_provider_api_base("custom", "https://example.openai.azure.com/openai/v1/") == "https://example.openai.azure.com/openai/v1/"


def test_multihop_runner_resolves_oauth_provider_from_model_prefix(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "agents": {
                    "defaults": {
                        "provider": "auto",
                        "model": "openai-codex/gpt-5.1-codex",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    resolved = _resolve_provider_settings(
        provider_arg=None,
        model_arg=None,
        api_key_arg=None,
        api_base_arg=None,
        config_path=config_path,
    )

    assert resolved.provider == "openai_codex"
    assert resolved.model == "openai-codex/gpt-5.1-codex"
    assert resolved.api_key == ""


def test_multihop_request_samples_match_current_demo_contract() -> None:
    multihop_payload = json.loads((DEMO_ROOT / "request_samples" / "root_request_payload.json").read_text(encoding="utf-8"))
    single_hop_payload = json.loads(
        (DEMO_ROOT / "request_samples" / "root_request_payload.single_hop.json").read_text(encoding="utf-8")
    )
    local_subagent_payload = json.loads(
        (DEMO_ROOT / "request_samples" / "root_request_payload.local_subagent.json").read_text(encoding="utf-8")
    )

    for payload in (multihop_payload, single_hop_payload):
        assert payload["kind"] == "common_ground.work_order.v1"
        assert isinstance(payload["objective"], str) and payload["objective"]
        assert isinstance(payload["input"], dict)
        assert isinstance(payload["expected_output"], dict)
        assert isinstance(payload["delegation_policy"], dict)
        assert isinstance(payload["provenance"], dict)
        assert payload["delegation_policy"]["may_delegate"] is True

    unsupported_contract_keys = {
        "missing_information",
        "information_plan",
        "execution_plan",
        "preferred_expert_agent",
        "must_dispatch_via_commonground",
        "single_hop_playbook",
        "offer_lookup_is_not_required",
        "forbidden_fallbacks",
        "task_type",
        "child_task",
    }
    assert unsupported_contract_keys.isdisjoint(_all_object_keys(multihop_payload))
    assert unsupported_contract_keys.isdisjoint(_all_object_keys(single_hop_payload))

    repo_resource = single_hop_payload["input"]["available_resources"][0]
    assert repo_resource["clone_url"] == "${DEMO_REPO_URL}"
    assert repo_resource["kind"] == "repository"

    assert local_subagent_payload["kind"] == "common_ground.work_order.v1"
    assert local_subagent_payload["input"]["local_subagent_smoke_test"] is True
    assert local_subagent_payload["input"]["fixed_subagent_output"] == "CG_LOCAL_SUBAGENT_FIXED_OUTPUT_V1"
    assert local_subagent_payload["delegation_policy"] == {
        "may_delegate": False,
        "may_use_local_subagent": True,
    }


def test_multihop_demo_public_metadata_keeps_expert_discovery_broad() -> None:
    metadata_by_agent = _demo_public_metadata_by_agent()

    peer_offer = metadata_by_agent["nanobot_b"]["turn_offers"][0]
    assert peer_offer["input_contract"]["example_payload"] == {
        "objective": "Answer a delegated clarification request using the available context."
    }
    assert "fixture-local context" in peer_offer["notes"]

    codex_offers = metadata_by_agent["codex_c"]["turn_offers"]
    conversation_offer = next(offer for offer in codex_offers if offer["turn_kind"] == "turn.conversation.v1")
    coding_offer = next(offer for offer in codex_offers if offer["turn_kind"] == "coding")

    assert "clone_url" not in json.dumps(conversation_offer["input_contract"])
    assert conversation_offer["variants"]["domains"] == ["coding"]
    assert coding_offer["variants"]["advisory_only"] is True
    assert "broad work-order envelope" in coding_offer["notes"]


def test_multihop_runner_helpers_preserve_authoritative_payloads_and_extract_evidence(tmp_path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "objective": "Use this payload as opaque demo input.",
                "input": {
                    "artifacts": [
                        {
                            "kind": "repo",
                            "url": "${DEMO_REPO_URL}",
                        }
                    ],
                    "notes": {
                        "verbatim_repo_reference": "${DEMO_REPO_URL}",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    prepared_path = _prepare_payload_file(payload_path, demo_repo_url="file:///tmp/demo-site.git", tmp_root=tmp_path)
    prepared_payload = json.loads(prepared_path.read_text(encoding="utf-8"))
    assert prepared_payload["input"]["artifacts"][0]["url"] == "file:///tmp/demo-site.git"
    assert prepared_payload["input"]["notes"]["verbatim_repo_reference"] == "file:///tmp/demo-site.git"

    replaced = _replace_payload_placeholders(
        {
            "a": "${DEMO_REPO_URL}",
            "b": ["unchanged", "${DEMO_REPO_URL}"],
            "c": {"nested": "${DEMO_REPO_URL}"},
        },
        replacements={"${DEMO_REPO_URL}": "file:///tmp/demo-site.git"},
    )
    assert replaced == {
        "a": "file:///tmp/demo-site.git",
        "b": ["unchanged", "file:///tmp/demo-site.git"],
        "c": {"nested": "file:///tmp/demo-site.git"},
    }

    service_log = tmp_path / "service.log"
    log_a = tmp_path / "nanobot_a.log"
    log_b = tmp_path / "nanobot_b.log"
    log_c = tmp_path / "codex_c.log"
    service_log.write_text(
        '\n'.join(
            [
                'INFO "POST /v3r1/projects/cg-demo/turns%3Adispatch HTTP/1.1" 200 OK',
                'INFO "POST /v3r1/projects/cg-demo/turns/T-10%3Asuspend HTTP/1.1" 200 OK',
                'INFO "POST /v3r1/projects/cg-demo/turns/T-10%3Aresume HTTP/1.1" 200 OK',
                'INFO "POST /v3r1/projects/cg-demo/turns/T-10%3Afinish HTTP/1.1" 200 OK',
                "ignore me",
            ]
        ),
        encoding="utf-8",
    )
    log_a.write_text(
        '\n'.join(
            [
                "Tool call: cg_list_agents(...)",
                "Tool call: cg_list_turn_offers(...)",
                "Tool call: cg_get_agent(...)",
                "Tool call: cg_dispatch_child(...)",
                "Response to cg:cg_companion: {",
                "noise",
            ]
        ),
        encoding="utf-8",
    )
    log_b.write_text(
        '\n'.join(
            [
                "You are executing a CommonGround work order.",
                "Response to cg:cg_companion: {",
            ]
        ),
        encoding="utf-8",
    )
    log_c.write_text(
        '\n'.join(
            [
                "Tool call: exec(...)",
                "Response to cg:cg_companion: {",
            ]
        ),
        encoding="utf-8",
    )

    wait = {
        "result": {
            "turn_id": "T-10",
            "final_payload": {
                "child_results": [
                    {"child_turn_id": "T-11", "agent_id": "nanobot_b"},
                    {"child_turn_id": "T-12", "agent_id": "codex_c"},
                ],
                "final_outcome": {"opaque_marker": {"kind": "artifact", "value": "abc123"}},
            }
        }
    }
    lineage = {
        "result": {
            "direct_children": [
                {"turn_id": "T-11", "target_agent_id": "nanobot_b"},
                {"turn_id": "T-12", "target_agent_id": "codex_c"},
            ]
        }
    }
    child_turns = {
        "T-11": {"ok": True, "result": {"final_payload": {"status": "succeeded"}}},
        "T-12": {"ok": True, "result": {"final_payload": {"opaque_marker": {"kind": "artifact", "value": "abc123"}}}},
    }

    summary = _build_summary(
        project_id="cg-demo",
        parent_turn_id="T-10",
        wait=wait,
        lineage=lineage,
        child_turns=child_turns,
        service_log=service_log,
        log_a=log_a,
        log_b=log_b,
        log_c=log_c,
        demo_repo_url="file:///tmp/demo-site.git",
        directory_snapshot={"agents": [], "turn_offers": []},
    )

    assert summary["parent"] == wait["result"]
    assert summary["children"] == child_turns
    assert summary["parent"]["final_payload"]["final_outcome"]["opaque_marker"] == {
        "kind": "artifact",
        "value": "abc123",
    }
    assert summary["evidence"]["nanobot_a"] == [
        "Tool call: cg_list_agents(...)",
        "Tool call: cg_list_turn_offers(...)",
        "Tool call: cg_get_agent(...)",
        "Tool call: cg_dispatch_child(...)",
        "Response to cg:cg_companion: {",
    ]
    assert summary["evidence"]["service"] == [
        'INFO "POST /v3r1/projects/cg-demo/turns%3Adispatch HTTP/1.1" 200 OK',
        'INFO "POST /v3r1/projects/cg-demo/turns/T-10%3Asuspend HTTP/1.1" 200 OK',
        'INFO "POST /v3r1/projects/cg-demo/turns/T-10%3Aresume HTTP/1.1" 200 OK',
        'INFO "POST /v3r1/projects/cg-demo/turns/T-10%3Afinish HTTP/1.1" 200 OK',
    ]
    assert summary["workflow_evidence"]["nanobot_a_selection_flow"] == [
        "Tool call: cg_list_agents(...)",
        "Tool call: cg_list_turn_offers(...)",
        "Tool call: cg_get_agent(...)",
        "Tool call: cg_dispatch_child(...)",
    ]
    assert summary["workflow_checks"]["nanobot_a_discovery_before_first_child_dispatch"] is True


def test_multihop_runner_extracts_reported_turn_records() -> None:
    context = {
        "result": {
            "semantic_items": [
                {
                    "record": {
                        "ref": {"record_id": "rec-1"},
                        "turn_seq": 1,
                        "record_role": "bootstrap",
                    },
                    "content": {
                        "cards": [
                            {
                                "content": {
                                    "__type__": "JsonContent",
                                    "data": {"task": "root"},
                                }
                            }
                        ]
                    },
                },
                {
                    "record": {
                        "ref": {"record_id": "rec-2"},
                        "turn_seq": 2,
                        "record_role": "local_subagent",
                    },
                    "content": {
                        "cards": [
                            {
                                "content": {
                                    "__type__": "JsonContent",
                                    "data": {
                                        "type": "local_subagent_result",
                                        "status": "ok",
                                    },
                                }
                            }
                        ]
                    },
                },
            ]
        }
    }

    assert _semantic_records_from_context(context) == [
        {
            "record_id": "rec-1",
            "turn_seq": 1,
            "record_role": "bootstrap",
            "payload": {"task": "root"},
        },
        {
            "record_id": "rec-2",
            "turn_seq": 2,
            "record_role": "local_subagent",
            "payload": {"type": "local_subagent_result", "status": "ok"},
        },
    ]
