from core.status import STATUS_FAILED, STATUS_SUCCESS
from services.agent_worker.resume_decision import (
    ResumeResultInfo,
    resolve_resume_outcome,
)


def test_resolve_resume_outcome_suspend_when_no_terminate() -> None:
    infos = [
        ResumeResultInfo(card=None, status=STATUS_SUCCESS, after_execution="suspend", payload={"a": 1}),
        ResumeResultInfo(card=None, status=STATUS_FAILED, after_execution=None, payload={"b": 2}),
    ]
    outcome = resolve_resume_outcome(infos)
    assert outcome.aggregate_after_execution == "suspend"
    assert outcome.task_status == STATUS_SUCCESS
    assert outcome.primary_payload is None


def test_resolve_resume_outcome_prefers_failed_terminate_result() -> None:
    infos = [
        ResumeResultInfo(card=None, status=STATUS_SUCCESS, after_execution="terminate", payload={"ok": True}),
        ResumeResultInfo(card=None, status=STATUS_FAILED, after_execution="terminate", payload={"error": "x"}),
    ]
    outcome = resolve_resume_outcome(infos)
    assert outcome.aggregate_after_execution == "terminate"
    assert outcome.task_status == STATUS_FAILED
    assert outcome.primary_payload == {"error": "x"}


def test_resolve_resume_outcome_uses_first_terminate_when_all_success() -> None:
    infos = [
        ResumeResultInfo(card=None, status=STATUS_SUCCESS, after_execution="terminate", payload={"k": 1}),
        ResumeResultInfo(card=None, status=STATUS_SUCCESS, after_execution="terminate", payload={"k": 2}),
    ]
    outcome = resolve_resume_outcome(infos)
    assert outcome.aggregate_after_execution == "terminate"
    assert outcome.task_status == STATUS_SUCCESS
    assert outcome.primary_payload == {"k": 1}
