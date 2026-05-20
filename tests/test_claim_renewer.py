from __future__ import annotations

import threading
import time
from datetime import UTC, datetime

import httpx

from CommonGround.agent_client import ClaimAutoRenewer, ClaimTurnPartialFailure, PollingWorker
from CommonGround.agent_client.http_client import HttpAgentClient
from CommonGround.contracts import AgentRef, CauseRef, ClaimRenewal, ClaimToken, TurnRef, TurnSnapshot, TurnState
from CommonGround.sdk import TurnContext


def _claim() -> ClaimToken:
    return ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="worker",
        token="claim-token",
        expires_at=datetime(2026, 4, 11, 12, 0, 30, tzinfo=UTC),
    )


def _turn_context() -> TurnContext:
    turn = TurnRef(project_id="demo", turn_id="T-1")
    return TurnContext(
        turn=TurnSnapshot(
            turn=turn,
            target_agent=AgentRef(project_id="demo", agent_id="worker"),
            turn_kind="turn.conversation.v1",
            cause=CauseRef(kind="external", id="req-1"),
            state=TurnState.RUNNING,
            outcome=None,
            stop_requested=False,
            current_claim_agent_id="worker",
            claim_expires_at=datetime(2026, 4, 11, 12, 0, 30, tzinfo=UTC),
            spawn_key="spawn-1",
            final_record_role=None,
            final_cardbox_ref=None,
            final_payload=None,
            created_at=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            updated_at=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            closed_at=None,
        ),
        semantic_items=(),
    )


def test_claim_auto_renewer_uses_server_recommended_interval() -> None:
    claim = _claim()
    calls: list[float] = []
    done = threading.Event()
    started_at = time.monotonic()

    class FakeClient:
        def renew_claim(self, loaded_claim: ClaimToken) -> ClaimRenewal:
            assert loaded_claim == claim
            calls.append(round(time.monotonic() - started_at, 3))
            if len(calls) == 2:
                done.set()
            return ClaimRenewal(
                server_time=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
                expires_at=datetime(2026, 4, 11, 12, 0, 30, tzinfo=UTC),
                recommended_interval_seconds=0.05 if len(calls) == 1 else 1.0,
            )

    renewer = ClaimAutoRenewer(FakeClient(), claim=claim, interval_seconds=0.01)
    renewer.start()
    try:
        assert done.wait(1.0)
    finally:
        renewer.stop()

    assert len(calls) == 2
    assert calls[0] < 0.05
    assert 0.04 <= calls[1] - calls[0] <= 0.15


def test_claim_auto_renewer_continues_after_transient_failure() -> None:
    claim = _claim()
    attempts: list[str] = []
    done = threading.Event()

    class FakeClient:
        def renew_claim(self, loaded_claim: ClaimToken) -> ClaimRenewal:
            assert loaded_claim == claim
            if not attempts:
                attempts.append("failed")
                raise RuntimeError("temporary")
            attempts.append("renewed")
            done.set()
            return ClaimRenewal(
                server_time=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
                expires_at=datetime(2026, 4, 11, 12, 0, 30, tzinfo=UTC),
                recommended_interval_seconds=1.0,
            )

    renewer = ClaimAutoRenewer(FakeClient(), claim=claim, interval_seconds=0.01)
    renewer.start()
    try:
        assert done.wait(1.0)
    finally:
        renewer.stop()

    assert attempts == ["failed", "renewed"]


def test_claim_auto_renewer_escalates_after_repeated_failures() -> None:
    claim = _claim()

    class FakeClient:
        def renew_claim(self, loaded_claim: ClaimToken) -> ClaimRenewal:
            assert loaded_claim == claim
            raise RuntimeError("down")

    renewer = ClaimAutoRenewer(FakeClient(), claim=claim, interval_seconds=0.01, max_consecutive_failures=2)
    renewer.start()
    try:
        deadline = time.monotonic() + 1.0
        while renewer.fatal_error() is None and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        renewer.stop()

    assert renewer.fatal_error() is not None


def test_polling_worker_safe_stops_when_context_fetch_fails() -> None:
    claim = _claim()
    suspends: list[tuple[str, str | None]] = []

    class FakeClient:
        def claim_turn_handle(self, agent: AgentRef):
            assert agent == AgentRef(project_id="demo", agent_id="worker")
            return claim

        def is_stop_requested(self, turn: TurnRef) -> bool:
            return False

        def fetch_context(self, turn: TurnRef) -> TurnContext:
            raise RuntimeError("context boom")

        def suspend_turn(self, loaded_claim: ClaimToken, *, reason: str, note: str | None = None, meta=None) -> None:
            assert loaded_claim == claim
            suspends.append((reason, note))

        def renew_claim(self, loaded_claim: ClaimToken) -> ClaimRenewal:
            return ClaimRenewal(
                server_time=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
                expires_at=datetime(2026, 4, 11, 12, 0, 30, tzinfo=UTC),
                recommended_interval_seconds=1.0,
            )

    class UnusedHandler:
        def handle_turn(self, context, client, loaded_claim):
            raise AssertionError("handler should not run when context fetch fails")

    worker = PollingWorker(client=FakeClient(), agent=AgentRef(project_id="demo", agent_id="worker"), handler=UnusedHandler())
    result = worker.run_once()

    assert result.action == "safe_stop"
    assert suspends == [("context_fetch_error", "context boom")]


def test_polling_worker_safe_stops_when_claim_lease_is_lost() -> None:
    claim = _claim()

    class FakeClient:
        def claim_turn_handle(self, agent: AgentRef):
            return claim

        def is_stop_requested(self, turn: TurnRef) -> bool:
            return False

        def fetch_context(self, turn: TurnRef) -> TurnContext:
            return _turn_context()

        def renew_claim(self, loaded_claim: ClaimToken) -> ClaimRenewal:
            raise RuntimeError("lease lost")

        def get_turn(self, turn: TurnRef) -> TurnSnapshot:
            return _turn_context().turn

        def suspend_turn(self, loaded_claim: ClaimToken, *, reason: str, note: str | None = None, meta=None) -> None:
            raise AssertionError("worker should not try to suspend after lease loss")

        def finish_turn(self, loaded_claim: ClaimToken, **kwargs) -> None:
            raise AssertionError("worker should not try to finish after lease loss")

        def append_record(self, loaded_claim: ClaimToken, payload, *, role: str = "progress"):
            raise AssertionError("lease-aware client should block append after lease loss")

    class AppendHandler:
        def handle_turn(self, context, client, loaded_claim):
            time.sleep(0.05)
            client.append_record(loaded_claim, {"status": "late"})
            raise AssertionError("append should not succeed")

    worker = PollingWorker(
        client=FakeClient(),
        agent=AgentRef(project_id="demo", agent_id="worker"),
        handler=AppendHandler(),
        claim_heartbeat_interval_seconds=0.01,
    )
    result = worker.run_once()

    assert result.action == "safe_stop"


def test_claim_turn_helper_suspends_when_context_fetch_fails() -> None:
    claim = _claim()
    suspends: list[tuple[str, str | None]] = []

    class FakeClient(HttpAgentClient):
        def __init__(self) -> None:
            pass

        def claim_turn_handle(self, agent: AgentRef, *, meta=None):
            assert agent == AgentRef(project_id="demo", agent_id="worker")
            return claim

        def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100) -> TurnContext:
            raise RuntimeError("context boom")

        def suspend_turn(self, loaded_claim: ClaimToken, *, reason: str, note: str | None = None, meta=None) -> None:
            suspends.append((reason, note))

    client = FakeClient()
    try:
        client.claim_turn(AgentRef(project_id="demo", agent_id="worker"))
    except ClaimTurnPartialFailure as exc:
        assert exc.claim == claim
        assert str(exc.context_error) == "context boom"
        assert exc.suspend_error is None
    else:
        raise AssertionError("claim_turn should surface a partial failure with the claim handle")

    assert suspends == [("context_fetch_error", "context boom")]


def test_claim_auto_renewer_escalates_immediately_on_definitive_lease_loss() -> None:
    claim = _claim()
    request = httpx.Request("POST", "http://cg.example/v3r1/projects/demo/claims:renew")
    response = httpx.Response(409, request=request, json={"error": "FencingError", "message": "claim expired"})

    class FakeClient:
        def renew_claim(self, loaded_claim: ClaimToken) -> ClaimRenewal:
            raise httpx.HTTPStatusError("conflict", request=request, response=response)

    renewer = ClaimAutoRenewer(FakeClient(), claim=claim, interval_seconds=0.01, max_consecutive_failures=5)
    renewer.start()
    try:
        deadline = time.monotonic() + 1.0
        while renewer.fatal_error() is None and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        renewer.stop()

    assert renewer.fatal_error() is not None
