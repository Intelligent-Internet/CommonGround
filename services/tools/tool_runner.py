from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Tuple, Union

import uuid6
from pydantic import ValidationError

from core.errors import BadRequestError, ProtocolViolationError
from core.headers import require_recursion_depth
from core.subject import SubjectParts, parse_subject
from core.trace import ensure_trace_headers, trace_id_from_headers
from core.utp_protocol import ToolCommandPayload
from infra.idempotency import IdempotentExecutor, NatsKvIdempotencyStore, build_idempotency_key
from infra.l0_engine import L0Engine
from infra.l0.tool_reports import publish_tool_result_report
from infra.tool_executor import (
    ToolCallContext,
    ToolCallHydrationError,
    ToolResultBuilder,
    ToolResultContext,
    hydrate_tool_call_context,
)


@dataclass(frozen=True)
class ToolPayloadValidation:
    forbid_keys: set[str] = field(default_factory=set)
    require_tool_call_card_id: bool = False
    require_tool_name: bool = False
    require_after_execution: bool = False
    allowed_after_execution: Optional[set[str]] = None

    def validate(self, data: Dict[str, Any]) -> ToolCommandPayload:
        return ToolCommandPayload.validate(
            data,
            forbid_keys=set(self.forbid_keys),
            require_tool_call_card_id=self.require_tool_call_card_id,
            require_tool_name=self.require_tool_name,
            require_after_execution=self.require_after_execution,
            allowed_after_execution=self.allowed_after_execution,
        )


@dataclass(frozen=True)
class ToolPreflightContext:
    subject: str
    parts: SubjectParts
    data: Dict[str, Any]
    headers: Dict[str, str]

@dataclass(frozen=True)
class ToolExecutionOutput:
    payload: Dict[str, Any]
    result_card: Any


@dataclass(frozen=True)
class ToolRunnerResult:
    handled: bool
    context: Optional[ToolResultContext | ToolPreflightContext] = None
    is_leader: Optional[bool] = None
    payload: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ToolIdempotency:
    store: NatsKvIdempotencyStore
    executor: IdempotentExecutor


ToolExecutionOutputLike = Union[ToolExecutionOutput, Tuple[Dict[str, Any], Any], Any]
ToolExecuteFn = Callable[[ToolCallContext], Awaitable[ToolExecutionOutputLike]]
ToolValidationErrorHandler = Callable[[ToolPreflightContext, Exception], Awaitable[bool]]
ToolPublishResultFn = Callable[[SubjectParts, Dict[str, str], Dict[str, Any], str], Awaitable[None]]


def build_tool_idempotency(
    nats: Any,
    *,
    bucket_name: str = "cg_tool_idem_v1",
    timeout_s: float = 30.0,
    ttl_s: Optional[int] = None,
    payload_max_bytes: Optional[int] = None,
    poll_interval_s: float = 0.5,
    retry_attempts: int = 1,
    retry_backoff_s: float = 0.5,
) -> ToolIdempotency:
    store = NatsKvIdempotencyStore(
        nats,
        bucket_name=bucket_name,
        ttl_s=ttl_s,
        payload_max_bytes=payload_max_bytes,
        poll_interval_s=poll_interval_s,
    )
    executor = IdempotentExecutor(
        store,
        timeout_s=timeout_s,
        retry_attempts=retry_attempts,
        retry_backoff_s=retry_backoff_s,
    )
    return ToolIdempotency(store=store, executor=executor)


class ToolRunner:
    """Shared shell for tool command handling with KV idempotency + report publish."""

    def __init__(
        self,
        *,
        target: str,
        source_agent_id: str,
        nats: Any,
        cardbox: Any,
        execution_store: Any,
        resource_store: Any = None,
        state_store: Any = None,
        l0_engine: Optional[L0Engine] = None,
        idempotent_executor: IdempotentExecutor,
        payload_validation: ToolPayloadValidation,
        logger: Optional[logging.Logger] = None,
        service_name: Optional[str] = None,
        category: str = "cmd",
        component: str = "tool",
        idempotency_namespace: str = "tool",
        result_author_id: Optional[str] = None,
        default_function_name: Optional[str] = None,
        publish_result: Optional[ToolPublishResultFn] = None,
        require_tool_call_type: bool = False,
        validate_tool_call_id_match: bool = False,
    ):
        self.target = str(target)
        self.source_agent_id = str(source_agent_id)
        self.nats = nats
        self.cardbox = cardbox
        self.execution_store = execution_store
        self.resource_store = resource_store
        self.state_store = state_store
        self.l0_engine = l0_engine
        self.idempotent_executor = idempotent_executor
        self.payload_validation = payload_validation
        self.logger = logger or logging.getLogger(__name__)
        self.service_name = service_name or self.target
        self.category = str(category)
        self.component = str(component)
        self.idempotency_namespace = str(idempotency_namespace)
        self.result_author_id = str(result_author_id or f"tool.{self.target}")
        self.default_function_name = str(default_function_name or self.target)
        self.publish_result = publish_result
        self.require_tool_call_type = bool(require_tool_call_type)
        self.validate_tool_call_id_match = bool(validate_tool_call_id_match)

    async def run(
        self,
        *,
        subject: str,
        data: Dict[str, Any],
        headers: Mapping[str, str] | None,
        execute: ToolExecuteFn,
        parts: Optional[SubjectParts] = None,
        tool_name_fallback: Optional[str] = None,
        idempotency_domain: Optional[str] = None,
        source_agent_id: Optional[str] = None,
        on_validation_error: Optional[ToolValidationErrorHandler] = None,
    ) -> ToolRunnerResult:
        resolved_parts = parts or parse_subject(subject)
        if (
            resolved_parts is None
            or resolved_parts.category != self.category
            or resolved_parts.component != self.component
            or resolved_parts.target != self.target
        ):
            return ToolRunnerResult(handled=False)

        merged_headers = self.nats.merge_headers(dict(headers or {}))
        try:
            merged_headers, _, _ = ensure_trace_headers(merged_headers)
        except ProtocolViolationError as exc:
            self.logger.error("%s invalid trace headers: %s", self.service_name, exc)
            return ToolRunnerResult(
                handled=True,
                context=ToolPreflightContext(
                    subject=subject,
                    parts=resolved_parts,
                    data=data,
                    headers=merged_headers,
                ),
            )

        preflight = ToolPreflightContext(
            subject=subject,
            parts=resolved_parts,
            data=data,
            headers=merged_headers,
        )
        try:
            payload = self.payload_validation.validate(data)
        except (ValidationError, BadRequestError, ProtocolViolationError) as exc:
            handled = False
            if on_validation_error is not None:
                try:
                    handled = bool(await on_validation_error(preflight, exc))
                except Exception as callback_exc:  # noqa: BLE001
                    self.logger.error(
                        "%s validation error handler failed: %s",
                        self.service_name,
                        callback_exc,
                        exc_info=True,
                    )
                    raise
            if not handled:
                handled = await self._publish_validation_failure(preflight, exc)
            if handled:
                return ToolRunnerResult(handled=True, context=preflight)
            self._log_payload_error(exc)
            raise exc

        key_domain = str(idempotency_domain or payload.tool_name or tool_name_fallback or self.target)
        key = build_idempotency_key(
            self.idempotency_namespace,
            resolved_parts.project_id,
            key_domain,
            str(payload.tool_call_id),
        )

        base_context = ToolResultContext.from_cmd_data(
            project_id=resolved_parts.project_id,
            cmd_data=data,
            tool_call_meta={},
            subject=subject,
            parts=resolved_parts,
            headers=merged_headers,
            payload=payload,
            raw_data=data,
        )

        async def _leader_execute() -> Dict[str, Any]:
            active_context: ToolResultContext = base_context
            try:
                tool_context = await hydrate_tool_call_context(
                    cardbox=self.cardbox,
                    subject=subject,
                    parts=resolved_parts,
                    data=data,
                    headers=merged_headers,
                    payload=payload,
                    require_tool_call_type=self.require_tool_call_type,
                    validate_tool_call_id_match=self.validate_tool_call_id_match,
                )
                active_context = tool_context
                execution_output = await execute(tool_context)
                normalized_output = self._normalize_execution_output(
                    execution_output,
                    context=tool_context,
                )
            except ToolCallHydrationError as hydration_exc:
                failure_context = ToolResultContext.from_cmd_data(
                    project_id=resolved_parts.project_id,
                    cmd_data=data,
                    tool_call_meta=hydration_exc.tool_call_meta,
                    subject=subject,
                    parts=resolved_parts,
                    headers=merged_headers,
                    payload=payload,
                    raw_data=data,
                )
                active_context = failure_context
                normalized_output = self._build_failed_output(
                    context=failure_context,
                    exc=hydration_exc.cause,
                    function_name=payload.tool_name or tool_name_fallback or self.default_function_name,
                )
            except Exception as exc:  # noqa: BLE001
                normalized_output = self._build_failed_output(
                    context=active_context,
                    exc=exc,
                    function_name=payload.tool_name or tool_name_fallback or self.default_function_name,
                )
            await self.cardbox.save_card(normalized_output.result_card)
            return normalized_output.payload

        try:
            outcome = await self.idempotent_executor.execute(key, _leader_execute)
            effective_source = str(source_agent_id or self.source_agent_id)
            await self.ensure_request_edge_for_report(
                parts=resolved_parts,
                headers=merged_headers,
                payload=outcome.payload,
            )
            await self._publish_result(
                parts=resolved_parts,
                headers=merged_headers,
                payload=outcome.payload,
                source_agent_id=effective_source,
            )
            if outcome.is_leader:
                self.logger.info(
                    "%s tool_result sent (leader) tool_call_id=%s",
                    self.service_name,
                    payload.tool_call_id,
                )
            else:
                self.logger.info(
                    "%s tool_result re-sent (follower) tool_call_id=%s",
                    self.service_name,
                    payload.tool_call_id,
                )
            return ToolRunnerResult(
                handled=True,
                context=base_context,
                is_leader=outcome.is_leader,
                payload=outcome.payload,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.error("%s execution failed: %s", self.service_name, exc, exc_info=True)
            raise

    def _log_payload_error(self, exc: Exception) -> None:
        if isinstance(exc, ValidationError):
            missing = ToolCommandPayload.missing_fields_from_error(exc)
            self.logger.error(
                "%s missing required tool payload fields: %s",
                self.service_name,
                ",".join(missing) or "invalid payload",
            )
            return
        if isinstance(exc, BadRequestError):
            self.logger.error("%s bad tool payload: %s", self.service_name, exc)
            return
        if isinstance(exc, ProtocolViolationError):
            self.logger.error("%s protocol violation: %s", self.service_name, exc)
            return
        self.logger.error("%s invalid tool payload: %s", self.service_name, exc)

    async def _publish_validation_failure(self, preflight: ToolPreflightContext, exc: Exception) -> bool:
        cmd_data = self._build_validation_cmd_data(preflight.data)
        if cmd_data is None:
            return False
        normalized_exc: Exception = exc
        if isinstance(exc, ValidationError):
            missing = ToolCommandPayload.missing_fields_from_error(exc)
            normalized_exc = BadRequestError(f"missing required fields {missing or ['unknown']}")
        result_context = ToolResultContext.from_cmd_data(
            project_id=preflight.parts.project_id,
            cmd_data=cmd_data,
            tool_call_meta={},
            subject=preflight.subject,
            parts=preflight.parts,
            headers=preflight.headers,
            raw_data=preflight.data,
        )
        builder = ToolResultBuilder(
            result_context,
            author_id=self.result_author_id,
            function_name=cmd_data.get("tool_name") or self.default_function_name,
            error_source="tool",
        )
        payload, result_card = builder.fail(
            normalized_exc,
            function_name=cmd_data.get("tool_name") or self.default_function_name,
            after_execution=cmd_data.get("after_execution"),
        )
        await self.cardbox.save_card(result_card)
        await self.ensure_request_edge_for_report(
            parts=preflight.parts,
            headers=preflight.headers,
            payload=payload,
        )
        await self._publish_result(
            parts=preflight.parts,
            headers=preflight.headers,
            payload=payload,
            source_agent_id=self.source_agent_id,
        )
        self.logger.info(
            "%s tool_result sent (validation_failed) tool_call_id=%s",
            self.service_name,
            str(cmd_data.get("tool_call_id") or ""),
        )
        return True

    def _build_failed_output(
        self,
        *,
        context: ToolResultContext,
        exc: Exception,
        function_name: Optional[str],
    ) -> ToolExecutionOutput:
        builder = ToolResultBuilder(
            context,
            author_id=self.result_author_id,
            function_name=function_name or self.default_function_name,
            error_source="tool",
        )
        payload, result_card = builder.fail(
            exc,
            function_name=function_name or self.default_function_name,
        )
        return ToolExecutionOutput(payload=payload, result_card=result_card)

    @staticmethod
    def _build_validation_cmd_data(data: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        tool_call_id = ToolRunner._safe_str(data.get("tool_call_id"))
        agent_turn_id = ToolRunner._safe_str(data.get("agent_turn_id"))
        agent_id = ToolRunner._safe_str(data.get("agent_id"))
        if not tool_call_id or not agent_turn_id or not agent_id:
            return None
        try:
            turn_epoch = int(data.get("turn_epoch"))
        except Exception:
            return None
        after_execution = ToolRunner._safe_str(data.get("after_execution")) or "suspend"
        tool_name = ToolRunner._safe_str(data.get("tool_name"))
        cmd_data: Dict[str, Any] = {
            "tool_call_id": tool_call_id,
            "agent_turn_id": agent_turn_id,
            "turn_epoch": turn_epoch,
            "agent_id": agent_id,
            "after_execution": after_execution,
            "tool_name": tool_name,
        }
        tool_call_card_id = ToolRunner._safe_str(data.get("tool_call_card_id"))
        if tool_call_card_id:
            cmd_data["tool_call_card_id"] = tool_call_card_id
        return cmd_data

    @staticmethod
    def _safe_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    async def _publish_result(
        self,
        *,
        parts: SubjectParts,
        headers: Dict[str, str],
        payload: Dict[str, Any],
        source_agent_id: str,
    ) -> None:
        if self.publish_result is not None:
            await self.publish_result(parts, headers, payload, source_agent_id)
            return
        await publish_tool_result_report(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=self.state_store,
            l0_engine=self.l0_engine,
            project_id=parts.project_id,
            channel_id=parts.channel_id,
            payload=payload,
            headers=headers,
            source_agent_id=source_agent_id,
        )

    async def ensure_request_edge_for_report(
        self,
        *,
        parts: SubjectParts,
        headers: Mapping[str, str],
        payload: Dict[str, Any],
    ) -> None:
        correlation_id = self._safe_str(payload.get("tool_call_id"))
        if not correlation_id:
            return
        if not hasattr(self.execution_store, "get_request_recursion_depth"):
            return
        if not hasattr(self.execution_store, "insert_execution_edge"):
            return
        try:
            existing_depth = await self.execution_store.get_request_recursion_depth(
                project_id=parts.project_id,
                correlation_id=correlation_id,
            )
            if existing_depth is not None:
                return
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "%s request-edge depth lookup failed tool_call_id=%s: %s",
                self.service_name,
                correlation_id,
                exc,
            )
            return

        try:
            depth = int(require_recursion_depth(headers))
        except Exception:
            depth = 0

        agent_id = self._safe_str(payload.get("agent_id"))
        agent_turn_id = self._safe_str(payload.get("agent_turn_id"))
        step_id = self._safe_str(payload.get("step_id"))
        trace_id = self._safe_str(payload.get("trace_id")) or trace_id_from_headers(headers)
        edge_id = f"edge_{uuid6.uuid7().hex}"

        try:
            await self.execution_store.insert_execution_edge(
                edge_id=edge_id,
                project_id=parts.project_id,
                channel_id=parts.channel_id,
                primitive="tool_call",
                edge_phase="request",
                source_agent_id=agent_id,
                source_agent_turn_id=agent_turn_id,
                source_step_id=step_id,
                target_agent_id=agent_id,
                target_agent_turn_id=agent_turn_id,
                correlation_id=correlation_id,
                enqueue_mode=None,
                recursion_depth=int(depth),
                trace_id=trace_id,
                parent_step_id=step_id,
                metadata={
                    "tool_call_id": correlation_id,
                    "source": "tool_runner_backfill",
                    "service": self.service_name,
                    "recursion_depth": int(depth),
                },
            )
            self.logger.warning(
                "%s backfilled missing request edge tool_call_id=%s depth=%s",
                self.service_name,
                correlation_id,
                int(depth),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "%s request-edge backfill failed tool_call_id=%s: %s",
                self.service_name,
                correlation_id,
                exc,
            )

    def _normalize_execution_output(
        self,
        output: ToolExecutionOutputLike,
        *,
        context: ToolResultContext,
    ) -> ToolExecutionOutput:
        if isinstance(output, ToolExecutionOutput):
            return output
        if isinstance(output, tuple) and len(output) == 2:
            payload, result_card = output
            if not isinstance(payload, dict):
                raise TypeError("Tool execution payload must be a dict")
            return ToolExecutionOutput(payload=payload, result_card=result_card)
        payload, result_card = ToolResultBuilder(
            context,
            author_id=self.result_author_id,
            function_name=context.tool_name or self.default_function_name,
            error_source="tool",
        ).success(output)
        return ToolExecutionOutput(payload=payload, result_card=result_card)
