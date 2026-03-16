from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Tuple, Union

from pydantic import ValidationError

from core.cg_context import CGContext
from core.errors import BadRequestError, ProtocolViolationError
from core.subject import SubjectParts, parse_subject
from core.utils import safe_str
from core.utp_protocol import ToolCommandPayload
from infra.messaging.ingress import build_nats_ingress_context
from infra.idempotency import IdempotentExecutor, NatsKvIdempotencyStore, build_idempotency_key
from infra.l0_engine import L0Engine
from infra.l0.tool_reports import publish_tool_result_report
from infra.tool_executor import (
    ToolCallEnvelope,
    ToolCallHydrationError,
    ToolResultBuilder,
    ToolResultContext,
    hydrate_tool_call_context,
)
from services.tools.tool_helpers import build_validation_failure_package


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
    parts: SubjectParts
    ctx: CGContext
    data: Dict[str, Any]

@dataclass(frozen=True)
class ToolExecutionOutput:
    payload: Dict[str, Any]
    result_card: Any


@dataclass(frozen=True)
class ToolRunnerResult:
    handled: bool
    is_leader: Optional[bool] = None
    payload: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ToolIdempotency:
    store: NatsKvIdempotencyStore
    executor: IdempotentExecutor


ToolExecutionOutputLike = Union[ToolExecutionOutput, Tuple[Dict[str, Any], Any], Any]
ToolExecuteFn = Callable[[ToolCallEnvelope], Awaitable[ToolExecutionOutputLike]]
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
        # L0 source validation binds the reported actor to the tool.result author.
        self.result_author_id = str(result_author_id or self.source_agent_id)
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
        tool_name_fallback: Optional[str] = None,
        idempotency_domain: Optional[str] = None,
        source_agent_id: Optional[str] = None,
        on_validation_error: Optional[ToolValidationErrorHandler] = None,
    ) -> ToolRunnerResult:
        resolved_parts = parse_subject(subject)
        if (
            resolved_parts is None
            or resolved_parts.category != self.category
            or resolved_parts.component != self.component
            or resolved_parts.target != self.target
        ):
            return ToolRunnerResult(handled=False)

        try:
            ingress_ctx, _ = build_nats_ingress_context(
                headers=headers,
                raw_payload=data,
                subject_parts=resolved_parts,
            )
        except ProtocolViolationError as exc:
            self.logger.error("%s protocol violation: %s", self.service_name, exc)
            try:
                ingress_ctx, _ = build_nats_ingress_context(
                    headers=headers,
                    raw_payload={},
                    subject_parts=resolved_parts,
                )
            except ProtocolViolationError:
                return ToolRunnerResult(handled=True)
            handled = await self._publish_validation_failure(
                ToolPreflightContext(
                    parts=resolved_parts,
                    ctx=ingress_ctx,
                    data=dict(data or {}),
                ),
                exc,
            )
            if handled:
                return ToolRunnerResult(handled=True)
            return ToolRunnerResult(handled=True)

        preflight = ToolPreflightContext(
            parts=resolved_parts,
            ctx=ingress_ctx,
            data=dict(data or {}),
        )
        try:
            payload = self.payload_validation.validate(dict(data or {}))
            resolved_tool_call_id = ingress_ctx.require_tool_call_id
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
                return ToolRunnerResult(handled=True)
            self._log_payload_error(exc)
            raise exc

        key_domain = str(idempotency_domain or payload.tool_name or tool_name_fallback or self.target)
        key = build_idempotency_key(
            self.idempotency_namespace,
            resolved_parts.project_id,
            key_domain,
            str(resolved_tool_call_id),
        )

        base_context = ToolResultContext.from_cmd_data(
            ctx=ingress_ctx,
            cmd_data=dict(data or {}),
            payload=payload,
        )

        async def _leader_execute() -> Dict[str, Any]:
            active_context: ToolResultContext = base_context
            try:
                tool_context = await hydrate_tool_call_context(
                    cardbox=self.cardbox,
                    cg_ctx=ingress_ctx,
                    data=dict(data or {}),
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
                tool_meta = dict(hydration_exc.tool_call_meta or {})
                failure_ctx = ingress_ctx.with_lineage_meta(tool_meta)
                failure_context = ToolResultContext(
                    ctx=failure_ctx,
                    cmd_data=dict(data or {}),
                    payload=payload,
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
            source_ctx = ingress_ctx.evolve(agent_id=effective_source, agent_turn_id="", step_id=None)
            report_ctx = ingress_ctx.with_tool_call(str(resolved_tool_call_id))
            await self._publish_result(
                parts=resolved_parts,
                payload=outcome.payload,
                target_ctx=report_ctx,
                source_ctx=source_ctx,
            )
            if outcome.is_leader:
                self.logger.info(
                    "%s tool_result sent (leader) tool_call_id=%s",
                    self.service_name,
                    resolved_tool_call_id,
                )
            else:
                self.logger.info(
                    "%s tool_result re-sent (follower) tool_call_id=%s",
                    self.service_name,
                    resolved_tool_call_id,
                )
            return ToolRunnerResult(
                handled=True,
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
        package = await build_validation_failure_package(
            cardbox=self.cardbox,
            ingress_ctx=preflight.ctx,
            data=preflight.data,
            exc=exc,
            default_tool_name=self.default_function_name,
            author_id=self.result_author_id,
        )
        if package is None:
            return False
        validation_ctx, payload, result_card = package
        await self.cardbox.save_card(result_card)
        await self._publish_result(
            parts=preflight.parts,
            payload=payload,
            target_ctx=validation_ctx,
            source_ctx=validation_ctx.evolve(agent_id=self.source_agent_id, agent_turn_id="", step_id=None),
        )
        self.logger.info(
            "%s tool_result sent (validation_failed) tool_call_id=%s",
            self.service_name,
            str(validation_ctx.tool_call_id or ""),
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

    async def _publish_result(
        self,
        *,
        parts: SubjectParts,
        payload: Dict[str, Any],
        target_ctx: CGContext,
        source_ctx: CGContext,
    ) -> None:
        source_agent_id = str(source_ctx.agent_id or self.source_agent_id)
        headers = target_ctx.to_nats_headers()
        if self.publish_result is not None:
            await self.publish_result(parts, headers, payload, source_agent_id)
            return
        await publish_tool_result_report(
            nats=self.nats,
            execution_store=self.execution_store,
            cardbox=self.cardbox,
            resource_store=self.resource_store,
            state_store=self.state_store,
            l0_engine=self.l0_engine,
            source_ctx=source_ctx,
            target_ctx=target_ctx,
            payload=payload,
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
