"""
Jina OpenAPI Tool Service

This service bridges UTP tool commands to Jina Search/Reader HTTP endpoints.
It keeps the ToolRunner + Inbox/Wakeup callback contract unchanged.

References:
- https://s.jina.ai/docs
- https://r.jina.ai/docs

Usage:
    uv run services/tools/openapi_service.py --target-service jina

Behavior:
    1. Listens on: cg.{PROTOCOL_VERSION}.*.*.cmd.tool.{target_service}.*
    2. Validates UTP hard-cut fields (no inline args).
    3. Fetches tool definition from `resource.tools`.
    4. Reads `options.jina` (preferred) or legacy `options.openapi` config.
    5. Executes request against s.jina.ai / r.jina.ai.
    6. Writes tool.result, reports to Inbox, emits cmd.agent.{worker_target}.wakeup.
"""

import argparse
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import quote, urlparse

import httpx

from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config, config_to_dict
from core.errors import BadRequestError, NotFoundError, ProtocolViolationError, UpstreamUnavailableError
from core.subject import parse_subject, subject_pattern
from core.utils import set_loop_policy
from core.utp_protocol import ALLOWED_AFTER_EXECUTION_VALUES, extract_after_execution_override
from infra.agent_routing import resolve_agent_target
from infra.observability.otel import get_tracer, mark_span_error, traced
from infra.service_runtime import ServiceBase
from infra.tool_executor import ToolCallContext
from services.tools.tool_runner import ToolPayloadValidation, ToolRunner, build_tool_idempotency

set_loop_policy()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("OpenAPI")
_TRACER = get_tracer("services.tools.openapi_service")

_JINA_SEARCH_BASE_URL = "https://s.jina.ai"
_JINA_READER_BASE_URL = "https://r.jina.ai"
_ALLOWED_JINA_HOSTS = {"s.jina.ai", "r.jina.ai"}
_DEFAULT_JINA_AUTH_ENV = "JINA_API_KEY"


def _subject_span_attrs(args: Dict[str, Any]) -> Dict[str, Any]:
    return {"cg.subject": str(args.get("subject"))}


def _safe_host(base_url: str) -> str:
    parsed = urlparse(base_url or "")
    return str(parsed.hostname or "").lower()


class OpenAPIToolService(ServiceBase):
    def __init__(
        self,
        cfg: Dict[str, Any],
        target_service: str,
        *,
        service_envs: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(cfg, use_nats=True, use_cardbox=True)
        self.cfg = cfg or {}
        self.target_service = target_service
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        self.http_client = httpx.AsyncClient(timeout=30.0)

        idempotency = build_tool_idempotency(self.nats)
        self.idempotency_store = idempotency.store
        self.idempotent_executor = idempotency.executor
        self.tool_runner = ToolRunner(
            target=self.target_service,
            source_agent_id="tool.openapi",
            nats=self.nats,
            cardbox=self.cardbox,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            idempotent_executor=self.idempotent_executor,
            payload_validation=ToolPayloadValidation(
                forbid_keys={"args", "arguments", "result"},
                require_tool_call_card_id=True,
                require_tool_name=True,
                require_after_execution=True,
                allowed_after_execution=ALLOWED_AFTER_EXECUTION_VALUES,
            ),
            result_author_id=f"tool.openapi.{self.target_service}",
            logger=logger,
            service_name=f"OpenAPI[{self.target_service}]",
            require_tool_call_type=True,
            validate_tool_call_id_match=True,
        )
        self.service_envs = service_envs if isinstance(service_envs, dict) else {}

    async def start(self):
        try:
            await self.open()
            subject = subject_pattern(
                project_id="*",
                channel_id="*",
                category="cmd",
                component="tool",
                target=self.target_service,
                suffix="*",
                protocol_version=PROTOCOL_VERSION,
            )
            queue_group = f"tool_{self.target_service}_openapi"

            logger.info("Starting OpenAPI service for target='%s'", self.target_service)
            logger.info("Listening on: %s (queue=%s)", subject, queue_group)

            await self.nats.subscribe_cmd(
                subject,
                queue_group,
                self._handle_cmd,
                durable_name=f"{queue_group}_cmd_{PROTOCOL_VERSION}",
                deliver_policy="all",
            )
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            logger.info("Stopping service...")
        finally:
            await self.cleanup()

    async def cleanup(self):
        await self.http_client.aclose()
        await self.close()
        logger.info("Service stopped.")

    @traced(_TRACER, "tool.execute_logic", span_arg="_span")
    async def _execute_logic(self, ctx: ToolCallContext, _span: Any = None) -> Dict[str, Any]:
        parts = ctx.parts
        payload = ctx.payload
        args = ctx.args
        span = _span
        if span is not None:
            span.set_attribute("cg.project_id", str(parts.project_id))

        tool_name = payload.tool_name or self.target_service
        agent_turn_id = payload.agent_turn_id
        tool_call_id = payload.tool_call_id
        after_execution = payload.after_execution

        logger.info("Received CMD: %s (turn=%s, call=%s)", tool_name, agent_turn_id, tool_call_id)

        target = await resolve_agent_target(
            resource_store=self.resource_store,
            project_id=parts.project_id,
            agent_id=str(payload.agent_id),
        )
        if not target:
            raise ProtocolViolationError(
                "missing worker_target for agent",
                detail={
                    "project_id": parts.project_id,
                    "agent_id": str(payload.agent_id),
                    "tool_call_id": str(tool_call_id),
                },
            )

        tool_def = await self.resource_store.fetch_tool_definition(
            project_id=parts.project_id,
            tool_name=str(tool_name),
        )
        if not tool_def:
            raise NotFoundError(f"Tool '{tool_name}' not found in DB")

        db_after_execution = tool_def.get("after_execution")
        if db_after_execution in ALLOWED_AFTER_EXECUTION_VALUES:
            if after_execution not in (None, "", db_after_execution):
                logger.warning(
                    "after_execution mismatch: payload=%r db=%r; using db default for callback",
                    after_execution,
                    db_after_execution,
                )
            ctx.override_after_execution(str(db_after_execution))

        tool_options = tool_def.get("options", {}) if isinstance(tool_def.get("options"), dict) else {}
        jina_config = tool_options.get("jina") if isinstance(tool_options.get("jina"), dict) else {}
        openapi_config = tool_options.get("openapi") if isinstance(tool_options.get("openapi"), dict) else {}

        if not jina_config and not openapi_config:
            raise BadRequestError(
                f"Tool '{tool_name}' missing options.jina/options.openapi config"
            )

        start = time.monotonic()
        result = await self._execute_jina_http(
            jina_config=jina_config,
            openapi_config=openapi_config,
            tool_options=tool_options,
            args=args,
        )
        latency_ms = int((time.monotonic() - start) * 1000)

        override_after_execution = extract_after_execution_override(result)
        if isinstance(result, dict):
            control = result.get("__cg_control")
            if isinstance(control, dict):
                candidate = control.get("after_execution")
                if override_after_execution is None and candidate not in (None, ""):
                    logger.warning(
                        "OpenAPI result.__cg_control.after_execution invalid: %r (tool=%s call=%s)",
                        candidate,
                        tool_name,
                        tool_call_id,
                    )

        if isinstance(result, dict) and result.get("error") is not None:
            status_code = result.get("status_code")
            message = str(result.get("error") or "jina_upstream_error")
            detail = {
                "status_code": status_code,
                "details": result.get("details"),
                "tool_name": tool_name,
                "tool_call_id": tool_call_id,
            }
            if isinstance(status_code, int) and 400 <= status_code < 500:
                raise BadRequestError(message, detail=detail)
            raise UpstreamUnavailableError(message, detail=detail)

        if override_after_execution is not None:
            logger.info(
                "AUDIT openapi __cg_control override tool=%s tool_call_id=%s caller=%s override=%s base=%s",
                tool_name,
                tool_call_id,
                payload.agent_id,
                override_after_execution,
                ctx.base_after_execution,
            )

        if isinstance(result, dict):
            result_obj = dict(result)
            result_obj.setdefault("latency_ms", latency_ms)
            return result_obj
        return {"value": result, "latency_ms": latency_ms}

    @traced(
        _TRACER,
        "tool.handle_cmd",
        headers_arg="headers",
        include_links=True,
        mark_error_on_exception=True,
        span_arg="_span",
        attributes_getter=_subject_span_attrs,
    )
    async def _handle_cmd(
        self,
        subject: str,
        data: Dict[str, Any],
        headers: Dict[str, str],
        _span: Any = None,
    ):
        span = _span
        try:
            parts = parse_subject(subject)
            if not parts or parts.target != self.target_service:
                return

            async def _execute(ctx: ToolCallContext) -> Dict[str, Any]:
                payload = ctx.payload
                if span is not None:
                    span.set_attribute("cg.project_id", str(ctx.parts.project_id))
                    span.set_attribute("cg.channel_id", str(ctx.parts.channel_id))
                    span.set_attribute("cg.tool_name", str(payload.tool_name or ""))
                    span.set_attribute("cg.tool_call_id", str(payload.tool_call_id or ""))
                    span.set_attribute("cg.agent_id", str(payload.agent_id or ""))
                    span.set_attribute("cg.agent_turn_id", str(payload.agent_turn_id or ""))
                    span.set_attribute("cg.after_execution", str(payload.after_execution or ""))
                logger.info("Leader acquired lock tool_call_id=%s", payload.tool_call_id)
                return await self._execute_logic(ctx)

            await self.tool_runner.run(
                subject=subject,
                data=data,
                headers=headers,
                execute=_execute,
                parts=parts,
                tool_name_fallback=self.target_service,
                source_agent_id="tool.openapi",
            )
        except Exception:
            logger.exception("Unexpected error handling %s", subject)
            raise

    def _resolve_timeout_s(
        self,
        tool_options: Dict[str, Any],
        jina_config: Dict[str, Any],
        openapi_config: Dict[str, Any],
    ) -> Optional[float]:
        candidates = [
            jina_config.get("timeout_s"),
            jina_config.get("timeout"),
            openapi_config.get("timeout_s"),
            openapi_config.get("timeout"),
            tool_options.get("timeout_s"),
            tool_options.get("timeout"),
        ]
        for v in candidates:
            if v is None:
                continue
            try:
                timeout_s = float(v)
            except Exception:
                continue
            if timeout_s > 0:
                return timeout_s
        return None

    def _apply_envs(self, args: Dict[str, Any], tool_options: Dict[str, Any]) -> Dict[str, Any]:
        final_args = dict(args) if isinstance(args, dict) else {}
        envs: Dict[str, Any] = {}
        tool_envs = tool_options.get("envs") if isinstance(tool_options, dict) else None
        if isinstance(tool_envs, dict):
            envs.update(tool_envs)
        if isinstance(self.service_envs, dict) and self.service_envs:
            envs.update(self.service_envs)
        if not envs:
            return final_args

        for key, env_name in envs.items():
            if not isinstance(key, str):
                continue
            if not isinstance(env_name, str) or not env_name.strip():
                continue
            value = os.getenv(env_name)
            if value is None or value == "":
                logger.warning("Env var '%s' missing or empty for arg '%s'", env_name, key)
                continue
            final_args[key] = value
        return final_args

    def _resolve_jina_mode(self, jina_config: Dict[str, Any], openapi_config: Dict[str, Any]) -> str:
        mode = str(jina_config.get("mode") or "").strip().lower()
        if mode in {"search", "reader"}:
            return mode
        if mode in {"read", "crawl"}:
            return "reader"

        base_candidates = [
            jina_config.get("base_url"),
            openapi_config.get("base_url"),
        ]
        for candidate in base_candidates:
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            host = _safe_host(candidate.strip())
            if host == "s.jina.ai":
                return "search"
            if host == "r.jina.ai":
                return "reader"

        path = str(openapi_config.get("path") or "").strip().lower()
        if path == "/search":
            return "search"

        raise BadRequestError(
            "unable to resolve jina mode; set options.jina.mode to 'search' or 'reader'"
        )

    def _resolve_jina_base_url(
        self,
        *,
        mode: str,
        jina_config: Dict[str, Any],
        openapi_config: Dict[str, Any],
    ) -> str:
        default_base = _JINA_SEARCH_BASE_URL if mode == "search" else _JINA_READER_BASE_URL
        raw = str(
            jina_config.get("base_url")
            or openapi_config.get("base_url")
            or default_base
        ).strip()
        host = _safe_host(raw)
        if host not in _ALLOWED_JINA_HOSTS:
            raise BadRequestError(
                f"unsupported base_url host '{host}' for openapi_service; only s.jina.ai/r.jina.ai are allowed"
            )
        if mode == "search" and host != "s.jina.ai":
            raise BadRequestError("jina search mode requires base_url host 's.jina.ai'")
        if mode == "reader" and host != "r.jina.ai":
            raise BadRequestError("jina reader mode requires base_url host 'r.jina.ai'")
        return raw.rstrip("/")

    @staticmethod
    def _load_service_envs(config: Dict[str, Any], target_service: str) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        if not isinstance(config, dict):
            return merged

        openapi_cfg = config.get("openapi")
        if isinstance(openapi_cfg, dict) and isinstance(openapi_cfg.get("envs"), dict):
            merged.update(openapi_cfg["envs"])

        tools_cfg = config.get("tools")
        if isinstance(tools_cfg, dict):
            openapi_tool_cfg = tools_cfg.get("openapi")
            if isinstance(openapi_tool_cfg, dict):
                if isinstance(openapi_tool_cfg.get("envs"), dict):
                    merged.update(openapi_tool_cfg["envs"])
                services_cfg = openapi_tool_cfg.get("services")
                if isinstance(services_cfg, dict):
                    svc_cfg = services_cfg.get(target_service)
                    if isinstance(svc_cfg, dict) and isinstance(svc_cfg.get("envs"), dict):
                        merged.update(svc_cfg["envs"])
            openapi_services_cfg = tools_cfg.get("openapi_services")
            if isinstance(openapi_services_cfg, dict):
                svc_cfg = openapi_services_cfg.get(target_service)
                if isinstance(svc_cfg, dict) and isinstance(svc_cfg.get("envs"), dict):
                    merged.update(svc_cfg["envs"])

            jina_tool_cfg = tools_cfg.get("jina")
            if isinstance(jina_tool_cfg, dict):
                if isinstance(jina_tool_cfg.get("envs"), dict):
                    merged.update(jina_tool_cfg["envs"])
                services_cfg = jina_tool_cfg.get("services")
                if isinstance(services_cfg, dict):
                    svc_cfg = services_cfg.get(target_service)
                    if isinstance(svc_cfg, dict) and isinstance(svc_cfg.get("envs"), dict):
                        merged.update(svc_cfg["envs"])
        return merged

    def _resolve_jina_auth_header(
        self,
        *,
        jina_config: Dict[str, Any],
        openapi_config: Dict[str, Any],
    ) -> Optional[str]:
        auth_env = str(
            jina_config.get("auth_env")
            or (openapi_config.get("auth") or {}).get("env")
            or _DEFAULT_JINA_AUTH_ENV
        ).strip()
        if not auth_env:
            return None
        token = os.getenv(auth_env)
        if not token:
            logger.warning("Jina auth env '%s' missing or empty; request will be anonymous", auth_env)
            return None
        return f"Bearer {token}"

    def _build_request_headers(
        self,
        *,
        jina_config: Dict[str, Any],
        openapi_config: Dict[str, Any],
    ) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if isinstance(openapi_config.get("headers"), dict):
            for k, v in openapi_config["headers"].items():
                if isinstance(k, str) and k.strip():
                    headers[k] = str(v)
        if isinstance(jina_config.get("headers"), dict):
            for k, v in jina_config["headers"].items():
                if isinstance(k, str) and k.strip():
                    headers[k] = str(v)

        auth_value = self._resolve_jina_auth_header(jina_config=jina_config, openapi_config=openapi_config)
        if auth_value:
            headers["Authorization"] = auth_value
        headers.setdefault("Accept", str(jina_config.get("accept") or "text/plain"))
        return headers

    async def _execute_jina_http(
        self,
        *,
        jina_config: Dict[str, Any],
        openapi_config: Dict[str, Any],
        tool_options: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Dict[str, Any]:
        mode = self._resolve_jina_mode(jina_config, openapi_config)
        method = str(jina_config.get("method") or openapi_config.get("method") or "GET").upper()
        base_url = self._resolve_jina_base_url(mode=mode, jina_config=jina_config, openapi_config=openapi_config)
        final_args = self._apply_envs(args, tool_options)

        defaults = jina_config.get("defaults") if isinstance(jina_config.get("defaults"), dict) else {}
        req_args = dict(defaults)
        req_args.update(final_args)

        headers = self._build_request_headers(jina_config=jina_config, openapi_config=openapi_config)
        timeout_s = self._resolve_timeout_s(tool_options, jina_config, openapi_config)

        if mode == "search":
            endpoint = str(jina_config.get("path") or openapi_config.get("path") or "/search").strip() or "/search"
            if not endpoint.startswith("/"):
                endpoint = f"/{endpoint}"
            query = req_args.get("q") or req_args.get("query")
            if query is None or str(query).strip() == "":
                raise BadRequestError("jina search requires args.query (or args.q)")
            req_args["q"] = str(query)
            req_args.pop("query", None)
            url = f"{base_url}{endpoint}"
            params = req_args if method == "GET" else {}
            json_body = req_args if method != "GET" else None
            return await self._do_http_request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json_body=json_body,
                timeout_s=timeout_s,
            )

        target_url = req_args.pop("url", None) or req_args.pop("target_url", None)
        if target_url is None or str(target_url).strip() == "":
            raise BadRequestError("jina reader requires args.url (or args.target_url)")

        endpoint_template = str(jina_config.get("endpoint_template") or "/{url}")
        encoded_target = quote(str(target_url).strip(), safe="")
        endpoint = endpoint_template.replace("{url}", encoded_target)
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        url = f"{base_url}{endpoint}"
        params = req_args if method == "GET" else {}
        json_body = req_args if method != "GET" else None
        return await self._do_http_request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json_body=json_body,
            timeout_s=timeout_s,
        )

    @traced(_TRACER, "tool.http_request", span_arg="_span")
    async def _do_http_request(
        self,
        *,
        method: str,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        json_body: Optional[Dict[str, Any]],
        timeout_s: Optional[float],
        _span: Any = None,
    ) -> Dict[str, Any]:
        span = _span
        try:
            logger.info("HTTP %s %s", method, url)
            if span is not None:
                span.set_attribute("http.method", str(method))
                span.set_attribute("url.full", str(url))

            response = await self.http_client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout_s,
            )
            if span is not None:
                span.set_attribute("http.status_code", int(response.status_code))

            logger.info("HTTP response %s", response.status_code)
            response.raise_for_status()

            content_type = str(response.headers.get("content-type") or "").lower()
            if "application/json" in content_type:
                try:
                    return response.json()
                except Exception:
                    pass
            return {"data": response.text}

        except httpx.HTTPStatusError as exc:
            mark_span_error(span, exc)
            text = exc.response.text if exc.response is not None else str(exc)
            logger.error("HTTP error: %s", text)
            return {
                "error": f"HTTP {exc.response.status_code if exc.response else 'error'}",
                "details": text,
                "status_code": exc.response.status_code if exc.response else None,
            }
        except Exception as exc:  # noqa: BLE001
            mark_span_error(span, exc)
            logger.error("Execution failed: %s", exc)
            return {"error": str(exc)}


async def main():
    parser = argparse.ArgumentParser(description="OpenAPI Tool Service for Jina Search/Reader")
    parser.add_argument(
        "--target-service",
        required=True,
        help="Provider name (recommended: jina). service listens on cmd.tool.<target>.*",
    )
    args = parser.parse_args()

    config = config_to_dict(load_app_config())
    pg_dsn = os.getenv("PG_DSN")
    cardbox_cfg = config.get("cardbox")
    if not isinstance(cardbox_cfg, dict):
        cardbox_cfg = {}
        config["cardbox"] = cardbox_cfg
    if pg_dsn:
        cardbox_cfg["postgres_dsn"] = pg_dsn
    else:
        cardbox_cfg.setdefault("postgres_dsn", "postgresql://postgres:postgres@localhost:5432/cardbox")

    service_envs = OpenAPIToolService._load_service_envs(config, args.target_service)
    service = OpenAPIToolService(
        cfg=config,
        target_service=args.target_service,
        service_envs=service_envs,
    )
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
