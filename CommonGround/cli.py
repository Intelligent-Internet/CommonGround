from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Callable, Mapping, TextIO

import httpx

from CommonGround.agent_client.caller_headers import agent_auth_headers
from CommonGround.agent_client.safe_suspend import try_suspend_after_context_fetch_error
from CommonGround.cli_config import DEFAULT_CONFIG_PATH, resolve_cli_config, write_cli_client_config
from CommonGround.cli_profiles import AgentProfile, CliProfileStore, default_token_file, profile_key, read_token_file, write_token_file
from CommonGround.contracts import AgentRef, ConflictError, DispatchAuthority, DispatchAuthorityMode, TurnOutcome, TurnRef, TurnSnapshot, TurnState, normalize_dispatch_anchor
from CommonGround.env import load_local_env
from CommonGround.version import get_package_version

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_ADMIN_BASE_URL = "http://127.0.0.1:8001"
DEFAULT_WORK_MEMORY_PROFILE_KIND = "byoa.work_memory_reporter.v1"
DEFAULT_CONVERSATION_WORKER_PROFILE_KIND = "byoa.conversation_worker.v1"
DEFAULT_MANUAL_SHELL_RUNTIME_KIND = "manual.shell.v1"
DEFAULT_LOCAL_PROJECT_ID = "cg-demo"
DEFAULT_LOCAL_CREATOR_REF = "local-dev"
SERVER_EXTRA_PRECONDITIONS = (
    "  Install the server-ready CLI package first:\n"
    "    uv tool install 'commonground-kernel[server]'\n"
    "  If dependencies are missing, cg reports missing_extra."
)
WAIT_TERMINAL_STATES = frozenset({TurnState.CLOSED})
ADMIN_SERVICE_PROFILE_BOOTSTRAP_ERROR_CODES = frozenset(
    {
        "project_not_seeded",
        "project_bootstrap_conflict",
        "admin_service_credential_required",
        "invitation_validator_required",
        "invitation_code_required",
        "invitation_code_invalid",
    }
)
ClaimAutoRenewer = None
_UNSET = object()


@dataclass(frozen=True)
class CliExitResult:
    payload: dict[str, Any]
    exit_code: int


@dataclass(frozen=True)
class ResolvedClientAuth:
    auth_token: str | None
    headers: dict[str, str] | None
    profile_name: str | None = None


class CliHandledError(Exception):
    def __init__(self, *, code: str, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class _CliArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        add_help = kwargs.pop("add_help", True)
        super().__init__(*args, add_help=False, **kwargs)
        if add_help:
            self.add_argument("-h", "--help", action="help", help="show this help message")

    def error(self, message: str) -> None:
        raise CliHandledError(code="invalid_arguments", message=message, status=2)

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if status == 0:
            raise SystemExit(status)
        raise CliHandledError(
            code="invalid_arguments",
            message=(message or f"argument parsing failed with status {status}").strip(),
            status=status,
        )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be greater than or equal to 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = _CliArgumentParser(
        prog="cg",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Agent work vanishes when the session ends. Every handoff restarts from fragments.\n"
            "\n"
            "CommonGround is an open-source ground layer for real human-agent and multi-agent work.\n"
            "It keeps agent work durable, public, and ready for the next human, agent, tool, or\n"
            "external runtime that picks it up.\n"
            "\n"
            "It is also a small constitutional ledger kernel: it preserves the minimum public facts,\n"
            "work boundaries, semantic ownership, and causal relationships needed for independent\n"
            "participants to cooperate without being absorbed into one central runtime."
        ),
        epilog=(
            "For local first run:\n"
            "\n"
            "  cg setup -h\n"
            "  cg local -h\n"
            "\n"
            "If an external Agent will interact with CommonGround, answer one question: what\n"
            "relationship should that Agent have with CommonGround?\n"
            "\n"
            "Choose one integration path:\n"
            "\n"
            "  1. A local Agent finished work and only needs to publish public work records\n"
            "     Use this when the Agent should not receive CommonGround Turns and should not\n"
            "     own worker lifecycle.\n"
            "     Learn with:\n"
            "       cg report -h\n"
            "       cg profile ensure-agent -h\n"
            "       cg report work-memory -h\n"
            "       cg turn context -h\n"
            "\n"
            "  2. An external runtime should receive and complete turn.conversation.v1 like a CG worker\n"
            "     Invitation admission is required because the resulting Agent accepts work.\n"
            "     Learn with:\n"
            "       cg admission invite create -h\n"
            "       cg agent join -h\n"
            "       cg worker loop -h\n"
            "       cg smoke pair -h\n"
            "\n"
            "  3. The runtime harness itself should understand CommonGround worker semantics\n"
            "     This is the advanced runtime integration lane.\n"
            "     Learn with:\n"
            "       cg worker -h\n"
            "       cg worker claim -h\n"
            "       cg dispatch -h\n"
            "       cg turn -h\n"
            "\n"
            "Repository:\n"
            "  https://github.com/Intelligent-Internet/CommonGround"
        ),
    )
    parser.add_argument("--version", action="version", version=f"cg {get_package_version()}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    dispatch = subparsers.add_parser(
        "dispatch",
        help="Dispatch a root turn to CommonGround (low-level)",
        description=(
            "Dispatch a root turn. Provide --request-id or --dispatch-key as the birth-time "
            "causality/idempotency anchor; if only one is set, cg mirrors it to the other."
        ),
        epilog=(
            "This is a low-level command.\n"
            "\n"
            "For end-to-end worker verification, prefer 'cg smoke pair'."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    dispatch.add_argument("--project-id", required=True, help="Project that receives the root turn.")
    dispatch.add_argument("--requested-by", required=True, help="Agent id of the authenticated requester.")
    dispatch.add_argument("--target-agent", required=True, help="Agent id that should receive the turn.")
    dispatch.add_argument("--turn-kind", required=True, help="Turn kind to dispatch, for example turn.conversation.v1.")
    dispatch.add_argument(
        "--request-id",
        help="External request/correlation anchor; at least one of --request-id or --dispatch-key is required.",
    )
    dispatch.add_argument(
        "--dispatch-key",
        help="Idempotency anchor; at least one of --request-id or --dispatch-key is required.",
    )
    dispatch_payload = dispatch.add_mutually_exclusive_group(required=True)
    dispatch_payload.add_argument("--payload-file", help="JSON file containing the opaque dispatch payload. Any JSON value is accepted; cg does not enforce a payload schema here.")
    dispatch_payload.add_argument("--payload-json", help="Inline JSON value to use as the opaque dispatch payload. Any JSON value is accepted; cg does not enforce a payload schema here.")
    dispatch_payload.add_argument("--payload-stdin", action="store_true", help="Read the opaque dispatch payload as JSON from stdin. Any JSON value is accepted; cg does not enforce a payload schema here.")
    dispatch.set_defaults(handler=_handle_dispatch)

    turn = subparsers.add_parser(
        "turn",
        help="Low-level turn read and resume operations",
        description=(
            "Read and resume existing Turns. Use this lane when you already have a Turn id and need "
            "to inspect context, wait for closure, or resume suspended work."
        ),
        epilog=(
            "Typical use:\n"
            "  Provide --project-id with the target --turn-id.\n"
            "\n"
            "Learn with:\n"
            "  cg turn get -h\n"
            "  cg turn context -h\n"
            "  cg turn wait -h\n"
            "  cg turn resume -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    turn_subparsers = turn.add_subparsers(dest="turn_command", required=True)

    turn_get = turn_subparsers.add_parser("get", help="Fetch a turn snapshot", parents=[_cg_service_cli_parent(caller=True)])
    turn_get.add_argument("--project-id", required=True, help="Project that owns the turn.")
    turn_get.add_argument("--turn-id", required=True, help="Turn id to fetch.")
    turn_get.set_defaults(handler=_handle_turn_get)

    turn_context = turn_subparsers.add_parser("context", help="Fetch a turn context with semantic records", parents=[_cg_service_cli_parent(caller=True)])
    turn_context.add_argument("--project-id", required=True, help="Project that owns the turn.")
    turn_context.add_argument("--turn-id", required=True, help="Turn id whose context should be fetched.")
    turn_context.add_argument("--after-turn-seq", type=int, default=0, help="Return context after this turn sequence offset. Default: 0.")
    turn_context.add_argument("--limit", type=_positive_int, default=100, help="Maximum context items to return. Default: 100.")
    turn_context.set_defaults(handler=_handle_turn_context)

    turn_wait = turn_subparsers.add_parser("wait", help="Wait for a turn to reach a terminal state", parents=[_cg_service_cli_parent(caller=True)])
    turn_wait.add_argument("--project-id", required=True, help="Project that owns the turn.")
    turn_wait.add_argument("--turn-id", required=True, help="Turn id to wait for.")
    turn_wait.add_argument("--timeout-seconds", type=_non_negative_float, default=60.0, help="Maximum seconds to wait. Default: 60.")
    turn_wait.add_argument("--poll-interval-ms", type=_positive_int, default=500, help="Polling interval in milliseconds. Default: 500.")
    turn_wait.set_defaults(handler=_handle_turn_wait)

    turn_resume = turn_subparsers.add_parser("resume", help="Resume a suspended turn", parents=[_cg_service_cli_parent()])
    turn_resume.add_argument("--project-id", required=True, help="Project that owns the suspended turn.")
    turn_resume.add_argument("--turn-id", required=True, help="Suspended turn id to resume.")
    turn_resume.add_argument("--requested-by", required=True, help="Agent id requesting the resume.")
    turn_resume.set_defaults(handler=_handle_turn_resume)

    agent = subparsers.add_parser(
        "agent",
        help="Agent join and lifecycle operations",
        description=(
            "Join a CommonGround deployment as an admitted Agent, or manage whether an Agent is "
            "accepting new work."
        ),
        epilog=(
            "Typical onboarding flow:\n"
            "  cg admission invite create -h\n"
            "  cg agent join -h\n"
            "\n"
            "Lifecycle controls:\n"
            "  cg agent drain -h\n"
            "  cg agent resume -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    agent_subparsers = agent.add_subparsers(dest="agent_command", required=True)

    agent_get = agent_subparsers.add_parser("get", help="Fetch an agent snapshot", parents=[_cg_service_cli_parent(caller=True)])
    agent_get.add_argument("--project-id", required=True, help="Project that owns the agent.")
    agent_get.add_argument("--agent-id", required=True, help="Agent id to fetch.")
    agent_get.set_defaults(handler=_handle_agent_get)

    agent_drain = agent_subparsers.add_parser("drain", help="Stop admitting new turns for an agent", parents=[_cg_service_cli_parent()])
    agent_drain.add_argument("--project-id", required=True, help="Project that owns the agent.")
    agent_drain.add_argument("--agent-id", required=True, help="Agent id to drain.")
    agent_drain.add_argument("--requested-by", help="Requester agent id for audit; defaults to --agent-id when omitted.")
    agent_drain.set_defaults(handler=_handle_agent_drain)

    agent_resume = agent_subparsers.add_parser("resume", help="Resume admitting new turns for an agent", parents=[_cg_service_cli_parent()])
    agent_resume.add_argument("--project-id", required=True, help="Project that owns the agent.")
    agent_resume.add_argument("--agent-id", required=True, help="Agent id to resume.")
    agent_resume.add_argument("--requested-by", help="Requester agent id for audit; defaults to --agent-id when omitted.")
    agent_resume.set_defaults(handler=_handle_agent_resume)

    agent_join = agent_subparsers.add_parser(
        "join",
        help="Redeem an Agent join code and write a local profile",
        description=(
            "Redeem a scoped Agent join code, then write local CLI config, profile, and AgentCredential "
            "token file."
        ),
        epilog=(
            "Preconditions:\n"
            "  A scoped join invite must already exist.\n"
            "\n"
            "Notes:\n"
            "  The Agent operator does not need the Admin Service bearer token.\n"
            "\n"
            "Example:\n"
            "  cg agent join http://127.0.0.1:8000 cgjoin_...\n"
            "  cg agent join --base-url http://cg.example --admin-base-url http://admin.example --join-code cgjoin_..."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_join_cli_parent()],
    )
    agent_join.add_argument("server_url", nargs="?", help="Single-port CommonGround/Admin Service URL.")
    agent_join.add_argument("join_code_arg", nargs="?", help="Scoped Agent join code.")
    agent_join.add_argument("--join-code", help="Scoped Agent join code. Use this with --base-url for explicit form.")
    agent_join.set_defaults(handler=_handle_agent_join, client_kind="none")

    provision = subparsers.add_parser(
        "provision",
        help="Provision-request dispatch",
        description="Dispatch provision requests to a provisioner Agent.",
        epilog=(
            "This is a low-level command.\n"
            "\n"
            "Use it when you already have a provisioner Agent and need to dispatch a\n"
            "turn.provision.agent.spawn.v1 request explicitly."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    provision_subparsers = provision.add_subparsers(dest="provision_command", required=True)

    provision_spawn = provision_subparsers.add_parser(
        "spawn",
        help="Dispatch a provision request for a role",
        description=(
            "Dispatch a provision request. Provide --request-id or --dispatch-key as the birth-time "
            "causality/idempotency anchor; if only one is set, cg mirrors it to the other."
        ),
        parents=[_cg_service_cli_parent()],
    )
    provision_spawn.add_argument("--project-id", required=True, help="Project that receives the provision request.")
    provision_spawn.add_argument("--requested-by", required=True, help="Agent id of the authenticated requester.")
    provision_spawn.add_argument("--provisioner-agent", required=True, help="Agent id of the provisioner.")
    provision_spawn.add_argument("--role", required=True, help="Requested provision role from the provisioner offer catalog.")
    provision_spawn.add_argument(
        "--request-id",
        help="External request/correlation anchor; at least one of --request-id or --dispatch-key is required.",
    )
    provision_spawn.add_argument(
        "--dispatch-key",
        help="Idempotency anchor; at least one of --request-id or --dispatch-key is required.",
    )
    provision_spawn.set_defaults(handler=_handle_provision_spawn)

    profile = subparsers.add_parser(
        "profile",
        help="Create or refresh a local Agent profile",
        description=(
            "Profiles let the CLI request and store an Agent credential through the Admin Service.\n"
            "\n"
            "Use this for work-memory reporter profiles and lower-level Admin Service testing. For new "
            "conversation-worker onboarding, prefer 'cg admission invite create' plus 'cg agent join'."
        ),
        epilog=(
            "If local setup is already prepared, start with:\n"
            "  cg profile ensure-agent -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    profile_subparsers = profile.add_subparsers(dest="profile_command", required=True)
    profile_ensure = profile_subparsers.add_parser(
        "ensure-agent",
        help="Ensure a local Agent profile and credential through Admin Service",
        description=(
            "Profiles let the CLI request and store an Agent credential through the Admin Service.\n"
            "\n"
            "Use Admin Service bearer auth to register or refresh an AgentCredential, then write the local "
            "destination profile named by --profile. The resulting Agent credential token is stored in a "
            "local token file."
        ),
        epilog=(
            "Preconditions:\n"
            "  A profile command needs Admin Service bearer auth through --admin-auth-token,\n"
            "  --admin-auth-token-file, or config.\n"
            "  The target project must already be admitted by the target Admin Service.\n"
            "  After 'cg setup project seed --default-local' plus 'cg local run', that project is cg-demo.\n"
            "\n"
            "Recommended use:\n"
            "  For new conversation-worker onboarding, prefer 'cg admission invite create' plus\n"
            "  'cg agent join'. 'cg profile ensure-agent' remains useful for work-memory reporter\n"
            "  profiles and lower-level Admin Service testing.\n"
            "\n"
            "Example:\n"
            "  cg profile ensure-agent --profile cg-demo/reporter --project-id cg-demo --requested-agent-id reporter \\\n"
            "    --runtime-kind external-runtime.v1 --display-name Reporter"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_admin_cli_parent(base_url=True, profile=True)],
    )
    profile_ensure.add_argument("--project-id", required=True, help="Project where the Agent profile belongs.")
    profile_ensure.add_argument("--requested-agent-id", required=True, help="Agent id to register or refresh through Admin Service.")
    profile_ensure.add_argument("--profile-kind", default=DEFAULT_WORK_MEMORY_PROFILE_KIND, help=f"Profile kind to request. Default: {DEFAULT_WORK_MEMORY_PROFILE_KIND}.")
    profile_invitation = profile_ensure.add_mutually_exclusive_group()
    profile_invitation.add_argument(
        "--invitation-code-file",
        help="File containing a BYOA invitation code. Prefer this for conversation-worker profile bootstrap.",
    )
    profile_invitation.add_argument(
        "--invitation-code",
        help="BYOA invitation code for local demos. Prefer --invitation-code-file to avoid shell history exposure.",
    )
    profile_ensure.add_argument("--runtime-kind", required=True, help="Runtime kind recorded for this Agent profile.")
    profile_ensure.add_argument("--display-name", required=True, help="Human-readable Agent profile name.")
    profile_ensure.add_argument("--description", help="Optional Agent profile description.")
    profile_ensure.set_defaults(handler=_handle_profile_ensure_agent, client_kind="none")

    report = subparsers.add_parser(
        "report",
        help="Report local public work facts to CommonGround",
        description=(
            "Shallow BYOA work-memory reporting does not need NanoBot. It is a harness-agnostic path for "
            "an Agent that finishes local work first, then reports selected public work facts to "
            "CommonGround.\n"
            "\n"
            "Use this lane when the Agent should not receive CommonGround Turns and should not own "
            "worker lifecycle."
        ),
        epilog=(
            "Local first-run path:\n"
            "  uv tool install 'commonground-kernel[server]'\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local\n"
            "  cg setup project status --default-local\n"
            "  cg setup project client-config --default-local \\\n"
            "    --base-url http://127.0.0.1:8000 --admin-base-url http://127.0.0.1:8000\n"
            "  cg local run --project-id cg-demo --host 127.0.0.1 --port 8000\n"
            "\n"
            "After local setup is prepared:\n"
            "  cg profile ensure-agent -h\n"
            "  cg report work-memory -h\n"
            "  cg turn context -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    report_subparsers = report.add_subparsers(dest="report_command", required=True)
    report_work_memory = report_subparsers.add_parser(
        "work-memory",
        help="Submit a work-memory report",
        description=(
            "Shallow BYOA work-memory reporting does not need NanoBot. It is a harness-agnostic path for "
            "an Agent that finishes local work first, then reports selected public work facts to "
            "CommonGround.\n"
            "\n"
            "Use this lane when the Agent should not receive CommonGround Turns and should not own "
            "worker lifecycle.\n"
            "\n"
            "The manifest must be a JSON object. It must include request_id and a non-empty records list, "
            "and it must not include top-level meta."
        ),
        epilog=(
            "Preconditions:\n"
            "  Bootstrap the profile explicitly with 'cg profile ensure-agent', then submit the report\n"
            "  with the prepared profile.\n"
            "  The report must target a project admitted by the target CommonGround service.\n"
            "  After the default local setup path, that project is cg-demo.\n"
            "\n"
            "This lane does not require worker claims. It submits a born-closed work-memory report Turn.\n"
            "\n"
            "Manifest shape:\n"
            "  {\n"
            "    \"kind\": \"agent_work_memory_report_manifest.v1\",\n"
            "    \"request_id\": \"local-agent-report-001\",\n"
            "    \"summary\": \"Optional top-level summary.\",\n"
            "    \"records\": [\n"
            "      {\n"
            "        \"role\": \"summary\",\n"
            "        \"payload\": {\n"
            "          \"summary\": \"Public work facts only.\",\n"
            "          \"evidence\": [\"fact-1\", \"fact-2\"]\n"
            "        }\n"
            "      }\n"
            "    ],\n"
            "    \"final_payload\": {\n"
            "      \"kind\": \"agent_work_memory_report_result.v1\",\n"
            "      \"summary\": \"Optional final result payload.\"\n"
            "    }\n"
            "  }\n"
            "\n"
            "Manifest rules:\n"
            "  request_id is required.\n"
            "  records must be a non-empty list.\n"
            "  Each record must include role and payload.\n"
            "  Optional declared_project_id or declared_agent_id must match the trusted actor.\n"
            "  Unsupported top-level fields are rejected. Top-level meta is rejected.\n"
            "\n"
            "Example:\n"
            "  cg profile ensure-agent --profile cg-demo/reporter --project-id cg-demo --requested-agent-id reporter \\\n"
            "    --runtime-kind external-runtime.v1 --display-name Reporter\n"
            "\n"
            "  cg report work-memory --profile cg-demo/reporter --project-id cg-demo --agent-id reporter \\\n"
            "    --manifest-file report.json"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    report_work_memory.add_argument("--project-id", required=True, help="Project that owns the reporting Agent.")
    report_work_memory.add_argument("--agent-id", required=True, help="Reporting Agent id; caller identity is inferred from this value.")
    report_work_memory.add_argument(
        "--manifest-file",
        required=True,
        help=(
            "JSON object manifest to submit. Required: request_id and non-empty records. "
            "Top-level meta is rejected."
        ),
    )
    report_work_memory.add_argument("--request-id", help="Optional override for manifest request_id; must match if the manifest already has one.")
    report_work_memory.set_defaults(handler=_handle_report_work_memory)

    service = subparsers.add_parser(
        "service",
        help="Run the CommonGround Service",
        description="Run the CommonGround Service API as a long-running local operator process.",
        epilog=(
            "Preconditions:\n"
            f"{SERVER_EXTRA_PRECONDITIONS}\n"
            "  Set PG_DSN before running this command."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    service_subparsers = service.add_subparsers(dest="service_command", required=True)
    service_run = service_subparsers.add_parser(
        "run",
        help="Run the CommonGround service",
        description=(
            "Run the CommonGround service as a long-running local ops command. This command does not emit "
            "a JSON envelope."
        ),
        epilog=(
            "Preconditions:\n"
            f"{SERVER_EXTRA_PRECONDITIONS}\n"
            "  Set PG_DSN before running this command."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    service_run.set_defaults(handler=_handle_service_run, local_command=True)

    local = subparsers.add_parser(
        "local",
        help="Run CommonGround and Admin Service on one local port",
        description=(
            "Run the local first-run bundle on one port without merging CommonGround and Admin Service "
            "authority boundaries."
        ),
        epilog=(
            "Recommended first-run sequence:\n"
            "  uv tool install 'commonground-kernel[server]'\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local\n"
            "  cg setup project status --default-local\n"
            "  cg setup project client-config --default-local \\\n"
            "    --base-url http://127.0.0.1:8000 --admin-base-url http://127.0.0.1:8000\n"
            "  cg local run --project-id cg-demo --host 127.0.0.1 --port 8000"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    local_subparsers = local.add_subparsers(dest="local_command", required=True)
    local_run = local_subparsers.add_parser(
        "run",
        help="Run CommonGround Service and Admin Service on one local port",
        description="Run /v3r1 and /admin/v1 in one uvicorn process without merging their authority boundaries.",
        epilog=(
            "Preconditions:\n"
            f"{SERVER_EXTRA_PRECONDITIONS}\n"
            "  Provide PG_DSN or --pg-dsn.\n"
            "\n"
            "This is the recommended first-run path because it serves /v3r1 and /admin/v1\n"
            "from one local port."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    local_run.add_argument("--pg-dsn", help="PostgreSQL DSN. Defaults to PG_DSN.")
    local_run.add_argument("--project-id", help=f"Project namespace admitted by this API. Defaults to CG_PROJECT_ID or {DEFAULT_LOCAL_PROJECT_ID}.")
    local_run.add_argument("--admin-service-token-file", help="File containing the admin-service CG AgentCredential. Defaults to setup default.")
    local_run.add_argument("--admin-auth-token-file", help="File containing the Admin Service API bearer token. Defaults to setup default.")
    local_run.add_argument("--host", help="Bind host. Defaults to CG_HOST or 127.0.0.1.")
    local_run.add_argument("--port", type=int, help="Bind port. Defaults to CG_PORT or 8000.")
    local_run.add_argument("--base-url", help="Service URL used by the co-located Admin Service. Defaults to CG_BASE_URL or http://127.0.0.1:<port>.")
    local_run.add_argument("--log-level", help="uvicorn log level. Defaults to CG_LOG_LEVEL or info.")
    local_run.set_defaults(handler=_handle_local_run, local_command=True)

    admission = subparsers.add_parser(
        "admission",
        help="Create join invites and run Admin Service admission API",
        description=(
            "Run the product-side Admin Service or create scoped join invites for worker admission. "
            "Use this lane when an external runtime should become a CommonGround worker."
        ),
        epilog=(
            "Local first-run path:\n"
            "  uv tool install 'commonground-kernel[server]'\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local\n"
            "  cg setup project status --default-local\n"
            "  cg setup project client-config --default-local \\\n"
            "    --base-url http://127.0.0.1:8000 --admin-base-url http://127.0.0.1:8000\n"
            "  cg local run --project-id cg-demo --host 127.0.0.1 --port 8000\n"
            "\n"
            "Typical worker onboarding flow:\n"
            "  cg admission invite create -h\n"
            "  cg agent join -h\n"
            "  cg worker loop -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    admission_subparsers = admission.add_subparsers(dest="admission_command", required=True)
    admission_run = admission_subparsers.add_parser(
        "run",
        help="Run the Admin Service admission API",
        description="Run the product-side Admin Service admission API. This command does not emit a JSON envelope.",
        epilog=(
            "Preconditions:\n"
            f"{SERVER_EXTRA_PRECONDITIONS}\n"
            "  Provide PG_DSN or --pg-dsn.\n"
            "\n"
            "Invite config shape:\n"
            "  --invite-config-json is optional. When provided, it must load a JSON object with\n"
            "  an 'invitations' list, or a top-level list of invitation entries.\n"
            "  Each entry requires invite_id, project_id, issued_by_user_id, issuer_role,\n"
            "  allowed_profile_kinds, and code_sha256.\n"
            "  issuer_role must be project_owner. allowed_profile_kinds must be non-empty.\n"
            "\n"
            "Minimal invite config:\n"
            "  {\n"
            "    \"invitations\": [\n"
            "      {\n"
            "        \"invite_id\": \"invite-local-conversation\",\n"
            "        \"project_id\": \"cg-demo\",\n"
            "        \"issued_by_user_id\": \"user-123\",\n"
            "        \"issuer_role\": \"project_owner\",\n"
            "        \"allowed_profile_kinds\": [\"byoa.conversation_worker.v1\"],\n"
            "        \"code_sha256\": \"sha256:<64_hex_sha256_of_invitation_code>\",\n"
            "        \"enabled\": true\n"
            "      }\n"
            "    ]\n"
            "  }\n"
            "\n"
            "Use this separated-service path when you are not using 'cg local run'."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    admission_run.add_argument("--pg-dsn", help="PostgreSQL DSN. Defaults to PG_DSN.")
    admission_run.add_argument("--base-url", help=f"CommonGround service URL. Defaults to CG_BASE_URL or {DEFAULT_BASE_URL}.")
    admission_run.add_argument("--project-id", help=f"Project namespace admitted by this API. Defaults to CG_PROJECT_ID or {DEFAULT_LOCAL_PROJECT_ID}.")
    admission_run.add_argument("--admin-service-token-file", help="File containing the admin-service CG AgentCredential. Defaults to CG_ADMIN_SERVICE_TOKEN_FILE or setup default.")
    admission_run.add_argument("--admin-auth-token-file", help="File containing the Admin Service API bearer token. Defaults to CG_ADMIN_AUTH_TOKEN_FILE or setup default.")
    admission_run.add_argument("--host", help="Bind host. Defaults to CG_ADMIN_HOST or 127.0.0.1.")
    admission_run.add_argument("--port", type=int, help="Bind port. Defaults to CG_ADMIN_PORT or 8001.")
    admission_run.add_argument(
        "--invite-config-json",
        help=(
            "Optional BYOA invite config JSON file. Expected shape: object with an 'invitations' list "
            "or a top-level list of invitation entries. Defaults to CG_ADMIN_INVITE_CONFIG_JSON."
        ),
    )
    admission_run.add_argument("--log-level", help="uvicorn log level. Defaults to CG_ADMIN_LOG_LEVEL or info.")
    admission_run.set_defaults(handler=_handle_admission_run, local_command=True)
    admission_invite = admission_subparsers.add_parser(
        "invite",
        help="Agent admission invite operations",
        description=(
            "Create scoped join invites for worker admission. Use this lane when an external runtime should "
            "be admitted as a CommonGround Agent that accepts work."
        ),
        epilog=(
            "Typical flow:\n"
            "  cg admission invite create -h\n"
            "  cg agent join -h\n"
            "  cg worker loop -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    admission_invite_subparsers = admission_invite.add_subparsers(dest="admission_invite_command", required=True)
    admission_invite_create = admission_invite_subparsers.add_parser(
        "create",
        help="Create a scoped Agent join invite",
        description=(
            "Use Admin Service bearer auth to create a scoped Agent join invite and emit a copyable "
            "cg agent join command."
        ),
        epilog=(
            "Preconditions:\n"
            "  A create command needs Admin Service bearer auth through --admin-auth-token,\n"
            "  --admin-auth-token-file, or config.\n"
            "  The target project must already be admitted by the target Admin Service.\n"
            "  After the default local setup path, that project is cg-demo.\n"
            "\n"
            "Automation output:\n"
            "  --out writes the same result object that cg returns on stdout.\n"
            "  It includes project_id, agent_id, join_code, join_command, and invite.\n"
            "\n"
            "Example:\n"
            "  cg admission invite create --project-id cg-demo --agent-id worker-001 \\\n"
            "    --join-base-url http://127.0.0.1:8000"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_admin_cli_parent()],
    )
    admission_invite_create.add_argument("--project-id", default=DEFAULT_LOCAL_PROJECT_ID, help=f"Project for the invite. Default: {DEFAULT_LOCAL_PROJECT_ID}.")
    admission_invite_create.add_argument("--agent-id", required=True, help="Agent id that may redeem this invite.")
    admission_invite_create.add_argument("--profile-kind", default=DEFAULT_CONVERSATION_WORKER_PROFILE_KIND, help=f"Profile kind to grant. Default: {DEFAULT_CONVERSATION_WORKER_PROFILE_KIND}.")
    admission_invite_create.add_argument("--runtime-kind", default=DEFAULT_MANUAL_SHELL_RUNTIME_KIND, help=f"Runtime kind to record. Default: {DEFAULT_MANUAL_SHELL_RUNTIME_KIND}.")
    admission_invite_create.add_argument("--display-name", help="Human-readable Agent name for the invite.")
    admission_invite_create.add_argument("--description", help="Optional invite/profile description.")
    admission_invite_create.add_argument("--expires-in", default="24h", help="Invite lifetime, for example 24h, 60m, or 3600s. Default: 24h.")
    admission_invite_create.add_argument("--max-uses", type=_positive_int, default=1, help="Maximum successful redemptions. Default: 1.")
    admission_invite_create.add_argument("--join-base-url", help="URL to embed in the copyable cg agent join command. Defaults to Admin Service URL.")
    admission_invite_create.add_argument("--out", help="Write the same result JSON object that cg emits on stdout, including join_code, join_command, and invite.")
    admission_invite_create.add_argument("--json", action="store_true", help="Accepted for automation scripts; cg already emits JSON envelopes.")
    admission_invite_create.set_defaults(handler=_handle_admission_invite_create, client_kind="none")

    setup = subparsers.add_parser(
        "setup",
        help="Seed and configure a local project",
        description=(
            "Prepare a local operator environment: ensure local operator tables exist, seed a project "
            "namespace, inspect readiness, and write local CLI client config."
        ),
        epilog=(
            "Typical local first-run sequence:\n"
            "  uv tool install 'commonground-kernel[server]'\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local\n"
            "  cg setup project status --default-local\n"
            "  cg setup project client-config --default-local \\\n"
            "    --base-url http://127.0.0.1:8000 --admin-base-url http://127.0.0.1:8000\n"
            "  cg local run --project-id cg-demo --host 127.0.0.1 --port 8000"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    setup_subparsers = setup.add_subparsers(dest="setup_command", required=True)
    setup_project_parser = setup_subparsers.add_parser(
        "project",
        help="Local project setup operations",
        description=(
            "Ensure local operator tables exist, seed a local project namespace, inspect local setup "
            "readiness, and write local CLI client config."
        ),
        epilog=(
            "Preconditions:\n"
            "  Install the server-ready CLI package before 'seed' or 'status':\n"
            "    uv tool install 'commonground-kernel[server]'\n"
            "  'cg setup project seed' and 'cg setup project status' require PG_DSN or --pg-dsn.\n"
            "  'cg setup project client-config' does not require database access.\n"
            "\n"
            "Typical flow:\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local\n"
            "  cg setup project status --default-local\n"
            "  cg setup project client-config --default-local"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    setup_project_subparsers = setup_project_parser.add_subparsers(dest="setup_project_command", required=True)
    setup_project_seed = setup_project_subparsers.add_parser(
        "seed",
        help="Seed a local project namespace",
        description=(
            "Ensure the local operator tables exist, seed the local project namespace, and write local "
            "setup artifacts, including the project-side Admin Service AgentCredential token file and "
            "Admin Service bearer token file."
        ),
        epilog=(
            "Preconditions:\n"
            f"{SERVER_EXTRA_PRECONDITIONS}\n"
            "  Provide PG_DSN or --pg-dsn.\n"
            "\n"
            "This command ensures the local operator tables exist before it seeds the project.\n"
            "\n"
            "Example:\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_setup_project_args(setup_project_seed)
    setup_project_seed.add_argument("--creator-ref", default=DEFAULT_LOCAL_CREATOR_REF, help=f"Creator reference recorded for project bootstrap. Default: {DEFAULT_LOCAL_CREATOR_REF}.")
    setup_project_seed.add_argument("--rotate-admin-service-token", action="store_true", help="Rotate the project-scoped admin-service AgentCredential token during setup.")
    setup_project_seed.add_argument("--rotate-admin-auth-token", action="store_true", help="Rotate the local Admin Service bearer token during setup.")
    setup_project_seed.set_defaults(handler=_handle_setup_project_seed, operator_command=True)
    setup_project_status = setup_project_subparsers.add_parser(
        "status",
        help="Inspect local project setup readiness",
        description="Inspect local setup artifacts and bootstrap readiness. This is not a live CommonGround service health check.",
        epilog=(
            "Preconditions:\n"
            f"{SERVER_EXTRA_PRECONDITIONS}\n"
            "  Provide PG_DSN or --pg-dsn.\n"
            "\n"
            "Example:\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project status --default-local"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_setup_project_args(setup_project_status)
    setup_project_status.set_defaults(handler=_handle_setup_project_status, operator_command=True)
    setup_project_client_config = setup_project_subparsers.add_parser(
        "client-config",
        help="Write local CLI client config for a prepared project",
        description=(
            "Write local CLI client config for a prepared project, including base_url, admin_base_url, "
            "and the Admin Service bearer token file reference."
        ),
        epilog=(
            "Preconditions:\n"
            "  This command does not require PG_DSN.\n"
            "  The referenced Admin Service bearer token file must already exist.\n"
            "\n"
            "Example:\n"
            "  cg setup project client-config --default-local \\\n"
            "    --base-url http://127.0.0.1:8000 --admin-base-url http://127.0.0.1:8000"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_setup_project_client_config_args(setup_project_client_config)
    setup_project_client_config.set_defaults(handler=_handle_setup_project_client_config, operator_command=True)

    project = subparsers.add_parser(
        "project",
        help="Observe project agents, offers, turns, and feed",
        description="Read project-scoped discovery and observation surfaces such as agents, offers, turns, lineage, and feed.",
        epilog=(
            "Use this lane after dispatch, report, or worker execution when you need to inspect\n"
            "project-scoped observation surfaces.\n"
            "\n"
            "Learn with:\n"
            "  cg project agent list -h\n"
            "  cg project offer list -h\n"
            "  cg project turn list -h\n"
            "  cg project feed -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    project_subparsers = project.add_subparsers(dest="project_command", required=True)

    project_agent = project_subparsers.add_parser(
        "agent",
        help="Project agent observation operations",
        description="Read projected Agent snapshots for one project.",
    )
    project_agent_subparsers = project_agent.add_subparsers(dest="project_agent_command", required=True)
    project_agent_list = project_agent_subparsers.add_parser("list", help="List project agents", parents=[_cg_service_cli_parent(caller=True)])
    project_agent_list.add_argument("--project-id", required=True, help="Project to inspect.")
    project_agent_list.add_argument("--enabled-only", action="store_true", help="Only return enabled agents.")
    project_agent_list.add_argument("--accepts-work-only", action="store_true", help="Only return agents currently accepting work.")
    project_agent_list.add_argument("--role", help="Filter by projected agent role.")
    project_agent_list.add_argument("--capability", help="Filter by projected capability.")
    project_agent_list.add_argument("--limit", type=_positive_int, default=100, help="Maximum agents to return. Default: 100.")
    project_agent_list.set_defaults(handler=_handle_project_agent_list, client_kind="projection")

    project_offer = project_subparsers.add_parser(
        "offer",
        help="Project canonical turn-offer observation operations",
        description="Read canonical projected turn offers for one project.",
    )
    project_offer_subparsers = project_offer.add_subparsers(dest="project_offer_command", required=True)
    project_offer_list = project_offer_subparsers.add_parser("list", help="List project turn offers", parents=[_cg_service_cli_parent(caller=True)])
    project_offer_list.add_argument("--project-id", required=True, help="Project to inspect.")
    project_offer_list.add_argument("--turn-kind", help="Filter by offered turn kind.")
    project_offer_list.add_argument("--agent-id", help="Filter by offering agent id.")
    project_offer_list.add_argument("--enabled-only", action="store_true", help="Only return enabled offers.")
    project_offer_list.add_argument("--accepts-work-only", action="store_true", help="Only return offers from agents accepting work.")
    project_offer_list.add_argument("--limit", type=_positive_int, default=100, help="Maximum offers to return. Default: 100.")
    project_offer_list.set_defaults(handler=_handle_project_offer_list, client_kind="projection")
    project_offer_get = project_offer_subparsers.add_parser("get", help="Get one canonical turn offer", parents=[_cg_service_cli_parent(caller=True)])
    project_offer_get.add_argument("--project-id", required=True, help="Project to inspect.")
    project_offer_get.add_argument("--turn-kind", required=True, help="Offered turn kind.")
    project_offer_get.add_argument("--agent-id", required=True, help="Offering agent id.")
    project_offer_get.set_defaults(handler=_handle_project_offer_get, client_kind="projection")

    project_turn = project_subparsers.add_parser(
        "turn",
        help="Project turn observation operations",
        description="Read projected Turn lists and lineage for one project.",
    )
    project_turn_subparsers = project_turn.add_subparsers(dest="project_turn_command", required=True)
    project_turn_list = project_turn_subparsers.add_parser("list", help="List project turns", parents=[_cg_service_cli_parent(caller=True)])
    project_turn_list.add_argument("--project-id", required=True, help="Project to inspect.")
    project_turn_list.add_argument("--target-agent-id", help="Filter by target agent id.")
    project_turn_list.add_argument("--turn-kind", help="Filter by turn kind.")
    project_turn_list.add_argument("--state", help="Filter by turn state.")
    project_turn_list.add_argument("--outcome", help="Filter by terminal outcome.")
    project_turn_list.add_argument("--stop-requested-only", action="store_true", help="Only return turns with stop requested.")
    project_turn_list.add_argument("--limit", type=_positive_int, default=100, help="Maximum turns to return. Default: 100.")
    project_turn_list.set_defaults(handler=_handle_project_turn_list, client_kind="projection")

    project_turn_lineage = project_turn_subparsers.add_parser("lineage", help="Get one turn lineage", parents=[_cg_service_cli_parent(caller=True)])
    project_turn_lineage.add_argument("--project-id", required=True, help="Project to inspect.")
    project_turn_lineage.add_argument("--turn-id", required=True, help="Turn id whose lineage should be fetched.")
    project_turn_lineage.add_argument("--limit", type=_positive_int, default=100, help="Maximum lineage nodes to return. Default: 100.")
    project_turn_lineage.set_defaults(handler=_handle_project_turn_lineage, client_kind="projection")

    project_feed = project_subparsers.add_parser("feed", help="Fetch project feed", parents=[_cg_service_cli_parent(caller=True)])
    project_feed.add_argument("--project-id", required=True, help="Project to inspect.")
    project_feed.add_argument("--after-ledger-seq", type=int, default=0, help="Return feed entries after this ledger sequence. Default: 0.")
    project_feed.add_argument("--limit", type=_positive_int, default=100, help="Maximum feed entries to return. Default: 100.")
    project_feed.set_defaults(handler=_handle_project_feed, client_kind="projection")

    smoke = subparsers.add_parser(
        "smoke",
        help="Verify a worker path end-to-end",
        description="Run end-to-end verification flows that dispatch work and wait for a result.",
        epilog=(
            "Typical use:\n"
            "  After 'cg admission invite create -h', 'cg agent join -h', and 'cg worker loop -h',\n"
            "  use 'cg smoke pair -h' to verify the worker path end-to-end."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    smoke_subparsers = smoke.add_subparsers(dest="smoke_command", required=True)
    smoke_pair = smoke_subparsers.add_parser(
        "pair",
        help="Dispatch and wait for a pair smoke turn",
        description=(
            "Verify a pair path by discovering the target offer, dispatching a turn, waiting for closure, "
            "and returning final context. The default payload asks for a concise smoke-test result."
        ),
        epilog=(
            "Preconditions:\n"
            "  The requester profile named by --from must already exist.\n"
            "  The target agent named by --to must already publish an enabled, accepts-work offer\n"
            "  for the selected turn kind."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent(profile=False)],
    )
    smoke_pair.add_argument("--from", dest="profile", required=True, help="Requester profile, usually <project_id>/<agent_id>.")
    smoke_pair.add_argument("--to", dest="target_agent", required=True, help="Target agent id.")
    smoke_pair.add_argument("--turn-kind", default="turn.conversation.v1", help="Turn kind to verify. Default: turn.conversation.v1.")
    smoke_pair.add_argument("--request-id", help="External request/correlation anchor; defaults to a generated smoke id.")
    smoke_pair.add_argument("--dispatch-key", help="Idempotency anchor; mirrors --request-id when omitted.")
    smoke_pair.add_argument("--payload-json", default='{"task":"Reply with a concise CommonGround smoke-test result."}', help="Inline JSON smoke payload. Default asks for a concise smoke-test result.")
    smoke_pair.add_argument("--timeout-seconds", type=_non_negative_float, default=60.0, help="Maximum seconds to wait. Default: 60.")
    smoke_pair.add_argument("--poll-interval-ms", type=_positive_int, default=500, help="Polling interval in milliseconds. Default: 500.")
    smoke_pair.set_defaults(handler=_handle_smoke_pair)

    _add_worker_commands(subparsers)
    return parser


def _add_worker_commands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    worker = subparsers.add_parser(
        "worker",
        help="Receive, claim, and complete CommonGround-assigned work",
        description=(
            "BYOA conversation-worker admission does not need NanoBot. It is a harness-agnostic path for "
            "an external runtime that should receive turn.conversation.v1 work as a CommonGround Agent.\n"
            "\n"
            "Use this lane when the runtime should claim and finish CommonGround Turns under its own "
            "Agent identity.\n"
            "\n"
            "Start with 'worker loop' for the generic adapter path; use 'worker claim *' when your "
            "runtime needs explicit claim, renew, append, dispatch-child, suspend, and finish control."
        ),
        epilog=(
            "Local first-run path:\n"
            "  uv tool install 'commonground-kernel[server]'\n"
            "  export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME\n"
            "  cg setup project seed --default-local\n"
            "  cg setup project status --default-local\n"
            "  cg setup project client-config --default-local \\\n"
            "    --base-url http://127.0.0.1:8000 --admin-base-url http://127.0.0.1:8000\n"
            "  cg local run --project-id cg-demo --host 127.0.0.1 --port 8000\n"
            "\n"
            "Typical worker flow:\n"
            "  cg admission invite create -h\n"
            "  cg agent join -h\n"
            "  cg worker loop -h\n"
            "  cg smoke pair -h\n"
            "\n"
            "Lower-level worker flow:\n"
            "  cg worker claim -h\n"
            "  cg dispatch -h\n"
            "  cg turn -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    worker_subparsers = worker.add_subparsers(dest="worker_command", required=True)

    worker_once = worker_subparsers.add_parser(
        "once",
        help="Claim one turn and run a shell worker adapter command",
        description=(
            "Claim one turn and run a thin shell adapter. The child reads CG_CONTEXT_FILE and may write "
            "CG_FINAL_FILE, CG_SUSPEND_FILE, or CG_FAILURE_FILE."
        ),
        epilog=(
            "Identity:\n"
            "  Provide --profile, or provide --project-id with --agent-id.\n"
            "\n"
            "Child file contract:\n"
            "  CG_CONTEXT_FILE is written by cg as a Turn context JSON object.\n"
            "  CG_FINAL_FILE may contain any JSON value. When child exit code is 0, cg finishes the Turn\n"
            "  with outcome=succeeded from that payload.\n"
            "  CG_SUSPEND_FILE must contain a JSON object like {\"reason\":\"waiting_on_human\",\"note\":\"optional\"}.\n"
            "  When child exit code is 0, cg suspends the Turn from that object.\n"
            "  CG_FAILURE_FILE may contain any JSON value. If the child exits non-zero or omits a final\n"
            "  payload, cg merges that value into the failure payload.\n"
            "\n"
            "This is the one-shot form of the generic shell worker adapter."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    _add_worker_adapter_args(worker_once)
    worker_once.set_defaults(handler=_handle_worker_once)

    worker_loop = worker_subparsers.add_parser(
        "loop",
        help="Continuously claim turns and run a shell worker adapter command",
        description=(
            "Loop over the same thin shell adapter contract as worker once. Sleeps when no turn is claimed "
            "unless --max-iterations stops first."
        ),
        epilog=(
            "Identity:\n"
            "  Provide --profile, or provide --project-id with --agent-id.\n"
            "\n"
            "Child file contract:\n"
            "  CG_CONTEXT_FILE is written by cg as a Turn context JSON object.\n"
            "  CG_FINAL_FILE may contain any JSON value for a successful finish.\n"
            "  CG_SUSPEND_FILE must contain a JSON object with reason and optional note.\n"
            "  CG_FAILURE_FILE may contain any JSON value that cg merges into the failure payload.\n"
            "\n"
            "This is the recommended generic worker adapter path for BYOA conversation-worker admission."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    _add_worker_adapter_args(worker_loop)
    worker_loop.add_argument("--idle-sleep-seconds", type=_non_negative_float, default=1.0, help="Seconds to sleep after an empty claim. Default: 1.")
    worker_loop.add_argument("--max-iterations", type=_positive_int, help="Stop after this many claim attempts.")
    worker_loop.set_defaults(handler=_handle_worker_loop)

    claim = worker_subparsers.add_parser(
        "claim",
        help="Claim operations",
        description="Low-level worker claim operations for runtimes that manage claim lifecycle explicitly.",
        epilog=(
            "Claim-file commands infer identity from the claim file where possible.\n"
            "They still need a matching AgentCredential; the claim file is not a standalone bearer token.\n"
            "Accepted claim-file forms are either the raw claim JSON object written by\n"
            "'cg worker claim next --claim-out-file' or 'cg worker claim run --claim-out-file',\n"
            "or a CLI JSON envelope whose result.claim contains that object.\n"
            "The raw claim object requires project_id, turn_id, agent_id, token, and expires_at.\n"
            "\n"
            "Raw claim object example:\n"
            "  {\n"
            "    \"project_id\": \"cg-demo\",\n"
            "    \"turn_id\": \"T-123\",\n"
            "    \"agent_id\": \"worker-001\",\n"
            "    \"token\": \"cgclaim_...\",\n"
            "    \"expires_at\": \"2026-05-19T12:00:00+00:00\"\n"
            "  }\n"
            "\n"
            "Learn with:\n"
            "  cg worker claim next -h\n"
            "  cg worker claim renew -h\n"
            "  cg worker claim append -h\n"
            "  cg worker claim finish -h"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    claim_subparsers = claim.add_subparsers(dest="claim_command", required=True)

    claim_next = claim_subparsers.add_parser("next", help="Claim the next available turn for an agent", parents=[_cg_service_cli_parent()])
    claim_next.add_argument("--project-id", required=True, help="Project to claim from.")
    claim_next.add_argument("--agent-id", required=True, help="Agent id to claim as.")
    claim_next.add_argument("--context-after-turn-seq", type=int, default=0, help="Return context after this turn sequence offset. Default: 0.")
    claim_next.add_argument("--context-limit", type=_positive_int, default=100, help="Maximum context items to return. Default: 100.")
    claim_next.add_argument("--claim-out-file", help="Write the raw claim JSON object to this file for later claim commands.")
    claim_next.set_defaults(handler=_handle_claim_next)

    claim_renew = claim_subparsers.add_parser(
        "renew",
        help="Renew an active claim and return lease timing",
        description="Renew the claim stored in --claim-file. The matching AgentCredential must authenticate the claim owner.",
        epilog=(
            "Claim-file shape:\n"
            "  Accepts a raw claim object with project_id, turn_id, agent_id, token, and expires_at,\n"
            "  or a CLI envelope whose result.claim contains that object."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    claim_renew.add_argument("--claim-file", required=True, help="Claim JSON file returned by cg worker claim next/run, or a CLI envelope whose result.claim contains that object.")
    claim_renew.set_defaults(handler=_handle_claim_renew)

    claim_run_help = "Claim a turn, auto-renew the lease, and run a child command"
    claim_run = claim_subparsers.add_parser(
        "run",
        help=claim_run_help,
        description=claim_run_help,
        epilog=(
            "The child command runs with CG_CLAIM_FILE, CG_CONTEXT_FILE, and related claim/context\n"
            "environment variables.\n"
            "\n"
            "Output files:\n"
            "  --claim-out-file writes the raw claim JSON object.\n"
            "  --context-out-file writes the Turn context JSON object fetched for that claim.\n"
            "\n"
            "Example:\n"
            "  cg worker claim run --project-id <project_id> --agent-id <agent_id> -- ./worker-bin --flag"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    claim_run.add_argument("--project-id", required=True, help="Project to claim from.")
    claim_run.add_argument("--agent-id", required=True, help="Agent id to claim as.")
    claim_run.add_argument("--context-after-turn-seq", type=int, default=0, help="Return context after this turn sequence offset. Default: 0.")
    claim_run.add_argument("--context-limit", type=_positive_int, default=100, help="Maximum context items to write. Default: 100.")
    claim_run.add_argument("--claim-out-file", help="Write the raw claim JSON object to this file. Later claim-file commands accept this exact object.")
    claim_run.add_argument("--context-out-file", help="Write the Turn context JSON object to this file before running the child command.")
    claim_run.add_argument("--renew-interval-seconds", type=_non_negative_float, default=0.5, help="Claim lease renew interval. Default: 0.5.")
    claim_run.add_argument(
        "child_command",
        nargs=argparse.REMAINDER,
        metavar="-- CHILD_CMD [ARGS ...]",
        help="Child command to run after '--'.",
    )
    claim_run.set_defaults(handler=_handle_claim_run)

    claim_append = claim_subparsers.add_parser(
        "append",
        help="Append a semantic record to an active claim",
        description="Append using the claim in --claim-file. The matching AgentCredential must authenticate the claim owner.",
        epilog=(
            "Claim-file shape:\n"
            "  Accepts a raw claim object with project_id, turn_id, agent_id, token, and expires_at,\n"
            "  or a CLI envelope whose result.claim contains that object.\n"
            "\n"
            "Payload-file shape:\n"
            "  Any JSON value is accepted. cg does not enforce a payload schema here."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    claim_append.add_argument("--claim-file", required=True, help="Claim JSON file returned by cg worker claim next/run, or a CLI envelope whose result.claim contains that object.")
    claim_append.add_argument("--payload-file", required=True, help="JSON payload file for the semantic record to append. Any JSON value is accepted; cg does not enforce a payload schema here.")
    claim_append.add_argument("--role", default="progress", help="Semantic record role to append. Default: progress.")
    claim_append.set_defaults(handler=_handle_claim_append)

    claim_finish = claim_subparsers.add_parser(
        "finish",
        help="Finish an active claimed turn",
        description="Finish using the claim in --claim-file. The matching AgentCredential must authenticate the claim owner.",
        epilog=(
            "Claim-file shape:\n"
            "  Accepts a raw claim object with project_id, turn_id, agent_id, token, and expires_at,\n"
            "  or a CLI envelope whose result.claim contains that object.\n"
            "\n"
            "Payload-file shape:\n"
            "  Optional. When provided, any JSON value is accepted as the final payload.\n"
            "  cg does not enforce a final payload schema here."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    claim_finish.add_argument("--claim-file", required=True, help="Claim JSON file returned by cg worker claim next/run, or a CLI envelope whose result.claim contains that object.")
    claim_finish.add_argument("--outcome", required=True, choices=[item.value for item in TurnOutcome], help="Terminal Turn outcome.")
    claim_finish.add_argument("--payload-file", help="Optional JSON final payload file. Any JSON value is accepted; cg does not enforce a final payload schema here.")
    claim_finish.add_argument("--final-record-role", default="deliverable", help="Semantic record role for the final payload. Default: deliverable.")
    claim_finish.set_defaults(handler=_handle_claim_finish)

    claim_dispatch_child = claim_subparsers.add_parser(
        "dispatch-child",
        help="Dispatch a child turn from an active parent claim",
        description="Dispatch from the parent claim in --claim-file. The matching AgentCredential must authenticate the requested child actor.",
        epilog=(
            "Claim-file shape:\n"
            "  Accepts a raw claim object with project_id, turn_id, agent_id, token, and expires_at,\n"
            "  or a CLI envelope whose result.claim contains that object.\n"
            "\n"
            "Payload-file shape:\n"
            "  Any JSON value is accepted as the child Turn input payload. cg does not enforce a\n"
            "  payload schema here."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    claim_dispatch_child.add_argument("--claim-file", required=True, help="Claim JSON file returned by cg worker claim next/run, or a CLI envelope whose result.claim contains that object.")
    claim_dispatch_child.add_argument("--requested-by", required=True, help="Agent id requesting the child dispatch.")
    claim_dispatch_child.add_argument("--target-agent", required=True, help="Target child Agent id.")
    claim_dispatch_child.add_argument("--payload-file", required=True, help="JSON payload file for the child Turn input. Any JSON value is accepted; cg does not enforce a payload schema here.")
    claim_dispatch_child.add_argument("--dispatch-key", required=True, help="Dispatch idempotency anchor for the child Turn.")
    claim_dispatch_child.add_argument("--turn-kind", default="turn.conversation.v1", help="Child Turn kind. Default: turn.conversation.v1.")
    claim_dispatch_child.set_defaults(handler=_handle_claim_dispatch_child)

    claim_suspend = claim_subparsers.add_parser(
        "suspend",
        help="Suspend an active claimed turn",
        description="Suspend using the claim in --claim-file. The matching AgentCredential must authenticate the claim owner.",
        epilog=(
            "Claim-file shape:\n"
            "  Accepts a raw claim object with project_id, turn_id, agent_id, token, and expires_at,\n"
            "  or a CLI envelope whose result.claim contains that object."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[_cg_service_cli_parent()],
    )
    claim_suspend.add_argument("--claim-file", required=True, help="Claim JSON file returned by cg worker claim next/run, or a CLI envelope whose result.claim contains that object.")
    claim_suspend.add_argument("--reason", required=True, help="Suspend reason.")
    claim_suspend.add_argument("--note", help="Optional suspend note.")
    claim_suspend.set_defaults(handler=_handle_claim_suspend)


def _add_worker_adapter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project-id", help="Project to claim from. Required unless --profile identifies the worker.")
    parser.add_argument("--agent-id", help="Worker Agent id to claim as. Required unless --profile identifies the worker.")
    parser.add_argument("--context-after-turn-seq", type=int, default=0, help="Return context after this turn sequence offset. Default: 0.")
    parser.add_argument("--context-limit", type=_positive_int, default=100, help="Maximum context items to write. Default: 100.")
    parser.add_argument("--work-dir", help="Directory for context/final/suspend files. Defaults to a temporary directory.")
    parser.add_argument("--context-out-file", help="Context JSON path written by cg and passed to the child as CG_CONTEXT_FILE.")
    parser.add_argument("--final-in-file", help="Final payload JSON path the child writes through CG_FINAL_FILE. Any JSON value is accepted.")
    parser.add_argument("--suspend-in-file", help="Suspend request JSON path the child writes through CG_SUSPEND_FILE. Expected shape: {\"reason\":\"...\",\"note\":\"optional\"}.")
    parser.add_argument("--failure-in-file", help="Failure details JSON path the child writes through CG_FAILURE_FILE. Any JSON value is accepted and merged into cg's failure payload.")
    parser.add_argument("--renew-interval-seconds", type=_non_negative_float, default=0.5, help="Claim lease renew interval. Default: 0.5.")
    parser.add_argument("--final-record-role", default="deliverable", help="Semantic record role for a successful final payload. Default: deliverable.")
    parser.add_argument(
        "--command",
        nargs=argparse.REMAINDER,
        required=True,
        help="Worker command. Reads CG_CONTEXT_FILE and writes CG_FINAL_FILE, CG_SUSPEND_FILE, or CG_FAILURE_FILE.",
    )


def _join_cli_parent() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--base-url",
        default=argparse.SUPPRESS,
        help=f"CommonGround service URL to store in local config. Defaults to server_url or {DEFAULT_BASE_URL}.",
    )
    parser.add_argument(
        "--admin-base-url",
        default=argparse.SUPPRESS,
        help="Admin Service URL used to redeem the join code. Defaults to server_url or --base-url.",
    )
    parser.add_argument(
        "--config",
        default=argparse.SUPPRESS,
        help="Path to CLI JSON config where the joined profile will be written.",
    )
    return parser


def _admin_cli_parent(*, base_url: bool = False, profile: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    if base_url:
        parser.add_argument(
            "--base-url",
            default=argparse.SUPPRESS,
            help=(
                f"CommonGround service URL. Used to validate an existing stored AgentCredential and as the "
                f"fallback service/admin URL when --admin-base-url is not set. Precedence: flag > "
                f"CG_BASE_URL > config file > default {DEFAULT_BASE_URL}."
            ),
        )
    parser.add_argument(
        "--admin-base-url",
        default=argparse.SUPPRESS,
        help="Admin Service URL. Precedence: flag > CG_ADMIN_BASE_URL > config file > CommonGround service URL.",
    )
    parser.add_argument(
        "--admin-auth-token",
        default=argparse.SUPPRESS,
        help="Admin Service bearer token. Precedence: flag > CG_ADMIN_AUTH_TOKEN > config file.",
    )
    parser.add_argument(
        "--admin-auth-token-file",
        default=argparse.SUPPRESS,
        help="File containing Admin Service bearer token. Precedence: flag > CG_ADMIN_AUTH_TOKEN_FILE > config file.",
    )
    if profile:
        parser.add_argument(
            "--profile",
            default=argparse.SUPPRESS,
            required=True,
            help="Local destination profile key to write or refresh, usually <project_id>/<agent_id>.",
        )
    parser.add_argument(
        "--config",
        default=argparse.SUPPRESS,
        help="Path to CLI JSON config. Precedence: flag > CG_CONFIG_PATH > ~/.config/commonground/config.json if present.",
    )
    return parser


def _cg_service_cli_parent(*, caller: bool = False, profile: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--base-url",
        default=argparse.SUPPRESS,
        help=f"CommonGround service URL. Precedence: flag > CG_BASE_URL > config file > default {DEFAULT_BASE_URL}.",
    )
    parser.add_argument(
        "--auth-token",
        default=argparse.SUPPRESS,
        help="Agent credential bearer token. Precedence: flag > CG_AGENT_CREDENTIAL_TOKEN > config file.",
    )
    parser.add_argument(
        "--auth-token-file",
        default=argparse.SUPPRESS,
        help="File containing bearer token. Used if no direct token is set; precedence: flag > CG_AGENT_CREDENTIAL_TOKEN_FILE > config file.",
    )
    if profile:
        parser.add_argument(
            "--profile",
            default=argparse.SUPPRESS,
            help="CLI-managed Agent profile key, usually <project_id>/<agent_id>.",
        )
    if caller:
        parser.add_argument(
            "--caller-project-id",
            default=argparse.SUPPRESS,
            help="Claimed Agent project id. Must be paired with --caller-agent-id when caller identity is not inferred.",
        )
        parser.add_argument(
            "--caller-agent-id",
            default=argparse.SUPPRESS,
            help="Claimed Agent id. Must be paired with --caller-project-id when caller identity is not inferred.",
        )
    parser.add_argument(
        "--config",
        default=argparse.SUPPRESS,
        help="Path to CLI JSON config. Precedence: flag > CG_CONFIG_PATH > ~/.config/commonground/config.json if present.",
    )
    return parser


def _add_setup_project_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pg-dsn", help="PostgreSQL DSN for local operator setup. Defaults to PG_DSN.")
    parser.add_argument("--project-id", help=f"Project namespace to seed. Defaults to {DEFAULT_LOCAL_PROJECT_ID}.")
    parser.add_argument("--default-local", action="store_true", help=f"Use the default local project id: {DEFAULT_LOCAL_PROJECT_ID}.")
    parser.add_argument("--admin-service-token-file", help="File for the project-scoped admin-service CG AgentCredential.")
    parser.add_argument("--admin-auth-token-file", dest="setup_admin_auth_token_file", help="File for the local Admin Service API bearer token.")


def _add_setup_project_client_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project-id", help=f"Project namespace for the client config. Defaults to {DEFAULT_LOCAL_PROJECT_ID}.")
    parser.add_argument("--default-local", action="store_true", help=f"Use the default local project id: {DEFAULT_LOCAL_PROJECT_ID}.")
    parser.add_argument("--base-url", default=argparse.SUPPRESS, help=f"CommonGround service URL. Defaults to CG_BASE_URL or {DEFAULT_BASE_URL}.")
    parser.add_argument(
        "--admin-base-url",
        default=argparse.SUPPRESS,
        help=f"Admin Service URL. Defaults to CG_ADMIN_BASE_URL or {DEFAULT_ADMIN_BASE_URL}.",
    )
    parser.add_argument("--admin-auth-token-file", dest="setup_admin_auth_token_file", help="File for the local Admin Service API bearer token.")
    parser.add_argument("--config", default=argparse.SUPPRESS, help="CLI JSON config path. Defaults to CG_CONFIG_PATH or ~/.config/commonground/config.json.")


def main(
    argv: list[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    client_factory: Callable[..., Any] | None = None,
    projection_client_factory: Callable[..., Any] | None = None,
    admin_client_factory: Callable[..., Any] | None = None,
) -> int:
    return _run_cli(
        build_parser,
        argv=argv,
        stdout=stdout,
        stderr=stderr,
        sleep_fn=sleep_fn,
        client_factory=client_factory,
        projection_client_factory=projection_client_factory,
        admin_client_factory=admin_client_factory,
    )


def _run_cli(
    parser_factory: Callable[[], argparse.ArgumentParser],
    *,
    argv: list[str] | None,
    stdout: TextIO | None,
    stderr: TextIO | None,
    sleep_fn: Callable[[float], None],
    client_factory: Callable[..., Any] | None,
    projection_client_factory: Callable[..., Any] | None,
    admin_client_factory: Callable[..., Any] | None,
) -> int:
    load_local_env()
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = parser_factory()
    args = None
    client = None
    try:
        args = parser.parse_args(argv)
    except CliHandledError as exc:
        _emit(
            {
                "ok": False,
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "status": exc.status,
                },
            },
            stream=stdout,
        )
        return 1
    try:
        _validate_command_surface(args)
    except CliHandledError as exc:
        _emit(
            {
                "ok": False,
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "status": exc.status,
                },
            },
            stream=stdout,
        )
        return 1
    if getattr(args, "local_command", False):
        return _run_local_command(args, stdout=stdout, stderr=stderr)
    if getattr(args, "operator_command", False):
        return _run_operator_command(args, stdout=stdout, sleep_fn=sleep_fn)
    try:
        runtime = resolve_cli_config(
            args,
            default_base_url=DEFAULT_BASE_URL,
            resolve_auth=_command_reads_agent_auth(args),
            resolve_admin_auth=_command_reads_admin_auth(args),
            resolve_profile=_command_accepts_profile(args),
            resolve_caller=_command_accepts_caller(args),
        )
        client_kind = getattr(args, "client_kind", "agent")
        if _command_resolves_client_auth(args):
            auth = _resolve_client_auth(args, runtime, admin_client_factory=admin_client_factory)
        else:
            auth = ResolvedClientAuth(auth_token=None, headers=None, profile_name=None)
        setattr(args, "_resolved_runtime", runtime)
        setattr(args, "_resolved_auth", auth)
        setattr(args, "_admin_client_factory", admin_client_factory)
        setattr(args, "_projection_client_factory", projection_client_factory)
        if client_kind == "none":
            client = None
        else:
            factory = _resolve_client_factory(client_kind, client_factory, projection_client_factory)
            client = factory(
                base_url=runtime.base_url,
                auth_token=auth.auth_token,
                headers=auth.headers,
            )
        setattr(args, "_resolved_profile_name", auth.profile_name)
        result = args.handler(args, client=client, sleep_fn=sleep_fn)
    except CliHandledError as exc:
        _emit(
            {
                "ok": False,
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "status": exc.status,
                },
            },
            stream=stdout,
        )
        return 1
    except httpx.HTTPStatusError as exc:
        _emit({"ok": False, "error": _http_error_payload(exc)}, stream=stdout)
        return 1
    except httpx.TimeoutException as exc:
        _emit({"ok": False, "error": {"code": "timeout", "message": str(exc), "status": 408}}, stream=stdout)
        return 1
    except httpx.RequestError as exc:
        _emit({"ok": False, "error": {"code": "request_error", "message": str(exc), "status": 503}}, stream=stdout)
        return 1
    except FileNotFoundError as exc:
        _emit({"ok": False, "error": {"code": "file_not_found", "message": str(exc), "status": 404}}, stream=stdout)
        return 1
    except JSONDecodeError as exc:
        _emit({"ok": False, "error": {"code": "invalid_json", "message": str(exc), "status": 422}}, stream=stdout)
        return 1
    except ValueError as exc:
        _emit({"ok": False, "error": {"code": "invalid_input", "message": str(exc), "status": 422}}, stream=stdout)
        return 1
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()
    exit_code = 0
    if isinstance(result, CliExitResult):
        exit_code = result.exit_code
        result = result.payload
    _emit({"ok": True, "result": result}, stream=stdout)
    return exit_code


def _resolve_client_factory(
    client_kind: str,
    client_factory: Callable[..., Any] | None,
    projection_client_factory: Callable[..., Any] | None,
) -> Callable[..., Any]:
    if client_kind == "projection":
        if projection_client_factory is None:
            from CommonGround.projection_client import ProjectionHttpClient

            return ProjectionHttpClient
        return projection_client_factory
    if client_factory is None:
        from CommonGround.agent_client.http_client import HttpAgentClient

        return HttpAgentClient
    return client_factory


def _validate_command_surface(args: argparse.Namespace) -> None:
    if getattr(args, "command", None) == "report" and getattr(args, "report_command", None) == "work-memory":
        _reject_surface_args(
            args,
            "cg report work-memory",
            (
                "admin_base_url",
                "admin_auth_token",
                "admin_auth_token_file",
                "caller_project_id",
                "caller_agent_id",
            ),
        )


def _is_agent_join_command(args: argparse.Namespace) -> bool:
    return getattr(args, "command", None) == "agent" and getattr(args, "agent_command", None) == "join"


def _is_profile_ensure_agent_command(args: argparse.Namespace) -> bool:
    return getattr(args, "command", None) == "profile" and getattr(args, "profile_command", None) == "ensure-agent"


def _is_admission_invite_create_command(args: argparse.Namespace) -> bool:
    return (
        getattr(args, "command", None) == "admission"
        and getattr(args, "admission_command", None) == "invite"
        and getattr(args, "admission_invite_command", None) == "create"
    )


def _is_read_command(args: argparse.Namespace) -> bool:
    command = getattr(args, "command", None)
    if command == "turn" and getattr(args, "turn_command", None) in {"get", "context", "wait"}:
        return True
    if command == "agent" and getattr(args, "agent_command", None) == "get":
        return True
    return command == "project"


def _reject_surface_args(args: argparse.Namespace, command_name: str, attrs: tuple[str, ...]) -> None:
    present = [f"--{attr.replace('_', '-')}" for attr in attrs if hasattr(args, attr)]
    if present:
        raise CliHandledError(
            code="invalid_arguments",
            message=f"{command_name} does not accept {', '.join(present)}",
            status=2,
        )


def _command_reads_admin_auth(args: argparse.Namespace) -> bool:
    return _is_profile_ensure_agent_command(args) or _is_admission_invite_create_command(args)


def _command_reads_agent_auth(args: argparse.Namespace) -> bool:
    if _is_agent_join_command(args):
        return False
    if _is_profile_ensure_agent_command(args):
        return False
    if _is_admission_invite_create_command(args):
        return False
    return True


def _command_accepts_profile(args: argparse.Namespace) -> bool:
    if _is_agent_join_command(args) or _is_admission_invite_create_command(args):
        return False
    return True


def _command_accepts_caller(args: argparse.Namespace) -> bool:
    return _is_read_command(args)


def _command_bootstraps_profile(args: argparse.Namespace) -> bool:
    return _is_profile_ensure_agent_command(args)


def _command_resolves_client_auth(args: argparse.Namespace) -> bool:
    if _is_agent_join_command(args):
        return False
    if _is_admission_invite_create_command(args):
        return False
    return True


def _run_operator_command(args: argparse.Namespace, *, stdout: TextIO, sleep_fn: Callable[[float], None]) -> int:
    try:
        result = args.handler(args, client=None, sleep_fn=sleep_fn)
    except CliHandledError as exc:
        _emit(
            {
                "ok": False,
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "status": exc.status,
                },
            },
            stream=stdout,
        )
        return 1
    except FileNotFoundError as exc:
        _emit({"ok": False, "error": {"code": "file_not_found", "message": str(exc), "status": 404}}, stream=stdout)
        return 1
    except JSONDecodeError as exc:
        _emit({"ok": False, "error": {"code": "invalid_json", "message": str(exc), "status": 422}}, stream=stdout)
        return 1
    except ValueError as exc:
        _emit({"ok": False, "error": {"code": "invalid_input", "message": str(exc), "status": 422}}, stream=stdout)
        return 1
    except ModuleNotFoundError as exc:
        _emit({"ok": False, "error": _missing_extra_error(exc, extra="server")}, stream=stdout)
        return 1
    except Exception as exc:
        if not _is_psycopg_operational_error(exc):
            raise
        _emit(
            {
                "ok": False,
                "error": {
                    "code": "setup_pg_unavailable",
                    "message": "PostgreSQL is unavailable for cg setup project",
                    "status": 503,
                },
            },
            stream=stdout,
        )
        return 1
    _emit({"ok": True, "result": result}, stream=stdout)
    return 0


def _run_local_command(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    try:
        return int(args.handler(args, stdout=stdout, stderr=stderr))
    except CliHandledError as exc:
        print(f"{exc.code}: {exc.message}", file=stderr)
        return 1


def _resolve_client_auth(
    args: argparse.Namespace,
    runtime,
    *,
    admin_client_factory: Callable[..., Any] | None,
) -> ResolvedClientAuth:
    explicit_project_id = runtime.caller_project_id
    explicit_agent_id = runtime.caller_agent_id
    inferred = _infer_authenticated_caller(args)
    _validate_explicit_caller_matches_inferred(explicit_project_id, explicit_agent_id, inferred)
    profile = _resolve_profile_for_command(
        args,
        runtime,
        inferred,
        admin_client_factory=admin_client_factory,
    )
    if profile is not None:
        _validate_explicit_caller_matches_profile(explicit_project_id, explicit_agent_id, profile)
        explicit_project_id = explicit_project_id or profile.project_id
        explicit_agent_id = explicit_agent_id or profile.agent_id
    project_id = explicit_project_id or (None if inferred is None else inferred.project_id)
    agent_id = explicit_agent_id or (None if inferred is None else inferred.agent_id)
    if (
        profile is None
        and inferred is not None
        and runtime.auth_token is None
        and _command_supports_inferred_profile(args)
    ):
        raise CliHandledError(
            code="profile_missing",
            message=f"CLI profile not found: {profile_key(inferred.project_id, inferred.agent_id)}",
            status=404,
        )
    auth_token = _profile_token(profile) if profile is not None else runtime.auth_token
    if project_id is None and agent_id is None:
        return ResolvedClientAuth(auth_token=auth_token, headers=None, profile_name=None if profile is None else profile.name)
    if project_id is None or agent_id is None:
        raise CliHandledError(
            code="invalid_arguments",
            message="claimed agent identity requires both project and agent ids",
            status=2,
        )
    if auth_token is None:
        raise CliHandledError(
            code="invalid_arguments",
            message="agent credential token is required when claimed agent identity is set",
            status=2,
        )
    return ResolvedClientAuth(
        auth_token=auth_token,
        headers=agent_auth_headers(AgentRef(project_id=project_id, agent_id=agent_id), auth_token),
        profile_name=None if profile is None else profile.name,
    )


def _validate_explicit_caller_matches_inferred(
    explicit_project_id: str | None,
    explicit_agent_id: str | None,
    inferred: AgentRef | None,
) -> None:
    if inferred is None:
        return
    if explicit_project_id is not None and explicit_project_id != inferred.project_id:
        raise CliHandledError(
            code="invalid_arguments",
            message="claimed agent project does not match command actor",
            status=2,
        )
    if explicit_agent_id is not None and explicit_agent_id != inferred.agent_id:
        raise CliHandledError(
            code="invalid_arguments",
            message="claimed agent id does not match command actor",
            status=2,
        )


def _validate_explicit_caller_matches_profile(
    explicit_project_id: str | None,
    explicit_agent_id: str | None,
    profile: AgentProfile,
) -> None:
    if explicit_project_id is not None and explicit_project_id != profile.project_id:
        raise CliHandledError(
            code="invalid_arguments",
            message="claimed agent project does not match profile",
            status=2,
        )
    if explicit_agent_id is not None and explicit_agent_id != profile.agent_id:
        raise CliHandledError(
            code="invalid_arguments",
            message="claimed agent id does not match profile",
            status=2,
        )


def _resolve_profile_for_command(
    args: argparse.Namespace,
    runtime,
    inferred: AgentRef | None,
    *,
    admin_client_factory: Callable[..., Any] | None,
) -> AgentProfile | None:
    explicit_profile = runtime.profile_name
    ensure_profile = _command_bootstraps_profile(args)
    profile_capable = _command_supports_inferred_profile(args)
    if ensure_profile:
        profile_capable = True
    if inferred is None and not ensure_profile and explicit_profile is None:
        return None
    if explicit_profile is None and not ensure_profile and not profile_capable:
        return None

    expected_name = None if inferred is None else profile_key(inferred.project_id, inferred.agent_id)
    selected_name = explicit_profile or expected_name
    if selected_name is None:
        return None
    if explicit_profile is not None and expected_name is not None and explicit_profile != expected_name:
        raise CliHandledError(
            code="invalid_arguments",
            message="profile does not match command actor",
            status=2,
        )
    if ensure_profile:
        _profile_actor_from_args(selected_name, args)

    store = CliProfileStore(runtime.write_config_path)
    with store.locked():
        profile = store.profile(selected_name)
        if profile is not None and ensure_profile:
            profile = _ensure_profile_token_usable(
                profile,
                args=args,
                runtime=runtime,
                store=store,
                admin_client_factory=admin_client_factory,
            )
        if profile is not None:
            return profile
        if ensure_profile:
            return _bootstrap_profile(
                selected_name,
                args=args,
                runtime=runtime,
                store=store,
                admin_client_factory=admin_client_factory,
            )
    if explicit_profile is not None:
        raise CliHandledError(
            code="profile_missing",
            message=f"CLI profile not found: {explicit_profile}",
            status=404,
        )
    return None


def _command_supports_inferred_profile(args: argparse.Namespace) -> bool:
    command = getattr(args, "command", None)
    if command == "dispatch":
        return True
    if command == "turn" and getattr(args, "turn_command", None) == "resume":
        return True
    if command == "agent" and getattr(args, "agent_command", None) in {"drain", "resume"}:
        return True
    if command == "provision" and getattr(args, "provision_command", None) == "spawn":
        return True
    if command == "worker" and getattr(args, "worker_command", None) in {"once", "loop"}:
        return True
    if command == "smoke" and getattr(args, "smoke_command", None) == "pair":
        return True
    return command == "report" and getattr(args, "report_command", None) == "work-memory"


def _ensure_profile_token_usable(
    profile: AgentProfile,
    *,
    args: argparse.Namespace,
    runtime,
    store: CliProfileStore,
    admin_client_factory: Callable[..., Any] | None,
) -> AgentProfile:
    try:
        token = read_token_file(profile.token_file)
    except FileNotFoundError:
        return _bootstrap_profile(profile.name, args=args, runtime=runtime, store=store, admin_client_factory=admin_client_factory)
    except PermissionError as exc:
        raise CliHandledError(
            code="profile_token_permissions",
            message=str(exc),
            status=403,
        ) from exc
    if token is None:
        return _bootstrap_profile(profile.name, args=args, runtime=runtime, store=store, admin_client_factory=admin_client_factory)
    try:
        _validate_agent_profile_token(runtime.base_url, profile, token)
        return profile
    except httpx.HTTPStatusError as exc:
        if _is_stale_credential_error(exc):
            return _bootstrap_profile(profile.name, args=args, runtime=runtime, store=store, admin_client_factory=admin_client_factory)
        raise


def _bootstrap_profile(
    name: str,
    *,
    args: argparse.Namespace,
    runtime,
    store: CliProfileStore,
    admin_client_factory: Callable[..., Any] | None,
) -> AgentProfile:
    if runtime.admin_auth_token is None:
        raise CliHandledError(
            code="profile_auth_required",
            message="Admin Service bearer token is required to ensure an Agent profile",
            status=401,
        )
    project_id, agent_id = _profile_actor_from_args(name, args)
    profile_kind = _required_arg(args, "profile_kind", "--profile-kind")
    runtime_kind = _required_arg(args, "runtime_kind", "--runtime-kind")
    display_name = _required_arg(args, "display_name", "--display-name")
    invitation_code = _invitation_code_from_args(args)
    response = _request_agent_credential_token(
        admin_base_url=runtime.admin_base_url or runtime.base_url,
        admin_auth_token=runtime.admin_auth_token,
        project_id=project_id,
        agent_id=agent_id,
        profile_kind=profile_kind,
        runtime_kind=runtime_kind,
        display_name=display_name,
        description=getattr(args, "description", None),
        invitation_code=invitation_code,
        admin_client_factory=admin_client_factory,
    )
    credential = response.get("credential")
    token = response.get("agent_credential_token")
    if not isinstance(credential, Mapping) or not isinstance(credential.get("credential_id"), str):
        raise CliHandledError(code="invalid_input", message="Admin Service response missing credential_id", status=422)
    if not isinstance(token, str) or not token.strip():
        raise CliHandledError(code="invalid_input", message="Admin Service response missing agent credential token", status=422)
    credential_id = credential["credential_id"]
    token_file = default_token_file(project_id=project_id, agent_id=agent_id, credential_id=credential_id)
    write_token_file(token_file, token)
    profile = AgentProfile(
        project_id=project_id,
        agent_id=agent_id,
        profile_kind=profile_kind,
        runtime_kind=runtime_kind,
        display_name=display_name,
        credential_id=credential_id,
        token_file=str(token_file),
        status="ready",
    )
    store.upsert_profile(profile)
    return profile


def _request_agent_credential_token(
    *,
    admin_base_url: str,
    admin_auth_token: str,
    project_id: str,
    agent_id: str,
    profile_kind: str,
    runtime_kind: str,
    display_name: str,
    description: str | None,
    invitation_code: str | None,
    admin_client_factory: Callable[..., Any] | None,
) -> Mapping[str, Any]:
    close_client = False
    client = None
    try:
        if admin_client_factory is None:
            client = httpx.Client(base_url=admin_base_url.rstrip("/"), timeout=10.0)
            close_client = True
        else:
            client = admin_client_factory(base_url=admin_base_url.rstrip("/"))
        payload = {
            "request_id": f"profile-bootstrap:{project_id}:{agent_id}:{profile_kind}:{runtime_kind}",
            "requested_agent_id": agent_id,
            "display_name": display_name,
            "description": description,
            "runtime_kind": runtime_kind,
            "profile_kind": profile_kind,
        }
        if invitation_code is not None:
            payload["invitation_code"] = invitation_code
        response = client.post(
            f"/admin/v1/projects/{project_id}/agent-credential-tokens:request",
            headers={"Authorization": f"Bearer {admin_auth_token}"},
            json=payload,
        )
        if response.status_code in {401, 403}:
            response_code = _response_error_code(response)
            if response_code in ADMIN_SERVICE_PROFILE_BOOTSTRAP_ERROR_CODES:
                raise CliHandledError(
                    code=response_code,
                    message=_response_message(response) or "Admin Service profile bootstrap failed",
                    status=response.status_code,
                )
            raise CliHandledError(
                code="profile_auth_required",
                message=_response_message(response) or "Admin Service bearer auth is required to ensure an Agent profile",
                status=response.status_code,
            )
        response_code = _response_error_code(response)
        if response_code in ADMIN_SERVICE_PROFILE_BOOTSTRAP_ERROR_CODES:
            raise CliHandledError(
                code=response_code,
                message=_response_message(response) or "Admin Service profile bootstrap failed",
                status=response.status_code,
            )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, Mapping):
            raise CliHandledError(code="invalid_input", message="Admin Service response must be a JSON object", status=422)
        return data
    finally:
        if close_client and client is not None:
            client.close()


def _request_agent_join_invite(
    *,
    admin_base_url: str,
    admin_auth_token: str,
    project_id: str,
    agent_id: str,
    profile_kind: str,
    runtime_kind: str,
    display_name: str | None,
    description: str | None,
    expires_in_seconds: int,
    max_uses: int,
    admin_client_factory: Callable[..., Any] | None,
) -> Mapping[str, Any]:
    close_client = False
    client = None
    try:
        if admin_client_factory is None:
            client = httpx.Client(base_url=admin_base_url.rstrip("/"), timeout=10.0)
            close_client = True
        else:
            client = admin_client_factory(base_url=admin_base_url.rstrip("/"))
        response = client.post(
            f"/admin/v1/projects/{project_id}/agent-join-invites",
            headers={"Authorization": f"Bearer {admin_auth_token}"},
            json={
                "agent_id": agent_id,
                "profile_kind": profile_kind,
                "runtime_kind": runtime_kind,
                "display_name": display_name,
                "description": description,
                "expires_in_seconds": expires_in_seconds,
                "max_uses": max_uses,
                "single_use": max_uses == 1,
            },
        )
        _raise_admin_stable_error(response, default_code="admin_auth_required")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, Mapping):
            raise CliHandledError(code="invalid_input", message="Admin Service invite response must be a JSON object", status=422)
        return data
    finally:
        if close_client and client is not None:
            client.close()


def _redeem_agent_join(
    *,
    admin_base_url: str,
    join_code: str,
    admin_client_factory: Callable[..., Any] | None,
) -> Mapping[str, Any]:
    close_client = False
    client = None
    try:
        if admin_client_factory is None:
            client = httpx.Client(base_url=admin_base_url.rstrip("/"), timeout=10.0)
            close_client = True
        else:
            client = admin_client_factory(base_url=admin_base_url.rstrip("/"))
        response = client.post(
            "/admin/v1/agent-joins:redeem",
            json={"join_code": join_code},
        )
        _raise_admin_stable_error(response, default_code="join_failed")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, Mapping):
            raise CliHandledError(code="invalid_input", message="Admin Service join response must be a JSON object", status=422)
        return data
    finally:
        if close_client and client is not None:
            client.close()


def _raise_admin_stable_error(response: httpx.Response, *, default_code: str) -> None:
    if response.status_code < 400:
        return
    response_code = _response_error_code(response) or default_code
    raise CliHandledError(
        code=response_code,
        message=_response_message(response) or response.text.strip() or "Admin Service request failed",
        status=response.status_code,
    )


def _validate_agent_profile_token(base_url: str, profile: AgentProfile, token: str) -> None:
    with httpx.Client(base_url=base_url.rstrip("/"), timeout=5.0) as client:
        response = client.get(
            f"/v3r1/projects/{profile.project_id}/agents/{profile.agent_id}",
            headers=agent_auth_headers(AgentRef(project_id=profile.project_id, agent_id=profile.agent_id), token),
        )
        response.raise_for_status()


def _profile_token(profile: AgentProfile | None) -> str | None:
    if profile is None:
        return None
    try:
        token = read_token_file(profile.token_file)
    except FileNotFoundError as exc:
        raise CliHandledError(
            code="profile_stale",
            message=f"CLI profile token file is missing: {profile.name}",
            status=401,
        ) from exc
    except PermissionError as exc:
        raise CliHandledError(
            code="profile_token_permissions",
            message=str(exc),
            status=403,
        ) from exc
    if token is None:
        raise CliHandledError(
            code="profile_stale",
            message=f"CLI profile token file is empty: {profile.name}",
            status=401,
        )
    return token


def _is_stale_credential_error(exc: httpx.HTTPStatusError) -> bool:
    try:
        payload = exc.response.json()
    except ValueError:
        payload = {}
    message = payload.get("message") if isinstance(payload, Mapping) else None
    if not isinstance(message, str):
        return False
    return (
        "agent credential not found" in message
        or "agent credential expired" in message
        or "agent credential status is not active" in message
        or "agent credential is no longer active" in message
        or "invalid agent credential secret" in message
    )


def _response_message(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or None
    if not isinstance(payload, Mapping):
        return None
    message = payload.get("message")
    return message if isinstance(message, str) and message else None


def _response_error_code(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, Mapping):
        return None
    code = payload.get("code")
    return code if isinstance(code, str) and code else None


def _profile_actor_from_args(profile_name: str, args: argparse.Namespace) -> tuple[str, str]:
    project_id = getattr(args, "project_id", None)
    agent_id = getattr(args, "agent_id", None) or getattr(args, "requested_agent_id", None)
    if not project_id or not agent_id:
        raise CliHandledError(
            code="invalid_arguments",
            message="profile ensure requires project id and agent id",
            status=2,
        )
    expected = profile_key(project_id, agent_id)
    if profile_name != expected:
        raise CliHandledError(
            code="invalid_arguments",
            message="profile does not match requested agent",
            status=2,
        )
    return project_id, agent_id


def _required_arg(args: argparse.Namespace, attr: str, flag_name: str) -> str:
    value = getattr(args, attr, None)
    if not isinstance(value, str) or not value.strip():
        raise CliHandledError(
            code="invalid_arguments",
            message=f"{flag_name} is required when ensuring an Agent profile",
            status=2,
        )
    return value.strip()


def _required_mapping_string(value: Mapping[str, Any], field_name: str) -> str:
    item = value.get(field_name)
    if not isinstance(item, str) or not item.strip():
        raise CliHandledError(code="invalid_input", message=f"Admin Service response missing {field_name}", status=422)
    return item.strip()


def _optional_mapping_string(value: Mapping[str, Any], field_name: str) -> str | None:
    item = value.get(field_name)
    if item is None:
        return None
    if not isinstance(item, str):
        raise CliHandledError(code="invalid_input", message=f"Admin Service response field {field_name} must be a string", status=422)
    return item.strip() or None


def _display_name_from_agent_id(agent_id: str) -> str:
    return agent_id.replace("-", " ").replace("_", " ").strip().title() or agent_id


def _agent_join_urls(args: argparse.Namespace, runtime) -> tuple[str, str]:
    positional = getattr(args, "server_url", None)
    if isinstance(positional, str) and positional.strip():
        single_port_url = positional.strip().rstrip("/")
        return single_port_url, single_port_url
    return runtime.base_url.rstrip("/"), (runtime.admin_base_url or runtime.base_url).rstrip("/")


def _agent_join_code(args: argparse.Namespace) -> str:
    positional = getattr(args, "join_code_arg", None)
    flagged = getattr(args, "join_code", None)
    if positional and flagged and positional != flagged:
        raise CliHandledError(code="invalid_arguments", message="join code positional value conflicts with --join-code", status=2)
    value = flagged or positional
    if not isinstance(value, str) or not value.strip():
        raise CliHandledError(code="invalid_arguments", message="cg agent join requires a join code", status=2)
    return value.strip()


def _parse_duration_seconds(value: str) -> int:
    if not isinstance(value, str) or not value.strip():
        raise CliHandledError(code="invalid_arguments", message="--expires-in must be non-empty", status=2)
    text = value.strip().lower()
    suffix = text[-1]
    multiplier = 1
    number = text
    if suffix in {"s", "m", "h", "d"}:
        number = text[:-1]
        multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[suffix]
    try:
        parsed = int(number)
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message="--expires-in must be a duration like 24h, 60m, or 3600s", status=2) from exc
    if parsed <= 0:
        raise CliHandledError(code="invalid_arguments", message="--expires-in must be greater than zero", status=2)
    return parsed * multiplier


def _invitation_code_from_args(args: argparse.Namespace) -> str | None:
    invitation_code_file = getattr(args, "invitation_code_file", None)
    if isinstance(invitation_code_file, str) and invitation_code_file.strip():
        path = Path(invitation_code_file).expanduser()
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise CliHandledError(
                code="invalid_arguments",
                message=f"unable to read --invitation-code-file: {exc}",
                status=2,
            ) from exc
        if not value:
            raise CliHandledError(
                code="invalid_arguments",
                message="--invitation-code-file must not be empty",
                status=2,
            )
        return value

    invitation_code = getattr(args, "invitation_code", None)
    if invitation_code is None:
        return None
    if not isinstance(invitation_code, str) or not invitation_code.strip():
        raise CliHandledError(
            code="invalid_arguments",
            message="--invitation-code must not be empty",
            status=2,
        )
    return invitation_code.strip()


def _infer_authenticated_caller(args: argparse.Namespace) -> AgentRef | None:
    command = getattr(args, "command", None)
    if command == "dispatch":
        return AgentRef(project_id=args.project_id, agent_id=args.requested_by)
    if command == "turn" and getattr(args, "turn_command", None) == "resume":
        return AgentRef(project_id=args.project_id, agent_id=args.requested_by)
    if command == "agent" and getattr(args, "agent_command", None) in {"drain", "resume"}:
        return AgentRef(project_id=args.project_id, agent_id=args.requested_by or args.agent_id)
    if command == "provision" and getattr(args, "provision_command", None) == "spawn":
        return AgentRef(project_id=args.project_id, agent_id=args.requested_by)
    if command == "report" and getattr(args, "report_command", None) == "work-memory":
        return AgentRef(project_id=args.project_id, agent_id=args.agent_id)
    if command == "smoke" and getattr(args, "smoke_command", None) == "pair":
        return _profile_ref_from_name(args.profile)
    if command == "worker" and getattr(args, "worker_command", None) == "claim":
        claim_command = getattr(args, "claim_command", None)
        if claim_command in {"next", "run"}:
            return AgentRef(project_id=args.project_id, agent_id=args.agent_id)
        if claim_command in {"renew", "append", "finish", "suspend"}:
            return _load_claim_file(args.claim_file).agent_ref()
        if claim_command == "dispatch-child":
            claim = _load_claim_file(args.claim_file)
            return AgentRef(project_id=claim.project_id, agent_id=args.requested_by)
    if command == "worker" and getattr(args, "worker_command", None) in {"once", "loop"}:
        return _worker_adapter_agent_ref(args)
    return None


def _profile_ref_from_name(profile_name: str) -> AgentRef:
    if not isinstance(profile_name, str) or "/" not in profile_name:
        raise CliHandledError(code="invalid_arguments", message="profile must be <project_id>/<agent_id>", status=2)
    project_id, agent_id = profile_name.split("/", 1)
    if not project_id or not agent_id or "/" in agent_id:
        raise CliHandledError(code="invalid_arguments", message="profile must be <project_id>/<agent_id>", status=2)
    return AgentRef(project_id=project_id, agent_id=agent_id)


def _worker_adapter_agent_ref(args: argparse.Namespace) -> AgentRef:
    project_id = getattr(args, "project_id", None)
    agent_id = getattr(args, "agent_id", None)
    if project_id and agent_id:
        return AgentRef(project_id=project_id, agent_id=agent_id)
    profile_name = getattr(args, "profile", None)
    if profile_name:
        ref = _profile_ref_from_name(profile_name)
        if project_id and project_id != ref.project_id:
            raise CliHandledError(code="invalid_arguments", message="--project-id does not match --profile", status=2)
        if agent_id and agent_id != ref.agent_id:
            raise CliHandledError(code="invalid_arguments", message="--agent-id does not match --profile", status=2)
        return ref
    raise CliHandledError(code="invalid_arguments", message="cg worker once/loop requires --profile or --project-id with --agent-id", status=2)


def _handle_dispatch(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    request_id, dispatch_key = _resolved_idempotency_anchor(
        request_id=args.request_id,
        dispatch_key=args.dispatch_key,
        command_name="cg dispatch",
    )
    payload = _load_dispatch_payload(args)
    turn = client.dispatch(
        requested_by=AgentRef(project_id=args.project_id, agent_id=args.requested_by),
        target_agent=AgentRef(project_id=args.project_id, agent_id=args.target_agent),
        input_payload=payload,
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id),
        dispatch_key=dispatch_key,
        turn_kind=args.turn_kind,
    )
    return {
        "project_id": turn.project_id,
        "turn_id": turn.turn_id,
        "agent_id": args.target_agent,
        "request_id": request_id,
        "dispatch_key": dispatch_key,
    }


def _handle_turn_get(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    turn = TurnRef(project_id=args.project_id, turn_id=args.turn_id)
    snapshot = client.get_turn(turn)
    return _build_turn_result(client, snapshot)


def _handle_turn_context(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    turn = TurnRef(project_id=args.project_id, turn_id=args.turn_id)
    context = client.fetch_context(
        turn,
        after_turn_seq=args.after_turn_seq,
        limit=args.limit,
    )
    return to_jsonable(context)


def _handle_turn_wait(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    turn = TurnRef(project_id=args.project_id, turn_id=args.turn_id)
    started_at = time.monotonic()
    deadline = started_at + args.timeout_seconds
    poll_count = 0
    while True:
        snapshot = client.get_turn(turn)
        if snapshot.state in WAIT_TERMINAL_STATES:
            result = _build_turn_result(client, snapshot)
            result["poll_count"] = poll_count
            result["waited_seconds"] = round(time.monotonic() - started_at, 3)
            return result
        now = time.monotonic()
        if now >= deadline:
            raise CliHandledError(
                code="timeout",
                message=f"turn {args.turn_id} did not reach a terminal wait state within {args.timeout_seconds} seconds",
                status=408,
            )
        poll_count += 1
        sleep_fn(args.poll_interval_ms / 1000.0)


def _handle_turn_resume(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    turn = TurnRef(project_id=args.project_id, turn_id=args.turn_id)
    requested_by = AgentRef(project_id=args.project_id, agent_id=args.requested_by)
    client.resume_turn(requested_by, turn)
    snapshot = client.get_turn(turn)
    result = _build_turn_result(client, snapshot)
    result["requested_by"] = requested_by.agent_id
    return result


def _handle_agent_get(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    snapshot = client.get_agent(AgentRef(project_id=args.project_id, agent_id=args.agent_id))
    if snapshot is None:
        raise CliHandledError(code="not_found", message=f"agent not found: {args.agent_id}", status=404)
    return {
        "project_id": snapshot.agent.project_id,
        "agent_id": snapshot.agent.agent_id,
        "role": snapshot.role,
        "description": snapshot.description,
        "enabled": snapshot.enabled,
        "accepts_work": snapshot.accepts_work,
        "snapshot": to_jsonable(snapshot),
    }


def _handle_agent_drain(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    agent = AgentRef(project_id=args.project_id, agent_id=args.agent_id)
    requested_by = AgentRef(project_id=args.project_id, agent_id=args.requested_by or args.agent_id)
    client.drain_agent(agent, requested_by=requested_by)
    snapshot = _require_agent_snapshot(client, agent, not_found_message=f"agent not found: {args.agent_id}")
    return {
        "project_id": snapshot.agent.project_id,
        "agent_id": snapshot.agent.agent_id,
        "accepts_work": snapshot.accepts_work,
        "drained": not snapshot.accepts_work,
        "requested_by": requested_by.agent_id,
        "snapshot": to_jsonable(snapshot),
    }


def _handle_agent_resume(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    agent = AgentRef(project_id=args.project_id, agent_id=args.agent_id)
    requested_by = AgentRef(project_id=args.project_id, agent_id=args.requested_by or args.agent_id)
    client.resume_agent(agent, requested_by=requested_by)
    snapshot = _require_agent_snapshot(client, agent, not_found_message=f"agent not found: {args.agent_id}")
    return {
        "project_id": snapshot.agent.project_id,
        "agent_id": snapshot.agent.agent_id,
        "accepts_work": snapshot.accepts_work,
        "resumed": snapshot.accepts_work,
        "requested_by": requested_by.agent_id,
        "snapshot": to_jsonable(snapshot),
    }


def _handle_agent_join(args: argparse.Namespace, *, client: HttpAgentClient | None, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del client, sleep_fn
    runtime = getattr(args, "_resolved_runtime")
    base_url, admin_base_url = _agent_join_urls(args, runtime)
    join_code = _agent_join_code(args)
    response = _redeem_agent_join(
        admin_base_url=admin_base_url,
        join_code=join_code,
        admin_client_factory=getattr(args, "_admin_client_factory", None),
    )
    profile_payload = response.get("profile")
    invite_payload = response.get("invite")
    credential = response.get("credential")
    token = response.get("agent_credential_token")
    if not isinstance(profile_payload, Mapping):
        raise CliHandledError(code="invalid_input", message="Admin Service join response missing profile", status=422)
    if not isinstance(credential, Mapping) or not isinstance(credential.get("credential_id"), str):
        raise CliHandledError(code="invalid_input", message="Admin Service join response missing credential_id", status=422)
    if not isinstance(token, str) or not token.strip():
        raise CliHandledError(code="invalid_input", message="Admin Service join response missing agent credential token", status=422)
    project_id = _required_mapping_string(profile_payload, "project_id")
    agent_id = _required_mapping_string(profile_payload, "agent_id")
    profile_kind = _required_mapping_string(profile_payload, "profile_kind")
    runtime_kind = _required_mapping_string(profile_payload, "runtime_kind")
    display_name = _optional_mapping_string(profile_payload, "display_name")
    if display_name is None and isinstance(invite_payload, Mapping):
        display_name = _optional_mapping_string(invite_payload, "display_name")
    display_name = display_name or _display_name_from_agent_id(agent_id)
    credential_id = credential["credential_id"]
    token_file = default_token_file(project_id=project_id, agent_id=agent_id, credential_id=credential_id)
    write_token_file(token_file, token)
    profile = AgentProfile(
        project_id=project_id,
        agent_id=agent_id,
        profile_kind=profile_kind,
        runtime_kind=runtime_kind,
        display_name=display_name,
        credential_id=credential_id,
        token_file=str(token_file),
        status="ready",
    )
    store = CliProfileStore(runtime.write_config_path)
    with store.locked():
        store.upsert_connection_and_profile(base_url=base_url, admin_base_url=admin_base_url, profile=profile)
    return {
        "profile": profile.name,
        "project_id": profile.project_id,
        "agent_id": profile.agent_id,
        "status": profile.status,
        "base_url": base_url,
        "admin_base_url": admin_base_url,
        "credential_id": profile.credential_id,
    }


def _handle_provision_spawn(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    request_id, dispatch_key = _resolved_idempotency_anchor(
        request_id=args.request_id,
        dispatch_key=args.dispatch_key,
        command_name="cg provision spawn",
    )
    payload = {
        "task": "provision",
        "agent": {
            "role": args.role,
        },
    }
    turn = client.dispatch(
        requested_by=AgentRef(project_id=args.project_id, agent_id=args.requested_by),
        target_agent=AgentRef(project_id=args.project_id, agent_id=args.provisioner_agent),
        input_payload=payload,
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id),
        dispatch_key=dispatch_key,
        turn_kind="turn.provision.agent.spawn.v1",
    )
    return {
        "project_id": turn.project_id,
        "turn_id": turn.turn_id,
        "agent_id": args.provisioner_agent,
        "request_id": request_id,
        "dispatch_key": dispatch_key,
        "turn_kind": "turn.provision.agent.spawn.v1",
        "requested_role": args.role,
    }


def _handle_profile_ensure_agent(args: argparse.Namespace, *, client: HttpAgentClient | None, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del client, sleep_fn
    profile_name = getattr(args, "_resolved_profile_name", None)
    if not profile_name:
        raise CliHandledError(
            code="invalid_arguments",
            message="cg profile ensure-agent requires --profile",
            status=2,
        )
    runtime = getattr(args, "_resolved_runtime")
    store = CliProfileStore(runtime.write_config_path)
    profile = store.profile(profile_name)
    if profile is None:
        raise CliHandledError(code="profile_missing", message=f"CLI profile not found: {profile_name}", status=404)
    return {
        "profile": profile.name,
        "project_id": profile.project_id,
        "agent_id": profile.agent_id,
        "credential_id": profile.credential_id,
        "status": profile.status,
    }


def _handle_report_work_memory(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    manifest = json.loads(Path(args.manifest_file).read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise CliHandledError(code="invalid_input", message="work-memory manifest must be a JSON object", status=422)
    if "meta" in manifest:
        raise CliHandledError(
            code="invalid_input",
            message="work-memory manifest must not include meta; operation metadata and audit annotations are not accepted in prompt-facing manifests",
            status=422,
        )
    if args.request_id:
        existing_request_id = manifest.get("request_id")
        if existing_request_id is not None and existing_request_id != args.request_id:
            raise CliHandledError(
                code="invalid_arguments",
                message="--request-id does not match manifest request_id",
                status=2,
            )
        manifest["request_id"] = args.request_id
    actor = AgentRef(project_id=args.project_id, agent_id=args.agent_id)
    result = client.submit_work_memory_report(actor, manifest)
    return to_jsonable(result)


def _handle_service_run(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    del args
    del stdout
    del stderr
    try:
        service_main()
    except ModuleNotFoundError as exc:
        raise CliHandledError(**_missing_extra_error(exc, extra="server")) from exc
    return 0


def _handle_local_run(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    del stdout
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise CliHandledError(**_missing_extra_error(exc, extra="server")) from exc
    try:
        from CommonGround.service.config import ServiceConfig
        from CommonGround.service.http import create_service_app
        from Integrations.admin_service.admission_runner import LocalAdmissionSettings, create_local_admission_app
    except ModuleNotFoundError as exc:
        raise CliHandledError(**_missing_extra_error(exc, extra="server")) from exc

    project_id = _local_arg_env(args, "project_id", "CG_PROJECT_ID", DEFAULT_LOCAL_PROJECT_ID)
    pg_dsn = _local_arg_env(args, "pg_dsn", "PG_DSN", None)
    if not pg_dsn:
        raise CliHandledError(code="setup_pg_dsn_required", message="PG_DSN or --pg-dsn is required for cg local run", status=2)
    host = _local_arg_env(args, "host", "CG_HOST", "127.0.0.1") or "127.0.0.1"
    port = int(_local_arg_env(args, "port", "CG_PORT", "8000") or "8000")
    log_level = _local_arg_env(args, "log_level", "CG_LOG_LEVEL", "info") or "info"
    base_url = _local_arg_env(args, "base_url", "CG_BASE_URL", None) or f"http://127.0.0.1:{port}"
    service_config = ServiceConfig(
        host=host,
        port=port,
        log_level=log_level,
        pg_dsn=pg_dsn,
    )
    admission_settings = LocalAdmissionSettings(
        pg_dsn=pg_dsn,
        base_url=base_url,
        project_id=project_id or DEFAULT_LOCAL_PROJECT_ID,
        admin_service_token_file=_optional_path(getattr(args, "admin_service_token_file", None))
        or default_admin_service_token_file(project_id or DEFAULT_LOCAL_PROJECT_ID),
        admin_auth_token_file=_optional_path(getattr(args, "admin_auth_token_file", None))
        or default_admin_auth_token_file(project_id or DEFAULT_LOCAL_PROJECT_ID),
        host=host,
        port=port,
        log_level=log_level,
    )
    try:
        app = create_service_app(config=service_config)
        app.mount("/admin", create_local_admission_app(admission_settings, prefix="/v1"))
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message=str(exc), status=2) from exc
    except FileNotFoundError as exc:
        raise CliHandledError(code="file_not_found", message=str(exc), status=404) from exc
    print(
        f"Starting CommonGround local server project={admission_settings.project_id} "
        f"base_url={admission_settings.base_url} host={host} port={port} paths=/v3r1,/admin/v1",
        file=stderr,
    )
    uvicorn.run(app, host=host, port=port, log_level=log_level)
    return 0


def _handle_admission_run(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    del stdout
    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise CliHandledError(**_missing_extra_error(exc, extra="server")) from exc
    try:
        from Integrations.admin_service.admission_runner import create_local_admission_app, resolve_local_admission_settings
    except ModuleNotFoundError as exc:
        raise CliHandledError(**_missing_extra_error(exc, extra="server")) from exc

    try:
        settings = resolve_local_admission_settings(args)
        app = create_local_admission_app(settings)
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message=str(exc), status=2) from exc
    except FileNotFoundError as exc:
        raise CliHandledError(code="file_not_found", message=str(exc), status=404) from exc
    print(
        f"Starting CommonGround Admin Service admission API project={settings.project_id} "
        f"base_url={settings.base_url} host={settings.host} port={settings.port}",
        file=stderr,
    )
    uvicorn.run(app, host=settings.host, port=settings.port, log_level=settings.log_level)
    return 0


def _handle_admission_invite_create(args: argparse.Namespace, *, client: HttpAgentClient | None, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del client, sleep_fn
    runtime = getattr(args, "_resolved_runtime")
    admin_base_url = runtime.admin_base_url or runtime.base_url
    admin_auth_token = runtime.admin_auth_token or _default_admin_auth_token_for_project(args.project_id)
    response = _request_agent_join_invite(
        admin_base_url=admin_base_url,
        admin_auth_token=admin_auth_token,
        project_id=args.project_id,
        agent_id=args.agent_id,
        profile_kind=args.profile_kind,
        runtime_kind=args.runtime_kind,
        display_name=args.display_name,
        description=args.description,
        expires_in_seconds=_parse_duration_seconds(args.expires_in),
        max_uses=args.max_uses,
        admin_client_factory=getattr(args, "_admin_client_factory", None),
    )
    join_code = response.get("join_code")
    invite = response.get("invite")
    if not isinstance(join_code, str) or not join_code.strip():
        raise CliHandledError(code="invalid_input", message="Admin Service invite response missing join_code", status=422)
    if not isinstance(invite, Mapping):
        raise CliHandledError(code="invalid_input", message="Admin Service invite response missing invite", status=422)
    join_command = _agent_join_command_for_invite(
        join_code=join_code,
        join_base_url=args.join_base_url,
        base_url=runtime.base_url,
        admin_base_url=admin_base_url,
    )
    result = {
        "project_id": args.project_id,
        "agent_id": args.agent_id,
        "invite": dict(invite),
        "join_command": join_command,
        "join_code": join_code,
        "canonical_command": "cg agent join",
    }
    if args.out:
        _write_private_json(Path(args.out), result)
    return result


def _agent_join_command_for_invite(*, join_code: str, join_base_url: str | None, base_url: str, admin_base_url: str) -> str:
    if join_base_url:
        return f"cg agent join {shlex.quote(join_base_url)} {shlex.quote(join_code)}"
    if base_url.rstrip("/") == admin_base_url.rstrip("/"):
        return f"cg agent join {shlex.quote(base_url.rstrip('/'))} {shlex.quote(join_code)}"
    return (
        "cg agent join "
        f"--base-url {shlex.quote(base_url.rstrip('/'))} "
        f"--admin-base-url {shlex.quote(admin_base_url.rstrip('/'))} "
        f"--join-code {shlex.quote(join_code)}"
    )


def _default_admin_auth_token_for_project(project_id: str) -> str:
    try:
        token = read_token_file(default_admin_auth_token_file(project_id))
    except FileNotFoundError as exc:
        raise CliHandledError(
            code="admin_auth_required",
            message="Admin Service bearer token is required to create an Agent join invite",
            status=401,
        ) from exc
    except PermissionError as exc:
        raise CliHandledError(code="admin_auth_token_file_invalid", message=str(exc), status=403) from exc
    if not token:
        raise CliHandledError(
            code="admin_auth_required",
            message="Admin Service bearer token is required to create an Agent join invite",
            status=401,
        )
    return token


def _write_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    tmp_fd: int | None = None
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp_fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as stream:
            tmp_fd = None
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    finally:
        if tmp_fd is not None:
            os.close(tmp_fd)
        if tmp_path.exists():
            tmp_path.unlink()


def _local_arg_env(args: argparse.Namespace, attr: str, env_name: str, default: str | None) -> str | None:
    value = getattr(args, attr, None)
    if value is not None:
        return str(value).strip()
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return env_value.strip()
    return default


def service_main() -> None:
    from CommonGround.service import main as _service_main

    _service_main()


def project_status(**kwargs):
    from Integrations.admin_service.project_setup import project_status as _project_status

    return _project_status(**kwargs)


def setup_project(**kwargs):
    from Integrations.admin_service.project_setup import setup_project as _setup_project

    return _setup_project(**kwargs)


def default_admin_service_token_file(project_id: str) -> Path:
    from Integrations.admin_service.project_setup import default_admin_service_token_file as _default_admin_service_token_file

    return _default_admin_service_token_file(project_id)


def default_admin_auth_token_file(project_id: str) -> Path:
    from Integrations.admin_service.project_setup import default_admin_auth_token_file as _default_admin_auth_token_file

    return _default_admin_auth_token_file(project_id)


def _missing_extra_error(exc: ModuleNotFoundError, *, extra: str) -> dict[str, Any]:
    module_name = exc.name or "dependency"
    return {
        "code": "missing_extra",
        "message": f"Missing optional dependency {module_name!r}; install commonground-kernel[{extra}] to use this command.",
        "status": 2,
    }


def _is_psycopg_operational_error(exc: Exception) -> bool:
    if exc.__class__.__name__ != "OperationalError":
        return False
    return exc.__class__.__module__.split(".", 1)[0] == "psycopg"


def _handle_setup_project_seed(args: argparse.Namespace, *, client: HttpAgentClient | None, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del client, sleep_fn
    pg_dsn = _setup_pg_dsn(args)
    project_id = _setup_project_id(args)
    try:
        status = setup_project(
            pg_dsn=pg_dsn,
            project_id=project_id,
            creator_ref=args.creator_ref,
            admin_service_token_file=_setup_admin_service_token_file(args, project_id),
            admin_auth_token_file=_setup_admin_auth_token_file(args, project_id),
            rotate_admin_service_token=args.rotate_admin_service_token,
            rotate_admin_auth_token=args.rotate_admin_auth_token,
        )
    except ConflictError as exc:
        _raise_setup_conflict(exc)
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message=str(exc), status=2) from exc
    return status.to_payload()


def _handle_setup_project_status(args: argparse.Namespace, *, client: HttpAgentClient | None, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del client, sleep_fn
    pg_dsn = _setup_pg_dsn(args)
    project_id = _setup_project_id(args)
    try:
        status = project_status(
            pg_dsn=pg_dsn,
            project_id=project_id,
            admin_service_token_file=_setup_admin_service_token_file(args, project_id),
            admin_auth_token_file=_setup_admin_auth_token_file(args, project_id),
        )
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message=str(exc), status=2) from exc
    if not status.seeded:
        raise CliHandledError(
            code="project_not_seeded",
            message=f"project is not seeded: {project_id}",
            status=404,
        )
    return status.to_payload()


def _handle_setup_project_client_config(args: argparse.Namespace, *, client: HttpAgentClient | None, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del client, sleep_fn
    project_id = _setup_project_id(args)
    config_path = _setup_client_config_path(args)
    admin_auth_token_file = _setup_admin_auth_token_file(args, project_id)
    _validate_admin_auth_token_file(admin_auth_token_file)
    try:
        written = write_cli_client_config(
            config_path,
            base_url=_setup_client_base_url(args),
            admin_base_url=_setup_client_admin_base_url(args),
            admin_auth_token_file=admin_auth_token_file,
        )
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message=str(exc), status=2) from exc
    payload = written.to_payload()
    payload["project_id"] = project_id
    return payload


def _setup_pg_dsn(args: argparse.Namespace) -> str:
    value = getattr(args, "pg_dsn", None) or os.environ.get("PG_DSN")
    if not isinstance(value, str) or not value.strip():
        raise CliHandledError(
            code="setup_pg_dsn_required",
            message="PG_DSN or --pg-dsn is required for cg setup project",
            status=2,
        )
    return value.strip()


def _setup_project_id(args: argparse.Namespace) -> str:
    project_id = getattr(args, "project_id", None)
    if getattr(args, "default_local", False):
        if project_id and project_id != DEFAULT_LOCAL_PROJECT_ID:
            raise CliHandledError(
                code="invalid_arguments",
                message="--default-local cannot be combined with a different --project-id",
                status=2,
            )
        return DEFAULT_LOCAL_PROJECT_ID
    return project_id or DEFAULT_LOCAL_PROJECT_ID


def _optional_path(value: str | None) -> Path | None:
    if not value:
        return None
    return Path(value).expanduser()


def _non_empty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _setup_admin_service_token_file(args: argparse.Namespace, project_id: str) -> Path:
    return _optional_path(args.admin_service_token_file) or default_admin_service_token_file(project_id)


def _setup_admin_auth_token_file(args: argparse.Namespace, project_id: str) -> Path:
    return _optional_path(args.setup_admin_auth_token_file) or default_admin_auth_token_file(project_id)


def _setup_client_config_path(args: argparse.Namespace) -> Path:
    return _optional_path(getattr(args, "config", None)) or _optional_path(os.environ.get("CG_CONFIG_PATH")) or DEFAULT_CONFIG_PATH


def _setup_client_base_url(args: argparse.Namespace) -> str:
    return _non_empty(getattr(args, "base_url", None)) or _non_empty(os.environ.get("CG_BASE_URL")) or DEFAULT_BASE_URL


def _setup_client_admin_base_url(args: argparse.Namespace) -> str:
    return _non_empty(getattr(args, "admin_base_url", None)) or _non_empty(os.environ.get("CG_ADMIN_BASE_URL")) or DEFAULT_ADMIN_BASE_URL


def _validate_admin_auth_token_file(path: Path) -> None:
    try:
        token = read_token_file(path)
    except PermissionError as exc:
        raise CliHandledError(code="admin_auth_token_file_invalid", message=str(exc), status=403) from exc
    if not token:
        raise CliHandledError(
            code="admin_auth_token_file_invalid",
            message=f"Admin Service bearer token file is empty: {path}",
            status=422,
        )


def _raise_setup_conflict(exc: ConflictError) -> None:
    message = str(exc)
    code = "project_bootstrap_conflict"
    if "Admin Service bearer token file" in message:
        code = "admin_auth_token_file_invalid"
    elif "AgentCredential" in message or "admin-service token file" in message:
        code = "admin_service_credential_required"
    raise CliHandledError(code=code, message=message, status=409) from exc


def _handle_project_agent_list(args: argparse.Namespace, *, client: ProjectionHttpClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    return to_jsonable(
        client.list_agents(
            project_id=args.project_id,
            enabled_only=True if args.enabled_only else None,
            accepts_work_only=True if args.accepts_work_only else None,
            role=args.role,
            capability=args.capability,
            limit=args.limit,
        )
    )


def _handle_project_offer_list(args: argparse.Namespace, *, client: ProjectionHttpClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    return to_jsonable(
        client.list_turn_offers(
            project_id=args.project_id,
            turn_kind=args.turn_kind,
            agent_id=args.agent_id,
            enabled_only=True if args.enabled_only else None,
            accepts_work_only=True if args.accepts_work_only else None,
            limit=args.limit,
        )
    )


def _handle_project_offer_get(args: argparse.Namespace, *, client: ProjectionHttpClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    page = client.list_turn_offers(
        project_id=args.project_id,
        turn_kind=args.turn_kind,
        agent_id=args.agent_id,
        limit=2,
    )
    if not page.items:
        raise CliHandledError(
            code="not_found",
            message=f"turn offer not found: agent_id={args.agent_id} turn_kind={args.turn_kind}",
            status=404,
        )
    if len(page.items) > 1:
        raise CliHandledError(
            code="conflict",
            message=f"multiple turn offers matched: agent_id={args.agent_id} turn_kind={args.turn_kind}",
            status=409,
        )
    return to_jsonable(page.items[0])


def _handle_project_turn_list(args: argparse.Namespace, *, client: ProjectionHttpClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    return to_jsonable(
        client.list_turns(
            project_id=args.project_id,
            target_agent_id=args.target_agent_id,
            turn_kind=args.turn_kind,
            state=args.state,
            outcome=args.outcome,
            stop_requested_only=True if args.stop_requested_only else None,
            limit=args.limit,
        )
    )


def _handle_project_turn_lineage(args: argparse.Namespace, *, client: ProjectionHttpClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    return to_jsonable(
        client.get_turn_lineage(
            project_id=args.project_id,
            turn_id=args.turn_id,
            limit=args.limit,
        )
    )


def _handle_project_feed(args: argparse.Namespace, *, client: ProjectionHttpClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    return to_jsonable(
        client.fetch_project_feed(
            project_id=args.project_id,
            after_ledger_seq=args.after_ledger_seq,
            limit=args.limit,
        )
    )


def _handle_smoke_pair(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    requester = _profile_ref_from_name(args.profile)
    project_id = requester.project_id
    projection_client = _make_projection_client_for_handler(args)
    try:
        offers = projection_client.list_turn_offers(
            project_id=project_id,
            turn_kind=args.turn_kind,
            agent_id=args.target_agent,
            enabled_only=True,
            accepts_work_only=True,
            limit=2,
        )
    finally:
        close = getattr(projection_client, "close", None)
        if callable(close):
            close()
    if not offers.items:
        raise CliHandledError(
            code="not_found",
            message=f"target offer not found: agent_id={args.target_agent} turn_kind={args.turn_kind}",
            status=404,
        )
    payload = json.loads(args.payload_json)
    request_id, dispatch_key = _resolved_idempotency_anchor(
        request_id=args.request_id or _default_smoke_request_id(requester.agent_id, args.target_agent),
        dispatch_key=args.dispatch_key,
        command_name="cg smoke pair",
    )
    turn = client.dispatch(
        requested_by=requester,
        target_agent=AgentRef(project_id=project_id, agent_id=args.target_agent),
        input_payload=payload,
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id),
        dispatch_key=dispatch_key,
        turn_kind=args.turn_kind,
    )
    wait_args = argparse.Namespace(
        project_id=turn.project_id,
        turn_id=turn.turn_id,
        timeout_seconds=args.timeout_seconds,
        poll_interval_ms=args.poll_interval_ms,
    )
    terminal = _handle_turn_wait(wait_args, client=client, sleep_fn=sleep_fn)
    context = client.fetch_context(turn, after_turn_seq=0, limit=100)
    return {
        "project_id": project_id,
        "from": requester.agent_id,
        "to": args.target_agent,
        "offer": to_jsonable(offers.items[0]),
        "dispatch": {
            "turn_id": turn.turn_id,
            "request_id": request_id,
            "dispatch_key": dispatch_key,
            "turn_kind": args.turn_kind,
        },
        "terminal_payload": terminal.get("final_payload"),
        "terminal": terminal,
        "context": to_jsonable(context),
    }


def _make_projection_client_for_handler(args: argparse.Namespace):
    factory = getattr(args, "_projection_client_factory", None)
    runtime = getattr(args, "_resolved_runtime")
    auth = getattr(args, "_resolved_auth")
    if factory is None:
        from CommonGround.projection_client import ProjectionHttpClient

        factory = ProjectionHttpClient
    return factory(base_url=runtime.base_url, auth_token=auth.auth_token, headers=auth.headers)


def _default_smoke_request_id(from_agent: str, to_agent: str) -> str:
    return f"smoke-{from_agent}-to-{to_agent}-{int(time.time())}"


def _require_agent_snapshot(client: HttpAgentClient, agent: AgentRef, *, not_found_message: str) -> Any:
    snapshot = client.get_agent(agent)
    if snapshot is None:
        raise CliHandledError(code="not_found", message=not_found_message, status=404)
    return snapshot

def _resolved_idempotency_anchor(*, request_id: str | None, dispatch_key: str | None, command_name: str) -> tuple[str, str]:
    if not request_id and not dispatch_key:
        raise CliHandledError(code="invalid_arguments", message=f"{command_name} requires --request-id or --dispatch-key", status=2)
    normalized_request_id = None if request_id is None else _normalize_cli_dispatch_anchor(request_id, field_name="request_id")
    normalized_dispatch_key = None if dispatch_key is None else _normalize_cli_dispatch_anchor(dispatch_key, field_name="dispatch_key")
    resolved_request_id = normalized_request_id or normalized_dispatch_key
    resolved_spawn_key = normalized_dispatch_key or normalized_request_id
    assert resolved_request_id is not None
    assert resolved_spawn_key is not None
    return resolved_request_id, resolved_spawn_key


def _normalize_cli_dispatch_anchor(value: str, *, field_name: str) -> str:
    try:
        return normalize_dispatch_anchor(value, field_name=field_name)
    except ValueError as exc:
        raise CliHandledError(code="invalid_arguments", message=str(exc), status=2) from exc


def _load_dispatch_payload(args: argparse.Namespace) -> Any:
    if args.payload_file:
        return json.loads(Path(args.payload_file).read_text(encoding="utf-8"))
    if args.payload_json is not None:
        return json.loads(args.payload_json)
    if args.payload_stdin:
        return json.loads(sys.stdin.read())
    raise CliHandledError(code="invalid_arguments", message="cg dispatch requires a payload source", status=2)


def _handle_worker_once(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> CliExitResult:
    return _run_worker_adapter_once(args, client=client, sleep_fn=sleep_fn)


def _handle_worker_loop(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> CliExitResult | dict[str, Any]:
    iterations = 0
    claimed_count = 0
    last_result: dict[str, Any] | None = None
    max_iterations = getattr(args, "max_iterations", None)
    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        run = _run_worker_adapter_once(args, client=client, sleep_fn=sleep_fn)
        last_result = run.payload
        if not run.payload.get("claimed"):
            if max_iterations is not None:
                break
            sleep_fn(args.idle_sleep_seconds)
            continue
        claimed_count += 1
        if run.exit_code != 0:
            return CliExitResult(
                payload={
                    "iterations": iterations,
                    "claimed_count": claimed_count,
                    "last": last_result,
                },
                exit_code=run.exit_code,
            )
    return {
        "iterations": iterations,
        "claimed_count": claimed_count,
        "last": last_result,
    }


def _run_worker_adapter_once(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> CliExitResult:
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise CliHandledError(code="invalid_arguments", message="worker adapter requires --command", status=2)

    agent = _worker_adapter_agent_ref(args)
    claim = client.claim_turn_handle(agent)
    if claim is None:
        return CliExitResult(payload={"claimed": False}, exit_code=0)

    temp_dir = None
    try:
        root = Path(args.work_dir).expanduser() if args.work_dir else None
        if root is None:
            import tempfile

            temp_dir = tempfile.TemporaryDirectory(prefix="cg_worker_adapter_")
            root = Path(temp_dir.name)
        root.mkdir(parents=True, exist_ok=True)
        context_path = Path(args.context_out_file).expanduser() if args.context_out_file else root / "context.json"
        final_path = Path(args.final_in_file).expanduser() if args.final_in_file else root / "final.json"
        suspend_path = Path(args.suspend_in_file).expanduser() if args.suspend_in_file else root / "suspend.json"
        failure_path = Path(args.failure_in_file).expanduser() if args.failure_in_file else root / "failure.json"
        for output_path in (final_path, suspend_path, failure_path):
            if output_path.exists():
                output_path.unlink()
        context_path.parent.mkdir(parents=True, exist_ok=True)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        suspend_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.parent.mkdir(parents=True, exist_ok=True)

        renewer_factory = _claim_auto_renewer_factory()
        renewer = renewer_factory(client, claim=claim, interval_seconds=max(0.01, float(args.renew_interval_seconds)))
        renewer.start()
        try:
            try:
                context = client.fetch_context(claim.turn_ref(), after_turn_seq=args.context_after_turn_seq, limit=args.context_limit)
            except Exception as exc:
                suspend_error = try_suspend_after_context_fetch_error(
                    client,
                    claim,
                    exc,
                    before_suspend=renewer.raise_if_unhealthy,
                )
                result = {
                    "claimed": True,
                    "project_id": claim.project_id,
                    "turn_id": claim.turn_id,
                    "agent_id": claim.agent_id,
                    "context": None,
                    "context_fetch_failed": True,
                    "context_error": str(exc),
                    "suspended_after_failure": suspend_error is None,
                }
                if suspend_error is not None:
                    result["suspend_error"] = str(suspend_error)
                return CliExitResult(payload=result, exit_code=1)

            context_path.write_text(json.dumps(to_jsonable(context), ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
            env = os.environ.copy()
            for key in tuple(env):
                if key.startswith("CG_CLAIM"):
                    env.pop(key, None)
            env.update(
                {
                    "CG_PROJECT_ID": claim.project_id,
                    "CG_TURN_ID": claim.turn_id,
                    "CG_AGENT_ID": claim.agent_id,
                    "CG_CONTEXT_FILE": str(context_path),
                    "CG_FINAL_FILE": str(final_path),
                    "CG_SUSPEND_FILE": str(suspend_path),
                    "CG_FAILURE_FILE": str(failure_path),
                }
            )
            process = subprocess.Popen(command, env=env, stdout=sys.stderr, stderr=sys.stderr)
            while True:
                returncode = process.poll()
                if returncode is not None:
                    break
                fatal = renewer.fatal_error()
                if fatal is not None:
                    process.terminate()
                    try:
                        returncode = process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        returncode = process.wait()
                    return CliExitResult(
                        payload={
                            "claimed": True,
                            "project_id": claim.project_id,
                            "turn_id": claim.turn_id,
                            "agent_id": claim.agent_id,
                            "context_file": str(context_path),
                            "child_exit_code": returncode,
                            "lease_lost": True,
                            "lease_error": str(fatal),
                        },
                        exit_code=1,
                    )
                sleep_fn(0.1)

            fatal = renewer.fatal_error()
            if fatal is not None:
                return CliExitResult(
                    payload={
                        "claimed": True,
                        "project_id": claim.project_id,
                        "turn_id": claim.turn_id,
                        "agent_id": claim.agent_id,
                        "context_file": str(context_path),
                        "child_exit_code": returncode,
                        "lease_lost": True,
                        "lease_error": str(fatal),
                    },
                    exit_code=1,
                )
            return _complete_worker_adapter_claim(
                args,
                client=client,
                claim=claim,
                context_path=context_path,
                final_path=final_path,
                suspend_path=suspend_path,
                failure_path=failure_path,
                child_exit_code=int(returncode),
            )
        finally:
            renewer.stop()
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def _complete_worker_adapter_claim(
    args: argparse.Namespace,
    *,
    client: HttpAgentClient,
    claim,
    context_path: Path,
    final_path: Path,
    suspend_path: Path,
    failure_path: Path,
    child_exit_code: int,
) -> CliExitResult:
    base = {
        "claimed": True,
        "project_id": claim.project_id,
        "turn_id": claim.turn_id,
        "agent_id": claim.agent_id,
        "context_file": str(context_path),
        "child_exit_code": child_exit_code,
        "lease_lost": False,
    }
    if child_exit_code == 0 and suspend_path.exists():
        suspend_payload = _load_json_file(str(suspend_path))
        if not isinstance(suspend_payload, Mapping):
            raise CliHandledError(code="invalid_input", message="worker suspend file must contain a JSON object", status=422)
        reason = suspend_payload.get("reason") or "worker_suspended"
        if not isinstance(reason, str) or not reason.strip():
            raise CliHandledError(code="invalid_input", message="worker suspend reason must be a non-empty string", status=422)
        note = suspend_payload.get("note")
        if note is not None and not isinstance(note, str):
            raise CliHandledError(code="invalid_input", message="worker suspend note must be a string", status=422)
        client.suspend_turn(claim, reason=reason.strip(), note=note)
        snapshot = client.get_turn(claim.turn_ref())
        return CliExitResult(payload={**base, "suspended": True, "reason": reason.strip(), "turn": _build_turn_result(client, snapshot)}, exit_code=0)

    if child_exit_code == 0 and final_path.exists():
        final_payload = _load_json_file(str(final_path))
        client.finish_turn(claim, outcome=TurnOutcome.SUCCEEDED, final_payload=final_payload, final_record_role=args.final_record_role)
        snapshot = client.get_turn(claim.turn_ref())
        return CliExitResult(payload={**base, "finished": True, "outcome": TurnOutcome.SUCCEEDED.value, "turn": _build_turn_result(client, snapshot)}, exit_code=0)

    failure_payload = _worker_failure_payload(failure_path, child_exit_code=child_exit_code)
    client.finish_turn(claim, outcome=TurnOutcome.FAILED, final_payload=failure_payload, final_record_role="error_report")
    snapshot = client.get_turn(claim.turn_ref())
    return CliExitResult(
        payload={**base, "finished": True, "outcome": TurnOutcome.FAILED.value, "failure_payload": failure_payload, "turn": _build_turn_result(client, snapshot)},
        exit_code=1,
    )


def _worker_failure_payload(failure_path: Path, *, child_exit_code: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error": "worker_command_failed" if child_exit_code != 0 else "worker_command_missing_final",
        "child_exit_code": child_exit_code,
    }
    if not failure_path.exists():
        return payload
    failure = _load_json_file(str(failure_path))
    if isinstance(failure, Mapping):
        return {**payload, **dict(failure)}
    payload["details"] = failure
    return payload


def _handle_claim_next(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    from CommonGround.agent_client.types import ClaimTurnPartialFailure

    del sleep_fn
    agent = AgentRef(project_id=args.project_id, agent_id=args.agent_id)
    try:
        claimed = client.claim_turn(
            agent,
            context_after_turn_seq=args.context_after_turn_seq,
            context_limit=args.context_limit,
        )
    except ClaimTurnPartialFailure as exc:
        claim_json = to_jsonable(exc.claim)
        if args.claim_out_file:
            Path(args.claim_out_file).write_text(json.dumps(claim_json, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        result = {
            "claimed": True,
            "project_id": exc.claim.project_id,
            "turn_id": exc.claim.turn_id,
            "agent_id": exc.claim.agent_id,
            "claim": claim_json,
            "context": None,
            "context_fetch_failed": True,
            "context_error": str(exc.context_error),
            "suspended_after_failure": exc.suspend_error is None,
        }
        if exc.suspend_error is not None:
            result["suspend_error"] = str(exc.suspend_error)
        return result
    if claimed is None:
        return {"claimed": False}
    claim_json = to_jsonable(claimed.claim)
    if args.claim_out_file:
        Path(args.claim_out_file).write_text(json.dumps(claim_json, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    return {
        "claimed": True,
        "project_id": claimed.claim.project_id,
        "turn_id": claimed.claim.turn_id,
        "agent_id": claimed.claim.agent_id,
        "claim": claim_json,
        "context": to_jsonable(claimed.context),
    }


def _handle_claim_renew(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    claim = _load_claim_file(args.claim_file)
    renewed = client.renew_claim(claim)
    return {
        "project_id": claim.project_id,
        "turn_id": claim.turn_id,
        "agent_id": claim.agent_id,
        "server_time": renewed.server_time.isoformat(),
        "expires_at": renewed.expires_at.isoformat(),
        "recommended_interval_seconds": renewed.recommended_interval_seconds,
    }


def _handle_claim_run(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> CliExitResult | dict[str, Any]:
    command = list(args.child_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise CliHandledError(code="invalid_arguments", message="claim run requires a command after --", status=2)

    agent = AgentRef(project_id=args.project_id, agent_id=args.agent_id)
    claim = client.claim_turn_handle(agent)
    if claim is None:
        return {"claimed": False}

    claim_json = to_jsonable(claim)
    claim_path = Path(args.claim_out_file) if args.claim_out_file else None
    context_path = Path(args.context_out_file) if args.context_out_file else None
    temp_dir = None
    if claim_path is None or context_path is None:
        import tempfile

        temp_dir = tempfile.TemporaryDirectory(prefix="cg_worker_claim_run_")
        temp_root = Path(temp_dir.name)
        claim_path = claim_path or temp_root / "claim.json"
        context_path = context_path or temp_root / "context.json"

    assert claim_path is not None
    assert context_path is not None
    claim_path.write_text(json.dumps(claim_json, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")

    renewer_factory = _claim_auto_renewer_factory()
    renewer = renewer_factory(client, claim=claim, interval_seconds=max(0.01, float(args.renew_interval_seconds)))
    renewer.start()
    try:
        try:
            context = client.fetch_context(claim.turn_ref(), after_turn_seq=args.context_after_turn_seq, limit=args.context_limit)
        except Exception as exc:
            suspend_error = try_suspend_after_context_fetch_error(
                client,
                claim,
                exc,
                before_suspend=renewer.raise_if_unhealthy,
            )
            result = {
                "claimed": True,
                "project_id": claim.project_id,
                "turn_id": claim.turn_id,
                "agent_id": claim.agent_id,
                "claim": claim_json,
                "claim_file": str(claim_path),
                "context": None,
                "context_fetch_failed": True,
                "context_error": str(exc),
                "suspended_after_failure": suspend_error is None,
            }
            if suspend_error is not None:
                result["suspend_error"] = str(suspend_error)
            return CliExitResult(payload=result, exit_code=1)

        context_path.write_text(json.dumps(to_jsonable(context), ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        env = os.environ.copy()
        env.update(
            {
                "CG_PROJECT_ID": claim.project_id,
                "CG_TURN_ID": claim.turn_id,
                "CG_AGENT_ID": claim.agent_id,
                "CG_CLAIM_FILE": str(claim_path),
                "CG_CONTEXT_FILE": str(context_path),
                "CG_CLAIM_TOKEN": json.dumps(claim_json, ensure_ascii=False, separators=(",", ":")),
            }
        )
        process = subprocess.Popen(command, env=env, stdout=sys.stderr, stderr=sys.stderr)
        while True:
            returncode = process.poll()
            if returncode is not None:
                break
            fatal = renewer.fatal_error()
            if fatal is not None:
                process.terminate()
                try:
                    returncode = process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    returncode = process.wait()
                return CliExitResult(
                    payload={
                        "claimed": True,
                        "project_id": claim.project_id,
                        "turn_id": claim.turn_id,
                        "agent_id": claim.agent_id,
                        "claim": claim_json,
                        "claim_file": str(claim_path),
                        "context_file": str(context_path),
                        "child_exit_code": returncode,
                        "lease_lost": True,
                        "lease_error": str(fatal),
                    },
                    exit_code=1,
                )
            sleep_fn(0.1)

        fatal = renewer.fatal_error()
        if fatal is not None:
            return CliExitResult(
                payload={
                    "claimed": True,
                    "project_id": claim.project_id,
                    "turn_id": claim.turn_id,
                    "agent_id": claim.agent_id,
                    "claim": claim_json,
                    "claim_file": str(claim_path),
                    "context_file": str(context_path),
                    "child_exit_code": returncode,
                    "lease_lost": True,
                    "lease_error": str(fatal),
                },
                exit_code=1,
            )
        return CliExitResult(
            payload={
                "claimed": True,
                "project_id": claim.project_id,
                "turn_id": claim.turn_id,
                "agent_id": claim.agent_id,
                "claim": claim_json,
                "claim_file": str(claim_path),
                "context_file": str(context_path),
                "child_exit_code": returncode,
                "lease_lost": False,
            },
            exit_code=int(returncode),
        )
    finally:
        renewer.stop()
        if temp_dir is not None:
            temp_dir.cleanup()


def _handle_claim_append(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    claim = _load_claim_file(args.claim_file)
    payload = _load_json_file(args.payload_file)
    record = client.append_record(claim, payload, role=args.role)
    return {
        "project_id": claim.project_id,
        "turn_id": claim.turn_id,
        "agent_id": claim.agent_id,
        "role": args.role,
        "record": to_jsonable(record),
    }


def _handle_claim_finish(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    claim = _load_claim_file(args.claim_file)
    final_payload = _load_json_file(args.payload_file) if args.payload_file else _UNSET
    finish_kwargs = {
        "outcome": TurnOutcome(args.outcome),
        "final_record_role": args.final_record_role,
    }
    if final_payload is not _UNSET:
        finish_kwargs["final_payload"] = final_payload
    client.finish_turn(claim, **finish_kwargs)
    snapshot = client.get_turn(claim.turn_ref())
    result = _build_turn_result(client, snapshot)
    result["claimed_by"] = claim.agent_id
    return result


def _handle_claim_dispatch_child(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    claim = _load_claim_file(args.claim_file)
    child_turn = client.dispatch(
        requested_by=AgentRef(project_id=claim.project_id, agent_id=args.requested_by),
        target_agent=AgentRef(project_id=claim.project_id, agent_id=args.target_agent),
        input_payload=_load_json_file(args.payload_file),
        authority=DispatchAuthority(mode=DispatchAuthorityMode.CHILD_DERIVATION, parent_claim=claim),
        dispatch_key=args.dispatch_key,
        turn_kind=args.turn_kind,
    )
    return {
        "project_id": child_turn.project_id,
        "turn_id": child_turn.turn_id,
        "agent_id": args.target_agent,
        "parent_turn_id": claim.turn_id,
        "dispatch_key": args.dispatch_key,
    }


def _handle_claim_suspend(args: argparse.Namespace, *, client: HttpAgentClient, sleep_fn: Callable[[float], None]) -> dict[str, Any]:
    del sleep_fn
    claim = _load_claim_file(args.claim_file)
    client.suspend_turn(claim, reason=args.reason, note=args.note)
    snapshot = client.get_turn(claim.turn_ref())
    result = _build_turn_result(client, snapshot)
    result["claimed_by"] = claim.agent_id
    result["reason"] = args.reason
    return result


def _build_turn_result(client: HttpAgentClient, snapshot: TurnSnapshot) -> dict[str, Any]:
    del client
    return {
        "project_id": snapshot.turn.project_id,
        "turn_id": snapshot.turn.turn_id,
        "agent_id": snapshot.target_agent.agent_id,
        "state": _string_value(snapshot.state),
        "outcome": None if snapshot.outcome is None else _string_value(snapshot.outcome),
        "final_record_role": snapshot.final_record_role,
        "final_payload": snapshot.final_payload,
        "snapshot": to_jsonable(snapshot),
    }


def _load_json_file(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_claim_file(path: str):
    payload = _load_json_file(path)
    if not isinstance(payload, Mapping):
        raise CliHandledError(code="invalid_input", message="claim file must contain a JSON object", status=422)
    if any(key in payload for key in ("ok", "result", "error")):
        result = payload.get("result")
        if not isinstance(result, Mapping) or not isinstance(result.get("claim"), Mapping):
            raise CliHandledError(
                code="invalid_input",
                message="claim file envelope must contain result.claim as an object",
                status=422,
            )
        payload = result["claim"]
    try:
        return _parse_claim_token(payload)
    except (KeyError, TypeError, ValueError) as exc:
        raise CliHandledError(code="invalid_input", message=f"invalid claim file: {exc}", status=422) from exc


def _parse_claim_token(data: Mapping[str, Any]):
    from CommonGround.contracts import ClaimToken

    return ClaimToken(
        project_id=data["project_id"],
        turn_id=data["turn_id"],
        agent_id=data["agent_id"],
        token=data["token"],
        expires_at=datetime.fromisoformat(data["expires_at"]),
    )


def _claim_auto_renewer_factory():
    global ClaimAutoRenewer
    if ClaimAutoRenewer is None:
        from CommonGround.agent_client.claim_renewer import ClaimAutoRenewer as _ClaimAutoRenewer

        ClaimAutoRenewer = _ClaimAutoRenewer
    return ClaimAutoRenewer


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {item.name: to_jsonable(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return to_jsonable(value.to_dict())
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return to_jsonable(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    return value


def _http_error_payload(exc: httpx.HTTPStatusError) -> dict[str, Any]:
    payload = None
    try:
        payload = exc.response.json()
    except ValueError:
        payload = None
    message = exc.response.text.strip() or str(exc)
    code = _status_to_code(exc.response.status_code)
    if isinstance(payload, dict):
        body_message = payload.get("message")
        if isinstance(body_message, str) and body_message:
            message = body_message
        body_error = payload.get("error")
        if isinstance(body_error, str) and body_error:
            code = _normalize_error_code(body_error)
    return {"code": code, "message": message, "status": exc.response.status_code}


def _normalize_error_code(value: str) -> str:
    stripped = value[:-5] if value.endswith("Error") else value
    chars: list[str] = []
    for index, char in enumerate(stripped):
        if char.isupper() and index > 0 and (not stripped[index - 1].isupper()):
            chars.append("_")
        chars.append(char.lower())
    normalized = "".join(chars).replace("-", "_")
    return normalized or "error"


def _status_to_code(status: int) -> str:
    return {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        408: "timeout",
        409: "conflict",
        422: "invalid_input",
    }.get(status, "http_error")


def _string_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _emit(payload: dict[str, Any], *, stream: TextIO) -> None:
    json.dump(payload, stream, ensure_ascii=False, separators=(",", ":"))
    stream.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
