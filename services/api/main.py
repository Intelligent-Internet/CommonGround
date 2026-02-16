from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, Query
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.app_config import load_app_config, config_to_dict
from core.config import PROTOCOL_VERSION
from core.config_defaults import DEFAULT_API_LISTEN_HOST, DEFAULT_API_PORT
from core.utils import set_loop_policy, REPO_ROOT
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers
from core.errors import (
    BadRequestError,
    CGError,
    ConflictError,
    InternalError,
    NotFoundError,
    http_detail_from_exception,
    http_status_from_exception,
)
import uuid6
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.l0_engine import L0Engine
from infra.project_bootstrap import ProjectBootstrapper
from infra.service_runtime import ServiceRuntime
from infra.stores import (
    ExecutionStore,
    ProfileStore,
    ProjectAlreadyExistsError,
    ProjectStore,
    ResourceStore,
    StateStore,
    StepStore,
    IdentityStore,
    ToolStore,
    SkillStore,
    ArtifactStore,
)
from services.api.ground_control_models import (
    AgentRoster,
    AgentRosterUpsert,
    AgentStateHead,
    AgentStepRow,
    BatchBoxesRequest,
    BatchBoxesResponse,
    BatchCardsRequest,
    BatchCardsResponse,
    CardBoxDTO,
    CardBoxHydratedDTO,
    CardDTO,
    ExecutionEdgeRow,
    GodStopRequest,
    GodStopResponse,
    IdentityEdgeRow,
    PMOToolCallRequest,
    PMOToolCallResponse,
)
from services.api.models import Project, ProjectCreate, ProjectUpdate
from services.api.profile_models import Profile
from services.api.profile_service import ProfileService
from services.api.profile_yaml import normalize_profile_yaml
from services.api.service import ProjectService
from services.api.tool_models import Tool
from services.api.tool_service import ToolService
from services.api.tool_yaml import normalize_tool_yaml
from services.api.skill_models import Skill, SkillPatch, SkillUploadResponse
from services.api.skill_service import SkillService
from services.api.artifact_models import ArtifactSignedUrlResponse, ArtifactUploadResponse
from infra.gcs_client import GcsConfig
from infra.gcs_client import GcsClient
from infra.artifacts import ArtifactManager
from core.utp_protocol import Card, ToolCallContent, ToolResultContent

set_loop_policy()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("api")
_REPORT_VIEWER_DIR = REPO_ROOT / "observability" / "report_viewer"


def _raise_http_exception(exc: Exception, *, default_code: str = "internal_error") -> None:
    raise HTTPException(
        status_code=http_status_from_exception(exc),
        detail=http_detail_from_exception(exc, default_code=default_code),
    ) from exc


def _is_god_api_enabled() -> bool:
    flag = str(os.getenv("CG_ENABLE_GOD_API", "false")).lower()
    return flag in {"1", "true", "yes", "on"}


def _ensure_table(cfg: dict, key: str) -> dict:
    section = cfg.get(key)
    if not isinstance(section, dict):
        return {}
    return section


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise BadRequestError("artifact storage_uri must be gs://")
    raw = uri[5:]
    if "/" not in raw:
        return raw, ""
    bucket, path = raw.split("/", 1)
    return bucket, path


def _parse_step_cursor(raw_cursor: str) -> tuple[datetime, str]:
    cursor = (raw_cursor or "").strip()
    if not cursor:
        raise BadRequestError("cursor is empty")
    if "|" in cursor:
        started_at_raw, step_id = cursor.split("|", 1)
    elif "," in cursor:
        started_at_raw, step_id = cursor.split(",", 1)
    else:
        raise BadRequestError("cursor must be 'started_at,step_id'")
    started_at_raw = started_at_raw.strip()
    step_id = step_id.strip()
    if not started_at_raw or not step_id:
        raise BadRequestError("cursor must include started_at and step_id")
    if started_at_raw.endswith("Z"):
        started_at_raw = started_at_raw[:-1] + "+00:00"
    try:
        started_at = datetime.fromisoformat(started_at_raw)
    except ValueError as exc:
        raise BadRequestError("invalid cursor started_at") from exc
    return started_at, step_id


async def _parse_yaml_upload(
    file: UploadFile,
    *,
    normalizer: Callable[[Any], Any],
) -> Any:
    try:
        raw = (await file.read()).decode("utf-8")
    except Exception as exc:  # noqa: BLE001
        raise BadRequestError(f"failed to read file: {exc}") from exc

    try:
        import yaml  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise InternalError("PyYAML not installed (add pyyaml)") from exc

    try:
        data = yaml.safe_load(raw)
        return normalizer(data)
    except CGError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise BadRequestError(str(exc)) from exc


class AppState:
    store: Optional[ProjectStore] = None
    cardbox: Optional[CardBoxClient] = None
    nats: Optional[NATSClient] = None
    service: Optional[ProjectService] = None
    profile_service: Optional[ProfileService] = None
    tool_service: Optional[ToolService] = None
    skill_service: Optional[SkillService] = None
    resource_store: Optional[ResourceStore] = None
    state_store: Optional[StateStore] = None
    step_store: Optional[StepStore] = None
    execution_store: Optional[ExecutionStore] = None
    identity_store: Optional[IdentityStore] = None
    runtime: Optional[ServiceRuntime] = None
    artifact_store: Optional[ArtifactStore] = None
    gcs_client: Optional[GcsClient] = None


app_state = AppState()


@asynccontextmanager
async def lifespan(_: FastAPI):
    cfg = config_to_dict(load_app_config())
    cardbox_cfg = _ensure_table(cfg, "cardbox")

    dsn = cardbox_cfg.get("postgres_dsn", "postgresql://postgres:postgres@localhost:5433/cardbox")
    logger.info("init projects api (dsn=...@%s)", dsn.split("@")[-1] if "@" in dsn else "...")

    runtime = ServiceRuntime.from_config(cfg, use_nats=False, use_cardbox=True)
    store = runtime.register_store(runtime.ctx.stores.project_store())
    resource_store = runtime.register_store(runtime.ctx.stores.resource_store())
    state_store = runtime.register_store(runtime.ctx.stores.state_store())
    step_store = runtime.register_store(runtime.ctx.stores.step_store())
    execution_store = runtime.register_store(runtime.ctx.stores.execution_store())
    identity_store = runtime.register_store(runtime.ctx.stores.identity_store())
    await runtime.open()

    nats_client: Optional[NATSClient] = None
    try:
        nats_client = runtime.ctx.nats
        await nats_client.connect()
    except Exception:  # noqa: BLE001
        logger.exception("init nats failed; god-mode endpoints will be unavailable")
        nats_client = None

    bootstrapper = ProjectBootstrapper(pool=store.pool)
    service = ProjectService(store=store, bootstrapper=bootstrapper)

    app_state.store = store
    app_state.service = service
    app_state.nats = nats_client
    app_state.runtime = runtime

    cardbox = runtime.ctx.cardbox
    app_state.cardbox = cardbox
    app_state.profile_service = ProfileService(
        project_store=store,
        profile_store=ProfileStore(pool=runtime.ctx.pool),
        cardbox=cardbox,
    )
    app_state.tool_service = ToolService(project_store=store, tool_store=ToolStore(pool=runtime.ctx.pool))
    app_state.resource_store = resource_store
    app_state.state_store = state_store
    app_state.step_store = step_store
    app_state.execution_store = execution_store
    app_state.identity_store = identity_store
    gcs_cfg = _ensure_table(cfg, "gcs")

    gcs_bucket = str(gcs_cfg.get("bucket") or "").strip()
    gcs_config = GcsConfig(
        bucket=gcs_bucket,
        prefix=str(gcs_cfg.get("prefix") or "").strip(),
        credentials_path=str(gcs_cfg.get("credentials_path") or "").strip() or None,
    )
    if gcs_bucket:
        try:
            app_state.gcs_client = GcsClient(gcs_config)
            app_state.artifact_store = ArtifactStore(dsn)
            await app_state.artifact_store.open()
            app_state.skill_service = SkillService(
                project_store=store,
                skill_store=SkillStore(pool=runtime.ctx.pool),
                gcs_cfg=gcs_config,
            )
            await app_state.skill_service.skill_store.open()
        except Exception:  # noqa: BLE001
            logger.exception("init gcs/skills failed; skills API disabled")
            app_state.gcs_client = None
            app_state.skill_service = None
            if app_state.artifact_store:
                try:
                    await app_state.artifact_store.close()
                except Exception:  # noqa: BLE001
                    logger.exception("failed to close artifact store after gcs init failure")
                app_state.artifact_store = None
    else:
        logger.warning("gcs.bucket missing; skills API disabled")

    try:
        yield
    finally:
        try:
            if app_state.artifact_store:
                await app_state.artifact_store.close()
            if app_state.nats:
                await app_state.nats.close()
        except Exception:  # noqa: BLE001
            logger.exception("failed to close nats")
        try:
            if app_state.runtime:
                await app_state.runtime.close(pool_timeout=10.0)
        except Exception:  # noqa: BLE001
            logger.exception("failed to close pg pool")
        logger.exception("failed to close service runtime")


def get_service() -> ProjectService:
    if not app_state.service:
        _raise_http_exception(InternalError("service not initialized"))
    return app_state.service


def get_profile_service() -> ProfileService:
    if not app_state.profile_service:
        _raise_http_exception(InternalError("profile service not initialized"))
    return app_state.profile_service


def get_tool_service() -> ToolService:
    if not app_state.tool_service:
        _raise_http_exception(InternalError("tool service not initialized"))
    return app_state.tool_service


def get_skill_service() -> SkillService:
    if not app_state.skill_service:
        _raise_http_exception(InternalError("skill service not initialized"))
    return app_state.skill_service


def get_artifact_store() -> ArtifactStore:
    if not app_state.artifact_store:
        _raise_http_exception(InternalError("artifact store not initialized"))
    return app_state.artifact_store


def get_artifact_manager() -> ArtifactManager:
    if not app_state.gcs_client:
        _raise_http_exception(InternalError("gcs client not initialized"))
    store = get_artifact_store()
    return ArtifactManager(store=store, gcs=app_state.gcs_client)


def get_gcs_client() -> GcsClient:
    if not app_state.gcs_client:
        _raise_http_exception(InternalError("gcs client not initialized"))
    return app_state.gcs_client


def get_store() -> ProjectStore:
    if not app_state.store:
        _raise_http_exception(InternalError("store not initialized"))
    return app_state.store


def get_cardbox() -> CardBoxClient:
    if not app_state.cardbox:
        _raise_http_exception(InternalError("cardbox not initialized"))
    return app_state.cardbox


def get_nats() -> NATSClient:
    if not app_state.nats:
        _raise_http_exception(InternalError("nats not initialized"))
    return app_state.nats


def get_resource_store() -> ResourceStore:
    if not app_state.resource_store:
        _raise_http_exception(InternalError("resource store not initialized"))
    return app_state.resource_store


def get_state_store() -> StateStore:
    if not app_state.state_store:
        _raise_http_exception(InternalError("state store not initialized"))
    return app_state.state_store


def get_step_store() -> StepStore:
    if not app_state.step_store:
        _raise_http_exception(InternalError("step store not initialized"))
    return app_state.step_store


def get_execution_store() -> ExecutionStore:
    if not app_state.execution_store:
        _raise_http_exception(InternalError("execution store not initialized"))
    return app_state.execution_store


def get_identity_store() -> IdentityStore:
    if not app_state.identity_store:
        _raise_http_exception(InternalError("identity store not initialized"))
    return app_state.identity_store


app = FastAPI(title="CommonGround API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    # Allow only the known local frontend hosts used during development.
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8010",
        "http://localhost:3000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

if _REPORT_VIEWER_DIR.exists():
    app.mount(
        "/observability/report-viewer",
        StaticFiles(directory=str(_REPORT_VIEWER_DIR), html=True),
        name="report_viewer",
    )


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/observability/report-viewer", include_in_schema=False)
async def report_viewer_index() -> RedirectResponse:
    return RedirectResponse(url="/observability/report-viewer/")


async def _run_project_report_generator(
    *,
    project_id: str,
    since: Optional[str],
    until: Optional[str],
    jaeger: str,
    max_otel_traces: int,
) -> Dict[str, Any]:
    cmd = [
        "uv",
        "run",
        "scripts/admin/report_project_graph.py",
        "--project",
        str(project_id),
        "--jaeger",
        str(jaeger),
        "--max-otel-traces",
        str(int(max_otel_traces)),
    ]
    if since:
        cmd.extend(["--since", str(since)])
    if until:
        cmd.extend(["--until", str(until)])

    env = dict(os.environ)
    if not env.get("CG_CONFIG_TOML"):
        env["CG_CONFIG_TOML"] = str(REPO_ROOT / "config.toml")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(REPO_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout_b, stderr_b = await proc.communicate()
    stdout = stdout_b.decode("utf-8", errors="replace").strip()
    stderr = stderr_b.decode("utf-8", errors="replace").strip()
    if proc.returncode != 0:
        msg = stderr or stdout or f"report generator failed (exit={proc.returncode})"
        raise InternalError(msg)
    if not stdout:
        raise InternalError("report generator returned empty output")
    try:
        data = json.loads(stdout)
    except Exception as exc:  # noqa: BLE001
        logger.exception("invalid report json from generator")
        raise InternalError(f"invalid report json: {exc}") from exc
    if not isinstance(data, dict):
        raise InternalError("report generator output must be a JSON object")
    return data


@app.get("/projects/{project_id}/observability/report")
async def generate_project_observability_report(
    project_id: str,
    since: Optional[str] = Query(default=None, description="ISO8601"),
    until: Optional[str] = Query(default=None, description="ISO8601"),
    jaeger: str = Query(default="http://jaeger:16686"),
    max_otel_traces: int = Query(default=2000, ge=0, le=20000),
) -> Dict[str, Any]:
    try:
        return await _run_project_report_generator(
            project_id=project_id,
            since=since,
            until=until,
            jaeger=jaeger,
            max_otel_traces=max_otel_traces,
        )
    except CGError as exc:
        _raise_http_exception(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("generate_project_observability_report failed")
        _raise_http_exception(exc)


@app.post("/projects", response_model=Project, status_code=201)
async def create_project(data: ProjectCreate, service: ProjectService = Depends(get_service)) -> Project:
    try:
        return await service.create_project(data)
    except CGError as exc:
        _raise_http_exception(exc)
    except ProjectAlreadyExistsError as exc:
        _raise_http_exception(ConflictError(str(exc)))
    except Exception as exc:  # noqa: BLE001
        logger.exception("create_project failed")
        _raise_http_exception(exc)


@app.get("/projects", response_model=list[Project])
async def list_projects(
    limit: int = 100,
    offset: int = 0,
    service: ProjectService = Depends(get_service),
) -> list[Project]:
    return await service.list_projects(limit=limit, offset=offset)


@app.get("/projects/{project_id}", response_model=Project)
async def get_project(project_id: str, service: ProjectService = Depends(get_service)) -> Project:
    project = await service.get_project(project_id)
    if not project:
        _raise_http_exception(NotFoundError("project not found"))
    return project


@app.patch("/projects/{project_id}", response_model=Project)
async def update_project(project_id: str, data: ProjectUpdate, service: ProjectService = Depends(get_service)) -> Project:
    project = await service.update_project(project_id, data)
    if not project:
        _raise_http_exception(NotFoundError("project not found"))
    return project


@app.delete("/projects/{project_id}", status_code=204)
async def delete_project(project_id: str, service: ProjectService = Depends(get_service)) -> None:
    deleted = await service.delete_project(project_id)
    if not deleted:
        _raise_http_exception(NotFoundError("project not found"))


@app.post("/projects/{project_id}/bootstrap", status_code=202)
async def bootstrap_project(project_id: str, service: ProjectService = Depends(get_service)) -> Dict[str, str]:
    try:
        await service.bootstrap_project(project_id)
    except CGError as exc:
        _raise_http_exception(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("bootstrap_project failed")
        _raise_http_exception(exc)
    return {"status": "ok"}


@app.post("/projects/{project_id}/profiles", response_model=Profile, status_code=201)
async def create_profile_from_yaml_upload(
    project_id: str,
    file: UploadFile = File(...),
    service: ProfileService = Depends(get_profile_service),
) -> Profile:
    try:
        profile = await _parse_yaml_upload(file, normalizer=normalize_profile_yaml)
    except CGError as exc:
        _raise_http_exception(exc)

    try:
        return await service.create_profile_from_yaml(project_id=project_id, profile=profile)
    except CGError as exc:
        _raise_http_exception(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("create_profile failed")
        _raise_http_exception(exc)


@app.get("/projects/{project_id}/profiles", response_model=list[Profile])
async def list_profiles(
    project_id: str,
    limit: int = 200,
    offset: int = 0,
    service: ProfileService = Depends(get_profile_service),
) -> list[Profile]:
    try:
        return await service.list_profiles(project_id=project_id, limit=limit, offset=offset)
    except CGError as exc:
        _raise_http_exception(exc)


@app.get("/projects/{project_id}/profiles/{name}", response_model=Profile)
async def get_profile(
    project_id: str,
    name: str,
    service: ProfileService = Depends(get_profile_service),
) -> Profile:
    try:
        profile = await service.get_profile(project_id=project_id, name=name)
    except CGError as exc:
        _raise_http_exception(exc)
    if not profile:
        _raise_http_exception(NotFoundError("profile not found"))
    return profile


@app.delete("/projects/{project_id}/profiles/{name}", status_code=204)
async def delete_profile(
    project_id: str,
    name: str,
    service: ProfileService = Depends(get_profile_service),
) -> None:
    try:
        deleted = await service.delete_profile(project_id=project_id, name=name)
    except CGError as exc:
        _raise_http_exception(exc)
    if not deleted:
        _raise_http_exception(NotFoundError("profile not found"))


@app.post("/projects/{project_id}/tools", response_model=Tool, status_code=201)
async def create_tool_from_yaml_upload(
    project_id: str,
    file: UploadFile = File(...),
    service: ToolService = Depends(get_tool_service),
) -> Tool:
    try:
        tool = await _parse_yaml_upload(file, normalizer=normalize_tool_yaml)
    except CGError as exc:
        _raise_http_exception(exc)

    try:
        return await service.upsert_external_tool(project_id=project_id, tool=tool)
    except CGError as exc:
        _raise_http_exception(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("create_tool failed")
        _raise_http_exception(exc)


@app.get("/projects/{project_id}/tools", response_model=list[Tool])
async def list_tools(
    project_id: str,
    limit: int = 200,
    offset: int = 0,
    service: ToolService = Depends(get_tool_service),
) -> list[Tool]:
    try:
        return await service.list_external_tools(project_id=project_id, limit=limit, offset=offset)
    except CGError as exc:
        _raise_http_exception(exc)


@app.get("/projects/{project_id}/tools/{tool_name}", response_model=Tool)
async def get_tool(
    project_id: str,
    tool_name: str,
    service: ToolService = Depends(get_tool_service),
) -> Tool:
    try:
        tool = await service.get_external_tool(project_id=project_id, tool_name=tool_name)
    except CGError as exc:
        _raise_http_exception(exc)
    if not tool:
        _raise_http_exception(NotFoundError("tool not found"))
    return tool


@app.delete("/projects/{project_id}/tools/{tool_name}", status_code=204)
async def delete_tool(
    project_id: str,
    tool_name: str,
    service: ToolService = Depends(get_tool_service),
) -> None:
    try:
        deleted = await service.delete_external_tool(project_id=project_id, tool_name=tool_name)
    except CGError as exc:
        _raise_http_exception(exc)
    if not deleted:
        _raise_http_exception(NotFoundError("tool not found"))


@app.post("/projects/{project_id}/skills:upload", response_model=SkillUploadResponse, status_code=201)
async def upload_skill(
    project_id: str,
    file: UploadFile = File(...),
    service: SkillService = Depends(get_skill_service),
) -> SkillUploadResponse:
    try:
        data = await file.read()
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(BadRequestError(f"failed to read zip: {exc}"))
    try:
        return await service.upload_skill_zip(project_id=project_id, zip_bytes=data)
    except CGError as exc:
        _raise_http_exception(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("upload_skill failed")
        _raise_http_exception(exc)


@app.post("/projects/{project_id}/artifacts:upload", response_model=ArtifactUploadResponse, status_code=201)
async def upload_artifact(
    project_id: str,
    file: UploadFile = File(...),
    manager: ArtifactManager = Depends(get_artifact_manager),
) -> ArtifactUploadResponse:
    try:
        data = await file.read()
    except Exception as exc:  # noqa: BLE001
        _raise_http_exception(BadRequestError(f"failed to read file: {exc}"))
    if not data:
        _raise_http_exception(BadRequestError("file is empty"))
    filename = file.filename or "upload.bin"
    try:
        record = await manager.save_bytes(
            project_id=project_id,
            filename=filename,
            data=data,
            mime=file.content_type,
            metadata={"source": "upload"},
        )
        return ArtifactUploadResponse(**record)
    except CGError as exc:
        _raise_http_exception(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("upload_artifact failed")
        _raise_http_exception(exc)


@app.get("/projects/{project_id}/skills", response_model=list[Skill])
async def list_skills(
    project_id: str,
    limit: int = 200,
    offset: int = 0,
    service: SkillService = Depends(get_skill_service),
) -> list[Skill]:
    try:
        return await service.list_skills(project_id=project_id, limit=limit, offset=offset)
    except CGError as exc:
        _raise_http_exception(exc)


@app.get("/projects/{project_id}/skills/{skill_name}", response_model=Skill)
async def get_skill(
    project_id: str,
    skill_name: str,
    service: SkillService = Depends(get_skill_service),
) -> Skill:
    try:
        skill = await service.get_skill(project_id=project_id, skill_name=skill_name)
    except CGError as exc:
        _raise_http_exception(exc)
    if not skill:
        _raise_http_exception(NotFoundError("skill not found"))
    return skill


@app.patch("/projects/{project_id}/skills/{skill_name}", response_model=Skill)
async def patch_skill(
    project_id: str,
    skill_name: str,
    patch: SkillPatch,
    service: SkillService = Depends(get_skill_service),
) -> Skill:
    try:
        skill = await service.patch_skill(project_id=project_id, skill_name=skill_name, patch=patch)
    except CGError as exc:
        _raise_http_exception(exc)
    if not skill:
        _raise_http_exception(NotFoundError("skill not found"))
    return skill


@app.post("/projects/{project_id}/skills/{skill_name}:activate", response_model=Skill)
async def activate_skill(
    project_id: str,
    skill_name: str,
    payload: dict,
    service: SkillService = Depends(get_skill_service),
) -> Skill:
    version_hash = payload.get("version_hash") if isinstance(payload, dict) else None
    if not isinstance(version_hash, str) or not version_hash.strip():
        _raise_http_exception(BadRequestError("version_hash is required"))
    try:
        skill = await service.activate_skill_version(
            project_id=project_id, skill_name=skill_name, version_hash=version_hash.strip()
        )
    except CGError as exc:
        _raise_http_exception(exc)
    if not skill:
        _raise_http_exception(NotFoundError("skill or version not found"))
    return skill


@app.delete("/projects/{project_id}/skills/{skill_name}", status_code=204)
async def delete_skill(
    project_id: str,
    skill_name: str,
    service: SkillService = Depends(get_skill_service),
) -> None:
    try:
        deleted = await service.delete_skill(project_id=project_id, skill_name=skill_name)
    except CGError as exc:
        _raise_http_exception(exc)
    if not deleted:
        _raise_http_exception(NotFoundError("skill not found"))


@app.get(
    "/projects/{project_id}/artifacts/{artifact_id}",
    response_model=ArtifactSignedUrlResponse,
)
async def get_artifact_signed_url(
    project_id: str,
    artifact_id: str,
    mode: str = Query("json", pattern="^(json|redirect)$"),
    expires_seconds: int = Query(600, ge=60, le=86400),
    store: ArtifactStore = Depends(get_artifact_store),
    gcs: GcsClient = Depends(get_gcs_client),
):
    row = await store.get_artifact(project_id=project_id, artifact_id=artifact_id)
    if not row:
        _raise_http_exception(NotFoundError("artifact not found"))
    storage_uri = row.get("storage_uri")
    if not isinstance(storage_uri, str) or not storage_uri:
        _raise_http_exception(InternalError("artifact storage_uri missing"))
    bucket, path = _parse_gcs_uri(storage_uri)
    if bucket != gcs.bucket_name:
        _raise_http_exception(BadRequestError("artifact bucket mismatch"))
    try:
        signed_url = gcs.generate_signed_url_raw(path, expires_seconds=expires_seconds)
    except Exception as exc:  # noqa: BLE001
        logger.exception("generate signed url failed")
        _raise_http_exception(exc)

    expires_at = datetime.utcnow().replace(microsecond=0) + timedelta(seconds=int(expires_seconds))

    if mode == "redirect":
        return RedirectResponse(url=signed_url)

    return ArtifactSignedUrlResponse(
        artifact_id=artifact_id,
        project_id=project_id,
        filename=row.get("filename"),
        mime=row.get("mime"),
        size=row.get("size"),
        sha256=row.get("sha256"),
        storage_uri=storage_uri,
        signed_url=signed_url,
        expires_at=expires_at,
    )


@app.get("/projects/{project_id}/state/agents", response_model=list[AgentStateHead])
async def list_agent_state_heads(
    project_id: str,
    channel_id: Optional[str] = None,
    limit: int = 50,
    state_store: StateStore = Depends(get_state_store),
) -> list[AgentStateHead]:
    rows = await state_store.list_agent_state_heads(
        project_id=project_id,
        channel_id=channel_id,
        limit=max(1, min(int(limit), 200)),
    )
    return [AgentStateHead.model_validate(r) for r in (rows or []) if r]


@app.get("/projects/{project_id}/agents", response_model=list[AgentRoster])
async def list_project_agents(
    project_id: str,
    limit: int = 200,
    offset: int = 0,
    worker_target: Optional[str] = None,
    tag: Optional[str] = None,
    resource_store: ResourceStore = Depends(get_resource_store),
) -> list[AgentRoster]:
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    rows = await resource_store.list_roster(
        project_id,
        limit=limit,
        offset=offset,
        worker_target=worker_target,
        tag=tag,
    )
    return [AgentRoster.model_validate(row) for row in (rows or [])]


@app.get("/projects/{project_id}/agents/{agent_id}", response_model=AgentRoster)
async def get_project_agent(
    project_id: str,
    agent_id: str,
    resource_store: ResourceStore = Depends(get_resource_store),
) -> AgentRoster:
    row = await resource_store.fetch_roster(project_id, agent_id)
    if not row:
        _raise_http_exception(NotFoundError("agent not found"))
    return AgentRoster.model_validate(row)


@app.post("/projects/{project_id}/agents", response_model=AgentRoster, status_code=201)
async def upsert_project_agent(
    project_id: str,
    payload: AgentRosterUpsert,
    resource_store: ResourceStore = Depends(get_resource_store),
    state_store: StateStore = Depends(get_state_store),
) -> AgentRoster:
    profile_box_id = payload.profile_box_id
    profile_row = None
    if not profile_box_id and payload.profile_name:
        profile_row = await resource_store.find_profile_by_name(project_id, payload.profile_name)
        if not profile_row:
            _raise_http_exception(NotFoundError("profile not found"))
        profile_box_id = str(profile_row.get("profile_box_id"))
    if not profile_box_id:
        _raise_http_exception(BadRequestError("profile_box_id or profile_name required"))
    if not profile_row:
        profile_row = await resource_store.fetch_profile(project_id, profile_box_id)

    worker_target = payload.worker_target.strip() if payload.worker_target else ""
    if not worker_target and isinstance(profile_row, dict):
        worker_target = str(profile_row.get("worker_target") or "").strip()
    if not worker_target:
        _raise_http_exception(BadRequestError("worker_target is required"))

    if payload.tags is not None:
        tags = list(payload.tags or [])
    else:
        tags = []
        if isinstance(profile_row, dict):
            profile_tags = profile_row.get("tags")
            if isinstance(profile_tags, list):
                tags = profile_tags

    metadata = dict(payload.metadata or {})
    profile_name_value = ""
    if isinstance(profile_row, dict):
        profile_name_value = str(profile_row.get("name") or "").strip()
    if not profile_name_value:
        profile_name_value = (payload.profile_name or "").strip()
    if profile_name_value:
        metadata["profile_name"] = profile_name_value

    await resource_store.upsert_project_agent(
        project_id=project_id,
        agent_id=payload.agent_id,
        profile_box_id=str(profile_box_id),
        worker_target=worker_target,
        tags=tags,
        display_name=payload.display_name,
        owner_agent_id=payload.owner_agent_id,
        metadata=metadata,
    )
    if payload.init_state:
        await state_store.init_if_absent(
            project_id=project_id,
            agent_id=payload.agent_id,
            active_channel_id=payload.channel_id or "public",
            profile_box_id=str(profile_box_id),
        )

    row = await resource_store.fetch_roster(project_id, payload.agent_id)
    if not row:
        _raise_http_exception(InternalError("failed to upsert agent"))
    return AgentRoster.model_validate(row)


@app.get("/projects/{project_id}/state/steps", response_model=list[AgentStepRow])
async def list_agent_steps(
    project_id: str,
    channel_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    agent_turn_id: Optional[str] = None,
    step_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    status: Optional[str] = None,
    started_after: Optional[datetime] = None,
    started_before: Optional[datetime] = None,
    cursor: Optional[str] = None,
    order: Optional[str] = "desc",
    limit: int = 80,
    step_store: StepStore = Depends(get_step_store),
) -> list[AgentStepRow]:
    ch = (channel_id or "").strip()
    agent = (agent_id or "").strip()
    agent_turn = (agent_turn_id or "").strip()
    step = (step_id or "").strip()
    parent = (parent_step_id or "").strip()
    trace = (trace_id or "").strip()
    status_value = (status or "").strip()
    order_value = (order or "desc").strip().lower()
    if order_value not in ("asc", "desc"):
        _raise_http_exception(BadRequestError("order must be 'asc' or 'desc'"))
    if started_after and started_before and started_after > started_before:
        _raise_http_exception(BadRequestError("started_after must be <= started_before"))
    cursor_started_at: Optional[datetime] = None
    cursor_step_id: Optional[str] = None
    if cursor:
        try:
            cursor_started_at, cursor_step_id = _parse_step_cursor(cursor)
        except CGError as exc:
            _raise_http_exception(exc)
        except Exception as exc:
            _raise_http_exception(BadRequestError(f"invalid cursor: {exc}"))
    rows = await step_store.list_agent_steps(
        project_id=project_id,
        channel_id=ch or None,
        agent_id=agent or None,
        agent_turn_id=agent_turn or None,
        step_id=step or None,
        parent_step_id=parent or None,
        trace_id=trace or None,
        status=status_value or None,
        started_after=started_after,
        started_before=started_before,
        cursor_started_at=cursor_started_at,
        cursor_step_id=cursor_step_id,
        order=order_value,
        limit=max(1, min(int(limit), 300)),
    )
    return [AgentStepRow.model_validate(r) for r in (rows or []) if r]


@app.get("/projects/{project_id}/state/execution/edges", response_model=list[ExecutionEdgeRow])
async def list_execution_edges(
    project_id: str,
    channel_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    primitive: Optional[str] = None,
    edge_phase: Optional[str] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    limit: int = 200,
    execution_store: ExecutionStore = Depends(get_execution_store),
) -> list[ExecutionEdgeRow]:
    ch = (channel_id or "").strip()
    agent = (agent_id or "").strip()
    primitive = (primitive or "").strip()
    edge_phase = (edge_phase or "").strip()
    trace_id = (trace_id or "").strip()
    parent_step_id = (parent_step_id or "").strip()
    correlation_id = (correlation_id or "").strip()
    rows = await execution_store.list_execution_edges(
        project_id=project_id,
        channel_id=ch or None,
        agent_id=agent or None,
        primitive=primitive or None,
        edge_phase=edge_phase or None,
        trace_id=trace_id or None,
        parent_step_id=parent_step_id or None,
        correlation_id=correlation_id or None,
        limit=max(1, min(int(limit), 500)),
    )
    return [ExecutionEdgeRow.model_validate(r) for r in (rows or []) if r]


@app.get("/projects/{project_id}/state/identity/edges", response_model=list[IdentityEdgeRow])
async def list_identity_edges(
    project_id: str,
    channel_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    owner: Optional[str] = None,
    worker_target: Optional[str] = None,
    tag: Optional[str] = None,
    action: Optional[str] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    limit: int = 200,
    identity_store: IdentityStore = Depends(get_identity_store),
) -> list[IdentityEdgeRow]:
    ch = (channel_id or "").strip()
    agent = (agent_id or "").strip()
    owner = (owner or "").strip()
    worker_target = (worker_target or "").strip()
    tag = (tag or "").strip()
    action = (action or "").strip()
    trace_id = (trace_id or "").strip()
    parent_step_id = (parent_step_id or "").strip()
    rows = await identity_store.list_identity_edges(
        project_id=project_id,
        channel_id=ch or None,
        agent_id=agent or None,
        owner=owner or None,
        worker_target=worker_target or None,
        tag=tag or None,
        action=action or None,
        trace_id=trace_id or None,
        parent_step_id=parent_step_id or None,
        limit=max(1, min(int(limit), 500)),
    )
    return [IdentityEdgeRow.model_validate(r) for r in (rows or []) if r]


@app.get("/projects/{project_id}/boxes/{box_id}", response_model=CardBoxDTO)
async def get_box(
    project_id: str,
    box_id: str,
    cardbox: CardBoxClient = Depends(get_cardbox),
) -> CardBoxDTO:
    box = await cardbox.get_box(box_id, project_id=project_id)
    if not box:
        _raise_http_exception(NotFoundError("box not found"))
    return CardBoxDTO(
        box_id=str(getattr(box, "box_id", box_id)),
        card_ids=list(getattr(box, "card_ids", []) or []),
    )


@app.post(
    "/projects/{project_id}/boxes/batch",
    response_model=BatchBoxesResponse,
    response_model_exclude_none=True,
)
async def batch_get_boxes(
    project_id: str,
    payload: BatchBoxesRequest,
    cardbox: CardBoxClient = Depends(get_cardbox),
) -> BatchBoxesResponse:
    raw_ids = list(payload.box_ids or [])
    seen = set()
    deduped_ids: list[str] = []
    for bid in raw_ids:
        if bid not in seen:
            seen.add(bid)
            deduped_ids.append(bid)

    if len(deduped_ids) > 100:
        _raise_http_exception(BadRequestError("box_ids exceeds limit 100"))

    if not deduped_ids:
        return BatchBoxesResponse(boxes=[], missing_box_ids=[], missing_card_ids=[])

    boxes = await cardbox.get_boxes_batch(deduped_ids, project_id=project_id)

    boxes_out: list[CardBoxHydratedDTO] = []
    missing_box_ids: list[str] = []
    box_tail_ids: dict[str, list[str]] = {}
    limit_value = payload.limit if payload.limit is not None else 200
    limit = max(1, min(int(limit_value), 500)) if payload.hydrate else 0

    for requested_id, box in zip(deduped_ids, boxes):
        if not box:
            missing_box_ids.append(requested_id)
            continue
        box_id = str(getattr(box, "box_id", requested_id))
        card_ids = list(getattr(box, "card_ids", []) or [])
        box_dto = CardBoxHydratedDTO(box_id=box_id, card_ids=card_ids)
        if payload.hydrate:
            tail_ids = card_ids[-limit:] if len(card_ids) > limit else list(card_ids)
            box_tail_ids[box_id] = tail_ids
        boxes_out.append(box_dto)

    missing_card_ids: list[str] = []
    if payload.hydrate and box_tail_ids:
        all_tail_ids: list[str] = []
        seen_card_ids = set()
        for tail_ids in box_tail_ids.values():
            for cid in tail_ids:
                if cid not in seen_card_ids:
                    seen_card_ids.add(cid)
                    all_tail_ids.append(cid)

        cards = await cardbox.get_cards_batch(all_tail_ids, project_id=project_id)
        cards_by_id = {card.card_id: card for card in (cards or [])}

        missing_seen = set()
        for box_dto in boxes_out:
            tail_ids = box_tail_ids.get(box_dto.box_id, [])
            hydrated_cards: list[CardDTO] = []
            for cid in tail_ids:
                card = cards_by_id.get(cid)
                if card:
                    hydrated_cards.append(CardDTO.model_validate(card.model_dump()))
                elif cid not in missing_seen:
                    missing_seen.add(cid)
                    missing_card_ids.append(cid)
            box_dto.hydrated_cards = hydrated_cards

    return BatchBoxesResponse(
        boxes=boxes_out,
        missing_box_ids=missing_box_ids,
        missing_card_ids=missing_card_ids,
    )


@app.get("/projects/{project_id}/boxes/{box_id}/cards", response_model=list[CardDTO])
async def list_box_cards(
    project_id: str,
    box_id: str,
    limit: int = 200,
    cardbox: CardBoxClient = Depends(get_cardbox),
) -> list[CardDTO]:
    limit = max(1, min(int(limit), 500))
    box = await cardbox.get_box(box_id, project_id=project_id)
    if not box:
        _raise_http_exception(NotFoundError("box not found"))
    card_ids = list(getattr(box, "card_ids", []) or [])
    if len(card_ids) > limit:
        card_ids = card_ids[-limit:]
    cards = await cardbox.get_cards(card_ids, project_id=project_id)
    return [CardDTO.model_validate(c.model_dump()) for c in (cards or [])]


@app.get("/projects/{project_id}/cards/{card_id}", response_model=CardDTO)
async def get_card(
    project_id: str,
    card_id: str,
    cardbox: CardBoxClient = Depends(get_cardbox),
) -> CardDTO:
    cards = await cardbox.get_cards([card_id], project_id=project_id)
    card = cards[0] if cards else None
    if not card:
        _raise_http_exception(NotFoundError("card not found"))
    return CardDTO.model_validate(card.model_dump())


@app.post("/projects/{project_id}/cards/batch", response_model=BatchCardsResponse)
async def batch_get_cards(
    project_id: str,
    payload: BatchCardsRequest,
    cardbox: CardBoxClient = Depends(get_cardbox),
) -> BatchCardsResponse:
    raw_ids = list(payload.card_ids or [])
    seen = set()
    deduped_ids: list[str] = []
    for cid in raw_ids:
        if cid not in seen:
            seen.add(cid)
            deduped_ids.append(cid)

    if len(deduped_ids) > 100:
        _raise_http_exception(BadRequestError("card_ids exceeds limit 100"))

    if not deduped_ids:
        return BatchCardsResponse(cards=[], missing_ids=[])

    cards = await cardbox.get_cards_batch(deduped_ids, project_id=project_id)
    cards_out = [CardDTO.model_validate(card.model_dump()) for card in (cards or [])]
    found_ids = {card.card_id for card in (cards or [])}

    missing_ids = [cid for cid in deduped_ids if cid not in found_ids]
    return BatchCardsResponse(cards=cards_out, missing_ids=missing_ids)


async def _wait_tool_result_inbox_via_store(
    *,
    execution_store: ExecutionStore,
    project_id: str,
    agent_id: str,
    tool_call_id: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    deadline = asyncio.get_event_loop().time() + float(timeout_seconds)
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise TimeoutError("timeout waiting for tool_result inbox")
        rows = await execution_store.list_inbox_by_correlation(
            project_id=project_id,
            agent_id=agent_id,
            correlation_id=tool_call_id,
            limit=20,
        )
        for row in rows:
            if row.get("message_type") != "tool_result":
                continue
            inbox_id = row.get("inbox_id")
            if inbox_id:
                await execution_store.update_inbox_status(
                    inbox_id=inbox_id,
                    project_id=project_id,
                    status="consumed",
                )
            payload = row.get("payload") or {}
            if isinstance(payload, dict):
                return payload
        await asyncio.sleep(min(0.5, remaining))


@app.post("/projects/{project_id}/god/pmo/{tool_name}:call", response_model=PMOToolCallResponse)
async def god_call_pmo_internal_tool(
    project_id: str,
    tool_name: str,
    data: PMOToolCallRequest,
    nats: NATSClient = Depends(get_nats),
    execution_store: ExecutionStore = Depends(get_execution_store),
    cardbox: CardBoxClient = Depends(get_cardbox),
) -> PMOToolCallResponse:
    if not _is_god_api_enabled():
        _raise_http_exception(NotFoundError("Not Found"))

    tool_call_id = f"call_{uuid6.uuid7().hex}"
    step_id = f"step_gc_{uuid6.uuid7().hex}"
    agent_turn_id = f"turn_gc_{uuid6.uuid7().hex}"

    pmo_subject = f"cg.{PROTOCOL_VERSION}.{project_id}.{data.channel_id}.cmd.sys.pmo.internal.{tool_name}"
    tool_call_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="tool.call",
        content=ToolCallContent(
            tool_name=tool_name,
            arguments=dict(data.args or {}),
            status="called",
            target_subject=pmo_subject,
        ),
        created_at=datetime.utcnow(),
        author_id=str(data.caller_agent_id),
        metadata={
            "agent_turn_id": agent_turn_id,
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "role": "assistant",
            "trace_id": str(data.trace_id or uuid6.uuid7()),
        },
        tool_call_id=tool_call_id,
    )
    await cardbox.save_card(tool_call_card)

    payload = {
        "agent_id": str(data.caller_agent_id),
        "agent_turn_id": agent_turn_id,
        "turn_epoch": 1,
        "tool_call_id": tool_call_id,
        "tool_name": tool_name,
        "after_execution": str(data.after_execution),
        "tool_call_card_id": tool_call_card.card_id,
        "step_id": step_id,
    }

    headers, _, trace_id = ensure_trace_headers({}, trace_id=data.trace_id)
    headers = ensure_recursion_depth(headers, default_depth=0)

    await execution_store.insert_execution_edge(
        edge_id=f"edge_{uuid6.uuid7().hex}",
        project_id=project_id,
        channel_id=data.channel_id,
        primitive="enqueue",
        edge_phase="request",
        source_agent_id=str(data.caller_agent_id),
        source_agent_turn_id=agent_turn_id,
        source_step_id=step_id,
        target_agent_id="sys.pmo",
        target_agent_turn_id=None,
        correlation_id=tool_call_id,
        enqueue_mode="call",
        recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
        trace_id=trace_id,
        parent_step_id=step_id,
        metadata={
            "message_type": "tool_call",
            "enqueue_mode": "call",
            "inbox_id": tool_call_id,
            "recursion_depth": int(headers.get("CG-Recursion-Depth", "0")),
        },
    )
    await nats.publish_event(pmo_subject, payload, headers=headers)

    try:
        inbox_payload = await _wait_tool_result_inbox_via_store(
            execution_store=execution_store,
            project_id=project_id,
            agent_id=str(data.caller_agent_id),
            tool_call_id=tool_call_id,
            timeout_seconds=float(data.timeout_seconds),
        )
    except TimeoutError as exc:
        _raise_http_exception(BadRequestError(str(exc)))

    tool_result: Dict[str, Any] = {}
    tool_result_card_id = inbox_payload.get("tool_result_card_id")
    if isinstance(tool_result_card_id, str) and tool_result_card_id:
        cards = await cardbox.get_cards([tool_result_card_id], project_id=project_id)
        tr = cards[0] if cards else None
        if tr:
            content_obj = getattr(tr, "content", None)
            if isinstance(content_obj, ToolResultContent):
                tool_result = {
                    "status": content_obj.status,
                    "after_execution": content_obj.after_execution,
                    "result": content_obj.result,
                    "error": content_obj.error,
                }
            elif isinstance(content_obj, dict):
                tool_result = content_obj
            else:
                tool_result = {"status": "failed", "result": {"error_message": "tool.result content not dict"}}

    _ = trace_id
    return PMOToolCallResponse(
        tool_call_id=tool_call_id,
        step_id=step_id,
        tool_result_card_id=str(tool_result_card_id) if tool_result_card_id else None,
        tool_result=tool_result,
    )


@app.post("/projects/{project_id}/god/stop", response_model=GodStopResponse)
async def god_stop(
    project_id: str,
    data: GodStopRequest,
    nats: NATSClient = Depends(get_nats),
    execution_store: ExecutionStore = Depends(get_execution_store),
    resource_store: ResourceStore = Depends(get_resource_store),
    state_store: StateStore = Depends(get_state_store),
) -> GodStopResponse:
    if not _is_god_api_enabled():
        _raise_http_exception(NotFoundError("Not Found"))
    payload = {
        "agent_id": data.agent_id,
        "agent_turn_id": data.agent_turn_id,
        "turn_epoch": int(data.turn_epoch),
        "reason": data.reason,
    }
    headers = {"CG-Source": "ground_control"}
    headers, _, trace_id = ensure_trace_headers(headers)
    headers = ensure_recursion_depth(headers, default_depth=0)
    l0 = L0Engine(
        nats=nats,
        execution_store=execution_store,
        resource_store=resource_store,
        state_store=state_store,
    )
    wakeup_signals = []
    async with execution_store.pool.connection() as conn:
        async with conn.transaction():
            result = await l0.report(
                project_id=project_id,
                channel_id=data.channel_id,
                target_agent_id=str(data.agent_id),
                message_type="stop",
                payload=payload,
                correlation_id=str(data.agent_turn_id),
                recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
                trace_id=trace_id,
                source_agent_id="sys.ground_control",
                headers=headers,
                wakeup=True,
                conn=conn,
            )
            wakeup_signals = list(result.wakeup_signals or ())
    if wakeup_signals:
        await l0.publish_wakeup_signals(wakeup_signals)
    return GodStopResponse()


def main() -> None:
    cfg = config_to_dict(load_app_config())
    api_cfg = _ensure_table(cfg, "api")
    host = str(api_cfg.get("listen_host") or DEFAULT_API_LISTEN_HOST)
    port = int(api_cfg.get("port") or DEFAULT_API_PORT)
    uvicorn.run(
        "services.api.main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
