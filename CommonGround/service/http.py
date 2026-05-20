from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
import threading

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from CommonGround.adapters import DispatchIngressAdapter, ManagementAdapter
from CommonGround.app import KernelApp, build_kernel_app
from CommonGround.contracts import ConflictError, FencingError, ForbiddenError, InvariantError, KernelError, NotFoundError, OperationMeta, UnauthorizedError
from CommonGround.infra import PostgresAgentCredentialStore, PostgresConnectionPool

from .config import ServiceConfig
from .projection import PostgresProjectionSource, ProjectionSource
from .projection.router import router as projection_router
from .read_policy import ServiceReadPolicy
from .routes import router
from .write_guard import ServiceWriteGuard

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ServiceDeps:
    config: ServiceConfig
    kernel_app: KernelApp
    management: ManagementAdapter
    dispatch_ingress: DispatchIngressAdapter
    projection_source: ProjectionSource
    credential_store: PostgresAgentCredentialStore
    postgres_pool: PostgresConnectionPool | None
    read_policy: ServiceReadPolicy
    write_guard: ServiceWriteGuard


def create_service_app(
    *,
    config: ServiceConfig | None = None,
    kernel_app: KernelApp | None = None,
    projection_source: ProjectionSource | None = None,
) -> FastAPI:
    config = config or ServiceConfig.from_env()
    postgres_pool = PostgresConnectionPool(config.pg_dsn) if config.pg_dsn else None
    if kernel_app is None:
        if not config.pg_dsn:
            raise ValueError("PG_DSN is required for CommonGround service startup")
        kernel_app = build_kernel_app(
            claim_timeout_seconds=config.claim_timeout_seconds,
            pg_dsn=config.pg_dsn,
            connection_provider=postgres_pool,
        )
    # When callers supply a custom kernel_app backed by a non-default truth store,
    # they should also inject a matching projection_source. Otherwise the service
    # falls back to config.pg_dsn for observation reads.
    deps = ServiceDeps(
        config=config,
        kernel_app=kernel_app,
        management=ManagementAdapter(
            topology=kernel_app.topology,
            lifecycle=kernel_app.lifecycle,
            sdk=kernel_app.sdk,
        ),
        dispatch_ingress=DispatchIngressAdapter(sdk=kernel_app.sdk),
        projection_source=projection_source or PostgresProjectionSource(
            config.pg_dsn if config.pg_dsn else _require_projection_pg_dsn(),
            connection_provider=postgres_pool,
        ),
        credential_store=PostgresAgentCredentialStore(
            config.pg_dsn if config.pg_dsn else _require_projection_pg_dsn(),
            connection_provider=postgres_pool,
        ),
        postgres_pool=postgres_pool,
        read_policy=ServiceReadPolicy(),
        write_guard=ServiceWriteGuard(),
    )
    reaper_stop = threading.Event()
    reaper_thread: threading.Thread | None = None

    def _run_claim_reaper() -> None:
        interval_seconds = config.claim_reaper_interval_seconds
        while not reaper_stop.wait(interval_seconds):
            try:
                deps.kernel_app.lifecycle.reconcile_expired_claim(
                    None,
                    meta=OperationMeta(
                        reason="service_claim_reaper",
                        note="service-owned claim reaper",
                        annotations={"owner": "service"},
                    ),
                )
            except Exception:
                logger.warning("service claim reaper iteration failed", exc_info=True)

    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        nonlocal reaper_thread
        try:
            if postgres_pool is not None:
                postgres_pool.open()
            if config.claim_reaper_interval_seconds > 0:
                reaper_thread = threading.Thread(target=_run_claim_reaper, name="cg-claim-reaper", daemon=True)
                reaper_thread.start()
            yield
        finally:
            reaper_stop.set()
            if reaper_thread is not None:
                reaper_thread.join(timeout=max(1.0, config.claim_reaper_interval_seconds * 2))
            if postgres_pool is not None:
                postgres_pool.close()

    app = FastAPI(title="CommonGround Kernel Service", version="v3r1", lifespan=_lifespan)
    app.state.service_deps = deps

    @app.exception_handler(NotFoundError)
    async def _not_found(_, exc: NotFoundError):
        return JSONResponse(status_code=404, content={"error": exc.__class__.__name__, "message": str(exc)})

    @app.exception_handler(UnauthorizedError)
    async def _unauthorized(_, exc: UnauthorizedError):
        return JSONResponse(status_code=401, content={"error": exc.__class__.__name__, "message": str(exc)})

    @app.exception_handler(ForbiddenError)
    async def _forbidden(_, exc: ForbiddenError):
        return JSONResponse(status_code=403, content={"error": exc.__class__.__name__, "message": str(exc)})

    @app.exception_handler(ConflictError)
    async def _conflict(_, exc: ConflictError):
        return JSONResponse(status_code=409, content={"error": exc.__class__.__name__, "message": str(exc)})

    @app.exception_handler(FencingError)
    async def _fencing(_, exc: FencingError):
        return JSONResponse(status_code=409, content={"error": exc.__class__.__name__, "message": str(exc)})

    @app.exception_handler(InvariantError)
    async def _invariant(_, exc: InvariantError):
        return JSONResponse(status_code=422, content={"error": exc.__class__.__name__, "message": str(exc)})

    @app.exception_handler(KernelError)
    async def _kernel(_, exc: KernelError):
        return JSONResponse(status_code=400, content={"error": exc.__class__.__name__, "message": str(exc)})

    app.include_router(router)
    app.include_router(projection_router)
    return app


def _require_projection_pg_dsn() -> str:
    raise ValueError("PG_DSN is required to construct the default projection source")
