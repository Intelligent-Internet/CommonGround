from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from core.app_config import AppConfig, normalize_config
from infra.observability.otel import init_otel, shutdown_otel
from infra.service_context import ServiceContext
from infra.stores.base import BaseStore


logger = logging.getLogger(__name__)


@dataclass
class ServiceRuntime:
    ctx: ServiceContext
    use_nats: bool = True
    use_cardbox: bool = True
    service_name: Optional[str] = None
    stores: list[BaseStore] = field(default_factory=list)
    _opened: bool = False
    _otel_ready: bool = False

    @classmethod
    def from_config(
        cls,
        config: Optional[AppConfig | Dict[str, Any]] = None,
        *,
        use_nats: bool = True,
        use_cardbox: bool = True,
        service_name: Optional[str] = None,
    ) -> "ServiceRuntime":
        cfg = normalize_config(config)
        ctx = ServiceContext.from_config(cfg)
        return cls(
            ctx=ctx,
            use_nats=use_nats,
            use_cardbox=use_cardbox,
            service_name=service_name,
        )

    def register_store(self, store: BaseStore) -> BaseStore:
        self.stores.append(store)
        return store

    async def open(self) -> None:
        if self._opened:
            return
        self._otel_ready = init_otel(
            cfg=self.ctx.cfg,
            service_name=self.service_name,
        )
        await self.ctx.open_pool()
        if self.use_nats:
            await self.ctx.nats.connect()
        if self.use_cardbox:
            await self.ctx.cardbox.init()
        for store in self.stores:
            await store.open()
        self._opened = True

    async def close(self, *, pool_timeout: Optional[float] = None) -> None:
        if not self._opened:
            return
        for store in reversed(self.stores):
            try:
                await store.close()
            except Exception as exc:
                logger.warning(
                    "ServiceRuntime store close failed for %s: %s",
                    type(store).__name__,
                    exc,
                    exc_info=True,
                )
        if self.use_nats:
            try:
                await self.ctx.nats.close()
            except Exception as exc:
                logger.warning("ServiceRuntime NATS close failed: %s", exc, exc_info=True)
        if self.use_cardbox:
            try:
                await self.ctx.cardbox.close()
            except Exception as exc:
                logger.warning("ServiceRuntime CardBox close failed: %s", exc, exc_info=True)
        try:
            await self.ctx.close_pool(timeout=pool_timeout)
        finally:
            if self._otel_ready:
                shutdown_otel()
                self._otel_ready = False
            self._opened = False


class ServiceBase:
    def __init__(
        self,
        config: Optional[AppConfig | Dict[str, Any]] = None,
        *,
        use_nats: bool = True,
        use_cardbox: bool = True,
        service_name: Optional[str] = None,
    ) -> None:
        self.runtime = ServiceRuntime.from_config(
            config,
            use_nats=use_nats,
            use_cardbox=use_cardbox,
            service_name=service_name or self.__class__.__name__,
        )
        self.ctx = self.runtime.ctx
        self.nats = self.ctx.nats
        self.cardbox = self.ctx.cardbox

    def register_store(self, store: BaseStore) -> BaseStore:
        return self.runtime.register_store(store)

    async def open(self) -> None:
        await self.runtime.open()

    async def close(self) -> None:
        await self.runtime.close()
