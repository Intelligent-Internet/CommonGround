from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import (
    BatchStore,
    ExecutionStore,
    IdentityStore,
    ProjectStore,
    ResourceStore,
    StateStore,
    StepStore,
    SkillStore,
)
from infra.stores.pool import create_pool
from psycopg_pool import AsyncConnectionPool
from core.config_defaults import DEFAULT_CARDBOX_DSN
from core.app_config import AppConfig, config_to_dict


@dataclass(frozen=True)
class StoreFactory:
    dsn: str
    min_size: Optional[int]
    max_size: Optional[int]
    pool: AsyncConnectionPool

    def resource_store(self) -> ResourceStore:
        return ResourceStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def execution_store(self) -> ExecutionStore:
        return ExecutionStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def state_store(self) -> StateStore:
        return StateStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def step_store(self) -> StepStore:
        return StepStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def project_store(self) -> ProjectStore:
        return ProjectStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def identity_store(self) -> IdentityStore:
        return IdentityStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def batch_store(self) -> BatchStore:
        return BatchStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)

    def skill_store(self) -> SkillStore:
        return SkillStore(pool=self.pool, dsn=self.dsn, min_size=self.min_size, max_size=self.max_size)


@dataclass(frozen=True)
class ServiceContext:
    cfg: Dict[str, Any]
    nats_cfg: Dict[str, Any]
    cardbox_cfg: Dict[str, Any]
    dsn: str
    db_pool_min_size: Optional[int]
    db_pool_max_size: Optional[int]
    pool: AsyncConnectionPool
    nats: NATSClient
    cardbox: CardBoxClient
    stores: StoreFactory

    @classmethod
    def from_config(
        cls,
        config: Optional[AppConfig | Dict[str, Any]] = None,
        *,
        dsn_default: str = DEFAULT_CARDBOX_DSN,
    ) -> "ServiceContext":
        cfg = config_to_dict(config)
        nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}
        cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
        dsn = cardbox_cfg.get("postgres_dsn", dsn_default)
        min_size = cardbox_cfg.get("psycopg_min_size", cardbox_cfg.get("postgres_min_size"))
        max_size = cardbox_cfg.get("psycopg_max_size", cardbox_cfg.get("postgres_max_size"))
        pool = create_pool(str(dsn), min_size=min_size, max_size=max_size)
        store_factory = StoreFactory(dsn=str(dsn), min_size=min_size, max_size=max_size, pool=pool)
        return cls(
            cfg=cfg,
            nats_cfg=nats_cfg,
            cardbox_cfg=cardbox_cfg,
            dsn=str(dsn),
            db_pool_min_size=min_size,
            db_pool_max_size=max_size,
            pool=pool,
            nats=NATSClient(config=nats_cfg),
            cardbox=CardBoxClient(config=cardbox_cfg, pool=pool),
            stores=store_factory,
        )

    async def open_pool(self) -> None:
        await self.pool.open()

    async def close_pool(self, *, timeout: Optional[float] = None) -> None:
        if timeout is None:
            await self.pool.close()
        else:
            await self.pool.close(timeout=timeout)
