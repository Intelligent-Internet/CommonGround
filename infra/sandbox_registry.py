from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from core.errors import BadRequestError
from infra.stores.sandbox_store import SandboxStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SandboxRecord:
    project_id: str
    agent_id: str
    sandbox_id: str
    domain: Optional[str]
    template: Optional[str]
    status: str
    expires_at: Optional[datetime]
    hard_expires_at: Optional[datetime]


class SandboxRegistry:
    def __init__(
        self,
        *,
        store: SandboxStore,
        reuse_mode: str = "none",
        idle_ttl_sec: int = 1800,
        hard_ttl_sec: int = 21600,
        lock_timeout_sec: int = 600,
    ):
        self.store = store
        self.reuse_mode = (reuse_mode or "none").strip().lower()
        self.idle_ttl_sec = int(idle_ttl_sec)
        self.hard_ttl_sec = int(hard_ttl_sec)
        self.lock_timeout_sec = int(lock_timeout_sec)

    @property
    def enabled(self) -> bool:
        return self.reuse_mode not in ("", "none", "off", "false", "0")

    def compute_expires(self, *, now: Optional[datetime] = None) -> tuple[datetime, datetime]:
        now = now or _utcnow()
        return (
            now + timedelta(seconds=self.idle_ttl_sec),
            now + timedelta(seconds=self.hard_ttl_sec),
        )

    def is_expired(self, row: Dict[str, Any]) -> bool:
        now = _utcnow()
        expires_at = row.get("expires_at")
        hard_expires_at = row.get("hard_expires_at")
        if isinstance(expires_at, datetime) and expires_at < now:
            return True
        if isinstance(hard_expires_at, datetime) and hard_expires_at < now:
            return True
        return False

    async def get_active(self, *, project_id: str, agent_id: str) -> Optional[SandboxRecord]:
        row = await self.store.get_sandbox(project_id=project_id, agent_id=agent_id)
        if not row:
            return None
        if self.is_expired(row):
            return None
        return SandboxRecord(
            project_id=project_id,
            agent_id=agent_id,
            sandbox_id=str(row.get("sandbox_id")),
            domain=row.get("domain"),
            template=row.get("template"),
            status=str(row.get("status") or "active"),
            expires_at=row.get("expires_at"),
            hard_expires_at=row.get("hard_expires_at"),
        )

    async def get_existing(self, *, project_id: str, agent_id: str) -> Optional[Dict[str, Any]]:
        return await self.store.get_sandbox(project_id=project_id, agent_id=agent_id)

    async def acquire_lock(self, *, project_id: str, agent_id: str, lock_id: str) -> None:
        if not lock_id:
            return
        lock_expires_at = _utcnow() + timedelta(seconds=self.lock_timeout_sec)
        ok = await self.store.try_lock(
            project_id=project_id,
            agent_id=agent_id,
            locked_by=lock_id,
            lock_expires_at=lock_expires_at,
        )
        if not ok:
            raise BadRequestError("sandbox is busy; try again later")

    async def release_lock(self, *, project_id: str, agent_id: str, lock_id: str) -> None:
        if not lock_id:
            return
        await self.store.release_lock(project_id=project_id, agent_id=agent_id, locked_by=lock_id)

    async def register_new(
        self,
        *,
        project_id: str,
        agent_id: str,
        sandbox_id: str,
        domain: Optional[str],
        template: Optional[str],
        lock_id: Optional[str],
    ) -> SandboxRecord:
        expires_at, hard_expires_at = self.compute_expires()
        row = await self.store.upsert_sandbox(
            project_id=project_id,
            agent_id=agent_id,
            sandbox_id=sandbox_id,
            domain=domain,
            template=template,
            status="active",
            expires_at=expires_at,
            hard_expires_at=hard_expires_at,
            locked_by=lock_id,
            lock_expires_at=_utcnow() + timedelta(seconds=self.lock_timeout_sec) if lock_id else None,
        )
        return SandboxRecord(
            project_id=project_id,
            agent_id=agent_id,
            sandbox_id=str(row.get("sandbox_id")),
            domain=row.get("domain"),
            template=row.get("template"),
            status=str(row.get("status") or "active"),
            expires_at=row.get("expires_at"),
            hard_expires_at=row.get("hard_expires_at"),
        )

    async def touch(self, *, project_id: str, agent_id: str) -> None:
        expires_at, _ = self.compute_expires()
        await self.store.touch_sandbox(
            project_id=project_id,
            agent_id=agent_id,
            expires_at=expires_at,
        )
