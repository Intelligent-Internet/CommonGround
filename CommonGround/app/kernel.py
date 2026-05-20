from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from CommonGround.contracts import CardBoxPort, Clock, LedgerEvent, TruthDebugPort, TurnRow
from CommonGround.infra import PostgresCardBoxService, PostgresTruthRepository
from CommonGround.infra.postgres_pool import ConnectionFactory, SyncPostgresConnectionProvider
from CommonGround.kernel import LedgerKernel, LifecycleKernel, SemanticKernel, TopologyKernel
from CommonGround.sdk import KernelSDK


@dataclass(slots=True)
class KernelDebugInspector:
    truth: TruthDebugPort

    @property
    def ledger_events(self) -> list[LedgerEvent]:
        return self.truth.list_ledger_events()

    def list_turn_rows(self) -> list[TurnRow]:
        return self.truth.list_turn_rows()


@dataclass(slots=True)
class KernelApp:
    debug: KernelDebugInspector
    clock: Clock | None
    cardbox: CardBoxPort
    topology: TopologyKernel
    lifecycle: LifecycleKernel
    semantic: SemanticKernel
    ledger: LedgerKernel
    sdk: KernelSDK

    def advance_time(self, *, seconds: int) -> None:
        if self.clock is None or not hasattr(self.clock, "advance"):
            raise RuntimeError("advance_time requires ManualClock")
        self.clock.advance(delta=timedelta(seconds=seconds))


def build_kernel_app(
    *,
    pg_dsn: str,
    claim_timeout_seconds: int = 30,
    clock: Clock | None = None,
    connection_provider: SyncPostgresConnectionProvider | None = None,
    connection_factory: ConnectionFactory | None = None,
) -> KernelApp:
    truth = PostgresTruthRepository(
        pg_dsn,
        claim_timeout=timedelta(seconds=claim_timeout_seconds),
        clock=clock,
        connection_provider=connection_provider,
        connection_factory=connection_factory,
    )
    debug = KernelDebugInspector(truth=truth)
    content = PostgresCardBoxService(pg_dsn)
    topology = TopologyKernel(truth)
    lifecycle = LifecycleKernel(truth, content)
    semantic = SemanticKernel(truth, content)
    ledger = LedgerKernel(truth)
    sdk = KernelSDK(
        topology=topology,
        lifecycle=lifecycle,
        semantic=semantic,
        ledger=ledger,
        cardbox=content,
    )
    return KernelApp(
        debug=debug,
        clock=clock,
        cardbox=content,
        topology=topology,
        lifecycle=lifecycle,
        semantic=semantic,
        ledger=ledger,
        sdk=sdk,
    )
