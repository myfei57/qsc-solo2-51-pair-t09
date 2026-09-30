"""Wiring of the console and its subsystems from a configuration."""

from dataclasses import dataclass

from .audit.recorder import Recorder
from .clock import LogicalClock
from .config import Config
from .console.plant import Plant
from .console.router import Console
from .console.subscriptions import SnapshotRegistry, SubscriptionService
from .store.snapshot import Snapshot
from .store.store import Store


@dataclass
class Application:
    """Everything the process needs to serve the console."""

    config: Config
    store: Store
    clock: LogicalClock
    recorder: Recorder
    snapshots: SnapshotRegistry
    subscriptions: SubscriptionService
    plant: Plant
    console: Console

    def snapshot(self) -> Snapshot:
        """Frozen view of the persisted document."""
        return self.store.snapshot()


def build_application(config: Config) -> Application:
    """Build the service from a configuration."""
    store = Store(config.store_path())
    clock = LogicalClock()
    recorder = Recorder(store, clock)
    snapshots = SnapshotRegistry(store)
    subscriptions = SubscriptionService(
        store,
        snapshots,
        recorder,
        backlog_limit=config.subscription_backlog,
        max_wait_seconds=float(config.subscription_wait_seconds),
    )
    plant = Plant(store, clock, recorder)
    console = Console(plant, subscriptions)
    plant.set_change_publishers(subscriptions.capture, subscriptions.fanout)
    return Application(
        config=config,
        store=store,
        clock=clock,
        recorder=recorder,
        snapshots=snapshots,
        subscriptions=subscriptions,
        plant=plant,
        console=console,
    )
