"""On-demand fetch and change-subscription delivery engine.

The engine decouples the running plant from external consumers:

* the plant calls :meth:`publish` after a state change. Publishing builds one
  snapshot and fans it out to every change subscriber's *bounded* queue using
  ``put_nowait`` semantics, so a slow consumer can never block the line;
* a subscriber that falls behind has its oldest pending snapshots evicted and
  the drop is recorded, so the gap is observable and the client can resync;
* a subscriber whose delivery raises is failed in isolation and audited, so
  one broken consumer never stops the others;
* pulls resume from a client cursor idempotently: re-asking with the same
  cursor returns the same retained snapshots, and the version/checksum lets a
  client deduplicate and verify;
* long pulls are clamped to a server-side wait and batch cap.

Every subscription lifecycle, snapshot version and delivery is appended to
the audit record.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any, Callable, Deque, Sequence

from ..audit.recorder import Recorder
from ..clock import LogicalClock
from ..errors import (
    InvalidCursorError,
    SubscriptionClosedError,
    UnknownPointError,
    UnknownSnapshotError,
    UnknownSubscriptionError,
)
from ..ns.ids import new_id
from ..store import keys
from ..store.store import Store
from . import points as points_module
from .policy import SnapshotPolicy, SubscriptionType
from .snapshot import Snapshot, build_snapshot
from .subscription import Subscription

DEFAULT_MAX_QUEUE = 64
DEFAULT_MAX_WAIT = 30.0  # seconds
DEFAULT_MAX_BATCH = 64
DEFAULT_MAX_POINTS = 64

AUDIT_SUBSCRIPTION_CREATED = "subscription_created"
AUDIT_SUBSCRIPTION_CANCELLED = "subscription_cancelled"
AUDIT_SNAPSHOT_ISSUED = "snapshot_issued"
AUDIT_DELIVERED = "snapshot_delivered"
AUDIT_LAGGED = "subscription_lagged"
AUDIT_DELIVERY_FAILED = "delivery_failed"

QueueFactory = Callable[[str], Deque[Snapshot]]


class PulseEngine:
    """Fans versioned snapshots out to subscribers without blocking the plant."""

    def __init__(
        self,
        plant: Any,
        store: Store,
        recorder: Recorder,
        clock: LogicalClock,
        *,
        max_queue: int = DEFAULT_MAX_QUEUE,
        max_wait: float = DEFAULT_MAX_WAIT,
        max_batch: int = DEFAULT_MAX_BATCH,
        max_points: int = DEFAULT_MAX_POINTS,
        queue_factory: QueueFactory | None = None,
    ) -> None:
        if max_queue < 1:
            raise ValueError("max_queue must be positive")
        if max_wait < 0:
            raise ValueError("max_wait must not be negative")
        if max_batch < 1:
            raise ValueError("max_batch must be positive")
        if max_points < 1:
            raise ValueError("max_points must be positive")
        self.plant = plant
        self.store = store
        self.recorder = recorder
        self.clock = clock
        self.max_queue = max_queue
        self.max_wait = max_wait
        self.max_batch = max_batch
        self.max_points = max_points
        self._queue_factory = queue_factory or (lambda _sub_id: deque(maxlen=max_queue))
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._subscriptions: dict[str, Subscription] = {}
        self._queues: dict[str, Deque[Snapshot]] = {}
        self._latest: Snapshot | None = None
        self._version = int(store.get(keys.PULSE_SNAPSHOT_VERSION, 0) or 0)
        self._load()

    # ------------------------------------------------------------- lifecycle

    def _load(self) -> None:
        """Reload persisted subscriptions and the latest snapshot."""
        for key in self.store.keys(keys.PULSE_SUBSCRIPTION_PREFIX):
            row = self.store.get(key)
            if not isinstance(row, dict):
                continue
            sub = Subscription.from_dict(row)
            self._subscriptions[sub.id] = sub
            if sub.is_change() and sub.active:
                self._queues[sub.id] = self._queue_factory(sub.id)
        latest = self.store.get(keys.PULSE_LATEST_SNAPSHOT)
        if isinstance(latest, dict):
            self._latest = Snapshot.from_dict(latest)
            for sub in self._subscriptions.values():
                if sub.is_change() and sub.active and self._latest is not None:
                    self._queues[sub.id].append(self._latest)

    def _save_subscription(self, sub: Subscription) -> None:
        self.store.put(keys.pulse_subscription_key(sub.id), sub.to_dict())

    def _save_version(self) -> None:
        self.store.put(keys.PULSE_SNAPSHOT_VERSION, self._version)

    def _index_snapshot(self, snap: Snapshot) -> None:
        index = dict(self.store.get(keys.PULSE_SNAPSHOT_INDEX, {}) or {})
        index[str(snap.version)] = {
            "checksum": snap.checksum,
            "tick": snap.tick,
            "policy": snap.policy,
            "point_count": len(snap.points),
        }
        self.store.put(keys.PULSE_SNAPSHOT_INDEX, index)
        self.store.put(keys.PULSE_LATEST_SNAPSHOT, snap.to_dict())

    # ------------------------------------------------------------- snapshots

    def _build(self, names: tuple[str, ...], policy: SnapshotPolicy) -> Snapshot:
        """Read points under the lock, stamp a version and record it."""
        points = points_module.read_points(self.plant, names)
        self._version += 1
        self._save_version()
        snap = build_snapshot(
            version=self._version,
            tick=self.clock.tick,
            points=points,
            policy=policy.value,
        )
        self._index_snapshot(snap)
        self.recorder.log(
            AUDIT_SNAPSHOT_ISSUED,
            f"v{snap.version} policy={snap.policy} points={len(snap.points)} "
            f"checksum={snap.checksum}",
        )
        return snap

    def fetch(self, names: Sequence[str]) -> Snapshot:
        """On-demand read: a fresh, consistent snapshot of the named points."""
        selected = self._validate_names(names)
        with self._lock:
            return self._build(selected, SnapshotPolicy.FRESH)

    def publish(self) -> Snapshot:
        """Fan out the current state to every change subscriber.

        Never blocks on a consumer: each delivery is a bounded, best-effort
        enqueue and a failure or overflow is recorded per subscriber.
        """
        with self._lock:
            names = self._watched_points()
            snap = self._build(names, SnapshotPolicy.REUSE)
            self._latest = snap
            for sub in list(self._subscriptions.values()):
                if sub.active and sub.is_change():
                    self._deliver(sub, snap)
            self._condition.notify_all()
            return snap

    def _watched_points(self) -> tuple[str, ...]:
        names: set[str] = set()
        for sub in self._subscriptions.values():
            if sub.active and sub.is_change():
                names.update(sub.points)
        return tuple(sorted(names))

    def _deliver(self, sub: Subscription, snap: Snapshot) -> None:
        """Enqueue one snapshot for one subscriber, isolating failures."""
        queue = self._queues.get(sub.id)
        if queue is None:
            queue = self._queue_factory(sub.id)
            self._queues[sub.id] = queue
        try:
            before = len(queue)
            queue.append(snap)
            if before >= self.max_queue:
                # The bounded queue evicted its oldest pending snapshot.
                sub.dropped += 1
                self.recorder.log(
                    AUDIT_LAGGED,
                    f"subscription {sub.id} lagged; oldest pending snapshot "
                    f"dropped (latest v{snap.version})",
                )
        except Exception as error:  # noqa: BLE001 - one subscriber must not sink fan-out
            sub.failed += 1
            self.recorder.log(
                AUDIT_DELIVERY_FAILED,
                f"subscription {sub.id} delivery failed: {error}",
            )

    # ------------------------------------------------------- subscriptions

    def subscribe(self, type_: str | SubscriptionType, names: Sequence[str]) -> Subscription:
        """Register a subscription over a point selection."""
        stype = SubscriptionType(type_) if isinstance(type_, str) else type_
        selected = self._validate_names(names)
        with self._lock:
            sub = Subscription(
                id=new_id(),
                type=stype.value,
                points=selected,
                cursor=0,
                status="active",
                created_tick=self.clock.tick,
            )
            self._subscriptions[sub.id] = sub
            if stype is SubscriptionType.CHANGE:
                self._queues[sub.id] = self._queue_factory(sub.id)
                if self._latest is not None:
                    # Catch the new subscriber up with the current state of
                    # its own points, as a reuse snapshot at this tick.
                    catch_up = self._build(selected, SnapshotPolicy.REUSE)
                    self._queues[sub.id].append(catch_up)
            self._save_subscription(sub)
            self.recorder.log(
                AUDIT_SUBSCRIPTION_CREATED,
                f"{stype.value} subscription {sub.id} over {len(selected)} points "
                f"policy={stype.snapshot_policy.value}",
            )
            return sub

    def cancel(self, sub_id: str) -> Subscription:
        """Stop a subscription and drop its pending queue."""
        with self._lock:
            sub = self._require_subscription(sub_id)
            if not sub.active:
                raise SubscriptionClosedError(f"subscription {sub_id} already cancelled")
            sub.status = "cancelled"
            self._queues.pop(sub_id, None)
            self._save_subscription(sub)
            self.recorder.log(
                AUDIT_SUBSCRIPTION_CANCELLED,
                f"subscription {sub_id} cancelled",
            )
            return sub

    def subscriptions(self) -> tuple[Subscription, ...]:
        """All registered subscriptions, stable-sorted."""
        with self._lock:
            return tuple(sorted(self._subscriptions.values(), key=lambda item: item.id))

    def subscription(self, sub_id: str) -> Subscription:
        """One subscription by id."""
        with self._lock:
            return self._require_subscription(sub_id)

    def _require_subscription(self, sub_id: str) -> Subscription:
        sub = self._subscriptions.get(sub_id)
        if sub is None:
            raise UnknownSubscriptionError(f"no subscription {sub_id}")
        return sub

    # --------------------------------------------------------------- pull

    def pull(
        self,
        sub_id: str,
        after: int | None = None,
        wait: float = 0.0,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Long-pull a subscription, resuming after a cursor idempotently.

        ``after`` is the newest snapshot version the client has processed; the
        server returns retained snapshots newer than that, at most ``limit``
        and waiting at most ``wait`` seconds. Re-issuing the same cursor
        returns the same retained snapshots, so a lost response is safe to
        retry.
        """
        bounded_wait = self._bounded_wait(wait)
        bounded_batch = self._bounded_batch(limit)
        with self._lock:
            sub = self._require_subscription(sub_id)
            if not sub.active:
                raise SubscriptionClosedError(f"subscription {sub_id} is cancelled")
            if sub.is_poll():
                return self._pull_poll(sub)
            cursor = self._effective_cursor(sub, after)
            snapshots = self._drain(sub, cursor, bounded_batch)
            if not snapshots and bounded_wait > 0:
                self._condition.wait(timeout=bounded_wait)
                snapshots = self._drain(sub, cursor, bounded_batch)
            if snapshots:
                for snap in snapshots:
                    sub.cursor = max(sub.cursor, snap.version)
                    sub.deliveries += 1
                    self._audit_delivery(sub, snap)
                self._save_subscription(sub)
            return {
                "subscription": sub.to_dict(),
                "cursor": sub.cursor,
                "snapshots": [snap.to_dict() for snap in snapshots],
            }

    def _pull_poll(self, sub: Subscription) -> dict[str, Any]:
        """A poll subscription always reads a fresh snapshot."""
        snap = self._build(sub.points, SnapshotPolicy.FRESH)
        sub.cursor = snap.version
        sub.deliveries += 1
        self._save_subscription(sub)
        self._audit_delivery(sub, snap)
        return {
            "subscription": sub.to_dict(),
            "cursor": snap.version,
            "snapshots": [snap.to_dict()],
        }

    def _effective_cursor(self, sub: Subscription, after: int | None) -> int:
        if after is None:
            return sub.cursor
        if after < 0:
            raise InvalidCursorError(f"cursor must not be negative: {after}")
        # The client owns the cursor: re-issuing the same ``after`` returns the
        # same retained window, so a lost response is safe to retry. The
        # server's own high-water mark is only reported, never used as a floor.
        return after

    def _drain(self, sub: Subscription, cursor: int, limit: int) -> list[Snapshot]:
        queue = self._queues.get(sub.id)
        if queue is None:
            return []
        ready = [snap for snap in queue if snap.version > cursor]
        ready.sort(key=lambda snap: snap.version)
        return ready[:limit]

    def _audit_delivery(self, sub: Subscription, snap: Snapshot) -> None:
        self.recorder.log(
            AUDIT_DELIVERED,
            f"subscription {sub.id} delivered v{snap.version} "
            f"checksum={snap.checksum}",
        )

    def _bounded_wait(self, requested: float) -> float:
        if requested < 0:
            raise ValueError("wait must not be negative")
        return min(requested, self.max_wait)

    def _bounded_batch(self, requested: int | None) -> int:
        if requested is None:
            return self.max_batch
        if requested < 1:
            raise ValueError("limit must be positive")
        return min(requested, self.max_batch)

    def _validate_names(self, names: Sequence[str]) -> tuple[str, ...]:
        if isinstance(names, str):
            names = [names]
        cleaned = tuple(dict.fromkeys(names))  # dedupe while preserving order
        if not cleaned:
            raise ValueError("at least one point is required")
        if len(cleaned) > self.max_points:
            raise ValueError(f"too many points requested (max {self.max_points})")
        unknown = [name for name in cleaned if name not in points_module.CATALOG]
        if unknown:
            raise UnknownPointError(f"unknown point(s): {', '.join(unknown)}")
        return cleaned

    # ------------------------------------------------------------- verify

    def verify(self, version: int) -> dict[str, Any]:
        """Look up the recorded checksum for a snapshot version."""
        index = self.store.get(keys.PULSE_SNAPSHOT_INDEX, {}) or {}
        row = index.get(str(version))
        if row is None:
            raise UnknownSnapshotError(f"no snapshot version {version}")
        return {"version": version, **row}
