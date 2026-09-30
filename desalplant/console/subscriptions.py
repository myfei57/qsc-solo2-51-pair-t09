"""Durable point snapshots, subscriptions and resumable change deliveries."""

import threading
from dataclasses import dataclass
from typing import Any, Mapping, TYPE_CHECKING

from ..store import keys
from ..store.checksum import digest
from ..store.store import Store
from . import points

FRESH_TYPE = "snapshot"
CHANGE_TYPE = "changes"
SUBSCRIPTION_TYPES = frozenset({FRESH_TYPE, CHANGE_TYPE})
DEFAULT_BACKLOG_LIMIT = 256
MAX_BACKLOG_LIMIT = 1024
MAX_WAIT_SECONDS = 30.0

if TYPE_CHECKING:  # pragma: no cover - type-only import
    from .plant import Plant


class SubscriptionError(ValueError):
    """Raised when a subscriber asks for an invalid subscription state."""


@dataclass(frozen=True)
class Subscription:
    """Durable selector for one external subscriber."""

    id: str
    type: str
    points: tuple[str, ...]
    event_kinds: tuple[str, ...]
    backlog_limit: int
    status: str = "active"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "points": list(self.points),
            "event_kinds": list(self.event_kinds),
            "backlog_limit": self.backlog_limit,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "Subscription":
        return cls(
            id=str(row["id"]),
            type=str(row["type"]),
            points=tuple(str(item) for item in row.get("points", [])),
            event_kinds=tuple(str(item) for item in row.get("event_kinds", [])),
            backlog_limit=int(row.get("backlog_limit", DEFAULT_BACKLOG_LIMIT)),
            status=str(row.get("status", "active")),
        )

    def matches(self, event_kind: str) -> bool:
        """Whether an event kind belongs in this subscription."""
        return not self.event_kinds or event_kind in self.event_kinds


class SnapshotRegistry:
    """Versioned, reusable catalogue of full-point snapshots."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._lock = threading.RLock()
        row = store.get(keys.POINT_SNAPSHOT_LATEST)
        if row is None:
            self._latest: dict[str, Any] | None = None
            self._version = 0
        else:
            self._latest = dict(row)
            self._version = int(self._latest.get("version", 0))

    @property
    def version(self) -> int:
        """Version of the most recently published snapshot."""
        return self._version

    def latest(self) -> dict[str, Any] | None:
        """Detached latest snapshot, or zero-state before publication."""
        with self._lock:
            return None if self._latest is None else dict(self._latest)

    def matching_version(self, plant: "Plant") -> tuple[Any, int]:
        """Read current points and return a snapshot plus a matching stored version."""
        with plant.point_lock:
            candidate, version = self._matching_version_locked(plant)
        return candidate, version

    def _matching_version_locked(self, plant: "Plant") -> tuple[Any, int]:
        candidate = points.capture(plant, self._version + 1)
        with self._lock:
            if self._latest is not None and self._latest["checksum"] == candidate.checksum:
                return candidate, self._version
            return candidate, 0

    def capture_change_locked(self, plant: "Plant", recorder: Any = None) -> dict[str, Any] | None:
        """Capture under the caller's already-held point lock."""
        with self._lock:
            candidate = points.capture(plant, self._version + 1).to_dict()
            if self._latest is not None and self._latest["checksum"] == candidate["checksum"]:
                return None
            self._store.update(
                {
                    keys.point_snapshot_key(candidate["version"]): candidate,
                    keys.POINT_SNAPSHOT_LATEST: candidate,
                }
            )
            self._latest = candidate
            self._version = int(candidate["version"])
            if recorder is not None:
                recorder.audit_log(
                    "snapshot.published",
                    f"snapshot {candidate['version']} {candidate['checksum']}",
                )
            return dict(candidate)

    def capture_initial(
        self,
        plant: "Plant",
        lock: Any,
        recorder: Any = None,
    ) -> dict[str, Any]:
        """Return the latest snapshot, publishing version one when necessary."""
        with lock:
            with self._lock:
                if self._latest is None:
                    snapshot = points.capture(plant, 1).to_dict()
                    self._store.update(
                        {
                            keys.point_snapshot_key(1): snapshot,
                            keys.POINT_SNAPSHOT_LATEST: snapshot,
                        }
                    )
                    self._latest = snapshot
                    self._version = 1
                    if recorder is not None:
                        recorder.audit_log(
                            "snapshot.published",
                            f"snapshot {snapshot['version']} {snapshot['checksum']}",
                        )
                return dict(self._latest)


class SubscriptionService:
    """Coordinates fresh reads, durable subscriptions and fan-out delivery."""

    def __init__(
        self,
        store: Store,
        snapshots: SnapshotRegistry,
        recorder: Any = None,
        backlog_limit: int = DEFAULT_BACKLOG_LIMIT,
        max_wait_seconds: float = MAX_WAIT_SECONDS,
    ) -> None:
        self._store = store
        self._snapshots = snapshots
        self._recorder = recorder
        self._default_backlog_limit = self._valid_backlog(backlog_limit)
        self._max_wait_seconds = min(max(0.0, float(max_wait_seconds)), MAX_WAIT_SECONDS)
        self._lock = threading.RLock()
        self._conditions: dict[str, threading.Condition] = {}
        self._refresh_subscriptions()

    def fresh_snapshot(self, plant: "Plant", selected: tuple[str, ...]) -> dict[str, Any]:
        """Always calculate a fresh consistent projection for on-demand reads."""
        with plant.point_lock:
            full, version = self._snapshots.matching_version(plant)
            row = full.to_dict()
            row["version"] = version
            row["points"] = points.project(row, selected)
            row["checksum"] = digest_of(row["points"])
        row["source"] = FRESH_TYPE
        return row

    def subscribe(
        self,
        subscription_type: str,
        selected_points: tuple[str, ...],
        event_kinds: tuple[str, ...] = (),
        backlog_limit: int | None = None,
        plant: "Plant | None" = None,
    ) -> dict[str, Any]:
        """Create one durable subscription and optionally send its baseline."""
        if subscription_type not in SUBSCRIPTION_TYPES:
            raise SubscriptionError(f"unknown subscription type: {subscription_type}")
        limit = self._valid_backlog(
            self._default_backlog_limit if backlog_limit is None else backlog_limit
        )
        baseline = None
        if subscription_type == CHANGE_TYPE and plant is not None:
            baseline = self._snapshots.capture_initial(plant, plant.point_lock, self._recorder)
        with self._lock:
            existing = self._subscriptions()
            subscription_id = str(max((int(item.id) for item in existing.values()), default=0) + 1)
            subscription = Subscription(
                id=subscription_id,
                type=subscription_type,
                points=selected_points,
                event_kinds=event_kinds,
                backlog_limit=limit,
            )
            rows = {keys.subscription_key(subscription_id): subscription.to_dict()}
            if baseline is not None:
                notification = self._notification(
                    subscription,
                    baseline,
                    event_id="",
                    event_kind="snapshot",
                    reason="baseline",
                )
                rows[keys.notification_key(subscription_id, 1)] = notification
                rows[keys.subscription_sequence_key(subscription_id)] = 1
            audit_rows = self._audit_rows(
                (
                    (
                        "subscription.created",
                        f"subscription {subscription_id} created for {subscription_type}",
                    ),
                )
                if self._recorder is not None
                else ()
            )
            rows.update(audit_rows)
            self._store.update(rows)
            self._conditions[subscription_id] = threading.Condition(self._lock)
            result = {"subscription": subscription.to_dict()}
            if baseline is not None:
                result["notification"] = notification
            return result

    def list_subscriptions(self) -> list[dict[str, Any]]:
        """All durable subscriptions, ordered by subscriber identifier."""
        with self._lock:
            subscriptions = self._subscriptions()
            return [subscriptions[key].to_dict() for key in sorted(subscriptions, key=int)]

    def delete(self, subscription_id: str) -> None:
        """Remove a subscription and its retained delivery records."""
        with self._lock:
            subscription = self._require(subscription_id)
            rows = {
                keys.subscription_key(subscription_id): None,
                keys.subscription_sequence_key(subscription_id): None,
                keys.subscription_cursor_key(subscription_id): None,
            }
            for stored_key in self._store.keys(keys.NOTIFICATION_PREFIX):
                if stored_key.startswith(f"{keys.NOTIFICATION_PREFIX}{subscription_id}/"):
                    rows[stored_key] = None
            for key, value in rows.items():
                if value is None:
                    self._store.delete(key)
            self._conditions.pop(subscription_id, None)
            self._audit("subscription.deleted", f"subscription {subscription.id} deleted")

    def capture(self, plant: "Plant") -> dict[str, Any] | None:
        """Publish the next reusable snapshot while the point lock is held."""
        return self._snapshots.capture_change_locked(plant, self._recorder)

    def fanout(self, event: Any, snapshot: dict[str, Any]) -> dict[str, Any]:
        """Enqueue one already-published snapshot to matching subscribers."""
        with self._lock:
            deliveries = []
            enqueued = []
            overflow_entries = []
            for subscription in self._subscriptions().values():
                if subscription.status != "active":
                    continue
                if subscription.type != CHANGE_TYPE or not subscription.matches(event.kind):
                    continue
                sequence = self._sequence(subscription.id) + 1
                pending = sequence - self._acknowledged_sequence(subscription.id)
                if pending > subscription.backlog_limit:
                    self._invalidate(subscription)
                    overflow_entries.append(
                        (
                            "subscription.overflow",
                            f"subscription {subscription.id} exceeded {subscription.backlog_limit} records",
                        )
                    )
                    continue
                notification = self._notification(
                    subscription,
                    snapshot,
                    event_id=event.id,
                    event_kind=event.kind,
                    reason="changed",
                    sequence=sequence,
                )
                deliveries.append(notification)
                enqueued.append((subscription, notification))
            rows = {}
            audit_entries = []
            for subscription, notification in enqueued:
                rows[keys.notification_key(subscription.id, notification["sequence"])] = notification
                rows[keys.subscription_sequence_key(subscription.id)] = notification["sequence"]
                audit_entries.append(
                    (
                        "subscription.delivered",
                        " ".join(
                            (
                                f"subscription {notification['subscription_id']}",
                                f"sequence {notification['sequence']}",
                                f"snapshot {notification['snapshot_version']}",
                                f"event {notification['event_kind']}",
                            )
                        ),
                    )
                )
            if self._recorder is not None:
                rows.update(self._audit_rows(tuple(overflow_entries + audit_entries)))
            if rows:
                self._store.update(rows)
            for _, notification in enqueued:
                self._condition_for(notification["subscription_id"]).notify_all()
            return {"snapshot": snapshot, "delivered": len(deliveries), "deliveries": deliveries}

    def poll(
        self,
        subscription_id: str,
        after: int,
        limit: int = 100,
        wait_seconds: float = 0.0,
    ) -> dict[str, Any]:
        """Fetch idempotent notifications after a subscriber sequence."""
        limit = self._valid_poll_limit(limit)
        wait_seconds = min(max(0.0, float(wait_seconds)), self._max_wait_seconds)
        if wait_seconds > 0 and not wait_seconds.is_integer():
            raise ValueError("wait must be a whole number of seconds")
        with self._lock:
            subscription = self._require(subscription_id)
            condition = self._condition_for(subscription_id)
            if after < 0 or after > self._sequence(subscription_id):
                raise SubscriptionError("after is outside this subscription's sequence range")
            notifications = self._notifications(subscription_id, after, limit)
            if not notifications and wait_seconds > 0 and subscription.status == "active":
                condition.wait(wait_seconds)
                notifications = self._notifications(subscription_id, after, limit)
            result = {
                "subscription": subscription.to_dict(),
                "after": after,
                "notifications": notifications,
                "next_after": after + len(notifications),
                "has_more": self._sequence(subscription_id) > after + len(notifications),
                "max_wait_seconds": self._max_wait_seconds,
            }
            if subscription.status != "active":
                result.update({"status": subscription.status, "resubscribe": True})
            return result

    def acknowledge(self, subscription_id: str, sequence: int) -> dict[str, Any]:
        """Mark one already-enqueued record as acknowledged; replay is still safe."""
        with self._lock:
            self._require(subscription_id)
            sequence = int(sequence)
            if sequence < 1 or sequence > self._sequence(subscription_id):
                raise SubscriptionError(f"unknown notification {subscription_id}/{sequence}")
            notification = self._store.require(keys.notification_key(subscription_id, sequence))
            notification["acknowledged"] = True
            self._store.update(
                {
                    keys.notification_key(subscription_id, sequence): notification,
                    keys.subscription_cursor_key(subscription_id): sequence,
                }
            )
            return {"subscription_id": subscription_id, "sequence": sequence, "status": "ack"}

    def _audit_rows(self, entries: tuple[tuple[str, str], ...]) -> dict[str, Any]:
        """Build store rows for infrastructure evidence and reserve their sequences."""
        if self._recorder is None or not entries:
            return {}
        events = self._recorder.audit_logs_batch(entries)
        rows: dict[str, Any] = {}
        for event in events:
            rows[keys.audit_log_key(event.sequence)] = event.to_dict()
            rows[keys.audit_log_last_key(event.kind)] = event.sequence
            rows[keys.AUDIT_LOG_SEQUENCE] = event.sequence
        return rows

    def _notification(
        self,
        subscription: Subscription,
        snapshot: Mapping[str, Any],
        event_id: str,
        event_kind: str,
        reason: str,
        sequence: int = 1,
    ) -> dict[str, Any]:
        projected = points.project(snapshot, subscription.points)
        return {
            "subscription_id": subscription.id,
            "sequence": sequence,
            "event_kind": event_kind,
            "event_id": event_id,
            "reason": reason,
            "snapshot_version": int(snapshot["version"]),
            "snapshot_kind": int(snapshot["kind"]),
            "checksum": digest_of(projected),
            "points": projected,
            "acknowledged": False,
        }

    def _invalidate(self, subscription: Subscription) -> None:
        updated = Subscription(
            id=subscription.id,
            type=subscription.type,
            points=subscription.points,
            event_kinds=subscription.event_kinds,
            backlog_limit=subscription.backlog_limit,
            status="overflow",
        )
        self._store.put(keys.subscription_key(subscription.id), updated.to_dict())

    def _notifications(self, subscription_id: str, after: int, limit: int) -> list[dict[str, Any]]:
        highest = self._sequence(subscription_id)
        return [
            self._store.require(keys.notification_key(subscription_id, sequence))
            for sequence in range(after + 1, min(highest, after + limit) + 1)
        ]

    def _acknowledged_sequence(self, subscription_id: str) -> int:
        return int(
            self._store.get(keys.subscription_cursor_key(subscription_id), 0) or 0
        )

    def _subscriptions(self) -> dict[str, Subscription]:
        return {
            key[len(keys.SUBSCRIPTION_PREFIX):]: Subscription.from_dict(
                self._store.require(key)
            )
            for key in self._store.keys(keys.SUBSCRIPTION_PREFIX)
        }

    def _require(self, subscription_id: str) -> Subscription:
        row = self._store.get(keys.subscription_key(subscription_id))
        if row is None:
            raise SubscriptionError(f"unknown subscription: {subscription_id}")
        return Subscription.from_dict(row)

    def _sequence(self, subscription_id: str) -> int:
        return int(self._store.get(keys.subscription_sequence_key(subscription_id), 0) or 0)

    def _condition_for(self, subscription_id: str) -> threading.Condition:
        condition = self._conditions.get(subscription_id)
        if condition is None:
            condition = threading.Condition(self._lock)
            self._conditions[subscription_id] = condition
        return condition

    def _refresh_subscriptions(self) -> None:
        for subscription in self._subscriptions().values():
            self._conditions[subscription.id] = threading.Condition(self._lock)

    def _audit(self, kind: str, detail: str) -> Any | None:
        if self._recorder is None:
            return None
        return self._recorder.audit_log(kind, detail)

    @staticmethod
    def _valid_backlog(value: Any) -> int:
        number = int(value)
        if number < 1 or number > MAX_BACKLOG_LIMIT:
            raise SubscriptionError(f"backlog_limit must be between 1 and {MAX_BACKLOG_LIMIT}")
        return number

    @staticmethod
    def _valid_poll_limit(value: Any) -> int:
        number = int(value)
        if number < 1 or number > 100:
            raise ValueError("limit must be between 1 and 100")
        return number


def digest_of(values: Mapping[str, Any]) -> str:
    """Checksum used for a projected notification payload."""
    return digest(values)
