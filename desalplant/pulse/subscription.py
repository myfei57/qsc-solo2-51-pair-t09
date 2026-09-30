"""A registered consumer of point changes.

A subscription is durable: it is persisted with its high-water cursor, so a
reconnecting client can resume after the last snapshot it processed. The
``cursor`` is the newest snapshot version delivered; ``dropped`` counts
snapshots evicted under backpressure and ``failed`` counts deliveries that
raised, so a slow or broken consumer is observable without being hidden.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass
class Subscription:
    """One consumer's selection and delivery progress."""

    id: str
    type: str  # "poll" | "change"
    points: tuple[str, ...]
    cursor: int = 0
    status: str = "active"  # "active" | "cancelled"
    created_tick: int = 0
    deliveries: int = 0
    dropped: int = 0
    failed: int = 0

    def is_change(self) -> bool:
        """Whether this subscription is pushed change snapshots."""
        return self.type == "change"

    def is_poll(self) -> bool:
        """Whether this subscription reads on demand."""
        return self.type == "poll"

    @property
    def active(self) -> bool:
        """Whether the subscription still accepts deliveries."""
        return self.status == "active"

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly projection used by the console."""
        return {
            "id": self.id,
            "type": self.type,
            "points": list(self.points),
            "cursor": self.cursor,
            "status": self.status,
            "created_tick": self.created_tick,
            "deliveries": self.deliveries,
            "dropped": self.dropped,
            "failed": self.failed,
        }

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "Subscription":
        """Rebuild a subscription from a persisted row."""
        return cls(
            id=str(row["id"]),
            type=str(row["type"]),
            points=tuple(row.get("points", [])),
            cursor=int(row.get("cursor", 0)),
            status=str(row.get("status", "active")),
            created_tick=int(row.get("created_tick", 0)),
            deliveries=int(row.get("deliveries", 0)),
            dropped=int(row.get("dropped", 0)),
            failed=int(row.get("failed", 0)),
        )
