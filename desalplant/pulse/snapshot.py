"""A versioned, consistent batch of point values.

Every snapshot is stamped with a monotonically increasing ``version``, the
logical ``tick`` it was taken at and a ``checksum`` of its values, so a
consumer can verify that a batch is exactly one frozen read and re-read it by
version. The ``policy`` records whether the values were computed for this read
(``fresh``) or reused from a published snapshot (``reuse``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..store.checksum import digest


@dataclass(frozen=True)
class Snapshot:
    """One frozen batch of point values at one version."""

    version: int
    tick: int
    points: dict[str, Any]
    checksum: str
    policy: str

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly projection used by the console."""
        return {
            "version": self.version,
            "tick": self.tick,
            "points": dict(self.points),
            "checksum": self.checksum,
            "policy": self.policy,
        }

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "Snapshot":
        """Rebuild a snapshot from a persisted row."""
        return cls(
            version=int(row["version"]),
            tick=int(row["tick"]),
            points=dict(row.get("points", {})),
            checksum=str(row["checksum"]),
            policy=str(row["policy"]),
        )


def build_snapshot(
    version: int,
    tick: int,
    points: Mapping[str, Any],
    policy: str,
) -> Snapshot:
    """Freeze a mapping of point values into a verifiable snapshot."""
    frozen = dict(points)
    return Snapshot(
        version=version,
        tick=tick,
        points=frozen,
        checksum=digest(frozen),
        policy=policy,
    )
