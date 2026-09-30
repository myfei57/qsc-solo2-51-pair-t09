"""Subscription kinds and the snapshot policy each kind is fixed to.

The service supports two ways to consume point data:

* a :data:`SubscriptionType.POLL` subscription is a saved selection read on
  demand; every pull computes a brand-new snapshot (``fresh``).
* a :data:`SubscriptionType.CHANGE` subscription is notified when the line
  changes; deliveries reuse the one snapshot the plant published at that
  instant (``reuse``), so a slow fan-out never recomputes the world.

The policy is a property of the subscription type, not a client option, so the
snapshot semantics are stable and auditable.
"""

from __future__ import annotations

from enum import Enum


class SnapshotPolicy(str, Enum):
    """How a snapshot's values are sourced."""

    FRESH = "fresh"
    REUSE = "reuse"


class SubscriptionType(str, Enum):
    """The two subscription shapes the console offers."""

    POLL = "poll"
    CHANGE = "change"

    @property
    def snapshot_policy(self) -> SnapshotPolicy:
        """Snapshot policy fixed for this subscription type."""
        if self is SubscriptionType.POLL:
            return SnapshotPolicy.FRESH
        return SnapshotPolicy.REUSE
