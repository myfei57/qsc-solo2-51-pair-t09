"""Catalog of readable measurement points.

A point is a named, typed reading of the line. The catalog is the only source
of truth for which names are valid; reads go through :func:`read_points`, which
evaluates every reader against the same plant instance so a single snapshot
observes one consistent state.
"""

from __future__ import annotations

from typing import Any, Callable

from ..store import keys

PointReader = Callable[[Any], Any]


def _running_ticks(plant: Any) -> int:
    """Ticks elapsed since the line started, zero when still idle."""
    started = plant.store.has(keys.PLANT_STARTED_AT)
    if not started:
        return 0
    started_at = int(plant.store.require(keys.PLANT_STARTED_AT))
    return plant.clock.elapsed(started_at) if started_at <= plant.clock.tick else 0


CATALOG: dict[str, PointReader] = {
    "state": lambda plant: str(plant.state),
    "tick": lambda plant: plant.clock.tick,
    "started": lambda plant: plant.store.has(keys.PLANT_STARTED_AT),
    "running_ticks": _running_ticks,
    "feed_flowing": lambda plant: plant.feed.flowing,
    "feed_scaled": lambda plant: plant.feed.scaled,
    "membrane_flow": lambda plant: plant.membrane.flow,
    "valve_open": lambda plant: plant.valve.open,
    "valve_stressed": lambda plant: plant.valve.stressed,
    "valve_emergency": lambda plant: plant.valve.emergency,
    "pump_pressure": lambda plant: plant.hp_pump.pressure,
    "pump_supply": lambda plant: plant.hp_pump.supply,
    "pump_stalled": lambda plant: plant.hp_pump.stalled,
    "pump_efficiency": lambda plant: plant.hp_pump.efficiency,
    "product_dumping": lambda plant: plant.product.dumping,
    "product_filling": lambda plant: plant.product.filling,
    "tank_level": lambda plant: plant.level.percent,
    "rack_blocked": lambda plant: plant.rack.blocked,
    "flush_started": lambda plant: plant.flusher.started,
    "flush_effective": lambda plant: plant.flusher.effective,
    "store_entries": lambda plant: plant.store.count(),
}


def available_points() -> tuple[str, ...]:
    """Sorted names every snapshot may request."""
    return tuple(sorted(CATALOG))


def read_points(plant: Any, names: tuple[str, ...]) -> dict[str, Any]:
    """Read a named selection against one plant instance.

    Callers hold the snapshot lock for the whole call, so the returned mapping
    is a consistent view at a single instant.
    """
    return {name: CATALOG[name](plant) for name in names}
