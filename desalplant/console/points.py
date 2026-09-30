"""Point selection and consistent point snapshots."""

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from ..store.checksum import digest

SNAPSHOT_KIND = 1
MAX_POINTS = 500
SCREEN_UPSTREAM = 96.0
SCREEN_DOWNSTREAM = 82.0
OPERATING_FLOW = 45.6
PERMEATE_FLOW = 52.0

PointReader = Callable[[Any], Any]

POINT_READERS: dict[str, PointReader] = {
    "state": lambda plant: str(plant.state),
    "tick": lambda plant: plant.clock.tick,
    "feed_flowing": lambda plant: bool(plant.feed.flowing),
    "feed_scaled": lambda plant: bool(plant.feed.scaled),
    "membrane_flow": lambda plant: float(plant.membrane.flow),
    "screen_delta": lambda plant: SCREEN_UPSTREAM - SCREEN_DOWNSTREAM,
    "brine_recovery": lambda plant: float(plant.brine_stream.recover(plant.hp_pump.pressure)),
    "operating_flow": lambda plant: OPERATING_FLOW,
    "permeate_flow": lambda plant: PERMEATE_FLOW,
    "supply": lambda plant: float(plant.hp_pump.supply),
    "pump_pressure": lambda plant: float(plant.hp_pump.pressure),
    "pump_running": lambda plant: bool(plant.hp_pump.running),
    "pump_stalled": lambda plant: bool(plant.hp_pump.stalled),
    "valve_open": lambda plant: bool(plant.valve.open),
    "valve_emergency": lambda plant: bool(plant.valve.emergency),
}
AVAILABLE_POINTS = tuple(sorted(POINT_READERS))


@dataclass(frozen=True)
class PointSnapshot:
    """Point values read from one compatible state version."""

    version: int
    points: dict[str, Any]
    checksum: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "kind": SNAPSHOT_KIND,
            "checksum": self.checksum,
            "points": self.points,
        }


def normalize_points(raw: Any) -> tuple[str, ...]:
    """Validate requested point names and return their stable order."""
    if raw is None or raw == "":
        raw = []
    if isinstance(raw, str):
        raw = [item.strip() for item in raw.split(",")]
    if not isinstance(raw, (list, tuple, set)):
        raise ValueError("points must be a list")
    points = [str(item).strip() for item in raw]
    if any(not point for point in points):
        raise ValueError("point names must not be empty")
    unknown = sorted({point for point in points if point not in POINT_READERS})
    if unknown:
        raise ValueError(f"unknown points: {', '.join(unknown)}")
    if len(points) > MAX_POINTS:
        raise ValueError(f"at most {MAX_POINTS} points can be requested")
    duplicated = len(points) != len(set(points))
    if duplicated:
        raise ValueError("points must not be duplicated")
    return tuple(points)


def capture(plant: Any, version: int, selected: tuple[str, ...] = ()) -> PointSnapshot:
    """Read selected points as one snapshot; empty selection reads all points."""
    requested = tuple(selected) or AVAILABLE_POINTS
    values = _read_points(plant, requested)
    return PointSnapshot(version=version, points=values, checksum=digest(values))


def project(snapshot: Mapping[str, Any], points: tuple[str, ...]) -> dict[str, Any]:
    """Select points from an already consistent full snapshot."""
    values = snapshot["points"]
    if not points:
        return dict(values)
    return {point: values[point] for point in points if point in values}


def _read_points(plant: Any, points: tuple[str, ...]) -> dict[str, Any]:
    return {point: POINT_READERS[point](plant) for point in points}
