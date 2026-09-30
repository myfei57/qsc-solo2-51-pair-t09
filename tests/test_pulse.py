"""On-demand point fetch and versioned change-subscription delivery.

Covers the nine requirements:

* on-demand fetch of selected points;
* change subscriptions;
* one consistent, verifiable snapshot per batch with a version;
* slow subscribers never block the plant (bounded queues, drops reported);
* one subscriber's failure is isolated from the others;
* idempotent resume after reconnect (cursor, no duplicates, no loss);
* long pulls are clamped to a wait and batch cap;
* subscription, snapshot-version and delivery records are audited;
* the snapshot policy is fixed per subscription type.
"""

import threading
import time
from collections import deque

import pytest

from support import call, ok, refused


def _small_pulse(services, **overrides):
    """Replace the app engine with one using small, deterministic caps."""
    from desalplant.pulse import PulseEngine

    settings = dict(max_queue=4, max_wait=0.05, max_batch=2, max_points=8)
    settings.update(overrides)
    pulse = PulseEngine(
        services.plant,
        services.store,
        services.recorder,
        services.clock,
        **settings,
    )
    services.plant.pulse = pulse
    services.pulse = pulse
    return pulse


# ------------------------------------------------------------- on-demand


def test_points_catalog_is_served(console):
    body = ok(console, "GET", "/api/points")
    assert "points" in body
    assert "pump_pressure" in body["points"]
    assert "state" in body["points"]


def test_data_fetches_only_the_requested_points(console):
    body = ok(console, "GET", "/api/data?points=state,pump_pressure")
    assert set(body["points"]) == {"state", "pump_pressure"}
    assert body["policy"] == "fresh"
    assert body["version"] >= 1
    assert body["tick"] >= 0
    assert len(body["checksum"]) == 64


def test_data_accepts_a_json_list(console):
    body = ok(
        console,
        "POST",
        "/api/subscriptions",
        {"type": "change", "points": ["state", "valve_open"]},
    )
    assert body["points"] == ["state", "valve_open"]


def test_unknown_point_is_refused(console):
    response = refused(console, "GET", "/api/data?points=state,nope")
    assert response.status == 404
    assert "nope" in response.body["error"]


def test_empty_point_selection_is_refused(console):
    response = refused(console, "GET", "/api/data?points=")
    assert response.status == 400


def test_too_many_points_is_refused(services):
    from desalplant.pulse import points as pulse_points

    pulse = _small_pulse(services)
    names = ",".join(pulse_points.available_points()[: pulse.max_points + 1])
    response = refused(services.console, "GET", f"/api/data?points={names}")
    assert response.status == 400


# ------------------------------------------------------- snapshot version


def test_snapshot_checksum_verifies(console):
    from desalplant.store.checksum import digest

    body = ok(console, "GET", "/api/data?points=state,pump_pressure")
    assert body["checksum"] == digest(body["points"])
    verified = ok(console, "GET", f"/api/snapshots/{body['version']}")
    assert verified["checksum"] == body["checksum"]
    assert verified["policy"] == "fresh"


def test_snapshot_versions_are_monotonic(console):
    first = ok(console, "GET", "/api/data?points=state")
    second = ok(console, "GET", "/api/data?points=state")
    assert second["version"] > first["version"]


def test_unknown_snapshot_version_is_refused(console):
    response = refused(console, "GET", "/api/snapshots/999999")
    assert response.status == 404


def test_snapshot_is_consistent_within_one_read(console, plant):
    """Every point in a snapshot is read at the same tick."""
    ok(console, "POST", "/api/start", {})
    body = ok(console, "GET", "/api/data?points=tick,state,pump_pressure,valve_open")
    assert body["tick"] == plant.clock.tick
    assert body["points"]["state"] == "running"
    assert body["points"]["valve_open"] is True


# ------------------------------------------------------------- subscriptions


def test_subscribe_creates_a_change_subscription(console):
    body = ok(console, "POST", "/api/subscriptions", {"type": "change", "points": ["state"]})
    assert body["type"] == "change"
    assert body["status"] == "active"
    assert body["points"] == ["state"]
    assert body["cursor"] == 0
    assert body["dropped"] == 0


def test_subscribe_defaults_to_change(console):
    body = ok(console, "POST", "/api/subscriptions", {"points": "state"})
    assert body["type"] == "change"


def test_subscribe_poll_type_is_fresh(console):
    body = ok(console, "POST", "/api/subscriptions", {"type": "poll", "points": "state"})
    assert body["type"] == "poll"


def test_subscription_type_is_validated(console):
    response = refused(console, "POST", "/api/subscriptions", {"type": "nope", "points": "state"})
    assert response.status == 400


def test_subscription_unknown_point_is_refused(console):
    response = refused(console, "POST", "/api/subscriptions", {"type": "change", "points": "nope"})
    assert response.status == 404


def test_list_and_get_subscriptions(console):
    created = ok(console, "POST", "/api/subscriptions", {"type": "change", "points": "state"})
    listing = ok(console, "GET", "/api/subscriptions")
    assert any(item["id"] == created["id"] for item in listing)
    fetched = ok(console, "GET", f"/api/subscriptions/{created['id']}")
    assert fetched["id"] == created["id"]


def test_unknown_subscription_is_refused(console):
    response = refused(console, "GET", "/api/subscriptions/nope")
    assert response.status == 404


def test_cancel_subscription(console):
    created = ok(console, "POST", "/api/subscriptions", {"type": "change", "points": "state"})
    cancelled = ok(console, "POST", f"/api/subscriptions/{created['id']}/cancel")
    assert cancelled["status"] == "cancelled"
    response = refused(console, "POST", f"/api/subscriptions/{created['id']}/cancel")
    assert response.status == 409


def test_pull_cancelled_subscription_is_refused(console):
    created = ok(console, "POST", "/api/subscriptions", {"type": "change", "points": "state"})
    ok(console, "POST", f"/api/subscriptions/{created['id']}/cancel")
    response = refused(console, "GET", f"/api/subscriptions/{created['id']}/pull")
    assert response.status == 409


# ------------------------------------------------------------- change delivery


def test_change_subscription_receives_published_snapshots(services):
    pulse = services.pulse
    sub = pulse.subscribe("change", ["state", "pump_pressure"])
    services.plant.start()
    result = pulse.pull(sub.id, after=0, limit=10)
    assert len(result["snapshots"]) == 1
    snap = result["snapshots"][0]
    assert snap["policy"] == "reuse"
    assert snap["points"]["state"] == "running"
    assert result["cursor"] == snap["version"]


def test_change_subscription_new_subscriber_gets_current_state(services):
    pulse = services.pulse
    services.plant.start()
    sub = pulse.subscribe("change", ["state"])
    result = pulse.pull(sub.id, after=0, limit=10)
    assert len(result["snapshots"]) == 1
    assert result["snapshots"][0]["points"]["state"] == "running"


def test_poll_subscription_pull_is_fresh(services):
    pulse = services.pulse
    sub = pulse.subscribe("poll", ["state"])
    services.plant.start()
    result = pulse.pull(sub.id)
    assert len(result["snapshots"]) == 1
    assert result["snapshots"][0]["policy"] == "fresh"
    assert result["snapshots"][0]["points"]["state"] == "running"


def test_pull_returns_nothing_when_no_changes(services):
    pulse = services.pulse
    sub = pulse.subscribe("change", ["state"])
    result = pulse.pull(sub.id, after=0, limit=10)
    assert result["snapshots"] == []
    assert result["cursor"] == 0


# ------------------------------------------------------------- idempotent resume


def test_resume_with_cursor_skips_delivered(services):
    pulse = services.pulse
    sub = pulse.subscribe("change", ["state"])
    services.plant.start()
    first = pulse.pull(sub.id, after=0, limit=10)
    v1 = first["snapshots"][0]["version"]
    services.plant.produce()
    second = pulse.pull(sub.id, after=v1, limit=10)
    assert len(second["snapshots"]) == 1
    assert second["snapshots"][0]["version"] > v1


def test_retry_with_same_cursor_is_idempotent(services):
    """A lost response is safe to retry: same cursor returns same snapshots."""
    pulse = services.pulse
    sub = pulse.subscribe("change", ["state"])
    services.plant.start()
    services.plant.produce()
    first = pulse.pull(sub.id, after=0, limit=10)
    second = pulse.pull(sub.id, after=0, limit=10)
    assert [s["version"] for s in first["snapshots"]] == [
        s["version"] for s in second["snapshots"]
    ]


def test_invalid_cursor_is_refused(console):
    created = ok(console, "POST", "/api/subscriptions", {"type": "change", "points": "state"})
    response = refused(console, "GET", f"/api/subscriptions/{created['id']}/pull?after=-1")
    assert response.status == 422


# ------------------------------------------------------------- backpressure


def test_slow_subscriber_does_not_block_the_plant(services):
    pulse = _small_pulse(services)
    sub = pulse.subscribe("change", ["state"])
    for _ in range(10):
        services.plant.produce()
    # The plant kept running; the subscriber's queue stayed bounded.
    assert services.plant.clock.tick == 10
    assert sub.dropped > 0
    assert len(pulse._queues[sub.id]) <= pulse.max_queue


def test_dropped_snapshots_are_reported_on_pull(services):
    pulse = _small_pulse(services)
    sub = pulse.subscribe("change", ["state"])
    for _ in range(10):
        services.plant.produce()
    result = pulse.pull(sub.id, after=0, limit=100)
    assert result["subscription"]["dropped"] > 0
    # Only the retained window is delivered.
    assert len(result["snapshots"]) <= pulse.max_queue


def test_fresh_fetch_is_unaffected_by_a_slow_subscriber(services):
    pulse = _small_pulse(services)
    sub = pulse.subscribe("change", ["state"])
    services.plant.start()
    for _ in range(10):
        services.plant.produce()
    snap = pulse.fetch(["state"])
    assert snap.policy == "fresh"
    assert snap.points["state"] == "running"


# ------------------------------------------------------------- failure isolation


def test_failing_subscriber_does_not_stop_the_others(services):
    pulse = _small_pulse(services)
    good = pulse.subscribe("change", ["state"])
    bad = pulse.subscribe("change", ["state"])

    class RaisingQueue:
        def append(self, item):
            raise RuntimeError("queue gone")

        def __len__(self):
            return 0

    pulse._queues[bad.id] = RaisingQueue()
    services.plant.start()
    assert bad.failed == 1
    result = pulse.pull(good.id, after=0, limit=10)
    assert len(result["snapshots"]) == 1
    assert result["snapshots"][0]["points"]["state"] == "running"


# ------------------------------------------------------------- bounded long-poll


def test_wait_is_clamped_to_the_server_cap(services):
    pulse = _small_pulse(services)
    assert pulse._bounded_wait(999999.0) == pulse.max_wait
    assert pulse._bounded_wait(0.01) == 0.01


def test_batch_is_clamped_to_the_server_cap(services):
    pulse = _small_pulse(services)
    sub = pulse.subscribe("change", ["state"])
    for _ in range(8):
        services.plant.produce()
    result = pulse.pull(sub.id, after=0, limit=999999)
    assert len(result["snapshots"]) <= pulse.max_batch


def test_long_poll_returns_when_data_arrives(services):
    pulse = services.pulse
    sub = pulse.subscribe("change", ["state"])
    result = {}

    def wait_pull():
        result["pull"] = pulse.pull(sub.id, after=0, wait=2.0, limit=10)

    thread = threading.Thread(target=wait_pull)
    thread.start()
    time.sleep(0.2)
    pulse.publish()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert len(result["pull"]["snapshots"]) == 1


# ------------------------------------------------------------- audit


def test_subscription_and_snapshot_and_delivery_are_audited(services):
    pulse = services.pulse
    sub = pulse.subscribe("change", ["state"])
    services.plant.start()
    pulse.pull(sub.id, after=0, limit=10)
    kinds = {event.kind for event in services.plant.events(limit=0)}
    assert "subscription_created" in kinds
    assert "snapshot_issued" in kinds
    assert "snapshot_delivered" in kinds


def test_cancellation_is_audited(console):
    created = ok(console, "POST", "/api/subscriptions", {"type": "change", "points": "state"})
    ok(console, "POST", f"/api/subscriptions/{created['id']}/cancel")
    kinds = {event.kind for event in console.plant.events(limit=0)}
    assert "subscription_cancelled" in kinds


def test_lag_and_failure_are_audited(services):
    pulse = _small_pulse(services)
    sub = pulse.subscribe("change", ["state"])
    for _ in range(10):
        services.plant.produce()
    pulse.pull(sub.id, after=0, limit=100)
    kinds = {event.kind for event in services.plant.events(limit=0)}
    assert "subscription_lagged" in kinds

    good = pulse.subscribe("change", ["state"])

    class RaisingQueue:
        def append(self, item):
            raise RuntimeError("queue gone")

        def __len__(self):
            return 0

    pulse._queues[good.id] = RaisingQueue()
    pulse.publish()
    kinds = {event.kind for event in services.plant.events(limit=0)}
    assert "delivery_failed" in kinds


# ------------------------------------------------------------- policy fixed per type


def test_snapshot_policy_is_fixed_by_subscription_type(services):
    pulse = services.pulse
    change = pulse.subscribe("change", ["state"])
    poll = pulse.subscribe("poll", ["state"])
    services.plant.start()
    change_result = pulse.pull(change.id, after=0, limit=10)
    poll_result = pulse.pull(poll.id)
    assert change_result["snapshots"][0]["policy"] == "reuse"
    assert poll_result["snapshots"][0]["policy"] == "fresh"


# ------------------------------------------------------------- restart resume


def test_subscription_survives_a_restart_and_resumes(tmp_path):
    from desalplant.config import Config
    from tests.support import build

    config = Config(data_dir=str(tmp_path / "data"))
    first = build(config)
    first.plant.start()
    sub = first.pulse.subscribe("change", ["state"])
    first.plant.produce()
    first.pulse.pull(sub.id, after=0, limit=10)

    reopened = build(config)
    reloaded = reopened.pulse.subscription(sub.id)
    assert reloaded.type == "change"
    assert reloaded.status == "active"
    assert reloaded.cursor >= 1
    # The latest snapshot is still served on resume.
    result = reopened.pulse.pull(sub.id, after=0, limit=10)
    assert len(result["snapshots"]) >= 1
    assert result["snapshots"][0]["points"]["state"] == "running"
