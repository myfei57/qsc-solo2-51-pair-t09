"""On-demand point snapshots and durable change subscriptions."""

from support import call, ok, refused


def test_on_demand_points_return_only_selected_consistent_values(console):
    body = ok(console, "POST", "/api/points", {"points": ["state", "tick"]})
    assert set(body["points"]) == {"state", "tick"}
    assert body["points"]["state"] == "idle"
    assert body["source"] == "snapshot"
    assert body["kind"] == 1
    assert body["version"] == 0
    assert len(body["checksum"]) == 64


def test_on_demand_points_accept_query_csv_and_reject_unknown_point(console):
    body = ok(console, "GET", "/api/points?points=state,tick")
    assert set(body["points"]) == {"state", "tick"}
    refused(console, "POST", "/api/points", {"points": ["missing"]})


def test_change_subscription_reuses_one_snapshot_for_every_subscriber(services):
    service = services.subscriptions
    plant = services.plant
    first = service.subscribe("changes", ("state",), plant=plant)
    second = service.subscribe("changes", ("state",), plant=plant)
    assert first["notification"]["snapshot_version"] == 1
    assert second["notification"]["snapshot_version"] == 1

    plant.start()
    poll = service.poll("1", 0, 100)
    poll2 = service.poll("2", 0, 100)
    assert poll["notifications"][1]["snapshot_version"] == 2
    assert poll2["notifications"][1]["snapshot_version"] == 2


def test_subscription_poll_is_resumable_idempotent_and_filtered(services):
    service = services.subscriptions
    plant = services.plant
    subscribed = service.subscribe(
        "changes",
        ("state", "pump_pressure"),
        event_kinds=("start",),
        backlog_limit=10,
        plant=plant,
    )
    subscription_id = subscribed["subscription"]["id"]

    plant.switch_tank()
    plant.start()

    first = service.poll(subscription_id, 0, 100)
    assert [item["sequence"] for item in first["notifications"]] == [1, 2]
    assert first["notifications"][0]["reason"] == "baseline"
    assert set(first["notifications"][1]["points"]) == {"state", "pump_pressure"}

    replayed = service.poll(subscription_id, 0, 100)
    assert replayed["notifications"] == first["notifications"]
    continuation = service.poll(subscription_id, 1, 100)
    assert continuation["notifications"][0]["sequence"] == 2


def test_subscriber_overflows_but_other_subscribers_continue(services):
    service = services.subscriptions
    plant = services.plant
    slow = service.subscribe(
        "changes",
        ("state",),
        backlog_limit=1,
        plant=plant,
    )["subscription"]["id"]
    healthy = service.subscribe("changes", ("state",), plant=plant)["subscription"]["id"]

    plant.start()
    plant.shutdown()

    slow_batch = service.poll(slow, 0, 100)
    assert slow_batch["subscription"]["status"] == "overflow"
    assert slow_batch["resubscribe"] is True
    healthy_batch = service.poll(healthy, 0, 100)
    assert healthy_batch["subscription"]["status"] == "active"
    assert len(healthy_batch["notifications"]) == 3


def test_subscription_records_are_durable_and_audited(services):
    service = services.subscriptions
    service.subscribe("changes", ("state",), plant=services.plant)
    services.plant.start()
    assert services.store.keys("subscription/item/")
    assert services.store.keys("subscription/notification/")
    assert services.store.keys("point/snapshot/")
    assert services.recorder.audit_latest("subscription.created") is not None
    assert services.recorder.audit_latest("snapshot.published") is not None
    assert services.recorder.audit_latest("subscription.delivered") is not None
    audit_body = ok(services.console, "GET", "/api/audit/log")
    kinds = {event["kind"] for event in audit_body["events"]}
    assert {
        "snapshot.published",
        "subscription.created",
        "subscription.delivered",
    } <= kinds


def test_subscription_state_resumes_from_the_audit_store(tmp_path):
    from desalplant.app import build_application
    from desalplant.config import Config

    config = Config(data_dir=str(tmp_path / "data"))
    first = build_application(config)
    first.subscriptions.subscribe("changes", ("state",), plant=first.plant)
    first.plant.start()

    reopened = build_application(config)
    batch = reopened.subscriptions.poll("1", 1, 100)
    assert batch["notifications"][0]["snapshot_version"] == 2
    assert batch["notifications"][0]["points"]["state"] == "running"


def test_subscription_api_poll_ack_delete_and_resume(console):
    response = call(
        console,
        "POST",
        "/api/subscriptions",
        {"type": "changes", "points": ["state"], "backlog_limit": 10},
    )
    assert response.status == 201
    created = response.body
    subscription_id = created["subscription"]["id"]
    ok(console, "POST", "/api/start", {})
    poll = ok(
        console,
        "GET",
        f"/api/subscriptions/{subscription_id}/notifications?after=0&limit=10",
    )
    assert len(poll["notifications"]) == 2
    ack = ok(
        console,
        "POST",
        f"/api/subscriptions/{subscription_id}/ack",
        {"sequence": 2},
    )
    assert ack["status"] == "ack"
    resumed = ok(
        console,
        "GET",
        f"/api/subscriptions/{subscription_id}/notifications?after=2&limit=10",
    )
    assert resumed["notifications"] == []
    assert ok(console, "DELETE", f"/api/subscriptions/{subscription_id}")["status"] == "deleted"
    refused(
        console,
        "GET",
        f"/api/subscriptions/{subscription_id}/notifications?after=0",
    )


def test_fresh_subscription_type_does_not_send_a_baseline(services):
    result = services.subscriptions.subscribe("snapshot", ("state",))
    assert "notification" not in result
    assert result["subscription"]["type"] == "snapshot"


def test_long_poll_has_a_configured_upper_bound(services):
    subscription_id = services.subscriptions.subscribe("snapshot", ("state",))["subscription"]["id"]
    batch = services.subscriptions.poll(subscription_id, 0, 100, wait_seconds=0)
    assert batch["notifications"] == []
    assert batch["max_wait_seconds"] <= 30
