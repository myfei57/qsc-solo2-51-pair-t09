"""Read-only console handlers."""

from . import probes, routes
from .http import Request, Response, json_response
from .points import normalize_points
from .subscriptions import CHANGE_TYPE, FRESH_TYPE


def health(console, request: Request) -> Response:
    """Liveness probe."""
    return json_response({"status": "ok"})


def status(console, request: Request) -> Response:
    """Current state of the line."""
    return json_response(console.plant.status())


def telemetry(console, request: Request) -> Response:
    """One telemetry sweep."""
    return json_response(console.plant.telemetry().to_dict())


def points(console, request: Request) -> Response:
    """Fresh, consistent projection of selected point values."""
    body = request.body()
    selected = normalize_points(
        body.get("points") if "points" in body else request.param("points", "")
    )
    return json_response(console.services.fresh_snapshot(console.plant, selected))


def create_subscription(console, request: Request) -> Response:
    """Create a durable fresh-snapshot or change-notification subscription."""
    body = request.body()
    subscription_type = str(body.get("type", CHANGE_TYPE))
    selected = normalize_points(body.get("points", []))
    raw_kinds = body.get("event_kinds", body.get("kinds", []))
    if raw_kinds == "":
        raw_kinds = []
    if not isinstance(raw_kinds, (list, tuple, set)):
        raise ValueError("event_kinds must be a list")
    event_kinds = tuple(str(item) for item in raw_kinds if str(item))
    backlog = body.get("backlog_limit")
    plant = console.plant if subscription_type == CHANGE_TYPE else None
    result = console.services.subscribe(
        subscription_type,
        selected,
        event_kinds,
        None if backlog is None else int(backlog),
        plant,
    )
    return json_response(result, status=201)


def list_subscriptions(console, request: Request) -> Response:
    """List durable subscriptions currently retained by the service."""
    return json_response(
        {
            "types": [FRESH_TYPE, CHANGE_TYPE],
            "subscriptions": console.services.list_subscriptions(),
        }
    )


def notifications(console, request: Request) -> Response:
    """Read an idempotent, resumable notification batch."""
    subscription_id = _subscription_id(request)
    after = request.integer("after", 0)
    limit = request.integer("limit", 100)
    wait = request.number("wait", 0.0)
    body = request.body()
    if "after" in body:
        after = int(body["after"])
    if "limit" in body:
        limit = int(body["limit"])
    if "wait" in body:
        wait = float(body["wait"])
    result = console.services.poll(subscription_id, after, limit, wait)
    return json_response(result)


def acknowledge(console, request: Request) -> Response:
    """Acknowledge a delivered notification."""
    subscription_id = _subscription_id(request)
    body = request.body()
    sequence = body.get("sequence", request.param("sequence"))
    if sequence is None:
        raise ValueError("sequence is required")
    return json_response(
        console.services.acknowledge(subscription_id, int(sequence))
    )


def delete_subscription(console, request: Request) -> Response:
    """Delete a durable subscription."""
    console.services.delete(_subscription_id(request))
    return json_response({"status": "deleted"})


def _subscription_id(request: Request) -> str:
    return request.path.split("/")[3]


def events(console, request: Request) -> Response:
    """Newest-first audit events."""
    limit = request.integer("limit", 100)
    return json_response(
        {
            "count": console.plant.event_count(),
            "events": [event.to_dict() for event in console.plant.events(limit)],
        }
    )


def alarms(console, request: Request) -> Response:
    """Current component alarms."""
    return json_response([alarm.to_dict() for alarm in console.plant.alarms()])


def diagnostics(console, request: Request) -> Response:
    """Structural diagnostics."""
    return json_response(console.plant.diagnostics())


def ramp(console, request: Request) -> Response:
    """Walk a stepped pressure ramp."""
    return json_response(probes.run_ramp_profile(console.plant))


def backwash(console, request: Request) -> Response:
    """Measure the media head loss."""
    return json_response(probes.run_backwash_check(console.plant))


def feed(console, request: Request) -> Response:
    """Ramp the feed and clean the screen."""
    return json_response(probes.run_feed_ramp(console.plant))


def recovery(console, request: Request) -> Response:
    """Position the brine valve from the recovery figure."""
    return json_response(probes.run_recovery_control(console.plant))


def flush(console, request: Request) -> Response:
    """Advance a flush program."""
    return json_response(probes.run_flush_program(console.plant))


def dosing(console, request: Request) -> Response:
    """Size a dose."""
    return json_response(probes.run_dosing_control(console.plant))


def hydraulics(console, request: Request) -> Response:
    """Report flux and vessel load."""
    return json_response(probes.run_hydraulics(console.plant))


def tank(console, request: Request) -> Response:
    """Fill the product tank and report the tank set."""
    return json_response(probes.run_tank_level(console.plant))


def report(console, request: Request) -> Response:
    """Numeric report."""
    return json_response(console.plant.report())


def audit(console, request: Request) -> Response:
    """Audit report."""
    return json_response(console.plant.audit_report())


def audit_log(console, request: Request) -> Response:
    """Infrastructure audit evidence for subscriptions, snapshots and delivery."""
    limit = request.integer("limit", 100)
    return json_response(
        {
            "count": console.plant.audit_log_count(),
            "events": [event.to_dict() for event in console.plant.audit_logs(limit)],
        }
    )


def summary(console, request: Request) -> Response:
    """Summary over the curves and stream."""
    return json_response(console.plant.summary())


def selftest(console, request: Request) -> Response:
    """Full-cycle self test."""
    return json_response(console.plant.selftest())


def version(console, request: Request) -> Response:
    """Build label of the running service."""
    return json_response({"label": console.plant.build().label()})


ROUTE_TABLE = (
    (routes.HEALTH, health),
    (routes.STATUS, status),
    (routes.TELEMETRY, telemetry),
    (routes.POINTS, points),
    (routes.SUBSCRIPTIONS, list_subscriptions),
    (routes.EVENTS, events),
    (routes.ALARMS, alarms),
    (routes.DIAGNOSTICS, diagnostics),
    (routes.OPS_RAMP, ramp),
    (routes.OPS_BACKWASH, backwash),
    (routes.OPS_FEED, feed),
    (routes.OPS_RECOVERY, recovery),
    (routes.OPS_FLUSH, flush),
    (routes.OPS_DOSING, dosing),
    (routes.OPS_HYDRAULICS, hydraulics),
    (routes.OPS_TANK, tank),
    (routes.REPORT, report),
    (routes.AUDIT, audit),
    (routes.AUDIT_LOG, audit_log),
    (routes.SUMMARY, summary),
    (routes.SELFTEST, selftest),
    (routes.VERSION, version),
)
