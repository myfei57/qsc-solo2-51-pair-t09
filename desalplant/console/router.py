"""Request routing for the control console."""

from typing import Any, Callable

from ..errors import (
    AlarmNotActiveError,
    ContractError,
    FeedAlreadyStoppedError,
    FlushNotEffectiveError,
    FlowNotReadyError,
    HandoffIncompleteError,
    InvalidCursorError,
    InvalidEfficiencyError,
    InvalidStagesError,
    MediaNotReadyError,
    MembraneCrackedError,
    PermeateRejectedError,
    PumpAlreadyStoppedError,
    ResidualAntiscalantError,
    RinseFailedError,
    StoreError,
    SubscriptionClosedError,
    SupplyInterruptedError,
    TransferFailedError,
    UnknownKeyError,
    UnknownPointError,
    UnknownSnapshotError,
    UnknownSubscriptionError,
    ValveNotOpenError,
)
from . import handlers, operations, routes
from .http import Request, Response, error_response

Handler = Callable[["Console", Request], Response]

CONFLICT = (
    FlowNotReadyError,
    MembraneCrackedError,
    FlushNotEffectiveError,
    PumpAlreadyStoppedError,
    ValveNotOpenError,
    FeedAlreadyStoppedError,
    ResidualAntiscalantError,
    PermeateRejectedError,
    SupplyInterruptedError,
    HandoffIncompleteError,
    AlarmNotActiveError,
    TransferFailedError,
    RinseFailedError,
    MediaNotReadyError,
    SubscriptionClosedError,
)
MALFORMED = (InvalidEfficiencyError, InvalidStagesError, InvalidCursorError)
MISSING = (
    UnknownKeyError,
    UnknownPointError,
    UnknownSubscriptionError,
    UnknownSnapshotError,
)

ROUTES: tuple[tuple[str, str, Handler], ...] = (
    *((method, path, handler) for path, handler in handlers.ROUTE_TABLE for method in ("GET",)),
    *handlers.PULSE_ROUTES,
    ("POST", routes.START, operations.start),
    ("POST", routes.SHUTDOWN, operations.shutdown),
    ("POST", routes.SWITCH, operations.switch),
    ("POST", routes.MAINTAIN, operations.maintain),
)


def _segments(path: str) -> tuple[str, ...]:
    """Significant path segments, without leading or trailing slashes."""
    return tuple(segment for segment in path.split("/") if segment)


def _match(template: tuple[str, ...], actual: tuple[str, ...]) -> dict[str, str] | None:
    """Bind a parameterized route template onto a path, or return ``None``."""
    if len(template) != len(actual):
        return None
    params: dict[str, str] = {}
    for expected, got in zip(template, actual):
        if expected.startswith("{") and expected.endswith("}"):
            params[expected[1:-1]] = got
        elif expected != got:
            return None
    return params


def status_for(error: ContractError) -> int:
    """Map a contract violation onto a response status."""
    if isinstance(error, CONFLICT):
        return 409
    if isinstance(error, MALFORMED):
        return 422
    if isinstance(error, MISSING):
        return 404
    if isinstance(error, StoreError):
        return 500
    return 400


class Console:
    """Routes requests onto the plant and renders JSON responses."""

    def __init__(self, plant: Any) -> None:
        self.plant = plant
        self._routes: tuple[tuple[str, tuple[str, ...], Handler], ...] = tuple(
            (method, _segments(path), handler) for method, path, handler in ROUTES
        )

    def routes(self) -> tuple[tuple[str, str], ...]:
        """Registered method and path pairs."""
        return tuple(sorted((method, "/".join(("", *segments))) for method, segments, _ in self._routes))

    def handle(self, request: Request) -> Response:
        """Dispatch a request, mapping contract violations onto statuses."""
        handler = None
        path_params: dict[str, str] = {}
        for method, segments, candidate in self._routes:
            if method != request.method:
                continue
            matched = _match(segments, _segments(request.path))
            if matched is not None:
                handler = candidate
                path_params = matched
                break
        if handler is None:
            return error_response(f"no route for {request.method} {request.path}", status=404)
        if path_params:
            request = Request(
                method=request.method,
                path=request.path,
                query=request.query,
                body_text=request.body_text,
                path_params=path_params,
            )
        try:
            return handler(self, request)
        except ContractError as error:
            return error_response(str(error), status=status_for(error))
        except (KeyError, ValueError, TypeError) as error:
            return error_response(f"malformed request: {error}", status=400)
