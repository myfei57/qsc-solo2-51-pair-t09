"""Write side of the append-only event record."""

from ..clock import LogicalClock
from ..ns.ids import new_id
from ..store import keys
from ..store.store import Store
from . import query
from .event import Event


class Recorder:
    """Appends events and keeps their running sequence number."""

    def __init__(self, store: Store, clock: LogicalClock | None = None) -> None:
        self._store = store
        self._clock = clock
        self._sequence = int(store.get(keys.AUDIT_SEQUENCE, 0) or 0)
        self._audit_sequence = int(store.get(keys.AUDIT_LOG_SEQUENCE, 0) or 0)

    @property
    def sequence(self) -> int:
        """Sequence number of the newest event."""
        return self._sequence

    def log(self, kind: str, detail: str) -> Event:
        """Append an event and return it."""
        return self._append(kind, detail)

    def _append(self, kind: str, detail: str) -> Event:
        if not kind:
            raise ValueError("event kind must not be empty")
        self._sequence += 1
        tick = self._clock.tick if self._clock is not None else 0
        event = Event(
            sequence=self._sequence,
            kind=kind,
            detail=detail,
            tick=tick,
            id=new_id(),
        )
        self._store.put(keys.AUDIT_SEQUENCE, self._sequence)
        self._store.put(keys.audit_event_key(self._sequence), event.to_dict())
        self._store.put(keys.audit_last_key(kind), self._sequence)
        return event

    def audit_log(self, kind: str, detail: str) -> Event:
        """Append infrastructure evidence without changing the control event stream."""
        if not kind:
            raise ValueError("event kind must not be empty")
        self._audit_sequence += 1
        tick = self._clock.tick if self._clock is not None else 0
        event = Event(
            sequence=self._audit_sequence,
            kind=kind,
            detail=detail,
            tick=tick,
            id=new_id(),
        )
        self._store.update(
            {
                keys.AUDIT_LOG_SEQUENCE: self._audit_sequence,
                keys.audit_log_key(self._audit_sequence): event.to_dict(),
                keys.audit_log_last_key(kind): self._audit_sequence,
            }
        )
        return event

    def audit_latest(self, kind: str) -> Event | None:
        """Newest infrastructure event of a kind, or ``None`` when absent."""
        sequence = int(self._store.get(keys.audit_log_last_key(kind), 0) or 0)
        if sequence <= 0:
            return None
        row = self._store.require(keys.audit_log_key(sequence))
        return Event.from_dict(row)

    def count(self) -> int:
        """Number of events recorded so far."""
        return self._sequence

    def audit_count(self) -> int:
        """Number of infrastructure audit events recorded."""
        return self._audit_sequence

    def audit_logs_batch(self, entries: tuple[tuple[str, str], ...]) -> tuple[Event, ...]:
        """Build infrastructure audit rows without flushing them."""
        events = []
        tick = self._clock.tick if self._clock is not None else 0
        for kind, detail in entries:
            if not kind:
                raise ValueError("event kind must not be empty")
            self._audit_sequence += 1
            events.append(
                Event(
                    sequence=self._audit_sequence,
                    kind=kind,
                    detail=detail,
                    tick=tick,
                    id=new_id(),
                )
            )
        return tuple(events)

    def audit_logs(self, limit: int = 100) -> tuple[Event, ...]:
        """Newest-first infrastructure audit events."""
        if self._audit_sequence <= 0:
            return ()
        bound = self._audit_sequence if limit <= 0 else min(limit, self._audit_sequence)
        return tuple(
            Event.from_dict(self._store.require(keys.audit_log_key(sequence)))
            for sequence in range(self._audit_sequence, self._audit_sequence - bound, -1)
        )

    def events(self, limit: int = 0) -> tuple[Event, ...]:
        """Newest-first events, capped at ``limit`` when it is positive."""
        return query.list_events(self._store, self._sequence, limit)

    def latest(self, kind: str) -> Event | None:
        """Newest event of a kind, or ``None`` when there is none."""
        sequence = query.latest_sequence(self._store, kind)
        if sequence <= 0:
            return None
        return query.event_at(self._store, sequence)
