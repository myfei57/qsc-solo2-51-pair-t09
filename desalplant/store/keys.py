"""Key layout of the persisted control state."""

PLANT_STATE = "plant/state"
PLANT_STARTED_AT = "plant/started_at"
PLANT_STOPPED_AT = "plant/stopped_at"
PLANT_MAINTAINED_AT = "plant/maintained_at"

AUDIT_SEQUENCE = "audit/sequence"
AUDIT_EVENT_PREFIX = "audit/event/"
AUDIT_LAST_PREFIX = "audit/last/"
AUDIT_LOG_PREFIX = "audit/log/"
AUDIT_LOG_SEQUENCE = "audit/log-sequence"
AUDIT_LOG_LAST_PREFIX = "audit/log-last/"

SUBSCRIPTION_PREFIX = "subscription/item/"
SUBSCRIPTION_SEQUENCE_PREFIX = "subscription/sequence/"
SUBSCRIPTION_CURSOR_PREFIX = "subscription/cursor/"
NOTIFICATION_PREFIX = "subscription/notification/"
POINT_SNAPSHOT_PREFIX = "point/snapshot/"
POINT_SNAPSHOT_LATEST = "point/snapshot/latest"


def audit_event_key(sequence: int) -> str:
    """Key holding the event written at ``sequence``."""
    if sequence < 1:
        raise ValueError("audit sequence starts at one")
    return f"{AUDIT_EVENT_PREFIX}{sequence:06d}"


def audit_last_key(kind: str) -> str:
    """Key holding the sequence of the latest event of ``kind``."""
    return f"{AUDIT_LAST_PREFIX}{kind}"


def audit_log_last_key(kind: str) -> str:
    """Key holding the sequence of the latest infrastructure event of ``kind``."""
    return f"{AUDIT_LOG_LAST_PREFIX}{kind}"


def audit_log_key(sequence: int) -> str:
    """Key holding an infrastructure audit row."""
    if sequence < 1:
        raise ValueError("audit log sequence starts at one")
    return f"{AUDIT_LOG_PREFIX}{sequence:012d}"


def subscription_key(subscription_id: str) -> str:
    """Key holding one durable subscription."""
    return f"{SUBSCRIPTION_PREFIX}{subscription_id}"


def subscription_sequence_key(subscription_id: str) -> str:
    """Key holding the newest notification sequence for a subscription."""
    return f"{SUBSCRIPTION_SEQUENCE_PREFIX}{subscription_id}"


def subscription_cursor_key(subscription_id: str) -> str:
    """Key holding the highest acknowledged notification sequence."""
    return f"{SUBSCRIPTION_CURSOR_PREFIX}{subscription_id}"


def notification_key(subscription_id: str, sequence: int) -> str:
    """Key holding one subscriber-scoped delivery record."""
    if sequence < 1:
        raise ValueError("notification sequence starts at one")
    return f"{NOTIFICATION_PREFIX}{subscription_id}/{sequence:012d}"


def point_snapshot_key(version: int) -> str:
    """Key holding one reusable full-point snapshot."""
    if version < 1:
        raise ValueError("stored snapshot versions start at one")
    return f"{POINT_SNAPSHOT_PREFIX}{version:012d}"
