"""Key layout of the persisted control state."""

PLANT_STATE = "plant/state"
PLANT_STARTED_AT = "plant/started_at"
PLANT_STOPPED_AT = "plant/stopped_at"
PLANT_MAINTAINED_AT = "plant/maintained_at"

AUDIT_SEQUENCE = "audit/sequence"
AUDIT_EVENT_PREFIX = "audit/event/"
AUDIT_LAST_PREFIX = "audit/last/"

PULSE_SNAPSHOT_VERSION = "pulse/snapshot_version"
PULSE_SNAPSHOT_INDEX = "pulse/snapshot_index"
PULSE_LATEST_SNAPSHOT = "pulse/latest_snapshot"
PULSE_SUBSCRIPTION_PREFIX = "pulse/subscription/"


def audit_event_key(sequence: int) -> str:
    """Key holding the event written at ``sequence``."""
    if sequence < 1:
        raise ValueError("audit sequence starts at one")
    return f"{AUDIT_EVENT_PREFIX}{sequence:06d}"


def audit_last_key(kind: str) -> str:
    """Key holding the sequence of the latest event of ``kind``."""
    return f"{AUDIT_LAST_PREFIX}{kind}"


def pulse_subscription_key(subscription_id: str) -> str:
    """Key holding the persisted record of one subscription."""
    return f"{PULSE_SUBSCRIPTION_PREFIX}{subscription_id}"
