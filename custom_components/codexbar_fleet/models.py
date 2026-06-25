"""Pure data models used by the CodexBar Fleet aggregation engine."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

PlatformName = Literal["sensor", "binary_sensor"]
DeviceKind = Literal["fleet", "machine", "account"]


@dataclass(slots=True, frozen=True)
class EngineConfig:
    """Configuration for the pure aggregation engine."""

    stale_after_seconds: int = 180
    cycle_timeout_seconds: int = 120
    max_attribution_gap_seconds: int = 900
    account_change_confirmations: int = 2
    retention_days: int = 120
    expected_machines: tuple[str, ...] = ()
    low_quota_threshold: float = 20.0
    low_coverage_threshold: float = 80.0
    enable_verbose_entities: bool = False


@dataclass(slots=True, frozen=True)
class AccountIdentity:
    """Canonical account identity derived from a CodexBar usage row."""

    provider: str
    account_key: str
    label: str
    confidence: str
    identity_kind: str
    identity_value: str
    aliases: tuple[str, ...] = ()
    machine_scoped: bool = False


@dataclass(slots=True)
class MetricSpec:
    """Home Assistant-neutral description of an entity."""

    key: str
    platform: PlatformName
    name: str
    value: Any
    device_kind: DeviceKind
    device_id: str
    device_name: str
    available: bool = True
    unit: str | None = None
    device_class: str | None = None
    state_class: str | None = None
    entity_category: str | None = None
    icon: str | None = None
    suggested_display_precision: int | None = None
    enabled_default: bool = True
    attributes: dict[str, Any] = field(default_factory=dict)
    manufacturer: str = "MMV3"
    model: str = "CodexBar Fleet"
    via_device_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serialisable representation."""
        return asdict(self)


def parse_datetime(value: Any) -> datetime | None:
    """Parse an ISO-8601 value into an aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def isoformat(value: datetime | None) -> str | None:
    """Serialise a datetime in stable UTC form."""
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_date(value: Any) -> date | None:
    """Parse a YYYY-MM-DD date."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def as_float(value: Any) -> float | None:
    """Return a finite float, or None."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def as_int(value: Any) -> int | None:
    """Return an integer, or None."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
