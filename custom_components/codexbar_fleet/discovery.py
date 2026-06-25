"""Pure MQTT fleet-discovery helpers."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

from .const import DISCOVERY_ROOT, DISCOVERY_SCHEMA


@dataclass(slots=True, frozen=True)
class FleetDiscovery:
    """Validated CodexBar fleet discovery beacon."""

    fleet_id: str
    topic_prefix: str
    machine_id: str
    machine_name: str
    agent_version: str | None
    discovery_topic: str


def _topic_segment(value: str) -> str:
    """Match the Mac agent's lowercase safe topic-segment encoding."""
    segment = re.sub(r"[^a-z0-9_-]+", "-", value.strip().casefold()).strip("-")
    return segment or "default"


def normalise_prefix(value: str) -> str:
    """Return the effective topic prefix used by the Mac agent."""
    return "/".join(
        _topic_segment(segment) for segment in value.strip("/").split("/") if segment.strip()
    )


def valid_prefix(value: str) -> bool:
    """Validate a publish prefix, not an MQTT subscription filter."""
    return (
        bool(value)
        and len(value.encode("utf-8")) <= 512
        and not any(token in value for token in ("#", "+", "\x00"))
    )


def fleet_id_for_prefix(topic_prefix: str) -> str:
    """Match the agent's first-64-bit SHA-256 fleet identifier."""
    canonical = normalise_prefix(topic_prefix)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def parse_discovery_beacon(topic: str, payload: str | bytes | bytearray) -> FleetDiscovery:
    """Parse and cross-check one retained Mac-agent discovery beacon."""
    raw = bytes(payload).decode("utf-8") if isinstance(payload, (bytes, bytearray)) else payload
    document = json.loads(raw)
    if not isinstance(document, dict) or document.get("schema") != DISCOVERY_SCHEMA:
        raise ValueError("unsupported discovery schema")

    parts = topic.split("/")
    root_parts = DISCOVERY_ROOT.split("/")
    if len(parts) != len(root_parts) + 2 or parts[: len(root_parts)] != root_parts:
        raise ValueError("unexpected discovery topic")
    topic_fleet_id, topic_machine_id = parts[-2:]

    fleet = document.get("fleet")
    machine = document.get("machine")
    agent = document.get("agent")
    if not isinstance(fleet, dict) or not isinstance(machine, dict):
        raise ValueError("missing fleet or machine metadata")

    contract_major = fleet.get("contract_major")
    if contract_major != 1:
        raise ValueError("unsupported discovery contract")

    prefix = normalise_prefix(str(fleet.get("topic_prefix") or ""))
    if not valid_prefix(prefix):
        raise ValueError("invalid fleet topic prefix")
    derived_fleet_id = fleet_id_for_prefix(prefix)
    payload_fleet_id = str(fleet.get("id") or "")
    if not payload_fleet_id or payload_fleet_id != derived_fleet_id:
        raise ValueError("fleet ID does not match topic prefix")
    if topic_fleet_id != payload_fleet_id:
        raise ValueError("fleet ID does not match discovery topic")

    machine_id = str(machine.get("id") or "").strip()
    if not machine_id or machine_id != topic_machine_id:
        raise ValueError("machine ID does not match discovery topic")

    machine_name = str(machine.get("name") or machine_id).strip() or machine_id
    agent_version = (
        str(agent.get("version")).strip()
        if isinstance(agent, dict) and agent.get("version") is not None
        else None
    )
    return FleetDiscovery(
        fleet_id=payload_fleet_id,
        topic_prefix=prefix,
        machine_id=machine_id,
        machine_name=machine_name,
        agent_version=agent_version,
        discovery_topic=topic,
    )
