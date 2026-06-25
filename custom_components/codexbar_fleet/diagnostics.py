"""Diagnostics support for CodexBar Fleet."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from . import CodexBarFleetConfigEntry

_TO_REDACT = {
    "account_key",
    "label",
    "account_email",
    "identity_value",
    "aliases",
    "hostname",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: CodexBarFleetConfigEntry,
) -> dict[str, Any]:
    """Return privacy-conscious diagnostics."""
    return {
        "entry": async_redact_data(
            {
                "title": entry.title,
                "data": dict(entry.data),
                "options": dict(entry.options),
                "unique_id": entry.unique_id,
            },
            _TO_REDACT,
        ),
        "runtime": async_redact_data(entry.runtime_data.diagnostics(), _TO_REDACT),
    }
