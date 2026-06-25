"""CodexBar Fleet integration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .const import CONF_FLEET_ID, CONF_TOPIC_PREFIX, NAME, PLATFORMS
from .discovery import fleet_id_for_prefix, normalise_prefix

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

    from .manager import CodexBarFleetManager

    type CodexBarFleetConfigEntry = ConfigEntry[CodexBarFleetManager]
else:
    CodexBarFleetConfigEntry = Any


async def async_migrate_entry(hass: HomeAssistant, entry: CodexBarFleetConfigEntry) -> bool:
    """Migrate legacy manually configured fleets to prefix-derived identity."""
    if entry.version > 2:
        return False
    if entry.version == 1:
        data = dict(entry.data)
        prefix = normalise_prefix(str(data.get(CONF_TOPIC_PREFIX) or "codexbar/v1"))
        fleet_id = fleet_id_for_prefix(prefix)
        data[CONF_TOPIC_PREFIX] = prefix
        data[CONF_FLEET_ID] = fleet_id
        hass.config_entries.async_update_entry(
            entry,
            data=data,
            title=f"{NAME} · {prefix}",
            unique_id=f"codexbar-fleet:{fleet_id}",
            version=2,
            minor_version=0,
        )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: CodexBarFleetConfigEntry) -> bool:
    """Set up CodexBar Fleet from a config entry."""
    from .manager import CodexBarFleetManager

    manager = CodexBarFleetManager(hass, entry)
    await manager.async_start()
    entry.runtime_data = manager
    entry.async_on_unload(entry.add_update_listener(_async_reload_entry))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: CodexBarFleetConfigEntry) -> bool:
    """Unload a CodexBar Fleet config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.async_stop()
    return unloaded


async def _async_reload_entry(hass: HomeAssistant, entry: CodexBarFleetConfigEntry) -> None:
    """Reload after options or reconfiguration changes."""
    await hass.config_entries.async_reload(entry.entry_id)
