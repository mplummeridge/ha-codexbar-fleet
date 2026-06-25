"""Shared dynamic entity implementation."""

from __future__ import annotations

from typing import Any

from homeassistant.core import callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity, EntityCategory

from .const import DOMAIN
from .manager import CodexBarFleetManager


class CodexBarFleetEntity(Entity):
    """Base class backed by a MetricSpec in the manager."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, manager: CodexBarFleetManager, metric_key: str) -> None:
        self.manager = manager
        self.metric_key = metric_key
        spec = manager.metric(metric_key)
        assert spec is not None
        entry_identity = manager.entry.unique_id or manager.entry.entry_id
        self._attr_unique_id = f"{entry_identity}:{metric_key}"
        self._attr_name = spec.name
        self._attr_icon = spec.icon
        self._attr_entity_registry_enabled_default = spec.enabled_default
        if spec.entity_category:
            self._attr_entity_category = EntityCategory(spec.entity_category)

    @property
    def available(self) -> bool:
        spec = self.manager.metric(self.metric_key)
        return spec is not None and spec.available

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        spec = self.manager.metric(self.metric_key)
        return spec.attributes if spec and spec.attributes else None

    @property
    def device_info(self) -> DeviceInfo | None:
        spec = self.manager.metric(self.metric_key)
        if spec is None:
            return None
        entry_identity = self.manager.entry.unique_id or self.manager.entry.entry_id
        identifiers = {(DOMAIN, f"{entry_identity}:{spec.device_id}")}
        via_device = (
            (DOMAIN, f"{entry_identity}:{spec.via_device_id}") if spec.via_device_id else None
        )
        return DeviceInfo(
            identifiers=identifiers,
            name=spec.device_name,
            manufacturer=spec.manufacturer,
            model=spec.model,
            via_device=via_device,
        )

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(self.manager.async_add_update_listener(self._async_manager_updated))

    @callback
    def _async_manager_updated(self) -> None:
        spec = self.manager.metric(self.metric_key)
        if spec is not None:
            self._attr_name = spec.name
            self._attr_icon = spec.icon
        self.async_write_ha_state()
