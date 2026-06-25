"""Binary sensor platform for CodexBar Fleet."""

from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import CodexBarFleetConfigEntry
from .entity import CodexBarFleetEntity
from .manager import CodexBarFleetManager


async def async_setup_entry(
    hass: HomeAssistant,
    entry: CodexBarFleetConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up dynamically projected CodexBar Fleet binary sensors."""
    manager = entry.runtime_data

    def _add(keys: list[str]) -> None:
        async_add_entities([CodexBarFleetBinarySensor(manager, key) for key in keys])

    entry.async_on_unload(manager.async_register_platform("binary_sensor", _add))


class CodexBarFleetBinarySensor(CodexBarFleetEntity, BinarySensorEntity):
    """A dynamically described CodexBar Fleet binary sensor."""

    def __init__(self, manager: CodexBarFleetManager, metric_key: str) -> None:
        super().__init__(manager, metric_key)
        spec = manager.metric(metric_key)
        assert spec is not None
        if spec.device_class:
            self._attr_device_class = BinarySensorDeviceClass(spec.device_class)

    @property
    def is_on(self) -> bool | None:
        spec = self.manager.metric(self.metric_key)
        return bool(spec.value) if spec is not None and spec.value is not None else None
