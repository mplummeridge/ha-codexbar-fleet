"""Sensor platform for CodexBar Fleet."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.util import dt as dt_util

from . import CodexBarFleetConfigEntry
from .entity import CodexBarFleetEntity
from .manager import CodexBarFleetManager


async def async_setup_entry(
    hass: HomeAssistant,
    entry: CodexBarFleetConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up dynamically projected CodexBar Fleet sensors."""
    manager = entry.runtime_data

    def _add(keys: list[str]) -> None:
        async_add_entities([CodexBarFleetSensor(manager, key) for key in keys])

    entry.async_on_unload(manager.async_register_platform("sensor", _add))


class CodexBarFleetSensor(CodexBarFleetEntity, SensorEntity):
    """A dynamically described CodexBar Fleet sensor."""

    def __init__(self, manager: CodexBarFleetManager, metric_key: str) -> None:
        super().__init__(manager, metric_key)
        spec = manager.metric(metric_key)
        assert spec is not None
        if spec.device_class:
            self._attr_device_class = SensorDeviceClass(spec.device_class)
        if spec.state_class:
            self._attr_state_class = SensorStateClass(spec.state_class)
        self._attr_native_unit_of_measurement = spec.unit
        self._attr_suggested_display_precision = spec.suggested_display_precision

    @property
    def native_value(self) -> Any:
        spec = self.manager.metric(self.metric_key)
        if spec is None:
            return None
        value = spec.value
        if spec.device_class == SensorDeviceClass.TIMESTAMP and isinstance(value, str):
            return dt_util.parse_datetime(value)
        return value
