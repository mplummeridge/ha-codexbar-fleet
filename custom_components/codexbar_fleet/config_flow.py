"""Config flow for CodexBar Fleet."""

from __future__ import annotations

from typing import Any, override

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.service_info.mqtt import MqttServiceInfo

from .const import (
    CONF_ACCOUNT_CHANGE_CONFIRMATIONS,
    CONF_CYCLE_TIMEOUT_SECONDS,
    CONF_DISCOVERY_TOPIC,
    CONF_ENABLE_VERBOSE_ENTITIES,
    CONF_EXPECTED_MACHINES,
    CONF_FLEET_ID,
    CONF_LOW_COVERAGE_THRESHOLD,
    CONF_LOW_QUOTA_THRESHOLD,
    CONF_MAX_ATTRIBUTION_GAP_SECONDS,
    CONF_MAX_PAYLOAD_BYTES,
    CONF_RETENTION_DAYS,
    CONF_STALE_AFTER_SECONDS,
    CONF_TOPIC_PREFIX,
    DEFAULT_ACCOUNT_CHANGE_CONFIRMATIONS,
    DEFAULT_CYCLE_TIMEOUT_SECONDS,
    DEFAULT_ENABLE_VERBOSE_ENTITIES,
    DEFAULT_LOW_COVERAGE_THRESHOLD,
    DEFAULT_LOW_QUOTA_THRESHOLD,
    DEFAULT_MAX_ATTRIBUTION_GAP_SECONDS,
    DEFAULT_MAX_PAYLOAD_BYTES,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_STALE_AFTER_SECONDS,
    DEFAULT_TOPIC_PREFIX,
    DOMAIN,
    NAME,
)
from .discovery import (
    FleetDiscovery,
    fleet_id_for_prefix,
    normalise_prefix,
    parse_discovery_beacon,
    valid_prefix,
)


def _entry_unique_id(fleet_id: str) -> str:
    return f"codexbar-fleet:{fleet_id}"


class CodexBarFleetConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle MQTT discovery and manual fallback configuration."""

    VERSION = 2
    MINOR_VERSION = 0

    def __init__(self) -> None:
        self._discovery: FleetDiscovery | None = None

    @staticmethod
    @callback
    @override
    def async_get_options_flow(config_entry):
        return CodexBarFleetOptionsFlow()

    @override
    async def async_step_mqtt(self, discovery_info: MqttServiceInfo) -> ConfigFlowResult:
        """Discover a fleet from the agent's retained well-known beacon."""
        try:
            discovery = parse_discovery_beacon(discovery_info.topic, discovery_info.payload)
        except (UnicodeDecodeError, ValueError, TypeError):
            return self.async_abort(reason="invalid_discovery_info")

        await self.async_set_unique_id(_entry_unique_id(discovery.fleet_id))
        # A custom abort reason keeps HA's wildcard MQTT discovery subscription
        # active so additional fleets can still be discovered.
        self._abort_if_unique_id_configured(error="fleet_already_configured")
        self._discovery = discovery
        self.context.update(
            {
                "title_placeholders": {
                    "fleet_id": discovery.fleet_id,
                    "machine": discovery.machine_name,
                    "topic_prefix": discovery.topic_prefix,
                }
            }
        )
        return await self.async_step_discovery_confirm()

    async def async_step_discovery_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm a zero-input discovered fleet."""
        if self._discovery is None:
            return self.async_abort(reason="invalid_discovery_info")
        discovery = self._discovery
        if user_input is not None:
            return self.async_create_entry(
                title=f"{NAME} · {discovery.topic_prefix}",
                data={
                    CONF_TOPIC_PREFIX: discovery.topic_prefix,
                    CONF_FLEET_ID: discovery.fleet_id,
                    CONF_DISCOVERY_TOPIC: discovery.discovery_topic,
                },
            )
        return self.async_show_form(
            step_id="discovery_confirm",
            description_placeholders={
                "machine": discovery.machine_name,
                "topic_prefix": discovery.topic_prefix,
                "fleet_id": discovery.fleet_id,
            },
        )

    @override
    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Manual fallback for legacy agents or restricted broker ACLs."""
        if not self.hass.config_entries.async_entries("mqtt"):
            return self.async_abort(reason="mqtt_required")

        errors: dict[str, str] = {}
        if user_input is not None:
            prefix = normalise_prefix(str(user_input[CONF_TOPIC_PREFIX]))
            if not valid_prefix(prefix):
                errors[CONF_TOPIC_PREFIX] = "invalid_topic_prefix"
            else:
                fleet_id = fleet_id_for_prefix(prefix)
                await self.async_set_unique_id(_entry_unique_id(fleet_id))
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"{NAME} · {prefix}",
                    data={
                        CONF_TOPIC_PREFIX: prefix,
                        CONF_FLEET_ID: fleet_id,
                    },
                    options={},
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {vol.Required(CONF_TOPIC_PREFIX, default=DEFAULT_TOPIC_PREFIX): str}
            ),
            errors=errors,
        )


class CodexBarFleetOptionsFlow(OptionsFlow):
    """Configure aggregation policy."""

    @override
    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)

        options = self.config_entry.options
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_EXPECTED_MACHINES,
                        default=options.get(CONF_EXPECTED_MACHINES, ""),
                    ): str,
                    vol.Optional(
                        CONF_STALE_AFTER_SECONDS,
                        default=options.get(CONF_STALE_AFTER_SECONDS, DEFAULT_STALE_AFTER_SECONDS),
                    ): vol.All(vol.Coerce(int), vol.Range(min=30, max=86400)),
                    vol.Optional(
                        CONF_CYCLE_TIMEOUT_SECONDS,
                        default=options.get(
                            CONF_CYCLE_TIMEOUT_SECONDS, DEFAULT_CYCLE_TIMEOUT_SECONDS
                        ),
                    ): vol.All(vol.Coerce(int), vol.Range(min=10, max=3600)),
                    vol.Optional(
                        CONF_MAX_ATTRIBUTION_GAP_SECONDS,
                        default=options.get(
                            CONF_MAX_ATTRIBUTION_GAP_SECONDS,
                            DEFAULT_MAX_ATTRIBUTION_GAP_SECONDS,
                        ),
                    ): vol.All(vol.Coerce(int), vol.Range(min=60, max=86400)),
                    vol.Optional(
                        CONF_ACCOUNT_CHANGE_CONFIRMATIONS,
                        default=options.get(
                            CONF_ACCOUNT_CHANGE_CONFIRMATIONS,
                            DEFAULT_ACCOUNT_CHANGE_CONFIRMATIONS,
                        ),
                    ): vol.All(vol.Coerce(int), vol.Range(min=1, max=10)),
                    vol.Optional(
                        CONF_RETENTION_DAYS,
                        default=options.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
                    ): vol.All(vol.Coerce(int), vol.Range(min=7, max=730)),
                    vol.Optional(
                        CONF_LOW_QUOTA_THRESHOLD,
                        default=options.get(CONF_LOW_QUOTA_THRESHOLD, DEFAULT_LOW_QUOTA_THRESHOLD),
                    ): vol.All(vol.Coerce(float), vol.Range(min=0, max=100)),
                    vol.Optional(
                        CONF_LOW_COVERAGE_THRESHOLD,
                        default=options.get(
                            CONF_LOW_COVERAGE_THRESHOLD, DEFAULT_LOW_COVERAGE_THRESHOLD
                        ),
                    ): vol.All(vol.Coerce(float), vol.Range(min=0, max=100)),
                    vol.Optional(
                        CONF_MAX_PAYLOAD_BYTES,
                        default=options.get(CONF_MAX_PAYLOAD_BYTES, DEFAULT_MAX_PAYLOAD_BYTES),
                    ): vol.All(vol.Coerce(int), vol.Range(min=65536, max=67108864)),
                    vol.Optional(
                        CONF_ENABLE_VERBOSE_ENTITIES,
                        default=options.get(
                            CONF_ENABLE_VERBOSE_ENTITIES, DEFAULT_ENABLE_VERBOSE_ENTITIES
                        ),
                    ): bool,
                }
            ),
        )
