"""MQTT subscription and Home Assistant lifecycle manager."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .aggregator import FleetAggregator
from .const import (
    CONF_ACCOUNT_CHANGE_CONFIRMATIONS,
    CONF_CYCLE_TIMEOUT_SECONDS,
    CONF_ENABLE_VERBOSE_ENTITIES,
    CONF_EXPECTED_MACHINES,
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
    PENDING_CYCLE_TICK_SECONDS,
    STORAGE_SAVE_DELAY_SECONDS,
    STORAGE_VERSION,
)
from .models import EngineConfig, MetricSpec

_LOGGER = logging.getLogger(__name__)

type AddMetricKeysCallback = Callable[[list[str]], None]
type UpdateCallback = Callable[[], None]


class CodexBarFleetManager:
    """Own MQTT ingestion, persistence and dynamic entity projection."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        merged = {**entry.data, **entry.options}
        expected_raw = merged.get(CONF_EXPECTED_MACHINES, "")
        if isinstance(expected_raw, str):
            expected_machines = tuple(
                item.strip() for item in expected_raw.split(",") if item.strip()
            )
        elif isinstance(expected_raw, list):
            expected_machines = tuple(
                str(item).strip() for item in expected_raw if str(item).strip()
            )
        else:
            expected_machines = ()
        self.topic_prefix = str(merged.get(CONF_TOPIC_PREFIX, DEFAULT_TOPIC_PREFIX)).strip("/")
        self.max_payload_bytes = int(merged.get(CONF_MAX_PAYLOAD_BYTES, DEFAULT_MAX_PAYLOAD_BYTES))
        self.engine = FleetAggregator(
            EngineConfig(
                stale_after_seconds=int(
                    merged.get(CONF_STALE_AFTER_SECONDS, DEFAULT_STALE_AFTER_SECONDS)
                ),
                cycle_timeout_seconds=int(
                    merged.get(CONF_CYCLE_TIMEOUT_SECONDS, DEFAULT_CYCLE_TIMEOUT_SECONDS)
                ),
                max_attribution_gap_seconds=int(
                    merged.get(
                        CONF_MAX_ATTRIBUTION_GAP_SECONDS, DEFAULT_MAX_ATTRIBUTION_GAP_SECONDS
                    )
                ),
                account_change_confirmations=int(
                    merged.get(
                        CONF_ACCOUNT_CHANGE_CONFIRMATIONS, DEFAULT_ACCOUNT_CHANGE_CONFIRMATIONS
                    )
                ),
                retention_days=int(merged.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS)),
                expected_machines=expected_machines,
                low_quota_threshold=float(
                    merged.get(CONF_LOW_QUOTA_THRESHOLD, DEFAULT_LOW_QUOTA_THRESHOLD)
                ),
                low_coverage_threshold=float(
                    merged.get(CONF_LOW_COVERAGE_THRESHOLD, DEFAULT_LOW_COVERAGE_THRESHOLD)
                ),
                enable_verbose_entities=bool(
                    merged.get(CONF_ENABLE_VERBOSE_ENTITIES, DEFAULT_ENABLE_VERBOSE_ENTITIES)
                ),
            )
        )
        self._store: Store[dict[str, Any]] = Store(
            hass,
            STORAGE_VERSION,
            f"{DOMAIN}.{entry.entry_id}",
            private=True,
            atomic_writes=True,
        )
        self._metrics: dict[str, MetricSpec] = {}
        self._platform_callbacks: dict[str, AddMetricKeysCallback] = {}
        self._platform_known: dict[str, set[str]] = {"sensor": set(), "binary_sensor": set()}
        self._update_callbacks: set[UpdateCallback] = set()
        self._unsubscribers: list[CALLBACK_TYPE] = []
        self._started = False

    async def async_start(self) -> None:
        """Load state and subscribe to the Mac-agent topic tree."""
        if self._started:
            return
        stored = await self._store.async_load()
        self.engine.import_state(stored)
        self._refresh_metrics()
        topics = (
            f"{self.topic_prefix}/nodes/+/availability",
            f"{self.topic_prefix}/nodes/+/meta",
            f"{self.topic_prefix}/nodes/+/heartbeat",
            f"{self.topic_prefix}/nodes/+/snapshots/#",
            f"{self.topic_prefix}/events/+/#",
        )
        try:
            for topic in topics:
                self._unsubscribers.append(
                    await mqtt.async_subscribe(
                        self.hass,
                        topic,
                        self._async_message_received,
                        qos=1,
                    )
                )
        except Exception as err:
            for unsubscribe in self._unsubscribers:
                unsubscribe()
            self._unsubscribers.clear()
            raise ConfigEntryNotReady(
                f"Unable to subscribe to CodexBar MQTT topics: {err}"
            ) from err
        self._unsubscribers.append(
            async_track_time_interval(
                self.hass,
                self._async_tick,
                timedelta(seconds=PENDING_CYCLE_TICK_SECONDS),
            )
        )
        self._started = True

    async def async_stop(self) -> None:
        """Unsubscribe and flush persistent state."""
        for unsubscribe in self._unsubscribers:
            unsubscribe()
        self._unsubscribers.clear()
        await self._store.async_save(self.engine.export_state())
        self._started = False

    async def _async_message_received(self, message: ReceiveMessage) -> None:
        payload = message.payload
        payload_size = (
            len(payload) if isinstance(payload, bytes) else len(str(payload).encode("utf-8"))
        )
        if payload_size > self.max_payload_bytes:
            _LOGGER.warning(
                "Ignoring CodexBar MQTT payload larger than configured limit: topic=%s bytes=%d",
                message.topic,
                payload_size,
            )
            self.engine.stats["invalid_messages"] += 1
            self._changed()
            return
        try:
            changed = self.engine.ingest_topic(
                self.topic_prefix,
                message.topic,
                payload,
                retained=message.retain,
                received_at=dt_util.utcnow(),
            )
        except Exception:
            _LOGGER.exception("Failed to process CodexBar MQTT message from %s", message.topic)
            self.engine.stats["invalid_messages"] += 1
            changed = True
        if changed:
            self._changed()

    async def _async_tick(self, _now: Any) -> None:
        if self.engine.tick(dt_util.utcnow()):
            self._changed()
        else:
            # Freshness and stale-state sensors change as time advances even without MQTT.
            if self._refresh_metrics():
                self._notify_update()

    @callback
    def _changed(self) -> None:
        self._refresh_metrics()
        self._notify_update()
        self._store.async_delay_save(
            self.engine.export_state,
            STORAGE_SAVE_DELAY_SECONDS,
        )

    @callback
    def _refresh_metrics(self) -> bool:
        new_metrics = self.engine.metrics(dt_util.utcnow())
        changed = new_metrics != self._metrics
        self._metrics = new_metrics
        for platform in tuple(self._platform_callbacks):
            self._add_new_platform_entities(platform)
        return changed

    @callback
    def async_register_platform(
        self,
        platform: str,
        add_callback: AddMetricKeysCallback,
    ) -> CALLBACK_TYPE:
        """Register a dynamic entity platform and add current entities."""
        self._platform_callbacks[platform] = add_callback
        self._add_new_platform_entities(platform)

        @callback
        def _remove() -> None:
            self._platform_callbacks.pop(platform, None)

        return _remove

    @callback
    def _add_new_platform_entities(self, platform: str) -> None:
        callback_fn = self._platform_callbacks.get(platform)
        if callback_fn is None:
            return
        current = {key for key, spec in self._metrics.items() if spec.platform == platform}
        new_keys = sorted(current - self._platform_known.setdefault(platform, set()))
        if not new_keys:
            return
        self._platform_known[platform].update(new_keys)
        callback_fn(new_keys)

    @callback
    def async_add_update_listener(self, listener: UpdateCallback) -> CALLBACK_TYPE:
        self._update_callbacks.add(listener)

        @callback
        def _remove() -> None:
            self._update_callbacks.discard(listener)

        return _remove

    @callback
    def _notify_update(self) -> None:
        for listener in tuple(self._update_callbacks):
            listener()

    def metric(self, key: str) -> MetricSpec | None:
        """Return the current metric definition."""
        return self._metrics.get(key)

    @property
    def metrics(self) -> dict[str, MetricSpec]:
        """Return current metric mapping."""
        return self._metrics

    def diagnostics(self) -> dict[str, Any]:
        """Return bounded integration diagnostics."""
        return self.engine.diagnostics(dt_util.utcnow())
