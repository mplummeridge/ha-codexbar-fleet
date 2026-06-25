"""Constants for the CodexBar Fleet integration."""

from __future__ import annotations

DOMAIN = "codexbar_fleet"
NAME = "CodexBar Fleet"
VERSION = "0.2.0"

PLATFORMS = ["sensor", "binary_sensor"]

CONF_TOPIC_PREFIX = "topic_prefix"
CONF_FLEET_ID = "fleet_id"
CONF_DISCOVERY_TOPIC = "discovery_topic"
CONF_STALE_AFTER_SECONDS = "stale_after_seconds"
CONF_CYCLE_TIMEOUT_SECONDS = "cycle_timeout_seconds"
CONF_MAX_ATTRIBUTION_GAP_SECONDS = "max_attribution_gap_seconds"
CONF_ACCOUNT_CHANGE_CONFIRMATIONS = "account_change_confirmations"
CONF_RETENTION_DAYS = "retention_days"
CONF_EXPECTED_MACHINES = "expected_machines"
CONF_LOW_QUOTA_THRESHOLD = "low_quota_threshold"
CONF_LOW_COVERAGE_THRESHOLD = "low_coverage_threshold"
CONF_MAX_PAYLOAD_BYTES = "max_payload_bytes"
CONF_ENABLE_VERBOSE_ENTITIES = "enable_verbose_entities"

DEFAULT_TOPIC_PREFIX = "codexbar/v1"
DEFAULT_STALE_AFTER_SECONDS = 180
DEFAULT_CYCLE_TIMEOUT_SECONDS = 120
DEFAULT_MAX_ATTRIBUTION_GAP_SECONDS = 900
DEFAULT_ACCOUNT_CHANGE_CONFIRMATIONS = 2
DEFAULT_RETENTION_DAYS = 120
DEFAULT_LOW_QUOTA_THRESHOLD = 20.0
DEFAULT_LOW_COVERAGE_THRESHOLD = 80.0
DEFAULT_MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
DEFAULT_ENABLE_VERBOSE_ENTITIES = False

DISCOVERY_ROOT = "codexbar/discovery/v1"
DISCOVERY_SCHEMA = "io.github.mplummeridge.codexbar_mqtt.discovery.v1"
OBSERVATION_SCHEMA = "io.github.mplummeridge.codexbar_mqtt.observation.v1"
NODE_META_SCHEMA = "io.github.mplummeridge.codexbar_mqtt.node_meta.v1"
HEARTBEAT_SCHEMA = "io.github.mplummeridge.codexbar_mqtt.heartbeat.v1"

STORAGE_VERSION = 1
STORAGE_SAVE_DELAY_SECONDS = 5
PENDING_CYCLE_TICK_SECONDS = 10
MAX_SEEN_EVENTS = 5000
MAX_PROCESSED_CYCLES = 2000
MAX_INTERVALS_PER_PROVIDER = 100
MAX_ACTIVE_EVIDENCE_PER_PROVIDER = 500

SIGNAL_METRICS_UPDATED = f"{DOMAIN}_metrics_updated"
