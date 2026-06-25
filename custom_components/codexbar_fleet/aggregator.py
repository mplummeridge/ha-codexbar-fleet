"""Event-sourced, confidence-aware aggregation for CodexBar Fleet.

This module deliberately has no Home Assistant imports. It can be tested as a
pure streaming projection engine and is persisted by the integration through
Home Assistant's Store helper.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict
from copy import deepcopy
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

from .const import (
    HEARTBEAT_SCHEMA,
    MAX_ACTIVE_EVIDENCE_PER_PROVIDER,
    MAX_INTERVALS_PER_PROVIDER,
    MAX_PROCESSED_CYCLES,
    MAX_SEEN_EVENTS,
    NODE_META_SCHEMA,
    OBSERVATION_SCHEMA,
)
from .identity import account_hash, derive_account_identity, identities_from_payload
from .models import (
    AccountIdentity,
    EngineConfig,
    MetricSpec,
    as_float,
    as_int,
    isoformat,
    parse_date,
    parse_datetime,
)

_SOURCE_PRIORITY = {
    "current_default_account_probe": 60,
    "cost_attribution_account_bracket": 55,
    "provider_status_enriched_usage": 50,
    "serve_usage_snapshot": 40,
    "all_visible_or_configured_accounts": 20,
    "all_registered_provider_snapshot": 10,
}

_NUMERIC_COST_FIELDS = (
    "inputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheCreationTokens",
    "totalTokens",
    "totalCost",
)

_TOKEN_FIELDS = tuple(field for field in _NUMERIC_COST_FIELDS if field != "totalCost")

_ATTRIBUTION_CATEGORIES = (
    "ambiguous",
    "unattributed",
    "historical_backfill",
    "long_gap",
)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _slug(value: Any, max_length: int = 80) -> str:
    text = str(value or "").strip().casefold()
    text = re.sub(r"[^a-z0-9_-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_-")
    return (text or "default")[:max_length]


def _sha(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(data).hexdigest()


def _deep_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _serialise_identity(identity: AccountIdentity) -> dict[str, Any]:
    return asdict(identity)


def _provider_hint_from_observation(observation: dict[str, Any]) -> str | None:
    """Recover a provider selector from successful or error observations."""
    collection = _deep_dict(observation.get("collection"))
    command = collection.get("command")
    if isinstance(command, list):
        for index, value in enumerate(command[:-1]):
            if str(value) == "--provider":
                provider = str(command[index + 1]).strip().casefold()
                if provider:
                    return provider

    scope = str(observation.get("snapshot_scope") or "").strip().casefold()
    for prefix in ("cli/active-account-probe-", "active-account-probe-"):
        if scope.startswith(prefix):
            provider = scope[len(prefix) :].strip()
            return provider or None
    if scope and "/" not in scope and scope not in {"default", "enabled", "all", "both"}:
        return scope
    return None


def _quota_semantic_value(value: dict[str, Any]) -> dict[str, Any]:
    """Return quota fields whose divergence represents a real source conflict."""
    return {
        key: value.get(key)
        for key in (
            "id",
            "title",
            "used_percent",
            "window_minutes",
            "resets_at",
            "reset_description",
            "next_regen_percent",
            "usage_known",
        )
    }


def _identity_from_dict(value: dict[str, Any] | None) -> AccountIdentity | None:
    if not value:
        return None
    try:
        return AccountIdentity(
            provider=str(value["provider"]),
            account_key=str(value["account_key"]),
            label=str(value["label"]),
            confidence=str(value["confidence"]),
            identity_kind=str(value["identity_kind"]),
            identity_value=str(value["identity_value"]),
            aliases=tuple(value.get("aliases", ())),
            machine_scoped=bool(value.get("machine_scoped", False)),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _is_strong(identity: AccountIdentity | None) -> bool:
    return (
        identity is not None
        and not identity.machine_scoped
        and identity.confidence
        in {
            "exact",
            "provider_identifier",
        }
    )


class FleetAggregator:
    """Maintain deterministic projections from CodexBar MQTT observations."""

    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()
        self.machines: dict[str, dict[str, Any]] = {}
        self.accounts: dict[str, dict[str, Any]] = {}
        self.baselines: dict[str, dict[str, Any]] = {}
        self.attribution: dict[str, dict[str, Any]] = {}
        self.pending_cycles: dict[str, dict[str, Any]] = {}
        self.seen_events: OrderedDict[str, str] = OrderedDict()
        self.processed_cycles: OrderedDict[str, str] = OrderedDict()
        self.stats: dict[str, Any] = {
            "observations": 0,
            "duplicates": 0,
            "invalid_messages": 0,
            "unsupported_schema": 0,
            "topic_machine_mismatch": 0,
            "ledger_resets": 0,
            "out_of_order_cost_cycles": 0,
            "completed_cost_cycles": 0,
            "timed_out_cost_cycles": 0,
            "last_event_at": None,
        }

    # ---------------------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------------------

    def export_state(self) -> dict[str, Any]:
        """Return state suitable for Home Assistant's JSON Store."""
        return {
            "machines": deepcopy(self.machines),
            "accounts": deepcopy(self.accounts),
            "baselines": deepcopy(self.baselines),
            "attribution": deepcopy(self.attribution),
            "seen_events": list(self.seen_events.items()),
            "processed_cycles": list(self.processed_cycles.items()),
            "stats": deepcopy(self.stats),
        }

    def import_state(self, state: dict[str, Any] | None) -> None:
        """Load persisted state, tolerating partial or older data."""
        if not isinstance(state, dict):
            return
        self.machines = (
            deepcopy(state.get("machines", {})) if isinstance(state.get("machines"), dict) else {}
        )
        self.accounts = (
            deepcopy(state.get("accounts", {})) if isinstance(state.get("accounts"), dict) else {}
        )
        self.baselines = (
            deepcopy(state.get("baselines", {})) if isinstance(state.get("baselines"), dict) else {}
        )
        self.attribution = (
            deepcopy(state.get("attribution", {}))
            if isinstance(state.get("attribution"), dict)
            else {}
        )
        self.seen_events = OrderedDict(
            (str(key), str(value))
            for key, value in state.get("seen_events", [])
            if isinstance(key, str)
        )
        self.processed_cycles = OrderedDict(
            (str(key), str(value))
            for key, value in state.get("processed_cycles", [])
            if isinstance(key, str)
        )
        if isinstance(state.get("stats"), dict):
            self.stats.update(deepcopy(state["stats"]))
        self.pending_cycles = {}
        self._trim_ordered(self.seen_events, MAX_SEEN_EVENTS)
        self._trim_ordered(self.processed_cycles, MAX_PROCESSED_CYCLES)

    # ---------------------------------------------------------------------
    # MQTT topic ingestion
    # ---------------------------------------------------------------------

    def ingest_topic(
        self,
        topic_prefix: str,
        topic: str,
        payload: str | bytes,
        *,
        retained: bool = False,
        received_at: datetime | None = None,
    ) -> bool:
        """Ingest one MQTT message from the Mac-agent topic contract."""
        received_at = (received_at or _now_utc()).astimezone(UTC)
        prefix = topic_prefix.strip("/")
        if topic == prefix:
            return False
        prefix_with_slash = f"{prefix}/"
        if not topic.startswith(prefix_with_slash):
            return False

        suffix = topic[len(prefix_with_slash) :]
        parts = suffix.split("/")
        raw = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload

        if len(parts) >= 3 and parts[0] == "nodes":
            machine_id = parts[1]
            leaf = parts[2]
            if leaf == "availability":
                return self.ingest_availability(machine_id, raw.strip(), received_at)
            if leaf in {"meta", "heartbeat"}:
                try:
                    document = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    self.stats["invalid_messages"] += 1
                    return False
                if leaf == "meta":
                    return self.ingest_meta(machine_id, document, received_at)
                return self.ingest_heartbeat(machine_id, document, received_at)
            if leaf == "snapshots":
                return self._ingest_observation_text(raw, machine_id, retained, received_at)

        if len(parts) >= 3 and parts[0] == "events":
            return self._ingest_observation_text(raw, parts[1], retained, received_at)

        return False

    def _ingest_observation_text(
        self,
        raw: str,
        topic_machine_id: str,
        retained: bool,
        received_at: datetime,
    ) -> bool:
        try:
            document = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            self.stats["invalid_messages"] += 1
            return False
        if not isinstance(document, dict):
            self.stats["invalid_messages"] += 1
            return False
        return self.ingest_observation(
            document,
            topic_machine_id=topic_machine_id,
            retained=retained,
            received_at=received_at,
        )

    def ingest_availability(self, machine_id: str, value: str, observed_at: datetime) -> bool:
        machine = self._machine(machine_id)
        new_value = value.casefold() if value else "unknown"
        changed = machine.get("availability") != new_value
        machine["availability"] = new_value
        machine["availability_observed_at"] = isoformat(observed_at)
        self._touch_machine(machine_id, observed_at)
        return changed

    def ingest_meta(self, machine_id: str, document: dict[str, Any], observed_at: datetime) -> bool:
        if document.get("schema") not in {None, NODE_META_SCHEMA}:
            self.stats["unsupported_schema"] += 1
        machine = self._machine(machine_id)
        machine["meta"] = deepcopy(document)
        machine["name"] = (
            _deep_dict(document.get("machine")).get("name") or machine.get("name") or machine_id
        )
        machine["hostname"] = _deep_dict(document.get("machine")).get("hostname")
        machine["last_meta_at"] = isoformat(observed_at)
        self._touch_machine(machine_id, observed_at)
        return True

    def ingest_heartbeat(
        self, machine_id: str, document: dict[str, Any], observed_at: datetime
    ) -> bool:
        if document.get("schema") not in {None, HEARTBEAT_SCHEMA}:
            self.stats["unsupported_schema"] += 1
        machine = self._machine(machine_id)
        candidate_at = parse_datetime(document.get("observed_at")) or observed_at
        current_at = parse_datetime(machine.get("last_heartbeat_at"))
        if current_at is not None and candidate_at < current_at:
            return False
        machine["heartbeat"] = deepcopy(document)
        machine["last_heartbeat_at"] = isoformat(candidate_at)
        self._touch_machine(machine_id, candidate_at)
        return True

    # ---------------------------------------------------------------------
    # Observation processing
    # ---------------------------------------------------------------------

    def ingest_observation(
        self,
        observation: dict[str, Any],
        *,
        topic_machine_id: str | None = None,
        retained: bool = False,
        received_at: datetime | None = None,
    ) -> bool:
        """Ingest a decoded observation envelope."""
        received_at = (received_at or _now_utc()).astimezone(UTC)
        if observation.get("schema") != OBSERVATION_SCHEMA:
            self.stats["unsupported_schema"] += 1
            return False

        event_id = observation.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            self.stats["invalid_messages"] += 1
            return False
        # Retained snapshots and non-retained events intentionally carry the
        # same observation/event ID. They are separate delivery classes: a
        # snapshot may bootstrap state, while only the event may complete a
        # correlation cycle. Never let a retained delivery suppress the event.
        seen_key = f"snapshot:{event_id}" if retained else event_id
        if seen_key in self.seen_events:
            self.stats["duplicates"] += 1
            return False

        machine_data = _deep_dict(observation.get("machine"))
        machine_id = str(machine_data.get("id") or topic_machine_id or "").strip()
        if not machine_id:
            self.stats["invalid_messages"] += 1
            return False
        if topic_machine_id and machine_id != topic_machine_id:
            self.stats["topic_machine_mismatch"] += 1
            return False

        observed_at = (
            parse_datetime(observation.get("observed_at"))
            or parse_datetime(_deep_dict(observation.get("collection")).get("finished_at"))
            or received_at
        )
        self._remember_event(seen_key, observed_at)
        self.stats["observations"] += 1
        previous_global_event = parse_datetime(self.stats.get("last_event_at"))
        if previous_global_event is None or observed_at > previous_global_event:
            self.stats["last_event_at"] = isoformat(observed_at)

        machine = self._machine(machine_id)
        previous_metadata_at = parse_datetime(machine.get("last_observation_metadata_at"))
        if previous_metadata_at is None or observed_at >= previous_metadata_at:
            machine["name"] = machine_data.get("name") or machine.get("name") or machine_id
            machine["hostname"] = machine_data.get("hostname") or machine.get("hostname")
            machine["tags"] = deepcopy(machine_data.get("tags", machine.get("tags", {})))
            machine["agent"] = deepcopy(_deep_dict(observation.get("agent")))
            machine["last_observation_metadata_at"] = isoformat(observed_at)
        previous_machine_event = parse_datetime(machine.get("last_event_at"))
        if previous_machine_event is None or observed_at > previous_machine_event:
            machine["last_event_at"] = isoformat(observed_at)
            machine["last_event_id"] = event_id
        self._touch_machine(machine_id, observed_at)

        collection = _deep_dict(observation.get("collection"))
        kind = str(observation.get("kind") or "")
        success = bool(collection.get("success", False))
        payload = observation.get("payload")

        if not success:
            error_key = str(observation.get("snapshot_scope") or kind or "unknown")
            machine.setdefault("errors", {})[error_key] = {
                "at": isoformat(observed_at),
                "error": collection.get("error"),
                "event_id": event_id,
                "semantic_scope": collection.get("semantic_scope"),
            }

            # A failed account probe is evidence that the active identity was not
            # observed. Record it as missing so a stale previous account cannot
            # bridge the failure and receive a later cost delta. Failed bracket
            # probes also complete their side of a correlation as ``missing``.
            semantic_scope = str(collection.get("semantic_scope") or "")
            if semantic_scope in {
                "current_default_account_probe",
                "cost_attribution_account_bracket",
            }:
                provider_hint = _provider_hint_from_observation(observation)
                if provider_hint:
                    self._observe_active_probe(
                        machine_id,
                        provider_hint,
                        [],
                        observed_at,
                        kind or "collection_error",
                        event_id,
                        retained,
                    )
                    correlation_id = collection.get("correlation_id")
                    phase = collection.get("phase")
                    if correlation_id and phase in {"before-cost", "after-cost"} and not retained:
                        self._record_cycle_probe(
                            machine_id,
                            str(correlation_id),
                            str(phase),
                            [],
                            provider_hint,
                            observed_at,
                            event_id,
                        )
            self._flush_ready_cycles(observed_at)
            self._prune(observed_at)
            return True

        if kind == "serve/health":
            current_health_at = parse_datetime(machine.get("last_health_at"))
            if current_health_at is None or observed_at >= current_health_at:
                machine["serve_health"] = deepcopy(payload) if isinstance(payload, dict) else {}
                machine["last_health_at"] = isoformat(observed_at)
        elif kind in {
            "serve/usage",
            "cli/active-account-probe",
            "cli/account-catalogue",
            "cli/usage-status",
        }:
            self._process_usage_observation(machine_id, observation, observed_at, retained)
        elif kind in {"serve/cost", "cli/cost-horizon"}:
            self._process_cost_observation(machine_id, observation, observed_at, retained)
        elif kind == "cli/config-validation":
            machine["config_validation"] = deepcopy(payload)
            machine["config_validation_at"] = isoformat(observed_at)
        elif kind == "agent/error":
            machine.setdefault("errors", {})[str(observation.get("snapshot_scope") or "agent")] = {
                "at": isoformat(observed_at),
                "payload": deepcopy(payload),
            }

        self._flush_ready_cycles(observed_at)
        self._prune(observed_at)
        return True

    def tick(self, now: datetime | None = None) -> bool:
        """Advance time-based projections and expire incomplete cycles."""
        now = (now or _now_utc()).astimezone(UTC)
        changed = self._flush_ready_cycles(now, include_timeouts=True)
        self._prune(now)
        return changed

    # ---------------------------------------------------------------------
    # Usage/account projections
    # ---------------------------------------------------------------------

    def _process_usage_observation(
        self,
        machine_id: str,
        observation: dict[str, Any],
        observed_at: datetime,
        retained: bool,
    ) -> None:
        payload = observation.get("payload")
        collection = _deep_dict(observation.get("collection"))
        semantic_scope = str(collection.get("semantic_scope") or "")
        kind = str(observation.get("kind") or "")
        priority = _SOURCE_PRIORITY.get(semantic_scope, 0)

        for row in _rows(payload):
            if not row.get("provider"):
                continue
            if row.get("error") and not isinstance(row.get("usage"), dict):
                self._machine(machine_id).setdefault("provider_errors", {})[
                    str(row.get("provider"))
                ] = {
                    "observed_at": isoformat(observed_at),
                    "error": deepcopy(row.get("error")),
                }
                continue
            identity = derive_account_identity(machine_id, row)
            self._update_account(machine_id, identity, row, observation, observed_at, priority)

        if semantic_scope in {"current_default_account_probe", "cost_attribution_account_bracket"}:
            provider_hint = str(observation.get("snapshot_scope") or "").casefold() or None
            grouped = identities_from_payload(machine_id, payload, provider_hint)
            providers = set(grouped)
            if provider_hint:
                providers.add(provider_hint)
            for provider in sorted(providers):
                self._observe_active_probe(
                    machine_id,
                    provider,
                    grouped.get(provider, []),
                    observed_at,
                    kind,
                    str(observation.get("event_id") or ""),
                    retained,
                )

        correlation_id = collection.get("correlation_id")
        phase = collection.get("phase")
        if correlation_id and phase in {"before-cost", "after-cost"} and not retained:
            self._record_cycle_probe(
                machine_id,
                str(correlation_id),
                str(phase),
                payload,
                str(observation.get("snapshot_scope") or ""),
                observed_at,
                str(observation.get("event_id") or ""),
            )

    def _update_account(
        self,
        machine_id: str,
        identity: AccountIdentity,
        row: dict[str, Any],
        observation: dict[str, Any],
        observed_at: datetime,
        priority: int,
    ) -> None:
        account = self.accounts.setdefault(
            identity.account_key,
            {
                "provider": identity.provider,
                "label": identity.label,
                "confidence": identity.confidence,
                "identity_kind": identity.identity_kind,
                "identity_value": identity.identity_value,
                "machine_scoped": identity.machine_scoped,
                "aliases": [],
                "machines": {},
                "quota_windows": {},
                "dashboard": None,
                "credits": None,
                "provider_cost": None,
                "status": None,
                "identity_source": None,
                "conflicts": 0,
            },
        )
        usage = _deep_dict(row.get("usage"))
        usage_updated_at = parse_datetime(usage.get("updatedAt")) or observed_at
        semantic_scope = str(_deep_dict(observation.get("collection")).get("semantic_scope") or "")
        source = str(row.get("source") or "")
        source_meta = {
            "machine_id": machine_id,
            "observed_at": isoformat(observed_at),
            "effective_at": isoformat(usage_updated_at),
            "semantic_scope": semantic_scope,
            "source": source,
            "priority": priority,
            "event_id": observation.get("event_id"),
        }

        identity_candidate = {**source_meta, "account_key": identity.account_key}
        if account.get("identity_source") is None or self._candidate_is_newer(
            account["identity_source"], identity_candidate
        ):
            account["label"] = identity.label or account.get("label")
            account["confidence"] = identity.confidence
            account["identity_kind"] = identity.identity_kind
            account["identity_value"] = identity.identity_value
            account["machine_scoped"] = identity.machine_scoped
            account["identity_source"] = identity_candidate
        account["aliases"] = list(dict.fromkeys([*account.get("aliases", []), *identity.aliases]))
        machine_seen = parse_datetime(_deep_dict(account.get("machines")).get(machine_id))
        if machine_seen is None or observed_at > machine_seen:
            account.setdefault("machines", {})[machine_id] = isoformat(observed_at)
        account["last_seen_at"] = isoformat(
            max(parse_datetime(account.get("last_seen_at")) or observed_at, observed_at)
        )

        window_candidates: list[tuple[str, str, dict[str, Any], bool]] = []
        for window_id in ("primary", "secondary", "tertiary"):
            window = usage.get(window_id)
            if isinstance(window, dict):
                window_candidates.append((window_id, window_id.title(), window, True))
        for extra in usage.get("extraRateWindows") or []:
            if not isinstance(extra, dict) or not isinstance(extra.get("window"), dict):
                continue
            window_id = str(extra.get("id") or extra.get("title") or "extra")
            window_candidates.append(
                (
                    window_id,
                    str(extra.get("title") or window_id),
                    extra["window"],
                    bool(extra.get("usageKnown", True)),
                )
            )

        dashboard = _deep_dict(row.get("openaiDashboard"))
        for key, title in (
            ("primaryLimit", "Dashboard primary"),
            ("secondaryLimit", "Dashboard secondary"),
            ("codeReviewLimit", "Code review"),
        ):
            window = dashboard.get(key)
            if isinstance(window, dict):
                window_candidates.append((f"dashboard-{key}", title, window, True))

        for window_id, title, window, usage_known in window_candidates:
            candidate = {
                "id": window_id,
                "title": title,
                "used_percent": as_float(window.get("usedPercent")),
                "window_minutes": as_int(window.get("windowMinutes")),
                "resets_at": isoformat(parse_datetime(window.get("resetsAt"))),
                "reset_description": window.get("resetDescription"),
                "next_regen_percent": as_float(window.get("nextRegenPercent")),
                "usage_known": usage_known,
                **source_meta,
            }
            self._replace_newest(account["quota_windows"], window_id, candidate)

        identity_payload = _deep_dict(usage.get("identity"))
        candidate_account_identity = {
            "account_email": identity_payload.get("accountEmail") or usage.get("accountEmail"),
            "account_organization": identity_payload.get("accountOrganization")
            or usage.get("accountOrganization"),
            "login_method": identity_payload.get("loginMethod") or usage.get("loginMethod"),
            "data_confidence": usage.get("dataConfidence"),
            "subscription_expires_at": usage.get("subscriptionExpiresAt"),
            "subscription_renews_at": usage.get("subscriptionRenewsAt"),
            **source_meta,
        }
        if account.get("identity") is None or self._candidate_is_newer(
            account["identity"], candidate_account_identity
        ):
            account["identity"] = candidate_account_identity

        if isinstance(row.get("credits"), dict):
            credits = _deep_dict(row["credits"])
            credits_time = parse_datetime(credits.get("updatedAt")) or usage_updated_at
            candidate_credits = {
                "remaining": as_float(credits.get("remaining")),
                "updated_at": isoformat(credits_time),
                "effective_at": isoformat(credits_time),
                "observed_at": isoformat(observed_at),
                "priority": priority,
                "events_count": len(credits.get("events") or []),
                "source_machine_id": machine_id,
            }
            if account.get("credits") is None or self._candidate_is_newer(
                account["credits"], candidate_credits
            ):
                account["credits"] = candidate_credits

        reset_credits = _deep_dict(usage.get("codexResetCredits"))
        if reset_credits:
            reset_time = parse_datetime(reset_credits.get("updatedAt")) or usage_updated_at
            candidate_reset = {
                "available_count": as_int(reset_credits.get("availableCount")),
                "updated_at": isoformat(reset_time),
                "effective_at": isoformat(reset_time),
                "observed_at": isoformat(observed_at),
                "priority": priority,
                "credits_count": len(reset_credits.get("credits") or []),
            }
            if account.get("reset_credits") is None or self._candidate_is_newer(
                account["reset_credits"], candidate_reset
            ):
                account["reset_credits"] = candidate_reset

        provider_cost = _deep_dict(usage.get("providerCost"))
        if provider_cost:
            cost_time = parse_datetime(provider_cost.get("updatedAt")) or usage_updated_at
            candidate_provider_cost = {
                "used": as_float(provider_cost.get("used")),
                "limit": as_float(provider_cost.get("limit")),
                "period": provider_cost.get("period"),
                "currency_code": provider_cost.get("currencyCode"),
                "resets_at": isoformat(parse_datetime(provider_cost.get("resetsAt"))),
                "updated_at": isoformat(cost_time),
                "effective_at": isoformat(cost_time),
                "observed_at": isoformat(observed_at),
                "priority": priority,
                "source_machine_id": machine_id,
            }
            if account.get("provider_cost") is None or self._candidate_is_newer(
                account["provider_cost"], candidate_provider_cost
            ):
                account["provider_cost"] = candidate_provider_cost

        if isinstance(row.get("status"), dict):
            status = _deep_dict(row["status"])
            candidate_status = {
                "indicator": status.get("indicator"),
                "description": status.get("description"),
                "updated_at": isoformat(parse_datetime(status.get("updatedAt")) or observed_at),
                "url": status.get("url"),
                "effective_at": isoformat(parse_datetime(status.get("updatedAt")) or observed_at),
                "observed_at": isoformat(observed_at),
                "priority": priority,
                "machine_id": machine_id,
            }
            current_status = account.get("status")
            if current_status is None or self._candidate_is_newer(current_status, candidate_status):
                account["status"] = candidate_status

        if dashboard:
            dashboard_updated_at = parse_datetime(dashboard.get("updatedAt")) or usage_updated_at
            breakdown = []
            for day in dashboard.get("usageBreakdown") or []:
                if not isinstance(day, dict) or not parse_date(day.get("day")):
                    continue
                services = []
                for service in day.get("services") or []:
                    if not isinstance(service, dict):
                        continue
                    services.append(
                        {
                            "service": str(service.get("service") or "Unknown"),
                            "credits_used": as_float(service.get("creditsUsed")) or 0.0,
                        }
                    )
                breakdown.append(
                    {
                        "day": day.get("day"),
                        "services": services,
                        "total_credits_used": as_float(day.get("totalCreditsUsed"))
                        or sum(item["credits_used"] for item in services),
                    }
                )
            candidate_dashboard = {
                "updated_at": isoformat(dashboard_updated_at),
                "effective_at": isoformat(dashboard_updated_at),
                "observed_at": isoformat(observed_at),
                "priority": priority,
                "machine_id": machine_id,
                "account_plan": dashboard.get("accountPlan"),
                "signed_in_email": dashboard.get("signedInEmail"),
                "credits_remaining": as_float(dashboard.get("creditsRemaining")),
                "usage_breakdown": breakdown,
                "credit_events_count": len(dashboard.get("creditEvents") or []),
                "daily_breakdown_count": len(dashboard.get("dailyBreakdown") or []),
            }
            current_dashboard = account.get("dashboard")
            if current_dashboard is None or self._candidate_is_newer(
                current_dashboard, candidate_dashboard
            ):
                account["dashboard"] = candidate_dashboard

    def _replace_newest(
        self,
        target: dict[str, Any],
        key: str,
        candidate: dict[str, Any],
    ) -> None:
        current = target.get(key)
        if current is None:
            target[key] = candidate
            return
        if self._candidate_is_newer(current, candidate):
            target[key] = candidate
            return
        if (
            current.get("effective_at") == candidate.get("effective_at")
            and current.get("priority") == candidate.get("priority")
            and _sha(_quota_semantic_value(current)) != _sha(_quota_semantic_value(candidate))
        ):
            account_key = next(
                (
                    key
                    for key, account in self.accounts.items()
                    if target is account.get("quota_windows")
                ),
                None,
            )
            if account_key:
                self.accounts[account_key]["conflicts"] = (
                    int(self.accounts[account_key].get("conflicts", 0)) + 1
                )

    @staticmethod
    def _candidate_is_newer(current: dict[str, Any], candidate: dict[str, Any]) -> bool:
        current_time = parse_datetime(current.get("effective_at")) or datetime.min.replace(
            tzinfo=UTC
        )
        candidate_time = parse_datetime(candidate.get("effective_at")) or datetime.min.replace(
            tzinfo=UTC
        )
        if candidate_time != current_time:
            return candidate_time > current_time
        current_priority = int(current.get("priority", 0))
        candidate_priority = int(candidate.get("priority", 0))
        if candidate_priority != current_priority:
            return candidate_priority > current_priority
        current_observed = parse_datetime(current.get("observed_at")) or datetime.min.replace(
            tzinfo=UTC
        )
        candidate_observed = parse_datetime(candidate.get("observed_at")) or datetime.min.replace(
            tzinfo=UTC
        )
        return candidate_observed > current_observed

    def _observe_active_probe(
        self,
        machine_id: str,
        provider: str,
        identities: list[AccountIdentity],
        observed_at: datetime,
        evidence_kind: str,
        event_id: str,
        retained: bool,
    ) -> None:
        """Record event-time account evidence and rebuild deterministic state.

        MQTT spool replay is delivery ordered, not necessarily event-time ordered.
        Rebuilding from a bounded evidence log prevents a delayed older message
        from rolling the current-account projection backwards.
        """
        machine = self._machine(machine_id)
        active = machine.setdefault("active_accounts", {})
        state = active.setdefault(
            provider,
            {
                "current": None,
                "candidate": None,
                "switch_count": 0,
                "intervals": [],
                "evidence": [],
            },
        )
        evidence_log = state.setdefault("evidence", [])

        # Seed state written by an earlier integration build so the first new
        # observation after upgrade cannot erase the previous current account.
        if not evidence_log and isinstance(state.get("current"), dict):
            current = _deep_dict(state.get("current"))
            current_key = current.get("account_key")
            if current_key:
                seed_at = (
                    current.get("since") or current.get("observed_at") or isoformat(observed_at)
                )
                evidence_log.append(
                    {
                        "event_id": f"migrated:{provider}:{seed_at}",
                        "observed_at": seed_at,
                        "status": "single",
                        "identity": {
                            "provider": provider,
                            "account_key": current_key,
                            "label": current.get("label") or current_key,
                            "confidence": current.get("confidence") or "machine_scoped",
                            "identity_kind": "migrated",
                            "identity_value": current_key,
                            "aliases": [],
                            "machine_scoped": current.get("confidence") == "machine_scoped",
                        },
                        "evidence_kind": "migrated_state",
                        "retained": True,
                    }
                )

        if event_id and any(
            isinstance(item, dict) and item.get("event_id") == event_id for item in evidence_log
        ):
            return

        if len(identities) == 1:
            status = "single"
            identity_payload: dict[str, Any] | None = _serialise_identity(identities[0])
            account_keys: list[str] = []
        elif len(identities) > 1:
            status = "ambiguous"
            identity_payload = None
            account_keys = sorted(identity.account_key for identity in identities)
        else:
            status = "missing"
            identity_payload = None
            account_keys = []

        evidence_log.append(
            {
                "event_id": event_id or f"anonymous:{provider}:{isoformat(observed_at)}",
                "observed_at": isoformat(observed_at),
                "status": status,
                "identity": identity_payload,
                "account_keys": account_keys,
                "evidence_kind": evidence_kind,
                "retained": retained,
            }
        )
        evidence_log.sort(
            key=lambda item: (
                parse_datetime(_deep_dict(item).get("observed_at"))
                or datetime.min.replace(tzinfo=UTC),
                str(_deep_dict(item).get("event_id") or ""),
            )
        )
        if len(evidence_log) > MAX_ACTIVE_EVIDENCE_PER_PROVIDER:
            del evidence_log[:-MAX_ACTIVE_EVIDENCE_PER_PROVIDER]
        self._rebuild_active_state(state)

    def _rebuild_active_state(self, state: dict[str, Any]) -> None:
        """Compile current account and intervals from event-time evidence."""
        current: dict[str, Any] | None = None
        candidate: dict[str, Any] | None = None
        intervals: list[dict[str, Any]] = []
        switch_count = 0
        last_switch_at: str | None = None
        last_switch_confirmed_at: str | None = None

        for evidence in state.get("evidence", []):
            if not isinstance(evidence, dict):
                continue
            observed_text = evidence.get("observed_at")
            if evidence.get("status") != "single":
                # An uncertain probe breaks a candidate confirmation sequence,
                # but does not assert that the last confirmed account changed.
                candidate = None
                continue
            identity = _identity_from_dict(_deep_dict(evidence.get("identity")))
            if identity is None:
                candidate = None
                continue
            point = {
                "account_key": identity.account_key,
                "label": identity.label,
                "confidence": identity.confidence,
                "observed_at": observed_text,
                "evidence_kind": evidence.get("evidence_kind"),
                "event_id": evidence.get("event_id"),
            }

            if current is None:
                current = {**point, "since": observed_text}
                candidate = None
                continue

            if current.get("account_key") == identity.account_key:
                current.update(point)
                candidate = None
                continue

            if candidate and candidate.get("account_key") == identity.account_key:
                candidate["count"] = int(candidate.get("count", 0)) + 1
                candidate["last_observed_at"] = observed_text
                candidate["last_event_id"] = evidence.get("event_id")
            else:
                candidate = {
                    **point,
                    "count": 1,
                    "first_observed_at": observed_text,
                    "last_observed_at": observed_text,
                    "last_event_id": evidence.get("event_id"),
                }

            required = max(1, self.config.account_change_confirmations)
            if int(candidate.get("count", 0)) < required:
                continue

            transition_at = candidate.get("first_observed_at") or observed_text
            intervals.append(
                {
                    "account_key": current.get("account_key"),
                    "label": current.get("label"),
                    "started_at": current.get("since"),
                    "ended_at": transition_at,
                    "switch_confirmed_at": observed_text,
                    "confidence": current.get("confidence"),
                }
            )
            switch_count += 1
            last_switch_at = transition_at
            last_switch_confirmed_at = observed_text
            current = {
                "account_key": identity.account_key,
                "label": identity.label,
                "confidence": identity.confidence,
                "observed_at": observed_text,
                "since": transition_at,
                "evidence_kind": evidence.get("evidence_kind"),
                "event_id": evidence.get("event_id"),
            }
            candidate = None

        state["current"] = current
        state["candidate"] = candidate
        state["switch_count"] = switch_count
        state["last_switch_at"] = last_switch_at
        state["last_switch_confirmed_at"] = last_switch_confirmed_at
        state["intervals"] = intervals[-MAX_INTERVALS_PER_PROVIDER:]

    # ---------------------------------------------------------------------
    # Cost projections and attribution cycles
    # ---------------------------------------------------------------------

    def _process_cost_observation(
        self,
        machine_id: str,
        observation: dict[str, Any],
        observed_at: datetime,
        retained: bool,
    ) -> None:
        payload = observation.get("payload")
        collection = _deep_dict(observation.get("collection"))
        semantic_scope = str(collection.get("semantic_scope") or "")
        for row in _rows(payload):
            self._update_machine_cost(machine_id, row, observation, observed_at)

        correlation_id = collection.get("correlation_id")
        if (
            correlation_id
            and collection.get("phase") == "cost"
            and semantic_scope == "machine_local_cost_snapshot"
            and not retained
        ):
            cycle = self._cycle(str(correlation_id), machine_id, observed_at)
            cycle["cost"] = deepcopy(observation)
            cycle["last_seen_at"] = isoformat(observed_at)

    def _update_machine_cost(
        self,
        machine_id: str,
        row: dict[str, Any],
        observation: dict[str, Any],
        observed_at: datetime,
    ) -> None:
        provider = str(row.get("provider") or "unknown").casefold()
        history_days = as_int(row.get("historyDays")) or 30
        machine = self._machine(machine_id)
        costs = machine.setdefault("costs", {}).setdefault(provider, {})
        key = str(history_days)
        current = costs.get(key)
        if current and (parse_datetime(current.get("observed_at")) or observed_at) > observed_at:
            return

        daily = []
        model_totals: dict[str, dict[str, float]] = {}
        for item in row.get("daily") or []:
            if not isinstance(item, dict) or not parse_date(item.get("date")):
                continue
            daily.append(
                {
                    "date": item.get("date"),
                    "input_tokens": as_int(item.get("inputTokens")),
                    "output_tokens": as_int(item.get("outputTokens")),
                    "cache_read_tokens": as_int(item.get("cacheReadTokens")),
                    "cache_creation_tokens": as_int(item.get("cacheCreationTokens")),
                    "total_tokens": as_int(item.get("totalTokens")),
                    "total_cost": as_float(item.get("totalCost")),
                }
            )
            for model in item.get("modelBreakdowns") or []:
                if not isinstance(model, dict):
                    continue
                name = str(model.get("modelName") or "unknown")
                target = model_totals.setdefault(name, {"total_tokens": 0.0, "total_cost": 0.0})
                target["total_tokens"] += as_float(model.get("totalTokens")) or 0.0
                target["total_cost"] += as_float(model.get("cost")) or 0.0

        totals = _deep_dict(row.get("totals"))
        costs[key] = {
            "provider": provider,
            "history_days": history_days,
            "observed_at": isoformat(observed_at),
            "updated_at": isoformat(parse_datetime(row.get("updatedAt")) or observed_at),
            "currency_code": row.get("currencyCode") or "USD",
            "session_tokens": as_int(row.get("sessionTokens")),
            "session_cost": as_float(row.get("sessionCostUSD")),
            "history_tokens": as_int(row.get("last30DaysTokens")),
            "history_cost": as_float(row.get("last30DaysCostUSD")),
            "total_input_tokens": as_int(totals.get("inputTokens")),
            "total_output_tokens": as_int(totals.get("outputTokens")),
            "cache_read_tokens": as_int(totals.get("cacheReadTokens")),
            "cache_creation_tokens": as_int(totals.get("cacheCreationTokens")),
            "total_tokens": as_int(totals.get("totalTokens")),
            "total_cost": as_float(totals.get("totalCost")),
            "daily": daily,
            "models": model_totals,
            "event_id": observation.get("event_id"),
        }

    def _record_cycle_probe(
        self,
        machine_id: str,
        correlation_id: str,
        phase: str,
        payload: Any,
        provider_hint: str,
        observed_at: datetime,
        event_id: str,
    ) -> None:
        if correlation_id in self.processed_cycles:
            return
        cycle = self._cycle(correlation_id, machine_id, observed_at)
        side = "before" if phase == "before-cost" else "after"
        grouped = identities_from_payload(machine_id, payload, provider_hint or None)
        if provider_hint and provider_hint.casefold() not in grouped:
            grouped.setdefault(provider_hint.casefold(), [])
        for provider, identities in grouped.items():
            if len(identities) == 1:
                result = {
                    "status": "single",
                    "identity": _serialise_identity(identities[0]),
                    "observed_at": isoformat(observed_at),
                    "event_id": event_id,
                }
            elif len(identities) > 1:
                result = {
                    "status": "ambiguous",
                    "account_keys": [identity.account_key for identity in identities],
                    "observed_at": isoformat(observed_at),
                    "event_id": event_id,
                }
            else:
                result = {
                    "status": "missing",
                    "observed_at": isoformat(observed_at),
                    "event_id": event_id,
                }
            cycle[side][provider] = result
        cycle["last_seen_at"] = isoformat(observed_at)

    def _cycle(self, correlation_id: str, machine_id: str, observed_at: datetime) -> dict[str, Any]:
        cycle = self.pending_cycles.setdefault(
            correlation_id,
            {
                "machine_id": machine_id,
                "first_seen_at": isoformat(observed_at),
                "last_seen_at": isoformat(observed_at),
                "before": {},
                "after": {},
                "cost": None,
            },
        )
        if cycle.get("machine_id") != machine_id:
            cycle["invalid"] = True
        return cycle

    def _flush_ready_cycles(
        self,
        now: datetime,
        *,
        include_timeouts: bool = False,
    ) -> bool:
        changed = False
        ready: list[tuple[datetime, str, bool]] = []
        for correlation_id, cycle in self.pending_cycles.items():
            cost = cycle.get("cost")
            cost_rows = _rows(_deep_dict(cost).get("payload")) if isinstance(cost, dict) else []
            providers = {str(row.get("provider") or "").casefold() for row in cost_rows}
            providers.discard("")
            complete = bool(cost) and all(
                provider in cycle.get("before", {}) and provider in cycle.get("after", {})
                for provider in providers
            )
            first_seen = parse_datetime(cycle.get("first_seen_at")) or now
            timed_out = include_timeouts and now - first_seen >= timedelta(
                seconds=max(1, self.config.cycle_timeout_seconds)
            )
            if complete or timed_out:
                cost_time = (
                    parse_datetime(
                        _deep_dict(_deep_dict(cost).get("collection")).get("finished_at")
                    )
                    if isinstance(cost, dict)
                    else None
                ) or first_seen
                ready.append((cost_time, correlation_id, timed_out and not complete))

        for _, correlation_id, timed_out in sorted(ready):
            cycle = self.pending_cycles.pop(correlation_id, None)
            if cycle is None:
                continue
            if correlation_id in self.processed_cycles:
                continue
            self._process_cycle(correlation_id, cycle, timed_out)
            self.processed_cycles[correlation_id] = isoformat(now) or ""
            self._trim_ordered(self.processed_cycles, MAX_PROCESSED_CYCLES)
            changed = True
        return changed

    def _process_cycle(
        self,
        correlation_id: str,
        cycle: dict[str, Any],
        timed_out: bool,
    ) -> None:
        self.stats["completed_cost_cycles"] += 1
        if timed_out:
            self.stats["timed_out_cost_cycles"] += 1
        cost_observation = cycle.get("cost")
        if not isinstance(cost_observation, dict):
            return
        cost_machine_id = str(
            _deep_dict(cost_observation.get("machine")).get("id")
            or cycle.get("machine_id")
            or "unknown"
        )
        if cost_machine_id != cycle.get("machine_id"):
            cycle["invalid"] = True
        machine_id = cost_machine_id
        collection = _deep_dict(cost_observation.get("collection"))
        observed_at = (
            parse_datetime(collection.get("finished_at"))
            or parse_datetime(cost_observation.get("observed_at"))
            or _now_utc()
        )

        for provider_row in _rows(cost_observation.get("payload")):
            provider = str(provider_row.get("provider") or "unknown").casefold()
            currency_code = str(provider_row.get("currencyCode") or "USD").upper()
            identity, category, reason = self._cycle_attribution(cycle, provider)
            for daily in provider_row.get("daily") or []:
                if not isinstance(daily, dict):
                    continue
                day = parse_date(daily.get("date"))
                if day is None:
                    continue
                self._process_ledger_row(
                    correlation_id=correlation_id,
                    machine_id=machine_id,
                    provider=provider,
                    currency_code=currency_code,
                    day=day,
                    model="__total__",
                    values={
                        field: as_float(daily.get(field)) or 0.0 for field in _NUMERIC_COST_FIELDS
                    },
                    observed_at=observed_at,
                    identity=identity,
                    category=category,
                    reason=reason,
                    is_model=False,
                )
                for model in daily.get("modelBreakdowns") or []:
                    if not isinstance(model, dict):
                        continue
                    model_name = str(model.get("modelName") or "unknown")
                    self._process_ledger_row(
                        correlation_id=correlation_id,
                        machine_id=machine_id,
                        provider=provider,
                        currency_code=currency_code,
                        day=day,
                        model=model_name,
                        values={
                            "totalTokens": as_float(model.get("totalTokens")) or 0.0,
                            "totalCost": as_float(model.get("cost")) or 0.0,
                        },
                        observed_at=observed_at,
                        identity=identity,
                        category=category,
                        reason=reason,
                        is_model=True,
                    )

    def _cycle_attribution(
        self,
        cycle: dict[str, Any],
        provider: str,
    ) -> tuple[AccountIdentity | None, str, str]:
        if cycle.get("invalid"):
            return None, "ambiguous", "correlation_machine_mismatch"
        before = _deep_dict(cycle.get("before")).get(provider)
        after = _deep_dict(cycle.get("after")).get(provider)
        if not isinstance(before, dict) or not isinstance(after, dict):
            return None, "unattributed", "missing_bracket"

        cost = _deep_dict(cycle.get("cost"))
        cost_at = parse_datetime(
            _deep_dict(cost.get("collection")).get("finished_at")
        ) or parse_datetime(cost.get("observed_at"))
        before_at = parse_datetime(before.get("observed_at"))
        after_at = parse_datetime(after.get("observed_at"))
        if (
            cost_at is None
            or before_at is None
            or after_at is None
            or before_at > cost_at
            or cost_at > after_at
        ):
            return None, "ambiguous", "invalid_bracket_order"
        if before.get("status") != "single" or after.get("status") != "single":
            if before.get("status") == "ambiguous" or after.get("status") == "ambiguous":
                return None, "ambiguous", "ambiguous_bracket"
            return None, "unattributed", "missing_bracket"
        before_identity = _identity_from_dict(_deep_dict(before.get("identity")))
        after_identity = _identity_from_dict(_deep_dict(after.get("identity")))
        if before_identity is None or after_identity is None:
            return None, "unattributed", "invalid_identity"
        if before_identity.account_key != after_identity.account_key:
            return None, "ambiguous", "account_changed_within_cycle"
        if not _is_strong(before_identity) or not _is_strong(after_identity):
            return None, "unattributed", "weak_identity"
        return before_identity, "attributed", "matching_brackets"

    def _process_ledger_row(
        self,
        *,
        correlation_id: str,
        machine_id: str,
        provider: str,
        currency_code: str,
        day: date,
        model: str,
        values: dict[str, float],
        observed_at: datetime,
        identity: AccountIdentity | None,
        category: str,
        reason: str,
        is_model: bool,
    ) -> None:
        baseline_key = "|".join((machine_id, provider, currency_code, day.isoformat(), model))
        previous = self.baselines.get(baseline_key)
        current = {
            "machine_id": machine_id,
            "provider": provider,
            "currency_code": currency_code,
            "day": day.isoformat(),
            "model": model,
            "values": values,
            "observed_at": isoformat(observed_at),
            "correlation_id": correlation_id,
        }
        if previous is None:
            self.baselines[baseline_key] = current
            return

        previous_at = parse_datetime(previous.get("observed_at")) or datetime.min.replace(
            tzinfo=UTC
        )
        if observed_at <= previous_at:
            self.stats["out_of_order_cost_cycles"] += 1
            return

        previous_values = _deep_dict(previous.get("values"))
        deltas = {
            key: values.get(key, 0.0) - (as_float(previous_values.get(key)) or 0.0)
            for key in values
        }
        self.baselines[baseline_key] = current

        if any(delta < -1e-9 for delta in deltas.values()):
            self.stats["ledger_resets"] += 1
            return
        if not any(delta > 1e-9 for delta in deltas.values()):
            return

        gap_seconds = (observed_at - previous_at).total_seconds()
        final_category = category
        final_reason = reason
        if final_category == "attributed" and identity is not None:
            intact, integrity_reason = self._active_interval_intact(
                machine_id, provider, identity.account_key, previous_at, observed_at
            )
            if not intact:
                final_category = "ambiguous"
                final_reason = integrity_reason
        if gap_seconds > self.config.max_attribution_gap_seconds:
            final_category = "long_gap"
            final_reason = "poll_gap_exceeded"

        observed_day = observed_at.date()
        within_midnight_grace = day == observed_day - timedelta(days=1) and observed_at.hour < 2
        if day != observed_day and not within_midnight_grace:
            final_category = "historical_backfill"
            final_reason = "historical_row_revision"

        target_key = (
            identity.account_key if final_category == "attributed" and identity else final_category
        )
        bucket = self._attribution_bucket(day, provider, target_key)
        metric_target = bucket.setdefault("models" if is_model else "totals", {})
        if is_model:
            metric_target = metric_target.setdefault(model, {})
        for field, delta in deltas.items():
            if delta <= 0:
                continue
            if field == "totalCost":
                if is_model:
                    currency_models = bucket.setdefault("model_costs", {}).setdefault(
                        currency_code, {}
                    )
                    currency_models[model] = (as_float(currency_models.get(model)) or 0.0) + delta
                else:
                    costs = bucket.setdefault("costs", {})
                    costs[currency_code] = (as_float(costs.get(currency_code)) or 0.0) + delta
            else:
                metric_target[field] = (as_float(metric_target.get(field)) or 0.0) + delta
        # One cost observation can contain both an aggregate daily row and
        # per-model breakdowns. Count attribution decisions once at the daily
        # aggregate level; otherwise coverage/reason counters double-count the
        # same ledger delta while model metrics remain additive diagnostics.
        if not is_model:
            bucket["events"] = int(bucket.get("events", 0)) + 1
            bucket.setdefault("reasons", {})[final_reason] = (
                int(_deep_dict(bucket.get("reasons")).get(final_reason, 0)) + 1
            )
            bucket["last_event_at"] = isoformat(observed_at)
            bucket.setdefault("machines", {})[machine_id] = isoformat(observed_at)

    def _active_interval_intact(
        self,
        machine_id: str,
        provider: str,
        account_key: str,
        previous_at: datetime,
        observed_at: datetime,
    ) -> tuple[bool, str]:
        """Require evidence that no observed account transition crossed the delta interval."""
        machine = self.machines.get(machine_id, {})
        state = _deep_dict(_deep_dict(machine.get("active_accounts")).get(provider))
        current = _deep_dict(state.get("current"))
        if current.get("account_key") != account_key:
            return False, "active_account_mismatch"
        since = parse_datetime(current.get("since"))
        if since is None or since > previous_at:
            return False, "account_interval_started_after_baseline"

        # Scan the event-time evidence, not just the final current state. This
        # catches A→B→A rotations, ambiguous probes and missing/error probes
        # between two otherwise matching cost brackets.
        for evidence in state.get("evidence", []):
            if not isinstance(evidence, dict):
                continue
            evidence_at = parse_datetime(evidence.get("observed_at"))
            if evidence_at is None or not (previous_at < evidence_at <= observed_at):
                continue
            status = evidence.get("status")
            if status == "ambiguous":
                return False, "ambiguous_active_probe_between_samples"
            if status == "missing":
                return False, "missing_active_probe_between_samples"
            evidence_identity = _identity_from_dict(_deep_dict(evidence.get("identity")))
            if evidence_identity is None:
                return False, "invalid_active_probe_between_samples"
            if evidence_identity.account_key != account_key:
                return False, "observed_other_account_between_samples"
        return True, "continuous_observed_account_interval"

    def _attribution_bucket(self, day: date, provider: str, target_key: str) -> dict[str, Any]:
        return (
            self.attribution.setdefault(day.isoformat(), {})
            .setdefault(provider, {})
            .setdefault(
                target_key,
                {
                    "totals": {},
                    "models": {},
                    "costs": {},
                    "model_costs": {},
                    "events": 0,
                    "reasons": {},
                    "machines": {},
                },
            )
        )

    # ---------------------------------------------------------------------
    # Entity projection
    # ---------------------------------------------------------------------

    def metrics(self, now: datetime | None = None) -> dict[str, MetricSpec]:
        """Build the current Home Assistant entity projection."""
        now = (now or _now_utc()).astimezone(UTC)
        metrics: dict[str, MetricSpec] = {}
        fleet_id = "fleet"

        machine_statuses = {
            machine_id: self._machine_status(machine_id, now) for machine_id in self.machines
        }
        collector_failures = {
            machine_id: self._machine_collector_failures(machine)
            for machine_id, machine in self.machines.items()
        }
        expected = set(self.config.expected_machines)
        all_machine_ids = set(self.machines) | expected
        online_count = sum(1 for status in machine_statuses.values() if status["online"])
        stale_count = sum(1 for status in machine_statuses.values() if status["stale"])
        missing_expected = sorted(expected - set(self.machines))

        self._add_metric(
            metrics,
            "fleet_machines_total",
            "sensor",
            "Machines",
            len(all_machine_ids),
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            icon="mdi:laptop-multiple",
            state_class="measurement",
        )
        self._add_metric(
            metrics,
            "fleet_machines_online",
            "sensor",
            "Machines online",
            online_count,
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            icon="mdi:lan-connect",
            state_class="measurement",
        )
        self._add_metric(
            metrics,
            "fleet_machines_stale",
            "sensor",
            "Machines stale",
            stale_count + len(missing_expected),
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            icon="mdi:lan-disconnect",
            state_class="measurement",
        )
        self._add_metric(
            metrics,
            "fleet_accounts_total",
            "sensor",
            "Accounts",
            len(self.accounts),
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            icon="mdi:account-multiple",
            state_class="measurement",
        )
        self._add_metric(
            metrics,
            "fleet_pending_cost_cycles",
            "sensor",
            "Pending cost cycles",
            len(self.pending_cycles),
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            icon="mdi:timer-sand",
            entity_category="diagnostic",
            state_class="measurement",
        )
        self._add_metric(
            metrics,
            "fleet_last_event_at",
            "sensor",
            "Last event",
            self.stats.get("last_event_at"),
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            device_class="timestamp",
            entity_category="diagnostic",
        )

        for days in (1, 7, 30):
            aggregate = self._aggregate_attribution(days, now.date())
            suffix = "today" if days == 1 else f"{days}d"
            total = (
                aggregate["attributed_tokens"]
                + aggregate["ambiguous_tokens"]
                + aggregate["unattributed_tokens"]
                + aggregate["historical_backfill_tokens"]
                + aggregate["long_gap_tokens"]
            )
            coverage = None if total <= 0 else aggregate["attributed_tokens"] / total * 100.0
            self._add_metric(
                metrics,
                f"fleet_attribution_coverage_{suffix}",
                "sensor",
                f"Attribution coverage {suffix}",
                round(coverage, 2) if coverage is not None else None,
                "fleet",
                fleet_id,
                "CodexBar Fleet",
                available=coverage is not None,
                unit="%",
                state_class="measurement",
                icon="mdi:chart-donut",
                suggested_display_precision=1,
                attributes={"observed_tokens": round(total)},
            )
            for category, label in (
                ("ambiguous", "Ambiguous"),
                ("unattributed", "Unattributed"),
                ("historical_backfill", "Historical backfill"),
                ("long_gap", "Long-gap"),
            ):
                self._add_metric(
                    metrics,
                    f"fleet_{category}_tokens_{suffix}",
                    "sensor",
                    f"{label} tokens {suffix}",
                    round(aggregate[f"{category}_tokens"]),
                    "fleet",
                    fleet_id,
                    "CodexBar Fleet",
                    unit="tokens",
                    state_class="measurement",
                    icon="mdi:counter",
                    enabled_default=days in {1, 30},
                )
                for currency, value in sorted(aggregate["costs"][category].items()):
                    self._add_metric(
                        metrics,
                        f"fleet_{category}_cost_{_slug(currency)}_{suffix}",
                        "sensor",
                        f"{label} cost {currency} {suffix}",
                        round(value, 6),
                        "fleet",
                        fleet_id,
                        "CodexBar Fleet",
                        unit=currency,
                        device_class="monetary",
                        state_class="measurement",
                        icon="mdi:cash-multiple",
                        suggested_display_precision=2,
                        enabled_default=days in {1, 30},
                    )

        fleet_coverage = self._aggregate_attribution(1, now.date())
        denom = sum(
            fleet_coverage[key]
            for key in (
                "attributed_tokens",
                "ambiguous_tokens",
                "unattributed_tokens",
                "historical_backfill_tokens",
                "long_gap_tokens",
            )
        )
        coverage_today = None if denom <= 0 else fleet_coverage["attributed_tokens"] / denom * 100.0
        machines_with_collector_failures = sorted(
            machine_id for machine_id, failures in collector_failures.items() if failures
        )
        fleet_problem = bool(
            missing_expected
            or stale_count
            or machines_with_collector_failures
            or (coverage_today is not None and coverage_today < self.config.low_coverage_threshold)
            or fleet_coverage["ambiguous_tokens"] > 0
        )
        self._add_metric(
            metrics,
            "fleet_problem",
            "binary_sensor",
            "Problem",
            fleet_problem,
            "fleet",
            fleet_id,
            "CodexBar Fleet",
            device_class="problem",
            icon="mdi:alert-circle",
            attributes={
                "missing_expected_machines": missing_expected,
                "machines_with_collector_failures": machines_with_collector_failures,
                "coverage_today": round(coverage_today, 2) if coverage_today is not None else None,
                "stats": deepcopy(self.stats),
            },
        )

        for machine_id in sorted(all_machine_ids):
            self._machine_metrics(metrics, machine_id, now, machine_statuses.get(machine_id))
        for account_key, account in sorted(self.accounts.items()):
            self._account_metrics(metrics, account_key, account, now)

        return metrics

    def _machine_metrics(
        self,
        metrics: dict[str, MetricSpec],
        machine_id: str,
        now: datetime,
        status: dict[str, Any] | None,
    ) -> None:
        machine = self.machines.get(machine_id, {})
        status = status or {"online": False, "stale": True, "age_seconds": None}
        device_id = f"machine:{machine_id}"
        name = machine.get("name") or machine_id
        attrs = {
            "hostname": machine.get("hostname"),
            "tags": machine.get("tags", {}),
            "availability": machine.get("availability"),
        }
        self._add_metric(
            metrics,
            f"machine_{_slug(machine_id)}_online",
            "binary_sensor",
            "Online",
            status["online"],
            "machine",
            device_id,
            name,
            device_class="connectivity",
            via_device_id="fleet",
            attributes=attrs,
        )
        self._add_metric(
            metrics,
            f"machine_{_slug(machine_id)}_stale",
            "binary_sensor",
            "Stale",
            status["stale"],
            "machine",
            device_id,
            name,
            device_class="problem",
            via_device_id="fleet",
            entity_category="diagnostic",
        )
        self._add_metric(
            metrics,
            f"machine_{_slug(machine_id)}_last_event",
            "sensor",
            "Last event",
            machine.get("last_event_at"),
            "machine",
            device_id,
            name,
            device_class="timestamp",
            entity_category="diagnostic",
            via_device_id="fleet",
        )
        self._add_metric(
            metrics,
            f"machine_{_slug(machine_id)}_event_age",
            "sensor",
            "Event age",
            status.get("age_seconds"),
            "machine",
            device_id,
            name,
            unit="s",
            device_class="duration",
            state_class="measurement",
            entity_category="diagnostic",
            via_device_id="fleet",
            enabled_default=False,
        )

        heartbeat = _deep_dict(machine.get("heartbeat"))
        spool = _deep_dict(heartbeat.get("spool"))
        if "mqtt_connected" in heartbeat:
            self._add_metric(
                metrics,
                f"machine_{_slug(machine_id)}_mqtt_connected",
                "binary_sensor",
                "MQTT connected",
                bool(heartbeat.get("mqtt_connected")),
                "machine",
                device_id,
                name,
                device_class="connectivity",
                entity_category="diagnostic",
                via_device_id="fleet",
            )
        if heartbeat.get("uptime_seconds") is not None:
            self._add_metric(
                metrics,
                f"machine_{_slug(machine_id)}_agent_uptime",
                "sensor",
                "Agent uptime",
                heartbeat.get("uptime_seconds"),
                "machine",
                device_id,
                name,
                unit="s",
                device_class="duration",
                state_class="measurement",
                entity_category="diagnostic",
                via_device_id="fleet",
            )
        for field, label, unit in (
            ("messages", "Spool messages", None),
            ("bytes", "Spool bytes", "B"),
            ("dropped", "Spool dropped", None),
        ):
            if field in spool:
                self._add_metric(
                    metrics,
                    f"machine_{_slug(machine_id)}_spool_{field}",
                    "sensor",
                    label,
                    spool.get(field),
                    "machine",
                    device_id,
                    name,
                    unit=unit,
                    state_class="measurement",
                    entity_category="diagnostic",
                    via_device_id="fleet",
                    icon="mdi:database-clock",
                )

        failures = self._machine_collector_failures(machine)
        collector_problem = bool(failures or (as_int(spool.get("dropped")) or 0) > 0)
        self._add_metric(
            metrics,
            f"machine_{_slug(machine_id)}_collector_problem",
            "binary_sensor",
            "Collector problem",
            collector_problem,
            "machine",
            device_id,
            name,
            device_class="problem",
            entity_category="diagnostic",
            via_device_id="fleet",
            attributes={"failed_jobs": failures, "spool_dropped": spool.get("dropped")},
        )

        meta = _deep_dict(machine.get("meta"))
        codexbar = _deep_dict(meta.get("codexbar"))
        agent = _deep_dict(meta.get("agent")) or _deep_dict(machine.get("agent"))
        for field, value, label in (
            ("agent_version", agent.get("version"), "Agent version"),
            (
                "codexbar_version",
                codexbar.get("cli_version")
                or _deep_dict(machine.get("serve_health")).get("version"),
                "CodexBar version",
            ),
        ):
            if value is not None:
                self._add_metric(
                    metrics,
                    f"machine_{_slug(machine_id)}_{field}",
                    "sensor",
                    label,
                    value,
                    "machine",
                    device_id,
                    name,
                    entity_category="diagnostic",
                    via_device_id="fleet",
                    icon="mdi:package-variant",
                )

        active_accounts = _deep_dict(machine.get("active_accounts"))
        for provider, active_state in sorted(active_accounts.items()):
            current = _deep_dict(_deep_dict(active_state).get("current"))
            provider_slug = _slug(provider)
            self._add_metric(
                metrics,
                f"machine_{_slug(machine_id)}_{provider_slug}_current_account",
                "sensor",
                f"{provider} current account",
                current.get("label") or "unknown",
                "machine",
                device_id,
                name,
                entity_category="diagnostic",
                via_device_id="fleet",
                icon="mdi:account-switch",
                attributes={
                    "account_key": current.get("account_key"),
                    "confidence": current.get("confidence"),
                    "since": current.get("since"),
                    "candidate": active_state.get("candidate"),
                },
            )
            self._add_metric(
                metrics,
                f"machine_{_slug(machine_id)}_{provider_slug}_switch_count",
                "sensor",
                f"{provider} switch count",
                active_state.get("switch_count", 0),
                "machine",
                device_id,
                name,
                state_class="measurement",
                entity_category="diagnostic",
                via_device_id="fleet",
                icon="mdi:swap-horizontal",
            )
            if active_state.get("last_switch_at"):
                self._add_metric(
                    metrics,
                    f"machine_{_slug(machine_id)}_{provider_slug}_last_switch",
                    "sensor",
                    f"{provider} last switch",
                    active_state.get("last_switch_at"),
                    "machine",
                    device_id,
                    name,
                    device_class="timestamp",
                    entity_category="diagnostic",
                    via_device_id="fleet",
                )

        for provider, horizons in sorted(_deep_dict(machine.get("costs")).items()):
            if not isinstance(horizons, dict):
                continue
            for horizon, cost in sorted(horizons.items(), key=lambda item: int(item[0])):
                if not isinstance(cost, dict):
                    continue
                prefix = f"machine_{_slug(machine_id)}_{_slug(provider)}_{horizon}d"
                attrs = {
                    "currency_code": cost.get("currency_code"),
                    "updated_at": cost.get("updated_at"),
                    "source_observed_at": cost.get("observed_at"),
                }
                self._add_metric(
                    metrics,
                    f"{prefix}_tokens",
                    "sensor",
                    f"{provider} {horizon}d tokens",
                    cost.get("total_tokens") or cost.get("history_tokens"),
                    "machine",
                    device_id,
                    name,
                    unit="tokens",
                    state_class="measurement",
                    via_device_id="fleet",
                    icon="mdi:counter",
                    attributes=attrs,
                    enabled_default=horizon in {"1", "7", "30"},
                )
                self._add_metric(
                    metrics,
                    f"{prefix}_cost",
                    "sensor",
                    f"{provider} {horizon}d cost",
                    cost.get("total_cost")
                    if cost.get("total_cost") is not None
                    else cost.get("history_cost"),
                    "machine",
                    device_id,
                    name,
                    unit=cost.get("currency_code") or "USD",
                    device_class="monetary",
                    state_class="measurement",
                    via_device_id="fleet",
                    icon="mdi:cash",
                    suggested_display_precision=2,
                    attributes=attrs,
                    enabled_default=horizon in {"1", "7", "30"},
                )
                if self.config.enable_verbose_entities:
                    for model_name, model_values in sorted(_deep_dict(cost.get("models")).items()):
                        model_slug = _slug(model_name)
                        self._add_metric(
                            metrics,
                            f"{prefix}_model_{model_slug}_tokens",
                            "sensor",
                            f"{provider} {horizon}d {model_name} tokens",
                            round(as_float(_deep_dict(model_values).get("total_tokens")) or 0),
                            "machine",
                            device_id,
                            name,
                            unit="tokens",
                            state_class="measurement",
                            via_device_id="fleet",
                            icon="mdi:chip",
                            enabled_default=False,
                        )

    def _account_metrics(
        self,
        metrics: dict[str, MetricSpec],
        account_key: str,
        account: dict[str, Any],
        now: datetime,
    ) -> None:
        account_id = account_hash(account_key)
        device_id = f"account:{account_id}"
        provider = str(account.get("provider") or "unknown")
        label = str(account.get("label") or account_key)
        name = f"{provider.title()} · {label}"
        active_machines = []
        observing_machines = []
        for machine_id, machine in self.machines.items():
            active_state = _deep_dict(_deep_dict(machine.get("active_accounts")).get(provider))
            if _deep_dict(active_state.get("current")).get("account_key") == account_key:
                observing_machines.append(machine_id)
                if self._machine_status(machine_id, now)["online"]:
                    active_machines.append(machine_id)
        base_attrs = {
            "provider": provider,
            "account_key": account_key,
            "identity_confidence": account.get("confidence"),
            "identity_kind": account.get("identity_kind"),
            "aliases": account.get("aliases", []),
            "machines_seen": sorted(_deep_dict(account.get("machines"))),
            "active_machines": sorted(active_machines),
            "observing_machines_including_stale": sorted(observing_machines),
        }
        self._add_metric(
            metrics,
            f"account_{account_id}_active_machines",
            "sensor",
            "Active machines",
            len(active_machines),
            "account",
            device_id,
            name,
            state_class="measurement",
            icon="mdi:laptop-account",
            via_device_id="fleet",
            attributes=base_attrs,
        )
        self._add_metric(
            metrics,
            f"account_{account_id}_last_seen",
            "sensor",
            "Last seen",
            account.get("last_seen_at"),
            "account",
            device_id,
            name,
            device_class="timestamp",
            entity_category="diagnostic",
            via_device_id="fleet",
            attributes=base_attrs,
        )
        self._add_metric(
            metrics,
            f"account_{account_id}_identity_confidence",
            "sensor",
            "Identity confidence",
            account.get("confidence"),
            "account",
            device_id,
            name,
            entity_category="diagnostic",
            via_device_id="fleet",
            attributes=base_attrs,
        )

        for window_id, window in sorted(_deep_dict(account.get("quota_windows")).items()):
            if not isinstance(window, dict):
                continue
            slug = _slug(window_id)
            used = as_float(window.get("used_percent"))
            reset_at = parse_datetime(window.get("resets_at"))
            reset_passed = reset_at is not None and reset_at <= now
            known = bool(window.get("usage_known", True)) and used is not None and not reset_passed
            remaining = max(0.0, 100.0 - used) if known and used is not None else None
            attrs = {
                **base_attrs,
                "window_id": window_id,
                "title": window.get("title"),
                "window_minutes": window.get("window_minutes"),
                "reset_description": window.get("reset_description"),
                "next_regen_percent": window.get("next_regen_percent"),
                "usage_known": known,
                "reset_passed_without_refresh": reset_passed,
                "source_machine_id": window.get("machine_id"),
                "semantic_scope": window.get("semantic_scope"),
                "source": window.get("source"),
                "effective_at": window.get("effective_at"),
            }
            title = str(window.get("title") or window_id)
            self._add_metric(
                metrics,
                f"account_{account_id}_quota_{slug}_remaining",
                "sensor",
                f"{title} remaining",
                round(remaining, 2) if remaining is not None else None,
                "account",
                device_id,
                name,
                available=known,
                unit="%",
                state_class="measurement",
                icon="mdi:gauge",
                suggested_display_precision=1,
                via_device_id="fleet",
                attributes=attrs,
            )
            self._add_metric(
                metrics,
                f"account_{account_id}_quota_{slug}_used",
                "sensor",
                f"{title} used",
                round(used, 2) if used is not None else None,
                "account",
                device_id,
                name,
                available=known,
                unit="%",
                state_class="measurement",
                icon="mdi:gauge-full",
                suggested_display_precision=1,
                via_device_id="fleet",
                attributes=attrs,
                enabled_default=False,
            )
            if window.get("resets_at"):
                self._add_metric(
                    metrics,
                    f"account_{account_id}_quota_{slug}_reset",
                    "sensor",
                    f"{title} reset",
                    window.get("resets_at"),
                    "account",
                    device_id,
                    name,
                    device_class="timestamp",
                    icon="mdi:restore",
                    via_device_id="fleet",
                    attributes=attrs,
                )
            self._add_metric(
                metrics,
                f"account_{account_id}_quota_{slug}_low",
                "binary_sensor",
                f"{title} low",
                bool(
                    known and remaining is not None and remaining < self.config.low_quota_threshold
                ),
                "account",
                device_id,
                name,
                device_class="problem",
                icon="mdi:gauge-empty",
                via_device_id="fleet",
                attributes={**attrs, "threshold": self.config.low_quota_threshold},
            )

        credits = _deep_dict(account.get("credits"))
        if credits:
            self._add_metric(
                metrics,
                f"account_{account_id}_credits_remaining",
                "sensor",
                "Credits remaining",
                credits.get("remaining"),
                "account",
                device_id,
                name,
                state_class="measurement",
                icon="mdi:ticket-percent",
                via_device_id="fleet",
                attributes={**base_attrs, "updated_at": credits.get("updated_at")},
            )

        reset_credits = _deep_dict(account.get("reset_credits"))
        if reset_credits:
            self._add_metric(
                metrics,
                f"account_{account_id}_reset_credits_available",
                "sensor",
                "Reset credits available",
                reset_credits.get("available_count"),
                "account",
                device_id,
                name,
                state_class="measurement",
                icon="mdi:backup-restore",
                via_device_id="fleet",
                attributes={**base_attrs, "updated_at": reset_credits.get("updated_at")},
            )

        provider_cost = _deep_dict(account.get("provider_cost"))
        if provider_cost:
            used = as_float(provider_cost.get("used"))
            limit = as_float(provider_cost.get("limit"))
            currency = provider_cost.get("currency_code") or "USD"
            remaining = limit - used if used is not None and limit is not None else None
            percent = used / limit * 100 if used is not None and limit and limit > 0 else None
            attrs = {
                **base_attrs,
                "period": provider_cost.get("period"),
                "updated_at": provider_cost.get("updated_at"),
                "limit": limit,
            }
            self._add_metric(
                metrics,
                f"account_{account_id}_provider_cost_used",
                "sensor",
                "Plan cost used",
                used,
                "account",
                device_id,
                name,
                unit=currency,
                device_class="monetary",
                state_class="measurement",
                icon="mdi:cash",
                suggested_display_precision=2,
                via_device_id="fleet",
                attributes=attrs,
            )
            self._add_metric(
                metrics,
                f"account_{account_id}_provider_cost_remaining",
                "sensor",
                "Plan cost remaining",
                remaining,
                "account",
                device_id,
                name,
                unit=currency,
                device_class="monetary",
                state_class="measurement",
                icon="mdi:cash-minus",
                suggested_display_precision=2,
                via_device_id="fleet",
                attributes=attrs,
            )
            self._add_metric(
                metrics,
                f"account_{account_id}_provider_cost_used_percent",
                "sensor",
                "Plan cost used",
                percent,
                "account",
                device_id,
                name,
                unit="%",
                state_class="measurement",
                icon="mdi:chart-donut",
                suggested_display_precision=1,
                via_device_id="fleet",
                attributes=attrs,
                enabled_default=False,
            )

        status = _deep_dict(account.get("status"))
        if status:
            self._add_metric(
                metrics,
                f"account_{account_id}_provider_status",
                "sensor",
                "Provider status",
                status.get("indicator") or "unknown",
                "account",
                device_id,
                name,
                icon="mdi:server-network",
                entity_category="diagnostic",
                via_device_id="fleet",
                attributes={
                    **base_attrs,
                    "description": status.get("description"),
                    "updated_at": status.get("updated_at"),
                    "url": status.get("url"),
                },
            )

        dashboard = _deep_dict(account.get("dashboard"))
        if dashboard:
            for days in (1, 7, 30):
                dashboard_aggregate = self._dashboard_totals(dashboard, days, now.date())
                suffix = "today" if days == 1 else f"{days}d"
                self._add_metric(
                    metrics,
                    f"account_{account_id}_dashboard_credits_{suffix}",
                    "sensor",
                    f"Dashboard credits {suffix}",
                    round(dashboard_aggregate["total"], 6),
                    "account",
                    device_id,
                    name,
                    unit="credits",
                    state_class="measurement",
                    icon="mdi:chart-timeline-variant",
                    suggested_display_precision=2,
                    via_device_id="fleet",
                    attributes={
                        **base_attrs,
                        "updated_at": dashboard.get("updated_at"),
                        "account_plan": dashboard.get("account_plan"),
                        "services": dashboard_aggregate["services"],
                    },
                )
                if self.config.enable_verbose_entities:
                    for service, value in sorted(dashboard_aggregate["services"].items()):
                        self._add_metric(
                            metrics,
                            f"account_{account_id}_dashboard_{_slug(service)}_{suffix}",
                            "sensor",
                            f"{service} credits {suffix}",
                            round(value, 6),
                            "account",
                            device_id,
                            name,
                            unit="credits",
                            state_class="measurement",
                            icon="mdi:chart-bar",
                            suggested_display_precision=2,
                            via_device_id="fleet",
                            enabled_default=False,
                        )

        for days in (1, 7, 30):
            values = self._account_attribution(account_key, days, now.date())
            suffix = "today" if days == 1 else f"{days}d"
            self._add_metric(
                metrics,
                f"account_{account_id}_attributed_tokens_{suffix}",
                "sensor",
                f"Attributed tokens {suffix}",
                round(values["totalTokens"]),
                "account",
                device_id,
                name,
                unit="tokens",
                state_class="measurement",
                icon="mdi:counter",
                via_device_id="fleet",
                attributes={
                    **base_attrs,
                    "input_tokens": round(values["inputTokens"]),
                    "output_tokens": round(values["outputTokens"]),
                    "cache_read_tokens": round(values["cacheReadTokens"]),
                    "cache_creation_tokens": round(values["cacheCreationTokens"]),
                    "models": values["models"],
                    "model_costs": values["model_costs"],
                },
            )
            for currency, value in sorted(values["costs"].items()):
                self._add_metric(
                    metrics,
                    f"account_{account_id}_attributed_cost_{_slug(currency)}_{suffix}",
                    "sensor",
                    f"Attributed cost {currency} {suffix}",
                    round(value, 6),
                    "account",
                    device_id,
                    name,
                    unit=currency,
                    device_class="monetary",
                    state_class="measurement",
                    icon="mdi:cash-check",
                    suggested_display_precision=2,
                    via_device_id="fleet",
                )

    @staticmethod
    def _add_metric(
        target: dict[str, MetricSpec],
        key: str,
        platform: str,
        name: str,
        value: Any,
        device_kind: str,
        device_id: str,
        device_name: str,
        **kwargs: Any,
    ) -> None:
        target[key] = MetricSpec(
            key=key,
            platform=platform,  # type: ignore[arg-type]
            name=name,
            value=value,
            device_kind=device_kind,  # type: ignore[arg-type]
            device_id=device_id,
            device_name=device_name,
            **kwargs,
        )

    # ---------------------------------------------------------------------
    # Aggregate helpers
    # ---------------------------------------------------------------------

    def _dashboard_totals(
        self,
        dashboard: dict[str, Any],
        days: int,
        end_date: date,
    ) -> dict[str, Any]:
        start = end_date - timedelta(days=days - 1)
        services: dict[str, float] = {}
        total = 0.0
        for item in dashboard.get("usage_breakdown") or []:
            if not isinstance(item, dict):
                continue
            day = parse_date(item.get("day"))
            if day is None or day < start or day > end_date:
                continue
            day_total = as_float(item.get("total_credits_used"))
            if day_total is None:
                day_total = 0.0
            total += day_total
            for service in item.get("services") or []:
                if not isinstance(service, dict):
                    continue
                name = str(service.get("service") or "Unknown")
                services[name] = services.get(name, 0.0) + (
                    as_float(service.get("credits_used")) or 0.0
                )
        return {"total": total, "services": services}

    def _account_attribution(
        self,
        account_key: str,
        days: int,
        end_date: date,
    ) -> dict[str, Any]:
        result = {field: 0.0 for field in _TOKEN_FIELDS}
        models: dict[str, dict[str, float]] = {}
        costs: dict[str, float] = {}
        model_costs: dict[str, dict[str, float]] = {}
        start = end_date - timedelta(days=days - 1)
        for day_text, providers in self.attribution.items():
            day = parse_date(day_text)
            if day is None or day < start or day > end_date or not isinstance(providers, dict):
                continue
            for provider_targets in providers.values():
                if not isinstance(provider_targets, dict):
                    continue
                bucket = provider_targets.get(account_key)
                if not isinstance(bucket, dict):
                    continue
                for field, value in _deep_dict(bucket.get("totals")).items():
                    if field in _TOKEN_FIELDS:
                        result[field] = result.get(field, 0.0) + (as_float(value) or 0.0)
                for currency, value in _deep_dict(bucket.get("costs")).items():
                    code = str(currency).upper()
                    costs[code] = costs.get(code, 0.0) + (as_float(value) or 0.0)
                for model, fields in _deep_dict(bucket.get("models")).items():
                    model_target = models.setdefault(model, {})
                    for field, value in _deep_dict(fields).items():
                        if field in _TOKEN_FIELDS:
                            model_target[field] = model_target.get(field, 0.0) + (
                                as_float(value) or 0.0
                            )
                for currency, currency_models in _deep_dict(bucket.get("model_costs")).items():
                    target_currency = model_costs.setdefault(str(currency).upper(), {})
                    for model, value in _deep_dict(currency_models).items():
                        target_currency[model] = target_currency.get(model, 0.0) + (
                            as_float(value) or 0.0
                        )
        result["models"] = models
        result["costs"] = costs
        result["model_costs"] = model_costs
        return result

    def _aggregate_attribution(self, days: int, end_date: date) -> dict[str, Any]:
        result: dict[str, Any] = {
            "attributed_tokens": 0.0,
            "ambiguous_tokens": 0.0,
            "unattributed_tokens": 0.0,
            "historical_backfill_tokens": 0.0,
            "long_gap_tokens": 0.0,
            "costs": {category: {} for category in ("attributed", *_ATTRIBUTION_CATEGORIES)},
        }
        start = end_date - timedelta(days=days - 1)
        for day_text, providers in self.attribution.items():
            day = parse_date(day_text)
            if day is None or day < start or day > end_date or not isinstance(providers, dict):
                continue
            for targets in providers.values():
                if not isinstance(targets, dict):
                    continue
                for target_key, bucket in targets.items():
                    if not isinstance(bucket, dict):
                        continue
                    totals = _deep_dict(bucket.get("totals"))
                    tokens = as_float(totals.get("totalTokens")) or 0.0
                    category = target_key if target_key in _ATTRIBUTION_CATEGORIES else "attributed"
                    result[f"{category}_tokens"] += tokens
                    category_costs = result["costs"][category]
                    for currency, value in _deep_dict(bucket.get("costs")).items():
                        code = str(currency).upper()
                        category_costs[code] = category_costs.get(code, 0.0) + (
                            as_float(value) or 0.0
                        )
        return result

    # ---------------------------------------------------------------------
    # State helpers
    # ---------------------------------------------------------------------

    def _machine(self, machine_id: str) -> dict[str, Any]:
        return self.machines.setdefault(
            machine_id,
            {
                "id": machine_id,
                "name": machine_id,
                "availability": "unknown",
                "active_accounts": {},
                "costs": {},
                "errors": {},
            },
        )

    def _touch_machine(self, machine_id: str, observed_at: datetime) -> None:
        machine = self._machine(machine_id)
        current = parse_datetime(machine.get("last_seen_at"))
        if current is None or observed_at > current:
            machine["last_seen_at"] = isoformat(observed_at)

    def _machine_status(self, machine_id: str, now: datetime) -> dict[str, Any]:
        machine = self.machines.get(machine_id, {})
        last = (
            parse_datetime(machine.get("last_heartbeat_at"))
            or parse_datetime(machine.get("last_event_at"))
            or parse_datetime(machine.get("last_seen_at"))
        )
        age = (now - last).total_seconds() if last else None
        stale = age is None or age > self.config.stale_after_seconds
        availability = str(machine.get("availability") or "unknown").casefold()
        online = not stale and availability != "offline"
        # Minute buckets prevent diagnostic event-age entities from producing a
        # new recorder row on every correlation timeout tick.
        age_seconds = int(age // 60 * 60) if age is not None else None
        return {"online": online, "stale": stale, "age_seconds": age_seconds}

    @staticmethod
    def _machine_collector_failures(machine: dict[str, Any]) -> dict[str, str]:
        """Return currently failing Mac-agent jobs from the retained heartbeat."""
        jobs = _deep_dict(_deep_dict(machine.get("heartbeat")).get("jobs"))
        return {
            str(name): str(_deep_dict(state).get("last_error"))
            for name, state in jobs.items()
            if _deep_dict(state).get("last_error")
        }

    def _remember_event(self, event_id: str, observed_at: datetime) -> None:
        self.seen_events[event_id] = isoformat(observed_at) or ""
        self.seen_events.move_to_end(event_id)
        self._trim_ordered(self.seen_events, MAX_SEEN_EVENTS)

    @staticmethod
    def _trim_ordered(values: OrderedDict[str, str], maximum: int) -> None:
        while len(values) > maximum:
            values.popitem(last=False)

    def _prune(self, now: datetime) -> None:
        cutoff = now.date() - timedelta(days=max(1, self.config.retention_days))
        for day_text in list(self.attribution):
            day = parse_date(day_text)
            if day is not None and day < cutoff:
                del self.attribution[day_text]
        for key, baseline in list(self.baselines.items()):
            day = parse_date(baseline.get("day"))
            if day is not None and day < cutoff:
                del self.baselines[key]
        for account in self.accounts.values():
            dashboard = _deep_dict(account.get("dashboard"))
            if dashboard:
                dashboard["usage_breakdown"] = [
                    item
                    for item in dashboard.get("usage_breakdown") or []
                    if (parse_date(_deep_dict(item).get("day")) or cutoff) >= cutoff
                ]

    def diagnostics(self, now: datetime | None = None) -> dict[str, Any]:
        """Return a bounded diagnostic summary without raw provider payloads."""
        now = (now or _now_utc()).astimezone(UTC)
        return {
            "config": asdict(self.config),
            "stats": deepcopy(self.stats),
            "machines": {
                machine_id: {
                    "name": machine.get("name"),
                    "status": self._machine_status(machine_id, now),
                    "last_event_at": machine.get("last_event_at"),
                    "last_heartbeat_at": machine.get("last_heartbeat_at"),
                    "availability": machine.get("availability"),
                    "active_accounts": deepcopy(machine.get("active_accounts", {})),
                    "cost_horizons": {
                        provider: sorted(int(day) for day in horizons)
                        for provider, horizons in _deep_dict(machine.get("costs")).items()
                        if isinstance(horizons, dict)
                    },
                    "errors": deepcopy(machine.get("errors", {})),
                }
                for machine_id, machine in self.machines.items()
            },
            "accounts": {
                account_hash(key): {
                    "provider": value.get("provider"),
                    "confidence": value.get("confidence"),
                    "machine_scoped": value.get("machine_scoped"),
                    "last_seen_at": value.get("last_seen_at"),
                    "quota_windows": sorted(_deep_dict(value.get("quota_windows"))),
                    "has_dashboard": bool(value.get("dashboard")),
                    "conflicts": value.get("conflicts", 0),
                }
                for key, value in self.accounts.items()
            },
            "pending_cycles": len(self.pending_cycles),
            "baselines": len(self.baselines),
            "attribution_days": len(self.attribution),
        }
