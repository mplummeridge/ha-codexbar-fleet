# Architecture and invariants

## Components

```text
Mac A ─ codexbar serve ─ codexbar-mqtt ─┐
Mac B ─ codexbar serve ─ codexbar-mqtt ─┼─ MQTT ─ Home Assistant CodexBar Fleet
Mac C ─ codexbar serve ─ codexbar-mqtt ─┘
```

The Mac agent is an evidence producer. Home Assistant owns canonical identity resolution, account-state compilation, ledger differencing, confidence classification, persistence, and entity projection.

## Observation classes

The integration recognises the agent's v1 semantic scopes:

- `machine_runtime_health`
- `serve_usage_snapshot`
- `all_registered_provider_snapshot`
- `current_default_account_probe`
- `all_visible_or_configured_accounts`
- `provider_status_enriched_usage`
- `machine_local_cost_snapshot`
- `machine_local_cost_history`
- `cost_attribution_account_bracket`
- `local_codexbar_config_validation`

Normal `/usage`, all-provider discovery, and account catalogues can update account quota/catalogue state, but only `current_default_account_probe` and bracket probes are activity evidence.

## Identity hierarchy

Canonicalisation prefers a real email from `usage.identity.accountEmail`, `usage.accountEmail`, or the OpenAI dashboard. An explicit `row.account` remains the display label but does not split the account when the email is stable.

A non-email provider account identifier carried in `accountEmail` is treated as provider-global evidence. Arbitrary labels and organisation-only identities are machine-scoped. This is intentionally conservative: false non-merges are repairable, false global merges corrupt every aggregate.

## Event-time compilation

Every active-account probe is retained in a bounded per-machine/provider evidence log and sorted by:

```text
observed_at, event_id
```

The current account, confirmation candidate, switch count, and intervals are rebuilt from that log. Broker arrival order is not trusted after spool replay.

Missing or ambiguous probes break a candidate sequence. They do not assert an account switch, but they invalidate cost attribution intervals that cross them.

## Account-global state

Quota windows, credits, reset credits, plan caps, provider status, and OpenAI dashboard data use a newest-wins comparator:

```text
effective source timestamp
semantic-source priority
observation timestamp
```

Equal-timestamp/equal-priority divergent quota values increment a conflict counter rather than being silently averaged.

## Local ledger state

A baseline is keyed by:

```text
machine + provider + currency + date + model
```

Daily aggregate and model rows are differenced independently. Only the daily aggregate contributes to attribution coverage/reason event counts; model rows add breakdown diagnostics without double-counting the decision.

## Attribution classes

- `attributed`: matching strong brackets plus continuous active interval.
- `ambiguous`: account changed, active evidence conflicted, event order invalid, or correlation crossed machines.
- `unattributed`: bracket missing or identity weak.
- `historical_backfill`: an older daily row was revised.
- `long_gap`: the delta interval exceeded configured policy.

Tokens are dimensionally safe to aggregate across currencies. Money remains grouped by currency through storage and entity projection.

## Persistence

Home Assistant's JSON Store persists:

- machine/account projections;
- daily ledger baselines;
- attributed and rejected daily deltas;
- bounded event/correlation deduplication sets;
- diagnostic counters.

Pending cycles are not persisted because partial before/cost/after transactions cannot be made trustworthy after restart using retained snapshots alone.


## MQTT integration discovery

Agents publish a retained beacon at `codexbar/discovery/v1/<fleet-id>/<machine-id>`.
The integration manifest subscribes to `codexbar/discovery/v1/+/+`, and
`async_step_mqtt` validates the beacon before presenting a zero-input discovered
config flow. `fleet-id` is derived from the effective data prefix, so all machines
using one prefix converge on one config entry while independent prefixes remain
separate fleets. The runtime manager then subscribes only to availability, meta,
heartbeat, snapshot and event contract branches beneath that discovered prefix.
