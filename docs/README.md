# CodexBar Fleet for Home Assistant

Native Home Assistant aggregation for the `codexbar-mqtt` Mac agent.

It consumes the raw MQTT evidence stream from every Mac, maintains a persistent event-time projection, discovers machines and accounts dynamically, and exposes fleet, machine, account, quota, dashboard, cost, and data-quality entities.

## What it treats as truth

| Data | Scope | Aggregation rule |
|---|---|---|
| CodexBar quota windows | Provider account | Newest account observation wins; percentages are never summed |
| OpenAI dashboard breakdown | Provider account | Newest dashboard snapshot wins; duplicate machine observations are not summed |
| CodexBar local cost ledger | Machine + provider + date + model | Keep revisions and compute positive deltas |
| Account-attributed local cost | Inferred account interval | Attribute only when before/after account probes agree and the observed account interval is continuous |
| Runtime health | Machine | Last heartbeat/event plus MQTT availability and expected-machine policy |

The integration deliberately keeps `ambiguous`, `unattributed`, `historical_backfill`, and `long_gap` deltas separate. It never hides uncertain usage inside an account total.

## Requirements

- Home Assistant with its MQTT integration configured.
- One or more Macs running `codexbar-mqtt` 0.2.x for zero-input discovery. Legacy 0.1.x agents remain available through manual topic-prefix setup.
- Every agent publishes beneath the same topic prefix, default `codexbar/v1`.
- Python 3.13-era Home Assistant APIs; this release targets current Home Assistant releases using config-entry `runtime_data`.

## Installation

### HACS custom repository

Add this repository as an **Integration** custom repository, install **CodexBar Fleet**, and restart Home Assistant. A 0.2 agent publishes a retained beacon under `codexbar/discovery/v1/#`; Home Assistant then shows **CodexBar Fleet discovered** under **Settings → Devices & services**. Confirm it without entering a prefix or machine list.

### Manual

Copy:

```text
custom_components/codexbar_fleet
```

into:

```text
/config/custom_components/codexbar_fleet
```

Restart Home Assistant. With a 0.2 agent, confirm the automatically discovered fleet. The manual **Add integration** flow remains only for legacy agents or restrictive broker ACLs; its normal prefix is `codexbar/v1`.

No broker credentials are entered into this integration; it uses Home Assistant's existing MQTT connection.

## Recommended options

Configure these from the integration's **Configure** dialog:

- **Expected machine IDs**: optional comma-separated pinned membership. Machines are otherwise learned dynamically; pin only nodes whose absence must become a fleet problem.
- **Machine stale after**: default 180 seconds.
- **Cost-cycle timeout**: default 120 seconds.
- **Maximum attributable polling gap**: default 900 seconds. Longer deltas remain separate.
- **Account-change confirmations**: default 2. This filters one-off probe noise while still allowing the before/after probes in one cost cycle to confirm a switch.
- **Retention**: default 120 days for attributed daily deltas and dashboard breakdowns.
- **Verbose entities**: creates per-model and per-dashboard-service entities; leave disabled unless needed.

## Discovery model

Discovery occurs at three levels:

1. A retained well-known beacon creates one pending config flow per MQTT fleet prefix.
2. Retained node metadata and heartbeats add Mac devices dynamically.
3. Usage evidence adds canonical provider-account devices and quota entities dynamically.

Multiple fleets are isolated by a prefix-derived fleet ID. A custom-prefix fleet is therefore discoverable without hard-coding its prefix in the Home Assistant manifest.

## Device model

The integration creates:

1. A **CodexBar Fleet** device for fleet health and attribution coverage.
2. One device per Mac for agent/MQTT health, current observed accounts, spool state, versions, and machine-local cost horizons.
3. One device per canonical provider account for quota windows, reset times, plan cost caps, dashboard usage, and inferred local token/cost totals.

Account entity IDs use a stable hash, while the human-readable device name preserves the provider and account label. Account identifiers are retained in attributes for diagnostics and automation.

## Cost attribution

A valid account attribution cycle is:

```text
before-cost active-account probe
             ↓
      local /cost snapshot
             ↓
after-cost active-account probe
```

The integration then compares the new machine/provider/date/model ledger revision with its previous baseline.

A positive delta is attributed only when:

- both probes identify one strong account;
- both identify the same account;
- their event timestamps bracket the cost sample;
- all correlation events came from the same machine;
- no different, missing, or ambiguous account evidence occurred since the previous ledger baseline;
- the polling gap is within policy;
- the changed row is current-day data, apart from a small post-midnight grace period.

The first observation establishes a baseline and creates no account usage. A negative revision is treated as a scanner reset/new baseline. Currency counters are isolated, so a currency change cannot produce a fictitious delta and different currencies are never summed into one money sensor.

## Replay and outage behaviour

- Non-retained events are deduplicated by event ID.
- Retained snapshots use a separate deduplication namespace, so a retained bootstrap message cannot suppress the matching evidence event later replayed from the Mac spool.
- Active-account evidence is sorted by event time and recompiled, preventing delayed spool events from rolling the current account backwards.
- Pending correlation cycles are intentionally not persisted; after an HA restart, retained snapshots restore current state but cannot pretend to reconstruct a complete historical transaction.
- Heartbeat spool drop counts and failing collection jobs surface as machine/fleet problems.

## Useful entities

Examples vary because account IDs are hashed:

```text
binary_sensor.codexbar_fleet_problem
sensor.codexbar_fleet_attribution_coverage_today
sensor.codexbar_fleet_ambiguous_tokens_today
sensor.codexbar_fleet_unattributed_tokens_today

binary_sensor.<machine>_online
binary_sensor.<machine>_collector_problem
sensor.<machine>_claude_current_account
sensor.<machine>_claude_30d_tokens
sensor.<machine>_claude_30d_cost

sensor.<account>_primary_remaining
sensor.<account>_secondary_remaining
binary_sensor.<account>_primary_low
sensor.<account>_dashboard_credits_30d
sensor.<account>_attributed_tokens_30d
sensor.<account>_attributed_cost_usd_30d
```

## Verification

On Home Assistant, inspect the MQTT topic tree:

```text
codexbar/v1/nodes/+/availability
codexbar/v1/nodes/+/heartbeat
codexbar/v1/events/#
codexbar/v1/nodes/+/snapshots/#
```

The fleet device should appear after retained node metadata or the first observation arrives. Diagnostics are available from the integration's overflow menu and redact account keys, labels, emails, aliases, and hostnames. Persistent projection state is written through Home Assistant's private, atomic JSON Store. MQTT ingestion uses scoped node/event subscriptions rather than subscribing to the entire fleet namespace.

## Known limits

- CodexBar's local `/cost` output is not intrinsically account-labelled. Account cost is therefore explicitly **inferred from deltas and account evidence**, not represented as exact provider billing.
- Local cost and OpenAI dashboard credits are different measures and remain separate.
- Historical ledger rows arriving for prior dates are retained as backfill but are never assigned to the currently active account.
- Providers without a stable account identifier are machine-scoped to avoid merging unrelated accounts called `default` or `Personal`.
- Disabling verbose entities does not delete registry entries already created while the option was enabled; they become unavailable and can be removed from the entity registry manually.

## Development

```bash
python -m pip install pytest ruff
ruff format --check .
ruff check .
pytest
```

The pure aggregation engine has no Home Assistant imports. The release suite contains 24 deterministic fixture and adversarial tests without requiring Home Assistant Core to start.
