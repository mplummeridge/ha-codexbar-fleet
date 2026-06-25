# CodexBar Fleet for Home Assistant

A native Home Assistant custom integration for aggregating CodexBar telemetry from multiple Macs through MQTT.

This repo contains the HACS-installable Home Assistant integration only. The macOS collector lives in [`mplummeridge/codexbar-mqtt`](https://github.com/mplummeridge/codexbar-mqtt).

## What it does

CodexBar Fleet treats each Mac as an observation source and Home Assistant as the fleet-level aggregator. It deduplicates account-global quota/dashboard data, compiles machine/provider account-observation timelines, and attributes machine-local token/cost deltas only when confidence is sufficient.

Core semantics:

- Account-global quota/dashboard data: newest valid account observation wins; never summed across machines.
- Local costs: differenced by machine, provider, day, model, and currency.
- Account attribution: accepted only when correlated before/cost/after probes agree and account evidence is continuous.
- Ambiguous, unattributed, historical-backfill, counter-reset, and long-gap deltas remain separate.
- Machines and accounts are discovered dynamically.

## HACS install

1. Ensure the Home Assistant MQTT integration is configured.
2. Add this repository to HACS as a custom repository with category **Integration**.
3. Install **CodexBar Fleet** from HACS.
4. Restart Home Assistant.
5. Run at least one Mac agent. A retained discovery beacon should make **CodexBar Fleet discovered** appear under Settings → Devices & services.

Custom repository URL:

```text
https://github.com/mplummeridge/ha-codexbar-fleet
```

The integration also supports manual fallback setup if MQTT discovery beacons are blocked by ACLs.

## Mac agent

Install the collector from:

```text
https://github.com/mplummeridge/codexbar-mqtt
```

The agent publishes to:

```text
codexbar/discovery/v1/#
codexbar/v1/#
```

## Repository layout

```text
custom_components/codexbar_fleet/  # HA integration
docs/                              # design/ops notes
.github/workflows/                 # validation and optional release packaging
hacs.json                          # HACS metadata
```

## Development

This is a custom integration, not a normal MQTT Discovery entity pack. That is intentional: the aggregator must consume the evidence stream centrally to deduplicate account-global quota data and perform confidence-aware cost attribution.

## License

No open-source license has been selected yet. Add a `LICENSE` file before publishing broadly if you want others to reuse the code.
