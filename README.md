# CodexBar Fleet for Home Assistant

A native Home Assistant custom integration for aggregating CodexBar telemetry from multiple Macs through MQTT.

This repo contains:

- `custom_components/codexbar_fleet/` — HACS-installable Home Assistant integration.
- `dist/` — macOS `codexbar-mqtt` agent release archives used by each Mac.
- `blueprints/` — optional Home Assistant automation blueprint.
- `dashboards/` — example dashboard notes.
- `docs/` — architecture, entity model, operations, and verification notes.

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

The integration also supports manual fallback setup if MQTT discovery beacons are blocked by ACLs.

## Mac agent

Download the matching archive from `dist/` or from GitHub Releases if you publish these files as release assets:

```bash
tar -xzf dist/codexbar-mqtt-0.2.0-darwin-arm64.tar.gz
cd codexbar-mqtt-0.2.0-darwin-arm64

MQTT_BROKER='mqtt://homeassistant.local:1883' \
MQTT_USERNAME='codexbar' \
MACHINE_ID='macbook-m4' \
./scripts/install.sh
```

Then set the password:

```bash
printf '%s' 'MQTT_PASSWORD' > \
  "$HOME/Library/Application Support/codexbar-mqtt/mqtt-password"

"$HOME/Library/Application Support/codexbar-mqtt/bin/codexbar-mqtt" \
  doctor \
  --config "$HOME/Library/Application Support/codexbar-mqtt/config.json"
```

MQTT ACLs must allow agent writes to:

```text
codexbar/discovery/v1/#
codexbar/v1/#
```

## Repository layout

```text
custom_components/codexbar_fleet/  # HA integration
dist/                              # macOS agent release artifacts
blueprints/automation/             # optional automations
dashboards/                        # example dashboard notes
docs/                              # deeper design/ops docs
```

## Development

This is a custom integration, not a normal MQTT Discovery entity pack. That is intentional: the aggregator must consume the evidence stream centrally to deduplicate account-global quota data and perform confidence-aware cost attribution.

## License

No open-source license has been selected in this generated package. Add a `LICENSE` file before publishing publicly if you want others to reuse the code.
