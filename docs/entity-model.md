# Entity model

## Fleet device

Core entities include machine/account counts, online/stale counts, pending cycles, last event, attribution coverage for today/7d/30d, rejected token/cost categories, and a fleet problem binary sensor.

The problem sensor turns on for:

- missing expected machines;
- stale machines;
- current Mac-agent collection failures;
- ambiguous current-day cost deltas;
- attribution coverage below policy once any attributable data exists.

## Machine devices

Each machine exposes:

- MQTT/agent connectivity and staleness;
- heartbeat age and uptime;
- spool messages/bytes/dropped count;
- agent and CodexBar versions;
- current confirmed provider account, candidate, switch count, and last switch;
- current collector failure state;
- local cost horizon token and monetary gauges;
- optional per-model token entities.

Machine-local cost entities do not claim an account association.

## Account devices

Each canonical account exposes:

- active online machines and machines that last observed the account;
- identity confidence and last seen time;
- one remaining/used/reset/low group per standard, extra, and dashboard rate window;
- credits, reset credits, and provider plan cap fields where present;
- provider status where collected by CLI status enrichment;
- account-global dashboard credit totals by rolling horizon;
- inferred local tokens and costs by rolling horizon and currency.

A quota whose reset timestamp has passed without a newer snapshot becomes unavailable rather than continuing to alarm on stale usage.

## Recorder guidance

The integration defaults noisy diagnostics and per-model/service entities off. For very large fleets, consider excluding these diagnostic entities from Recorder if enabled:

```yaml
recorder:
  exclude:
    entity_globs:
      - sensor.*_event_age
      - sensor.*_spool_*
      - sensor.*_model_*
```
