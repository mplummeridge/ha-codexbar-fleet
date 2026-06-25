# Operations

## Healthy baseline

A healthy fleet normally has:

- `binary_sensor.codexbar_fleet_problem` off;
- every expected machine online;
- zero failed collector jobs;
- zero spool drops;
- cost attribution coverage available and near 100% after two cost cycles establish baselines.

Coverage is unavailable before any positive ledger delta is observed. This is not equivalent to 100%.

## Diagnosing missing account cost

1. Confirm the machine has a current account probe for the provider.
2. Confirm the Mac agent includes that provider in `active_account_probe_providers`.
3. Inspect ambiguous/unattributed token entities and the fleet problem attributes.
4. Check the machine collector-problem entity and heartbeat failed-job attributes.
5. Remember that the first ledger sample only creates a baseline.

## Common rejection reasons

- `missing_bracket`
- `ambiguous_bracket`
- `account_changed_within_cycle`
- `weak_identity`
- `invalid_bracket_order`
- `correlation_machine_mismatch`
- `account_interval_started_after_baseline`
- `observed_other_account_between_samples`
- `ambiguous_active_probe_between_samples`
- `missing_active_probe_between_samples`
- `poll_gap_exceeded`
- `historical_row_revision`

These remain in persisted attribution buckets and integration diagnostics.

## Resetting development state

The integration intentionally has no remote reset service in this release. To clear development state safely:

1. Remove the integration entry.
2. Stop Home Assistant.
3. Remove the matching `.storage/codexbar_fleet.<entry-id>` file only when a backup exists.
4. Start Home Assistant and add the integration again.

For normal operation, do not edit `.storage` manually.
