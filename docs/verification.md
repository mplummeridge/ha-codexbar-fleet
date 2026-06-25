# Verification

Release `0.1.0` was validated on 2026-06-25 with:

```text
ruff format --check .
ruff check .
pytest -q
python -m compileall -q custom_components tests
python scripts/validate_assets.py
```

The pure aggregation suite contains 24 adversarial tests covering identity resolution, cross-machine quota selection, dashboard deduplication, account rotation, out-of-order replay, retained/event races, missing and failed probes, correlation collisions, multiple currencies, historical backfill, counter resets, stale quota expiry and state persistence.

The integration APIs were checked against current Home Assistant Core source for config-entry `runtime_data`, MQTT `async_subscribe`, dynamic entity platforms and JSON `Store` persistence. This build environment did not run a complete Home Assistant Core instance, so the first installation should be treated as an integration smoke test rather than evidence of a full HA boot test.
