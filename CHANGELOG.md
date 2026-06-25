# Changelog

## 0.2.0

- Add native Home Assistant MQTT integration discovery using a retained well-known agent beacon.
- Discover custom topic prefixes and multiple fleets without manual input.
- Auto-discover machines and accounts; expected machine IDs are now optional pinned membership.
- Validate fleet ID, topic prefix, machine ID and discovery contract against each other.
- Migrate 0.1 entries to prefix-derived stable fleet unique IDs.
- Scope runtime MQTT subscriptions to the five contract branches instead of `<prefix>/#`.
- Add runtime translation assets for custom-component config flows.

## 0.1.0

- Initial confidence-aware CodexBar fleet aggregation integration.
