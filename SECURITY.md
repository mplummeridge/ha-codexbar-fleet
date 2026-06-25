# Security

The integration executes no commands and opens no listener. It subscribes through Home Assistant's configured MQTT integration.

MQTT payloads can contain account emails, organisation names, machine hostnames, provider status details, and usage/cost metadata. Protect the broker with authentication and tailnet/LAN ACLs. Use a topic-restricted MQTT identity for every Mac agent where broker ACLs permit it.

Home Assistant state attributes intentionally expose account identity evidence for local diagnostics. Integration diagnostics redact account keys, labels, emails, aliases, and hostnames before download.

Report vulnerabilities privately to the repository maintainer rather than opening a public issue containing credentials or raw provider payloads.
