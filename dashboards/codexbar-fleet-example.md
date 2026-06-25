# Dashboard notes

Account entity IDs contain stable hashes and therefore differ by installation. Build the dashboard after the integration has discovered the fleet.

A useful first section contains:

- Fleet problem
- Machines online / stale
- Attribution coverage today
- Ambiguous and unattributed tokens today

For each provider account, add:

- primary and secondary remaining percentages;
- reset timestamps;
- low-quota binary sensors;
- dashboard credits for today and 30 days;
- attributed tokens and per-currency cost for today and 30 days;
- active-machine count.

For each machine, add online, collector problem, current account per provider, spool drops, and 30-day local token/cost gauges.
