"""Account identity resolution for CodexBar observations."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from .models import AccountIdentity

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = " ".join(value.strip().split())
    return value or None


def _normalise_identity(value: str, kind: str) -> str:
    value = value.strip()
    if kind == "email" or _EMAIL_RE.match(value):
        return value.casefold()
    return " ".join(value.casefold().split())


def account_hash(account_key: str, length: int = 12) -> str:
    """Return a stable, non-PII identifier for entity/device unique IDs."""
    return hashlib.sha256(account_key.encode("utf-8")).hexdigest()[:length]


def derive_account_identity(machine_id: str, row: dict[str, Any]) -> AccountIdentity:
    """Derive the strongest stable identity available in a usage row.

    The display precedence preserves CodexBar's explicit ``row.account`` label, but
    canonicalisation prefers a real email when one is available so account labels
    can change without splitting one account into several Home Assistant devices.
    """

    usage = row.get("usage") if isinstance(row.get("usage"), dict) else {}
    identity = usage.get("identity") if isinstance(usage.get("identity"), dict) else {}
    dashboard = row.get("openaiDashboard") if isinstance(row.get("openaiDashboard"), dict) else {}

    provider = _clean(row.get("provider")) or _clean(identity.get("providerID")) or "unknown"
    provider = provider.casefold()

    explicit_label = _clean(row.get("account"))
    email = (
        _clean(identity.get("accountEmail"))
        or _clean(usage.get("accountEmail"))
        or _clean(dashboard.get("signedInEmail"))
    )
    organisation = _clean(identity.get("accountOrganization")) or _clean(
        usage.get("accountOrganization")
    )

    aliases = tuple(
        dict.fromkeys(value for value in (explicit_label, email, organisation) if value is not None)
    )

    if email:
        kind = "email" if _EMAIL_RE.match(email) else "provider_identifier"
        canonical = _normalise_identity(email, kind)
        label = explicit_label or email
        return AccountIdentity(
            provider=provider,
            account_key=f"{provider}:{kind}:{canonical}",
            label=label,
            confidence="exact" if kind == "email" else "provider_identifier",
            identity_kind=kind,
            identity_value=canonical,
            aliases=aliases,
        )

    # An arbitrary configured label is not globally unique across machines. Scope
    # it locally rather than risk merging unrelated accounts named "Personal".
    if explicit_label:
        kind = "machine_label"
        canonical = _normalise_identity(explicit_label, kind)
        local_value = f"{machine_id}:{canonical}"
        return AccountIdentity(
            provider=provider,
            account_key=f"{provider}:machine-label:{local_value}",
            label=explicit_label,
            confidence="machine_scoped",
            identity_kind=kind,
            identity_value=local_value,
            aliases=aliases,
            machine_scoped=True,
        )

    if organisation:
        kind = "machine_organisation"
        canonical = _normalise_identity(organisation, kind)
        local_value = f"{machine_id}:{canonical}"
        return AccountIdentity(
            provider=provider,
            account_key=f"{provider}:machine-organisation:{local_value}",
            label=organisation,
            confidence="machine_scoped",
            identity_kind=kind,
            identity_value=local_value,
            aliases=aliases,
            machine_scoped=True,
        )

    local_value = f"{machine_id}:default"
    return AccountIdentity(
        provider=provider,
        account_key=f"{provider}:machine:{local_value}",
        label=f"{provider} on {machine_id}",
        confidence="machine_scoped",
        identity_kind="machine_default",
        identity_value=local_value,
        aliases=(),
        machine_scoped=True,
    )


def identities_from_payload(
    machine_id: str,
    payload: Any,
    provider_hint: str | None = None,
) -> dict[str, list[AccountIdentity]]:
    """Extract identities grouped by provider from a CodexBar usage payload."""
    rows = payload if isinstance(payload, list) else [payload]
    result: dict[str, list[AccountIdentity]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        # CodexBar represents provider failures as payload rows. They identify
        # the provider, not an active account; treating one as a default account
        # would create a false account transition during a transient failure.
        if item.get("error") and not isinstance(item.get("usage"), dict):
            continue
        if not item.get("provider") and not isinstance(
            (item.get("usage") or {}).get("identity")
            if isinstance(item.get("usage"), dict)
            else None,
            dict,
        ):
            continue
        identity = derive_account_identity(machine_id, item)
        if provider_hint and identity.provider != provider_hint.casefold():
            continue
        provider_identities = result.setdefault(identity.provider, [])
        if all(existing.account_key != identity.account_key for existing in provider_identities):
            provider_identities.append(identity)
    return result
