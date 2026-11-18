"""RadosGW tenant naming: a template rendered once per resource, at creation.

A tenant gives its users their own bucket namespace, and RGW addresses a tenanted
user as ``tenant$uid`` everywhere after creation. The tenant is therefore rendered
once, when the user is created, and carried in the backend id from then on: a slug
renamed or a template edited later cannot move an existing resource into another
namespace, because nothing recomputes it.
"""

import re
import string
from typing import Optional
from uuid import UUID

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset

# RGW accepts only letters, digits and underscores in a tenant name; a hyphen is
# refused. Lower case is enforced on top so that one Waldur object cannot map to
# two tenants depending on how its slug was cased.
_TENANT_RE = re.compile(r"^[a-z0-9_]{1,64}$")

TENANT_SEPARATOR = "$"


def _uuid_value(value: object) -> Optional[str]:
    # Hex without hyphens: the hyphenated form is not a valid tenant name.
    if isinstance(value, UUID):
        return value.hex
    if isinstance(value, str) and value:
        return UUID(value).hex
    return None


def _slug_value(value: object) -> Optional[str]:
    if isinstance(value, str) and value:
        return value.lower().replace("-", "_")
    return None


# Template field -> (resource attribute, how it is made tenant-safe).
_FIELDS = {
    "resource_slug": ("slug", _slug_value),
    "resource_uuid": ("uuid", _uuid_value),
    "customer_slug": ("customer_slug", _slug_value),
    "customer_uuid": ("customer_uuid", _uuid_value),
    "project_slug": ("project_slug", _slug_value),
    "project_uuid": ("project_uuid", _uuid_value),
    "offering_slug": ("offering_slug", _slug_value),
    "offering_uuid": ("offering_uuid", _uuid_value),
}
TEMPLATE_FIELDS = tuple(_FIELDS)


def validate_tenant_template(template: str) -> None:
    """Refuse a template that could not render a valid tenant, at construction.

    Only bare field names are accepted. str.format would otherwise resolve
    ``{customer_slug.__class__}`` and friends, and a typo in a field name would
    only surface on the first order.

    Format specs are allowed, so ``{customer_uuid:.8}`` truncates to eight
    characters.

    Raises:
        ValueError: naming the offending part of the template.
    """
    if not isinstance(template, str) or not template:
        raise ValueError("'tenant_template' must be a non-empty string")
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as e:
        raise ValueError(f"'tenant_template' {template!r} is malformed: {e}") from e

    literal = ""
    for text, field, _spec, conversion in parsed:
        literal += text
        if field is None:
            continue
        if field not in _FIELDS:
            raise ValueError(
                f"'tenant_template' uses unknown field {{{field}}}; "
                f"expected one of {', '.join(TEMPLATE_FIELDS)}"
            )
        if conversion:
            raise ValueError(
                f"'tenant_template' field {{{field}!{conversion}}} must not use a conversion"
            )
    # The literal parts end up in the tenant verbatim, so they are held to the
    # same alphabet now rather than failing every order later.
    if literal and not re.fullmatch(r"[a-z0-9_]+", literal):
        raise ValueError(
            f"'tenant_template' {template!r} contains characters RGW refuses in a "
            "tenant name; only lower-case letters, digits and '_' are allowed"
        )


def render_tenant(template: str, waldur_resource: WaldurResource) -> str:
    """Render the tenant for one resource.

    Raises:
        ValueError: when a field the template uses is absent on the resource, or
            the result is not a valid RGW tenant name.
    """
    values = {}
    for _text, field, _spec, _conversion in string.Formatter().parse(template):
        if field is None or field in values:
            continue
        attribute, normalise = _FIELDS[field]
        raw = getattr(waldur_resource, attribute, None)
        value = None if isinstance(raw, Unset) else normalise(raw)
        if not value:
            raise ValueError(
                f"Cannot render 'tenant_template' {template!r}: the resource has no "
                f"{attribute}"
            )
        values[field] = value

    tenant = template.format(**values)
    if not _TENANT_RE.match(tenant):
        raise ValueError(
            f"'tenant_template' {template!r} rendered {tenant!r}, which is not a "
            "valid RGW tenant name (1-64 of lower-case letters, digits and '_')"
        )
    return tenant


# A fixed tenant from the older ``default_tenant`` setting, passed to croit as is
# before templates existed. croit's own rule, which admits upper case -- narrower
# would refuse a configuration that has been working.
_FIXED_TENANT_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")


def validate_fixed_tenant(tenant: str) -> None:
    """Refuse a ``default_tenant`` RGW could not hold.

    Raises:
        ValueError: naming the value.
    """
    if not _FIXED_TENANT_RE.match(tenant):
        raise ValueError(
            f"'default_tenant' {tenant!r} is not a valid RGW tenant: 1-64 letters, "
            "digits or '_'"
        )


def qualify(tenant: Optional[str], uid: str) -> str:
    """The id RGW knows a user by: ``tenant$uid``, or the bare uid when untenanted."""
    return f"{tenant}{TENANT_SEPARATOR}{uid}" if tenant else uid


def split(uid: str) -> tuple:
    """Undo qualify: ``(tenant, bare uid)``, the tenant empty for an untenanted user."""
    tenant, separator, bare = uid.rpartition(TENANT_SEPARATOR)
    return (tenant, bare) if separator else ("", uid)
