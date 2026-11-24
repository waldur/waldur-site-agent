"""RadosGW tenant naming: one tenant per resource, rendered once, at creation.

A tenant gives its users their own bucket namespace. Resources sharing one can
squat each other's bucket names, learn them from "bucket already exists", and
grant each other access without a ``tenant:`` prefix; so every resource gets a
tenant of its own, and the tenant is what identifies the resource from then on.

The tenant is rendered from a template once, when the resource is created, and
becomes its backend id: a slug renamed or a template edited later cannot move an
existing resource into another namespace, because nothing recomputes it.
"""

import re
import string
import unicodedata
from uuid import UUID

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset

# RGW accepts letters, digits and underscores in a tenant name, at any length
# (rgw_validate_tenant_name). Lower case is enforced on top so that one Waldur
# object cannot map to two tenants depending on how its name was cased, and the
# length is capped so the name stays usable in every place RGW echoes it.
_TENANT_RE = re.compile(r"^[a-z0-9_]{1,64}$")
TENANT_MAX_LENGTH = 64

# Every resource's tenant holds one user Waldur creates, always under this name:
# the tenant already identifies the resource. In account mode the RGW account is
# named after it too; account names only need to be unique within a tenant.
ROOT_UID = "root"

TENANT_SEPARATOR = "$"

# A truncated UUID shorter than this stops being a credible uniqueness source.
_MIN_UUID_PREFIX = 8

# At most 55 characters: every field is cut, the abbreviation's slug fallback too.
DEFAULT_TENANT_TEMPLATE = (
    "{customer_abbreviation:.12}_{project_name:.12}_{resource_name:.20}_{resource_uuid:.8}"
)


def _uuid_value(value: object) -> str | None:
    # Hex without hyphens: the hyphenated form is not a valid tenant name.
    if isinstance(value, UUID):
        return value.hex
    if isinstance(value, str) and value:
        return UUID(value).hex
    return None


def _name_value(value: object) -> str | None:
    """Fold free text into RGW's tenant alphabet.

    Accents are dropped rather than the letters they sit on ("Infosüsteemi" reads
    "infosusteemi"), and every run of anything else becomes one underscore.
    """
    if not isinstance(value, str) or not value:
        return None
    ascii_text = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    folded = re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")
    return folded or None


# Template field -> resource attributes tried in order, and how each is made
# tenant-safe. A name that folds to nothing (a name in a script with no ASCII
# letters, say) falls back to the object's slug; an empty abbreviation, which
# most organisations have, falls back to the customer's slug.
_FIELDS = {
    "resource_slug": (("slug",), _name_value),
    "resource_uuid": (("uuid",), _uuid_value),
    "resource_name": (("name", "slug"), _name_value),
    "customer_slug": (("customer_slug",), _name_value),
    "customer_uuid": (("customer_uuid",), _uuid_value),
    "customer_name": (("customer_name", "customer_slug"), _name_value),
    "customer_abbreviation": (
        ("customer_abbreviation", "customer_slug"),
        _name_value,
    ),
    "project_slug": (("project_slug",), _name_value),
    "project_uuid": (("project_uuid",), _uuid_value),
    "project_name": (("project_name", "project_slug"), _name_value),
    "offering_slug": (("offering_slug",), _name_value),
    "offering_uuid": (("offering_uuid",), _uuid_value),
}
TEMPLATE_FIELDS = tuple(_FIELDS)

# The fields that tell one resource from another; a template needs one of them.
_RESOURCE_UNIQUE_FIELDS = ("resource_uuid", "resource_slug")

_TRUNCATION_RE = re.compile(r"^\.([1-9][0-9]*)$")


def _truncation(field: str, spec: str) -> int | None:
    """The length a ``{field:.N}`` spec cuts to, or None for no spec."""
    if not spec:
        return None
    match = _TRUNCATION_RE.match(spec)
    if not match:
        raise ValueError(
            f"'tenant_template' field {{{field}:{spec}}} has an unsupported format "
            "spec; only truncation, like {resource_uuid:.8}, is allowed"
        )
    return int(match.group(1))


def validate_tenant_template(template: str) -> None:
    """Refuse a template that could not render a valid per-resource tenant.

    Only bare field names are accepted. str.format would otherwise resolve
    ``{customer_slug.__class__}`` and friends, and a typo in a field name would
    only surface on the first order.

    The template must name the resource itself, through ``{resource_uuid}`` or
    ``{resource_slug}``: anything rendered from the customer, project or
    offering alone is a tenant every resource of that scope would share.

    Raises:
        ValueError: naming the offending part of the template.
    """
    if not isinstance(template, str) or not template:
        msg = "'tenant_template' must be a non-empty string"
        raise ValueError(msg)
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as e:
        raise ValueError(f"'tenant_template' {template!r} is malformed: {e}") from e

    literal = ""
    unique = False
    for text, field, spec, conversion in parsed:
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
        length = _truncation(field, spec or "")
        if field == "resource_uuid":
            if length is not None and length < _MIN_UUID_PREFIX:
                raise ValueError(
                    f"'tenant_template' truncates {{resource_uuid}} to {length} "
                    f"characters; keep at least {_MIN_UUID_PREFIX} so it stays unique"
                )
            unique = True
        elif field == "resource_slug":
            if length is not None:
                msg = (
                    "'tenant_template' must not truncate {resource_slug}: a cut slug "
                    "no longer tells resources apart"
                )
                raise ValueError(msg)
            unique = True
    # The literal parts end up in the tenant verbatim, so they are held to the
    # same alphabet now rather than failing every order later.
    if literal and not re.fullmatch(r"[a-z0-9_]+", literal):
        raise ValueError(
            f"'tenant_template' {template!r} contains characters RGW refuses in a "
            "tenant name; only lower-case letters, digits and '_' are allowed"
        )
    if not unique:
        raise ValueError(
            f"'tenant_template' {template!r} does not name the resource: every "
            "resource needs a tenant of its own, so the template must contain "
            "{resource_uuid} or {resource_slug}"
        )


def _field_value(field: str, waldur_resource: WaldurResource) -> str | None:
    attributes, normalise = _FIELDS[field]
    # A field newer than the SDK this agent was built with arrives among the
    # model's additional_properties rather than as an attribute.
    extra = getattr(waldur_resource, "additional_properties", None)
    extra = extra if isinstance(extra, dict) else {}
    for attribute in attributes:
        raw = getattr(waldur_resource, attribute, None)
        if raw is None or isinstance(raw, Unset):
            raw = extra.get(attribute)
        value = None if isinstance(raw, Unset) else normalise(raw)
        if value:
            return value
    return None


def render_tenant(
    template: str, waldur_resource: WaldurResource, max_length: int = TENANT_MAX_LENGTH
) -> str:
    """Render the tenant for one resource.

    A truncated value loses any underscore it ends on, so a cut never leaves
    ``__`` behind in the middle of the name.

    Raises:
        ValueError: when a field the template uses is absent on the resource, or
            the result is not a valid RGW tenant name of at most ``max_length``.
    """
    rendered = ""
    for text, field, spec, _conversion in string.Formatter().parse(template):
        rendered += text
        if field is None:
            continue
        value = _field_value(field, waldur_resource)
        if not value:
            raise ValueError(
                f"Cannot render 'tenant_template' {template!r}: the resource has no "
                f"{' or '.join(_FIELDS[field][0])}"
            )
        length = _truncation(field, spec or "")
        if length is not None:
            value = value[:length].rstrip("_")
        rendered += value

    tenant = rendered
    if len(tenant) > max_length or not _TENANT_RE.match(tenant):
        raise ValueError(
            f"'tenant_template' {template!r} rendered {tenant!r}, which is not a "
            f"valid RGW tenant name here (1-{max_length} of lower-case letters, "
            "digits and '_'); truncate its fields, e.g. {resource_name:.20}"
        )
    return tenant


def is_tenant_name(value: str) -> bool:
    """Whether a backend id has the shape of a tenant this module renders."""
    return bool(_TENANT_RE.match(value))


def backend_id_of(uid: str) -> str:
    """The backend id of the resource an RGW uid belongs to.

    This is the reverse of the lookup a backend id goes through. A tenant's root
    user is the tenant; any other uid is its own backend id, as resources from
    before the tenant scheme are.
    """
    tenant, bare = split(uid)
    return tenant if bare == ROOT_UID and is_tenant_name(tenant) else uid


def qualify(tenant: str | None, uid: str) -> str:
    """The id RGW knows a user by: ``tenant$uid``, or the bare uid when untenanted."""
    return f"{tenant}{TENANT_SEPARATOR}{uid}" if tenant else uid


def split(uid: str) -> tuple:
    """Undo qualify: ``(tenant, bare uid)``, the tenant empty for an untenanted user."""
    tenant, separator, bare = uid.rpartition(TENANT_SEPARATOR)
    return (tenant, bare) if separator else ("", uid)
