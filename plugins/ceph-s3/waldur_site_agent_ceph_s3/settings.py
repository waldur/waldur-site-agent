"""Which Ceph management API this offering talks to, and what that needs.

The two flavours reach the same RGW concepts by different routes: croit through
its management application, radosgw through Admin Ops on the gateway itself.
They share no credentials and almost no settings, so a configuration that mixes
them is a mistake rather than a merge.
"""

from typing import Tuple

CROIT = "croit"
RADOSGW = "radosgw"
FLAVOURS = (CROIT, RADOSGW)

# Settings that belong to exactly one flavour. Ignoring a stray key would let an
# operator read a half-migrated offering as configured when it is not: the agent
# would run, authenticate somewhere else entirely, and only the provisioning
# failure would say so.
_CROIT_ONLY: Tuple[str, ...] = ("api_url", "username", "password", "token")
_RADOSGW_ONLY: Tuple[str, ...] = ("admin_access_key", "admin_secret_key", "admin_path")

# Settings the radosgw flavour has no implementation for. Tenanted uids are
# "tenant$uid" and the client's path validator refuses "$" (widening it is
# security-relevant and needs its own review); Admin Ops user-create takes a
# placement but no storage class. Rejected rather than ignored, for the same
# reason as the foreign settings above: a dropped setting reads as configured.
_UNIMPLEMENTED_ON_RADOSGW: Tuple[str, ...] = ("default_tenant", "default_storage_class")


def resolve_flavour(backend_settings: dict) -> str:
    """Read the configured flavour, defaulting to croit.

    The default is croit rather than the newer flavour on purpose: every offering
    deployed before this setting existed is a croit one, and they carry no
    ``flavour`` key.
    """
    flavour = str(backend_settings.get("flavour") or CROIT).lower()
    if flavour not in FLAVOURS:
        raise ValueError(
            f"Unknown ceph-s3 flavour {flavour!r}; expected one of {', '.join(FLAVOURS)}"
        )
    return flavour


def _reject_foreign_settings(
    backend_settings: dict, foreign: Tuple[str, ...], flavour: str
) -> None:
    present = [name for name in foreign if backend_settings.get(name)]
    if present:
        raise ValueError(
            f"The {flavour} flavour does not use {', '.join(sorted(present))}; "
            "remove them from backend_settings"
        )


def _require(backend_settings: dict, name: str, flavour: str) -> None:
    if not backend_settings.get(name):
        raise ValueError(
            f"The {flavour} flavour requires {name!r} in backend_settings"
        )


def validate_settings(backend_settings: dict, flavour: str) -> None:
    """Fail on an incomplete or mixed configuration, before any request is made.

    Raises:
        ValueError: naming every setting that is missing or does not belong.
    """
    if flavour == RADOSGW:
        _reject_foreign_settings(backend_settings, _CROIT_ONLY, RADOSGW)
        _reject_foreign_settings(backend_settings, _UNIMPLEMENTED_ON_RADOSGW, RADOSGW)
        # Admin Ops hangs off the gateway, so this is both the tenants' address
        # and the agent's.
        _require(backend_settings, "s3_endpoint", RADOSGW)
        _require(backend_settings, "admin_access_key", RADOSGW)
        _require(backend_settings, "admin_secret_key", RADOSGW)
        return

    _reject_foreign_settings(backend_settings, _RADOSGW_ONLY, CROIT)
    if "token" not in backend_settings and (
        "username" not in backend_settings or "password" not in backend_settings
    ):
        raise ValueError(
            "Either 'token' or both 'username' and 'password' must be provided"
        )


def validate_components(backend_components: dict) -> None:
    """Refuse a storage component whose ``unit_factor`` was never configured.

    ``unit_factor`` carries a default of 1.0 in the core config model and every
    component is normalised through it, so an omitted value reaches the plugin as
    a real one rather than as an absence — there is nothing here that can tell the
    two apart. At 1 the quota path sends an ordered 5 GB ceiling as 5 *bytes* and
    the metering path divides by 1, reporting byte-days against a GB-day price.
    Neither surfaces as an error, so the offering is refused at construction.

    Raises:
        ValueError: naming the component and the factor it needs.
    """
    for name, config in backend_components.items():
        if config.get("backend_name") != "storage":
            continue
        unit_factor = config.get("unit_factor", 1)
        if not isinstance(unit_factor, (int, float)) or unit_factor <= 1:
            raise ValueError(
                f"The storage component {name!r} needs a 'unit_factor' above 1: "
                "it converts the ordered ceiling into bytes and the measured "
                "bytes back into billed units, so at 1 the cap and the invoice "
                f"are both wrong by the size of a byte. Got {unit_factor!r}; use "
                "1000000000 for decimal GB"
            )
