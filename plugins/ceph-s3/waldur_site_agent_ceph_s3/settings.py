"""How this offering reaches the cluster, and what that needs.

Users and accounts are managed through RGW's Admin Ops API on the gateway itself.
croit's own user management was the other route; it is gone, and croit remains
only as a statistics source for croit_usage (see ``croit_reporting``). An offering
still configured for croit is refused with the steps that move it, rather than
read as something it is not.
"""


RADOSGW = "radosgw"
FLAVOURS = (RADOSGW,)

# Top-level settings of the removed croit flavour. Its credentials now belong
# under croit_reporting, and only when croit_usage reads croit's statistics.
_CROIT_MANAGEMENT: tuple[str, ...] = ("api_url", "username", "password", "token")

MIGRATION_STEPS = (
    "1) create an RGW user with the caps 'users=*;buckets=*' (plus "
    "'usage=read' for rgw_usage and 'accounts=*' for account mode) and make Admin "
    "Ops (by default /admin on the S3 endpoint) reachable from the agent; "
    "2) set admin_access_key and admin_secret_key (and admin_path if renamed); "
    "3) if the offering reports with croit_usage, move api_url and token (or "
    "username and password) under croit_reporting, and set "
    "croit_reporting.bucket_state_file. Existing resources need no change"
)


def resolve_flavour(backend_settings: dict) -> str:
    """The flavour: radosgw, the only one.

    ``flavour`` may be left out. ``flavour: croit`` and croit's top-level
    credentials are an offering that has not been migrated yet, and are refused
    with the steps that migrate it.
    """
    flavour = str(backend_settings.get("flavour") or RADOSGW).lower()
    croit_keys = sorted(name for name in _CROIT_MANAGEMENT if backend_settings.get(name))
    if flavour == "croit" or croit_keys:
        found = f"flavour: {flavour}" if flavour == "croit" else ", ".join(croit_keys)
        raise ValueError(
            "ceph-s3 no longer manages users through croit; this offering is still "
            f"configured for it ({found}). Users are managed through RGW Admin Ops "
            f"now: {MIGRATION_STEPS}"
        )
    if flavour not in FLAVOURS:
        raise ValueError(
            f"Unknown ceph-s3 flavour {flavour!r}; the only one is {RADOSGW!r}"
        )
    return flavour


def _require(backend_settings: dict, name: str) -> None:
    if not backend_settings.get(name):
        raise ValueError(f"ceph-s3 requires {name!r} in backend_settings")


def validate_settings(backend_settings: dict, flavour: str = RADOSGW) -> None:
    """Fail on an incomplete configuration, before any request is made.

    Raises:
        ValueError: naming the setting that is missing or cannot work.
    """
    del flavour
    # Admin Ops hangs off the gateway, so this is both the tenants' address
    # and the agent's.
    _require(backend_settings, "s3_endpoint")
    _require(backend_settings, "admin_access_key")
    _require(backend_settings, "admin_secret_key")
    # Admin Ops applies a default storage class only as part of a placement rule
    # (rgw_rest_user.cc: default-storage-class is read into the target rule of
    # default-placement), so on its own it would be dropped without a word.
    if backend_settings.get("default_storage_class") and not backend_settings.get(
        "default_placement"
    ):
        raise ValueError(
            "'default_storage_class' needs 'default_placement': RGW applies a "
            "storage class only as part of a placement rule"
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
