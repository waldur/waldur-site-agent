"""Ceph S3 storage backend for Waldur Site Agent.

Manages one RGW tenant per resource through RGW's Admin Ops API: its user (or, in
account mode, its RGW account), quotas, suspension and access keys. Usage comes
from the reporting backends that build on this class: croit's statistics
(``reporting.CroitUsageBackend``) or RGW itself (``rgw_reporting.RgwUsageBackend``).

- S3 credentials are managed as resource API keys (encrypted in Waldur, never in
  metadata)
- Ceilings are applied as user and bucket quotas, or account quotas
"""

import logging
import re
import secrets
import string
from collections.abc import Iterator
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from uuid import UUID

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset
from waldur_site_agent.backend import (
    DEFAULT_RESOURCE_KEY_COUNT,
    BackendType,
    backends,
    structures,
)
from waldur_site_agent.backend.exceptions import DuplicateResourceError

from .clients.base import BaseS3AdminClient
from .clients.iam import AccountIamClient
from .clients.radosgw import RadosGWClient
from .exceptions import (
    CephS3AccountExistsError,
    CephS3AccountNotFoundError,
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3Error,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)
from .settings import (
    resolve_flavour,
    validate_components,
    validate_settings,
)
from .tenant import (
    DEFAULT_TENANT_TEMPLATE,
    ROOT_UID,
    is_tenant_name,
    qualify,
    render_tenant,
    split,
    validate_tenant_template,
)

logger = logging.getLogger(__name__)

# S3 access keys are 20 uppercase alphanumerics and secrets 40 characters, matching
# the RadosGW/AWS convention that S3 client libraries and users expect.
_ACCESS_KEY_LENGTH = 20
_SECRET_KEY_LENGTH = 40
_ACCESS_KEY_ALPHABET = string.ascii_uppercase + string.digits
# Two keys is what makes rotation zero-downtime: one stays usable while the other is
# replaced. The cap lives here rather than in mastermind because the sensible maximum
# is backend-specific — mastermind deliberately places no global limit. The default is
# the core's, because common.utils reconciles the count against what Waldur already
# holds and so has to agree with this module on the target.
_DEFAULT_KEY_COUNT = DEFAULT_RESOURCE_KEY_COUNT
_MAX_KEY_COUNT = 2

# The core defaults allocation_prefix to "", which would put a uid built from a
# consumer-chosen name straight into the cluster's global namespace. Waldur-managed
# users get their own namespace unless an operator names one.
_DEFAULT_UID_PREFIX = "waldur-"

# Ownership marker, written to the S3 user's email at creation. "Does this uid
# exist?" is the question both a foreign account and our own half-finished create
# answer yes to; this is what tells them apart. It goes in email rather than the
# display name because nothing outside this agent writes email, whereas the display
# name carries the consumer's own resource name. ".invalid" is reserved by RFC 2606,
# so the value can never collide with a deliverable address.
_STAMP_PREFIX = "waldur-"
_STAMP_SUFFIX = "@invalid"


def _ownership_stamp(resource_uuid: object) -> str:
    """The stamp of a user (or, in account mode, an account) provisioned for a resource.

    It reads ``waldur-<resource uuid hex>@invalid``.
    Short enough for an email local part (39 of 64 characters). One resource has
    one user, so no uid is needed to keep stamps apart -- and since both backends
    refuse an email another user holds, a second user for the same resource (a
    retry rendering a different tenant after a rename) fails loudly instead of
    leaving the first one orphaned.
    """
    return f"{_STAMP_PREFIX}{UUID(str(resource_uuid)).hex}{_STAMP_SUFFIX}"


def _legacy_stamp(resource_uuid: object, uid: str) -> str:
    """The stamp users created before the short form carry, uid included."""
    # A tenanted uid carries "$", which has no business in an email address.
    local_uid = uid.replace("$", ".")
    return f"{_STAMP_PREFIX}{resource_uuid}-{local_uid}{_STAMP_SUFFIX}"


def _is_stamped_for(email: object, resource_uuid: object, uid: str) -> bool:
    """Whether a user or account carries this resource's stamp, in either form."""
    return email in (_ownership_stamp(resource_uuid), _legacy_stamp(resource_uuid, uid))


def _is_waldur_stamp(email: object) -> bool:
    """Whether a user carries a stamp this agent wrote, for any resource.

    Used where the resource is not in scope — key minting is addressed by backend
    id alone — so it answers the weaker "Waldur made this" rather than "made for
    this resource". That still excludes every account the cluster already had.
    """
    return (
        isinstance(email, str)
        and email.startswith(_STAMP_PREFIX)
        and email.endswith(_STAMP_SUFFIX)
    )

# An account user's display name is its IAM UserName, which RGW holds to the IAM
# rules: anything else is refused with a bare 400 InvalidArgument.
_IAM_USER_NAME_RE = re.compile(r"^[\w+=,.@-]{1,64}$")

# Two places is what the reporter's "%.2f" formatting preserves on the wire.
_USAGE_PRECISION = Decimal("0.01")

# A series needs two samples before it covers any time.
_MIN_SERIES_POINTS = 2

_SECONDS_PER_DAY = Decimal(86400)


def _to_usage_units(value: Decimal) -> float:
    """Round a usage figure to the precision Waldur stores and hand it over as a float.

    The Decimal stays inside the arithmetic, where it keeps partial units from
    rounding away; every backend hands the reporter plain floats (see moab), and
    a two-place value survives the float round-trip intact.

    A non-finite figure is refused rather than reported: it formats onto the wire
    as "nan", and it passes both the idempotency comparison and the anomaly check
    on the way, so nothing downstream would catch it.
    """
    if not value.is_finite():
        msg = f"Refusing to report a non-finite usage figure: {value}"
        raise ValueError(msg)
    return float(value.quantize(_USAGE_PRECISION, rounding=ROUND_HALF_UP))


def _is_measurable(datapoints: list) -> bool:
    """Whether a series can yield a usage figure at all.

    Fewer than two points spans no interval, a non-advancing window spans nothing,
    and an all-null series is what croit returns for a window predating the S3 user.
    All three integrate to exactly zero, which is indistinguishable from a measured
    zero by the time it reaches the reporter — and a zero overwrites the period's
    accrued total.
    """
    if len(datapoints) < _MIN_SERIES_POINTS:
        return False
    if datapoints[-1]["t"] <= datapoints[0]["t"]:
        return False
    return any(point.get("v") is not None for point in datapoints)


def _as_attribute_dict(waldur_resource: WaldurResource) -> dict:
    """Read a resource's order attributes as a plain dict.

    The generated client models ``attributes`` as an attrs class that supports
    ``[]`` but not ``.get()``, and is truthy even when empty — so treating it as a
    mapping raises, and testing it for emptiness lies.
    """
    raw = getattr(waldur_resource, "attributes", None)
    if raw is None:
        return {}
    to_dict = getattr(raw, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    return raw or {}


def _as_options_dict(waldur_resource: WaldurResource) -> dict:
    """Read a resource's current options as a plain dict.

    Same shape problem as the attributes: the generated client gives an attrs
    class, None, or UNSET depending on whether the offering defines options.
    """
    raw = getattr(waldur_resource, "options", None)
    to_dict = getattr(raw, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    return raw if isinstance(raw, dict) else {}


def _first_limit(resource_options: dict, *keys: str) -> int:
    """Read the first ceiling present among ``keys``, defaulting to 0.

    Resources ordered before the rename carry the old key, and their quotas must
    survive a restore or a re-provision, so both spellings are accepted. The
    first key that is present wins even when it is empty: an update that clears
    the ceiling must not fall back to a legacy value left in the attributes.
    """
    for key in keys:
        if key in resource_options:
            value = resource_options[key]
            return int(value) if value else 0
    return 0


def _generate_key_pair() -> dict:
    """Mint one access/secret pair.

    RGW is told to take both halves from the caller (users are created with
    generate-key=False), so the agent is the source.
    """
    access_key = "".join(
        secrets.choice(_ACCESS_KEY_ALPHABET) for _ in range(_ACCESS_KEY_LENGTH)
    )
    return {
        "client_id": access_key,
        "api_key": secrets.token_urlsafe(_SECRET_KEY_LENGTH)[:_SECRET_KEY_LENGTH],
    }


class CephS3Backend(backends.BaseBackend):
    """Ceph S3 storage backend: RGW tenants, users, accounts, quotas and keys."""

    # The agent mints each access/secret pair, applies it to the S3 user, then reports
    # it to Waldur encrypted. client_id holds the access key, which moves on rotation.
    supports_resource_api_keys = True

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Initialize backend with settings and component configuration.

        Args:
            backend_settings: Backend configuration including API credentials
            backend_components: Component definitions for accounting
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = BackendType.CEPH_S3.value

        # RGW Admin Ops; an offering still configured for croit management is
        # refused here with the steps that migrate it.
        self.flavour = resolve_flavour(backend_settings)
        validate_settings(backend_settings, self.flavour)
        validate_components(backend_components)

        # The gateway tenants connect to, and the one Admin Ops is reached on.
        s3_endpoint = backend_settings.get("s3_endpoint")
        if not s3_endpoint:
            msg = "Ceph S3 backend requires 's3_endpoint' in backend_settings"
            raise ValueError(msg)
        self.s3_endpoint = str(s3_endpoint).rstrip("/")

        # S3 clients refuse to sign a request without a region, so one has to be
        # published. "default" is Ceph's out-of-the-box zonegroup name, correct until
        # an operator names theirs -- at which point they set it here.
        self.s3_region = str(backend_settings.get("s3_region") or "default")

        self.client: BaseS3AdminClient = RadosGWClient(
            # Admin Ops lives on the gateway itself, not on a separate host.
            endpoint=self.s3_endpoint,
            access_key=backend_settings["admin_access_key"],
            secret_key=backend_settings["admin_secret_key"],
            # rgw_admin_entry, if the operator renamed it.
            admin_path=str(backend_settings.get("admin_path") or "admin"),
            # RGW does not check the region against its zonegroup, but the
            # signature covers it, so both ends must use the same string.
            region=self.s3_region,
            verify_ssl=backend_settings.get("verify_ssl", True),
            timeout=backend_settings.get("timeout", 30),
        )

        # Backend-specific settings
        # Every resource gets a tenant of its own, rendered from tenant_template
        # (or the default). default_tenant put every resource of an offering into
        # one shared tenant, so it is refused rather than honoured.
        if backend_settings.get("default_tenant"):
            raise ValueError(
                "'default_tenant' is no longer supported: it puts every resource "
                "into one shared tenant. Remove it; each resource now gets its own "
                f"tenant from 'tenant_template' (default {DEFAULT_TENANT_TEMPLATE!r}). "
                "Existing resources keep theirs"
            )
        self.tenant_template = backend_settings.get("tenant_template") or DEFAULT_TENANT_TEMPLATE
        validate_tenant_template(self.tenant_template)
        # One RGW account per resource, with the resource's user as its root.
        self.rgw_accounts = bool(backend_settings.get("rgw_accounts"))
        # Backend id -> RGW uid, for ids whose uid had to be looked up.
        self._uid_cache: dict[str, str] = {}
        # The core addresses the client by resource id (get_resource and friends);
        # the client asks this backend which user that id names.
        self.client.resolve_resource_id = self._rgw_uid
        self.default_placement = backend_settings.get("default_placement", "")
        self.default_storage_class = backend_settings.get("default_storage_class", "")

        logger.info(
            "Ceph S3 backend initialized (%s flavour)", self.flavour
        )

    def _get_resource_backend_id(self, resource_slug: str, prefix: str = "") -> str:
        """Namespace the uid the processor generates, so it cannot name any account.

        The core builds this from a consumer-chosen resource name truncated to ten
        characters. With the core's empty default that lands in the cluster's global
        namespace, where an order named ``backup`` addresses whatever ``backup``
        already is. Waldur-managed uids get their own namespace instead.
        """
        del prefix  # The base shadows it too; the setting is the only source.
        namespace = self.backend_settings.get("allocation_prefix") or _DEFAULT_UID_PREFIX
        return f"{namespace}{resource_slug}".lower()

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if backend is online and accessible.

        Args:
            raise_exception: Whether to raise exception on failure

        Returns:
            True if backend is accessible, False otherwise
        """
        return self.client.ping(raise_exception=raise_exception)

    def diagnostics(self) -> bool:
        """Log diagnostic information about the backend.

        Returns:
            True if diagnostics completed successfully
        """
        try:
            logger.info("=== Ceph S3 Backend Diagnostics (%s) ===", self.flavour)
            for label, value in self.client.connection_summary().items():
                logger.info("%s: %s", label, value)
            logger.info("SSL Verification: %s", self.client.verify_ssl)
            logger.info("Components: %s", list(self.backend_components.keys()))

            # Test connectivity
            if self.ping():
                logger.info("✓ API connectivity successful")

                # List current users
                users = self.client.list_users()
                logger.info("Current S3 users: %d", len(users))

                # Show component configuration
                for component_name, config in self.backend_components.items():
                    logger.info("Component %s: %s", component_name, config)

                logger.info("=== Diagnostics completed successfully ===")
                return True
            logger.error("✗ API connectivity failed")
            return False

        except Exception:
            logger.exception("Diagnostics failed")
            return False

    def list_components(self) -> list[str]:
        """Return list of computing components supported by this backend.

        Returns:
            List of component names
        """
        return list(self.backend_components.keys())

    def _connection_info(self, username: str, account_id: str = "") -> dict:
        """Non-secret details a tenant needs to point an S3 client at this resource.

        Flat keys: an offering's Getting started text interpolates
        {backend_metadata_<key>} one level deep, so a nested dict renders as
        [object Object]. The credentials themselves are resource API keys and
        deliberately never appear here.
        """
        return {
            "s3_endpoint": self.s3_endpoint,
            "s3_region": self.s3_region,
            "s3_user": username,
            # Empty rather than absent for an untenanted user: a Getting started
            # text that names the key would otherwise render "undefined".
            "s3_tenant": username.rpartition("$")[0],
            # The account id is what IAM ARNs name, so a consumer writing
            # policies for their own users needs it.
            **({"s3_account_id": account_id} if self.rgw_accounts else {}),
        }

    def _backend_resource_info(
        self, username: str, account_id: str = "", backend_id: str = ""
    ) -> structures.BackendResourceInfo:
        """Describe a provisioned S3 user for Waldur.

        Connection info goes out as backend_metadata as well as an access endpoint.
        Only what this method returns is pushed at provisioning time;
        get_resource_metadata() runs on the reporting pass, which a deployment
        running just order and event processing never reaches — so leaving the
        endpoint to that method rendered the Getting started page as "undefined"
        for exactly as long as anyone was likely to read it.
        """
        return structures.BackendResourceInfo(
            backend_id=backend_id or username,
            backend_metadata=self._connection_info(username, account_id),
            endpoints=[{"name": "S3 endpoint", "url": self.s3_endpoint}],
        )

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: dict[Any, Any] | None = None,
    ) -> structures.BackendResourceInfo:
        """Create the resource's tenant and its root user.

        This is the method the processor actually calls. Without the override the
        base implementation would run instead, skipping bucket quotas and the S3
        endpoint.

        The id the processor generates is not used: the resource is its tenant,
        rendered from tenant_template, and that is what the backend id becomes.
        The exception is a re-create of a terminated resource, which passes the
        resource's stored backend id -- that id is authoritative, and rendering
        again would move the resource if a name or the template changed since.

        Args:
            waldur_resource: Waldur resource object containing limits and metadata
            resource_backend_id: The backend id the processor generated, or the
                stored one on a re-create
            user_context: Optional user context (not used for S3)

        Returns:
            Backend resource info whose backend id is the tenant
        """
        del user_context
        stored = waldur_resource.backend_id
        if isinstance(stored, str) and stored and resource_backend_id == stored:
            backend_id = stored
            uid = self._recreated_uid(stored)
        else:
            backend_id = self._render_tenant(waldur_resource)
            uid = self._root_uid(backend_id)
        self._uid_cache.pop(backend_id, None)
        try:
            return self._create_s3_user(waldur_resource, uid, backend_id)
        except DuplicateResourceError as e:
            # The processor answers DuplicateResourceError by retrying under a new
            # generated id -- up to backend_id_max_retries times -- but the tenant
            # does not come from that id, so every retry would meet the same user.
            raise CephS3Error(
                f"Cannot create resource {waldur_resource.uuid} as {uid}: {e}. If "
                "another resource holds that tenant, change the resource's name or "
                "the offering's tenant_template"
            ) from e

    def _render_tenant(self, waldur_resource: WaldurResource) -> str:
        """The new resource's tenant."""
        return render_tenant(self.tenant_template, waldur_resource)

    @staticmethod
    def _root_uid(tenant: str) -> str:
        """The uid of the one user a resource's tenant is created with.

        The same name in every tenant: the tenant already says which resource it
        is, and a name derived from a setting would move if the setting changed.
        """
        return qualify(tenant, ROOT_UID)

    def _recreated_uid(self, backend_id: str) -> str:
        """The uid a terminated resource is re-created under, from its stored id.

        Ids from before one tenant per resource name the user itself: ``tenant$uid``
        or a bare uid with a character no tenant may hold. A stored id shaped like a
        tenant is re-created as that tenant's root user -- including an old bare uid
        that happens to look like one, since its user is gone and nothing tells the
        two apart. The backend id stays as stored either way, and _rgw_uid finds
        the root user it now names.
        """
        if split(backend_id)[0] or not is_tenant_name(backend_id):
            return backend_id
        return self._root_uid(backend_id)

    def _rgw_uid(self, backend_id: str) -> str:
        """The RGW uid a backend id names.

        - ``tenant$uid``, and ids no tenant could be called (``waldur-<slug>`` has a
          hyphen): a user id from before one tenant per resource, addressed as stored.
        - an id shaped like a tenant: usually the resource's tenant, whose user is
          ``tenant$root``. But bare uids from before the ``waldur-`` prefix --
          ``backup``, or croit-s3's ``waldur_<org>_<project>_<uuid>`` -- look the
          same. So RGW is asked: the root user if it exists, else the bare uid if
          that does. A found answer is remembered until the resource is created or
          deleted; a miss is asked again next time and answers the root user.
        """
        if split(backend_id)[0] or not is_tenant_name(backend_id):
            return backend_id
        root = self._root_uid(backend_id)
        cached = self._uid_cache.get(backend_id)
        if cached is not None:
            return cached
        if self._user_exists(root):
            uid = root
        elif self._user_exists(backend_id):
            uid = backend_id
        else:
            return root
        self._uid_cache[backend_id] = uid
        return uid

    def _user_exists(self, uid: str) -> bool:
        try:
            self.client.get_user_info(uid)
        except CephS3UserNotFoundError:
            return False
        return True

    def _create_s3_user(
        self, waldur_resource: WaldurResource, username: str, backend_id: str
    ) -> structures.BackendResourceInfo:
        """Create one S3 user and apply its safety quotas.

        Args:
            waldur_resource: Waldur resource object containing limits and metadata
            username: The S3 uid to create
            backend_id: The id Waldur will know the resource by

        Returns:
            Backend resource info carrying the S3 access endpoint
        """
        account_id = (
            self._create_or_adopt_account(waldur_resource, username)
            if self.rgw_accounts
            else ""
        )
        # RGW is told not to mint a key of its own (generate-key=False), so a new
        # user holds no credential Waldur did not issue and there is nothing to sweep.
        self._create_or_adopt_user(waldur_resource, username, account_id)

        # Quotas are idempotent PUTs, so they are re-asserted on the adopt path too.
        # A create that erred after the user existed came back through that path and
        # returned success having applied no cap at all — which is the exact failure
        # this plugin's quota handling exists to prevent.
        #
        # Called even for an order carrying no attributes: the "nothing was capped"
        # warning lives inside, and guarding the call made the emptiest case — the
        # one most likely to be a misconfiguration — the only silent one.
        self._apply_bucket_quotas(username, _as_attribute_dict(waldur_resource))

        logger.info("S3 user %s is provisioned", username)
        return self._backend_resource_info(username, account_id, backend_id)

    def _create_or_adopt_account(
        self, waldur_resource: WaldurResource, username: str
    ) -> str:
        """Create the resource's RGW account, or adopt the one an earlier attempt made.

        The account is named after the uid and carries the same ownership stamp as
        its root user, so a retried create finds it by name and can tell it apart
        from an account someone else made.

        Returns:
            The account id.

        Raises:
            DuplicateResourceError: when an account of that name is not this
                resource's.
        """
        tenant, bare = split(username)
        if not _IAM_USER_NAME_RE.match(bare):
            # The root user's display name is the uid, and RGW holds it to the IAM
            # UserName rules; failing here names the reason, the gateway would not.
            raise CephS3Error(
                f"S3 user id {bare!r} cannot name an RGW account user: it must "
                "be 1-64 characters of letters, digits and _+=,.@-"
            )
        stamp = _ownership_stamp(waldur_resource.uuid)
        try:
            # A tenanted resource gets a tenanted account: its users and buckets
            # then live in the tenant's namespace, apart from every other
            # resource's. The account is named after the bare uid, which is unique
            # within the tenant.
            account = self.client.create_account(name=bare, email=stamp, tenant=tenant)  # type: ignore[attr-defined]
            logger.info("Created RGW account %s for user %s", account["id"], username)
            return str(account["id"])
        except CephS3AccountExistsError:
            pass

        try:
            account = self.client.get_account(name=bare, tenant=tenant)  # type: ignore[attr-defined]
        except CephS3Error as e:
            # Taken, but not by that name: the stamp is already in use elsewhere,
            # or the read failed. Either way it cannot be confirmed as ours.
            raise DuplicateResourceError(
                f"RGW account {username} could not be created or read back, so it "
                "cannot be confirmed as this resource's"
            ) from e
        if not _is_stamped_for(account.get("email"), waldur_resource.uuid, username):
            raise DuplicateResourceError(
                f"RGW account {username} already exists and was not provisioned for "
                f"resource {waldur_resource.uuid}; refusing to take it over"
            )
        logger.info(
            "Adopting RGW account %s (%s): it carries this resource's stamp",
            username,
            account["id"],
        )
        return str(account["id"])

    def _create_or_adopt_user(
        self, waldur_resource: WaldurResource, username: str, account_id: str = ""
    ) -> bool:
        """Create the S3 user, or adopt one that is already there.

        Returns:
            True when this call created the user, False when it adopted an existing one.
        """
        # Neutral snake_case: this is the client contract, and the client maps it
        # onto RGW's hyphenated parameters.
        user_data = {
            "uid": username,
            "name": waldur_resource.name or f"User for {waldur_resource.uuid}",
            "email": _ownership_stamp(waldur_resource.uuid),
        }
        if self.default_placement:
            user_data["default_placement"] = self.default_placement
        if self.default_storage_class:
            user_data["default_storage_class"] = self.default_storage_class
        if account_id:
            # The display name is the user's IAM UserName inside the account, held
            # to IAM's character rules, so the uid stands in for the resource name.
            user_data["name"] = split(username)[1]
            user_data["account_id"] = account_id
            user_data["account_root"] = True

        logger.info(
            "Creating S3 user: %s for resource %s", username, waldur_resource.uuid
        )
        try:
            self.client.create_user(**user_data)
        except CephS3UserExistsError:
            return self._adopt_if_ours(waldur_resource, username, account_id)
        except CephS3Error as e:
            if "email" in str(e).lower():
                # RGW answers EmailExists: another user already carries this
                # resource's stamp, so the resource has a user elsewhere -- most
                # likely an earlier attempt that rendered its tenant from names
                # changed since.
                raise CephS3Error(
                    f"Resource {waldur_resource.uuid} already has an S3 user on this "
                    f"cluster under another id (one carries its stamp "
                    f"{user_data['email']}), so {username} was not created. Remove "
                    "that user, or set its tenant as the resource's backend id"
                ) from e
            logger.exception("Failed to create S3 user for resource %s", waldur_resource.uuid)
            raise
        except Exception:
            logger.exception(
                "Unexpected error creating S3 user for resource %s", waldur_resource.uuid
            )
            raise
        return True

    def _adopt_if_ours(
        self, waldur_resource: WaldurResource, username: str, account_id: str = ""
    ) -> bool:
        """Continue against an existing uid only when this resource owns it.

        "Does this uid exist?" cannot separate the two cases that reach here: our
        own create that died after the user was made, and an account the cluster
        already had. The stamp can, so it is read back rather than trusted.

        Refusing raises DuplicateResourceError, which the order processor already
        handles by generating another id and, failing that, erring the order — the
        behaviour this plugin had before it overrode ``create_resource_with_id``.

        Returns:
            False, meaning "adopted rather than created", when the stamp matches.

        Raises:
            DuplicateResourceError: when the uid is not this resource's to use.
        """
        try:
            existing = self.client.get_user_info(username)
        except CephS3Error as e:
            # An unverifiable uid is refused: adopting on a failed read would make a
            # gateway blip enough to reach somebody else's account.
            raise DuplicateResourceError(
                f"S3 user {username} already exists and could not be read back, so "
                "it cannot be confirmed as this resource's"
            ) from e

        ours = _is_stamped_for(existing.get("email"), waldur_resource.uuid, username)
        if ours and account_id and (
            existing.get("account_id") != account_id
        ):
            # Ours, but outside the account it should be root of -- provisioned
            # before account mode was switched on. Membership cannot be added
            # without moving its buckets, so that is left to an operator.
            raise CephS3Error(
                f"S3 user {username} belongs to this resource but not to RGW account "
                f"{account_id}; it was provisioned before account mode was enabled"
            )
        if ours:
            logger.info(
                "Adopting S3 user %s: it carries this resource's stamp, so an "
                "earlier create was interrupted",
                username,
            )
            return False

        raise DuplicateResourceError(
            f"S3 user {username} already exists and was not provisioned for resource "
            f"{waldur_resource.uuid}; refusing to take it over"
        )

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> None:
        """Delete S3 user resource.

        Args:
            waldur_resource: Waldur resource object
            **kwargs: Additional arguments
        """
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id:
            logger.warning("No backend ID found for resource %s", waldur_resource.uuid)
            return
        self._uid_cache.pop(resource_backend_id, None)
        resource_backend_id = self._rgw_uid(resource_backend_id)

        if self.rgw_accounts:
            self._delete_account_resource(resource_backend_id)
            return

        try:
            logger.info("Deleting S3 user: %s", resource_backend_id)
            self.client.delete_user(resource_backend_id)
            logger.info("S3 user %s deleted successfully", resource_backend_id)

        except CephS3UserNotFoundError:
            logger.warning(
                "S3 user %s not found, considering as deleted", resource_backend_id
            )
        except CephS3Error:
            logger.exception("Failed to delete S3 user %s", resource_backend_id)
            raise
        except Exception:
            logger.exception("Unexpected error deleting S3 user %s", resource_backend_id)
            raise

    def _delete_account_resource(self, username: str) -> None:
        """Remove the resource's account: its users first, then the account.

        Refuses while the account holds any bucket. RGW would not stop it: an
        account root user deletes with 200 while the account still owns buckets,
        so the user-mode safety -- "a user with buckets cannot be deleted" --
        does not carry over and is checked here instead. Checking before any
        user goes means a refused termination leaves the account whole, rather
        than holding data nobody can log in to.

        Every member goes, including users the consumer made through the IAM API:
        RGW will not delete an account that still has users. Roles, groups and
        OIDC providers the consumer made go first, through the root user, since
        RGW will not delete an account holding them either and Admin Ops cannot
        reach them.
        """
        client: RadosGWClient = self.client  # type: ignore[assignment]
        try:
            info = client.get_user_info(username)
        except CephS3UserNotFoundError:
            info = None
        root_present = info is not None

        if info is not None:
            if not _is_waldur_stamp(info.get("email")):
                raise CephS3Error(
                    f"Refusing to delete S3 user {username}: it was not provisioned "
                    "by Waldur"
                )
            account_id = str(info.get("account_id") or "")
            if not account_id:
                # Provisioned before account mode was switched on: a plain user,
                # removed the plain way.
                client.delete_user(username)
                return
            # Listing through any member returns the account's buckets, including
            # those its IAM users created.
            buckets = client.get_user_buckets(username)
            if buckets:
                raise CephS3Error(
                    f"Cannot delete RGW account {account_id} of user {username}: it "
                    f"still owns {len(buckets)} bucket(s). Empty and remove them "
                    "first, then retry the termination"
                )
        else:
            # The root user is gone -- an earlier attempt got this far. The
            # account, if any is left, is found by the name it was created with.
            try:
                account = client.get_account(
                    name=split(username)[1], tenant=split(username)[0]
                )
            except CephS3AccountNotFoundError:
                logger.info("RGW account of user %s already absent", username)
                return
            if not _is_waldur_stamp(account.get("email")):
                raise CephS3Error(
                    f"Refusing to delete RGW account {username}: it was not "
                    "provisioned by Waldur"
                )
            account_id = str(account["id"])

        if root_present:
            # Before any user goes: what the account's IAM holds blocks the
            # account's deletion, and only its root user can remove it.
            self._clear_account_iam(username)
        members = client.list_account_users(account_id, tenant=split(username)[0])
        # The root user last, so a retry after a partial failure can still find
        # the account through it.
        for uid in sorted(members, key=lambda member: member == username):
            logger.info("Deleting user %s of RGW account %s", uid, account_id)
            client.delete_user(uid)
        # RGW refuses (409) if a bucket appeared since the check above.
        client.delete_account(account_id)
        logger.info("RGW account %s of user %s deleted", account_id, username)

    def _clear_account_iam(self, root_uid: str) -> None:
        """Remove the IAM roles, groups and OIDC providers of ``root_uid``'s account.

        The calls are signed as that root user with a temporary key.

        The temporary key is removed again whatever happens. A root user already
        at its key limit gives up one existing key first: termination removes
        every key of the account anyway.

        Raises:
            CephS3APIError: if IAM refuses; no user has been deleted by then.
        """
        client: RadosGWClient = self.client  # type: ignore[assignment]
        access_key = "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for _ in range(20)
        )
        secret_key = "".join(
            secrets.choice(string.ascii_letters + string.digits) for _ in range(40)
        )
        try:
            client.create_user_key(root_uid, access_key, secret_key)
        except CephS3APIError:
            keys = client.list_user_keys(root_uid)
            if not keys:
                raise
            client.delete_user_key(root_uid, keys[0]["access_key"])
            client.create_user_key(root_uid, access_key, secret_key)
        iam = AccountIamClient(
            client.gateway_url,
            access_key,
            secret_key,
            region=client._region,
            verify_ssl=client.verify_ssl,
            timeout=client.timeout,
        )
        try:
            removed = iam.clear()
        except CephS3APIError as e:
            raise CephS3APIError(
                f"Cannot terminate {root_uid}: removing the account's IAM roles, "
                f"groups or OIDC providers failed ({e}); nothing was deleted"
            ) from e
        finally:
            iam.close()
            client.delete_user_key(root_uid, access_key)
        if any(removed.values()):
            logger.info("Removed IAM resources of %s: %s", root_uid, removed)

    def generate_resource_keys(
        self, resource_backend_id: str, count: int = _DEFAULT_KEY_COUNT
    ) -> Iterator[dict]:
        """Mint up to two access/secret pairs, applying and yielding one at a time.

        Two is the cap: one key stays usable while the other is rotated.

        Yields rather than returns so the caller reports each pair before the next
        is applied. Applying both first meant a failure on the second left the
        first live on RGW with no row in Waldur — and no way back, since every
        recovery path needs a client_id Waldur holds.

        Purely additive -- RGW creates the user without a key of its own, so there
        is nothing here to clean up and no way for this to cut off a credential
        someone is using.

        Args:
            resource_backend_id: S3 username
            count: How many pairs to mint, capped at two

        Yields:
            {"client_id": access key, "api_key": secret key} dictionaries
        """
        if count > _MAX_KEY_COUNT:
            logger.warning(
                "Capping the requested %d S3 keys for user %s at %d",
                count,
                resource_backend_id,
                _MAX_KEY_COUNT,
            )
            count = _MAX_KEY_COUNT

        resource_backend_id = self._rgw_uid(resource_backend_id)
        self._refuse_foreign_user(resource_backend_id)

        for index in range(count):
            pair = _generate_key_pair()
            self.client.create_user_key(
                resource_backend_id, pair["client_id"], pair["api_key"]
            )
            logger.info(
                "Generated S3 key %d of %d for user %s",
                index + 1,
                count,
                resource_backend_id,
            )
            yield pair

    def prune_unknown_resource_keys(
        self, resource_backend_id: str, keep: list[str]
    ) -> None:
        """Clear the keys an adopted uid carried out of an interrupted create.

        An attempt that died part-way through minting leaves its own applied pairs
        behind, live and without a row in Waldur, so nothing could ever rotate them.

        Runs before this provisioning mints anything, so the only keys in scope are
        ones that predate it; ``keep`` is what Waldur holds, which on a retry is the
        pair the earlier attempt managed to report.
        """
        # Deleting is the more dangerous half of this pair, so it is checked first
        # and by the same rule. Minting refuses a foreign uid because it hands out a
        # credential; pruning can take away one a consumer is already using, and it
        # runs before minting -- so leaving it unguarded put the destructive step
        # outside the check and the additive step inside it.
        resource_backend_id = self._rgw_uid(resource_backend_id)
        self._refuse_foreign_user(resource_backend_id)

        candidates = {
            str(key["access_key"])
            for key in self.client.list_user_keys(resource_backend_id)
            if key.get("access_key")
        }
        self._prune_unknown_keys(resource_backend_id, set(keep), candidates)

    def _refuse_foreign_user(self, resource_backend_id: str) -> None:
        """Stop before touching the keys of a user this agent did not create.

        The create path already refuses a foreign uid, so this cannot trigger on a
        resource provisioned by this code. It guards the other way in: a backend id
        that arrives from Waldur's database rather than from a create this process
        performed.

        Both the mint and the prune call this, so a provisioning cycle pays for two
        reads. That is deliberate: they are separate entry points on the same uid,
        and skipping the read on either one is what put the destructive step outside
        the check while the additive one stayed inside it.

        The resource is not in scope here — the call is addressed by backend id
        alone — so this asks the weaker "did Waldur make this user", which still
        excludes every account the cluster already had.

        Raises:
            CephS3Error: when the user carries no stamp this agent wrote.
        """
        info = self.client.get_user_info(resource_backend_id)
        if _is_waldur_stamp(info.get("email")):
            return
        raise CephS3Error(
            f"Refusing to touch the S3 keys of user {resource_backend_id}: it was "
            "not provisioned by Waldur"
        )

    def rotate_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        known_client_ids: list[str] | None = None,
    ) -> dict:
        """Mint a fresh pair for the S3 user, then drop the old access key.

        The new key is applied before the old one is removed, and sibling keys are never
        touched, so a consumer on another key keeps working. Returns both halves: an S3
        rotation replaces the public identifier as well as the secret, so Waldur has to
        store the new access key too.

        ``known_client_ids`` closes the orphan window. A rotation whose reply to Waldur
        is lost has already replaced the access key at the backend, so re-issuing it
        rotates from a client_id RGW no longer has and would strand the intermediate
        key — live, and unknown to Waldur forever. Given the resource's known set, the
        rotation also drops anything outside it, so the invariant survives a retry: a
        credential that works is a credential Waldur can rotate.

        Args:
            client_id: The access key being rotated
            resource_backend_id: S3 username owning the key
            known_client_ids: Every access key Waldur holds for this resource. Omitted
                by callers that cannot supply it; then no pruning happens.

        Returns:
            The new {"client_id", "api_key"} pair
        """
        # Read the key set before minting anything. Pruning is then limited to keys
        # that already existed, so a key a concurrent rotation creates during this
        # one is structurally unprunable rather than merely unlikely to be caught —
        # deleting it would leave Waldur publishing a credential RGW does not have.
        resource_backend_id = self._rgw_uid(resource_backend_id)
        prunable = {
            str(key["access_key"])
            for key in self.client.list_user_keys(resource_backend_id)
            if key.get("access_key")
        } - {client_id}

        pair = _generate_key_pair()
        self.client.create_user_key(
            resource_backend_id, pair["client_id"], pair["api_key"]
        )
        self.client.delete_user_key(resource_backend_id, client_id)
        logger.info("Rotated S3 key %s of user %s", client_id, resource_backend_id)

        if known_client_ids is not None:
            self._prune_unknown_keys(
                resource_backend_id,
                {*known_client_ids, pair["client_id"]} - {client_id},
                prunable,
            )
        return pair

    def _prune_unknown_keys(self, username: str, keep: set, candidates: set) -> None:
        """Remove pre-existing keys of a user that Waldur does not know about.

        ``candidates`` is the key set read before this rotation minted anything, so
        a key created by a concurrent rotation cannot be deleted here.
        """
        for key in self.client.list_user_keys(username):
            access_key = key.get("access_key")
            if not access_key:
                logger.error(
                    "RGW returned a key entry for user %s with no access_key field "
                    "(keys: %s); it cannot be managed",
                    username,
                    sorted(key),
                )
                continue
            if str(access_key) in keep or str(access_key) not in candidates:
                continue
            logger.warning(
                "Removing the orphaned S3 key %s of user %s: Waldur does not hold it, "
                "so it could never be rotated",
                access_key,
                username,
            )
            self.client.delete_user_key(username, str(access_key))

    def _storage_components(self) -> dict:
        """Components the storage series is measured and capped against.

        Selected by backend_name rather than by component key so a rename cannot
        bill usage while silently skipping the quota — the usage path and the quota
        path used to look the component up in two different ways, and either naming
        choice broke one of them without an error.
        """
        return {
            name: config
            for name, config in self.backend_components.items()
            if config.get("backend_name") == "storage"
        }

    def update_resource_options(
        self, waldur_resource: WaldurResource, new_options: dict
    ) -> bool:
        """Re-apply both quotas from the options an UPDATE order sets.

        The ceilings are the only options this plugin enforces, and setting a
        quota is an idempotent PUT, so the update is the create-time quota step
        run again over the effective options: what the order set at creation,
        overlaid by the resource's current options, overlaid by the new ones. An
        option the order clears is sent as "no limit" rather than left out.

        Raises:
            CephS3Error: when the resource has no backend id, or its user was not
                provisioned by Waldur, or RGW refuses the quota.
        """
        username = waldur_resource.backend_id
        if not username or isinstance(username, Unset):
            raise CephS3Error(
                f"Resource {waldur_resource.uuid} has no backend id, so there is no "
                "S3 user to apply its options to"
            )
        username = self._rgw_uid(username)
        self._refuse_foreign_user(username)
        effective = {
            **_as_attribute_dict(waldur_resource),
            **_as_options_dict(waldur_resource),
            **new_options,
        }
        logger.info("Applying options %s to S3 user %s", effective, username)
        self._apply_bucket_quotas(username, effective, clear_missing=True)
        return True

    def _apply_bucket_quotas(
        self, username: str, resource_options: dict, clear_missing: bool = False
    ) -> None:
        """Apply safety limits from resource options as bucket quotas.

        Args:
            username: S3 username
            resource_options: Resource options containing safety limits
            clear_missing: Send -1 ("no limit") for a dimension the options do not
                set, instead of leaving it out. An update needs this: RadosGW
                merges a quota PUT, so a left-out dimension would keep the limit
                the consumer just removed.

        Raises:
            CephS3Error: If quota setting fails
        """
        quota_request: dict[str, Any] = {"enabled": True}
        quota_applied = False

        # Capacity ceilings, named apart from the billed quantity on purpose: storage
        # is invoiced in GB-days, so an order attribute called "storage_limit" next
        # to a GB-day usage figure reads like the billing basis rather than a cap on
        # how much may be held at once.
        storage_limit = _first_limit(resource_options, "max_storage_limit", "storage_limit")
        object_limit = _first_limit(resource_options, "max_object_limit", "object_limit")

        # Ordering a ceiling is the intent; there is deliberately no second switch
        # to enable enforcement. One used to exist (`enforce_limits`, read only
        # here and defaulting to False), which meant an operator could order a
        # limit, have it recorded on the resource, and get no quota at all --
        # silently, because a missing quota looks exactly like an offering that
        # never had one.
        storage_config = next(iter(self._storage_components().values()), None)
        if storage_limit > 0:
            if storage_config is None:
                # A ceiling cannot be converted without the component that carries
                # unit_factor. The factor's own value is checked at construction
                # (see settings.validate_components), so reaching here means the
                # component is genuinely absent rather than misconfigured.
                logger.warning(
                    "Ordered a %d GB ceiling for user %s but no s3_storage component "
                    "is configured to convert it; skipping the quota",
                    storage_limit,
                    username,
                )
            else:
                unit_factor = storage_config["unit_factor"]
                quota_request["max_size_bytes"] = int(storage_limit * unit_factor)
                quota_applied = True
                logger.debug(
                    "Setting storage quota: %d bytes (%d GB)",
                    quota_request["max_size_bytes"],
                    storage_limit,
                )

        # Objects need no unit conversion, so there is nothing to read from a
        # component and no reason for one to exist just to hold a flag.
        if object_limit > 0:
            quota_request["max_objects"] = object_limit
            quota_applied = True
            logger.debug("Setting object quota: %d objects", object_limit)

        if clear_missing:
            # -1 is "no limit". 0 is not: RadosGW would read it as a ceiling of
            # nothing.
            quota_request.setdefault("max_size_bytes", -1)
            quota_request.setdefault("max_objects", -1)
            if not quota_applied:
                logger.warning(
                    "Removing every capacity ceiling of user %s: options %s set "
                    "none; the resource has no cap on what it can hold, nor on "
                    "what it bills",
                    username,
                    resource_options,
                )
                self._write_quotas(username, quota_request)
                return

        # Apply quota if any limits were configured
        if quota_applied:
            logger.info("Applying quotas for user %s: %s", username, quota_request)
            # The ordered ceiling is a per-tenant figure, so the user quota is what
            # makes it true — a bucket quota caps each bucket, and the bucket count
            # is not capped, so on its own it bounds nothing. The
            # bucket quota is kept alongside it as a per-bucket guard.
            self._write_quotas(username, quota_request)
        else:
            # Warning, not info, and it names the attributes it looked at. An
            # unbounded resource is a resource with no bound on its invoice, and
            # the last time this happened it was invisible precisely because the
            # log line said nothing about what it had been given.
            logger.warning(
                "No capacity ceiling applied for user %s: order attributes %s, "
                "storage components %s; the resource has no cap on what it can "
                "hold, nor on what it bills",
                username,
                sorted(resource_options),
                sorted(self._storage_components()),
            )

    def _write_quotas(self, username: str, quota_request: dict) -> None:
        """Write the ceiling as the total quota and as the per-bucket guard.

        In account mode both go on the account: buckets belong to the account, not
        to its users, and RGW stores a per-user quota on an account user without
        enforcing it -- measured on Ceph 20.2.5, where a two-object user quota let
        five objects through.
        """
        if not self.rgw_accounts:
            self.client.set_user_quota(username, quota_request)
            self.client.set_user_bucket_quota(username, quota_request)
            return

        account_id = self._account_id_of(username)
        client: RadosGWClient = self.client  # type: ignore[assignment]
        client.set_account_quota(account_id, quota_request, "account")
        client.set_account_quota(account_id, quota_request, "bucket")

        # Ceph before 20.2.2 has no account quota endpoint, and 19.2.x answers the
        # PUT with 200 while changing nothing. Reading back is the only way to
        # tell, and an account left uncapped must fail the order, not pass it.
        try:
            account = client.get_account(account_id=account_id)
        except CephS3AuthenticationError as e:
            # Before 20.2.2 the read checks a cap that cannot be granted, so a
            # correctly configured agent is refused here, after a quota PUT that
            # changed nothing.
            raise CephS3Error(
                f"Cannot read back the quota of RGW account {account_id} ({e}). "
                "Account quotas need Ceph 20.2.2 or later"
            ) from e
        for scope in ("quota", "bucket_quota"):
            applied = account.get(scope) or {}
            for field in ("enabled", "max_size_bytes", "max_objects"):
                if field in quota_request and applied.get(field) != quota_request[field]:
                    raise CephS3Error(
                        f"RGW did not apply the {scope} of account {account_id}: "
                        f"{field} reads back {applied.get(field)!r}, expected "
                        f"{quota_request[field]!r}. Account quotas need Ceph 20.2.2 "
                        "or later"
                    )

    def _account_id_of(self, username: str) -> str:
        """The account the resource's root user belongs to.

        Raises:
            CephS3Error: when the user is in no account -- a resource provisioned
                before account mode was enabled on its offering.
        """
        account_id = str(self.client.get_user_info(username).get("account_id") or "")
        if not account_id:
            raise CephS3Error(
                f"S3 user {username} is not in an RGW account; it was provisioned "
                "before account mode was enabled"
            )
        return account_id

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """No usage from the management backend.

        Metering belongs to the reporting backends (CroitUsageBackend,
        RgwUsageBackend) that build on this class. Returning empty rather than
        raising because membership sync calls this too, on whichever backend is wired as
        membership_sync_backend — raising would break user synchronisation for a
        method that only ever contributed billing figures.
        """
        del resource_backend_ids
        return {}

    def _pull_backend_resource(
        self, resource_backend_id: str
    ) -> structures.BackendResourceInfo | None:
        """Pull resource data, without substituting zero for absent usage.

        The base fills every component with 0 when ``_get_usage_report`` omits a
        resource. That is right where "no usage record" means "used nothing", and
        wrong here twice over: this backend never reports usage at all, and the
        figure it would be overwriting is a period-to-date accrual. Left alone,
        every membership-sync pass would zero the month's GB-days — silently,
        because zero is a legitimate usage value.

        The info itself is still returned: membership sync needs the user list.
        """
        info = super()._pull_backend_resource(resource_backend_id)
        if info is None:
            return None
        return self._apply_usage_policy(resource_backend_id, info)

    def _apply_usage_policy(
        self, resource_backend_id: str, info: structures.BackendResourceInfo
    ) -> structures.BackendResourceInfo | None:
        """Decide what usage, if any, this pull may carry.

        A separate hook so that both the "never zero" rule and the subclass that
        actually meters live on one super() chain: CroitUsageBackend overrides
        this rather than _pull_backend_resource, which would otherwise run this
        method first and blank the very figure it wants to inspect.
        """
        del resource_backend_id
        info.usage = {}
        return info

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get S3 user metadata: connection info and usage summary.

        Credentials are deliberately absent — they live as resource API keys,
        encrypted in Waldur and revealed through a permission-gated, audited
        endpoint, rather than in the broadly-readable backend metadata.

        Args:
            resource_backend_id: S3 username

        Returns:
            Metadata dictionary with connection and usage info
        """
        resource_backend_id = self._rgw_uid(resource_backend_id)
        try:
            # Get user information
            user_info = self.client.get_user_info(resource_backend_id)

            # Get bucket information
            buckets = self.client.get_user_buckets(resource_backend_id)

            # Calculate storage summary
            total_size = sum(b.get("size_bytes", 0) for b in buckets)
            total_objects = sum(b.get("num_objects", 0) for b in buckets)

            # Get quota information
            account_id = str(user_info.get("account_id") or "")
            if self.rgw_accounts and account_id:
                account = self.client.get_account(account_id=account_id)  # type: ignore[attr-defined]
                quotas = {
                    "account_quota": account.get("quota", {}),
                    "bucket_quota": account.get("bucket_quota", {}),
                }
            else:
                quota_info = self.client.get_user_quota(resource_backend_id)
                quotas = {
                    "bucket_quota": quota_info.get("bucket_quota", {}),
                    "user_quota": quota_info.get("user_quota", {}),
                }

            return {
                **self._connection_info(resource_backend_id, account_id),
                "user_info": {
                    "uid": user_info.get("uid"),
                    "name": user_info.get("name"),
                    "suspended": user_info.get("suspended", False),
                },
                "storage_summary": {
                    "bucket_count": len(buckets),
                    "total_size_bytes": total_size,
                    "total_objects": int(total_objects),
                    "buckets": [
                        {
                            "name": bucket.get("name"),
                            "size_bytes": bucket.get("size_bytes", 0),
                            "objects": bucket.get("num_objects", 0),
                        }
                        for bucket in buckets
                    ],
                },
                "quotas": quotas,
                # Consumers read this metadata. The ownership stamp (email),
                # placement and storage-class names and the agent's own wiring are
                # deliberately left out: they describe the provider, not the
                # resource.
            }

        except Exception as e:
            logger.exception("Failed to get metadata for user %s", resource_backend_id)
            return {
                "error": f"Failed to retrieve metadata: {e}",
                "backend_type": self.backend_type,
            }

    # Abstract method implementations (minimal/no-op for S3 storage)
    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale resource - not applicable for S3 storage.

        S3 has no reduced mode short of rewriting the consumer's own policies.
        """
        logger.info("Downscale not applicable for S3 user: %s", resource_backend_id)
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Suspend every user of the resource, so none of its keys works.

        In account mode that is the root user and every IAM user the consumer
        created: RGW suspends per user, and a suspended root does not stop the
        others.
        """
        self._set_suspended(resource_backend_id, True)
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Lift the suspension pause_resource set, on every user of the resource."""
        self._set_suspended(resource_backend_id, False)
        return True

    def _set_suspended(self, resource_backend_id: str, suspended: bool) -> None:
        """Bring every user of the resource to ``suspended``.

        The core calls pause or restore for every resource on every sync, so the
        common case -- already in that state -- has to cost one read: the root
        user's flag. Only a change lists the account's members (which walks every
        user on the gateway). The root goes last both ways, so a pass cut short
        leaves the root unchanged and the next sync redoes it.

        Waldur's pause is the source of truth: a user an operator suspended by
        hand on the cluster is reinstated by the next restore.
        """
        username = self._rgw_uid(resource_backend_id)  # each client call validates it
        try:
            info = self.client.get_user_info(username)
        except CephS3UserNotFoundError:
            logger.warning(
                "Cannot %s %s: no such S3 user",
                "suspend" if suspended else "reinstate",
                username,
            )
            return
        if bool(info.get("suspended")) == suspended:
            return
        users = [username]
        account_id = str(info.get("account_id") or "") if self.rgw_accounts else ""
        if account_id:
            members = self.client.list_account_users(  # type: ignore[attr-defined]
                account_id, tenant=split(username)[0]
            )
            users = [uid for uid in members if uid != username] + [username]
        for uid in users:
            self.client.set_user_suspended(uid, suspended)
        logger.info(
            "%s %d user(s) of %s",
            "Suspended" if suspended else "Reinstated",
            len(users),
            username,
        )

    def add_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Add user to S3 resource - not applicable for individual S3 users."""
        del username, kwargs
        logger.info("Add user not applicable for S3 storage: %s", waldur_resource.backend_id)
        return True

    def remove_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Remove user from S3 resource - not applicable for individual S3 users."""
        del username, kwargs
        logger.info(
            "Remove user not applicable for S3 storage: %s", waldur_resource.backend_id
        )
        return True

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect limits for backend and Waldur separately.

        Args:
            waldur_resource: Waldur resource object

        Returns:
            Tuple of (backend_limits, waldur_limits) dictionaries
        """
        backend_limits: dict[str, int] = {}
        waldur_limits: dict[str, int] = {}

        if not waldur_resource.limits:
            return backend_limits, waldur_limits

        # Process each component and extract limits
        for component_name in self.backend_components:
            limit_value = getattr(waldur_resource.limits, component_name, 0)
            # Only include actual configurable components (not s3_user)
            if limit_value > 0 and component_name != "s3_user":
                backend_limits[component_name] = limit_value
                waldur_limits[component_name] = limit_value

        logger.debug(
            "Collected limits - Backend: %s, Waldur: %s", backend_limits, waldur_limits
        )
        return backend_limits, waldur_limits

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: dict | None = None,
    ) -> None:
        """Nothing to do before creating an S3 user.

        Abstract on BaseBackend, so it has to exist. It is never reached either:
        the base calls it from create_resource_with_id, which this backend
        overrides without delegating.
        """
        del waldur_resource, user_context
