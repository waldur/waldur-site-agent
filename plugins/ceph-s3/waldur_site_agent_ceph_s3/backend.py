"""Croit S3 storage backend for Waldur Site Agent.

This module provides integration between Waldur Mastermind and Croit S3 storage
via RadosGW API. It implements the backend interface for managing S3 users,
bucket quotas, and usage reporting.

Key Features:
- S3 user provisioning with slug-based naming
- Usage and limit-based accounting support
- Bucket quota enforcement for limit-based components
- Comprehensive usage reporting with storage and object metrics
- S3 credentials managed as resource API keys (encrypted in Waldur, never in metadata)
"""

import logging
import secrets
import string
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Iterator, Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import (
    DEFAULT_RESOURCE_KEY_COUNT,
    BackendType,
    backends,
    structures,
)
from waldur_site_agent.backend.exceptions import DuplicateResourceError

from .clients.base import BaseS3AdminClient
from .clients.croit import CroitClient
from .clients.radosgw import RadosGWClient
from .exceptions import CephS3Error, CephS3UserExistsError, CephS3UserNotFoundError
from .settings import (
    CROIT,
    RADOSGW,
    resolve_flavour,
    validate_components,
    validate_settings,
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


def _ownership_stamp(resource_uuid: object, uid: str) -> str:
    """The stamp a user provisioned for this resource, at this uid, must carry.

    The uid is in it for uniqueness, not for the ownership decision — the check
    reads the user *at* that uid, so it is already known to match. Both backends
    reject a create whose email another user holds, and without the uid two users
    of one resource would collide on it: a re-provision under a new id, while the
    old user lingers, would fail on the address rather than provision. No two users
    can share a uid, so no two can share a stamp.
    """
    return f"{_STAMP_PREFIX}{resource_uuid}-{uid}{_STAMP_SUFFIX}"


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

# Two places is what the reporter's "%.2f" formatting preserves on the wire.
_USAGE_PRECISION = Decimal("0.01")

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
    if len(datapoints) < 2:
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


def _first_limit(resource_options: dict, *keys: str) -> int:
    """Read the first ceiling present among ``keys``, defaulting to 0.

    Resources ordered before the rename carry the old key, and their quotas must
    survive a restore or a re-provision, so both spellings are accepted.
    """
    for key in keys:
        value = resource_options.get(key)
        if value:
            return int(value)
    return 0


def _generate_key_pair() -> dict:
    """Mint one access/secret pair.

    Croit does not generate keys — PUT /s3/users/{uid}/keys/{accessKey} takes both
    halves from the caller — so the agent is the source.
    """
    access_key = "".join(
        secrets.choice(_ACCESS_KEY_ALPHABET) for _ in range(_ACCESS_KEY_LENGTH)
    )
    return {
        "client_id": access_key,
        "api_key": secrets.token_urlsafe(_SECRET_KEY_LENGTH)[:_SECRET_KEY_LENGTH],
    }


class CephS3Backend(backends.BaseBackend):
    """Croit S3 storage backend implementation for Waldur Site Agent.

    This backend manages S3 user lifecycle, bucket quotas, and usage reporting
    for Croit storage systems. It supports both usage-based and limit-based
    accounting models.
    """

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

        # Which management API this offering talks to. Defaults to croit, so
        # offerings deployed before the setting existed keep working untouched.
        self.flavour = resolve_flavour(backend_settings)
        validate_settings(backend_settings, self.flavour)
        validate_components(backend_components)

        # The S3 data endpoint is a separate host from the management API — on a real
        # cluster RadosGW runs on the storage nodes, and croit exposes no VIP or DNS
        # name to derive it from. Publishing the management URL instead would hand
        # tenants an address that answers with the web UI, so this is required rather
        # than defaulted.
        s3_endpoint = backend_settings.get("s3_endpoint")
        if not s3_endpoint:
            raise ValueError(
                "Ceph S3 backend requires 's3_endpoint' in backend_settings: the "
                "address tenants use for S3, which is not derivable from 'api_url'"
            )
        self.s3_endpoint = str(s3_endpoint).rstrip("/")

        # S3 clients refuse to sign a request without a region, so one has to be
        # published. croit exposes no zonegroup, so this is not something we can read
        # back: "default" is Ceph's out-of-the-box zonegroup name, correct until an
        # operator names theirs — at which point they set it here.
        self.s3_region = str(backend_settings.get("s3_region") or "default")

        # Initialize the client for the configured flavour. Constructed here
        # rather than behind a factory so each flavour's settings stay visible
        # next to the backend that depends on them.
        self.client: BaseS3AdminClient
        if self.flavour == RADOSGW:
            self.client = RadosGWClient(
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
        else:
            self.client = CroitClient(
                api_url=backend_settings["api_url"],
                username=backend_settings.get("username"),
                password=backend_settings.get("password"),
                token=backend_settings.get("token"),
                verify_ssl=backend_settings.get("verify_ssl", True),
                timeout=backend_settings.get("timeout", 30),
            )

        # Backend-specific settings
        self.default_tenant = backend_settings.get("default_tenant", "")
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
            # Asked of the client rather than read off it: api_url and username
            # are croit's, and reading them here logged an AttributeError for
            # every radosgw offering instead of the diagnostics.
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
            else:
                logger.error("✗ API connectivity failed")
                return False

        except Exception as e:
            logger.exception("Diagnostics failed: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of computing components supported by this backend.

        Returns:
            List of component names
        """
        return list(self.backend_components.keys())

    def _connection_info(self, username: str) -> dict:
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
        }

    def _backend_resource_info(self, username: str) -> structures.BackendResourceInfo:
        """Describe a provisioned S3 user for Waldur.

        Connection info goes out as backend_metadata as well as an access endpoint.
        Only what this method returns is pushed at provisioning time;
        get_resource_metadata() runs on the reporting pass, which a deployment
        running just order and event processing never reaches — so leaving the
        endpoint to that method rendered the Getting started page as "undefined"
        for exactly as long as anyone was likely to read it.
        """
        return structures.BackendResourceInfo(
            backend_id=username,
            backend_metadata=self._connection_info(username),
            endpoints=[{"name": "S3 endpoint", "url": self.s3_endpoint}],
        )

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict[Any, Any]] = None,
    ) -> structures.BackendResourceInfo:
        """Create the S3 user under the backend id the order processor generated.

        This is the method the processor actually calls. Without the override the base
        implementation would run instead, skipping bucket quotas and the S3 endpoint.

        Args:
            waldur_resource: Waldur resource object containing limits and metadata
            resource_backend_id: The backend id the processor generated
            user_context: Optional user context (not used for S3)

        Returns:
            Backend resource info with created S3 username
        """
        del user_context
        return self._create_s3_user(waldur_resource, resource_backend_id)

    def _create_s3_user(
        self, waldur_resource: WaldurResource, username: str
    ) -> structures.BackendResourceInfo:
        """Create one S3 user and apply its safety quotas.

        Args:
            waldur_resource: Waldur resource object containing limits and metadata
            username: The S3 uid to create

        Returns:
            Backend resource info carrying the S3 access endpoint
        """
        created = self._create_or_adopt_user(waldur_resource, username)

        if created:
            # Only a user this call created can have its keys swept: on an adopted
            # uid they may be a tenant's live credentials.
            self._remove_auto_generated_keys(username)

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
        return self._backend_resource_info(username)

    def _create_or_adopt_user(
        self, waldur_resource: WaldurResource, username: str
    ) -> bool:
        """Create the S3 user, or adopt one that is already there.

        Returns:
            True when this call created the user, False when it adopted an existing one.
        """
        # Neutral snake_case, not croit's camelCase: this is the shared client
        # contract, and a name only one flavour recognises is a setting the other
        # silently drops into **kwargs.
        user_data = {
            "uid": username,
            "name": waldur_resource.name or f"User for {waldur_resource.uuid}",
            "email": _ownership_stamp(waldur_resource.uuid, username),
        }
        if self.default_tenant:
            user_data["tenant"] = self.default_tenant
        if self.default_placement:
            user_data["default_placement"] = self.default_placement
        if self.default_storage_class:
            user_data["default_storage_class"] = self.default_storage_class

        logger.info(
            "Creating S3 user: %s for resource %s", username, waldur_resource.uuid
        )
        try:
            self.client.create_user(**user_data)
        except CephS3UserExistsError:
            return self._adopt_if_ours(waldur_resource, username)
        except CephS3Error as e:
            # croit sometimes reports a duplicate uid as a 500 rather than a 409.
            if "exists" in str(e):
                return self._adopt_if_ours(waldur_resource, username)
            logger.error(
                "Failed to create S3 user for resource %s: %s", waldur_resource.uuid, e
            )
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error creating S3 user for resource %s: %s",
                waldur_resource.uuid,
                e,
            )
            raise
        return True

    def _adopt_if_ours(self, waldur_resource: WaldurResource, username: str) -> bool:
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
        expected = _ownership_stamp(waldur_resource.uuid, username)
        try:
            existing = self.client.get_user_info(username)
        except CephS3Error as e:
            # An unverifiable uid is refused: adopting on a failed read would make a
            # gateway blip enough to reach somebody else's account.
            raise DuplicateResourceError(
                f"S3 user {username} already exists and could not be read back, so "
                "it cannot be confirmed as this resource's"
            ) from e

        if existing.get("email") == expected:
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

        try:
            logger.info("Deleting S3 user: %s", resource_backend_id)
            self.client.delete_user(resource_backend_id)
            logger.info("S3 user %s deleted successfully", resource_backend_id)

        except CephS3UserNotFoundError:
            logger.warning(
                "S3 user %s not found, considering as deleted", resource_backend_id
            )
        except CephS3Error as e:
            logger.error("Failed to delete S3 user %s: %s", resource_backend_id, e)
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error deleting S3 user %s: %s", resource_backend_id, e
            )
            raise

    def _remove_auto_generated_keys(self, username: str) -> None:
        """Drop the key RadosGW mints for a user it has just created.

        Creating an S3 user auto-generates an access/secret pair whose secret Waldur
        never sees — a working credential that cannot be revealed or rotated. (The
        plugin used to publish exactly that pair as the resource's metadata
        credentials.) It has to go, or provisioning ends with three live credentials
        and only two under Waldur's control.

        Called only from the branch that just created the user, never from the one
        that adopted an existing uid: at this point the auto key is the *only* key,
        so "delete what is here" and "delete the auto key" are the same set, and no
        consumer holds credentials yet.
        """
        if self.flavour != CROIT:
            # Admin Ops takes generate-key=False on create, so the user never
            # holds a key Waldur did not issue and there is no window to close.
            # Listing anyway would be harmless but misleading: it would imply
            # this flavour has an auto-key problem that it does not have.
            return

        for key in self.client.list_user_keys(username):
            access_key = key.get("access_key")
            if not access_key:
                logger.error(
                    "croit returned a key entry for user %s with no access_key field "
                    "(keys: %s); it cannot be managed",
                    username,
                    sorted(key),
                )
                continue
            logger.info(
                "Removing the auto-generated S3 key %s of new user %s",
                access_key,
                username,
            )
            self.client.delete_user_key(username, str(access_key))

    def generate_resource_keys(
        self, resource_backend_id: str, count: int = _DEFAULT_KEY_COUNT
    ) -> Iterator[dict]:
        """Mint up to two access/secret pairs, applying and yielding one at a time.

        Two is the cap: one key stays usable while the other is rotated.

        Yields rather than returns so the caller reports each pair before the next
        is applied. Applying both first meant a failure on the second left the
        first live on croit with no row in Waldur — and no way back, since every
        recovery path needs a client_id Waldur holds.

        Purely additive — RadosGW's auto-generated key is already gone by the time
        this runs (see _remove_auto_generated_keys), so there is nothing here to clean
        up and no way for this to cut off a credential someone is using.

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

        RadosGW mints a pair with every user croit creates, and an attempt that died
        part-way through minting leaves its own applied pairs behind. Both are live
        and neither has a row in Waldur, so nothing could ever rotate them.

        Runs before this provisioning mints anything, so the only keys in scope are
        ones that predate it; ``keep`` is what Waldur holds, which on a retry is the
        pair the earlier attempt managed to report.
        """
        # Deleting is the more dangerous half of this pair, so it is checked first
        # and by the same rule. Minting refuses a foreign uid because it hands out a
        # credential; pruning can take away one a consumer is already using, and it
        # runs before minting -- so leaving it unguarded put the destructive step
        # outside the check and the additive step inside it.
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
        known_client_ids: Optional[list[str]] = None,
    ) -> dict:
        """Mint a fresh pair for the S3 user, then drop the old access key.

        The new key is applied before the old one is removed, and sibling keys are never
        touched, so a consumer on another key keeps working. Returns both halves: an S3
        rotation replaces the public identifier as well as the secret, so Waldur has to
        store the new access key too.

        ``known_client_ids`` closes the orphan window. A rotation whose reply to Waldur
        is lost has already replaced the access key at the backend, so re-issuing it
        rotates from a client_id croit no longer has and would strand the intermediate
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
        # deleting it would leave Waldur publishing a credential croit does not have.
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
                    "croit returned a key entry for user %s with no access_key field "
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

    def _apply_bucket_quotas(self, username: str, resource_options: dict) -> None:
        """Apply safety limits from resource options as bucket quotas.

        Args:
            username: S3 username
            resource_options: Resource options containing safety limits

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

        # Apply quota if any limits were configured
        if quota_applied:
            logger.info("Applying quotas for user %s: %s", username, quota_request)
            # The ordered ceiling is a per-tenant figure, so the user quota is what
            # makes it true — a bucket quota caps each bucket, and croit exposes no
            # way to cap the bucket count, so on its own it bounds nothing. The
            # bucket quota is kept alongside it as a per-bucket guard.
            self.client.set_user_quota(username, quota_request)
            self.client.set_user_bucket_quota(username, quota_request)
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

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """No usage from the management backend.

        Metering is croit's alone (see CroitUsageBackend), and this class serves
        both flavours. Returning empty rather than raising because membership
        sync calls this too, on whichever backend is wired as
        membership_sync_backend — raising would break user synchronisation for a
        method that only ever contributed billing figures.
        """
        del resource_backend_ids
        return {}

    def _pull_backend_resource(
        self, resource_backend_id: str
    ) -> Optional[structures.BackendResourceInfo]:
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
    ) -> Optional[structures.BackendResourceInfo]:
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
        try:
            # Get user information
            user_info = self.client.get_user_info(resource_backend_id)

            # Get bucket information
            buckets = self.client.get_user_buckets(resource_backend_id)

            # Calculate storage summary
            total_size = sum(b.get("size_bytes", 0) for b in buckets)
            total_objects = sum(b.get("num_objects", 0) for b in buckets)

            # Get quota information
            quota_info = self.client.get_user_quota(resource_backend_id)

            return {
                **self._connection_info(resource_backend_id),
                "user_info": {
                    "uid": user_info.get("uid"),
                    "name": user_info.get("name"),
                    "email": user_info.get("email"),
                    "suspended": user_info.get("suspended", False),
                    "default_placement": user_info.get("default_placement"),
                    "default_storage_class": user_info.get("default_storage_class"),
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
                "quotas": {
                    "bucket_quota": quota_info.get("bucket_quota", {}),
                    "user_quota": quota_info.get("user_quota", {}),
                },
                "backend_info": {
                    "backend_type": self.backend_type,
                    "flavour": self.flavour,
                    # api_url is croit's; every flavour has an S3 endpoint.
                    "s3_endpoint": self.s3_endpoint,
                    "created_via": "waldur_site_agent_ceph_s3",
                },
            }

        except Exception as e:
            logger.error(
                "Failed to get metadata for user %s: %s", resource_backend_id, e
            )
            return {
                "error": f"Failed to retrieve metadata: {e}",
                "backend_type": self.backend_type,
            }

    # Abstract method implementations (minimal/no-op for S3 storage)
    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale resource - not applicable for S3 storage."""
        logger.info("Downscale not applicable for S3 user: %s", resource_backend_id)
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause resource - not applicable for S3 storage."""
        logger.info("Pause not applicable for S3 user: %s", resource_backend_id)
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource - not applicable for S3 storage."""
        logger.info("Restore not applicable for S3 user: %s", resource_backend_id)
        return True

    def add_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Add user to S3 resource - not applicable for individual S3 users."""
        del kwargs
        logger.info("Add user not applicable for S3 storage: %s", waldur_resource.backend_id)
        return True

    def remove_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Remove user from S3 resource - not applicable for individual S3 users."""
        del kwargs
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
        for component_name, component_config in self.backend_components.items():
            limit_value = getattr(waldur_resource.limits, component_name, 0)
            if limit_value > 0:
                # Only include actual configurable components (not s3_user)
                if component_name != "s3_user":
                    backend_limits[component_name] = limit_value
                    waldur_limits[component_name] = limit_value

        logger.debug(
            "Collected limits - Backend: %s, Waldur: %s", backend_limits, waldur_limits
        )
        return backend_limits, waldur_limits

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Nothing to do before creating an S3 user.

        Abstract on BaseBackend, so it has to exist. It is never reached either:
        the base calls it from create_resource_with_id, which this backend
        overrides without delegating.
        """
        del waldur_resource, user_context
