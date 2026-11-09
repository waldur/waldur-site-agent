"""K8s UT ManagedNamespace backend for Waldur Site Agent."""

import json
import pprint
import re
import reprlib
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset
from waldur_site_agent_keycloak_client import KeycloakClient

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError, UserNotProvisionedError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent_k8s_ut_namespace.k8s_client import K8sUtNamespaceClient
from waldur_site_agent_k8s_ut_namespace.schemas import SECURITY_LEVELS

# Default Waldur role -> namespace access level mapping
DEFAULT_ROLE_MAPPING = {
    "manager": "admin",
    "admin": "admin",
    "member": "readwrite",
}
NS_ROLES = ("admin", "readwrite", "readonly")

# Default Waldur component type -> ManagedNamespace quota field mapping
DEFAULT_COMPONENT_QUOTA_MAPPING = {
    "cpu": "cpu",
    "ram": "memory",
    "storage": "storage",
    "gpu": "gpu",
}

# Namespace role -> ManagedNamespace CR spec field for groups/users
NS_ROLE_TO_CR_GROUP_FIELD = {
    "admin": "adminGroups",
    "readwrite": "rwGroups",
    "readonly": "roGroups",
}
NS_ROLE_TO_CR_USER_FIELD = {
    "admin": "adminUsers",
    "readwrite": "rwUsers",
    "readonly": "roUsers",
}

_NS_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_NS_NAME_MAX_LEN = 63

# Order-form attribute names, read from ``attributes`` on the resource, which is
# what the order form records.
QUOTA_ENABLED_ATTRIBUTE = "quota_enabled"
SECURITY_LEVEL_ATTRIBUTE = "security_level"

# Spellings a boolean may arrive in: what Waldur's own BooleanField (DRF) and
# the settings schema (pydantic, lax mode) accept, so a value either of them
# took is not refused here. Strings are matched case-insensitively and trimmed.
_TRUE_STRINGS = frozenset({"true", "t", "yes", "y", "on", "1"})
_FALSE_STRINGS = frozenset({"false", "f", "no", "n", "off", "0"})

# Annotation on the CR holding, as JSON, the whole ``spec.quota`` (quantities
# and ``enabled``) in force before a pause or downscale; restore writes it back.
QUOTA_BEFORE_PAUSE_ANNOTATION = "waldur.io/quota-before-pause"
# Its predecessor, holding only ``enabled`` as "true"/"false". Still read, so a
# namespace paused by an agent that wrote it can be restored.
LEGACY_QUOTA_ENABLED_ANNOTATION = "waldur.io/quota-enabled-before-pause"

# Order attributes are orderer-supplied and may be arbitrarily long: quote them
# in error messages through this, not with a bare ``!r``.
_short_repr = reprlib.Repr()
_short_repr.maxstring = 60
_short_repr.maxother = 60
_short_repr.maxlist = 4
_short_repr.maxdict = 4


def _is_blank(value: object) -> bool:
    """Whether a value means "not given": ``None`` or a whitespace-only string."""
    return value is None or (isinstance(value, str) and not value.strip())


class K8sUtNamespaceBackend(backends.BaseBackend):
    """Backend for managing Kubernetes ManagedNamespace CRs and Keycloak RBAC groups."""

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Initialize K8s UT Namespace backend."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "k8s-ut-namespace"

        # Initialize K8s client
        self.k8s_client = K8sUtNamespaceClient(backend_settings)

        # Initialize Keycloak client if configured
        self.keycloak_client: Optional[KeycloakClient] = None
        if backend_settings.get("keycloak_enabled", False):
            keycloak_settings = backend_settings.get("keycloak", {})
            try:
                self.keycloak_client = KeycloakClient(keycloak_settings)
                logger.info("Keycloak integration enabled for K8s UT namespace backend")
            except Exception as e:
                logger.warning(f"Failed to initialize Keycloak client: {e}")
                self.keycloak_client = None

        self.namespace_prefix = backend_settings.get("namespace_prefix", "waldur-")
        self.cr_namespace = backend_settings.get("cr_namespace", "waldur-system")
        self.default_role = backend_settings.get("default_role", "readwrite")
        self.keycloak_use_user_id = backend_settings.get("keycloak_use_user_id", True)
        self.sync_users_to_cr = backend_settings.get("sync_users_to_cr", False)
        self.cr_user_identity_field = backend_settings.get(
            "cr_user_identity_field", "email"
        )
        self.cr_user_identity_lowercase = backend_settings.get(
            "cr_user_identity_lowercase", False
        )

        # Defaults the order may override where the offering allows it. A
        # resource created before these existed carries no attribute, so it
        # resolves to what the offering says: quotas managed, security level
        # left to the operator. Kept raw here and parsed when a resource is
        # created, so a malformed setting is reported against the order that
        # hits it.
        self.quota_enabled = backend_settings.get("quota_enabled")
        self.allow_quota_override = backend_settings.get("allow_quota_override")
        self.default_security_level = backend_settings.get("default_security_level")
        self.allowed_security_levels = backend_settings.get("allowed_security_levels")

        self.namespace_labels: dict[str, str] = backend_settings.get(
            "namespace_labels", {}
        )
        self.namespace_annotations: dict[str, str] = backend_settings.get(
            "namespace_annotations", {}
        )

        # Configurable mappings with sensible defaults
        self.role_mapping: dict[str, str] = {
            **DEFAULT_ROLE_MAPPING,
            **backend_settings.get("role_mapping", {}),
        }
        self.component_quota_mapping: dict[str, str] = {
            **DEFAULT_COMPONENT_QUOTA_MAPPING,
            **backend_settings.get("component_quota_mapping", {}),
        }

        logger.info(
            "Initialized K8s UT namespace backend (CR namespace: %s, prefix: %s)",
            self.cr_namespace,
            self.namespace_prefix,
        )

    # ── Keycloak group naming ──────────────────────────────────────────────

    def _get_keycloak_group_name(self, resource_slug: str, ns_role: str) -> str:
        """Generate Keycloak group name for a namespace role."""
        return f"ns_{resource_slug}_{ns_role}"

    def _get_keycloak_group_names(self, resource_slug: str) -> dict[str, str]:
        """Get all 3 Keycloak group names for a resource."""
        return {
            role: self._get_keycloak_group_name(resource_slug, role)
            for role in NS_ROLES
        }

    # ── Keycloak group management ──────────────────────────────────────────

    def _create_keycloak_groups(self, resource_slug: str) -> dict[str, str]:
        """Create 3 Keycloak groups for namespace RBAC and return {role: group_id}."""
        if not self.keycloak_client:
            return {}

        group_ids = {}
        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            try:
                existing = self.keycloak_client.get_group_by_name(group_name)
                if existing:
                    group_ids[role] = existing["id"]
                    logger.info("Using existing Keycloak group: %s", group_name)
                else:
                    description = f"Namespace {resource_slug} {role} access"
                    group_id = self.keycloak_client.create_group(group_name, description)
                    group_ids[role] = group_id
                    logger.info("Created Keycloak group: %s", group_name)
            except Exception as e:
                logger.error("Failed to create Keycloak group %s: %s", group_name, e)
                raise BackendError(
                    f"Failed to create Keycloak group {group_name}: {e}"
                ) from e
        return group_ids

    def _delete_keycloak_groups(self, resource_slug: str) -> None:
        """Delete all 3 Keycloak groups for a resource."""
        if not self.keycloak_client:
            return

        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            try:
                group = self.keycloak_client.get_group_by_name(group_name)
                if group:
                    self.keycloak_client.delete_group(group["id"])
                    logger.info("Deleted Keycloak group: %s", group_name)
            except Exception as e:
                logger.warning("Failed to delete Keycloak group %s: %s", group_name, e)

    def _get_keycloak_group_ids(self, resource_slug: str) -> dict[str, str]:
        """Look up existing group IDs for all 3 roles. Returns {role: group_id}."""
        if not self.keycloak_client:
            return {}

        group_ids = {}
        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            group = self.keycloak_client.get_group_by_name(group_name)
            if group:
                group_ids[role] = group["id"]
        return group_ids

    # ── Order-form attributes ──────────────────────────────────────────────

    @staticmethod
    def _resource_attributes(waldur_resource: WaldurResource) -> dict:
        """A resource's order attributes as a plain dict.

        The generated client holds ``UNSET`` for a resource without attributes
        and a ``ResourceAttributes`` otherwise, which supports ``[]`` but not
        ``.get()``.
        """
        attributes = waldur_resource.attributes
        if isinstance(attributes, Unset):
            return {}
        return attributes.to_dict()

    @staticmethod
    def _parse_bool_attribute(source: str, value: object) -> bool:
        """Read a boolean switch, refusing anything ambiguous.

        A non-empty string is truthy in Python, so reading ``"false"`` straight
        out of the attributes bag would enable what the user turned off. Only
        the spellings a form can actually produce are accepted; anything else
        fails the order rather than being guessed at.

        Args:
            source: what the value came from, named in the error.
            value: the switch as the order form or the setting recorded it.

        Raises:
            BackendError: naming the source and the value it rejected.
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            normalised = value.strip().lower()
            if normalised in _TRUE_STRINGS:
                return True
            if normalised in _FALSE_STRINGS:
                return False
        msg = f"{source} must be true or false, got {_short_repr.repr(value)}"
        raise BackendError(msg)

    def _offering_quota_enabled(self) -> bool:
        """The offering's ``quota_enabled`` setting; unset or empty means true."""
        if _is_blank(self.quota_enabled):
            return True
        return self._parse_bool_attribute(
            f"The '{QUOTA_ENABLED_ATTRIBUTE}' backend setting", self.quota_enabled
        )

    def _quota_override_allowed(self) -> bool:
        """The offering's ``allow_quota_override`` setting; unset or empty means false."""
        if _is_blank(self.allow_quota_override):
            return False
        return self._parse_bool_attribute(
            "The 'allow_quota_override' backend setting", self.allow_quota_override
        )

    def _resolve_quota_enabled(self, waldur_resource: WaldurResource) -> bool:
        """Whether the ManagedNamespace should enforce this namespace's quota.

        The offering's ``quota_enabled`` setting is the default, and an offering
        that never mentioned quotas keeps them enforced. The order's
        ``quota_enabled`` attribute may differ from it only where the offering
        sets ``allow_quota_override``: Waldur stores order attributes the form
        does not declare, so without the gate any orderer could switch off the
        cap on a limit-billed offering through the API.

        An attribute equal to the default is accepted either way. An absent,
        ``null`` or whitespace-only attribute is no request at all and gets the
        default. An explicit ``null`` setting means ``true`` rather than off.

        Raises:
            BackendError: if a value cannot be read as a boolean, or the order
                asks for a value other than the default on an offering that does
                not allow it.
        """
        default = self._offering_quota_enabled()

        attributes = self._resource_attributes(waldur_resource)
        value = attributes.get(QUOTA_ENABLED_ATTRIBUTE)
        if _is_blank(value):
            return default

        requested = self._parse_bool_attribute(
            f"Attribute '{QUOTA_ENABLED_ATTRIBUTE}'", value
        )
        if requested == default:
            return requested

        if not self._quota_override_allowed():
            msg = (
                f"Attribute '{QUOTA_ENABLED_ATTRIBUTE}' is {_short_repr.repr(value)}, but "
                "this offering does not allow orders to change quota enforcement "
                f"(offering default: {QUOTA_ENABLED_ATTRIBUTE}={str(default).lower()}, "
                "allow_quota_override is off)"
            )
            raise BackendError(msg)
        return requested

    @staticmethod
    def _normalise_security_level(source: str, value: object) -> str:
        """Trim and lowercase a level, refusing anything the CRD would not accept.

        Raises:
            BackendError: naming the source and the value it rejected.
        """
        level = value.strip().lower() if isinstance(value, str) else None
        if level is None or level not in SECURITY_LEVELS:
            msg = (
                f"{source} must be one of {', '.join(SECURITY_LEVELS)}, "
                f"got {_short_repr.repr(value)}"
            )
            raise BackendError(msg)
        return level

    def _resolve_security_level(self, waldur_resource: WaldurResource) -> Optional[str]:
        """The Pod Security level to apply, or None to leave it to the operator.

        The offering's ``default_security_level`` applies unless the order's
        ``security_level`` attribute names another level, which it may do only
        for a level in ``allowed_security_levels``. The default allow-list is
        empty, and ``privileged`` is never implied. Waldur stores order
        attributes the form does not declare, so without the gate any orderer
        could ask for ``privileged`` -- node-level access on a shared cluster.

        The default level is offering-admin configuration and is only checked
        for being a level at all. An attribute equal to it is always accepted.
        Returns None when neither names a level: the operator then applies the
        installation's own default, which the agent cannot know and must not
        guess.

        Raises:
            BackendError: if a setting is not a valid level, or the order names
                a level the offering does not allow.
        """
        default: Optional[str] = None
        if not _is_blank(self.default_security_level):
            default = self._normalise_security_level(
                "The 'default_security_level' backend setting",
                self.default_security_level,
            )

        attributes = self._resource_attributes(waldur_resource)
        value = attributes.get(SECURITY_LEVEL_ATTRIBUTE)
        if _is_blank(value):
            return default

        # A non-string value matches no level and falls through to the refusal.
        requested = value.strip().lower() if isinstance(value, str) else None
        if requested is not None and requested == default:
            return default

        configured = self.allowed_security_levels or []
        if not isinstance(configured, list):
            msg = (
                "The 'allowed_security_levels' backend setting must be a list of "
                f"{', '.join(SECURITY_LEVELS)}, got {_short_repr.repr(configured)}"
            )
            raise BackendError(msg)
        allowed = {
            self._normalise_security_level(
                "Each entry of the 'allowed_security_levels' backend setting", level
            )
            for level in configured
        }
        if requested is not None and requested in allowed:
            return requested

        if default is not None:
            allowed.add(default)
        if allowed:
            allowed_text = ", ".join(level for level in SECURITY_LEVELS if level in allowed)
            msg = (
                f"Attribute '{SECURITY_LEVEL_ATTRIBUTE}' is {_short_repr.repr(value)}, "
                "but this offering "
                f"allows only these security levels: {allowed_text}"
            )
        else:
            msg = (
                f"Attribute '{SECURITY_LEVEL_ATTRIBUTE}' is {_short_repr.repr(value)}, "
                "but this offering "
                "does not let orders choose a security level (allowed levels: none)"
            )
        raise BackendError(msg)

    @staticmethod
    def _unsupported_spec_fields(
        created: object, quota_enabled: bool, security_level: Optional[str]
    ) -> list[str]:
        """Fields the API server dropped from the CR the agent just created.

        A CRD that does not declare a field prunes it on admission without an
        error, so an operator too old for these switches would silently enforce
        a quota the order turned off, or apply its default level instead of the
        one requested. Only the persisted object can tell.

        ``enabled: true`` is not checked: a CRD without the field enforces the
        quota anyway, which is what was asked for.
        """
        spec = created.get("spec") if isinstance(created, dict) else None
        spec = spec if isinstance(spec, dict) else {}
        missing = []
        if quota_enabled is False and (spec.get("quota") or {}).get("enabled") is not False:
            missing.append("spec.quota.enabled")
        if (
            security_level is not None
            and (spec.get("security") or {}).get("level") != security_level
        ):
            missing.append("spec.security.level")
        return missing

    # ── Quota conversion ───────────────────────────────────────────────────

    @staticmethod
    def _validate_namespace_name(name: str) -> None:
        """Validate that name is a valid RFC 1123 label.

        Raises BackendError if the name is empty, too long, or contains
        invalid characters.
        """
        if not name:
            msg = "Namespace name must not be empty"
            raise BackendError(msg)
        if len(name) > _NS_NAME_MAX_LEN:
            raise BackendError(
                f"Namespace name '{name}' exceeds {_NS_NAME_MAX_LEN} characters "
                f"({len(name)})"
            )
        if not _NS_NAME_RE.match(name):
            raise BackendError(
                f"Namespace name '{name}' is not a valid RFC 1123 label: "
                "must be lowercase alphanumeric or '-', must start and end "
                "with an alphanumeric character"
            )

    @staticmethod
    def _validate_limits(limits: dict[str, int]) -> None:
        """Raise BackendError if any limit value is negative."""
        negative = {k: v for k, v in limits.items() if v < 0}
        if negative:
            raise BackendError(
                f"Negative resource limits are not allowed: {negative}"
            )

    def _waldur_limits_to_quota(self, limits: dict[str, int]) -> dict[str, str]:
        """Convert Waldur component limits to ManagedNamespace quota spec.

        Values are formatted as K8s resource quantities.
        """
        self._validate_limits(limits)
        quota = {}
        for component_key, value in limits.items():
            component_config = self.backend_components.get(component_key, {})
            component_type = component_config.get("type", component_key)
            quota_field = self.component_quota_mapping.get(component_type)
            if quota_field:
                if component_type in {"ram", "storage"}:
                    quota[quota_field] = f"{value}Gi"
                else:
                    quota[quota_field] = str(value)
        return quota

    # ── BaseBackend abstract methods ───────────────────────────────────────

    def ping(self, raise_exception: bool = False) -> bool:
        """Check K8s cluster and Keycloak connectivity."""
        try:
            k8s_ok = self.k8s_client.ping()
            if not k8s_ok:
                if raise_exception:
                    msg = "Failed to ping Kubernetes cluster"
                    raise BackendError(msg)  # noqa: TRY301
                return False

            if self.keycloak_client:
                kc_ok = self.keycloak_client.ping()
                if not kc_ok:
                    if raise_exception:
                        msg = "Failed to ping Keycloak server"
                        raise BackendError(msg)  # noqa: TRY301
                    return False

            return True
        except BackendError:
            if raise_exception:
                raise
            return False
        except Exception as e:
            if raise_exception:
                raise
            logger.error("Failed to ping K8s/Keycloak: %s", e)
            return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about the backend."""
        fmt = "{:<30} = {:<10}"

        logger.info("=" * 60)
        logger.info("K8s UT Namespace Backend Diagnostics")
        logger.info("=" * 60)

        logger.info(fmt.format("CR namespace", self.cr_namespace))
        logger.info(fmt.format("Namespace prefix", self.namespace_prefix))
        logger.info(
            fmt.format("Keycloak enabled", "Yes" if self.keycloak_client else "No")
        )
        if self.namespace_labels:
            logger.info(fmt.format("Namespace labels", str(self.namespace_labels)))
        if self.namespace_annotations:
            logger.info(
                fmt.format("Namespace annotations", str(self.namespace_annotations))
            )

        logger.info("")
        logger.info("Backend components configuration:")
        logger.info(pprint.pformat(self.backend_components))
        logger.info("")

        try:
            self.ping(raise_exception=True)
            logger.info("K8s cluster connection successful")

            namespaces = self.k8s_client.list_managed_namespaces()
            logger.info("Found %d managed namespaces", len(namespaces))

            if self.keycloak_client:
                logger.info("Keycloak connection successful")

            return True
        except BackendError as err:
            logger.error("Unable to connect to K8s/Keycloak: %s", err)
            return False
        except Exception as e:
            logger.error("Unexpected error during diagnostics: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of available resource components."""
        return list(self.component_quota_mapping.keys())

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect and convert resource limits."""
        waldur_limits = waldur_resource.limits.to_dict()
        # For this backend, backend limits == Waldur limits (unit_factor = 1)
        backend_limits = dict(waldur_limits)
        return backend_limits, waldur_limits

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Return empty report — usage reporting is not supported.

        Billing is based on limits (allocation), not actual consumption.
        """
        del resource_backend_ids
        return {}

    @staticmethod
    def _parse_k8s_quantity(value: str) -> int:
        """Parse a K8s resource quantity string to an integer."""
        value = str(value)
        if value.endswith("Gi"):
            return int(value[:-2])
        if value.endswith("Mi"):
            return int(value[:-2]) // 1024
        if value.endswith("m"):
            return int(value[:-1]) // 1000
        try:
            return int(value)
        except ValueError:
            return 0

    @staticmethod
    def _parse_ready_condition(status: dict) -> dict:
        """Extract the Ready condition from a ManagedNamespace status.

        Returns a dict with keys: ready (bool or None), message (str).
        None means the condition is not yet set.
        """
        conditions = status.get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "Ready":
                cond_status = cond.get("status", "Unknown")
                return {
                    "ready": True
                    if cond_status == "True"
                    else (False if cond_status == "False" else None),
                    "message": cond.get("message", ""),
                }
        return {"ready": None, "message": ""}

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get K8s-specific metadata for the resource."""
        metadata = {}
        try:
            cr = self.k8s_client.get_managed_namespace(resource_backend_id)
            if cr:
                metadata["name"] = cr.get("metadata", {}).get("name", "")
                metadata["quota"] = cr.get("spec", {}).get("quota", {})
                status = cr.get("status") or {}
                metadata["status"] = self._parse_ready_condition(status)
                # The level the operator actually applied, not the one asked
                # for: the operator resolves an unset spec.security.level to the
                # installation default, so only status can say what is in force.
                security_level = status.get("securityLevel")
                requested = ((cr.get("spec") or {}).get("security") or {}).get("level")
                if not security_level and requested and metadata["status"]["ready"]:
                    # A ready namespace whose operator never reports the level:
                    # its controller predates spec.security.level (the CRD may
                    # have been upgraded ahead of it) and applies its default.
                    logger.warning(
                        "ManagedNamespace %s requests security level %s but the "
                        "operator reports no applied level; it may be older than "
                        "1.4.3 and running the namespace at its own default",
                        resource_backend_id,
                        requested,
                    )
                if security_level:
                    metadata["security_level"] = security_level
                    if requested and requested != security_level:
                        logger.warning(
                            "ManagedNamespace %s requests security level %s but the "
                            "operator applied %s",
                            resource_backend_id,
                            requested,
                            security_level,
                        )
        except Exception as e:
            logger.warning("Failed to get metadata for %s: %s", resource_backend_id, e)
        return metadata

    # ── Resource lifecycle ─────────────────────────────────────────────────

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Validate resource before creation."""
        del user_context
        if not waldur_resource.slug:
            raise BackendError(
                f"Resource {waldur_resource.uuid} has no slug, "
                "cannot create ManagedNamespace"
            )
        ns_name = f"{self.namespace_prefix}{waldur_resource.slug}"
        self._validate_namespace_name(ns_name)

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Create ManagedNamespace CR and Keycloak groups."""
        del resource_backend_id  # We generate our own name

        self._pre_create_resource(waldur_resource, user_context)

        resource_slug = waldur_resource.slug
        ns_name = f"{self.namespace_prefix}{resource_slug}"

        logger.info(
            "Creating K8s UT namespace %s for resource %s",
            ns_name,
            waldur_resource.uuid.hex,
        )

        # Resolve the order attributes first: they can reject the order, and
        # refusing it before any group is created leaves nothing to undo.
        #
        # The switch is always sent explicitly. The CRD defaults it to true, so
        # omitting it would silently re-enable a quota that was turned off. The
        # quantities go along even when it is off: they are what the namespace
        # is entitled to, and the switch is what decides whether they are
        # enforced. A later re-enable then needs no second write to be correct.
        quota_enabled = self._resolve_quota_enabled(waldur_resource)
        security_level = self._resolve_security_level(waldur_resource)

        # 1. Create Keycloak groups (3 roles)
        self._create_keycloak_groups(resource_slug)

        # 2. Build ManagedNamespace spec
        _, waldur_limits = self._collect_resource_limits(waldur_resource)
        quota = self._waldur_limits_to_quota(waldur_limits)

        spec: dict = {
            "name": ns_name,
            "quota": {**quota, "enabled": quota_enabled},
        }
        if security_level is not None:
            spec["security"] = {"level": security_level}

        # Add group references per role (CRD uses adminGroups/rwGroups/roGroups)
        if self.keycloak_client:
            for role in NS_ROLES:
                group_name = self._get_keycloak_group_name(resource_slug, role)
                cr_field = NS_ROLE_TO_CR_GROUP_FIELD[role]
                spec[cr_field] = [group_name]

        # Add namespace labels and annotations
        if self.namespace_labels:
            spec["labels"] = dict(self.namespace_labels)
        if self.namespace_annotations:
            spec["annotations"] = dict(self.namespace_annotations)

        # Add owner info as object (CRD expects {orgID, projectID})
        owner: dict[str, str] = {}
        if waldur_resource.customer_uuid:
            owner["orgID"] = str(waldur_resource.customer_uuid)
        if waldur_resource.project_uuid:
            owner["projectID"] = str(waldur_resource.project_uuid)
        if owner:
            spec["owner"] = owner

        # 3. Create ManagedNamespace CR
        try:
            created = self.k8s_client.create_managed_namespace(ns_name, spec)
        except BackendError:
            # Cleanup Keycloak groups on failure
            logger.error("Failed to create CR, cleaning up Keycloak groups")
            self._delete_keycloak_groups(resource_slug)
            raise

        # 4. Fail closed if the CRD pruned a switch the order relies on.
        unsupported = self._unsupported_spec_fields(created, quota_enabled, security_level)
        if unsupported:
            fields = ", ".join(unsupported)
            logger.error(
                "ManagedNamespace %s was stored without %s; removing it again",
                ns_name,
                fields,
            )
            outcome = "so the namespace was removed again"
            try:
                self.k8s_client.delete_managed_namespace(ns_name)
            except Exception as e:
                logger.error("Failed to delete ManagedNamespace %s: %s", ns_name, e)
                # Left behind, it runs without the requested setting -- possibly at
                # a looser security level -- so whoever reads the error must know.
                outcome = (
                    f"and removing it failed ({e}): delete ManagedNamespace {ns_name} "
                    f"in {self.cr_namespace} by hand"
                )
            self._delete_keycloak_groups(resource_slug)
            msg = (
                f"The ManagedNamespace operator does not support {fields}: the "
                f"cluster's CRD dropped it from {ns_name}, {outcome}. Upgrade the "
                "operator, or order without that setting."
            )
            raise BackendError(msg)

        return BackendResourceInfo(
            backend_id=ns_name,
            limits=waldur_limits,
        )

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> None:
        """Delete ManagedNamespace CR and Keycloak groups."""
        del kwargs
        ns_name = waldur_resource.backend_id
        if not ns_name or not ns_name.strip():
            logger.warning(
                "Resource %s has no backend_id, skipping deletion",
                waldur_resource.uuid,
            )
            return

        resource_slug = waldur_resource.slug or ns_name.removeprefix(self.namespace_prefix)

        try:
            self.k8s_client.delete_managed_namespace(ns_name)
            logger.info("Deleted ManagedNamespace CR: %s", ns_name)
        except Exception as e:
            logger.error("Failed to delete ManagedNamespace %s: %s", ns_name, e)
            raise BackendError(f"Failed to delete ManagedNamespace: {e}") from e

        self._delete_keycloak_groups(resource_slug)

    def set_resource_limits(
        self,
        resource_backend_id: str,
        limits: dict[str, int],
    ) -> None:
        """Patch ManagedNamespace CR spec.quota.

        While a pause or downscale is in force, the new quantities go into the
        quota it saved, not into the spec: patching the spec would lift the pause
        until the next status sync, and the restore would later bring back the
        quantities from before the update. The patch carries the CR's
        ``resourceVersion``, so a restore that removed the saved quota in the
        meantime makes it fail instead of leaving the update in a dropped
        annotation.
        """
        quota = self._waldur_limits_to_quota(limits)
        try:
            cr = self.k8s_client.get_managed_namespace(resource_backend_id)
            metadata = (cr.get("metadata") or {}) if isinstance(cr, dict) else {}
            annotations = metadata.get("annotations") or {}
            saved = self._saved_quota(annotations)
            if saved is None:
                patch: dict = {"spec": {"quota": quota}}
            else:
                patch = {
                    "metadata": {
                        "annotations": {
                            QUOTA_BEFORE_PAUSE_ANNOTATION: json.dumps(
                                {**saved, **quota}, sort_keys=True
                            ),
                            LEGACY_QUOTA_ENABLED_ANNOTATION: None,
                        }
                    }
                }
                if metadata.get("resourceVersion"):
                    patch["metadata"]["resourceVersion"] = metadata["resourceVersion"]
                logger.info(
                    "Namespace %s is paused or downscaled; new limits %s are saved "
                    "for the restore",
                    resource_backend_id,
                    quota,
                )
            self.k8s_client.patch_managed_namespace(resource_backend_id, patch)
        except Exception as e:
            logger.error("Failed to set limits for %s: %s", resource_backend_id, e)
            raise BackendError(f"Failed to set limits: {e}") from e

    # ── User management ────────────────────────────────────────────────────

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add users to correct Keycloak groups and/or ManagedNamespace CR.

        Uses `user_roles` kwarg to determine which group each user belongs to.
        Uses `user_attributes` kwarg to populate CR user fields when
        sync_users_to_cr is enabled. The field used for user identity is
        configured via `cr_user_identity_field` backend setting.
        Also reconciles role changes for all users in user_roles.
        """
        user_roles: dict[str, str] = kwargs.get("user_roles", {})
        user_attributes: dict[str, dict] = kwargs.get("user_attributes", {})

        # Sync user identities to the ManagedNamespace CR if enabled.
        # Only run when user_attributes is provided (team sync), not for
        # service account or course account syncs which would overwrite
        # the CR with empty lists.
        if self.sync_users_to_cr and "user_attributes" in kwargs:
            self._sync_users_to_cr(waldur_resource, user_roles, user_attributes)

        if not self.keycloak_client:
            logger.info("Keycloak not configured, skipping Keycloak user management")
            return user_ids

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        group_ids = self._get_keycloak_group_ids(resource_slug)

        if not group_ids:
            logger.warning("No Keycloak groups found for resource %s", resource_slug)
            return set()

        added_users = set()

        # Reconcile ALL users that have roles (handles both new users and role changes)
        users_to_reconcile = {
            username: role
            for username, role in user_roles.items()
            if username in user_ids or username  # include all users with roles
        }

        for username, waldur_role in users_to_reconcile.items():
            target_ns_role = self.role_mapping.get(waldur_role, self.default_role)
            target_group_id = group_ids.get(target_ns_role)

            if not target_group_id:
                logger.warning(
                    "No group found for role %s, skipping user %s",
                    target_ns_role,
                    username,
                )
                continue

            kc_user = self.keycloak_client.find_user(username, self.keycloak_use_user_id)
            if not kc_user:
                logger.warning("User %s not found in Keycloak", username)
                continue

            kc_user_id = kc_user["id"]

            # Remove from wrong groups
            for role, gid in group_ids.items():
                if (
                    role != target_ns_role
                    and self.keycloak_client.is_user_in_group(kc_user_id, gid)
                ):
                    try:
                        self.keycloak_client.remove_user_from_group(kc_user_id, gid)
                        logger.info(
                            "Removed %s from group %s (role change)",
                            username,
                            self._get_keycloak_group_name(resource_slug, role),
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to remove %s from group: %s", username, e
                        )

            # Add to correct group
            if not self.keycloak_client.is_user_in_group(kc_user_id, target_group_id):
                try:
                    self.keycloak_client.add_user_to_group(kc_user_id, target_group_id)
                    logger.info(
                        "Added %s to group %s (%s)",
                        username,
                        self._get_keycloak_group_name(resource_slug, target_ns_role),
                        target_ns_role,
                    )
                except Exception as e:
                    logger.warning("Failed to add %s to group: %s", username, e)
                    continue

            if username in user_ids:
                added_users.add(username)

        return added_users

    def _sync_users_to_cr(
        self,
        waldur_resource: WaldurResource,
        user_roles: dict[str, str],
        user_attributes: dict[str, dict],
    ) -> None:
        """Patch the ManagedNamespace CR with per-role user identity lists.

        Groups all users by their mapped namespace role and sets the
        adminUsers/rwUsers/roUsers fields on the CR spec. The identity
        value is determined by the cr_user_identity_field setting.
        """
        ns_name = waldur_resource.backend_id
        if not ns_name or not ns_name.strip():
            logger.warning("Resource has no backend_id, cannot sync users to CR")
            return

        identity_field = self.cr_user_identity_field

        # Build per-role identity lists from ALL users with roles
        role_identities: dict[str, list[str]] = {role: [] for role in NS_ROLES}
        for username, waldur_role in user_roles.items():
            attrs = user_attributes.get(username, {})
            identity = attrs.get(identity_field)
            if not identity:
                logger.warning(
                    "No '%s' attribute for user %s, skipping CR user sync",
                    identity_field,
                    username,
                )
                continue
            identity_str = str(identity)
            if self.cr_user_identity_lowercase:
                identity_str = identity_str.lower()
            ns_role = self.role_mapping.get(waldur_role, self.default_role)
            role_identities[ns_role].append(identity_str)

        # Build the patch with all role fields (empty lists clear removed users)
        spec_patch: dict = {}
        for role in NS_ROLES:
            cr_field = NS_ROLE_TO_CR_USER_FIELD[role]
            spec_patch[cr_field] = sorted(role_identities[role])

        try:
            self.k8s_client.patch_managed_namespace(ns_name, {"spec": spec_patch})
            logger.info(
                "Synced users to CR %s: %s",
                ns_name,
                {k: len(v) for k, v in spec_patch.items()},
            )
        except Exception as e:
            logger.error("Failed to sync users to CR %s: %s", ns_name, e)

    def add_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Add a single user to the default role group."""
        del kwargs
        if not self.keycloak_client:
            return True

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        group_ids = self._get_keycloak_group_ids(resource_slug)
        target_group_id = group_ids.get(self.default_role)

        if not target_group_id:
            msg = f"No Keycloak group for default role {self.default_role} on {resource_slug}"
            raise BackendError(msg)

        kc_user = self.keycloak_client.find_user(username, self.keycloak_use_user_id)
        if not kc_user:
            msg = f"User {username} not found in Keycloak (no first sign-in yet)"
            raise UserNotProvisionedError(msg)

        try:
            self.keycloak_client.add_user_to_group(kc_user["id"], target_group_id)
        except Exception as e:
            msg = f"Failed to add user {username} to Keycloak group: {e}"
            raise BackendError(msg) from e
        return True

    def remove_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Remove user from ALL 3 Keycloak groups."""
        del kwargs
        if not self.keycloak_client:
            return True

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        group_ids = self._get_keycloak_group_ids(resource_slug)

        kc_user = self.keycloak_client.find_user(username, self.keycloak_use_user_id)
        if not kc_user:
            logger.warning("User %s not found in Keycloak", username)
            return True  # User not in Keycloak, nothing to remove

        kc_user_id = kc_user["id"]
        for role, gid in group_ids.items():
            try:
                if self.keycloak_client.is_user_in_group(kc_user_id, gid):
                    self.keycloak_client.remove_user_from_group(kc_user_id, gid)
                    logger.info(
                        "Removed %s from group %s",
                        username,
                        self._get_keycloak_group_name(resource_slug, role),
                    )
            except Exception as e:
                logger.warning("Failed to remove %s from group: %s", username, e)

        return True

    # ── Pull resource (for membership sync) ────────────────────────────────

    def pull_resource(
        self, waldur_resource: WaldurResource
    ) -> Optional[BackendResourceInfo]:
        """Pull resource data including users from all 3 Keycloak groups."""
        ns_name = waldur_resource.backend_id
        if not ns_name:
            logger.warning("Backend ID is missing for resource %s", waldur_resource.name)
            return None

        try:
            cr = self.k8s_client.get_managed_namespace(ns_name)
            if cr is None:
                logger.warning("ManagedNamespace %s not found", ns_name)
                return None

            # Check readiness
            ready_info = self._parse_ready_condition(cr.get("status", {}))
            if ready_info["ready"] is False:
                logger.warning(
                    "ManagedNamespace %s is not ready: %s",
                    ns_name,
                    ready_info["message"],
                )

            # Collect users from all Keycloak groups
            users = self._list_all_keycloak_users(waldur_resource)

            # Get usage report
            report = self._get_usage_report([ns_name])
            usage = report.get(ns_name, {"TOTAL_ACCOUNT_USAGE": {}})

            return BackendResourceInfo(
                backend_id=ns_name,
                users=users,
                usage=usage,
                backend_metadata={"status": ready_info},
            )
        except Exception as e:
            if self.strict_pull_requested():
                # Returning None would tell a strict caller this namespace has
                # no users, which the teardown reads as "safe to release".
                raise
            logger.exception("Error pulling resource %s: %s", ns_name, e)
            return None

    def _list_all_keycloak_users(self, waldur_resource: WaldurResource) -> list[str]:
        """List all users across all 3 Keycloak groups (union)."""
        if not self.keycloak_client:
            return []

        resource_slug = (
            waldur_resource.slug
            or waldur_resource.backend_id.removeprefix(self.namespace_prefix)
        )
        all_users = set()

        for role in NS_ROLES:
            group_name = self._get_keycloak_group_name(resource_slug, role)
            group = self.keycloak_client.get_group_by_name(group_name)
            if group:
                members = self.keycloak_client.get_group_members(group["id"])
                for member in members:
                    if self.keycloak_use_user_id:
                        uid = member.get("id", "")
                    else:
                        uid = member.get("username", "")
                    if uid:
                        all_users.add(uid)

        return list(all_users)

    # ── Status operations ──────────────────────────────────────────────────

    def _force_quota(self, resource_backend_id: str, quota: dict[str, str]) -> None:
        """Patch reduced quantities in and switch quota enforcement on.

        Pause and downscale work by shrinking the quota, which does nothing on a
        namespace whose quota is not enforced -- so an order allowed
        ``quota_enabled: false`` would escape both. Enforcement is therefore
        forced on, and the whole quota in force before is kept in an annotation
        for ``restore_resource``: on a usage-billed offering no limit update ever
        follows a restore, so the quantities must come back from there too.

        A second pause or downscale keeps the snapshot the first one took, which
        is the quota from before either of them.
        """
        cr = self.k8s_client.get_managed_namespace(resource_backend_id)
        if cr is None:
            msg = f"ManagedNamespace {resource_backend_id} not found"
            raise BackendError(msg)

        patch: dict = {"spec": {"quota": {**quota, "enabled": True}}}
        annotations = (cr.get("metadata") or {}).get("annotations") or {}
        if (
            QUOTA_BEFORE_PAUSE_ANNOTATION not in annotations
            and LEGACY_QUOTA_ENABLED_ANNOTATION not in annotations
        ):
            snapshot = dict((cr.get("spec") or {}).get("quota") or {})
            patch["metadata"] = {
                "annotations": {
                    QUOTA_BEFORE_PAUSE_ANNOTATION: json.dumps(snapshot, sort_keys=True)
                }
            }
        self.k8s_client.patch_managed_namespace(resource_backend_id, patch)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale by patching CR quota to minimal values, enforced."""
        try:
            minimal_quota = {"cpu": "1", "memory": "1Gi", "storage": "1Gi"}
            self._force_quota(resource_backend_id, minimal_quota)
            logger.info("Downscaled namespace %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to downscale %s: %s", resource_backend_id, e)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause by patching CR quota to zero, enforced."""
        try:
            zero_quota = {"cpu": "0", "memory": "0Gi", "storage": "0Gi"}
            self._force_quota(resource_backend_id, zero_quota)
            logger.info("Paused namespace %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to pause %s: %s", resource_backend_id, e)
            return False

    def _saved_quota(self, annotations: dict) -> Optional[dict]:
        """The quota a pause or downscale recorded, or None if nothing was recorded.

        The legacy annotation recorded only ``enabled``; the quantities from
        before such a pause are not known, so they are left as they are.
        """
        raw = annotations.get(QUOTA_BEFORE_PAUSE_ANNOTATION)
        if raw is not None:
            snapshot = json.loads(raw)
            if not isinstance(snapshot, dict):
                msg = (
                    f"Annotation '{QUOTA_BEFORE_PAUSE_ANNOTATION}' must hold a JSON "
                    f"object, got {_short_repr.repr(raw)}"
                )
                raise BackendError(msg)
            return snapshot
        legacy = annotations.get(LEGACY_QUOTA_ENABLED_ANNOTATION)
        if legacy is not None:
            return {
                "enabled": self._parse_bool_attribute(
                    f"Annotation '{LEGACY_QUOTA_ENABLED_ANNOTATION}'", legacy
                )
            }
        return None

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Write back the quota a pause or downscale recorded.

        The quantities and ``enabled`` are both restored, and a quantity the
        namespace did not have before the pause is removed again (a merge patch
        with ``null``), so the quota ends up as it was. Without the annotation --
        nothing was paused -- this is a no-op.

        The saved ``enabled`` is checked against the offering's settings as they
        are now: if the offering has since stopped allowing orders to differ from
        its default, the default is restored instead.
        """
        try:
            cr = self.k8s_client.get_managed_namespace(resource_backend_id)
            annotations = ((cr or {}).get("metadata") or {}).get("annotations") or {}
            snapshot = self._saved_quota(annotations)
            if snapshot is None:
                logger.info("Restore for namespace %s is a no-op", resource_backend_id)
                return True

            # The CRD defaults the field to true, so absent means enforced.
            saved_enabled = snapshot.get("enabled")
            enabled = (
                True
                if saved_enabled is None
                else self._parse_bool_attribute(
                    f"Annotation '{QUOTA_BEFORE_PAUSE_ANNOTATION}' enabled", saved_enabled
                )
            )
            default = self._offering_quota_enabled()
            if enabled != default and not self._quota_override_allowed():
                logger.warning(
                    "Namespace %s had quota enforcement %s before it was paused, but "
                    "the offering no longer allows that; restoring the default (%s)",
                    resource_backend_id,
                    "on" if enabled else "off",
                    "on" if default else "off",
                )
                enabled = default

            quota: dict = {}
            if QUOTA_BEFORE_PAUSE_ANNOTATION in annotations:
                current = ((cr or {}).get("spec") or {}).get("quota") or {}
                quota = {key: None for key in current if key not in snapshot}
                quota.update({k: v for k, v in snapshot.items() if k != "enabled"})
            quota["enabled"] = enabled

            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {
                    "metadata": {
                        "annotations": {
                            QUOTA_BEFORE_PAUSE_ANNOTATION: None,
                            LEGACY_QUOTA_ENABLED_ANNOTATION: None,
                        }
                    },
                    "spec": {"quota": quota},
                },
            )
            logger.info(
                "Restored namespace %s: quota %s, enforcement %s",
                resource_backend_id,
                {k: v for k, v in quota.items() if k != "enabled"},
                "on" if enabled else "off",
            )
            return True
        except Exception as e:
            logger.error("Failed to restore %s: %s", resource_backend_id, e)
            return False


class K8sNamespaceBackend(K8sUtNamespaceBackend):
    """The same backend under the deployment-neutral ``k8s-namespace`` name.

    The ManagedNamespace operator is not specific to one deployment, so an
    offering should not have to name one site to use it. Settings, settings
    schema and behaviour are the parent's, unchanged. The subclass changes two
    things only:

    - the raw processor class path the agent registers in Waldur
      (``module.ClassName``) reads ``...backend.K8sNamespaceBackend`` instead
      of ``...backend.K8sUtNamespaceBackend``;
    - ``backend_type``, the name the core's ``BaseBackend`` log lines use for
      the backend (the paths that log it today -- the base create/delete and
      the cycle preflight -- are overridden or unused by this plugin).

    Anything Waldur derives from the reported class path (its
    ``reported_backend_name``, used e.g. by the limit-precision advisory) takes
    only the plugin package, so under either name Waldur's derived backend name
    is ``k8s-ut-namespace``. ``backend_type`` itself is not sent to Waldur.
    """

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Initialize the backend and report the ``k8s-namespace`` type."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "k8s-namespace"
