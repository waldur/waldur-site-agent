"""K8s UT ManagedNamespace backend for Waldur Site Agent."""

import datetime
import pprint
import re
from typing import Optional
from zoneinfo import ZoneInfo

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_keycloak_client import KeycloakClient

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.exceptions import BackendError, UserNotProvisionedError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent_k8s_ut_namespace.k8s_client import K8sUtNamespaceClient
from waldur_site_agent_k8s_ut_namespace.prometheus_billing import (
    PrometheusClient,
    collect_usage,
)
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

# Order-form attribute names. Both are read from ``attributes`` on the resource,
# which is what the order form records, and both fall back to backend_settings.
QUOTA_ENABLED_ATTRIBUTE = "quota_enabled"
SECURITY_LEVEL_ATTRIBUTE = "security_level"

# Spellings a boolean order attribute may arrive in. The form can record the
# value as a JSON boolean or, once it has been through a text field, as a string.
_TRUE_STRINGS = frozenset({"true", "1", "yes", "on"})
_FALSE_STRINGS = frozenset({"false", "0", "no", "off"})


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

        # Consumption billing is opt-in. An offering without a ``prometheus`` block
        # keeps the old behaviour -- no usage reported, nothing probed -- because a
        # cluster without a metrics stack must not have its orders held pending.
        #
        # A block that is present but unusable raises rather than degrading to no
        # billing: silently reporting nothing is the one failure mode that looks
        # exactly like an idle month.
        self.prometheus_client: Optional[PrometheusClient] = None
        prometheus_settings = backend_settings.get("prometheus")
        if prometheus_settings:
            self.prometheus_client = PrometheusClient(prometheus_settings)
            logger.info(
                "Prometheus billing enabled for K8s UT namespace backend (%s)",
                self.prometheus_client.url,
            )

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

        # Defaults the order may override. A resource created before these
        # existed carries neither attribute, so both resolve to what the offering
        # said at the time: quotas managed, security level left to the operator.
        # Kept raw here and parsed when a resource is created, so a malformed
        # setting is reported against the order that hits it.
        self.quota_enabled = backend_settings.get("quota_enabled", True)
        self.default_security_level: Optional[str] = backend_settings.get(
            "default_security_level"
        )

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
        """Read a resource's order attributes as a plain dict.

        The generated client models ``attributes`` as an attrs class that supports
        ``[]`` but not ``.get()``, and is truthy even when empty -- so treating it
        as a mapping raises, and testing it for emptiness lies. A resource that
        carries no attributes at all holds ``UNSET``, which has neither
        ``to_dict`` nor items.
        """
        raw = getattr(waldur_resource, "attributes", None)
        if raw is None:
            return {}
        to_dict = getattr(raw, "to_dict", None)
        attributes = to_dict() if callable(to_dict) else raw
        return attributes if isinstance(attributes, dict) else {}

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
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            normalised = value.strip().lower()
            if normalised in _TRUE_STRINGS:
                return True
            if normalised in _FALSE_STRINGS:
                return False
        msg = f"{source} must be true or false, got {value!r}"
        raise BackendError(msg)

    def _resolve_quota_enabled(self, waldur_resource: WaldurResource) -> bool:
        """Whether the ManagedNamespace should manage this namespace's quota.

        The order wins when it names the attribute; otherwise the offering's
        ``quota_enabled`` setting applies, and it defaults to on so an offering
        that never mentioned quotas keeps them managed. The setting goes through
        the same parser as the attribute: an explicit ``null`` falls back to the
        default rather than reading as off, and a stray string is refused instead
        of being taken as truthy.
        """
        attributes = self._resource_attributes(waldur_resource)
        value = attributes.get(QUOTA_ENABLED_ATTRIBUTE)
        source = f"Attribute '{QUOTA_ENABLED_ATTRIBUTE}'"
        if value is None:
            value = self.quota_enabled
            source = f"The '{QUOTA_ENABLED_ATTRIBUTE}' backend setting"
        if value is None:
            return True
        return self._parse_bool_attribute(source, value)

    def _resolve_security_level(self, waldur_resource: WaldurResource) -> Optional[str]:
        """The Pod Security level to apply, or None to leave it to the operator.

        Returns None when neither the order nor the offering names a level: the
        CRD then falls back to the installation's own default, which the agent
        cannot know and must not guess.

        Raises:
            BackendError: if the level is not one the CRD accepts. The CRD would
                reject it too, but an order that fails here says what was wrong
                instead of surfacing an admission error.
        """
        attributes = self._resource_attributes(waldur_resource)
        value = attributes.get(SECURITY_LEVEL_ATTRIBUTE)
        from_attribute = value not in (None, "")
        if not from_attribute:
            value = self.default_security_level
        if value in (None, ""):
            return None

        level = str(value).strip().lower()
        if level not in SECURITY_LEVELS:
            allowed = ", ".join(SECURITY_LEVELS)
            if from_attribute:
                msg = (
                    f"Attribute '{SECURITY_LEVEL_ATTRIBUTE}' must be one of "
                    f"{allowed}, got {value!r}"
                )
            else:
                msg = (
                    "The 'default_security_level' backend setting must be one of "
                    f"{allowed}, got {value!r}"
                )
            raise BackendError(msg)
        return level

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
        """Check K8s cluster, Keycloak and Prometheus connectivity."""
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

            # Billing reads from Prometheus, so an unreachable one means the cycle
            # cannot report anything. Failing here keeps orders pending instead of
            # letting them run and error, and does nothing at all when billing is
            # not configured.
            if self.prometheus_client:
                prometheus_ok = self.prometheus_client.ping()
                if not prometheus_ok:
                    if raise_exception:
                        msg = "Failed to ping Prometheus server"
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
            logger.error("Failed to ping K8s/Keycloak/Prometheus: %s", e)
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
        logger.info(
            fmt.format(
                "Prometheus billing", "Yes" if self.prometheus_client else "No"
            )
        )
        if self.prometheus_client:
            logger.info(fmt.format("Prometheus URL", self.prometheus_client.url))
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

            if self.prometheus_client:
                logger.info("Prometheus connection successful")

            return True
        except BackendError as err:
            logger.error("Unable to connect to K8s/Keycloak/Prometheus: %s", err)
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

    # ── Usage reporting ────────────────────────────────────────────────────

    def _reporting_tz(self) -> datetime.tzinfo:
        """The timezone the billing period is computed in.

        Injected by the reporting processor from the agent's global configuration, so
        the month boundary here cannot drift from the one the processor uses to decide
        which period a report lands in. The configuration validator rejects a timezone
        that does not parse, so the UTC fallback is unreachable in a configured agent.
        """
        if self.timezone:
            try:
                return ZoneInfo(self.timezone)
            except Exception:
                logger.warning(
                    "Unknown timezone %r, falling back to UTC for the billing period",
                    self.timezone,
                )
        return datetime.timezone.utc

    def _period_bounds(self, year: int, month: int) -> tuple[datetime.datetime, datetime.datetime]:
        """The half-open bounds of one calendar month in the reporting timezone.

        The end is the first instant of the next month rather than its last, so no
        second of the month falls outside the window and consecutive periods tile
        without a gap.
        """
        tz = self._reporting_tz()
        start = datetime.datetime(year, month, 1, tzinfo=tz)
        # December rolls both the month and the year over.
        end = datetime.datetime(
            year + month // 12, month % 12 + 1, 1, tzinfo=tz
        )
        return start, end

    def _collect_period_usage(
        self, resource_backend_ids: list[str], start: datetime.datetime, end: datetime.datetime
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Measure one period's consumption for every requested namespace.

        Every component Waldur declares is reported for every resource the backend
        could measure, because ``TOTAL_ACCOUNT_USAGE`` is the resource's whole bill and
        a component left out of it keeps whatever it last held rather than dropping to
        zero. The value is the native figure divided by the component's ``unit_factor``,
        which is what turns core-hours into the Waldur unit the price list is written
        in.

        A failed measurement leaves the affected resources out of the report rather than
        reporting them as zero, and never aborts the pass: the processor's missing-entry
        path then fills zeros, and the rest of the offering is still billed. One query
        covers every namespace, so a query failure costs the whole pass -- reporting a
        resource from some of its components would submit a figure that contradicts the
        one Waldur already holds for the others.
        """
        if not resource_backend_ids:
            return {}

        client = self.prometheus_client
        if client is None:
            return {}

        # Component *names* are what Waldur sees; component *types* are what the
        # queries are keyed on. The two differ whenever an offering renames a
        # component, and both the query and the mapping use the type.
        components = {
            name: config.get("type", name)
            for name, config in self.backend_components.items()
        }
        component_types = sorted(set(components.values()))

        measured, failed = collect_usage(
            client, component_types, resource_backend_ids, start, end
        )

        # The queries cover every namespace at once, so a component that failed failed
        # for all of them: no resource is billed on a partial reading.
        unmeasured = sorted(set(components.values()) & failed)
        if unmeasured:
            logger.warning(
                "Usage for %d namespace(s) is not being reported: Prometheus could not "
                "measure component type(s) %s, and a partial report would overwrite the "
                "period's accrued usage",
                len(resource_backend_ids),
                ", ".join(unmeasured),
            )
            return {}

        report: dict[str, dict[str, dict[str, float]]] = {}
        for backend_id in resource_backend_ids:
            total: dict[str, float] = {}
            for name, component_type in components.items():
                native = measured.get(component_type, {}).get(backend_id, 0.0)
                unit_factor = self.backend_components[name].get("unit_factor") or 1
                total[name] = round(native / float(unit_factor), 2)
            report[backend_id] = {"TOTAL_ACCOUNT_USAGE": total}
        return report

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Collect consumption for the current billing period.

        Returns ``{}`` when no ``prometheus`` block is configured: billing is opt-in,
        and an offering that never asked for it must keep behaving as it did.
        """
        if self.prometheus_client is None:
            return {}
        now = backend_utils.get_current_time_in_timezone(self.timezone)
        start, _ = self._period_bounds(now.year, now.month)
        return self._collect_period_usage(resource_backend_ids, start, now)

    def get_usage_report_for_period(
        self,
        resource_backend_ids: list[str],
        year: int,
        month: int,
        waldur_resource: Optional[WaldurResource] = None,
    ) -> dict:
        """Recompute a closed month's consumption with both window ends pinned.

        The current-period report is capped at the last pass before rollover, so a month
        is only complete once it has ended. Prometheus still holds the series, so the
        period is simply measured again -- which also repairs a month an agent outage
        left half-reported.
        """
        del waldur_resource
        if self.prometheus_client is None or not resource_backend_ids:
            return {}
        start, end = self._period_bounds(year, month)
        return self._collect_period_usage(resource_backend_ids, start, end)

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
                metadata["status"] = self._parse_ready_condition(
                    cr.get("status", {})
                )
                # The level the operator actually applied, not the one asked
                # for: the operator resolves an unset spec.security.level to the
                # installation default, so only status can say what is in force.
                security_level = cr.get("status", {}).get("securityLevel")
                if security_level:
                    metadata["security_level"] = security_level
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

        # Resolve the order attributes first: both parsers can reject the order,
        # and refusing it before any group is created leaves nothing to undo.
        #
        # The switch is always sent explicitly. The CRD defaults it to true, so
        # omitting it would silently re-enable a quota the user turned off. The
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
            self.k8s_client.create_managed_namespace(ns_name, spec)
        except BackendError:
            # Cleanup Keycloak groups on failure
            logger.error("Failed to create CR, cleaning up Keycloak groups")
            self._delete_keycloak_groups(resource_slug)
            raise

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
        """Patch ManagedNamespace CR spec.quota."""
        quota = self._waldur_limits_to_quota(limits)
        try:
            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {"spec": {"quota": quota}},
            )
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

            # Collect users from all 3 Keycloak groups
            users = self._list_all_keycloak_users(waldur_resource)

            # Get usage report. Skipped when the caller said it does not want it --
            # membership sync never reads usage, and a Prometheus query per
            # component on every sync cycle is work thrown away. The empty report
            # then falls through to the same zero-filled usage as an unmeasured
            # namespace, which is what a caller that skips usage expects to see.
            report = (
                self._get_usage_report([ns_name])
                if self.include_usage_requested()
                else {}
            )
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

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale by patching CR quota to minimal values."""
        try:
            minimal_quota = {"cpu": "1", "memory": "1Gi", "storage": "1Gi"}
            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {"spec": {"quota": minimal_quota}},
            )
            logger.info("Downscaled namespace %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to downscale %s: %s", resource_backend_id, e)
            return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause by patching CR quota to zero."""
        try:
            zero_quota = {"cpu": "0", "memory": "0Gi", "storage": "0Gi"}
            self.k8s_client.patch_managed_namespace(
                resource_backend_id,
                {"spec": {"quota": zero_quota}},
            )
            logger.info("Paused namespace %s", resource_backend_id)
            return True
        except Exception as e:
            logger.error("Failed to pause %s: %s", resource_backend_id, e)
            return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore is a no-op; limits should be re-set via set_resource_limits."""
        logger.info("Restore for namespace %s is a no-op", resource_backend_id)
        return True


class K8sNamespaceBackend(K8sUtNamespaceBackend):
    """The same backend under the deployment-neutral ``k8s-namespace`` name.

    The ManagedNamespace operator is not specific to one deployment, so an
    offering should not have to be called "UT" to use it. This subclass exists
    only to report that name: ``backend_type`` reaches Waldur in resource
    metadata, so both names cannot share one class. Settings, entry-point schema
    and behaviour are the parent's, unchanged.
    """

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Initialize the backend and report the ``k8s-namespace`` type."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "k8s-namespace"
