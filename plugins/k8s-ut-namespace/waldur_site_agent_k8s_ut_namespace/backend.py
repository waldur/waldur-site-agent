"""K8s UT ManagedNamespace backend for Waldur Site Agent."""

import copy
import datetime
import json
import pprint
import re
from typing import Optional

from kubernetes.client.rest import ApiException
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_keycloak_client import KeycloakClient

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError, UserNotProvisionedError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent_k8s_ut_namespace.k8s_client import HTTP_CONFLICT, K8sUtNamespaceClient

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

# Component types with a direct per-container pod resource-request analogue, and the
# key each one is requested under in a container spec's resources.requests. storage has
# no entry here deliberately: a PVC isn't a container resource request and persists
# independent of any pod, so it stays metered from the CR's own quota (allocation-based,
# like before) rather than live pod requests (job-like, like cpu/ram/gpu become below).
POD_REQUEST_KEYS = {
    "cpu": "cpu",
    "ram": "memory",
    "gpu": "nvidia.com/gpu",
}

# Annotation key persisting the running usage accumulator between
# _get_usage_report() calls (see that method). Domain-prefixed to match the
# CRD's own API group. metadata.annotations, unlike spec/status, are never
# subject to the operator's CRD schema validation, so this is always writable
# regardless of what fields that schema does or doesn't declare.
USAGE_ACCUMULATOR_ANNOTATION = "provisioning.hpc.ut.ee/usage-accumulator"

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

    def _current_quota_in_waldur_units(self, cr: dict) -> dict[str, float]:
        """Read a ManagedNamespace CR's current spec.quota, converted to Waldur units.

        The inverse of _waldur_limits_to_quota, restricted to the components
        this offering actually declares (backend_components).
        """
        quota = (cr.get("spec") or {}).get("quota") or {}
        result: dict[str, float] = {}
        for component_key, component_config in self.backend_components.items():
            component_type = component_config.get("type", component_key)
            quota_field = self.component_quota_mapping.get(component_type)
            if quota_field is None or quota_field not in quota:
                continue
            result[component_key] = float(self._parse_k8s_quantity(quota[quota_field]))
        return result

    def _credit_pod_container_usage(
        self, ns_name: str, pod_credit_state: dict[str, str], now: datetime.datetime
    ) -> tuple[dict[str, float], dict[str, str]]:
        """Credit requested cpu/ram/gpu per container from its own real timestamps.

        Uses each container's own real start/finish timestamps, not a
        namespace-wide snapshot rate.

        *ns_name* is the real workload namespace, not self.cr_namespace. *pod_credit_state*
        is the prior call's returned state (persisted across calls in the usage-accumulator
        annotation, alongside `accumulated` -- see _sample_and_accumulate_usage), keyed
        "<pod uid>/<container name>" -> ISO8601 "credited up to" timestamp.

        Earlier versions of this method only summed *currently Running* pods' requests,
        scaled by elapsed-since-last-sample at the namespace level. That missed any pod
        whose entire lifetime fell between two polls -- confirmed live: a job submitted
        and finished in under a report cycle contributed nothing, which an operator
        correctly flagged, since real batch jobs commonly run for seconds to a few
        minutes, well under a typical polling interval. Kubernetes already records a
        container's real `started_at`/`finished_at` (while Running: started_at only, used
        against *now*; once Terminated: both, the container's actual, exact lifetime) on
        the pod object itself -- using that instead of "was it Running when we happened to
        look" credits every container for the real wall-clock time it ran, regardless of
        whether any poll ever caught it mid-flight.

        Per container (not per pod): a pod's containers can start and stop at different
        times (regular containers only, not each init container's own request -- the
        Kubernetes scheduler computes a pod's *effective* request as the max of this sum
        and each init container's, a known simplification here, not the full scheduler
        algorithm). For each one currently visible:
          - Running: credit from max(prior credited-until, its own started_at) to *now*,
            and record *now* as its new credited-until (still accruing).
          - Terminated: credit from max(prior credited-until, its own started_at) to its
            own finished_at, and record finished_at as its credited-until -- NOT dropped:
            a Succeeded/Failed pod is not deleted by Kubernetes on its own, it lingers
            until garbage collected, often well past the next poll. Without recording
            that it's already been credited up to finished_at, a repeat sighting would
            see no prior state and credit its full duration all over again; recording it
            means a repeat sighting computes zero elapsed (finished_at - finished_at) and
            correctly adds nothing further.
          - Waiting (not yet started): nothing to credit yet, and no entry recorded.
        A container no longer present at all (pod fully garbage-collected) simply has no
        entry in the next call's returned state -- nothing more could ever accrue for it,
        so there's nothing to keep.

        A container discovered for the first time (no prior credited-until) that's
        *already* Terminated gets its full real duration credited in one shot -- this is
        the exact fix for the "missed between polls" problem above, not a new carve-out:
        if nothing credited it yet and it has a real recorded start/finish, that whole
        duration is owed.
        """
        component_keys = {
            key
            for key, cfg in self.backend_components.items()
            if cfg.get("type", key) in POD_REQUEST_KEYS
        }
        deltas: dict[str, float] = dict.fromkeys(component_keys, 0.0)
        new_state: dict[str, str] = {}
        if not component_keys:
            return deltas, new_state

        no_requests_count = 0
        for pod in self.k8s_client.list_pods(ns_name):
            pod_uid = (pod.get("metadata") or {}).get("uid")
            if not pod_uid:
                continue
            requests_by_container = {
                c.get("name"): (c.get("resources") or {}).get("requests") or {}
                for c in (pod.get("spec") or {}).get("containers") or []
            }
            for status in (pod.get("status") or {}).get("container_statuses") or []:
                container_name = status.get("name")
                requests = requests_by_container.get(container_name) or {}
                if not requests:
                    no_requests_count += 1
                    continue
                state = status.get("state") or {}
                running = state.get("running")
                terminated = state.get("terminated")
                key = f"{pod_uid}/{container_name}"
                prior_credited_until = self._parse_iso8601(pod_credit_state.get(key))

                if terminated:
                    started_at = terminated.get("started_at")
                    finished_at = terminated.get("finished_at")
                    if started_at is None or finished_at is None:
                        continue
                    credit_from = (
                        max(prior_credited_until, started_at)
                        if prior_credited_until
                        else started_at
                    )
                    credit_to = finished_at
                    # Carried forward (not dropped): a Succeeded/Failed pod is not
                    # deleted by Kubernetes on its own -- it lingers until garbage
                    # collected, often well past the next poll. Without recording that
                    # we've already credited it up to finished_at, the *next* sighting
                    # would see no prior state and credit its full duration all over
                    # again. Recording credit_to == finished_at here means a repeat
                    # sighting computes elapsed_minutes == 0 (finished_at - finished_at)
                    # and correctly adds nothing further.
                    new_state[key] = credit_to.isoformat()
                elif running:
                    started_at = running.get("started_at")
                    if started_at is None:
                        continue
                    credit_from = (
                        max(prior_credited_until, started_at)
                        if prior_credited_until
                        else started_at
                    )
                    credit_to = now
                    new_state[key] = credit_to.isoformat()
                else:
                    continue  # waiting -- hasn't started yet, nothing to credit

                elapsed_minutes = max(0.0, (credit_to - credit_from).total_seconds() / 60)
                if elapsed_minutes <= 0:
                    continue
                for component_key in component_keys:
                    component_type = self.backend_components[component_key].get(
                        "type", component_key
                    )
                    request_key = POD_REQUEST_KEYS[component_type]
                    if request_key in requests:
                        deltas[component_key] += (
                            self._parse_k8s_quantity(requests[request_key]) * elapsed_minutes
                        )

        if no_requests_count:
            # The single most common "looks like a bug but isn't" support question this
            # plugin gets: cpu/ram read 0.0 while storage keeps accruing normally. Billing
            # is requested, not measured, resources (see this method's own docstring) --
            # a container with no resources.requests has nothing to bill, by design. One
            # summary line per poll (not one per container) so a long-lived pod lacking
            # requests doesn't flood the log every cycle it's still visible.
            logger.info(
                "%s: %d container(s) have no resources.requests set -- contributing 0 to "
                "cpu/ram/gpu usage this cycle (expected: billing is requested, not "
                "measured, resources)",
                ns_name,
                no_requests_count,
            )
        return deltas, new_state

    @staticmethod
    def _load_usage_state(annotations: dict) -> Optional[dict]:
        """Parse the usage-accumulator annotation, if present and valid."""
        raw = annotations.get(USAGE_ACCUMULATOR_ANNOTATION)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Malformed usage accumulator annotation, resetting: %r", raw)
            return None

    @staticmethod
    def _parse_iso8601(value: Optional[str]) -> Optional[datetime.datetime]:
        """Parse a stored last_sample_at timestamp, tolerating a missing/bad value."""
        if not value:
            return None
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            return None

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Build the per-resource usage report for this poll.

        Meters cpu/ram/gpu from each container's own real start/finish timestamps;
        storage from the namespace's quota x elapsed time.

        cpu/ram/gpu (see _credit_pod_container_usage): credited from Kubernetes' own
        recorded per-container `started_at`/`finished_at`, not a namespace-wide snapshot
        rate -- so a container is credited for the real wall-clock time it actually ran,
        regardless of whether any poll happened to catch it mid-flight (confirmed live:
        an earlier, snapshot-based version of this missed any pod whose entire lifetime
        fell between two polls, which real batch jobs commonly do). Still the same
        *convention* SLURM's `ReqTRES x Elapsed` uses in this system (AQS bills SLURM's
        *requested* TRES for the job's elapsed time, not measured utilization; see
        ska-src-accounting-quota-api's docs/open-issues.md §5) -- requested, not measured
        utilization, just scoped to real, individually-timed containers instead of a
        polling snapshot.

        storage has no per-pod request (a PVC isn't a container resource and persists
        independent of any pod), so it's the one component type still metered from the
        CR's own spec.quota -- see _current_quota_in_waldur_units -- which is itself only
        ever a point-in-time snapshot, with no history of what it was a moment ago, so
        this samples it on every call and accumulates quota x (time since the last
        sample): metering *that* continuously bills for holding an allocation, which is
        the right model for storage specifically (unlike cpu/ram/gpu, a PVC's allocation
        doesn't track to any one job's lifetime).

        Either way, the running total per component accumulates into a month-to-date
        figure, mirroring how `sacct` itself provides SLURM's month-to-date figures.

        The running total is persisted as a JSON-encoded annotation on the
        CR itself (see USAGE_ACCUMULATOR_ANNOTATION) rather than in local
        site-agent state, so it survives an agent restart/reschedule and
        works the same way regardless of how many agent replicas poll this
        cluster. Resets to zero at the start of each new calendar month, to
        match `sacct`'s own month-to-date convention for SLURM.

        Requires accounting_type: "usage" on this offering's components in
        the site-agent config (see README) -- Waldur bills accounting_type:
        "limit" components from their allocated limit directly and never
        looks at what this method returns, regardless of its contents.

        Confirmed live against a real cluster (a real waldur-site-agent's
        `report` and `membership_sync` modes both call this, independently
        and uncoordinated -- both call pull_resource(), which calls this) --
        a naive read-modify-write on the annotation loses updates under that
        real concurrency: whichever call's write lands last silently
        clobbers the other's. See _sample_and_accumulate_usage for the fix
        (optimistic concurrency via the CR's own resourceVersion, retried on
        conflict).
        """
        report: dict[str, dict[str, dict[str, float]]] = {}
        now = datetime.datetime.now(datetime.timezone.utc)
        current_period = now.strftime("%Y-%m")

        for ns_name in resource_backend_ids:
            accumulated = self._sample_and_accumulate_usage(ns_name, now, current_period)
            if accumulated is not None:
                report[ns_name] = {"TOTAL_ACCOUNT_USAGE": accumulated}

        return report

    #: Retries for _sample_and_accumulate_usage's optimistic-concurrency loop --
    #: report and membership_sync mode both call it, uncoordinated, so a
    #: conflict is an expected, routine outcome, not a rare edge case.
    _USAGE_ACCUMULATOR_RETRIES = 5

    def _sample_and_accumulate_usage(
        self, ns_name: str, now: datetime.datetime, current_period: str
    ) -> Optional[dict[str, float]]:
        """Read-modify-write the usage accumulator for one namespace, safely.

        Re-reads the CR fresh on every attempt (not just once) and writes
        back via replace_managed_namespace, which -- unlike a merge patch --
        the API server rejects with 409 if the CR changed since this
        attempt's read. On conflict: re-read the now-current state (which
        may include another caller's own accumulation, and another caller's own
        pod_credit_state -- see _credit_pod_container_usage) and recompute from
        there, rather than retrying the same stale delta.
        """
        for attempt in range(self._USAGE_ACCUMULATOR_RETRIES):
            try:
                cr = self.k8s_client.get_managed_namespace(ns_name)
            except BackendError as e:
                logger.warning("Could not read ManagedNamespace %s for usage: %s", ns_name, e)
                return None
            if cr is None:
                return None

            # cpu/ram/gpu: credited from each container's own real start/finish
            # timestamps (see _credit_pod_container_usage), not a namespace-wide
            # snapshot rate. storage: the CR's own standing quota (see
            # _current_quota_in_waldur_units) -- a PVC isn't a per-pod request and
            # persists independent of any pod, so it stays allocation-based. spec.name
            # is the real workload namespace this CR manages, not self.cr_namespace.
            ns_real_name = (cr.get("spec") or {}).get("name") or ns_name
            annotations = (cr.get("metadata") or {}).get("annotations") or {}
            state = self._load_usage_state(annotations)
            prior_pod_credit_state = (state or {}).get("pod_credit_state") or {}

            pod_deltas, new_pod_credit_state = self._credit_pod_container_usage(
                ns_real_name, prior_pod_credit_state, now
            )
            quota = self._current_quota_in_waldur_units(cr)
            storage_like = {
                component_key: value
                for component_key, value in quota.items()
                if self.backend_components.get(component_key, {}).get("type", component_key)
                not in POD_REQUEST_KEYS
            }
            sample_keys = set(pod_deltas) | set(storage_like)
            if not sample_keys:
                return None

            if state is None or state.get("period") != current_period:
                # New period: zero out accumulated totals, same as before -- but
                # pod_credit_state is NOT part of that reset. It tracks each
                # container's own credited-until point, independent of which month's
                # bucket its credit lands in, so a container already straddling the
                # boundary doesn't get double-credited (or under-credited) for the
                # portion that already landed in the now-reset-away prior period.
                accumulated = dict.fromkeys(sample_keys, 0.0)
                storage_elapsed_minutes = 0.0
            else:
                prior = state.get("accumulated") or {}
                accumulated = {c: float(prior.get(c, 0.0)) for c in sample_keys}
                last_sample_at = self._parse_iso8601(state.get("last_sample_at")) or now
                storage_elapsed_minutes = max(0.0, (now - last_sample_at).total_seconds() / 60)

            for component, delta in pod_deltas.items():
                accumulated[component] = accumulated.get(component, 0.0) + delta
            for component, component_quota in storage_like.items():
                accumulated[component] = (
                    accumulated.get(component, 0.0) + component_quota * storage_elapsed_minutes
                )

            new_state = {
                "period": current_period,
                "last_sample_at": now.isoformat(),
                "accumulated": {c: round(v, 6) for c, v in accumulated.items()},
                "pod_credit_state": new_pod_credit_state,
            }
            updated_cr = copy.deepcopy(cr)
            updated_cr.setdefault("metadata", {}).setdefault("annotations", {})[
                USAGE_ACCUMULATOR_ANNOTATION
            ] = json.dumps(new_state)

            try:
                self.k8s_client.replace_managed_namespace(ns_name, updated_cr)
                return accumulated
            except ApiException as e:
                if e.status != HTTP_CONFLICT:
                    raise
                logger.info(
                    "Usage accumulator for %s changed concurrently (attempt %d/%d), retrying",
                    ns_name, attempt + 1, self._USAGE_ACCUMULATOR_RETRIES,
                )
                continue
            except BackendError as e:
                # Still report this sample's figures: an occasional missed
                # persist (the next call re-derives from an older
                # last_sample_at and slightly over-counts one interval) is a
                # smaller error than silently reporting nothing.
                logger.warning("Could not persist usage accumulator for %s: %s", ns_name, e)
                return accumulated

        logger.warning(
            "Giving up on the usage accumulator for %s after %d concurrent conflicts; "
            "reporting this attempt's figures unpersisted",
            ns_name, self._USAGE_ACCUMULATOR_RETRIES,
        )
        return accumulated

    @staticmethod
    def _parse_k8s_quantity(value: str) -> float:
        """Parse a K8s resource quantity string into a float.

        Units are this backend's own: whole cores for cpu, whole GiB for ram/storage,
        whole units for gpu or any other bare integer.

        True division throughout, not floor division: a pod requesting "500m" cpu or
        "512Mi" memory is extremely common, and truncating either to 0 (this used to be
        floor division, returning int) silently dropped most of a typical pod's real
        request once this helper started being used to sum live pod requests
        (_credit_pod_container_usage), not just namespace-level quota strings
        (usually whole units already, so the bug was latent there before).
        """
        value = str(value)
        if value.endswith("Gi"):
            return float(value[:-2])
        if value.endswith("Mi"):
            return float(value[:-2]) / 1024
        if value.endswith("m"):
            return float(value[:-1]) / 1000
        try:
            return float(value)
        except ValueError:
            return 0.0

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

        # 1. Create Keycloak groups (3 roles)
        self._create_keycloak_groups(resource_slug)

        # 2. Build ManagedNamespace spec
        _, waldur_limits = self._collect_resource_limits(waldur_resource)
        quota = self._waldur_limits_to_quota(waldur_limits)

        spec: dict = {
            "name": ns_name,
            "quota": quota,
        }

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

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale by patching CR quota to minimal values (1 unit per configured component).

        Derived from backend_components rather than a hardcoded cpu/memory/storage
        literal: the old literal silently omitted any component beyond those three
        (found live -- gpu kept its full quota through a pause, confirmed against a
        real cluster with a real over-budget resource: cpu/ram/storage correctly
        went to 0, gpu never did, because it wasn't in the hardcoded dict at all).
        """
        try:
            minimal_quota = self._waldur_limits_to_quota(
                dict.fromkeys(self.backend_components, 1)
            )
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
        """Pause by patching CR quota to zero, for every configured component.

        See downscale_resource's docstring -- same fix, same live-confirmed bug.
        """
        try:
            zero_quota = self._waldur_limits_to_quota(
                dict.fromkeys(self.backend_components, 0)
            )
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
