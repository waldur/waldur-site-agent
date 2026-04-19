"""SLURM-specific backend classes and functions."""

import datetime
import pprint
from enum import Enum
from typing import Optional

import requests
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import UNSET

from waldur_site_agent.backend import (
    BackendType,
    backends,
    logger,
)
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common.component_mapping import ComponentMapper
from waldur_site_agent_slurm import utils
from waldur_site_agent_slurm.client import SlurmClient


def _get_ldap_client(ldap_settings: dict):  # type: ignore[no-untyped-def]  # noqa: ANN202
    """Lazily import and instantiate the LDAP client if configured.

    Returns an LdapClient instance from waldur-site-agent-ldap.
    The return type is not annotated because the ldap plugin is an optional dependency.
    """
    try:
        from waldur_site_agent_ldap.client import LdapClient  # noqa: PLC0415
    except ImportError as e:
        msg = (
            "LDAP settings are configured in SLURM backend_settings but the "
            "waldur-site-agent-ldap package is not installed. "
            "Install it with: pip install waldur-site-agent-ldap"
        )
        raise BackendError(msg) from e
    return LdapClient(ldap_settings)


class PeriodicSettingsMode(Enum):
    """Mode in which periodic settings are applied."""

    PRODUCTION = "production"
    EMULATOR = "emulator"


class SlurmBackend(backends.BaseBackend):
    """Main class for management of SLURM resources."""

    def __init__(self, slurm_settings: dict, slurm_tres: dict[str, dict]) -> None:
        """Init backend data and creates a corresponding client."""
        super().__init__(slurm_settings, slurm_tres)
        self.backend_type = BackendType.SLURM.value
        slurm_bin_path = self.backend_settings.get("slurm_bin_path", "/usr/bin")
        self.cluster_name: Optional[str] = self.backend_settings.get("cluster_name")
        self.client: SlurmClient = SlurmClient(
            slurm_tres, slurm_bin_path=slurm_bin_path, cluster_name=self.cluster_name
        )

        # Optional LDAP integration for project groups
        self._ldap_client = None
        ldap_settings = self.backend_settings.get("ldap")
        if ldap_settings:
            self._ldap_client = _get_ldap_client(ldap_settings)

        # Optional QoS management
        self._qos_config = self.backend_settings.get("qos_management", {})

        # Optional project directory management
        self._project_dir_config = self.backend_settings.get("project_directory", {})

        # Optional partition assignment
        self._default_partition = self.backend_settings.get("default_partition")

        # Optional component mapping (Waldur components → SLURM TRES)
        self._component_mapper = ComponentMapper(slurm_tres)

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Pre-creation setup: account hierarchy, LDAP groups, filesystem, QoS."""
        if not waldur_resource.customer_slug or not waldur_resource.project_slug:
            logger.warning(
                "Resource %s has unset or missing slug fields. customer_slug: %s, project_slug: %s",
                waldur_resource.uuid,
                waldur_resource.customer_slug,
                waldur_resource.project_slug,
            )
            msg = (
                f"Resource {waldur_resource.uuid} has unset or missing slug fields. "
                f"customer_slug: {waldur_resource.customer_slug}, "
                f"project_slug: {waldur_resource.project_slug}. "
                "Cannot create backend resources with invalid slug values."
            )
            raise BackendError(msg)

        del user_context

        resource_backend_id = self._get_resource_backend_id(waldur_resource.slug)

        parent_account = self.backend_settings.get("parent_account")
        if parent_account:
            # Flat hierarchy: create account directly under configured parent
            project_backend_id = self._get_project_backend_id(waldur_resource.project_slug)
            self._create_backend_resource(
                project_backend_id,
                waldur_resource.project_name,
                project_backend_id,
                parent_account,
            )
        else:
            # Default hierarchy: customer -> project -> allocation
            project_backend_id = self._get_project_backend_id(waldur_resource.project_slug)
            customer_backend_id = self._get_customer_backend_id(waldur_resource.customer_slug)
            self._create_backend_resource(
                customer_backend_id,
                waldur_resource.customer_name,
                customer_backend_id,
                self.backend_settings.get("default_account"),
            )
            self._create_backend_resource(
                project_backend_id,
                waldur_resource.project_name,
                project_backend_id,
                customer_backend_id,
            )

        # Optional: create LDAP project group
        if self._ldap_client:
            try:
                self._ldap_client.create_project_group(resource_backend_id)
            except BackendError:
                logger.exception("Failed to create LDAP project group for %s", resource_backend_id)

        # Optional: create project directory and set quota
        if self._project_dir_config.get("enabled"):
            self._setup_project_directory(resource_backend_id)

        # Optional: create QoS for the account
        if self._qos_config.get("enabled"):
            self._setup_account_qos(resource_backend_id)

    def diagnostics(self) -> bool:
        """Runs diagnostics for SLURM cluster."""
        default_account_name = self.backend_settings["default_account"]

        format_string = "{:<30} = {:<10}"
        logger.info(
            format_string.format("SLURM customer prefix", self.backend_settings["customer_prefix"])
        )
        logger.info(
            format_string.format("SLURM project prefix", self.backend_settings["project_prefix"])
        )
        logger.info(
            format_string.format(
                "SLURM allocation prefix", self.backend_settings["allocation_prefix"]
            )
        )
        logger.info(format_string.format("SLURM default account", default_account_name))
        logger.info("")

        logger.info("SLURM tres components:\n%s\n", pprint.pformat(self.backend_components))

        try:
            slurm_version_info = self.client._execute_command(
                ["-V"], "sinfo", immediate=False, parsable=False
            )
            logger.info("Slurm version: %s", slurm_version_info.strip())
        except BackendError as err:
            logger.error("Unable to fetch SLURM info, reason: %s", err)
            return False

        if not self.client.validate_slurm_binary():
            logger.error(
                "SLURM binary validation failed: sacctmgr does not appear to be a real "
                "SLURM binary. This may indicate an emulator is shadowing the real binary. "
                "Check slurm_bin_path setting (current: '%s').",
                self.client.slurm_bin_path,
            )

        try:
            self.ping(raise_exception=True)
            logger.info("SLURM cluster ping is successful")
        except BackendError as err:
            logger.error("Unable to ping SLURM cluster, reason: %s", err)

        # Validate cluster filtering
        try:
            known_clusters = self.client.list_clusters()
            logger.info("SLURM clusters: %s", ", ".join(known_clusters) or "(none)")
            if self.cluster_name:
                if self.cluster_name in known_clusters:
                    logger.info('Cluster filter "%s" is valid', self.cluster_name)
                else:
                    logger.error(
                        'Configured cluster_name "%s" not found in SLURM. Known clusters: %s',
                        self.cluster_name,
                        ", ".join(known_clusters),
                    )
                    return False
            else:
                logger.warning(
                    "No cluster_name configured in backend_settings. "
                    "sacctmgr commands will not be scoped to a specific cluster. "
                    "Set cluster_name in backend_settings to match the offering backend_id."
                )
        except BackendError as err:
            logger.error("Unable to list SLURM clusters: %s", err)

        tres = self.list_components()
        logger.info("Available tres in the cluster: %s", ",".join(tres))

        default_account = self.client.get_resource(default_account_name)
        if default_account is None:
            logger.error("There is no account %s in the cluster", default_account)
            return False
        logger.info('Default parent account "%s" is in place', default_account_name)
        logger.info("")

        return True

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if the SLURM cluster is online."""
        try:
            self.client.list_resources()
        except BackendError as err:
            if raise_exception:
                raise
            logger.info("Error: %s", err)
            return False
        else:
            return True

    def list_components(self) -> list[str]:
        """Return a list of TRES on the SLURM cluster."""
        return self.client.list_tres()

    def post_create_resource(
        self,
        resource: BackendResourceInfo,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Post-create actions for SLURM resources."""
        del resource, waldur_resource
        # If user context is available and homedir creation is enabled, create homedirs proactively
        if user_context and self.backend_settings.get("enable_user_homedir_account_creation", True):
            offering_user_mappings = user_context.get("offering_user_mappings", {})
            if offering_user_mappings:
                # Extract usernames from offering users
                usernames = {
                    offering_user.username
                    for offering_user in offering_user_mappings.values()
                    if offering_user.username
                }

                if usernames:
                    logger.info(
                        "Creating home directories during resource creation for users: %s",
                        ", ".join(usernames),
                    )
                    umask = self.backend_settings.get("default_homedir_umask", "0077")
                    self.create_user_homedirs(usernames, umask)

    def has_prepaid_components(self) -> bool:
        """Return True if any backend component uses ONE_TIME (prepaid) billing."""
        return any(
            data.get("accounting_type") == "one"
            for data in self.backend_components.values()
        )

    @staticmethod
    def _calculate_duration_months(
        created: datetime.datetime, end_date: datetime.date
    ) -> int:
        """Calculate the number of whole months between created and end_date.

        Uses the same logic as Waldur backend for consistency.
        """
        created_date = created.date() if isinstance(created, datetime.datetime) else created
        months = (end_date.year - created_date.year) * 12 + (end_date.month - created_date.month)
        if end_date.day > created_date.day:
            months += 1
        return max(1, months)

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect SLURM and Waldur limits separately.

        When ``target_components`` are configured on any backend component,
        the ComponentMapper converts Waldur-facing limits to backend-facing
        SLURM TRES limits.  The ``factor`` in ``target_components`` encodes
        the full conversion (including any unit scaling).

        When no ``target_components`` are configured (passthrough mode),
        the standard ``unit_factor`` conversion is used.
        """
        allocation_limits: dict[str, int] = {}
        usage_based_limits = backend_utils.get_usage_based_limits(self.backend_components)
        limit_based_components = [
            component
            for component, data in self.backend_components.items()
            if data["accounting_type"] in ("limit", "one")
        ]
        waldur_resource_limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}

        # Add usage-based limits to allocation limits
        allocation_limits.update(usage_based_limits)

        if not self._component_mapper.is_passthrough:
            # Component mapping mode: convert Waldur limits → SLURM TRES
            limit_values = {
                k: v for k, v in waldur_resource_limits.items() if k in limit_based_components
            }
            converted = self._component_mapper.convert_limits_to_target(limit_values)
            allocation_limits.update(converted)
        else:
            # Standard mode: apply unit_factor per component
            for component_key in limit_based_components:
                if component_key in waldur_resource_limits:
                    allocation_limits[component_key] = (
                        waldur_resource_limits[component_key]
                        * self.backend_components[component_key]["unit_factor"]
                    )

        # For prepaid components, multiply allocation limits by subscription duration.
        # GrpTRESMins is a cumulative budget, so limit * months gives the total budget.
        prepaid_components = [
            name for name, data in self.backend_components.items()
            if data.get("accounting_type") == "one"
        ]
        if prepaid_components:
            resource_created = getattr(waldur_resource, "created", UNSET)
            resource_end_date = getattr(waldur_resource, "end_date", UNSET)
            if (
                not isinstance(resource_created, type(UNSET))
                and resource_created is not None
                and not isinstance(resource_end_date, type(UNSET))
                and resource_end_date is not None
            ):
                duration_months = self._calculate_duration_months(
                    resource_created, resource_end_date
                )
                logger.info(
                    "Prepaid resource: duration=%d months (created=%s, end_date=%s)",
                    duration_months, resource_created, resource_end_date,
                )
                for comp_key in prepaid_components:
                    if comp_key in allocation_limits:
                        allocation_limits[comp_key] *= duration_months
            else:
                logger.warning(
                    "Prepaid components configured but resource has no created/end_date, "
                    "skipping duration multiplication"
                )

        # Add usage-based limits to Waldur limits
        for component_key in usage_based_limits:
            waldur_resource_limits[component_key] = self.backend_components[component_key]["limit"]

        # Filter out keys that are not known SLURM TRES types to prevent sacctmgr errors.
        # Query SLURM directly; skip filter if unreachable or returns empty.
        try:
            known_tres = set(self.list_components())
        except Exception:
            known_tres = set()
        if known_tres:
            unknown_keys = set(allocation_limits) - known_tres
            if unknown_keys:
                logger.warning(
                    "Dropping unknown TRES keys from allocation limits (not in SLURM config): %s",
                    sorted(unknown_keys),
                )
                allocation_limits = {k: v for k, v in allocation_limits.items() if k in known_tres}

        # Convert to integers
        allocation_limits = {k: int(v) for k, v in allocation_limits.items()}
        waldur_resource_limits = {k: int(v) for k, v in waldur_resource_limits.items()}

        logger.info("SLURM allocation limits: %s", allocation_limits)
        logger.info("SLURM Waldur limits: %s", waldur_resource_limits)

        return allocation_limits, waldur_resource_limits

    def add_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Add user to SLURM account, with optional partition and LDAP group."""
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        if not resource_backend_id.strip():
            message = "Empty backend ID for resource"
            raise BackendError(message)

        logger.info("Adding user %s to resource %s", username, resource_backend_id)
        if not username:
            logger.warning("Username is blank, skipping creation of association")
            return False

        if not self.client.get_association(username, resource_backend_id):
            logger.info("Creating association between %s and %s", username, resource_backend_id)
            try:
                default_account = self.backend_settings.get("default_account", "root")
                if self._default_partition:
                    self.client.create_association_with_partition(
                        username,
                        resource_backend_id,
                        self._default_partition,
                        default_account,
                    )
                else:
                    self.client.create_association(
                        username,
                        resource_backend_id,
                        default_account,
                    )
                logger.info("Created association between %s and %s", username, resource_backend_id)
            except BackendError as err:
                logger.exception("Unable to create association on backend: %s", err)
                return False
        else:
            logger.info("Association already exists, skipping creation")

        # Optional: add user to LDAP project group
        if self._ldap_client:
            try:
                self._ldap_client.add_user_to_group(resource_backend_id, username)
            except BackendError:
                logger.exception(
                    "Failed to add %s to LDAP project group %s",
                    username,
                    resource_backend_id,
                )

        return True

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add specified users to the allocations on the SLURM cluster."""
        added_users = super().add_users_to_resource(waldur_resource, user_ids)

        if self.backend_settings.get("enable_user_homedir_account_creation", True):
            umask: str = str(kwargs.get("homedir_umask", "0077"))
            self.create_user_homedirs(added_users, umask=umask)

        return added_users

    def remove_user(self, waldur_resource: WaldurResource, username: str, **kwargs: str) -> bool:
        """Remove user from SLURM account, with optional LDAP group cleanup."""
        del kwargs
        result = super().remove_user(waldur_resource, username)

        # Optional: remove user from LDAP project group
        if result and self._ldap_client:
            resource_backend_id = waldur_resource.backend_id
            try:
                self._ldap_client.remove_user_from_group(resource_backend_id, username)
            except BackendError:
                logger.exception(
                    "Failed to remove %s from LDAP project group %s",
                    username,
                    resource_backend_id,
                )

        return result

    def process_existing_users(self, existing_users: set[str]) -> None:
        """Process existing users on the backend."""
        logger.info(
            "Processing existing users on the backend to ensure home directories exist: %s",
            ", ".join(existing_users),
        )
        if self.backend_settings.get("enable_user_homedir_account_creation", True):
            logger.info(
                "Processing existing users to ensure home directories exist: %s",
                ", ".join(existing_users),
            )
            umask = self.backend_settings.get("default_homedir_umask", "0077")
            self.create_user_homedirs(existing_users, umask=umask)

    # ===== QOS AND FILESYSTEM MANAGEMENT =====

    def _setup_account_qos(self, account_name: str) -> None:
        """Create a per-account QoS and attach it to the account."""
        if self.client.qos_exists(account_name):
            logger.info("QoS %s already exists, skipping creation", account_name)
            return

        flags = self._qos_config.get("flags", "DenyOnLimit,NoDecay")
        grp_tres = self._qos_config.get("grp_tres")
        max_jobs = self._qos_config.get("max_jobs")
        max_submit = self._qos_config.get("max_submit")
        max_wall = self._qos_config.get("max_wall")
        min_tres_per_job = self._qos_config.get("min_tres_per_job")

        self.client.create_qos(
            name=account_name,
            flags=flags,
            grp_tres=grp_tres,
            max_jobs=max_jobs,
            max_submit=max_submit,
            max_wall=max_wall,
            min_tres_per_job=min_tres_per_job,
        )

        # Attach QoS to the account
        self.client.add_account_qos(account_name, account_name)
        self.client.set_account_default_qos(account_name, account_name)

        # Attach additional QoSes if configured
        additional_qos = self._qos_config.get("additional_qos", [])
        for qos_name in additional_qos:
            self.client.add_account_qos(account_name, qos_name)

    def _setup_project_directory(self, project_id: str) -> None:
        """Create project directory and set filesystem quota."""
        base_path = self._project_dir_config.get("base_path", "/valhalla/projects")
        project_path = f"{base_path}/{project_id}"

        group_owner = self._project_dir_config.get("group_owner_source", "project_id")
        if group_owner == "project_id":
            group_name = project_id
        else:
            group_name = self._project_dir_config.get("group_name", project_id)

        dir_owner = self._project_dir_config.get("owner", "nobody")
        permissions = self._project_dir_config.get("permissions", "770")
        set_gid = self._project_dir_config.get("set_gid", True)
        set_acl = self._project_dir_config.get("set_acl", True)

        try:
            # Create directory
            self.client.execute_command(["mkdir", "-p", project_path])
            self.client.execute_command(["chmod", permissions, project_path])
            if set_gid:
                self.client.execute_command(["chmod", "g+s", project_path])
        except BackendError:
            logger.exception("Failed to create project directory %s", project_path)
            return

        try:
            self.client.execute_command(["chown", f"{dir_owner}:{group_name}", project_path])
        except BackendError:
            logger.exception("Failed to chown project directory %s", project_path)

        if set_acl:
            try:
                self.client.execute_command(
                    [
                        "setfacl",
                        "-R",
                        "-m",
                        f"group:{group_name}:rwx,d:group:{group_name}:rwx",
                        project_path,
                    ]
                )
            except BackendError:
                logger.exception("Failed to set ACL on project directory %s", project_path)

        logger.info("Created project directory %s", project_path)

        # Set Lustre quota if configured
        lustre_config = self._project_dir_config.get("lustre_quota")
        if lustre_config and self._ldap_client:
            gid = self._ldap_client.get_group_gid(group_name)
            if gid is not None:
                self._set_lustre_quota(gid, project_path, lustre_config)

    def _set_lustre_quota(self, gid: int, project_path: str, config: dict) -> None:
        """Set Lustre filesystem quota for a project."""
        mount_point = config.get("mount_point", "/valhalla")
        block_soft = config.get("block_softlimit")
        block_hard = config.get("block_hardlimit")
        inode_soft = config.get("inode_softlimit")
        inode_hard = config.get("inode_hardlimit")

        try:
            quota_cmd = ["lfs", "setquota", "-p", str(gid)]
            if block_soft is not None:
                quota_cmd.extend(["-b", str(block_soft)])
            if block_hard is not None:
                quota_cmd.extend(["-B", str(block_hard)])
            if inode_soft is not None:
                quota_cmd.extend(["-i", str(inode_soft)])
            if inode_hard is not None:
                quota_cmd.extend(["-I", str(inode_hard)])
            quota_cmd.append(mount_point)
            self.client.execute_command(quota_cmd)

            # Set project ID on directory
            self.client.execute_command(
                [
                    "lfs",
                    "project",
                    "-p",
                    str(gid),
                    "-r",
                    "-s",
                    project_path,
                ]
            )
            logger.info("Set Lustre quota for GID %d on %s", gid, mount_point)
        except BackendError:
            logger.exception("Failed to set Lustre quota for GID %d", gid)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale the resource QoS respecting the backend settings."""
        qos_downscaled = self.backend_settings.get("qos_downscaled")
        if not qos_downscaled:
            logger.error(
                "The QoS for dowscaling has incorrect value %s, skipping operation",
                qos_downscaled,
            )
            return False

        current_qos = self.client.get_current_account_qos(resource_backend_id)

        logger.info("Current QoS: %s", current_qos)

        if current_qos == qos_downscaled:
            logger.info("The account is already downscaled")
            self._last_qos = qos_downscaled
            return True

        logger.info("Setting %s QoS for the SLURM account", qos_downscaled)
        self.client.set_account_qos(resource_backend_id, qos_downscaled)
        self._last_qos = qos_downscaled
        logger.info("The new QoS successfully set")
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Set the resource QoS to a paused one respecting the backend settings."""
        qos_paused = self.backend_settings.get("qos_paused")
        if not qos_paused:
            logger.error(
                "The QoS for pausing has incorrect value %s, skipping operation",
                qos_paused,
            )
            return False

        current_qos = self.client.get_current_account_qos(resource_backend_id)

        logger.info("Current QoS: %s", current_qos)

        if current_qos == qos_paused:
            logger.info("The account is already paused")
            self._last_qos = qos_paused
            return True

        logger.info("Setting %s QoS for the SLURM account", qos_paused)
        self.client.set_account_qos(resource_backend_id, qos_paused)
        self._last_qos = qos_paused
        logger.info("The new QoS successfully set")
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource QoS to the default one."""
        current_qos = self.client.get_current_account_qos(resource_backend_id)

        default_qos = self.backend_settings.get("qos_default", "normal")

        if current_qos in [None, ""]:
            logger.info("The account does not have an active QoS set, skipping reset")
            self._last_qos = current_qos
            return False

        logger.info("The current QoS is %s", current_qos)

        if current_qos == default_qos:
            logger.info("The account already has the default QoS (%s)", default_qos)
            self._last_qos = current_qos
            return False

        logger.info("Setting %s QoS", default_qos)
        self.client.set_account_qos(resource_backend_id, default_qos)
        self._last_qos = default_qos
        logger.info("The new QoS is %s", default_qos)

        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return backend metadata for the SLURM account (QoS only for now).

        Reuses the QoS value cached by restore/pause/downscale if available,
        avoiding a duplicate sacctmgr query.
        """
        qos = getattr(self, "_last_qos", None)
        if qos is None:
            qos = self.client.get_current_account_qos(resource_backend_id)
        else:
            self._last_qos = None  # consume the cached value
        return {"qos": qos}

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, int]]]:
        """Example output.

        {
            "account_name": {
                "TOTAL_ACCOUNT_USAGE": {
                    'cpu': 1,
                    'gres/gpu': 2,
                    'mem': 3,
                },
                "user1": {
                    'cpu': 1,
                    'gres/gpu': 2,
                    'mem': 3,
                },
            }
        }
        """
        report: dict[str, dict[str, dict[str, int]]] = {}
        lines = self.client.get_usage_report(resource_backend_ids, timezone=self.timezone)

        for line in lines:
            report.setdefault(line.account, {}).setdefault(line.user, {})
            tres_usage = line.tres_usage
            user_usage_existing = report[line.account][line.user]
            user_usage_new = backend_utils.sum_dicts([user_usage_existing, tres_usage])
            report[line.account][line.user] = user_usage_new

        for account_usage in report.values():
            usages_per_user = list(account_usage.values())
            total = backend_utils.sum_dicts(usages_per_user)
            account_usage["TOTAL_ACCOUNT_USAGE"] = total

        return self._convert_usage_report(report)

    def get_usage_report_for_period(
        self,
        resource_backend_ids: list[str],
        year: int,
        month: int,
        waldur_resource: Optional[WaldurResource] = None,  # noqa: ARG002
    ) -> dict[str, dict[str, dict[str, int]]]:
        """Generate usage report for a specific billing period.

        Args:
            resource_backend_ids: List of SLURM account names to query
            year: Year to query (e.g., 2024)
            month: Month to query (1-12)
            waldur_resource: Optional Waldur resource (unused for SLURM)

        Returns:
            Dictionary with same structure as _get_usage_report() but for historical data
        """
        report: dict[str, dict[str, dict[str, int]]] = {}
        lines = self.client.get_historical_usage_report(resource_backend_ids, year, month)

        for line in lines:
            report.setdefault(line.account, {}).setdefault(line.user, {})
            tres_usage = line.tres_usage
            user_usage_existing = report[line.account][line.user]
            user_usage_new = backend_utils.sum_dicts([user_usage_existing, tres_usage])
            report[line.account][line.user] = user_usage_new

        for account_usage in report.values():
            usages_per_user = list(account_usage.values())
            total = backend_utils.sum_dicts(usages_per_user)
            account_usage["TOTAL_ACCOUNT_USAGE"] = total

        return self._convert_usage_report(report)

    def _convert_usage_report(
        self, report: dict[str, dict[str, dict[str, int]]]
    ) -> dict[str, dict[str, dict[str, int]]]:
        """Convert SLURM TRES usage to Waldur component units.

        When ``target_components`` are configured, uses the ComponentMapper
        reverse conversion.  Otherwise falls back to standard unit_factor
        division.
        """
        report_converted: dict[str, dict[str, dict[str, int]]] = {}
        for account, account_usage in report.items():
            report_converted[account] = {}
            for username, usage_dict in account_usage.items():
                if not self._component_mapper.is_passthrough:
                    float_usage = {k: float(v) for k, v in usage_dict.items()}
                    source_usage = self._component_mapper.convert_usage_from_target(float_usage)
                    converted = {k: round(v) for k, v in source_usage.items()}
                else:
                    converted = utils.convert_slurm_units_to_waldur_ones(
                        self.backend_components, usage_dict
                    )
                report_converted[account][username] = converted
        return report_converted

    def list_active_user_jobs(self, account: str, user: str) -> list[str]:
        """List active jobs for account and user."""
        logger.info("Listing jobs for account %s and user %s", account, user)
        return self.client.list_active_user_jobs(account, user)

    def cancel_active_jobs_for_account_user(self, account: str, user: str) -> None:
        """Cancel account the active jobs for the specified account and user."""
        logger.info("Cancelling jobs for the account %s and user %s", account, user)
        self.client.cancel_active_user_jobs(account, user)

    def _pre_delete_user_actions(self, resource_backend_id: str, username: str) -> None:
        if not self.client.check_user_exists(username):
            logger.info(
                'The user "%s" does not exist in the cluster, skipping job cancellation', username
            )
            return
        job_ids = self.list_active_user_jobs(resource_backend_id, username)
        if len(job_ids) > 0:
            logger.info(
                "The active jobs for account %s and user %s: %s",
                resource_backend_id,
                username,
                ", ".join(job_ids),
            )
            self.cancel_active_jobs_for_account_user(resource_backend_id, username)

    def _pre_delete_resource(self, waldur_resource: WaldurResource) -> None:
        """Delete all existing associations, cancel jobs, and clean up LDAP/QoS."""
        backend_id = waldur_resource.backend_id
        if self.client.account_has_users(backend_id):
            logger.info("Cancelling all active jobs for account %s", backend_id)
            self.client.cancel_active_user_jobs(backend_id)
            logger.info("Removing all users from account %s", backend_id)
            self.client.delete_all_users_from_account(backend_id)

        # Clean up per-account QoS
        if self._qos_config.get("enabled") and self.client.qos_exists(backend_id):
            try:
                self.client.delete_qos(backend_id)
                logger.info("Deleted QoS %s", backend_id)
            except BackendError:
                logger.exception("Failed to delete QoS %s", backend_id)

        # Clean up LDAP project group
        if self._ldap_client:
            try:
                self._ldap_client.delete_group(backend_id)
                logger.info("Deleted LDAP project group %s", backend_id)
            except BackendError:
                logger.exception("Failed to delete LDAP project group %s", backend_id)

    def get_resource_limits(self, resource_backend_id: str) -> dict[str, int]:
        """Get account limits converted to Waldur-readable values."""
        account_limits_raw = self.client.get_resource_limits(resource_backend_id)
        return utils.convert_slurm_units_to_waldur_ones(
            self.backend_components, account_limits_raw, to_int=True
        )

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Set limits using ComponentMapper when target_components are configured.

        The base class only applies unit_factor, which is incorrect when
        ComponentMapper converts Waldur components (e.g. node_hours) to
        SLURM TRES (e.g. cpu, gpu).
        """
        if not self._component_mapper.is_passthrough:
            limit_based_components = [
                component
                for component, data in self.backend_components.items()
                if data["accounting_type"] in ("limit", "one")
            ]
            limit_values = {k: v for k, v in limits.items() if k in limit_based_components}
            converted = self._component_mapper.convert_limits_to_target(limit_values)
            int_limits = {k: int(v) for k, v in converted.items()}
            self.client.set_resource_limits(resource_backend_id, int_limits)
        else:
            super().set_resource_limits(resource_backend_id, limits)

    def sync_resource_end_date(
        self,
        waldur_resource: WaldurResource,
        waldur_rest_client: AuthenticatedClient,  # noqa: ARG002
    ) -> None:
        """Recalculate SLURM limits when end_date changes (e.g., renewal).

        For prepaid resources, GrpTRESMins = limit * duration_months * unit_factor.
        When end_date is extended via renewal, the total budget must be recalculated.
        """
        if not self.has_prepaid_components():
            return
        backend_id = waldur_resource.backend_id
        if not backend_id:
            return
        backend_limits, _ = self._collect_resource_limits(waldur_resource)
        if backend_limits:
            logger.info(
                "Syncing prepaid limits for %s: %s", backend_id, backend_limits,
            )
            self.client.set_resource_limits(backend_id, backend_limits)

    def set_resource_user_limits(
        self, resource_backend_id: str, username: str, limits: dict[str, int]
    ) -> None:
        """Set limits for a specific user in a resource on the backend."""
        converted_limits = {
            key: value * self.backend_components[key]["unit_factor"]
            for key, value in limits.items()
        }
        super().set_resource_user_limits(resource_backend_id, username, converted_limits)

    # ===== PERIODIC LIMITS EXTENSION =====

    def apply_periodic_settings(
        self,
        resource_id: str,
        settings: dict,
        config: Optional[dict] = None,  # noqa: ARG002
    ) -> dict:
        """Apply periodic settings calculated by Waldur with emulator support."""
        logger.info("Applying periodic settings for resource %s", resource_id)
        logger.debug("Settings: %s", settings)

        # Get periodic limits configuration
        periodic_config = self.backend_settings.get("periodic_limits", {})
        if not periodic_config.get("enabled", False):
            logger.warning("Periodic limits not enabled, skipping apply_periodic_settings")
            return {"success": False, "reason": "periodic_limits_not_enabled"}

        # Check if running in emulator mode
        if periodic_config.get("emulator_mode", False):
            return self._apply_settings_emulator(resource_id, settings, periodic_config)

        return self._apply_settings_production(resource_id, settings, periodic_config)

    def _apply_settings_emulator(self, resource_id: str, settings: dict, config: dict) -> dict:
        """Apply settings to SLURM emulator via API."""
        emulator_url = config.get("emulator_base_url", "http://localhost:8080")
        logger.info("Applying settings to SLURM emulator at %s", emulator_url)

        try:
            # Apply fairshare
            if settings.get("fairshare"):
                logger.debug(
                    "Setting fairshare=%s for account %s", settings["fairshare"], resource_id
                )
                response = requests.post(
                    f"{emulator_url}/api/apply-periodic-settings",
                    json={"resource_id": resource_id, "fairshare": settings["fairshare"]},
                    timeout=10,
                )
                response.raise_for_status()

            # Apply limits
            if settings.get("grp_tres_mins"):
                logger.debug(
                    "Setting GrpTRESMins=%s for account %s", settings["grp_tres_mins"], resource_id
                )
                response = requests.post(
                    f"{emulator_url}/api/apply-periodic-settings",
                    json={"resource_id": resource_id, "grp_tres_mins": settings["grp_tres_mins"]},
                    timeout=10,
                )
                response.raise_for_status()

            # Check and apply QoS if needed
            if settings.get("qos_threshold"):
                current_usage = self._get_current_usage_emulator(resource_id, emulator_url)
                threshold = next(iter(settings["qos_threshold"].values()))

                if current_usage >= threshold:
                    logger.info(
                        "Usage %s exceeds threshold %s, applying slowdown QoS",
                        current_usage,
                        threshold,
                    )
                    response = requests.post(
                        f"{emulator_url}/api/downscale-resource",
                        json={"resource_id": resource_id},
                        timeout=10,
                    )
                    response.raise_for_status()

            # Reset raw usage if requested
            if settings.get("reset_raw_usage"):
                logger.debug("Resetting raw usage for account %s", resource_id)
                response = requests.post(
                    f"{emulator_url}/api/apply-periodic-settings",
                    json={"resource_id": resource_id, "reset_raw_usage": True},
                    timeout=10,
                )
                response.raise_for_status()

            logger.info("Successfully applied settings to emulator")
            return {"success": True, "mode": PeriodicSettingsMode.EMULATOR.value}

        except requests.exceptions.RequestException as e:
            logger.error("Failed to apply settings to emulator: %s", e)
            return {"success": False, "error": str(e), "mode": PeriodicSettingsMode.EMULATOR.value}
        except Exception as e:
            logger.error("Unexpected error applying settings to emulator: %s", e)
            return {"success": False, "error": str(e), "mode": PeriodicSettingsMode.EMULATOR.value}

    def _apply_settings_production(self, resource_id: str, settings: dict, config: dict) -> dict:
        """Apply settings to production SLURM cluster."""
        logger.info("Applying settings to production SLURM cluster")
        self.client.clear_executed_commands()

        try:
            # Apply fairshare
            if settings.get("fairshare"):
                logger.debug(
                    "Setting fairshare=%s for account %s", settings["fairshare"], resource_id
                )
                self.client.set_account_fairshare(resource_id, settings["fairshare"])

            # Apply limits based on limit_type
            if settings.get("grp_tres_mins") or settings.get("max_tres_mins"):
                limit_type = settings.get("limit_type", config.get("limit_type", "GrpTRESMins"))
                limits = settings.get("grp_tres_mins") or settings.get("max_tres_mins")
                logger.debug("Setting %s=%s for account %s", limit_type, limits, resource_id)
                self.client.set_account_limits(resource_id, limit_type, limits)

            # Reset raw usage if requested
            if settings.get("reset_raw_usage"):
                logger.debug("Resetting raw usage for account %s", resource_id)
                self.client.reset_raw_usage(resource_id)

            # Check QoS thresholds
            if settings.get("qos_threshold"):
                current_usage = self.client.get_current_usage(resource_id)
                self._check_and_apply_qos(resource_id, current_usage, settings, config)

            logger.info("Successfully applied settings to production SLURM")
            return {
                "success": True,
                "mode": PeriodicSettingsMode.PRODUCTION.value,
                "commands_executed": list(self.client.executed_commands),
            }

        except Exception as e:
            logger.error("Failed to apply settings to production SLURM: %s", e)
            return {
                "success": False,
                "error": str(e),
                "mode": PeriodicSettingsMode.PRODUCTION.value,
                "commands_executed": list(self.client.executed_commands),
            }

    def _get_current_usage_emulator(self, resource_id: str, emulator_url: str) -> float:
        """Get current usage from emulator."""
        try:
            response = requests.get(
                f"{emulator_url}/api/status",
                params={"account": resource_id},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("current_usage", 0.0)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to get current usage from emulator: %s", e)
            return 0.0

    def _check_and_apply_qos(
        self, resource_id: str, current_usage: dict, settings: dict, config: dict
    ) -> None:
        """Check usage thresholds and apply QoS if needed."""
        if not settings.get("qos_threshold"):
            return

        qos_levels = config.get("qos_levels", {})
        threshold_data = settings["qos_threshold"]

        # Convert current usage to comparable format (billing units)
        if config.get("tres_billing_enabled", True):
            # Use billing units
            usage_value = current_usage.get("billing", 0) if isinstance(current_usage, dict) else 0
            threshold_value = (
                threshold_data.get("billing", 0) if isinstance(threshold_data, dict) else 0
            )
        else:
            # Use node-hours
            usage_value = current_usage.get("node", 0) if isinstance(current_usage, dict) else 0
            threshold_value = (
                threshold_data.get("node", 0) if isinstance(threshold_data, dict) else 0
            )

        grace_limit = settings.get("grace_limit", {})
        if config.get("tres_billing_enabled", True):
            grace_value = (
                grace_limit.get("billing", float("inf"))
                if isinstance(grace_limit, dict)
                else float("inf")
            )
        else:
            grace_value = (
                grace_limit.get("node", float("inf"))
                if isinstance(grace_limit, dict)
                else float("inf")
            )

        current_qos = self.client.get_current_account_qos(resource_id)
        new_qos = None

        if usage_value >= grace_value:
            # Hard limit exceeded - block jobs
            new_qos = qos_levels.get("blocked", "blocked")
            logger.warning(
                "Account %s exceeded grace limit (%s >= %s), setting QoS to %s",
                resource_id,
                usage_value,
                grace_value,
                new_qos,
            )
        elif usage_value >= threshold_value:
            # Soft limit exceeded - apply slowdown
            new_qos = qos_levels.get("slowdown", "slowdown")
            logger.info(
                "Account %s exceeded threshold (%s >= %s), setting QoS to %s",
                resource_id,
                usage_value,
                threshold_value,
                new_qos,
            )
        else:
            # Usage under threshold - restore normal QoS
            new_qos = qos_levels.get("default", "normal")

        if current_qos != new_qos:
            logger.info("Changing QoS for account %s: %s -> %s", resource_id, current_qos, new_qos)
            self.client.set_account_qos(resource_id, new_qos)
        else:
            logger.debug("QoS for account %s unchanged: %s", resource_id, current_qos)
