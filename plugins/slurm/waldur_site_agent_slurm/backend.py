"""SLURM-specific backend classes and functions."""

import pprint
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import (
    BackendType,
    backends,
    logger,
)
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent_slurm import utils
from waldur_site_agent_slurm.client import SlurmClient


class SlurmBackend(backends.BaseBackend):
    """Main class for management of SLURM resources."""

    def __init__(self, slurm_settings: dict, slurm_tres: dict[str, dict]) -> None:
        """Init backend data and creates a corresponding client."""
        super().__init__(slurm_settings, slurm_tres)
        self.backend_type = BackendType.SLURM.value
        self.client: SlurmClient = SlurmClient(slurm_tres)

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Override parent method to validate slug fields."""
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

        project_backend_id = self._get_project_backend_id(waldur_resource.project_slug)

        # Setup customer resource
        customer_backend_id = self._get_customer_backend_id(waldur_resource.customer_slug)
        self._create_backend_resource(
            customer_backend_id, waldur_resource.customer_name, customer_backend_id
        )

        # Create project resource
        self._create_backend_resource(
            project_backend_id,
            waldur_resource.project_name,
            project_backend_id,
            customer_backend_id,
        )

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

        try:
            self.ping(raise_exception=True)
            logger.info("SLURM cluster ping is successful")
        except BackendError as err:
            logger.error("Unable to ping SLURM cluster, reason: %s", err)

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
                    umask = self.backend_settings.get("default_homedir_umask", "0700")
                    self.create_user_homedirs(usernames, umask)

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect SLURM and Waldur limits separately."""
        allocation_limits = backend_utils.get_usage_based_limits(self.backend_components)
        limit_based_components = [
            component
            for component, data in self.backend_components.items()
            if data["accounting_type"] == "limit"
        ]
        if waldur_resource.limits:
            # Add limit-based limits
            for component_key in limit_based_components:
                allocation_limits[component_key] = (
                    waldur_resource.limits.to_dict()[component_key]
                    * self.backend_components[component_key]["unit_factor"]
                )

            # Keep only limit-based components for Waldur resource
            if waldur_resource.limits:
                waldur_resource_limits = {
                    component_key: waldur_resource.limits.to_dict()[component_key]
                    for component_key, data in self.backend_components.items()
                    if data["accounting_type"] == "limit"
                }
        else:
            waldur_resource_limits = {}
        return allocation_limits, waldur_resource_limits

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add specified users to the allocations on the SLURM cluster."""
        added_users = super().add_users_to_resource(resource_backend_id, user_ids)

        if self.backend_settings.get("enable_user_homedir_account_creation", True):
            umask: str = str(kwargs.get("homedir_umask", "0700"))
            # Only create homedirs for users that don't already have them
            # (avoids duplicates if they were created during resource creation)
            self.create_user_homedirs(added_users, umask=umask)

        return added_users

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
            return True

        logger.info("Setting %s QoS for the SLURM account", qos_downscaled)
        self.client.set_account_qos(resource_backend_id, qos_downscaled)
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
            return True

        logger.info("Setting %s QoS for the SLURM account", qos_paused)
        self.client.set_account_qos(resource_backend_id, qos_paused)
        logger.info("The new QoS successfully set")
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore resource QoS to the default one."""
        current_qos = self.client.get_current_account_qos(resource_backend_id)

        default_qos = self.backend_settings.get("qos_default", "normal")

        if current_qos in [None, ""]:
            logger.info("The account does not have an active QoS set, skipping reset")
            return False

        logger.info("The current QoS is %s", current_qos)

        if current_qos == default_qos:
            logger.info("The account already has the default QoS (%s)", default_qos)
            return False

        logger.info("Setting %s QoS", default_qos)
        self.client.set_account_qos(resource_backend_id, default_qos)
        new_qos = self.client.get_current_account_qos(resource_backend_id)
        logger.info("The new QoS is %s", new_qos)

        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return backend metadata for the SLURM account (QoS only for now)."""
        current_qos = self.client.get_current_account_qos(resource_backend_id)
        return {"qos": current_qos}

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
        lines = self.client.get_usage_report(resource_backend_ids)

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

        # Convert SLURM units to Waldur ones
        report_converted: dict[str, dict[str, dict[str, int]]] = {}
        for account, account_usage in report.items():
            report_converted[account] = {}
            for username, usage_dict in account_usage.items():
                converted_usage_dict = utils.convert_slurm_units_to_waldur_ones(
                    self.backend_components, usage_dict
                )
                report_converted[account][username] = converted_usage_dict

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
        """Delete all existing associations and cancel all the active jobs."""
        backend_id = waldur_resource.backend_id
        if self.client.account_has_users(backend_id):
            logger.info("Cancelling all active jobs for account %s", backend_id)
            self.client.cancel_active_user_jobs(backend_id)
            logger.info("Removing all users from account %s", backend_id)
            self.client.delete_all_users_from_account(backend_id)

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Set limits for limit-based components in the SLURM allocation."""
        # Convert limits
        converted_limits = {
            key: value * self.backend_components[key]["unit_factor"]
            for key, value in limits.items()
        }
        super().set_resource_limits(resource_backend_id, converted_limits)

    def get_resource_limits(self, resource_backend_id: str) -> dict[str, int]:
        """Get account limits converted to Waldur-readable values."""
        account_limits_raw = self.client.get_resource_limits(resource_backend_id)
        return utils.convert_slurm_units_to_waldur_ones(
            self.backend_components, account_limits_raw, to_int=True
        )

    def set_resource_user_limits(
        self, resource_backend_id: str, username: str, limits: dict[str, int]
    ) -> None:
        """Set limits for a specific user in a resource on the backend."""
        converted_limits = {
            key: value * self.backend_components[key]["unit_factor"]
            for key, value in limits.items()
        }
        super().set_resource_user_limits(resource_backend_id, username, converted_limits)
