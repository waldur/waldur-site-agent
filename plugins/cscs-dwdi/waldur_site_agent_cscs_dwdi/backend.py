"""CSCS-DWDI backend implementations for compute and storage usage reporting."""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.types import Unset

from waldur_site_agent.backend import structures
from waldur_site_agent.backend.backends import BaseBackend

from .client import CSCSDWDIClient

logger = logging.getLogger(__name__)


class CSCSDWDIComputeBackend(BaseBackend):
    """Backend for reporting compute usage from CSCS-DWDI API."""

    def __init__(
        self, backend_settings: dict[str, Any], backend_components: dict[str, dict]
    ) -> None:
        """Initialize CSCS-DWDI backend.

        Args:
            backend_settings: Backend-specific settings from the offering
            backend_components: Component configuration from the offering
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "cscs-dwdi-compute"

        # Extract CSCS-DWDI specific configuration
        self.api_url = backend_settings.get("cscs_dwdi_api_url", "")
        self.client_id = backend_settings.get("cscs_dwdi_client_id", "")
        self.client_secret = backend_settings.get("cscs_dwdi_client_secret", "")

        # Required OIDC configuration
        self.oidc_token_url = backend_settings.get("cscs_dwdi_oidc_token_url", "")
        self.oidc_scope = backend_settings.get("cscs_dwdi_oidc_scope")

        # Optional SOCKS proxy configuration
        self.socks_proxy = backend_settings.get("socks_proxy")

        if not all([self.api_url, self.client_id, self.client_secret, self.oidc_token_url]):
            msg = (
                "CSCS-DWDI backend requires cscs_dwdi_api_url, cscs_dwdi_client_id, "
                "cscs_dwdi_client_secret, and cscs_dwdi_oidc_token_url in backend_settings"
            )
            raise ValueError(msg)

        self.cscs_client = CSCSDWDIClient(
            api_url=self.api_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            oidc_token_url=self.oidc_token_url,
            oidc_scope=self.oidc_scope,
            socks_proxy=self.socks_proxy,
        )

        if self.socks_proxy:
            logger.info("CSCS-DWDI Compute Backend: Using SOCKS proxy: %s", self.socks_proxy)

    def pull_resource(
        self, waldur_resource: WaldurResource
    ) -> Optional[structures.BackendResourceInfo]:
        """Pull resource from backend with cluster filtering support."""
        try:
            backend_id = waldur_resource.backend_id
            backend_resource_info = self._pull_backend_resource(backend_id, waldur_resource)
            if backend_resource_info is None:
                return None
        except Exception:
            logger.exception("Error while pulling resource [%s]", backend_id)
            return None
        else:
            return backend_resource_info

    def ping(self, raise_exception: bool = False) -> bool:  # noqa: ARG002
        """Check if CSCS-DWDI API is accessible.

        Args:
            raise_exception: Whether to raise an exception on failure

        Returns:
            True if API is accessible, False otherwise
        """
        return self.cscs_client.ping()

    def _get_usage_report(
        self, resource_backend_ids: list[str], clusters: Optional[list[str]] = None
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Get usage report for specified resources.

        This method queries the CSCS-DWDI API for the current month's usage
        and formats it according to Waldur's expected structure.

        Args:
            resource_backend_ids: List of account identifiers to report on
            clusters: Optional list of cluster names to filter by

        Returns:
            Dictionary mapping account names to usage data:
            {
                "account1": {
                    "TOTAL_ACCOUNT_USAGE": {
                        "nodeHours": 1234.56
                    },
                    "user1": {
                        "nodeHours": 500.00
                    },
                    "user2": {
                        "nodeHours": 734.56
                    }
                },
                ...
            }
        """
        if not resource_backend_ids:
            logger.warning("No resource backend IDs provided for usage report")
            return {}

        # Get current month's date range
        today = datetime.now(tz=timezone.utc).date()
        from_date = today.replace(day=1)
        to_date = today

        logger.info(
            "Fetching usage report for %d accounts from %s to %s",
            len(resource_backend_ids),
            from_date,
            to_date,
        )

        try:
            # Query CSCS-DWDI API for usage data
            response = self.cscs_client.get_usage_for_month(
                accounts=resource_backend_ids,
                from_date=from_date,
                to_date=to_date,
                clusters=clusters,
            )

            # Process the response
            usage_report = self._process_api_response(response)

            # Filter to only include requested accounts
            filtered_report = {
                account: data
                for account, data in usage_report.items()
                if account in resource_backend_ids
            }

            logger.info(
                "Successfully retrieved usage for %d accounts",
                len(filtered_report),
            )

            return filtered_report

        except Exception:
            logger.exception("Failed to get usage report from CSCS-DWDI")
            raise

    def _process_api_response(
        self, response: dict[str, Any]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Process CSCS-DWDI API response into Waldur format.

        Args:
            response: Raw API response from CSCS-DWDI

        Returns:
            Formatted usage report for Waldur with configured component mappings
        """
        usage_report = {}

        # The response has a "compute" field with list of account data
        compute_data = response.get("compute", [])

        for account_data in compute_data:
            account_name = account_data.get("account")
            if not account_name:
                logger.warning("Account data missing account name, skipping")
                continue

            # Extract total account usage for all configured components
            total_usage = self._extract_component_usage_from_account_data(account_data)

            # Initialize account entry
            usage_report[account_name] = {"TOTAL_ACCOUNT_USAGE": total_usage}

            # Process per-user usage
            users = account_data.get("users", [])
            user_usage: dict[str, dict[str, float]] = {}

            for user_data in users:
                username = user_data.get("username")
                if not username:
                    continue

                # Extract user component usage
                user_component_usage = self._extract_component_usage_from_user_data(user_data)

                if username in user_usage:
                    # Aggregate usage for same user across different dates/clusters
                    for component_name, value in user_component_usage.items():
                        user_usage[username][component_name] = (
                            user_usage[username].get(component_name, 0.0) + value
                        )
                else:
                    user_usage[username] = user_component_usage

            # Add rounded user usage to report
            for username, component_usage in user_usage.items():
                rounded_usage = {comp: round(value, 2) for comp, value in component_usage.items()}
                usage_report[account_name][username] = rounded_usage

        return usage_report

    def _extract_component_usage_from_account_data(
        self, account_data: dict[str, Any]
    ) -> dict[str, float]:
        """Extract component usage from account-level data.

        Args:
            account_data: Account data from CSCS-DWDI API response

        Returns:
            Dictionary mapping component names to usage values
        """
        usage = {}

        for component_name, component_config in self.backend_components.items():
            # Look for component usage in account data
            # Try multiple naming patterns to match API response fields to component names
            raw_value = 0.0

            # Try exact match first (e.g., nodeHours -> nodeHours)
            if component_name in account_data:
                raw_value = account_data[component_name]
            # Try account prefix preserving case (e.g., nodeHours -> accountNodeHours)
            elif f"account{component_name[0].upper()}{component_name[1:]}" in account_data:
                raw_value = account_data[f"account{component_name[0].upper()}{component_name[1:]}"]
            # Try total prefix (e.g., nodeHours -> totalNodeHours)
            elif f"total{component_name[0].upper()}{component_name[1:]}" in account_data:
                raw_value = account_data[f"total{component_name[0].upper()}{component_name[1:]}"]
            # Try lowercase with account prefix
            elif f"account{component_name}" in account_data:
                raw_value = account_data[f"account{component_name}"]

            # Apply unit factor conversion
            unit_factor = component_config.get("unit_factor", 1)
            converted_value = raw_value * unit_factor
            usage[component_name] = round(converted_value, 2)

        return usage

    def _extract_component_usage_from_user_data(
        self, user_data: dict[str, Any]
    ) -> dict[str, float]:
        """Extract component usage from user-level data.

        Args:
            user_data: User data from CSCS-DWDI API response

        Returns:
            Dictionary mapping component names to usage values
        """
        usage = {}

        for component_name, component_config in self.backend_components.items():
            # Look for component usage in user data
            # The API should return fields that match component names
            raw_value = user_data.get(component_name, 0.0)

            # Apply unit factor conversion
            unit_factor = component_config.get("unit_factor", 1)
            converted_value = raw_value * unit_factor
            usage[component_name] = converted_value

        return usage

    def _pull_backend_resource(
        self, resource_backend_id: str, waldur_resource: Optional[WaldurResource] = None
    ) -> Optional[structures.BackendResourceInfo]:
        """Pull resource data from the DWDI backend for usage reporting.

        For DWDI, we treat the resource_backend_id as an account name
        and fetch usage data for that account from the DWDI API.

        Args:
            resource_backend_id: Account name (e.g., 'g207')
            waldur_resource: Optional Waldur resource object for filtering

        Returns:
            BackendResourceInfo with usage data or None if account not found
        """
        logger.info("Pulling resource %s", resource_backend_id)

        # For DWDI, the resource_backend_id is the account name
        account_name = resource_backend_id

        # Extract cluster from offering_slug for filtering (always lowercase)
        clusters = None
        if (
            waldur_resource
            and hasattr(waldur_resource, "offering_slug")
            and waldur_resource.offering_slug
            and not isinstance(waldur_resource.offering_slug, Unset)
        ):
            # Use offering_slug as cluster name, converted to lowercase
            cluster_name = waldur_resource.offering_slug.lower()
            clusters = [cluster_name]
            logger.info(
                "Filtering DWDI query by cluster: %s (lowercase from offering_slug)",
                cluster_name,
            )

        # Get usage data for this account
        try:
            usage_report = self._get_usage_report([account_name], clusters=clusters)

            if account_name not in usage_report:
                logger.warning("There is no account with ID %s in the DWDI backend", account_name)
                return None

            # Extract usage data for this account
            account_usage = usage_report[account_name]

            # Extract users (everyone except TOTAL_ACCOUNT_USAGE)
            users = [username for username in account_usage if username != "TOTAL_ACCOUNT_USAGE"]

            logger.info(
                "Found usage data for account %s with %d users: %s", account_name, len(users), users
            )

            return structures.BackendResourceInfo(
                users=users,
                usage=account_usage,
            )

        except Exception:
            logger.exception("Error while pulling account %s from DWDI", account_name)
            return None

    # Methods not implemented for reporting-only backend
    def get_account(self, account_name: str) -> Optional[dict[str, Any]]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support account management"
        raise NotImplementedError(msg)

    def create_account(self, account_data: dict) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support account creation"
        raise NotImplementedError(msg)

    def delete_account(self, account_name: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support account deletion"
        raise NotImplementedError(msg)

    def update_account_limit_deposit(
        self,
        account_name: str,
        component_type: str,
        component_amount: float,
        offering_component_data: dict,
    ) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support limit updates"
        raise NotImplementedError(msg)

    def reset_account_limit_deposit(
        self,
        account_name: str,
        component_type: str,
        offering_component_data: dict,
    ) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support limit resets"
        raise NotImplementedError(msg)

    def add_account_users(self, account_name: str, user_backend_ids: list[str]) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support user management"
        raise NotImplementedError(msg)

    def delete_account_users(self, account_name: str, user_backend_ids: list[str]) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support user management"
        raise NotImplementedError(msg)

    def list_accounts(self) -> list[dict[str, Any]]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support account listing"
        raise NotImplementedError(msg)

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource limits"
        raise NotImplementedError(msg)

    def diagnostics(self) -> bool:
        """Get diagnostic information for the backend."""
        logger.info(
            "CSCS-DWDI Backend Diagnostics - Type: %s, API: %s, Components: %s, Ping: %s",
            self.backend_type,
            self.api_url,
            list(self.backend_components.keys()),
            self.ping(),
        )
        return self.ping()

    def get_resource_metadata(self, resource_backend_id: str) -> dict[str, Any]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource metadata"
        raise NotImplementedError(msg)

    def list_components(self) -> list[str]:
        """List configured components for this backend."""
        return list(self.backend_components.keys())

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource limits"
        raise NotImplementedError(msg)

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource creation"
        raise NotImplementedError(msg)

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource pausing"
        raise NotImplementedError(msg)

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource restoration"
        raise NotImplementedError(msg)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI backend is reporting-only and does not support resource downscaling"
        raise NotImplementedError(msg)


class CSCSDWDIStorageBackend(BaseBackend):
    """Backend for reporting storage usage from CSCS-DWDI API."""

    def __init__(
        self, backend_settings: dict[str, Any], backend_components: dict[str, dict]
    ) -> None:
        """Initialize CSCS-DWDI storage backend.

        Args:
            backend_settings: Backend-specific settings from the offering
            backend_components: Component configuration from the offering
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "cscs-dwdi-storage"

        # Extract CSCS-DWDI specific configuration
        self.api_url = backend_settings.get("cscs_dwdi_api_url", "")
        self.client_id = backend_settings.get("cscs_dwdi_client_id", "")
        self.client_secret = backend_settings.get("cscs_dwdi_client_secret", "")

        # Required OIDC configuration
        self.oidc_token_url = backend_settings.get("cscs_dwdi_oidc_token_url", "")
        self.oidc_scope = backend_settings.get("cscs_dwdi_oidc_scope")

        # Storage-specific settings
        self.filesystem = backend_settings.get("storage_filesystem", "")
        self.data_type = backend_settings.get("storage_data_type", "")
        self.tenant = backend_settings.get("storage_tenant", "")
        self.path_mapping = backend_settings.get("storage_path_mapping", {})

        # Optional SOCKS proxy configuration
        self.socks_proxy = backend_settings.get("socks_proxy")

        if not all([self.api_url, self.client_id, self.client_secret, self.oidc_token_url]):
            msg = (
                "CSCS-DWDI storage backend requires cscs_dwdi_api_url, cscs_dwdi_client_id, "
                "cscs_dwdi_client_secret, and cscs_dwdi_oidc_token_url in backend_settings"
            )
            raise ValueError(msg)

        if not all([self.filesystem, self.data_type]):
            msg = (
                "CSCS-DWDI storage backend requires storage_filesystem and storage_data_type "
                "in backend_settings"
            )
            raise ValueError(msg)

        self.cscs_client = CSCSDWDIClient(
            api_url=self.api_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            oidc_token_url=self.oidc_token_url,
            oidc_scope=self.oidc_scope,
            socks_proxy=self.socks_proxy,
        )

        if self.socks_proxy:
            logger.info("CSCS-DWDI Storage Backend: Using SOCKS proxy: %s", self.socks_proxy)

    def ping(self, raise_exception: bool = False) -> bool:  # noqa: ARG002
        """Check if CSCS-DWDI API is accessible.

        Args:
            raise_exception: Whether to raise an exception on failure

        Returns:
            True if API is accessible, False otherwise
        """
        return self.cscs_client.ping_storage()

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Get storage usage report for specified resources.

        This method queries the CSCS-DWDI storage API for the current month's usage
        and formats it according to Waldur's expected structure.

        Args:
            resource_backend_ids: List of resource identifiers (paths or mapped IDs)

        Returns:
            Dictionary mapping resource IDs to usage data:
            {
                "resource1": {
                    "TOTAL_ACCOUNT_USAGE": {
                        "storage_space": 1234.56,  # GB
                        "storage_inodes": 50000
                    }
                },
                ...
            }
        """
        if not resource_backend_ids:
            logger.warning("No resource backend IDs provided for storage usage report")
            return {}

        # Get current month for reporting
        today = datetime.now(tz=timezone.utc).date()
        exact_month = today.strftime("%Y-%m")

        logger.info(
            "Fetching storage usage report for %d resources for month %s",
            len(resource_backend_ids),
            exact_month,
        )

        usage_report = {}

        try:
            # Map resource IDs to storage paths
            paths_to_query = []
            id_to_path_map = {}

            for resource_id in resource_backend_ids:
                # Check if we have a path mapping for this resource ID
                if resource_id in self.path_mapping:
                    path = self.path_mapping[resource_id]
                    paths_to_query.append(path)
                    id_to_path_map[path] = resource_id
                else:
                    # Assume the resource_id is the path itself
                    paths_to_query.append(resource_id)
                    id_to_path_map[resource_id] = resource_id

            if not paths_to_query:
                logger.warning("No paths to query after mapping")
                return {}

            # Query CSCS-DWDI storage API
            response = self.cscs_client.get_storage_usage_for_month(
                paths=paths_to_query,
                tenant=self.tenant,
                filesystem=self.filesystem,
                data_type=self.data_type,
                exact_month=exact_month,
            )

            # Process the response
            storage_data = response.get("storage", [])

            for storage_entry in storage_data:
                path = storage_entry.get("path")
                if not path:
                    logger.warning("Storage entry missing path, skipping")
                    continue

                # Map path back to resource ID
                resource_id = id_to_path_map.get(path, path)

                # Extract storage metrics
                space_used_bytes = storage_entry.get("spaceUsed", 0)
                inodes_used = storage_entry.get("inodesUsed", 0)

                # Convert bytes to configured units (typically GB)
                storage_usage = {}
                for component_name, component_config in self.backend_components.items():
                    if (
                        "storage_space" in component_name.lower()
                        or "space" in component_name.lower()
                    ):
                        # Apply unit factor for space (e.g., bytes to GB)
                        unit_factor = component_config.get("unit_factor", 1)
                        storage_usage[component_name] = round(space_used_bytes * unit_factor, 2)
                    elif "inode" in component_name.lower() or "file" in component_name.lower():
                        # Inodes typically don't need conversion
                        unit_factor = component_config.get("unit_factor", 1)
                        storage_usage[component_name] = round(inodes_used * unit_factor, 2)

                usage_report[resource_id] = {"TOTAL_ACCOUNT_USAGE": storage_usage}

            logger.info(
                "Successfully retrieved storage usage for %d resources",
                len(usage_report),
            )

            return usage_report

        except Exception:
            logger.exception("Failed to get storage usage report from CSCS-DWDI")
            raise

    # Methods not implemented for reporting-only backend
    def get_account(self, account_name: str) -> Optional[dict[str, Any]]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support account management"
        raise NotImplementedError(msg)

    def create_account(self, account_data: dict) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support account creation"
        raise NotImplementedError(msg)

    def delete_account(self, account_name: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support account deletion"
        raise NotImplementedError(msg)

    def update_account_limit_deposit(
        self,
        account_name: str,
        component_type: str,
        component_amount: float,
        offering_component_data: dict,
    ) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support limit updates"
        raise NotImplementedError(msg)

    def reset_account_limit_deposit(
        self,
        account_name: str,
        component_type: str,
        offering_component_data: dict,
    ) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support limit resets"
        raise NotImplementedError(msg)

    def add_account_users(self, account_name: str, user_backend_ids: list[str]) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support user management"
        raise NotImplementedError(msg)

    def delete_account_users(self, account_name: str, user_backend_ids: list[str]) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support user management"
        raise NotImplementedError(msg)

    def list_accounts(self) -> list[dict[str, Any]]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support account listing"
        raise NotImplementedError(msg)

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support resource limits"
        raise NotImplementedError(msg)

    def diagnostics(self) -> bool:
        """Get diagnostic information for the backend."""
        logger.info(
            "CSCS-DWDI Storage Backend Diagnostics - Type: %s, API: %s, "
            "Filesystem: %s, DataType: %s, Components: %s, Ping: %s",
            self.backend_type,
            self.api_url,
            self.filesystem,
            self.data_type,
            list(self.backend_components.keys()),
            self.ping(),
        )
        return self.ping()

    def get_resource_metadata(self, resource_backend_id: str) -> dict[str, Any]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support resource metadata"
        raise NotImplementedError(msg)

    def list_components(self) -> list[str]:
        """List configured components for this backend."""
        return list(self.backend_components.keys())

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support resource limits"
        raise NotImplementedError(msg)

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support resource creation"
        raise NotImplementedError(msg)

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = "CSCS-DWDI storage backend is reporting-only and does not support resource pausing"
        raise NotImplementedError(msg)

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = (
            "CSCS-DWDI storage backend is reporting-only and does not support resource restoration"
        )
        raise NotImplementedError(msg)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Not implemented for reporting-only backend."""
        msg = (
            "CSCS-DWDI storage backend is reporting-only and does not support resource downscaling"
        )
        raise NotImplementedError(msg)
