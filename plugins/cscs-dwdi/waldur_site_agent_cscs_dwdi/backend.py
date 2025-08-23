"""CSCS-DWDI backend implementation for usage reporting."""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.backends import BaseBackend

from .client import CSCSDWDIClient

logger = logging.getLogger(__name__)


class CSCSDWDIBackend(BaseBackend):
    """Backend for reporting usage from CSCS-DWDI API."""

    def __init__(
        self, backend_settings: dict[str, Any], backend_components: dict[str, dict]
    ) -> None:
        """Initialize CSCS-DWDI backend.

        Args:
            backend_settings: Backend-specific settings from the offering
            backend_components: Component configuration from the offering
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "cscs-dwdi"

        # Extract CSCS-DWDI specific configuration
        self.api_url = backend_settings.get("cscs_dwdi_api_url", "")
        self.client_id = backend_settings.get("cscs_dwdi_client_id", "")
        self.client_secret = backend_settings.get("cscs_dwdi_client_secret", "")

        # Required OIDC configuration
        self.oidc_token_url = backend_settings.get("cscs_dwdi_oidc_token_url", "")
        self.oidc_scope = backend_settings.get("cscs_dwdi_oidc_scope")

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
        )

    def ping(self, raise_exception: bool = False) -> bool:  # noqa: ARG002
        """Check if CSCS-DWDI API is accessible.

        Args:
            raise_exception: Whether to raise an exception on failure

        Returns:
            True if API is accessible, False otherwise
        """
        return self.cscs_client.ping()

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Get usage report for specified resources.

        This method queries the CSCS-DWDI API for the current month's usage
        and formats it according to Waldur's expected structure.

        Args:
            resource_backend_ids: List of account identifiers to report on

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
