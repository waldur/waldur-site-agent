"""Custom backend implementation.

This module implements the BaseBackend interface for the custom backend.
See docs/plugin-development-guide.md for full documentation.
"""

from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.backends import BaseBackend

from .client import MyCustomClient


class MyCustomBackend(BaseBackend):
    """Custom backend for Waldur Site Agent.

    TODO: Replace with description of your backend system.
    """

    # Set to True if usage can decrease between reports (e.g., storage backends).
    supports_decreasing_usage: bool = False

    def __init__(self, backend_settings: dict, backend_components: dict[str, dict]) -> None:
        """Initialize the custom backend.

        Args:
            backend_settings: Backend-specific settings from YAML config.
            backend_components: Component definitions from YAML config.
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "mycustom"
        self.client = MyCustomClient()
        # TODO: Pass any needed settings to your client

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if the backend system is reachable.

        TODO: Implement a lightweight connectivity check.
        """
        # TODO: Replace with actual health check
        # Example: try listing resources or calling a status endpoint
        try:
            self.client.list_resources()
            return True
        except Exception:
            if raise_exception:
                raise
            return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about the backend.

        TODO: Log version, connectivity, configuration details.
        """
        # TODO: Add diagnostic logging
        return self.ping()

    def list_components(self) -> list[str]:
        """Return available component types on the backend.

        TODO: Query backend for available component types, or return
        a static list matching your backend_components config.
        """
        # TODO: Replace with actual component discovery
        return list(self.backend_components.keys())

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect usage report for the specified resources.

        Must return a dict with this structure:
        {
            "resource_id": {
                "TOTAL_ACCOUNT_USAGE": {"component": value, ...},
                "username": {"component": value, ...},
            }
        }

        Values must be in Waldur units (divide raw backend values by unit_factor).

        TODO: Implement usage collection from your backend.

        Note: self.timezone is available for timezone-aware date calculations.
        Use backend_utils.get_current_time_in_timezone(self.timezone) for current time.
        """
        report: dict = {}

        for resource_id in resource_backend_ids:
            # TODO: Fetch raw usage from backend via self.client.get_usage_report()
            # TODO: Convert from backend units to Waldur units by dividing by unit_factor
            # TODO: Aggregate per-user usage and compute TOTAL_ACCOUNT_USAGE

            empty_usage = dict.fromkeys(self.backend_components, 0)
            report[resource_id] = {"TOTAL_ACCOUNT_USAGE": empty_usage}

        return report

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Convert Waldur resource limits to backend limits.

        Returns (backend_limits, waldur_limits) where:
        - backend_limits = waldur_value * unit_factor (for setting on backend)
        - waldur_limits = original Waldur values (for storing in Waldur)

        TODO: Implement limit conversion for your backend.
        """
        backend_limits: dict[str, int] = {}
        waldur_limits: dict[str, int] = {}

        if not waldur_resource.limits:
            return backend_limits, waldur_limits

        for component_key, component_config in self.backend_components.items():
            waldur_value = getattr(waldur_resource.limits, component_key, None)
            if waldur_value is not None:
                unit_factor = component_config.get("unit_factor", 1)
                backend_limits[component_key] = int(waldur_value) * unit_factor
                waldur_limits[component_key] = int(waldur_value)

        return backend_limits, waldur_limits

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Perform actions before resource creation.

        TODO: Set up any prerequisites (e.g., parent accounts, projects).
        Use pass if no pre-creation setup is needed.
        """
        # TODO: Implement pre-creation logic if needed

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale the resource, restricting its capabilities.

        TODO: Implement downscaling or return True if not applicable.
        """
        del resource_backend_id
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause the resource, preventing all usage.

        TODO: Implement pausing or return True if not applicable.
        """
        del resource_backend_id
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore the resource to normal operation.

        TODO: Implement restoration or return True if not applicable.
        """
        del resource_backend_id
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get backend-specific resource metadata for Waldur.

        TODO: Return relevant metadata or empty dict.
        """
        del resource_backend_id
        return {}
