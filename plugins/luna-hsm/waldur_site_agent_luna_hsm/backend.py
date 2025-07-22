"""Thales Luna HSM backend for Waldur Site Agent."""

import pprint
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import backends, logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Resource
from waldur_site_agent_luna_hsm.client import LunaHsmClient
from waldur_site_agent_luna_hsm.storage import MonthlyStatsStorage


class LunaHsmBackend(backends.BaseBackend):
    """Backend for Thales Luna HSM crypto operations reporting."""

    def __init__(self, luna_hsm_settings: dict, luna_hsm_components: dict[str, dict]) -> None:
        """Initialize Luna HSM backend."""
        super().__init__(luna_hsm_settings, luna_hsm_components)
        self.backend_type = "luna_hsm"
        self.client: LunaHsmClient = LunaHsmClient(luna_hsm_settings)

        # Initialize storage for monthly statistics
        storage_path = luna_hsm_settings.get(
            "stats_storage_path", "/var/lib/waldur-site-agent/luna-hsm-stats.json"
        )
        self.storage = MonthlyStatsStorage(storage_path)

    def ping(self, raise_exception: bool = False) -> bool:
        """Test connectivity to Luna HSM."""
        try:
            metrics = self.client.get_metrics()
            if "metrics" in metrics:
                logger.debug("Luna HSM ping successful")
                return True
            logger.warning("Luna HSM ping failed - no metrics in response")
            if raise_exception:
                self._raise_ping_error("Luna HSM API returned invalid response")
            return False
        except Exception as e:
            logger.error("Luna HSM ping failed: %s", e)
            if raise_exception:
                msg = f"Failed to connect to Luna HSM: {e}"
                raise BackendError(msg) from e
            return False

    def diagnostics(self) -> bool:
        """Run diagnostics for Luna HSM backend."""
        format_string = "{:<30} = {:<30}"
        logger.info(format_string.format("Luna HSM API URL", self.backend_settings["api_base_url"]))
        logger.info(format_string.format("Luna HSM ID", self.backend_settings["hsm_id"]))
        logger.info(format_string.format("Admin Username", self.backend_settings["admin_username"]))
        logger.info(format_string.format("HSM Role", self.backend_settings["hsm_role"]))
        logger.info(
            format_string.format("Verify SSL", self.backend_settings.get("verify_ssl", False))
        )
        logger.info("")

        logger.info("Luna HSM components:\n%s\n", pprint.pformat(self.backend_components))

        try:
            # Test connectivity
            if not self.ping():
                logger.error("Luna HSM connectivity test failed")
                return False

            # Get and display current metrics
            metrics = self.client.get_metrics()
            logger.info("Current HSM metrics:")
            logger.info("Reset Time: %s", metrics.get("resetTime", "Unknown"))

            partition_count = len(metrics.get("metrics", []))
            logger.info("Number of partitions: %d", partition_count)

            for i, partition_data in enumerate(metrics.get("metrics", []), 1):
                partition_id = partition_data["partitionId"]
                label = partition_data.get("label", f"partition_{partition_id}")
                logger.info("  Partition %d: %s (ID: %s)", i, label, partition_id)

                # Show operation counts
                total_ops = 0
                for bin_data in partition_data.get("bins", []):
                    bin_id = bin_data["binId"]
                    for counter in bin_data.get("counters", []):
                        if counter["counterId"] == "REQUESTS":
                            count = counter["count"]
                            total_ops += count
                            logger.info("    %s: %d", bin_id, count)

                logger.info("    Total current operations: %d", total_ops)

                # Show monthly statistics
                stats_summary = self.storage.get_partition_stats_summary(str(partition_id))
                logger.info("    Monthly total: %d", stats_summary["current_month_total"])

            logger.info("Luna HSM diagnostics completed successfully")
            return True

        except Exception as e:
            logger.exception("Luna HSM diagnostics failed: %s", e)
            return False

    def list_components(self) -> list[str]:
        """Return list of components supported by Luna HSM backend."""
        return list(self.backend_components.keys())

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Collect usage report for HSM partitions."""
        usage_report = {}

        try:
            # Get current metrics from Luna HSM
            metrics = self.client.get_metrics()

            # Process each requested partition
            for backend_id in resource_backend_ids:
                logger.debug("Processing usage for partition %s", backend_id)

                # Find partition data in metrics
                partition_found = False
                for partition_data in metrics.get("metrics", []):
                    if str(partition_data["partitionId"]) == backend_id:
                        partition_found = True

                        # Calculate monthly total using storage
                        monthly_total = self.storage.get_partition_monthly_total(
                            backend_id, metrics
                        )

                        usage_report[backend_id] = {
                            "TOTAL_ACCOUNT_USAGE": {"operations": monthly_total}
                        }

                        logger.info(
                            "Usage for partition %s: %d operations this month",
                            backend_id,
                            monthly_total,
                        )
                        break

                if not partition_found:
                    logger.warning("Partition %s not found in HSM metrics", backend_id)
                    usage_report[backend_id] = {"TOTAL_ACCOUNT_USAGE": {"operations": 0}}

            # Clean up old statistics
            self.storage.cleanup_old_months()

        except Exception as e:
            logger.exception("Failed to get usage report: %s", e)
            # Return empty usage for all requested partitions
            for backend_id in resource_backend_ids:
                usage_report[backend_id] = {"TOTAL_ACCOUNT_USAGE": {"operations": 0}}

        return usage_report

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscaling not applicable for HSM partitions."""
        logger.debug("Downscaling not supported for HSM partition %s", resource_backend_id)
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pausing not applicable for HSM partitions."""
        logger.debug("Pausing not supported for HSM partition %s", resource_backend_id)
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restoring not applicable for HSM partitions."""
        logger.debug("Restoring not supported for HSM partition %s", resource_backend_id)
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get metadata for HSM partition."""
        try:
            account = self.client.get_account(resource_backend_id)
            if account:
                return {
                    "partition_id": resource_backend_id,
                    "label": account.description,
                    "backend_type": self.backend_type,
                }
        except Exception as e:
            logger.error("Failed to get metadata for partition %s: %s", resource_backend_id, e)

        return {"partition_id": resource_backend_id, "backend_type": self.backend_type}

    def _collect_resource_limits(
        self, _waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect limits - not applicable for HSM partitions."""
        # HSM partitions don't have configurable limits
        return {}, {}

    def _create_resource_in_backend(self, waldur_resource: WaldurResource) -> str:
        """Create resource in backend - HSM partitions are pre-existing."""
        # For HSM, the backend_id should be set in Waldur to match the partition ID
        # We don't create partitions, they must exist in the HSM
        backend_id = getattr(waldur_resource, "backend_id", None)

        if not backend_id:
            msg = (
                "HSM partitions must have backend_id set to the partition ID. "
                "Partitions cannot be created via the API."
            )
            raise BackendError(msg)

        # Verify partition exists
        account = self.client.get_account(backend_id)
        if not account:
            raise BackendError(f"HSM partition {backend_id} does not exist")

        logger.info("Using existing HSM partition %s", backend_id)
        return backend_id

    def _pre_create_resource(
        self, _waldur_resource: WaldurResource, _user_context: Optional[dict] = None
    ) -> None:
        """Skip project/customer setup for HSM."""
        # HSM doesn't need project/customer hierarchy
        logger.debug("Skipping project/customer setup for HSM partition")

    def post_create_resource(
        self,
        resource: Resource,
        _waldur_resource: WaldurResource,
        _user_context: Optional[dict] = None,
    ) -> None:
        """Post-creation actions for HSM partition."""
        logger.info("HSM partition %s is ready for use", resource.backend_id)

        # Initialize statistics tracking for this partition
        try:
            metrics = self.client.get_metrics()
            self.storage.get_partition_monthly_total(resource.backend_id, metrics)
            logger.debug("Initialized statistics tracking for partition %s", resource.backend_id)
        except Exception as e:
            logger.warning(
                "Failed to initialize statistics for partition %s: %s", resource.backend_id, e
            )

    def _raise_ping_error(self, message: str) -> None:
        """Raise ping error with given message."""
        raise BackendError(message)

    def __del__(self) -> None:
        """Clean up client session on destruction."""
        if hasattr(self, "client"):
            self.client.close_session()
