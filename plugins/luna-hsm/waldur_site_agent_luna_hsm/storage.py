"""Storage module for managing Luna HSM monthly statistics persistence."""

import json
from datetime import datetime, timezone
from pathlib import Path

from waldur_site_agent.backend import logger


class MonthlyStatsStorage:
    """Manages monthly statistics storage for Luna HSM crypto operations."""

    def __init__(
        self,
        storage_path: str = "/var/lib/waldur-site-agent/luna-hsm-stats.json",
    ) -> None:
        """Initialize storage with configurable path."""
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_current_month_key(self) -> str:
        """Get current month key in YYYY-MM format."""
        return datetime.now(timezone.utc).strftime("%Y-%m")

    def _parse_reset_time(self, reset_time: str) -> datetime:
        """Parse resetTime from Luna HSM API response."""
        return datetime.fromisoformat(reset_time.replace("Z", "+00:00"))

    def load_monthly_stats(self) -> dict:
        """Load monthly statistics from storage file."""
        if not self.storage_path.exists():
            logger.debug("Statistics file does not exist, returning empty stats")
            return {}

        try:
            with self.storage_path.open() as f:
                data = json.load(f)
                logger.debug("Loaded statistics from %s", self.storage_path)
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to load statistics file: %s", e)
            return {}

    def save_monthly_stats(self, stats: dict) -> None:
        """Save monthly statistics to storage file."""
        try:
            with self.storage_path.open("w") as f:
                json.dump(stats, f, indent=2, default=str)
                logger.debug("Saved statistics to %s", self.storage_path)
        except OSError as e:
            logger.error("Failed to save statistics file: %s", e)

    def get_partition_monthly_total(self, partition_id: str, current_metrics: dict) -> int:
        """Calculate total operations for a partition from the beginning of current month.

        Args:
            partition_id: HSM partition ID
            current_metrics: Current metrics data from Luna HSM API

        Returns:
            Total operations count since beginning of month
        """
        current_month = self._get_current_month_key()
        stored_stats = self.load_monthly_stats()

        # Get current totals from API
        current_totals = self._calculate_current_totals(current_metrics)
        current_reset_time = current_metrics.get("resetTime", "")

        # Initialize partition stats if not exists
        if partition_id not in stored_stats:
            stored_stats[partition_id] = {}

        partition_stats = stored_stats[partition_id]

        # Initialize current month if not exists
        if current_month not in partition_stats:
            partition_stats[current_month] = {
                "total_operations": 0,
                "last_reset_time": "",
                "last_api_total": 0,
                "accumulated_before_resets": 0,
            }

        month_stats = partition_stats[current_month]

        # Check if counters were reset since last check
        if self._counters_were_reset(month_stats["last_reset_time"], current_reset_time):
            logger.info("Detected counter reset for partition %s", partition_id)
            # Add previous API total to accumulated amount
            month_stats["accumulated_before_resets"] += month_stats["last_api_total"]

        # Update stats
        month_stats["last_reset_time"] = current_reset_time
        month_stats["last_api_total"] = current_totals
        month_stats["total_operations"] = month_stats["accumulated_before_resets"] + current_totals

        # Save updated stats
        self.save_monthly_stats(stored_stats)

        return month_stats["total_operations"]

    def _calculate_current_totals(self, metrics_data: dict) -> int:
        """Calculate total operations from current API metrics."""
        total_operations = 0

        for partition_metrics in metrics_data.get("metrics", []):
            for bin_data in partition_metrics.get("bins", []):
                for counter in bin_data.get("counters", []):
                    if counter.get("counterId") == "REQUESTS":
                        total_operations += counter.get("count", 0)

        return total_operations

    def _counters_were_reset(self, last_reset_time: str, current_reset_time: str) -> bool:
        """Check if counters were reset since last check."""
        if not last_reset_time or not current_reset_time:
            return False

        try:
            last_time = self._parse_reset_time(last_reset_time)
            current_time = self._parse_reset_time(current_reset_time)
            return current_time > last_time
        except ValueError as e:
            logger.warning("Failed to parse reset times: %s", e)
            return False

    def cleanup_old_months(self, keep_months: int = 12) -> None:
        """Remove statistics older than specified months."""
        stored_stats = self.load_monthly_stats()
        current_date = datetime.now(timezone.utc)

        months_to_remove = []

        for partition_id, partition_stats in stored_stats.items():
            partition_months_to_remove = []

            for month_key in partition_stats:
                try:
                    month_date = datetime.strptime(month_key, "%Y-%m").replace(tzinfo=timezone.utc)
                    months_diff = (
                        (current_date.year - month_date.year) * 12
                        + current_date.month
                        - month_date.month
                    )

                    if months_diff > keep_months:
                        partition_months_to_remove.append(month_key)
                except ValueError:
                    # Invalid month key format
                    partition_months_to_remove.append(month_key)

            for month_key in partition_months_to_remove:
                del partition_stats[month_key]
                logger.debug("Removed old statistics for %s/%s", partition_id, month_key)

            if not partition_stats:
                months_to_remove.append(partition_id)

        for partition_id in months_to_remove:
            del stored_stats[partition_id]

        if months_to_remove:
            self.save_monthly_stats(stored_stats)

    def get_partition_stats_summary(self, partition_id: str) -> dict:
        """Get statistics summary for a partition."""
        stored_stats = self.load_monthly_stats()
        partition_stats = stored_stats.get(partition_id, {})

        current_month = self._get_current_month_key()
        current_month_stats = partition_stats.get(current_month, {})

        return {
            "partition_id": partition_id,
            "current_month": current_month,
            "current_month_total": current_month_stats.get("total_operations", 0),
            "last_reset_time": current_month_stats.get("last_reset_time", ""),
            "available_months": list(partition_stats.keys()),
            "total_months_tracked": len(partition_stats),
        }
