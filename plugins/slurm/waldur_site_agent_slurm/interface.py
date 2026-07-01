"""Shared interface for SLURM clients (CLI and REST).

``SlurmBackend`` only talks to its client through this interface, so the
CLI client (``SlurmClient``) and the REST client (``SlurmRestClient``)
are interchangeable. The interface extends ``BaseClient`` with the
SLURM-specific operations the backend relies on (QoS management,
account hierarchy, partitions, job control, periodic limits).
"""

from __future__ import annotations

import abc
from collections.abc import Sequence
from typing import Optional

from waldur_site_agent.backend import clients


class SlurmClientInterface(clients.BaseClient, abc.ABC):
    """Contract for SLURM client implementations.

    Implementations must also provide the attributes ``slurm_tres``,
    ``cluster_name``, ``slurm_bin_path`` and ``executed_commands`` (a
    human-readable log of executed commands/requests for diagnostics).
    """

    slurm_tres: dict
    cluster_name: Optional[str]
    executed_commands: list[str]

    def clear_executed_commands(self) -> None:
        """Clear the list of tracked executed commands."""
        self.executed_commands = []

    @abc.abstractmethod
    def get_version(self) -> str:
        """Return the SLURM version string (e.g. 'slurm 24.05.4')."""

    @abc.abstractmethod
    def validate_slurm_binary(self) -> bool:
        """Validate that the client talks to a real SLURM backend."""

    @abc.abstractmethod
    def list_tres(self) -> list[str]:
        """Return a list of TRES available in the cluster."""

    @abc.abstractmethod
    def list_clusters(self) -> list[str]:
        """Return a list of cluster names known to SLURM."""

    @abc.abstractmethod
    def get_account_parent(self, account: str) -> Optional[str]:
        """Return the parent account name for the account, or None if not found."""

    @abc.abstractmethod
    def set_account_parent(self, account: str, new_parent: str) -> str:
        """Reparent a SLURM account under new_parent."""

    @abc.abstractmethod
    def delete_all_users_from_account(self, name: str) -> str:
        """Drop all the users from the account."""

    @abc.abstractmethod
    def account_has_users(self, account: str) -> bool:
        """Check if the account has associated users."""

    @abc.abstractmethod
    def get_historical_usage_report(
        self, resource_ids: list[str], year: int, month: int
    ) -> list:
        """Return per-user usage records for the accounts for a specific month."""

    # ===== QOS MANAGEMENT =====

    @abc.abstractmethod
    def qos_exists(self, qos_name: str) -> bool:
        """Check if a QoS exists in the SLURM cluster."""

    @abc.abstractmethod
    def create_qos(
        self,
        name: str,
        flags: Optional[str] = None,
        grp_tres: Optional[str] = None,
        max_jobs: Optional[int] = None,
        max_submit: Optional[int] = None,
        max_wall: Optional[str] = None,
        min_tres_per_job: Optional[str] = None,
    ) -> None:
        """Create a QoS with the specified parameters."""

    @abc.abstractmethod
    def delete_qos(self, name: str) -> None:
        """Delete a QoS from the SLURM cluster."""

    @abc.abstractmethod
    def set_account_qos(self, account: str, qos: str) -> None:
        """Set the specified QoS for the account."""

    @abc.abstractmethod
    def get_current_account_qos(self, account: str) -> str:
        """Return a name of the current QoS of the account."""

    @abc.abstractmethod
    def set_account_qos_list(self, account: str, qos_list: list[str]) -> None:
        """Set the full QoS list for an account."""

    @abc.abstractmethod
    def add_account_qos(self, account: str, qos_name: str) -> None:
        """Add a QoS to an account's QoS list."""

    @abc.abstractmethod
    def set_account_default_qos(self, account: str, qos_name: str) -> None:
        """Set the default QoS for an account."""

    # ===== PARTITION-AWARE ASSOCIATIONS =====

    @abc.abstractmethod
    def create_association_with_partition(
        self,
        username: str,
        resource_id: str,
        partition: str,
        default_account: Optional[str] = "",
    ) -> str:
        """Create a user-account association with a specific partition."""

    @abc.abstractmethod
    def create_association_with_partitions(
        self,
        username: str,
        resource_id: str,
        partitions: Sequence[str],
        default_account: Optional[str] = "",
    ) -> str:
        """Create a user-account association restricted to the given partitions."""

    # ===== JOB CONTROL =====

    @abc.abstractmethod
    def cancel_active_user_jobs(self, account: str, user: Optional[str] = None) -> None:
        """Cancel jobs for the account (and user, when given)."""

    @abc.abstractmethod
    def list_active_user_jobs(self, account: str, user: str) -> list[str]:
        """List active job IDs for the account and user."""

    @abc.abstractmethod
    def check_user_exists(self, username: str) -> bool:
        """Check if the user exists in the local system."""

    # ===== PERIODIC LIMITS =====

    @abc.abstractmethod
    def set_account_fairshare(self, account: str, fairshare: int) -> bool:
        """Set fairshare for the account."""

    @abc.abstractmethod
    def get_account_fairshare(self, account: str) -> int:
        """Get the current fairshare value for the account."""

    @abc.abstractmethod
    def set_account_limits(self, account: str, limit_type: str, limits: dict) -> bool:
        """Set GrpTRESMins, MaxTRESMins, or GrpTRES limits for the account."""

    @abc.abstractmethod
    def get_account_limits(self, account: str) -> dict:
        """Get current account limits (GrpTRES, GrpTRESMins, MaxTRES, MaxTRESMins)."""

    @abc.abstractmethod
    def reset_raw_usage(self, account: str) -> bool:
        """Reset raw usage of the account for a clean period start."""
