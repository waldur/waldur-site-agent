"""Luna HSM API client for communication with Thales Luna HSM."""

import time
from typing import Optional

import requests

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Account, Association


class LunaHsmClient(BaseClient):
    """Client for communicating with Thales Luna HSM API."""

    def __init__(self, backend_settings: dict) -> None:
        """Initialize Luna HSM client with authentication settings."""
        self.base_url = backend_settings["api_base_url"]
        self.hsm_id = backend_settings["hsm_id"]
        self.admin_auth = (backend_settings["admin_username"], backend_settings["admin_password"])
        self.hsm_credentials = {
            "role": backend_settings["hsm_role"],
            "password": backend_settings["hsm_password"],
            "ped": backend_settings["hsm_ped"],
        }
        self.verify_ssl = backend_settings.get("verify_ssl", False)
        self.session_timeout = backend_settings.get("session_timeout", 3600)

        # Session management
        self.session: Optional[requests.Session] = None
        self.session_created_at: Optional[float] = None

        self.headers = {"Content-Type": "application/vnd.safenetinc.lunasa+json;version="}

    def _is_session_valid(self) -> bool:
        """Check if current session is still valid."""
        if not self.session or not self.session_created_at:
            return False

        elapsed = time.time() - self.session_created_at
        return elapsed < self.session_timeout

    def _authenticate(self) -> None:
        """Perform two-step authentication with cookie persistence."""
        logger.debug("Starting Luna HSM authentication")

        # Create new session with cookie jar
        session = requests.Session()

        try:
            # Step 1: Session authentication - cookies saved automatically
            logger.debug("Step 1: Session authentication")
            resp = session.post(
                f"{self.base_url}/auth/session",
                headers=self.headers,
                auth=self.admin_auth,
                json={},
                verify=self.verify_ssl,
                timeout=30,
            )
            resp.raise_for_status()

            # Step 2: HSM login - additional cookies saved to same session
            logger.debug("Step 2: HSM login")
            resp = session.post(
                f"{self.base_url}/api/lunasa/hsms/{self.hsm_id}/login",
                headers=self.headers,
                json=self.hsm_credentials,
                verify=self.verify_ssl,
                timeout=30,
            )
            resp.raise_for_status()

            # Store authenticated session
            self.session = session
            self.session_created_at = time.time()
            logger.debug("Luna HSM authentication successful")

        except requests.RequestException as e:
            logger.error("Luna HSM authentication failed: %s", e)
            self.session = None
            self.session_created_at = None
            msg = f"Failed to authenticate with Luna HSM: {e}"
            raise BackendError(msg) from e

    def _ensure_authenticated(self) -> None:
        """Ensure we have a valid authenticated session."""
        if not self._is_session_valid():
            logger.debug("Session invalid or expired, re-authenticating")
            self._authenticate()

    def get_metrics(self) -> dict:
        """Fetch metrics from Luna HSM using authenticated session."""
        self._ensure_authenticated()
        try:
            # Step 3: Use cookies from authenticated session
            resp = self.session.get(  # type: ignore[union-attr]
                f"{self.base_url}/api/lunasa/hsms/{self.hsm_id}/metrics",
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            logger.error("Failed to fetch Luna HSM metrics: %s", e)
            # Try re-authentication once on failure
            self.session = None
            self.session_created_at = None
            self._ensure_authenticated()

            resp = self.session.get(  # type: ignore[attr-defined]
                f"{self.base_url}/api/lunasa/hsms/{self.hsm_id}/metrics",
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()

    def close_session(self) -> None:
        """Clean up session resources."""
        if self.session:
            self.session.close()
            self.session = None
            self.session_created_at = None

    # BaseClient abstract methods - simplified for HSM context
    def list_accounts(self) -> list[Account]:
        """List HSM partitions as accounts."""
        try:
            metrics = self.get_metrics()
            accounts = []
            for partition_data in metrics.get("metrics", []):
                partition_id = str(partition_data["partitionId"])
                label = partition_data.get("label", f"partition_{partition_id}")
                accounts.append(
                    Account(name=partition_id, description=label, organization=partition_id)
                )
            return accounts
        except Exception as e:
            logger.error("Failed to list HSM partitions: %s", e)
            return []

    def get_account(self, name: str) -> Optional[Account]:
        """Get specific HSM partition info."""
        try:
            metrics = self.get_metrics()
            for partition_data in metrics.get("metrics", []):
                if str(partition_data["partitionId"]) == name:
                    label = partition_data.get("label", f"partition_{name}")
                    return Account(name=name, description=label, organization=name)
            return None
        except Exception as e:
            logger.error("Failed to get HSM partition %s: %s", name, e)
            return None

    def create_account(
        self, _name: str, _description: str, _organization: str, _parent_name: Optional[str] = None
    ) -> str:
        """Create account - not applicable for HSM partitions."""
        return ""

    def delete_account(self, _name: str) -> str:
        """Delete account - not applicable for HSM partitions."""
        return ""

    def set_resource_limits(self, _account: str, _limits_dict: dict[str, int]) -> Optional[str]:
        """Set limits - not applicable for HSM."""
        return ""

    def get_resource_limits(self, _account: str) -> dict[str, int]:
        """Get limits - not applicable for HSM."""
        return {}

    def get_resource_user_limits(self, _account: str) -> dict[str, dict[str, int]]:
        """Get user limits - not applicable for HSM."""
        return {}

    def set_resource_user_limits(
        self, _account: str, _username: str, _limits_dict: dict[str, int]
    ) -> str:
        """Set user limits - not applicable for HSM."""
        return ""

    def get_association(self, _user: str, _account: str) -> Optional[Association]:
        """Get association - not applicable for HSM."""
        return None

    def create_association(
        self, _username: str, _account: str, _default_account: Optional[str] = None
    ) -> str:
        """Create association - not applicable for HSM."""
        return ""

    def delete_association(self, _username: str, _account: str) -> str:
        """Delete association - not applicable for HSM."""
        return ""

    def get_usage_report(self, _accounts: list[str]) -> list:
        """Get usage report - not applicable for HSM."""
        return []

    def list_account_users(self, _account: str) -> list[str]:
        """List account users - not applicable for HSM."""
        return []
