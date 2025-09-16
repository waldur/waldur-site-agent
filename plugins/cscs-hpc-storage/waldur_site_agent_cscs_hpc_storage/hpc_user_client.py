"""CSCS HPC User API client implementation."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

HTTP_OK = 200


class CSCSHpcUserClient:
    """Client for interacting with CSCS HPC User API for project information."""

    def __init__(
        self,
        api_url: str,
        client_id: str,
        client_secret: str,
        oidc_token_url: Optional[str] = None,
        oidc_scope: Optional[str] = None,
        socks_proxy: Optional[str] = None,
    ) -> None:
        """Initialize CSCS HPC User client.

        Args:
            api_url: Base URL for the CSCS HPC User API
            client_id: OIDC client ID for authentication
            client_secret: OIDC client secret for authentication
            oidc_token_url: OIDC token endpoint URL (required for authentication)
            oidc_scope: OIDC scope to request (optional)
            socks_proxy: SOCKS proxy URL (e.g., "socks5://localhost:12345")
        """
        self.api_url = api_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.oidc_token_url = oidc_token_url
        self.oidc_scope = oidc_scope or "openid"
        self.socks_proxy = socks_proxy
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

    def _get_auth_token(self) -> str:
        """Get or refresh OIDC authentication token.

        Returns:
            Valid authentication token

        Raises:
            httpx.HTTPError: If token acquisition fails
        """
        # Check if we have a valid cached token
        if (
            self._token
            and self._token_expires_at
            and datetime.now(tz=timezone.utc) < self._token_expires_at
        ):
            return self._token

        # Fail if OIDC endpoint not configured
        if not self.oidc_token_url:
            error_msg = (
                "OIDC authentication failed: hpc_user_oidc_token_url not configured. "
                "Set 'hpc_user_oidc_token_url' in backend_settings for production use."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Request new token from OIDC provider
        return self._acquire_oidc_token()

    def _acquire_oidc_token(self) -> str:
        """Acquire a new OIDC token from the configured provider.

        Returns:
            Valid authentication token

        Raises:
            httpx.HTTPError: If token acquisition fails
        """
        logger.debug("Acquiring new OIDC token from %s", self.oidc_token_url)

        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        # Add scope if specified
        if self.oidc_scope:
            token_data["scope"] = self.oidc_scope

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for token acquisition: %s", self.socks_proxy)

        with httpx.Client(**client_args) as client:
            response = client.post(self.oidc_token_url, data=token_data, headers=headers)
            response.raise_for_status()
            token_response = response.json()

            # Extract token and expiry information
            access_token = token_response.get("access_token")
            if not access_token:
                msg = f"No access_token in OIDC response: {token_response}"
                raise ValueError(msg)

            # Calculate token expiry time
            expires_in = token_response.get("expires_in", 3600)  # Default to 1 hour
            # Subtract 5 minutes from expiry for safety margin
            safe_expires_in = max(300, expires_in - 300)

            self._token = access_token
            self._token_expires_at = datetime.now(tz=timezone.utc) + timedelta(
                seconds=safe_expires_in
            )

            logger.info("Successfully acquired OIDC token, expires in %d seconds", expires_in)

            return self._token

    def get_projects(self, project_slugs: list[str]) -> list[dict[str, Any]]:
        """Get project information for multiple project slugs.

        Args:
            project_slugs: List of project slugs to query

        Returns:
            List of project data dictionaries

        Raises:
            httpx.HTTPError: If API request fails
        """
        token = self._get_auth_token()

        params: dict[str, Any] = {}

        # Add project filters if provided
        if project_slugs:
            params["projects"] = project_slugs

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.api_url}/api/v1/export/waldur/projects"

        logger.debug("Fetching project information for slugs: %s", project_slugs)

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for API request: %s", self.socks_proxy)

        with httpx.Client(**client_args) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()["projects"]

    def get_project_unix_gid(self, project_slug: str) -> Optional[int]:
        """Get unixGid for a specific project slug.

        Args:
            project_slug: Project slug to look up

        Returns:
            unixGid if found, None otherwise
        """
        try:
            projects_data = self.get_projects([project_slug])
            if len(projects_data) > 1:
                logger.error("Multiple projects found for slug: %s", project_slug)
                return None
            if len(projects_data) == 0:
                logger.warning("Project %s not found in HPC User API response", project_slug)
                return None
            project = projects_data[0]
            if project.get("posixName") == project_slug:
                return project.get("unixGid")

            logger.warning("Project %s not found in HPC User API response", project_slug)
            return None
        except Exception:
            logger.exception("Failed to fetch unixGid for project %s", project_slug)
            return None

    def ping(self) -> bool:
        """Check if CSCS HPC User API is accessible.

        Returns:
            True if API is accessible, False otherwise
        """
        try:
            token = self._get_auth_token()
            headers = {"Authorization": f"Bearer {token}"}

            url = f"{self.api_url}/api/v1/export/waldur/projects"

            # Configure httpx client with SOCKS proxy if specified
            client_args: dict[str, Any] = {"timeout": 10.0}
            if self.socks_proxy:
                client_args["proxy"] = self.socks_proxy
                logger.debug("Using SOCKS proxy for ping: %s", self.socks_proxy)

            # Test with a simple request (no project filters)
            with httpx.Client(**client_args) as client:
                response = client.get(url, headers=headers)
                return response.status_code == HTTP_OK
        except Exception:
            logger.exception("HPC User API ping failed")
            return False
