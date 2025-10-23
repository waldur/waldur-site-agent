"""CSCS-DWDI API client implementation."""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

HTTP_OK = 200


class CSCSDWDIClient:
    """Client for interacting with CSCS-DWDI API."""

    def __init__(
        self,
        api_url: str,
        client_id: str,
        client_secret: str,
        oidc_token_url: Optional[str] = None,
        oidc_scope: Optional[str] = None,
        socks_proxy: Optional[str] = None,
    ) -> None:
        """Initialize CSCS-DWDI client.

        Args:
            api_url: Base URL for the CSCS-DWDI API
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
                "OIDC authentication failed: cscs_dwdi_oidc_token_url not configured. "
                "Set 'cscs_dwdi_oidc_token_url' in backend_settings for production use."
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

    def get_usage_for_month(
        self,
        accounts: list[str],
        from_date: date,
        to_date: date,
        clusters: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Get usage data for multiple accounts for a month range.

        Args:
            accounts: List of account identifiers to query
            from_date: Start date (beginning of month)
            to_date: End date (end of month)
            clusters: Optional list of cluster names to filter by

        Returns:
            API response with usage data grouped by account

        Raises:
            httpx.HTTPError: If API request fails
        """
        token = self._get_auth_token()

        # Format dates as YYYY-MM for month endpoints
        from_month = from_date.strftime("%Y-%m")
        to_month = to_date.strftime("%Y-%m")

        params: dict[str, Any] = {
            "from": from_month,
            "to": to_month,
        }

        # Add account filters if provided
        if accounts:
            params["account"] = accounts

        # Add cluster filters if provided
        if clusters:
            params["cluster"] = clusters

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.api_url}/compute/usage-month/account"

        logger.debug(
            "Fetching usage for accounts %s from %s to %s",
            accounts,
            from_month,
            to_month,
        )

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for API request: %s", self.socks_proxy)

        with httpx.Client(**client_args) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    def get_usage_for_days(
        self,
        accounts: list[str],
        from_date: date,
        to_date: date,
        clusters: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Get usage data for multiple accounts for a day range.

        Args:
            accounts: List of account identifiers to query
            from_date: Start date
            to_date: End date
            clusters: Optional list of cluster names to filter by

        Returns:
            API response with usage data grouped by account

        Raises:
            httpx.HTTPError: If API request fails
        """
        token = self._get_auth_token()

        # Format dates as YYYY-MM-DD for day endpoints
        from_day = from_date.strftime("%Y-%m-%d")
        to_day = to_date.strftime("%Y-%m-%d")

        params: dict[str, Any] = {
            "from": from_day,
            "to": to_day,
        }

        # Add account filters if provided
        if accounts:
            params["account"] = accounts

        # Add cluster filters if provided
        if clusters:
            params["cluster"] = clusters

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.api_url}/compute/usage-day/account"

        logger.debug(
            "Fetching daily usage for accounts %s from %s to %s",
            accounts,
            from_day,
            to_day,
        )

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for API request: %s", self.socks_proxy)

        with httpx.Client(**client_args) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    def get_storage_usage_for_month(
        self,
        paths: list[str],
        tenant: Optional[str],
        filesystem: str,
        data_type: str,
        exact_month: str,
    ) -> dict[str, Any]:
        """Get storage usage data for specified paths for a month.

        Args:
            paths: List of storage paths to query
            tenant: Tenant identifier (optional)
            filesystem: Filesystem name (e.g., "lustre")
            data_type: Data type (e.g., "projects")
            exact_month: Month in YYYY-MM format

        Returns:
            API response with storage usage data

        Raises:
            httpx.HTTPError: If API request fails
        """
        token = self._get_auth_token()

        params: dict[str, Any] = {
            "exact-month": exact_month,
        }

        # Add optional parameters
        if paths:
            params["paths"] = paths
        if tenant:
            params["tenant"] = tenant
        if filesystem:
            params["filesystem"] = filesystem
        if data_type:
            params["data_type"] = data_type

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.api_url}/storage/usage-month/{filesystem}/{data_type}"

        logger.debug(
            "Fetching storage usage for paths %s for month %s",
            paths,
            exact_month,
        )

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for API request: %s", self.socks_proxy)

        with httpx.Client(**client_args) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    def get_storage_usage_for_day(
        self,
        paths: list[str],
        tenant: Optional[str],
        filesystem: str,
        data_type: str,
        exact_date: date,
    ) -> dict[str, Any]:
        """Get storage usage data for specified paths for a specific day.

        Args:
            paths: List of storage paths to query
            tenant: Tenant identifier (optional)
            filesystem: Filesystem name (e.g., "lustre")
            data_type: Data type (e.g., "projects")
            exact_date: Specific date

        Returns:
            API response with storage usage data

        Raises:
            httpx.HTTPError: If API request fails
        """
        token = self._get_auth_token()

        params: dict[str, Any] = {
            "exact-date": exact_date.strftime("%Y-%m-%d"),
        }

        # Add optional parameters
        if paths:
            params["paths"] = paths
        if tenant:
            params["tenant"] = tenant
        if filesystem:
            params["filesystem"] = filesystem
        if data_type:
            params["data_type"] = data_type

        headers = {"Authorization": f"Bearer {token}"}

        url = f"{self.api_url}/storage/usage-day/{filesystem}/{data_type}"

        logger.debug(
            "Fetching storage usage for paths %s for date %s",
            paths,
            exact_date,
        )

        # Configure httpx client with SOCKS proxy if specified
        client_args: dict[str, Any] = {"timeout": 30.0}
        if self.socks_proxy:
            client_args["proxy"] = self.socks_proxy
            logger.debug("Using SOCKS proxy for API request: %s", self.socks_proxy)

        with httpx.Client(**client_args) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    def ping(self) -> bool:
        """Check if CSCS-DWDI compute API is accessible.

        Returns:
            True if API is accessible, False otherwise
        """
        try:
            token = self._get_auth_token()
            headers = {"Authorization": f"Bearer {token}"}

            # Use a simple query to test connectivity
            today = datetime.now(tz=timezone.utc).date()
            params = {
                "from": today.strftime("%Y-%m-%d"),
                "to": today.strftime("%Y-%m-%d"),
            }

            url = f"{self.api_url}/compute/usage-day"

            # Configure httpx client with SOCKS proxy if specified
            client_args: dict[str, Any] = {"timeout": 10.0}
            if self.socks_proxy:
                client_args["proxy"] = self.socks_proxy
                logger.debug("Using SOCKS proxy for ping: %s", self.socks_proxy)

            with httpx.Client(**client_args) as client:
                response = client.get(url, params=params, headers=headers)
                return response.status_code == HTTP_OK
        except Exception:
            logger.exception("Compute ping failed")
            return False

    def ping_storage(self) -> bool:
        """Check if CSCS-DWDI storage API is accessible.

        Returns:
            True if API is accessible, False otherwise
        """
        try:
            token = self._get_auth_token()
            headers = {"Authorization": f"Bearer {token}"}

            # Use a simple query to test connectivity
            today = datetime.now(tz=timezone.utc).date()
            params = {
                "exact-date": today.strftime("%Y-%m-%d"),
            }

            # Use default values for ping test
            filesystem = "lustre"
            data_type = "projects"
            url = f"{self.api_url}/storage/usage-day/{filesystem}/{data_type}"

            # Configure httpx client with SOCKS proxy if specified
            client_args: dict[str, Any] = {"timeout": 10.0}
            if self.socks_proxy:
                client_args["proxy"] = self.socks_proxy
                logger.debug("Using SOCKS proxy for storage ping: %s", self.socks_proxy)

            with httpx.Client(**client_args) as client:
                response = client.get(url, params=params, headers=headers)
                return response.status_code == HTTP_OK
        except Exception:
            logger.exception("Storage ping failed")
            return False
