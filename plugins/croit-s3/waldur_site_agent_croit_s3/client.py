"""Croit S3 API client for managing S3 users and buckets."""

import logging
from typing import Optional

import requests
import urllib3
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Association, ClientResource

from .exceptions import (
    CroitS3APIError,
    CroitS3AuthenticationError,
    CroitS3UserExistsError,
    CroitS3UserNotFoundError,
)

logger = logging.getLogger(__name__)


class CroitS3Client(BaseClient):
    """Client for interacting with Croit S3 RadosGW API."""

    def __init__(
        self,
        api_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        """Initialize Croit S3 client.

        Args:
            api_url: Base URL of Croit API (e.g., https://192.168.240.34)
            username: API username (for Basic Auth)
            password: API password (for Basic Auth)
            token: Bearer token (alternative to username/password)
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        super().__init__()
        self.api_url = api_url.rstrip("/") + "/api"  # Add /api base path
        self.username = username
        self.password = password
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        # Disable SSL warnings if verification is disabled
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Configure session with retry strategy
        self.session = requests.Session()
        self.session.verify = verify_ssl

        # Set authentication
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        elif username and password:
            self.session.auth = HTTPBasicAuth(username, password)
        else:
            raise ValueError("Either token or username/password must be provided")

        # Add retry strategy for resilience
        retry_strategy = Retry(
            total=3,
            status_forcelist=[500, 502, 503, 504],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set default headers
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> requests.Response:
        """Make authenticated request to Croit API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            json_data: JSON payload for request body
            params: Query parameters

        Returns:
            Response object

        Raises:
            CroitS3AuthenticationError: If authentication fails
            CroitS3APIError: If API returns error
        """
        url = f"{self.api_url}{endpoint}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                json=json_data,
                params=params,
                timeout=self.timeout,
            )

            # Handle authentication errors
            if response.status_code == 401:
                raise CroitS3AuthenticationError(
                    f"Authentication failed for user {self.username}"
                )

            # Handle client/server errors
            if response.status_code >= 400:
                error_msg = f"API error {response.status_code}: {response.text[:500]}"
                if response.status_code == 404:
                    raise CroitS3UserNotFoundError(error_msg)
                elif response.status_code == 409:
                    raise CroitS3UserExistsError(error_msg)
                else:
                    raise CroitS3APIError(error_msg)

            return response

        except requests.exceptions.Timeout:
            raise CroitS3APIError(f"Request timeout after {self.timeout}s")
        except requests.exceptions.ConnectionError as e:
            raise CroitS3APIError(f"Connection error: {e}")
        except requests.exceptions.RequestException as e:
            raise CroitS3APIError(f"Request failed: {e}")

    def ping(self, raise_exception: bool = False) -> bool:
        """Test connection to Croit API.

        Args:
            raise_exception: Whether to raise exception on failure

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to list S3 users as a connectivity test
            response = self._request("GET", "/s3/users")
            response.raise_for_status()
            logger.info("Croit S3 API connection successful")
            return True
        except Exception as e:
            logger.error("Croit S3 API connection failed: %s", e)
            if raise_exception:
                raise BackendError(f"Croit S3 connection failed: {e}")
            return False

    def create_user(
        self, uid: str, name: str, email: Optional[str] = None, **kwargs
    ) -> dict:
        """Create new S3 user.

        Args:
            uid: Unique user identifier
            name: Display name for user
            email: User email address
            **kwargs: Additional user properties (tenant, defaultPlacement, etc.)

        Returns:
            User creation response

        Raises:
            CroitS3UserExistsError: If user already exists
            CroitS3APIError: If creation fails
        """
        user_data = {
            "uid": uid,
            "name": name,
        }

        if email:
            user_data["email"] = email

        # Add optional properties
        for key in ["tenant", "defaultPlacement", "defaultStorageClass"]:
            if key in kwargs and kwargs[key]:
                user_data[key] = kwargs[key]

        logger.info("Creating S3 user: %s", uid)
        response = self._request("POST", "/s3/users", json_data=user_data)

        # Croit API returns 201 or 204 for successful creation
        if response.status_code in (201, 204):
            logger.info("S3 user %s created successfully", uid)
            return response.json() if response.content else {}
        else:
            raise CroitS3APIError(f"Unexpected response code: {response.status_code}")

    def delete_user(self, uid: str) -> bool:
        """Delete S3 user.

        Args:
            uid: User identifier

        Returns:
            True if deletion successful

        Raises:
            CroitS3UserNotFoundError: If user doesn't exist
            CroitS3APIError: If deletion fails
        """
        logger.info("Deleting S3 user: %s", uid)
        response = self._request("DELETE", f"/s3/users/{uid}")

        # Croit API returns 204 for successful deletion
        if response.status_code == 204:
            logger.info("S3 user %s deleted successfully", uid)
            return True
        else:
            raise CroitS3APIError(f"Unexpected response code: {response.status_code}")

    def get_user_info(self, uid: str) -> dict:
        """Get S3 user information.

        Args:
            uid: User identifier

        Returns:
            User information dictionary

        Raises:
            CroitS3UserNotFoundError: If user doesn't exist
            CroitS3APIError: If request fails
        """
        response = self._request("GET", "/s3/users")
        response.raise_for_status()

        data = response.json()

        # Data is a simple array of users
        users = data if isinstance(data, list) else []

        # Filter for the specific user
        matching_users = [user for user in users if user.get("uid") == uid]

        if not matching_users:
            raise CroitS3UserNotFoundError(f"User {uid} not found")

        return matching_users[0]

    def get_user_keys(self, uid: str) -> dict:
        """Get S3 user access keys.

        Args:
            uid: User identifier

        Returns:
            Dictionary with access_key and secret_key

        Raises:
            CroitS3UserNotFoundError: If user doesn't exist
            CroitS3APIError: If request fails
        """
        response = self._request("GET", f"/s3/users/{uid}/key")
        response.raise_for_status()

        return response.json()

    def get_user_buckets(self, uid: str) -> list[dict]:
        """Get all buckets owned by user.

        Args:
            uid: User identifier

        Returns:
            List of bucket dictionaries with usage information

        Raises:
            CroitS3UserNotFoundError: If user doesn't exist
            CroitS3APIError: If request fails
        """
        response = self._request("GET", f"/s3/users/{uid}/buckets")
        response.raise_for_status()

        return response.json()

    def set_user_bucket_quota(self, uid: str, quota: dict) -> None:
        """Set bucket quota for all buckets owned by user.

        Args:
            uid: User identifier
            quota: Quota configuration dict with enabled, maxSize, maxObjects

        Raises:
            CroitS3UserNotFoundError: If user doesn't exist
            CroitS3APIError: If quota setting fails
        """
        logger.info("Setting bucket quota for user %s: %s", uid, quota)
        response = self._request(
            "PUT", f"/s3/users/{uid}/bucket-quota", json_data=quota
        )

        # Croit API returns 204 for successful quota update
        if response.status_code == 204:
            logger.info("Bucket quota set successfully for user %s", uid)
        else:
            raise CroitS3APIError(f"Unexpected response code: {response.status_code}")

    def get_user_quota(self, uid: str) -> dict:
        """Get current user quota settings.

        Args:
            uid: User identifier

        Returns:
            Dictionary with bucketQuota and userQuota information

        Raises:
            CroitS3UserNotFoundError: If user doesn't exist
            CroitS3APIError: If request fails
        """
        user_info = self.get_user_info(uid)
        return {
            "bucket_quota": user_info.get("bucketQuota", {}),
            "user_quota": user_info.get("userQuota", {}),
        }

    def list_users(self) -> list[dict]:
        """List all S3 users.

        Returns:
            List of user dictionaries

        Raises:
            CroitS3APIError: If request fails
        """
        # Get all users
        response = self._request("GET", "/s3/users")
        response.raise_for_status()

        data = response.json()

        # Data is a simple array of users
        return data if isinstance(data, list) else []

    def list_resources(self) -> list[ClientResource]:
        """Get resource list - maps to list_users for S3."""
        users = self.list_users()
        return [ClientResource(name=user.get("uid", "")) for user in users]

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get the resource's info - maps to get_user_info for S3."""
        try:
            user_info = self.get_user_info(resource_id)
            return ClientResource(name=user_info.get("uid", ""))
        except CroitS3UserNotFoundError:
            return None

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Create a resource in the cluster - maps to create_user for S3."""
        self.create_user(name, description)
        return name

    def delete_resource(self, name: str) -> str:
        """Delete a resource from the cluster - maps to delete_user for S3."""
        self.delete_user(name)
        return name

    def set_resource_limits(
        self, resource_id: str, limits_dict: dict[str, int]
    ) -> Optional[str]:
        """Set account limits - not applicable for S3."""
        return None

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get account limits - not applicable for S3."""
        return {}

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Get per-user limits for the account - not applicable for S3."""
        return {}

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set resource limits for a specific user - not applicable for S3."""
        return ""

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Get association between the user and the resource - not applicable for S3."""
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Create association between the user and the resource - not applicable for S3."""
        return ""

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association between the user and the resource - not applicable for S3."""
        return ""

    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get usage report for resources - not applicable for S3."""
        return []

    def list_resource_users(self, resource_id: str) -> list[str]:
        """List users associated with resource - not applicable for S3."""
        return []
