"""croit management API client — the croit flavour of the Ceph S3 backend.

Endpoint shapes verified against croit 2503 (OpenAPI 3.1).
"""

import logging
from typing import Any, Optional

import httpx

from waldur_site_agent.backend.exceptions import BackendError

from ..exceptions import (
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
    CroitS3GraphNotFoundError,
)
from .base import BaseS3AdminClient

logger = logging.getLogger(__name__)

# Per-S3-user storage over time, from croit's statistics subsystem. The name is
# croit's, defined in resources/statistics/graphite-queries.yml; the template
# parameter carries the uid. Verified against croit 2503 (OpenAPI 3.1).
STORAGE_GRAPH = "s3-user-data"
STORAGE_GRAPH_TEMPLATE = "template-s3-user-name"
# Native resolution is 180s and croit returns min(maxDataPoints, native), so this
# reaches native fidelity on windows up to ~4 days. croit consolidates by average
# -- measured: coarsening the buckets moves the integral monotonically *down* on a
# rising series, which rules out both max and last -- so the integral is preserved
# rather than approximated, and lowering this trades accuracy for nothing.
STORAGE_MAX_DATA_POINTS = 2000


class CroitClient(BaseS3AdminClient):
    """Client for interacting with croit's S3 (RadosGW) management API."""

    def __init__(
        self,
        api_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        """Initialize croit client.

        Args:
            api_url: Base URL of croit API (e.g. https://192.168.240.34)
            username: API username (for Basic Auth)
            password: API password (for Basic Auth)
            token: Bearer token (alternative to username/password)
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        super().__init__(timeout=timeout, verify_ssl=verify_ssl)
        self.api_url = api_url.rstrip("/") + "/api"  # Add /api base path
        self.username = username
        self.password = password
        self.token = token

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        auth = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            auth = httpx.BasicAuth(username, password)
        else:
            raise ValueError("Either token or username/password must be provided")

        # Transport-level retries cover connection errors;
        # 5xx responses are retried in _request
        self.session = httpx.Client(
            auth=auth,
            headers=headers,
            timeout=timeout,
            transport=httpx.HTTPTransport(verify=verify_ssl, retries=3),
        )

    def connection_summary(self) -> dict:
        """What to show an operator diagnosing this client."""
        return {"API URL": self.api_url, "Username": self.username}

    def _build_url(self, endpoint: str, params: Optional[dict] = None) -> str:
        """Resolve an endpoint against the /api base, letting httpx encode the query."""
        url = f"{self.api_url}{endpoint}"
        if params:
            return str(httpx.URL(url, params=params))
        return url

    def _auth_headers(self, method: str, url: str, body: Optional[bytes]) -> dict:
        """No per-request headers: the session carries the auth and content type."""
        del method, url, body
        return {}

    def _classify_error(self, response: httpx.Response) -> Exception:
        """Map croit's status codes onto the plugin's exceptions.

        croit is consistent enough to classify on the status alone — unlike
        RadosGW, which puts an absent key and a missing capability both on 403.
        """
        if response.status_code == 401:
            # Names the credential that was rejected rather than a user: under
            # bearer-token auth username is None, and the operator's order read
            # "Authentication failed for user None". Keyed off self.token because
            # that is what __init__ branches on, so the message tracks the
            # credential actually in use. croit's tokens are session tokens and
            # expire, which is the likeliest cause, so the message says where to
            # look. The token itself is never interpolated — this string reaches
            # the Waldur order.
            credential = "the API token" if self.token else f"user {self.username!r}"
            return CephS3AuthenticationError(
                f"croit rejected {credential}: check that it is valid and, for a "
                "token, that it has not expired"
            )

        error_msg = self._format_error(response)
        if response.status_code == 404:
            return CephS3UserNotFoundError(error_msg)
        if response.status_code == 409:
            return CephS3UserExistsError(error_msg)
        return CephS3APIError(error_msg)


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
            CephS3UserExistsError: If user already exists
            CephS3APIError: If creation fails
        """
        user_data = {
            "uid": uid,
            "name": name,
        }

        if email:
            user_data["email"] = email

        # croit spells these camelCase. The translation lives here rather than in
        # the backend so that the call the backend makes is identical for both
        # flavours — which is what stops a setting being dropped by whichever
        # client happens to be wired in.
        for neutral, croit_name in (
            ("tenant", "tenant"),
            ("default_placement", "defaultPlacement"),
            ("default_storage_class", "defaultStorageClass"),
        ):
            if kwargs.get(neutral):
                user_data[croit_name] = kwargs[neutral]

        uid = self._validate_uid(uid)
        logger.info("Creating S3 user: %s", uid)
        response = self._request("POST", "/s3/users", json_data=user_data)

        # Croit API returns 201 or 204 for successful creation
        if response.status_code in (201, 204):
            logger.info("S3 user %s created successfully", uid)
            return response.json() if response.content else {}
        else:
            raise CephS3APIError(f"Unexpected response code: {response.status_code}")

    def delete_user(self, uid: str) -> bool:
        """Delete S3 user.

        Args:
            uid: User identifier

        Returns:
            True if deletion successful

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If deletion fails
        """
        uid = self._validate_uid(uid)
        logger.info("Deleting S3 user: %s", uid)
        response = self._request("DELETE", f"/s3/users/{uid}")

        # Croit API returns 204 for successful deletion
        if response.status_code == 204:
            logger.info("S3 user %s deleted successfully", uid)
            return True
        else:
            raise CephS3APIError(f"Unexpected response code: {response.status_code}")

    def _get_raw_user(self, uid: str) -> dict:
        """croit's own user object, before normalisation.

        croit has no get-one-user endpoint, so this lists and filters; the 404
        every caller expects is synthesised here. Kept separate from
        get_user_info because the quota fields are croit-shaped and would be
        dropped by the normalisation.
        """
        response = self._request("GET", "/s3/users")
        response.raise_for_status()

        data = response.json()
        users = data if isinstance(data, list) else []
        matching = [user for user in users if user.get("uid") == uid]
        if not matching:
            raise CephS3UserNotFoundError(f"User {uid} not found")
        return matching[0]

    def get_user_info(self, uid: str) -> dict:
        """Get S3 user information.

        Args:
            uid: User identifier

        Returns:
            User information dictionary

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If request fails
        """
        user = self._get_raw_user(uid)
        # Normalised so the backend reads one spelling: croit says
        # defaultPlacement where RadosGW says default_placement.
        return {
            "uid": user.get("uid"),
            "name": user.get("name"),
            "email": user.get("email"),
            "suspended": user.get("suspended", False),
            "default_placement": user.get("defaultPlacement"),
            "default_storage_class": user.get("defaultStorageClass"),
            "keys": user.get("keys", []),
        }

    def list_user_keys(self, uid: str) -> list[dict]:
        """List every access key of an S3 user.

        Args:
            uid: User identifier

        Returns:
            List of {"user", "access_key", "secret_key"} dictionaries

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If request fails
        """
        uid = self._validate_uid(uid)
        response = self._request("GET", f"/s3/users/{uid}/keys")
        data = response.json()

        return data if isinstance(data, list) else []

    def create_user_key(self, uid: str, access_key: str, secret_key: str) -> None:
        """Add one access/secret pair to an S3 user.

        Croit takes the secret as a query parameter, so it reaches croit's access log.
        The alternative (PUT /s3/users/{uid}/keys with a JSON body) keeps the secret in
        the body but replaces the user's entire key set asynchronously — a
        read-modify-write whose race window drops a sibling key when two rotations
        overlap, which would break zero-downtime rotation.

        Args:
            uid: User identifier
            access_key: The public half of the pair
            secret_key: The secret half of the pair

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If creation fails
        """
        uid = self._validate_uid(uid)
        access_key = self._validate_access_key(access_key)
        logger.info("Adding S3 key %s to user %s", access_key, uid)
        response = self._request(
            "PUT",
            f"/s3/users/{uid}/keys/{access_key}",
            params={"secretKey": secret_key},
        )

        if response.status_code != 204:
            raise CephS3APIError(f"Unexpected response code: {response.status_code}")

    def delete_user_key(self, uid: str, access_key: str) -> None:
        """Remove one access key from an S3 user.

        A key that is already absent counts as success. A rotation whose reply to
        Waldur was lost has already deleted the old access key, and the
        reconciliation pass re-issues that rotation; erring here would turn a
        recoverable stall into a visible failure. The 404 cannot mean a missing
        *user* in that path — rotation creates the new key first, which would have
        failed on a missing user before reaching this call.

        Args:
            uid: User identifier
            access_key: The key to remove

        Raises:
            CephS3APIError: If deletion fails
        """
        uid = self._validate_uid(uid)
        access_key = self._validate_access_key(access_key)
        logger.info("Removing S3 key %s from user %s", access_key, uid)

        # croit answers 500 for a key it does not hold, not 404 — measured, and
        # the 404 branch below has therefore never fired for that case. 500 is
        # retryable, so going straight to the DELETE also costs three backoff
        # rounds before failing. One GET turns the common replay into a no-op.
        held = {key.get("access_key") for key in self.list_user_keys(uid)}
        if access_key not in held:
            logger.info("S3 key %s was already absent from user %s", access_key, uid)
            return

        try:
            response = self._request("DELETE", f"/s3/users/{uid}/keys/{access_key}")
        except CephS3UserNotFoundError:
            # _request maps every 404 to this; here it means the key is already gone.
            logger.info("S3 key %s was already absent from user %s", access_key, uid)
            return
        except CephS3APIError as e:
            # Lost the race against another delete; same outcome, so not an error.
            if "unable to find access key" not in str(e):
                raise
            logger.info("S3 key %s disappeared while deleting it", access_key)
            return

        if response.status_code != 204:
            raise CephS3APIError(f"Unexpected response code: {response.status_code}")

    def get_user_buckets(self, uid: str) -> list[dict]:
        """Get all buckets owned by user.

        Args:
            uid: User identifier

        Returns:
            List of {name, size_bytes, num_objects}, the shape both flavours
            answer in so the backend does not have to know which is talking.

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If request fails
        """
        uid = self._validate_uid(uid)
        response = self._request("GET", f"/s3/users/{uid}/buckets")
        response.raise_for_status()

        normalised = []
        for bucket in response.json():
            # croit answers usageSum: null for a bucket it has no statistics
            # for, and dict.get's default only covers a *missing* key.
            usage = bucket.get("usageSum") or {}
            normalised.append(
                {
                    "name": bucket.get("bucket"),
                    "size_bytes": usage.get("size", 0),
                    "num_objects": usage.get("numObjects", 0),
                }
            )
        return normalised

    @staticmethod
    def _to_croit_quota(quota: dict) -> dict:
        """Translate the neutral ceiling dict into croit's own body.

        Dimensions the order did not ask for are left out rather than sent as 0:
        croit reads 0 as "allow nothing", so emitting an unordered dimension
        would cap a tenant at zero bytes instead of leaving it unbounded.
        """
        body: dict[str, Any] = {"enabled": bool(quota.get("enabled", True))}
        if quota.get("max_size_bytes") is not None:
            body["maxSize"] = int(quota["max_size_bytes"])
        if quota.get("max_objects") is not None:
            body["maxObjects"] = int(quota["max_objects"])
        return body

    def set_user_bucket_quota(self, uid: str, quota: dict) -> None:
        """Set bucket quota for all buckets owned by user.

        Args:
            uid: User identifier
            quota: Neutral ceiling dict: enabled, max_size_bytes, max_objects

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If quota setting fails
        """
        uid = self._validate_uid(uid)
        logger.info("Setting bucket quota for user %s: %s", uid, quota)
        response = self._request(
            "PUT", f"/s3/users/{uid}/bucket-quota", json_data=self._to_croit_quota(quota)
        )

        # Croit API returns 204 for successful quota update
        if response.status_code == 204:
            logger.info("Bucket quota set successfully for user %s", uid)
        else:
            raise CephS3APIError(f"Unexpected response code: {response.status_code}")

    def set_user_quota(self, uid: str, quota: dict) -> None:
        """Set the aggregate quota across everything a user owns.

        The counterpart to set_user_bucket_quota, and the one that actually bounds
        a tenant: a bucket quota caps each bucket individually, so a user may hold
        N buckets of that size. croit exposes no way to cap the bucket count
        (RadosGW's max_buckets is absent from its API), so this is the only
        ceiling on a user's total.

        Args:
            uid: User identifier
            quota: Neutral ceiling dict: enabled, max_size_bytes, max_objects

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If quota setting fails
        """
        uid = self._validate_uid(uid)
        logger.info("Setting user quota for user %s: %s", uid, quota)
        response = self._request(
            "PUT", f"/s3/users/{uid}/quota", json_data=self._to_croit_quota(quota)
        )

        # Croit API returns 204 for successful quota update
        if response.status_code == 204:
            logger.info("User quota set successfully for user %s", uid)
        else:
            raise CephS3APIError(f"Unexpected response code: {response.status_code}")

    def get_user_storage_series(
        self, uid: str, start_time: int, end_time: int = 0
    ) -> list[dict]:
        """Read a user's storage-over-time series, for GB-day billing.

        Args:
            uid: User identifier
            start_time: Absolute unix seconds, or negative for "N seconds ago"
            end_time: Same form as start_time; 0 means now

        Returns:
            List of {"t": unix seconds, "v": bytes or None} datapoints, oldest
            first. Empty when croit holds no series for the user.

        Raises:
            CroitS3GraphNotFoundError: If the graph name no longer exists
            CephS3APIError: If the request fails
        """
        uid = self._validate_uid(uid)
        try:
            response = self._request(
                "GET",
                "/stats",
                params={
                    "graph": STORAGE_GRAPH,
                    STORAGE_GRAPH_TEMPLATE: uid,
                    "startTime": start_time,
                    "endTime": end_time,
                    "maxDataPoints": STORAGE_MAX_DATA_POINTS,
                },
            )
        except CephS3UserNotFoundError as e:
            # /stats answers 404 for an unknown graph, and _request cannot tell
            # that apart from an unknown user. Re-raise as its own type so the
            # reporting loop does not read a renamed graph as a gone resource.
            raise CroitS3GraphNotFoundError(
                f"Storage graph {STORAGE_GRAPH!r} not found on the croit API: {e}"
            )

        plots = response.json().get("axis1", {}).get("graphs", [])
        if not plots:
            logger.warning("No storage series for S3 user %s", uid)
            return []
        return plots[0].get("datapoints", [])

    def get_user_quota(self, uid: str) -> dict:
        """Get current user quota settings.

        Args:
            uid: User identifier

        Returns:
            {"user_quota": ..., "bucket_quota": ...}, each a neutral dict of
            {enabled, max_size_bytes, max_objects} — the same shape the radosgw
            flavour answers, so the backend reads one.

        Raises:
            CephS3UserNotFoundError: If user doesn't exist
            CephS3APIError: If request fails
        """
        # The raw user, not get_user_info: normalisation drops the quota fields,
        # and reading through it returned two empty dicts for every resource.
        user = self._get_raw_user(uid)
        return {
            "user_quota": self._from_croit_quota(user.get("userQuota") or {}),
            "bucket_quota": self._from_croit_quota(user.get("bucketQuota") or {}),
        }

    @staticmethod
    def _from_croit_quota(payload: dict) -> dict:
        """croit's camelCase quota into the neutral shape.

        maxSize is croit's read-back of a value it stored in KB, so it is the
        ordered ceiling rounded up to the next KiB, not the figure sent.
        """
        return {
            "enabled": bool(payload.get("enabled", False)),
            "max_size_bytes": payload.get("maxSize"),
            "max_objects": payload.get("maxObjects"),
        }

    def list_users(self) -> list:
        """List every S3 user id.

        Ids rather than user objects: the radosgw flavour can only answer ids,
        and BaseS3AdminClient.list_resources consumes this for both.

        Raises:
            CephS3APIError: If request fails
        """
        response = self._request("GET", "/s3/users")
        response.raise_for_status()

        data = response.json()
        users = data if isinstance(data, list) else []
        return [user.get("uid") for user in users if user.get("uid")]
