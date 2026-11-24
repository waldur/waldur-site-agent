"""HTTP transport shared by the plugin's clients, and the RGW management contract.

``HttpTransport`` is what RGW Admin Ops and croit's statistics API have in common:
retries, error formatting and secret scrubbing. Each client supplies how a URL is
built, how a request is authenticated, and how an error maps onto an exception.

``BaseS3AdminClient`` adds the management contract -- users, keys, quotas,
buckets -- and the core's generic client surface expressed through it.
"""

import abc
import json
import logging
import re
import time
from collections.abc import Callable

import httpx
from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.structures import Association, ClientResource
from waldur_site_agent.common.sentry import scrub_secret_query_params

from ..exceptions import CephS3APIError, CephS3UserNotFoundError
from ..tenant import backend_id_of

logger = logging.getLogger(__name__)

# Retry transient server errors, mirroring the previous
# urllib3 Retry(total=3, status_forcelist=[500, 502, 503, 504], backoff_factor=1)
RETRYABLE_STATUS_CODES = frozenset({500, 502, 503, 504})
MAX_STATUS_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1.0

# Identifiers are interpolated into request paths, and httpx resolves dot-segments
# before sending — so an unchecked ".." turns "delete one key" into croit's
# delete-the-whole-user endpoint, and "a/../../../v2/status" reaches a different
# API namespace with the same cluster-admin token. Both values arrive from outside
# this process (a STOMP frame, and a backend id the core builds by concatenating
# slugs), so they are validated where the URL is built rather than at each caller.
#
# The pattern excludes every character that could end a path segment and, via the
# lookahead, the two segments URL resolution treats as navigation. It deliberately
# does not pin length or case: this plugin mints 20-character upper-case access
# keys, but it also has to manage keys RadosGW generated and keys adopted from an
# existing cluster, and refusing to address one of those would make a live
# credential unmanageable — a worse failure than the one being prevented.
_SAFE_PATH_SEGMENT_RE = re.compile(r"^(?!\.{1,2}$)[A-Za-z0-9._-]{1,255}$")

# The tenant half of a tenanted uid, ``tenant$uid``. RGW allows letters, digits
# and '_' in a tenant name; nothing that could end or navigate a path segment.
_TENANT_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")


class HttpTransport(abc.ABC):
    """Retries, error formatting and secret scrubbing, for any of the plugin's APIs."""

    def __init__(self, timeout: int = 30, verify_ssl: bool = True) -> None:
        """Store the transport-level knobs."""
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session: httpx.Client

    @abc.abstractmethod
    def _build_url(self, endpoint: str, params: dict | None = None) -> str:
        """Return the absolute URL for one operation, query string included."""

    @abc.abstractmethod
    def _auth_headers(self, method: str, url: str, body: bytes | None) -> dict:
        """Return the headers that authenticate one request."""

    @abc.abstractmethod
    def _classify_error(self, response: httpx.Response) -> Exception:
        """Map a >=400 response onto this API's exception type."""

    @staticmethod
    def _validate_uid(uid: str) -> str:
        """Accept a bare uid, or a tenanted one as RGW spells it: ``tenant$uid``.

        Each half is held to its own alphabet, and neither can contain "/" or be a
        dot-segment, so the tenanted form still cannot address another endpoint.
        """
        if isinstance(uid, str):
            tenant, _, bare = uid.rpartition("$")
            tenant_ok = _TENANT_RE.match(tenant) if "$" in uid else True
            if tenant_ok and _SAFE_PATH_SEGMENT_RE.match(bare):
                return uid
        raise CephS3APIError(f"Refusing to build a request for the user id {uid!r}")

    @staticmethod
    def _validate_access_key(access_key: str) -> str:
        """Reject an access key that would resolve to a different endpoint."""
        if not isinstance(access_key, str) or not _SAFE_PATH_SEGMENT_RE.match(access_key):
            raise CephS3APIError(
                f"Refusing to build a request for the access key {access_key!r}"
            )
        return access_key

    @staticmethod
    def _format_error(response: httpx.Response) -> str:
        """Extract the gateway's own error message, for the Waldur order to show.

        croit answers ``{"code": 500, "message": "..."}`` and RadosGW answers
        ``{"Code": "NoSuchUser"}``; either way the payload is the only part an
        operator can act on. Anything else keeps the status code, which is then
        the sole diagnostic.

        Scrubbing is not optional here. Both flavours can echo the request back:
        croit repeats the path in some errors, and RadosGW takes ``secret-key``
        as a *query parameter*, so an unscrubbed message would put a live S3
        secret into a Waldur-visible error and into Sentry.
        """
        try:
            payload = json.loads(response.text)
        except ValueError:
            payload = None

        if isinstance(payload, dict):
            for key in ("message", "error", "detail", "Code"):
                message = payload.get(key)
                if isinstance(message, str) and message.strip():
                    return scrub_secret_query_params(message.strip()[:500])

        return scrub_secret_query_params(
            f"API error {response.status_code}: {response.text[:500]}"
        )

    def _send_with_status_retries(
        self,
        method: str,
        url: str,
        headers: dict | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        """Send request, retrying transient 5xx responses with exponential backoff."""
        response = self.session.request(method=method, url=url, headers=headers, content=content)
        for attempt in range(MAX_STATUS_RETRIES):
            if response.status_code not in RETRYABLE_STATUS_CODES:
                break
            time.sleep(RETRY_BACKOFF_FACTOR * (2**attempt))
            response = self.session.request(
                method=method, url=url, headers=headers, content=content
            )
        return response

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: dict | None = None,
        params: dict | None = None,
    ) -> httpx.Response:
        """Make an authenticated request and raise on any error status.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path, relative to the flavour's base URL
            json_data: JSON payload for the request body
            params: Query parameters

        Returns:
            The response, guaranteed to carry a status below 400.

        Raises:
            CephS3AuthenticationError: If authentication fails
            CephS3UserNotFoundError: If the user or key does not exist
            CephS3UserExistsError: If the user or key already exists
            CephS3APIError: For anything else, including transport failures
        """
        body = json.dumps(json_data).encode() if json_data is not None else None
        url = self._build_url(endpoint, params)

        try:
            response = self._send_with_status_retries(
                method,
                url,
                headers=self._auth_headers(method, url, body),
                content=body,
            )

            if response.status_code >= 400:
                raise self._classify_error(response)

            return response

        except httpx.TimeoutException:
            raise CephS3APIError(f"Request timeout after {self.timeout}s")
        except httpx.TransportError as e:
            raise CephS3APIError(f"Connection error: {e}")
        except httpx.HTTPError as e:
            raise CephS3APIError(f"Request failed: {e}")


class BaseS3AdminClient(HttpTransport, BaseClient):
    """The management contract, plus identifier validation."""

    def __init__(self, timeout: int = 30, verify_ssl: bool = True) -> None:
        """Store the transport knobs and the resource-id resolver."""
        HttpTransport.__init__(self, timeout=timeout, verify_ssl=verify_ssl)
        BaseClient.__init__(self)
        # Resource id -> the uid of the user that holds it. A resource is its
        # tenant, so the core's resource-level calls below cannot pass their id
        # through as a uid; the backend installs its resolver here.
        self.resolve_resource_id: Callable[[str], str] = lambda resource_id: resource_id

    # -- the management contract -------------------------------------------
    #
    # Declared here rather than left to duck typing so that a client missing
    # one of them fails at import, not at the first order that needs it.

    @abc.abstractmethod
    def ping(self, raise_exception: bool = False) -> bool:
        """Whether the management API answers."""

    @abc.abstractmethod
    def connection_summary(self) -> dict:
        """Label/value pairs identifying what this client talks to."""

    @abc.abstractmethod
    def create_user(
        self, uid: str, name: str, email: str | None = None, **kwargs: object
    ) -> dict:
        """Create an S3 user holding only the key pair Waldur issued."""

    @abc.abstractmethod
    def delete_user(self, uid: str) -> bool:
        """Remove an S3 user; an already-absent one is success."""

    @abc.abstractmethod
    def get_user_info(self, uid: str) -> dict:
        """One user, as {uid, name, email, suspended, default_*, keys}."""

    @abc.abstractmethod
    def list_users(self) -> list:
        """Every S3 user id known to the backend."""

    @abc.abstractmethod
    def list_user_keys(self, uid: str) -> list:
        """The user's keys, as [{user, access_key, secret_key}]."""

    @abc.abstractmethod
    def create_user_key(self, uid: str, access_key: str, secret_key: str) -> None:
        """Add a key pair to the user."""

    @abc.abstractmethod
    def delete_user_key(self, uid: str, access_key: str) -> None:
        """Drop one key; an already-absent one is a no-op."""

    @abc.abstractmethod
    def set_user_suspended(self, uid: str, suspended: bool) -> None:
        """Suspend the user (every request with its keys is refused) or lift it."""

    @abc.abstractmethod
    def get_user_quota(self, uid: str) -> dict:
        """Both ceilings, as {user_quota, bucket_quota} of neutral dicts."""

    @abc.abstractmethod
    def set_user_quota(self, uid: str, quota: dict) -> None:
        """Cap everything the user holds in total."""

    @abc.abstractmethod
    def set_user_bucket_quota(self, uid: str, quota: dict) -> None:
        """Cap each individual bucket the user owns."""

    @abc.abstractmethod
    def get_user_buckets(self, uid: str) -> list:
        """The user's buckets, as [{name, size_bytes, num_objects}]."""

    # -- the core BaseClient surface ---------------------------------------
    #
    # Waldur's generic client interface, expressed in terms of the S3 methods
    # above. Identical for every flavour, so it lives here rather than in each.

    def list_resources(self) -> list[ClientResource]:
        """Get resource list - maps to list_users for S3."""
        # list_users answers ids on both flavours. This used to call .get() on
        # each entry, which worked only because croit returned user objects —
        # against radosgw's list of strings it would have raised AttributeError.
        # Named by backend id, which is what the core matches against Waldur's
        # resources: a tenant's root user is listed as the tenant.
        return [ClientResource(name=backend_id_of(uid)) for uid in self.list_users()]

    def get_resource(self, resource_id: str) -> ClientResource | None:
        """Get the resource's info - maps to get_user_info for S3."""
        try:
            user_info = self.get_user_info(self.resolve_resource_id(resource_id))
            return ClientResource(name=backend_id_of(str(user_info.get("uid") or "")))
        except CephS3UserNotFoundError:
            return None

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: str | None = None,
    ) -> str:
        """Create a resource in the cluster - maps to create_user for S3."""
        self.create_user(name, description)
        return name

    def delete_resource(self, name: str) -> str:
        """Delete a resource from the cluster - maps to delete_user for S3."""
        self.delete_user(self.resolve_resource_id(name))
        return name

    def set_resource_limits(
        self, resource_id: str, limits_dict: dict[str, int]
    ) -> str | None:
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

    def get_association(self, user: str, resource_id: str) -> Association | None:
        """Get association between the user and the resource - not applicable for S3."""
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: str | None = None
    ) -> str:
        """Create association between the user and the resource - not applicable for S3."""
        return ""

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete association between the user and the resource - not applicable for S3."""
        return ""

    def get_usage_report(self, resource_ids: list[str], timezone: str | None = None) -> list:
        """Get usage report for resources - not applicable for S3."""
        return []

    def list_resource_users(self, resource_id: str) -> list[str]:
        """List users associated with resource - not applicable for S3."""
        return []
