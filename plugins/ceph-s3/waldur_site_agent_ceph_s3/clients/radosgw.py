"""RadosGW Admin Ops client — the vanilla Ceph flavour.

Behaviour verified against Ceph 19.2.0 (squid) rather than taken from
https://docs.ceph.com/en/latest/radosgw/adminops/, which is wrong about the
duplicate-user error code, claims a JSON request body works for quotas (it
answers 501), and documents no way to list users when ``?list`` does exactly
that. ``_ERROR_CODES`` below carries the measured response codes.
"""

import logging
from typing import Any, Optional
from urllib.parse import quote

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from waldur_site_agent.backend.exceptions import BackendError

from ..exceptions import (
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)
from .base import BaseS3AdminClient

logger = logging.getLogger(__name__)

# RGW reports an absent access key as 403/InvalidAccessKeyId and a missing
# capability as 403/AccessDenied, so the status cannot tell "already deleted"
# apart from "not allowed". Everything is classified on the JSON Code instead.
#
# InvalidAccessKeyId maps to not-found on purpose: to every caller here, a key
# the gateway does not know is a key that is already gone.
_ERROR_CODES = {
    "NoSuchUser": CephS3UserNotFoundError,
    "InvalidAccessKeyId": CephS3UserNotFoundError,
    "UserAlreadyExists": CephS3UserExistsError,
    "KeyExists": CephS3UserExistsError,
    # Returned when deleting a user that still owns buckets. The code is
    # misleading -- nothing is being created -- but it is what the wire carries.
    "BucketAlreadyExists": CephS3UserExistsError,
    "AccessDenied": CephS3AuthenticationError,
    "SignatureDoesNotMatch": CephS3AuthenticationError,
}


class RadosGWClient(BaseS3AdminClient):
    """Admin Ops client for a stock Ceph object gateway."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        admin_path: str = "admin",
        region: str = "us-east-1",
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        """Initialize the Admin Ops client.

        Args:
            endpoint: The S3 gateway address; Admin Ops hangs off it directly
            access_key: Access key of a user holding the required caps
            secret_key: Its secret
            admin_path: Value of rgw_admin_entry, if the operator renamed it
            region: Signing region. RGW does not validate it against the
                zonegroup, so the AWS default is fine for a zonegroup named
                "default".
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        super().__init__(timeout=timeout, verify_ssl=verify_ssl)
        self.base_url = endpoint.rstrip("/") + "/" + admin_path.strip("/")
        self._credentials = Credentials(access_key, secret_key)
        self._region = region
        self.session = httpx.Client(
            timeout=timeout,
            transport=httpx.HTTPTransport(verify=verify_ssl, retries=3),
        )

    def connection_summary(self) -> dict:
        """What to show an operator diagnosing this client.

        Never the admin secret: this is logged.
        """
        return {"Admin Ops URL": self.base_url, "Signing region": self._region}

    def _build_url(self, endpoint: str, params: Optional[dict] = None) -> str:
        """Compose the signed URL.

        Values are percent-encoded rather than form-encoded: a "+" standing in
        for a space is re-read as "+" by the signer and as " " by RGW, and the
        signature never matches. Valueless subresources go out as "key=" so that
        the canonical query string both ends compute agree.
        """
        query = "&".join(
            "{}={}".format(key, quote("" if value is None else str(value), safe=""))
            for key, value in (params or {}).items()
        )
        return f"{self.base_url}{endpoint}" + (f"?{query}" if query else "")

    def _auth_headers(self, method: str, url: str, body: Optional[bytes]) -> dict:
        """Sign the request with SigV4.

        S3SigV4Auth rather than the generic SigV4Auth: RGW requires
        x-amz-content-sha256 and answers SignatureDoesNotMatch without it.
        """
        request = AWSRequest(method=method, url=url, data=body)
        S3SigV4Auth(self._credentials, "s3", self._region).add_auth(request)
        return dict(request.headers)

    def _classify_error(self, response: httpx.Response) -> Exception:
        """Map an Admin Ops error onto the plugin's exceptions, by Code."""
        code = ""
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            code = str(payload.get("Code", "")).split(":", 1)[0].strip()

        message = self._format_error(response)
        return _ERROR_CODES.get(code, CephS3APIError)(message)

    # -- users --------------------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Check the gateway answers and the caps are usable.

        Deliberately not /admin/info: that endpoint is gated behind its own
        info=read cap, so a correctly configured agent would report itself
        unreachable. ?list needs only the users cap this flavour already
        requires.
        """
        try:
            self._request("GET", "/user", params={"list": None, "format": "json"})
        except Exception as e:
            logger.error("RadosGW admin API connection failed: %s", e)
            if raise_exception:
                raise BackendError(f"RadosGW connection failed: {e}")
            return False
        logger.info("RadosGW admin API connection successful")
        return True

    def create_user(
        self, uid: str, name: str, email: Optional[str] = None, **kwargs: object
    ) -> dict:
        """Create an S3 user holding only the key pair Waldur issued.

        Args:
            uid: Unique user identifier
            name: Display name; RGW answers 400 InvalidArgument without one
            email: User email address
            **kwargs: access_key/secret_key to apply, plus optional max_buckets
                and default_placement. tenant and default_storage_class are
                rejected for this flavour at configuration time, so they never
                arrive here.

        Returns:
            The created user object as RGW reports it.
        """
        uid = self._validate_uid(uid)
        params: dict[str, Any] = {
            "uid": uid,
            "display-name": name,
            # Without this RGW mints its own pair, leaving a live credential
            # Waldur neither knows nor can rotate.
            "generate-key": "False",
            "format": "json",
        }
        if email:
            params["email"] = email

        access_key = kwargs.get("access_key")
        secret_key = kwargs.get("secret_key")
        if access_key and secret_key:
            params["access-key"] = self._validate_access_key(str(access_key))
            params["secret-key"] = str(secret_key)

        for option, param in (
            ("max_buckets", "max-buckets"),
            ("default_placement", "default-placement"),
        ):
            if kwargs.get(option):
                params[param] = kwargs[option]

        logger.info("Creating S3 user: %s", uid)
        return self._request("PUT", "/user", params=params).json()

    def get_user_info(self, uid: str) -> dict:
        """Read one user, normalised. Unlike croit, RGW addresses users directly.

        RGW answers user_id/display_name where croit answers uid/name. The
        backend reads one spelling, and every read there is a .get() with a
        default — so returning the raw body publishes empty metadata rather
        than failing.
        """
        uid = self._validate_uid(uid)
        user = self._request(
            "GET", "/user", params={"uid": uid, "format": "json"}
        ).json()
        return {
            "uid": user.get("user_id"),
            "name": user.get("display_name"),
            "email": user.get("email"),
            "suspended": bool(user.get("suspended", 0)),
            "default_placement": user.get("default_placement"),
            "default_storage_class": user.get("default_storage_class"),
            "keys": user.get("keys", []),
        }

    def list_users(self) -> list:
        """List every S3 user id.

        ``?list`` is undocumented but present on squid and needs only the users
        cap. The metadata API (/admin/metadata/user) is the older route: it
        returns a bare array with no truncation signal and needs metadata=read.
        """
        payload = self._request(
            "GET", "/user", params={"list": None, "format": "json"}
        ).json()
        if payload.get("truncated"):
            logger.warning(
                "RadosGW truncated the user listing at %s entries", payload.get("count")
            )
        return payload.get("keys", [])

    def delete_user(self, uid: str) -> bool:
        """Remove the user, refusing to take its data with it.

        purge-data is deliberately not sent. RGW answers 409 with the misleading
        code BucketAlreadyExists for a user that still owns buckets, and letting
        that fail the order is the safer half of the trade: an erred terminate is
        recoverable, a purged bucket is not. croit behaves the same way, so both
        flavours agree.

        The 409 is re-raised with a message that says what actually happened,
        because "BucketAlreadyExists" on a delete tells an operator nothing.
        """
        uid = self._validate_uid(uid)
        try:
            self._request(
                "DELETE",
                "/user",
                params={"uid": uid, "format": "json"},
            )
        except CephS3UserNotFoundError:
            # Termination is retried, so an already-absent user is success.
            logger.info("S3 user %s already absent", uid)
        except CephS3UserExistsError as e:
            raise CephS3APIError(
                f"Cannot delete S3 user {uid}: it still owns buckets. Empty and "
                f"remove them first, then retry the termination ({e})"
            )
        return True

    # -- keys ---------------------------------------------------------------

    def list_user_keys(self, uid: str) -> list:
        """Every access key the user holds, in the shape the backend expects."""
        keys = self.get_user_info(uid).get("keys", [])
        return [
            {
                "user": key.get("user"),
                "access_key": key.get("access_key"),
                "secret_key": key.get("secret_key"),
            }
            for key in keys
        ]

    def create_user_key(self, uid: str, access_key: str, secret_key: str) -> None:
        """Add a key pair, or replace the secret of one the user already holds.

        Re-sending a known access key rewrites its secret in place — verified
        end to end: the new secret authenticates and the old one is refused.
        That is what lets a rotation keep a stable access key here, which croit
        cannot do.
        """
        uid = self._validate_uid(uid)
        access_key = self._validate_access_key(access_key)
        self._request(
            "PUT",
            "/user",
            params={
                "key": None,
                "uid": uid,
                "access-key": access_key,
                "secret-key": secret_key,
                "format": "json",
            },
        )

    def delete_user_key(self, uid: str, access_key: str) -> None:
        """Drop one access key, treating an unknown one as already dropped."""
        uid = self._validate_uid(uid)
        access_key = self._validate_access_key(access_key)
        try:
            self._request(
                "DELETE",
                "/user",
                params={
                    "key": None,
                    "uid": uid,
                    "access-key": access_key,
                    "format": "json",
                },
            )
        except CephS3UserNotFoundError:
            # 403/InvalidAccessKeyId, mapped to not-found in _ERROR_CODES.
            logger.info("Key %s already absent for user %s", access_key, uid)

    # -- quotas -------------------------------------------------------------

    @staticmethod
    def _from_rgw_quota(payload: dict) -> dict:
        """Read one quota scope into the neutral shape.

        max_size is authoritative and exact; max_size_kb is RGW's rounded view
        of the same number and is deliberately ignored.
        """
        return {
            "enabled": bool(payload.get("enabled", False)),
            "max_size_bytes": payload.get("max_size"),
            "max_objects": payload.get("max_objects"),
        }

    def _read_quota(self, uid: str, quota_type: str) -> dict:
        payload = self._request(
            "GET",
            "/user",
            params={
                "quota": None,
                "uid": uid,
                "quota-type": quota_type,
                "format": "json",
            },
        ).json()
        return self._from_rgw_quota(payload)

    def get_user_quota(self, uid: str) -> dict:
        """Both ceilings on a user, in the shape the backend's metadata expects."""
        uid = self._validate_uid(uid)
        return {
            "user_quota": self._read_quota(uid, "user"),
            "bucket_quota": self._read_quota(uid, "bucket"),
        }

    def _set_quota(self, uid: str, quota: dict, quota_type: str) -> None:
        """Write one quota scope.

        Sizes go in as bytes. max-size-kb exists and rounds up to the next KiB,
        which is why a croit-set ceiling never reads back equal; sending bytes
        keeps this flavour exact.

        A quota PUT merges, so dimensions the order did not ask for are left out
        rather than sent as 0 — which would cap the tenant at nothing.
        """
        params: dict[str, Any] = {
            "quota": None,
            "uid": uid,
            "quota-type": quota_type,
            "enabled": "True" if quota.get("enabled", True) else "False",
            "format": "json",
        }
        if quota.get("max_size_bytes") is not None:
            params["max-size"] = int(quota["max_size_bytes"])
        if quota.get("max_objects") is not None:
            params["max-objects"] = int(quota["max_objects"])

        logger.info("Setting %s quota for user %s: %s", quota_type, uid, quota)
        self._request("PUT", "/user", params=params)

    def set_user_quota(self, uid: str, quota: dict) -> None:
        """Cap everything the user holds in total."""
        self._set_quota(self._validate_uid(uid), quota, "user")

    def set_user_bucket_quota(self, uid: str, quota: dict) -> None:
        """Cap each individual bucket the user owns."""
        self._set_quota(self._validate_uid(uid), quota, "bucket")

    # -- buckets ------------------------------------------------------------

    def get_user_buckets(self, uid: str) -> list:
        """The user's buckets with their point-in-time size.

        Answers an empty list, never 404, for a user that owns nothing. A bucket
        RGW has no statistics for carries no rgw.main container at all, which
        reads as zero rather than raising.
        """
        uid = self._validate_uid(uid)
        buckets = self._request(
            "GET",
            "/bucket",
            params={"uid": uid, "stats": "True", "format": "json"},
        ).json()

        normalised = []
        for bucket in buckets:
            usage = (bucket.get("usage") or {}).get("rgw.main") or {}
            normalised.append(
                {
                    "name": bucket.get("bucket"),
                    # size_actual is the on-disk figure; size is the logical one.
                    "size_bytes": usage.get("size_actual", 0),
                    "num_objects": usage.get("num_objects", 0),
                }
            )
        return normalised
