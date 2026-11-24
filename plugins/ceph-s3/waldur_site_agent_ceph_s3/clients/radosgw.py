"""RadosGW Admin Ops client — the vanilla Ceph flavour.

Behaviour verified against Ceph 19.2.0 (squid) rather than taken from
https://docs.ceph.com/en/latest/radosgw/adminops/, which is wrong about the
duplicate-user error code, claims a JSON request body works for quotas (it
answers 501), and documents no way to list users when ``?list`` does exactly
that. ``_ERROR_CODES`` below carries the measured response codes.
"""

import datetime
import logging
import re
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_ceph_s3.clients.base import _TENANT_RE, BaseS3AdminClient
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3AccountExistsError,
    CephS3AccountNotFoundError,
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)

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
    "AccountAlreadyExists": CephS3AccountExistsError,
}

# An RGW account id is "RGW" followed by 17 digits; the gateway refuses any other
# shape with "account id must be 20 bytes long". Checked before it goes into a
# query string, like every other identifier this client sends.
_ACCOUNT_ID_RE = re.compile(r"^RGW[0-9]{17}$")

# Users are listed in pages so an account's members can be found on a cluster of
# any size; ?list answers "truncated" with a marker past this many entries.
_USER_LIST_PAGE = 1000


def _usage_time(moment: datetime.datetime) -> str:
    """A usage-log bound as RGW parses it: a UTC "YYYY-MM-DD HH:MM:SS" string."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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
        self.gateway_url = endpoint.rstrip("/")
        self.base_url = self.gateway_url + "/" + admin_path.strip("/")
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

    def _build_url(self, endpoint: str, params: dict | None = None) -> str:
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

    def _auth_headers(self, method: str, url: str, body: bytes | None) -> dict:
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
        # The account endpoints report an absent account as 404/NoSuchKey, a code
        # that elsewhere means an absent object, so it is mapped for them alone.
        if code == "NoSuchKey" and response.request.url.path.endswith("/account"):
            return CephS3AccountNotFoundError(message)
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
            logger.exception("RadosGW admin API connection failed")
            if raise_exception:
                raise BackendError(f"RadosGW connection failed: {e}") from e
            return False
        logger.info("RadosGW admin API connection successful")
        return True

    def create_user(
        self, uid: str, name: str, email: str | None = None, **kwargs: object
    ) -> dict:
        """Create an S3 user holding only the key pair Waldur issued.

        Args:
            uid: Unique user identifier
            name: Display name; RGW answers 400 InvalidArgument without one
            email: User email address
            **kwargs: access_key/secret_key to apply, plus optional max_buckets
                default_placement and default_storage_class (RGW applies the
                class only within the placement rule; settings refuse one
                without the other). account_id makes the user a member of that
                RGW account, and account_root its root user. A tenant arrives
                inside the uid, as ``tenant$uid``.

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
            ("default_storage_class", "default-storage-class"),
        ):
            if kwargs.get(option):
                params[param] = kwargs[option]

        account_id = kwargs.get("account_id")
        if account_id:
            params["account-id"] = self._validate_account_id(str(account_id))
            if kwargs.get("account_root"):
                params["account-root"] = "True"

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
            # Empty for a user outside any account; "root" marks an account's
            # root user, "none" a user created through the IAM API.
            "account_id": user.get("account_id") or "",
            "type": user.get("type") or "",
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
            ) from e
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
        That is what lets a rotation keep a stable access key.
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

    def set_user_suspended(self, uid: str, suspended: bool) -> None:
        """Suspend or reinstate a user.

        A suspended user's keys are refused (UserSuspended) on every request, IAM
        users of an account included.
        """
        self._request(
            "POST",
            "/user",
            params={
                "uid": self._validate_uid(uid),
                "suspended": "True" if suspended else "False",
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
                    "logical_bytes": usage.get("size", 0),
                    "num_objects": usage.get("num_objects", 0),
                }
            )
        return normalised

    # -- usage log ------------------------------------------------------------

    def get_usage(
        self, uid: str, start: datetime.datetime, end: datetime.datetime
    ) -> dict:
        """Traffic and operations of everything an owner's buckets served in a window.

        Read from the RGW usage log, which the gateway keeps per bucket owner and
        hour, and only while ``rgw_enable_usage_log`` is on. The owner is a user's
        uid, or an account's id for everything in an account, whichever user made
        the request; a root user's uid finds nothing there (measured on 20.2.5).

        Needs the ``usage=read`` cap: without it RGW answers AccessDenied.

        Args:
            uid: The bucket owner: a (tenant-qualified) uid or an account id.
            start: Start of the window; the log is hourly, so whole hours count.
            end: End of the window.

        Returns:
            ``{"bytes_sent", "bytes_received", "ops", "successful_ops"}`` summed
            over the window; all zero when the log holds nothing for the owner.
        """
        owner = uid if _ACCOUNT_ID_RE.match(uid) else self._validate_uid(uid)
        try:
            payload = self._request(
                "GET",
                "/usage",
                params={
                    "uid": owner,
                    "start": _usage_time(start),
                    "end": _usage_time(end),
                    "show-entries": "False",
                    "show-summary": "True",
                    "format": "json",
                },
            ).json()
        except CephS3AuthenticationError as e:
            raise CephS3AuthenticationError(
                "RGW refused to read its usage log; the agent's admin user needs the "
                "'usage=read' cap (radosgw-admin caps add --uid <admin> --caps "
                f"usage=read): {e}"
            ) from e

        return self._sum_usage(payload, owner)

    def usage_log_has_entries(
        self, start: datetime.datetime, end: datetime.datetime
    ) -> bool:
        """Whether the usage log holds anything at all, for any owner, in a window.

        An empty log across every owner means it is not being written
        (``rgw_enable_usage_log`` is off by default) far more often than that
        nobody used the cluster. RGW reads every owner's entries in the window to
        answer, so keep the window short.
        """
        payload = self._request(
            "GET",
            "/usage",
            params={
                "start": _usage_time(start),
                "end": _usage_time(end),
                "show-entries": "False",
                "show-summary": "True",
                "format": "json",
            },
        ).json()
        return bool((payload or {}).get("summary"))

    @staticmethod
    def _sum_usage(payload: dict, owner: str) -> dict:
        totals = dict.fromkeys(("bytes_sent", "bytes_received", "ops", "successful_ops"), 0)
        for summary in (payload or {}).get("summary") or []:
            if summary.get("user") not in (None, owner):
                continue
            total = summary.get("total") or {}
            for key in totals:
                totals[key] += int(total.get(key) or 0)
        return totals

    # -- accounts -----------------------------------------------------------
    #
    # Accounts need Ceph 20.2.2 or later. Earlier gateways check GET and DELETE
    # /admin/account against a cap named "account", which cannot be granted, and
    # have no account quota endpoint at all -- 19.2.x answers that PUT with 200
    # and changes nothing. Callers therefore read a quota back after writing it.

    @staticmethod
    def _validate_account_id(account_id: str) -> str:
        if not _ACCOUNT_ID_RE.match(account_id):
            raise CephS3APIError(f"Not an RGW account id: {account_id!r}")
        return account_id

    @staticmethod
    def _from_rgw_account(account: dict) -> dict:
        return {
            "id": account.get("id"),
            "name": account.get("name"),
            "email": account.get("email"),
            "tenant": account.get("tenant") or "",
            "quota": RadosGWClient._from_rgw_quota(account.get("quota") or {}),
            "bucket_quota": RadosGWClient._from_rgw_quota(
                account.get("bucket_quota") or {}
            ),
        }

    def create_account(
        self, name: str, email: str | None = None, tenant: str = ""
    ) -> dict:
        """Create an RGW account; the gateway picks its id.

        A tenanted account holds only users of its tenant, and its buckets live in
        that tenant's namespace. Measured on 20.2.5: RGW refuses (400) to add an
        untenanted or other-tenant user to it.

        Raises:
            CephS3AccountExistsError: when the name or the email is taken.
        """
        params: dict[str, Any] = {"name": self._validate_uid(name), "format": "json"}
        if tenant:
            params["tenant"] = self._validate_tenant(tenant)
        if email:
            params["email"] = email
        logger.info("Creating RGW account: %s", name)
        account = self._request("POST", "/account", params=params).json()
        return self._from_rgw_account(account)

    def get_account(
        self,
        account_id: str | None = None,
        name: str | None = None,
        tenant: str = "",
    ) -> dict:
        """Read one account by id or, failing that, by name within its tenant.

        Raises:
            CephS3AccountNotFoundError: when no such account exists.
        """
        params: dict[str, str]
        if account_id:
            params = {"id": self._validate_account_id(account_id)}
        elif name:
            params = {"name": self._validate_uid(name)}
            if tenant:
                params["tenant"] = self._validate_tenant(tenant)
        else:
            msg = "An account is read by id or by name"
            raise CephS3APIError(msg)
        account = self._request(
            "GET", "/account", params={**params, "format": "json"}
        ).json()
        return self._from_rgw_account(account)

    def set_account_quota(self, account_id: str, quota: dict, quota_type: str) -> None:
        """Write the account-wide quota ("account") or its per-bucket one ("bucket").

        Like the user quota, a PUT merges: dimensions left out keep their value.
        """
        if quota_type not in ("account", "bucket"):
            raise CephS3APIError(f"Unknown account quota type: {quota_type!r}")
        params: dict[str, Any] = {
            "quota": None,
            "id": self._validate_account_id(account_id),
            "quota-type": quota_type,
            "enabled": "True" if quota.get("enabled", True) else "False",
            "format": "json",
        }
        if quota.get("max_size_bytes") is not None:
            params["max-size"] = int(quota["max_size_bytes"])
        if quota.get("max_objects") is not None:
            params["max-objects"] = int(quota["max_objects"])
        logger.info(
            "Setting %s quota for account %s: %s", quota_type, account_id, quota
        )
        self._request("PUT", "/account", params=params)

    def delete_account(self, account_id: str) -> None:
        """Remove an account that has no users left; an absent one is success.

        RGW refuses (409) while any user remains, whatever the users own.
        """
        try:
            self._request(
                "DELETE",
                "/account",
                params={"id": self._validate_account_id(account_id), "format": "json"},
            )
        except CephS3AccountNotFoundError:
            logger.info("RGW account %s already absent", account_id)
        except CephS3APIError as e:
            if "BucketNotEmpty" not in str(e):
                raise
            # RGW answers ENOTEMPTY, spelled BucketNotEmpty, for anything the
            # account still holds -- users, buckets, but also IAM roles, groups
            # and OIDC providers, which Admin Ops cannot list.
            raise CephS3APIError(
                f"RGW refused to delete account {account_id}: it still holds "
                "users, buckets or IAM resources (roles, groups, OIDC providers)"
            ) from e

    def _iter_user_ids(self) -> Iterator[str]:
        marker = ""
        while True:
            params: dict[str, Any] = {
                "list": None,
                "max-entries": _USER_LIST_PAGE,
                "format": "json",
            }
            if marker:
                params["marker"] = marker
            page = self._request("GET", "/user", params=params).json()
            yield from page.get("keys", [])
            marker = page.get("marker") or ""
            if not page.get("truncated") or not marker:
                return

    @staticmethod
    def _validate_tenant(tenant: str) -> str:
        if not _TENANT_RE.match(tenant):
            raise CephS3APIError(f"Not an RGW tenant name: {tenant!r}")
        return tenant

    def list_account_users(self, account_id: str, tenant: str = "") -> list:
        """Every user id belonging to the account.

        Admin Ops can neither filter the user listing by account nor list an
        account's members, so this walks the gateway's user listing. RGW keeps an
        account's users in the account's tenant, so only uids of that tenant are
        read -- for a tenanted account, a small slice of the gateway. Users created
        through the IAM API get a generated uid, which is why membership is read
        from account_id, not from names.
        """
        account_id = self._validate_account_id(account_id)
        if tenant:
            self._validate_tenant(tenant)
        members = []
        for uid in self._iter_user_ids():
            if uid.rpartition("$")[0] != tenant:
                continue
            try:
                info = self.get_user_info(uid)
            except CephS3UserNotFoundError:
                continue
            except CephS3APIError:
                # A uid this client will not address, e.g. one holding "@". It is
                # not ours -- this plugin never creates one -- and one such user on
                # the gateway must not make every termination fail.
                logger.debug("Skipping user %r, which this client cannot address", uid)
                continue
            if info.get("account_id") == account_id:
                members.append(uid)
        return members
