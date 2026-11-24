"""croit statistics client: the stored-bytes series ``croit_usage`` integrates.

Users and buckets are managed through RGW Admin Ops (``clients.radosgw``); croit is
reached only for its statistics, with credentials of its own. Endpoint shapes
verified against croit 2503 and v2611.1.
"""

import logging
import time
from urllib.parse import urlencode

import httpx

from ..exceptions import (
    CephS3APIError,
    CephS3AuthenticationError,
    CephS3UserNotFoundError,
    CroitS3GraphNotFoundError,
)
from ..tenant import split
from .base import HttpTransport

logger = logging.getLogger(__name__)

# Per-S3-user storage over time, from croit's statistics subsystem. The name is
# croit's, defined in resources/statistics/graphite-queries.yml; the template
# parameter carries the uid. Verified against croit 2503 (OpenAPI 3.1).
STORAGE_GRAPH = "s3-user-data"
STORAGE_GRAPH_TEMPLATE = "template-s3-user-name"
# Per-bucket storage, the only croit series that sees what an RGW account owns:
# an account's buckets belong to the account, so its users' series stay empty.
# Each answer carries two plots, "Data" (logical bytes) and "Data with overhead"
# (allocated, RGW's size_actual); "Data" is what s3-user-data reports for a
# user, so it is the one read, keeping both kinds of resource billed alike.
BUCKET_STORAGE_GRAPH = "s3-bucket-data"
BUCKET_STORAGE_GRAPH_TEMPLATE = "template-s3-bucket-name"
BUCKET_STORAGE_PLOT = "Data"
# croit samples storage every 180 s and returns min(maxDataPoints, native) points,
# but never more than 10,000 per request (measured on v2611.1: asked for 15,000 to
# 50,000 over 31 days, it answers 10,000 at 267 s). Past that it averages into
# steps placed relative to the requested window, so the same past hours integrate
# to a slightly different total whenever the window's end moves -- and a month
# recomputed from its start every pass could then come out lower than before.
# A long window is therefore read in chunks small enough to stay at native
# resolution, whose samples sit on croit's fixed 180 s grid and never move.
STATS_NATIVE_STEP_SECONDS = 180
STATS_CHUNK_POINTS = 9000
# A session token is renewed this long before croit says it expires, so a request
# never starts with a token that lapses on the way.
SESSION_TOKEN_RENEW_MARGIN_SECONDS = 60


class CroitClient(HttpTransport):
    """Read-only client for croit's statistics API."""

    def __init__(
        self,
        api_url: str,
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        """Initialize the croit statistics client.

        Args:
            api_url: Base URL of croit API (e.g. https://192.168.240.34)
            username: API username, exchanged for a session token
            password: API password, exchanged for a session token
            token: Bearer token (alternative to username/password)
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        super().__init__(timeout=timeout, verify_ssl=verify_ssl)
        self.api_url = api_url.rstrip("/") + "/api"  # Add /api base path
        self.username = username
        self.password = password
        self.token = token
        self._session_token: str | None = None
        self._session_token_expires_at = 0.0

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif not (username and password):
            raise ValueError("Either token or username/password must be provided")

        # Transport-level retries cover connection errors;
        # 5xx responses are retried in _request
        self.session = httpx.Client(
            headers=headers,
            timeout=timeout,
            transport=httpx.HTTPTransport(verify=verify_ssl, retries=3),
        )

    def _build_url(self, endpoint: str, params: dict | None = None) -> str:
        """Resolve an endpoint against the /api base, letting httpx encode the query."""
        url = f"{self.api_url}{endpoint}"
        if params:
            return str(httpx.URL(url, params=params))
        return url

    def _auth_headers(self, method: str, url: str, body: bytes | None) -> dict:
        """A session token for username/password auth; an API token rides the session.

        croit answers HTTP Basic on its API endpoints with 401: Basic is accepted
        only by its login endpoint. A username and password are therefore
        exchanged for a session token, which expires (an hour on croit 2503) and
        is renewed ahead of that.
        """
        del method, url, body
        if self.token:
            return {}
        if (
            self._session_token is None
            or time.monotonic() >= self._session_token_expires_at
        ):
            self._log_in()
        return {"Authorization": f"Bearer {self._session_token}"}

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: dict | None = None,
        params: dict | None = None,
    ) -> httpx.Response:
        """Send one request, logging in again once if croit dropped the session.

        croit can invalidate a session token before its stated expiry (a restart,
        a logout elsewhere). Without the retry, the first call after that fails a
        real operation -- an order, or a resource's usage for the pass -- and only
        the call after it logs in afresh. Retried only when the refused token was
        a cached one: a token this very call just obtained being refused is a
        credentials problem, and repeating it would only hide that.
        """
        cached_session = (
            not self.token
            and self._session_token is not None
            and time.monotonic() < self._session_token_expires_at
        )
        try:
            return super()._request(method, endpoint, json_data=json_data, params=params)
        except CephS3AuthenticationError:
            if not cached_session:
                raise
            logger.info("croit refused the cached session token; logging in again")
            return super()._request(method, endpoint, json_data=json_data, params=params)

    def _log_in(self) -> None:
        """Exchange the username and password for a croit session token."""
        try:
            response = self.session.request(
                "POST",
                self._build_url("/auth/login-form"),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                content=urlencode(
                    {
                        "grant_type": "password",
                        "username": self.username,
                        "password": self.password,
                    }
                ).encode(),
            )
        except httpx.HTTPError as e:
            raise CephS3APIError(f"croit login failed: {e}") from e
        if response.status_code >= 400:
            raise self._classify_error(response)
        try:
            payload = response.json()
            access_token = payload["access_token"]
        except (ValueError, KeyError, TypeError) as e:
            raise CephS3AuthenticationError(
                f"croit login for user {self.username!r} returned no access token"
            ) from e
        expires_in = payload.get("expires_in")
        lifetime = float(expires_in) if isinstance(expires_in, (int, float)) else 3600.0
        self._session_token = str(access_token)
        self._session_token_expires_at = time.monotonic() + max(
            lifetime - SESSION_TOKEN_RENEW_MARGIN_SECONDS, 0.0
        )

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
            # A session token croit dropped early (restart, logout) is fetched
            # afresh on the next call rather than replayed until it expires.
            self._session_token = None
            return CephS3AuthenticationError(
                f"croit rejected {credential}: check that it is valid and, for a "
                "token, that it has not expired"
            )

        error_msg = self._format_error(response)
        if response.status_code == 404:
            return CephS3UserNotFoundError(error_msg)
        return CephS3APIError(error_msg)

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
        query_name = self._stats_query_name(uid)
        if query_name is None:
            # Unmeasured, not zero: the reporter skips an empty series, where a
            # zero would overwrite the period's accrued total.
            return []
        points = self._storage_series(
            STORAGE_GRAPH, STORAGE_GRAPH_TEMPLATE, query_name, None, start_time, end_time
        )
        if not points:
            logger.warning("No storage series for S3 user %s", uid)
        return points

    def get_bucket_storage_series(
        self, bucket: str, tenant: str, start_time: int, end_time: int = 0
    ) -> list[dict]:
        """Read one bucket's storage-over-time series.

        croit names a tenanted bucket ``tenant_bucket`` in its statistics -- the
        "$", "/", ":" and "." spellings return nothing, measured on croit v2611.1.

        Args:
            bucket: Bucket name
            tenant: The bucket's RGW tenant, empty for none
            start_time: Absolute unix seconds, or negative for "N seconds ago"
            end_time: Same form as start_time; 0 means now

        Returns:
            List of {"t", "v"} datapoints, oldest first. Empty when croit holds no
            series for the bucket, e.g. one created since its last sample.

        Raises:
            CroitS3GraphNotFoundError: If the graph name no longer exists
            CephS3APIError: If the request fails
        """
        name = self._validate_uid(f"{tenant}_{bucket}" if tenant else bucket)
        return self._storage_series(
            BUCKET_STORAGE_GRAPH,
            BUCKET_STORAGE_GRAPH_TEMPLATE,
            name,
            BUCKET_STORAGE_PLOT,
            start_time,
            end_time,
        )

    @staticmethod
    def _stats_windows(start_time: int, end_time: int) -> list[tuple[int, int]]:
        """Split [start, end] into consecutive windows of STATS_CHUNK_POINTS
        native samples, after resolving croit's relative times (negative = that
        many seconds ago, 0 = now) against one clock reading."""
        now = int(time.time())
        start = now + start_time if start_time < 0 else start_time
        end = now + end_time if end_time <= 0 else end_time
        span = STATS_CHUNK_POINTS * STATS_NATIVE_STEP_SECONDS
        windows = []
        while True:
            windows.append((start, min(start + span, end)))
            start += span
            if start >= end:
                return windows

    def _storage_series(
        self,
        graph: str,
        template_key: str,
        name: str,
        plot_name: str | None,
        start_time: int,
        end_time: int,
    ) -> list[dict]:
        """One storage series at croit's native resolution, read window by window
        and joined. ``plot_name`` picks a plot by name; None takes the first.

        Raises:
            CroitS3GraphNotFoundError: If the graph name no longer exists
            CephS3APIError: If a request fails
        """
        points: list[dict] = []
        for start, end in self._stats_windows(start_time, end_time):
            try:
                response = self._request(
                    "GET",
                    "/stats",
                    params={
                        "graph": graph,
                        template_key: name,
                        "startTime": start,
                        "endTime": end,
                        # +1: a window holds both of its edge samples.
                        "maxDataPoints": STATS_CHUNK_POINTS + 1,
                    },
                )
            except CephS3UserNotFoundError as e:
                # /stats answers 404 for an unknown graph, and _request cannot
                # tell that apart from an unknown user. Re-raise as its own type
                # so the reporting loop does not read a renamed graph as a gone
                # resource.
                raise CroitS3GraphNotFoundError(
                    f"Storage graph {graph!r} not found on the croit API: {e}"
                )
            plots = response.json().get("axis1", {}).get("graphs", [])
            chosen = (
                plots[:1]
                if plot_name is None
                else [plot for plot in plots if plot.get("name") == plot_name]
            )
            if plots and not chosen:
                logger.warning(
                    "croit's %s answer for %s has no %r plot (got %s)",
                    graph,
                    name,
                    plot_name,
                    [plot.get("name") for plot in plots],
                )
            last = points[-1]["t"] if points else None
            # Adjacent windows share their edge sample; keep it once.
            points.extend(
                point
                for point in (chosen[0].get("datapoints", []) if chosen else [])
                if last is None or point.get("t", 0) > last
            )
        return points

    @staticmethod
    def _stats_query_name(uid: str) -> str | None:
        """The value croit's statistics know a user by, for the graph template.

        For an untenanted user that is the uid itself, as measured. croit refuses
        a tenanted ``tenant$uid`` there outright (400, "$" is not a valid template
        character), every spelling without the "$" tried against a live croit
        returned no series, and croit's published API (its own /api/openapi.json)
        offers no way to look the name up. So a tenanted user is reported as
        unmeasurable rather than sent in a form croit rejects.

        Returns:
            The template value, or None when croit offers no way to name the user.
        """
        if not split(uid)[0]:
            return uid
        logger.warning(
            "croit offers no statistics name for tenanted S3 user %s, so its "
            "storage cannot be metered; it is reported as unmeasured",
            uid,
        )
        return None
