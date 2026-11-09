"""Prometheus-backed consumption metering for the k8s namespace backend.

Waldur bills a namespace by what it consumed in a billing period, and the cluster
already measures that: cAdvisor, the kubelet and kube-state-metrics expose it, and the
monitoring stack retains it. So the backend reads consumption from Prometheus rather
than reconstructing it from the namespace's quota, which is an entitlement and not a
measurement.

Everything is reported in the period's **native units** -- core-hours, GiB-hours,
GPU-hours -- because those are what a price list is written against. Waldur's
``unit_factor`` converts from there.

One instant query per component type covers every namespace it was asked about: the
requested backend ids are folded into a single ``namespace=~"a|b|c"`` selector, so
measuring N namespaces for M components costs M requests, not N times M. The queries
are sent as ``POST /api/v1/query``: the selector grows with the offering, and a POST
form body has no URL length limit to run into.

The data is only as trustworthy as its source. Every built-in query pins the scrape
job that produces its metric (``{cadvisor_selector}``, ``{kubelet_selector}``,
``{kube_state_metrics_selector}``) and clamps each series at zero, so a series a tenant
publishes under the same metric name from its own exporter neither cancels nor adds to
a bill.
"""

from __future__ import annotations

import datetime
import json
import math
import re
import ssl
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlsplit

import httpx

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError

DEFAULT_TIMEOUT = 30.0
# Resolution of the gauge integrals. A step point measures the state at one instant,
# so anything shorter than the step that falls between two points is not seen at all,
# and anything that covers a point is billed for the whole step: with an hourly step
# a job running :10-:40 bills nothing and one running :55-:05 bills six times its use.
# A minute keeps that error to about a minute per pod start or stop.
DEFAULT_STEP = "1m"
# Prometheus's default lookback (--query.lookback-delta). A step longer than this
# leaves stretches between points that no point sees.
MAX_STEP_SECONDS = 300.0
# A period younger than the step has no step point before its end; it is then
# measured at this resolution instead.
SHORT_WINDOW_STEP_SECONDS = 60
# A period that has only just opened has a window of seconds. Prometheus rejects a
# zero-length range selector, so the shortest window asked for is a minute.
MIN_WINDOW_SECONDS = 60
# Assumed when the retention is neither configured nor readable from Prometheus:
# Prometheus's own default.
FALLBACK_RETENTION = "15d"
# A closed month is measured again only this long after it ended. Waldur takes usage
# for a past month only while that month's invoice is still open, which by default
# it is not at all (INVOICE_FINALIZATION_GRACE_PERIOD_HOURS = 0).
DEFAULT_CLOSED_PERIOD_HOURS = 24.0
# A per-namespace sum is a few hundred bytes per namespace. An answer this large is
# an override that forgot to aggregate, not a bill.
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_MAX_ERROR_BODY = 300

SECONDS_PER_HOUR = 3600

# Where kube-prometheus-stack scrapes each metric from. Pinning the job keeps out a
# tenant's own exporter publishing the same metric names, and pinning the cAdvisor
# path keeps out the kubelet's /metrics/resource endpoint, which repeats the same
# containers' usage and would bill it twice.

# What an empty selector becomes: a matcher every series satisfies.
MATCH_ANY = '__name__!=""'

DEFAULT_SELECTORS: dict[str, str] = {
    "cadvisor": 'job="kubelet", metrics_path="/metrics/cadvisor"',
    "kubelet": 'job="kubelet", metrics_path="/metrics"',
    "kube_state_metrics": 'job="kube-state-metrics"',
    # Prometheus's own scrape job; only the coverage fallback uses it.
    "prometheus": MATCH_ANY,
}

# Placeholders a query template may use:
#
# - ``{namespaces}``: the alternation of requested backend ids (required);
# - ``{window}``: the period as a PromQL duration (``2678400s``);
# - ``{window_seconds}``: the same as a bare number, for arithmetic;
# - ``{step}``: the resolution of the gauge integrals, as a PromQL duration;
# - ``{step_seconds}``: the same as a bare number;
# - ``{<name>_selector}``: each entry of the ``selectors`` setting.
#
# Templates are filled by literal replacement rather than ``str.format``: a PromQL
# selector is full of braces (``{namespace=~"..."}``), and ``format`` would read every
# one of them as a field.
#
# The gauges (ram, storage, gpu) are *integrated*: the per-namespace sum is sampled
# once per step across the period, and each point stands for the step that follows
# it, so ``sum_over_time(...) * {step_seconds}`` is the area under the curve in
# unit-seconds. An average would divide by the points that exist, billing a namespace
# present for half the period as if it had been present for all of it.
#
# Where the points fall. The queries are evaluated at the period's end, and a
# subquery ``[{window}:{step}]`` there yields the step-aligned instants in
# ``(end - window, end]`` (Prometheus 3 evaluates ranges left-open). ``offset {step}``
# shifts that to ``(start - step, end - step]``: the first point is the period's start
# and the last is one step before its end. Consecutive periods therefore tile -- every
# aligned instant belongs to exactly one month -- and a series that begins exactly at
# a month boundary is not billed a step to the month before.
#
# A point measures the latest sample within Prometheus's lookback, so a pod is billed
# from the first point at or after its first sample to the last point that still sees
# one: about a step out at each start and stop. A series that stops is marked stale at
# the next scrape; one that stops without a staleness marker (backfilled data, a
# Prometheus restart) stays visible for the whole lookback.
DEFAULT_QUERIES: dict[str, str] = {
    # Core-hours: the counter counts CPU-seconds, so the period's increase divided by
    # 3600 is the cores held for an hour. ``container!=""`` drops cAdvisor's pod- and
    # namespace-level rollup rows, which repeat the same seconds; ``image!=""`` drops
    # the pause container and the image-less rows of other endpoints.
    "cpu": (
        "sum by (namespace) (clamp_min(increase(container_cpu_usage_seconds_total"
        '{namespace=~"{namespaces}", {cadvisor_selector}, container!="", image!=""}'
        "[{window}]), 0)) / 3600"
    ),
    # GiB-hours: the working set integrated over the period.
    "ram": (
        "sum_over_time((sum by (namespace) (clamp_min(container_memory_working_set_bytes"
        '{namespace=~"{namespaces}", {cadvisor_selector}, container!="", image!=""}, 0)))'
        "[{window}:{step}] offset {step}) * {step_seconds} / 3600 / 1073741824"
    ),
    # GiB-hours of what is stored, not of the capacity granted. A ReadWriteMany
    # claim mounted on several nodes is reported by each node's kubelet, so each
    # claim counts once, at its largest reading.
    "storage": (
        "sum_over_time((sum by (namespace) (max by (namespace, persistentvolumeclaim) ("
        'clamp_min(kubelet_volume_stats_used_bytes{namespace=~"{namespaces}", {kubelet_selector}}'
        ", 0))))[{window}:{step}] offset {step}) * {step_seconds} / 3600 / 1073741824"
    ),
    # GPU-hours: what the scheduler holds for each pod -- the larger of its largest
    # init-container request and the sum of its containers' requests -- while the pod
    # is Running. A Pending pod holds no GPU yet, and a Succeeded or Failed one keeps
    # its request in kube-state-metrics until it is deleted. Each container is counted
    # once even when two kube-state-metrics replicas report it.
    "gpu": (
        "sum_over_time((sum by (namespace) (max by (namespace, pod) ("
        "label_replace(sum by (namespace, pod) (max by (namespace, pod, container) ("
        "clamp_min(kube_pod_container_resource_requests"
        '{namespace=~"{namespaces}", {kube_state_metrics_selector}, resource="nvidia_com_gpu"}'
        ', 0))), "requests", "containers", "", "")'
        " or "
        "label_replace(max by (namespace, pod) ("
        "clamp_min(kube_pod_init_container_resource_requests"
        '{namespace=~"{namespaces}", {kube_state_metrics_selector}, resource="nvidia_com_gpu"}'
        ', 0)), "requests", "init", "", "")'
        ") * on (namespace, pod) group_left () (max by (namespace, pod) ("
        'kube_pod_status_phase{namespace=~"{namespaces}", {kube_state_metrics_selector}, '
        'phase="Running"}) == 1)))'
        "[{window}:{step}] offset {step}) * {step_seconds} / 3600"
    ),
}

# The characters RE2 reads as pattern syntax. ``re.escape`` is not used directly: it
# also escapes ``-`` and other punctuation, which turns a plain namespace into
# ``waldur\-foo`` and makes the query that reached Prometheus hard to read back.
_REGEX_METACHARACTERS = re.compile(r"([\\.^$*+?{}\[\]|()])")
# Escapes for a PromQL double-quoted string literal.
_PROMQL_STRING_ESCAPES = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"}

_PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")
_SELECTOR_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_BASE_PLACEHOLDERS = frozenset(
    {"namespaces", "window", "window_seconds", "step", "step_seconds"}
)

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d|w|y)")
_DURATION_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": SECONDS_PER_HOUR,
    "d": 86400.0,
    "w": 604800.0,
    "y": 31536000.0,
}

_UNSET = object()

# Query timeouts in a row after which Prometheus counts as unavailable for the cycle.
MAX_CONSECUTIVE_QUERY_TIMEOUTS = 3
# Share of the agent's timeout given to Prometheus to evaluate a query, so that a slow
# query comes back as Prometheus's own timeout error before the agent stops waiting.
SERVER_TIMEOUT_SHARE = 0.8


class PrometheusError(BackendError):
    """Prometheus could not be used to measure a period."""


class PrometheusQueryError(PrometheusError):
    """Prometheus refused or could not answer one query; the others may still work."""


class PrometheusQueryTimeoutError(PrometheusQueryError):
    """Prometheus gave up on one query after its timeout.

    Usually the query, not the server: a namespace with many series makes its own
    month-to-date subqueries slow. Only a run of them in a row counts as the server
    being unavailable (``MAX_CONSECUTIVE_QUERY_TIMEOUTS``).
    """


class PrometheusUnavailableError(PrometheusError):
    """Prometheus could not be reached, timed out or rejected the credentials.

    Every further query would fail the same way, so a pass stops at the first one.
    """


class PrometheusNotFoundError(PrometheusUnavailableError):
    """The endpoint does not exist on this server."""


class PrometheusCoverageError(PrometheusError):
    """Prometheus no longer holds, or never held, all of the period asked about."""


class PrometheusPeriodClosedError(PrometheusError):
    """The period ended longer ago than ``closed_period_hours``."""


def verify_ssl_setting(settings: dict) -> Union[bool, ssl.SSLContext]:
    """Read ``ssl_verify``: on by default, off only when explicitly false, or a CA bundle.

    ``.get("ssl_verify", True)`` only covers an absent key: a bare ``ssl_verify:`` in
    the YAML is *present* and ``None``, which ``bool()`` reads as False and silently
    turns certificate verification off. A string names a CA bundle file or a
    directory of certificates, as the keycloak client's setting does.

    Raises:
        PrometheusError: if a CA bundle path does not exist.
    """
    verify = settings.get("ssl_verify")
    if verify is None:
        return True
    if isinstance(verify, str):
        path = Path(verify.strip())
        if path.is_dir():
            return ssl.create_default_context(capath=str(path))
        if path.is_file():
            return ssl.create_default_context(cafile=str(path))
        msg = (
            "'ssl_verify' must be true, false or the path of a CA bundle; "
            f"{str(path)!r} does not exist"
        )
        raise PrometheusError(msg)
    return bool(verify)


def timeout_setting(settings: dict) -> float:
    """Read ``timeout``, defaulting when it is absent or explicitly null."""
    timeout = settings.get("timeout")
    return DEFAULT_TIMEOUT if timeout is None else float(timeout)


def duration_seconds(value: Union[str, float, None], setting: str = "step") -> float:
    """Read a PromQL duration (``1h30m``) or a plain number of seconds.

    Args:
        value: the duration as configured.
        setting: the setting it came from, named in the error.

    Raises:
        PrometheusError: if the value is not a positive duration this can read.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value <= 0:
            msg = f"The '{setting}' setting must be a positive duration, got {value!r}"
            raise PrometheusError(msg)
        return float(value)
    text = str(value or "").strip()
    if not text:
        msg = f"The '{setting}' setting must be a duration such as '1h'"
        raise PrometheusError(msg)

    position = 0
    total = 0.0
    for match in _DURATION_PART.finditer(text):
        if match.start() != position:
            break
        total += float(match.group(1)) * _DURATION_UNITS[match.group(2)]
        position = match.end()
    if position != len(text) or not total:
        msg = f"The '{setting}' setting must be a PromQL duration such as '1h', got {text!r}"
        raise PrometheusError(msg)
    return total


def format_duration(seconds: float) -> str:
    """Write a number of seconds as the largest whole PromQL unit (``40d``, ``90m``)."""
    whole = int(seconds)
    if whole != seconds:
        return f"{seconds:g}s"
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if whole and whole % size == 0:
            return f"{whole // size}{unit}"
    return f"{whole}s"


def _validated_url(url: str) -> str:
    """Check the ``url`` setting and return it without a trailing slash.

    Credentials are refused rather than used: ``url`` is not a secret field, so they
    would sit in the configuration and the logs in plain text. The message does not
    repeat the URL for the same reason.
    """
    parts = urlsplit(url)
    if "@" in parts.netloc:
        msg = (
            "The Prometheus 'url' must not carry credentials; put them in 'username' "
            "and 'password' (or 'bearer_token'), which are secret settings"
        )
        raise PrometheusError(msg)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        msg = f"The Prometheus 'url' must be an http(s) URL with a host, got {url!r}"
        raise PrometheusError(msg)
    if parts.query or parts.fragment:
        msg = (
            "The Prometheus 'url' must not carry a query string or fragment: queries "
            f"are sent to <url>/api/v1/query, got {url!r}"
        )
        raise PrometheusError(msg)
    return url.rstrip("/")


def render_query(template: str, placeholders: dict[str, str]) -> str:
    """Fill the placeholders a query template names, leaving PromQL's braces alone."""
    for name, value in placeholders.items():
        template = template.replace("{" + name + "}", value)
    return template


def namespace_pattern(namespaces: Sequence[str]) -> str:
    """Fold the requested backend ids into one regex alternation, quoted for PromQL.

    Prometheus anchors a matcher's regex at both ends, so a bare alternation matches
    exactly the namespaces asked about. Each id is escaped twice: for RE2, so it
    stays a literal, and then for the PromQL string literal the regex sits in, where
    a lone backslash or a double quote would end or break the string.
    """
    regex = "|".join(_REGEX_METACHARACTERS.sub(r"\\\1", n) for n in namespaces)
    return "".join(_PROMQL_STRING_ESCAPES.get(c, c) for c in regex)


def _series_value(raw: object) -> Optional[float]:
    """Read one series' value, or None when it says nothing billable.

    ``NaN`` (a counter Prometheus could not evaluate) and infinities are dropped, so
    one of them cannot turn a namespace's figure into ``NaN``. A negative value is
    dropped too: consumption cannot be negative, and the built-in queries clamp every
    input series at zero, so a negative answer comes from an override and would
    otherwise lower a bill.
    """
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    if value < 0:
        logger.warning("Ignoring a negative value %s from Prometheus", value)
        return None
    return value


def _parse_result(payload: object) -> dict[str, float]:
    """Sum one query's answer per namespace.

    A namespace with no series is absent from the result rather than present as zero:
    "Prometheus has nothing for it" and "it used nothing" are different answers.

    A series without a ``namespace`` label is dropped with a warning: there is no
    honest way to say whose it is, and guessing could bill one tenant for another's
    usage.

    Raises:
        PrometheusQueryError: if the answer is not a vector this can read.
    """
    if not isinstance(payload, dict) or payload.get("status") != "success":
        reason = payload.get("error") if isinstance(payload, dict) else None
        msg = f"Prometheus did not answer the query: {reason or payload!r:.{_MAX_ERROR_BODY}}"
        raise PrometheusQueryError(msg)

    data = payload.get("data")
    data = data if isinstance(data, dict) else {}
    result_type = data.get("resultType")
    if result_type != "vector":
        msg = (
            f"Prometheus answered with a {result_type!r} rather than a vector of "
            "per-namespace series; aggregate the query with 'sum by (namespace)'"
        )
        raise PrometheusQueryError(msg)

    totals: dict[str, float] = {}
    unlabelled = 0
    for series in data.get("result") or []:
        if not isinstance(series, dict):
            continue
        metric = series.get("metric")
        metric = metric if isinstance(metric, dict) else {}
        namespace = metric.get("namespace")
        if not namespace:
            unlabelled += 1
            continue
        value = _series_value((series.get("value") or [None, None])[1])
        if value is None:
            continue
        namespace = str(namespace)
        totals[namespace] = totals.get(namespace, 0.0) + value
    if unlabelled:
        logger.warning(
            "Ignoring %d Prometheus series with no namespace label: there is no way to "
            "tell which namespace they belong to",
            unlabelled,
        )
    return totals


def _placeholders_of(template: str) -> set[str]:
    return set(_PLACEHOLDER.findall(template))


class PrometheusClient:
    """Reads period consumption from a Prometheus HTTP API.

    Holds the configuration, and caches what it learns about the server's retention
    for its own lifetime -- one reporting cycle, as the agent builds a new backend
    every cycle. Each pass opens its own pooled HTTP client with :meth:`connect` and
    closes it when the pass ends, so no connection pool outlives its cycle.
    """

    def __init__(
        self, settings: Optional[dict], transport: Optional[httpx.BaseTransport] = None
    ) -> None:
        """Initialize the client from the offering's ``prometheus`` settings block.

        Args:
            settings: the ``prometheus`` block of the backend settings.
            transport: an httpx transport to send requests through instead of the
                network; for tests.

        Raises:
            PrometheusError: if the block cannot be used as configured.
        """
        settings = settings or {}
        url = settings.get("url")
        if not url:
            msg = "Prometheus billing requires 'url' in the prometheus settings"
            raise PrometheusError(msg)
        self.url = _validated_url(str(url))
        self.verify_ssl = verify_ssl_setting(settings)
        self.timeout = timeout_setting(settings)
        if self.timeout <= 0:
            msg = f"The 'timeout' setting must be positive, got {self.timeout!r}"
            raise PrometheusError(msg)

        self.step_seconds = duration_seconds(settings.get("step") or DEFAULT_STEP, "step")
        if self.step_seconds > MAX_STEP_SECONDS:
            msg = (
                f"The 'step' setting must be at most 5m, Prometheus's lookback; got "
                f"{settings.get('step')!r}. A longer step skips whatever runs between "
                "two step points and bills whatever covers one for the whole step."
            )
            raise PrometheusError(msg)

        retention = settings.get("retention")
        # None: read it from Prometheus. Set: trusted as is, for a store whose own
        # status does not describe the data it serves (Thanos, Mimir).
        self.retention_seconds: Optional[float] = (
            None if retention in (None, "") else duration_seconds(retention, "retention")
        )
        closed_hours = settings.get("closed_period_hours")
        closed_hours = DEFAULT_CLOSED_PERIOD_HOURS if closed_hours is None else float(closed_hours)
        if closed_hours < 0:
            msg = f"The 'closed_period_hours' setting must not be negative, got {closed_hours!r}"
            raise PrometheusError(msg)
        self.closed_period_seconds = closed_hours * SECONDS_PER_HOUR

        self.selectors = self._selectors(settings.get("selectors"))
        self.queries = self._queries(settings.get("queries"))
        self.transport = transport

        bearer_token = settings.get("bearer_token")
        username = settings.get("username")
        password = settings.get("password")
        if bearer_token and (username or password):
            # httpx's auth flow would overwrite the bearer header with basic auth,
            # so the credential that wins would not be the one an operator expects.
            msg = "Give Prometheus either 'bearer_token' or 'username'/'password', not both"
            raise PrometheusError(msg)
        self.auth: Optional[httpx.Auth] = None
        if username or password:
            if not (username and password):
                msg = (
                    "Prometheus basic auth needs both a username and a password; "
                    "only one was given"
                )
                raise PrometheusError(msg)
            self.auth = httpx.BasicAuth(str(username), str(password))
        self.headers = {"Accept": "application/json"}
        if bearer_token:
            self.headers["Authorization"] = f"Bearer {bearer_token}"

        if (bearer_token or username) and self.url.startswith("http://"):
            logger.warning(
                "Prometheus credentials are sent in clear text to %s; use https", self.url
            )
        if self.verify_ssl is False:
            logger.warning(
                "TLS certificate verification is off for Prometheus at %s", self.url
            )

        self._server_retention: object = _UNSET
        self._lowest_timestamp: object = _UNSET
        self._consecutive_timeouts = 0

    @staticmethod
    def _selectors(configured: Optional[dict]) -> dict[str, str]:
        selectors = dict(DEFAULT_SELECTORS)
        for name, selector in (configured or {}).items():
            name = str(name)
            if not _SELECTOR_NAME.match(name):
                msg = (
                    f"Selector name {name!r} must be lowercase letters, digits and "
                    "underscores; it is used as the placeholder {" + name + "_selector}"
                )
                raise PrometheusError(msg)
            selector = str(selector or "").strip().strip(",").strip()
            selectors[name] = selector or MATCH_ANY
        return selectors

    def _queries(self, configured: Optional[dict]) -> dict[str, str]:
        """Merge the overrides over the built-in queries and check every template.

        A template without ``{namespaces}`` would answer for every namespace on the
        cluster at once, and one naming an unknown placeholder would reach Prometheus
        as a parse error; both are refused when the backend is built.
        """
        queries = {
            **DEFAULT_QUERIES,
            **{
                str(component_type): str(template)
                for component_type, template in (configured or {}).items()
            },
        }
        known = _BASE_PLACEHOLDERS | {f"{name}_selector" for name in self.selectors}
        for component_type, template in queries.items():
            names = _placeholders_of(template)
            if "namespaces" not in names:
                msg = (
                    f"The Prometheus query for component type '{component_type}' must "
                    "select by namespace with the {namespaces} placeholder"
                )
                raise PrometheusError(msg)
            unknown = sorted(names - known)
            if unknown:
                msg = (
                    f"The Prometheus query for component type '{component_type}' uses "
                    f"unknown placeholder(s) {', '.join('{' + n + '}' for n in unknown)}"
                )
                raise PrometheusError(msg)
        return queries

    def placeholders(self) -> dict[str, str]:
        """The selector placeholders, filled the same way for every query."""
        return {f"{name}_selector": selector for name, selector in self.selectors.items()}

    def connect(self) -> httpx.Client:
        """Open a pooled HTTP client for one pass; use it as a context manager."""
        return httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            headers=self.headers,
            auth=self.auth,
            transport=self.transport,
        )

    def ping(self) -> bool:
        """Return True when Prometheus answers a query.

        The query API rather than ``/-/healthy``: the latter is unauthenticated, so a
        probe against it would report a token Prometheus rejects as healthy.
        """
        try:
            with self.connect() as session:
                payload = self._call(
                    session,
                    "POST",
                    "/api/v1/query",
                    {"query": "vector(1)", "time": f"{time.time():.3f}"},
                )
            if payload.get("status") != "success":
                msg = f"Prometheus did not answer the probe query: {payload!r:.{_MAX_ERROR_BODY}}"
                raise PrometheusError(msg)  # noqa: TRY301
        except PrometheusError as exc:
            logger.error("Prometheus ping against %s failed: %s", self.url, exc)
            return False
        return True

    def query(
        self, session: httpx.Client, expression: str, at: datetime.datetime
    ) -> dict[str, float]:
        """Evaluate one instant query at ``at`` and sum its answer per namespace.

        Raises:
            PrometheusQueryError: if Prometheus refused the query or its answer is
                unreadable.
            PrometheusUnavailableError: if Prometheus could not be reached, timed out
                or rejected the credentials.
        """
        try:
            payload = self._call(
                session,
                "POST",
                "/api/v1/query",
                {
                    "query": expression,
                    "time": f"{at.timestamp():.3f}",
                    # Prometheus gives up first, and says so, before the agent does.
                    "timeout": f"{max(int(self.timeout * SERVER_TIMEOUT_SHARE), 1)}s",
                },
            )
        except PrometheusQueryTimeoutError as exc:
            self._consecutive_timeouts += 1
            if self._consecutive_timeouts >= MAX_CONSECUTIVE_QUERY_TIMEOUTS:
                msg = f"{self._consecutive_timeouts} queries in a row timed out: {exc}"
                raise PrometheusUnavailableError(msg) from exc
            raise
        self._consecutive_timeouts = 0
        return _parse_result(payload)

    def _call(
        self,
        session: httpx.Client,
        method: str,
        path: str,
        data: Optional[dict[str, str]] = None,
    ) -> dict:
        """Send one request and return its decoded JSON object.

        The body is read in chunks, refused beyond ``MAX_RESPONSE_BYTES`` and abandoned
        once the request has taken longer than ``timeout`` in total: httpx's own
        timeout bounds each read, not a server that drips its answer.
        """
        deadline = time.monotonic() + self.timeout
        try:
            with session.stream(method, f"{self.url}{path}", data=data) as response:
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES:
                        msg = (
                            f"Prometheus's answer is larger than {MAX_RESPONSE_BYTES} bytes; "
                            "a per-namespace query should be far smaller"
                        )
                        raise PrometheusQueryError(msg)
                    if time.monotonic() > deadline:
                        msg = f"Prometheus took longer than {self.timeout:g}s to answer"
                        raise PrometheusUnavailableError(msg)
                    chunks.append(chunk)
                status = response.status_code
        except httpx.TimeoutException as exc:
            msg = f"Prometheus did not answer within {self.timeout:g}s: {exc}"
            raise PrometheusUnavailableError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"Prometheus request error: {exc}"
            raise PrometheusUnavailableError(msg) from exc

        body = b"".join(chunks)
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        if httpx.codes.is_success(status):
            if not isinstance(payload, dict):
                msg = "Prometheus answered with something that is not a JSON object"
                raise PrometheusUnavailableError(msg)
            return payload

        if isinstance(payload, dict) and payload.get("error"):
            reason = f"{payload.get('errorType') or 'error'}: {payload['error']}"
        else:
            reason = body.decode(errors="replace")
        reason = reason[:_MAX_ERROR_BODY]
        if status in (httpx.codes.BAD_REQUEST, httpx.codes.UNPROCESSABLE_ENTITY):
            # bad_data (a parse error) or execution: this query, not the server.
            msg = f"Prometheus refused the query: {status} {reason}"
            raise PrometheusQueryError(msg)
        if (
            status == httpx.codes.SERVICE_UNAVAILABLE
            and isinstance(payload, dict)
            and payload.get("errorType") == "timeout"
        ):
            msg = f"Prometheus timed out evaluating the query: {reason}"
            raise PrometheusQueryTimeoutError(msg)
        if status == httpx.codes.NOT_FOUND:
            msg = f"Prometheus has no {path} endpoint: {status} {reason}"
            raise PrometheusNotFoundError(msg)
        # 401/403 (credentials), 429, other 5xx: every query would meet the same answer.
        msg = f"Prometheus answered {status}: {reason}"
        raise PrometheusUnavailableError(msg)

    # ── What Prometheus holds ─────────────────────────────────────────────────

    def server_retention_seconds(self) -> Optional[float]:
        """The time-based retention Prometheus reports, or None when it has none.

        Read once from ``/api/v1/status/runtimeinfo`` and cached. A size-only
        retention (``512MiB``) has no time limit; the lowest timestamp covers it.
        A server without the endpoint is assumed to keep ``FALLBACK_RETENTION``.
        """
        if self._server_retention is _UNSET:
            with self.connect() as session:
                try:
                    payload = self._call(session, "GET", "/api/v1/status/runtimeinfo")
                    text = str(payload["data"]["storageRetention"])
                except (PrometheusNotFoundError, PrometheusQueryError, KeyError, TypeError) as exc:
                    logger.warning(
                        "Could not read the retention of Prometheus at %s (%s); assuming "
                        "%s. Set 'retention' in the prometheus settings.",
                        self.url,
                        exc,
                        FALLBACK_RETENTION,
                    )
                    text = FALLBACK_RETENTION
            durations = []
            for part in text.split(" or "):
                try:
                    durations.append(duration_seconds(part.strip(), "retention"))
                except PrometheusError:
                    continue  # a size limit such as 512MiB
            self._server_retention = min(durations) if durations else None
        return self._server_retention  # type: ignore[return-value]

    def lowest_timestamp(self) -> Optional[float]:
        """The Unix time of the oldest sample Prometheus holds, or None when unknown.

        Catches what the time-based retention cannot: a size-based retention that
        deleted more, and a server younger than its retention. Read once and cached:
        from the TSDB block list where the server has it, else from the server's own
        ``prometheus_tsdb_lowest_timestamp_seconds``, narrowed by the ``prometheus``
        selector. That is a scraped metric: its smallest value is used, so a forged
        one lower than the real one only disables this check. A forged one *higher*
        than every real one -- possible when the selector is left open and Prometheus
        does not scrape itself -- makes the current period look uncovered and stops
        its reporting, which is why the selector should name Prometheus's own job.
        """
        if self._lowest_timestamp is _UNSET:
            lowest: Optional[float] = None
            with self.connect() as session:
                try:
                    blocks = self._call(session, "GET", "/api/v1/status/tsdb/blocks")
                    times = [
                        float(block["minTime"]) / 1000
                        for block in (blocks.get("data") or {}).get("blocks") or []
                    ]
                    if not times:
                        head = self._call(session, "GET", "/api/v1/status/tsdb")
                        times = [float(head["data"]["headStats"]["minTime"]) / 1000]
                    lowest = min(times)
                except (
                    PrometheusNotFoundError,
                    PrometheusQueryError,
                    KeyError,
                    TypeError,
                    ValueError,
                ):
                    try:
                        values = self.query(
                            session,
                            "label_replace(min(prometheus_tsdb_lowest_timestamp_seconds{"
                            + self.selectors["prometheus"]
                            + '}), "namespace", "x", "", "")',
                            datetime.datetime.now(datetime.timezone.utc),
                        )
                        lowest = values.get("x")
                    except PrometheusQueryError:
                        lowest = None
            # An empty head reports a minimum in the far future.
            if lowest is not None and lowest > time.time():
                lowest = None
            self._lowest_timestamp = lowest
        return self._lowest_timestamp  # type: ignore[return-value]

    def covered_since(self, now: datetime.datetime) -> tuple[Optional[float], str]:
        """The earliest instant Prometheus is taken to hold in full, and why.

        Returns ``(unix_time, explanation)``; the time is None when nothing limits
        it. A configured ``retention`` is trusted as is. Otherwise the server's
        retention and its oldest sample are read, and the later of the two applies.

        Raises:
            PrometheusUnavailableError: if Prometheus cannot be asked.
        """
        floor: Optional[float] = None
        if self.retention_seconds is not None:
            floor = now.timestamp() - self.retention_seconds
            return floor, f"the configured retention of {format_duration(self.retention_seconds)}"
        reasons = []
        retention = self.server_retention_seconds()
        if retention is not None:
            floor = now.timestamp() - retention
            reasons.append(f"a retention of {format_duration(retention)}")
        lowest = self.lowest_timestamp()
        if lowest is not None:
            reasons.append(
                "an oldest sample from "
                + datetime.datetime.fromtimestamp(lowest, datetime.timezone.utc).isoformat()
            )
            floor = lowest if floor is None else max(floor, lowest)
        return floor, "Prometheus's own figures: " + (
            " and ".join(reasons) or "no time limit"
        )


def collect_usage(
    client: PrometheusClient,
    component_types: Sequence[str],
    namespaces: Sequence[str],
    start: datetime.datetime,
    end: datetime.datetime,
) -> tuple[dict[str, dict[str, float]], set[str]]:
    """Measure every component type for every namespace over one period.

    Returns ``({component_type: {namespace: native_value}}, failed_types)``. A
    namespace for which a query returned no series is absent from that component's
    dict, not present as zero.

    A component type with no query at all is left out of both: the caller reports it
    as zero. A query Prometheus refused is listed in ``failed_types`` and the other
    types are still asked for, so the log names every broken query.

    Raises:
        PrometheusUnavailableError: at the first transport error, timeout or rejected
            credential. The remaining types are not asked for: they would wait out
            the same timeout, one after another.
    """
    # From the timestamps, not ``end - start``: two datetimes sharing one ZoneInfo are
    # subtracted as wall-clock times, which makes a month that crosses a DST change an
    # hour short or long.
    window_seconds = max(int(end.timestamp() - start.timestamp()), MIN_WINDOW_SECONDS)
    step_seconds = max(int(client.step_seconds), 1)
    if step_seconds > window_seconds:
        # Clamping to the window instead would move the step grid, and put the one
        # point of a young period before the period's start.
        step_seconds = SHORT_WINDOW_STEP_SECONDS
    placeholders = {
        **client.placeholders(),
        "window": f"{window_seconds}s",
        "window_seconds": str(window_seconds),
        "step": f"{step_seconds}s",
        "step_seconds": str(step_seconds),
        "namespaces": namespace_pattern(namespaces),
    }

    values: dict[str, dict[str, float]] = {}
    failed: set[str] = set()
    with client.connect() as session:
        for component_type in component_types:
            template = client.queries.get(component_type)
            if template is None:
                logger.warning(
                    "No Prometheus query is configured for component type '%s'; its "
                    "usage will be reported as zero",
                    component_type,
                )
                continue
            try:
                values[component_type] = client.query(
                    session, render_query(template, placeholders), end
                )
            except PrometheusQueryTimeoutError as exc:
                logger.error(
                    "Prometheus timed out measuring component type '%s' for %s; not "
                    "asking for the other types in this pass: %s",
                    component_type,
                    ", ".join(namespaces),
                    exc,
                )
                failed.add(component_type)
                break
            except PrometheusQueryError as exc:
                logger.error(
                    "Prometheus could not measure component type '%s': %s",
                    component_type,
                    exc,
                )
                failed.add(component_type)
            except PrometheusUnavailableError as exc:
                logger.error(
                    "Prometheus became unavailable while measuring component type '%s': %s",
                    component_type,
                    exc,
                )
                raise
    return values, failed
