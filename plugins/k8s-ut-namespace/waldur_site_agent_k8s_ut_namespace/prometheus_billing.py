"""Prometheus-backed consumption metering for the k8s namespace backends.

Waldur bills a namespace by what it consumed in a billing period, and the cluster
already measures that: cAdvisor and kube-state-metrics expose it, and the monitoring
stack retains it. So the backend reads consumption from Prometheus rather than
reconstructing it from the namespace's quota, which is an entitlement and not a
measurement.

Everything is reported in the period's **native units** -- core-hours, GB-hours,
GPU-hours -- because those are what a price list is written against. Waldur's
``unit_factor`` converts from there.

One instant query per component type covers every namespace it was asked about: the
requested backend ids are folded into a single ``namespace=~"a|b|c"`` selector, so
measuring N namespaces for M components costs M requests, not N times M.

How many namespaces arrive together is the caller's business, not this module's.
``_get_usage_report`` is reached once per resource by the reporting processor, so a
pass currently measures one namespace at a time; the selector shape means a caller
that batches them pays the same M requests for the whole batch.

The queries are sent as ``POST /api/v1/query`` rather than GET. The namespace
selector grows with the offering, and a GET URL carrying a few hundred namespaces
runs into the URL limits of the proxies and load balancers in front of Prometheus;
the POST form body has no such bound. Both methods take the same parameters.
"""

from __future__ import annotations

import datetime
import math
import re
from typing import Optional, Sequence, Union

import httpx

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError

DEFAULT_TIMEOUT = 30.0
# Resolution of the period averages below. An hour is fine-grained enough for a
# month-long average to be stable and coarse enough that a month of them is a few
# hundred points per series rather than tens of thousands.
DEFAULT_STEP = "1h"

# A period that has only just opened has a window of seconds. Prometheus rejects a
# zero-length range selector and needs at least two samples to say anything about a
# counter, so the shortest window asked for is a minute; the answer is then a zero
# or a near-zero, which is what a month with a minute behind it really consumed.
MIN_WINDOW_SECONDS = 60

SECONDS_PER_HOUR = 3600
BYTES_PER_GIB = 1024**3

# ``{window}`` is the period as a PromQL duration, ``{window_seconds}`` the same as a
# bare number (PromQL cannot multiply by a duration), ``{step}`` the resolution of the
# subqueries that average a gauge over the period, and ``{namespaces}`` the
# alternation of requested backend ids.
#
# Templates are filled by literal replacement rather than ``str.format``: a PromQL
# selector is full of braces (``{namespace=~"..."}``), and ``format`` would read every
# one of them as a field. Literal replacement also means an operator can paste a
# query straight from the Prometheus UI and add the placeholders they want.
DEFAULT_QUERIES: dict[str, str] = {
    # Core-hours: the counter counts CPU-seconds, so the period's increase divided
    # by 3600 is the cores held for an hour. ``container!=""`` drops cAdvisor's
    # pod- and namespace-level rollup rows, which repeat the same seconds and would
    # otherwise be billed twice.
    "cpu": (
        "sum by (namespace) ("
        'increase(container_cpu_usage_seconds_total{namespace=~"{namespaces}", container!=""}'
        "[{window}])) / 3600"
    ),
    # GB-hours: the working set is a gauge, so the memory held over the period is its
    # mean times the period's length. Averaged *after* the per-namespace sum, so a
    # container that starts mid-period pulls the mean down instead of being credited
    # with the whole period.
    "ram": (
        "avg_over_time((sum by (namespace) ("
        'container_memory_working_set_bytes{namespace=~"{namespaces}", container!=""}'
        "))[{window}:{step}]) * {window_seconds} / 1073741824 / 3600"
    ),
    # GB-hours, billed by what is stored rather than by the capacity granted: a
    # namespace that reserves 100Gi and stores 2Gi consumed 2Gi.
    "storage": (
        "avg_over_time((sum by (namespace) ("
        'kubelet_volume_stats_used_bytes{namespace=~"{namespaces}"}'
        "))[{window}:{step}]) * {window_seconds} / 1073741824 / 3600"
    ),
    # GPU-hours, from the requests the scheduler actually honours: kube-state-metrics
    # normalizes the extended resource name to ``nvidia_com_gpu``.
    "gpu": (
        "avg_over_time((sum by (namespace) ("
        "kube_pod_container_resource_requests"
        '{namespace=~"{namespaces}", resource="nvidia_com_gpu"}'
        "))[{window}:{step}]) * {window_seconds} / 3600"
    ),
}

# The characters RE2 reads as pattern syntax. ``re.escape`` is not used directly: it
# also escapes ``-`` and other punctuation, which turns a plain namespace into
# ``waldur\-foo`` and makes the query that reached Prometheus hard to read back in a log.
_REGEX_METACHARACTERS = re.compile(r"([\\.^$*+?{}\[\]|()])")

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


class PrometheusError(BackendError):
    """Prometheus refused a query or could not be reached."""


def verify_ssl_setting(settings: dict) -> bool:
    """Read ``ssl_verify``, defaulting to on.

    ``.get("ssl_verify", True)`` only covers an absent key: a bare ``ssl_verify:`` in
    the YAML is *present* and ``None``, which ``bool()`` reads as False and silently
    turns certificate verification off. Verification is not something to lose to a
    stray colon, so only an explicit false value disables it.
    """
    verify = settings.get("ssl_verify")
    return True if verify is None else bool(verify)


def timeout_setting(settings: dict) -> float:
    """Read ``timeout``, defaulting when it is absent or explicitly null.

    Same trap as ``ssl_verify``: a present-but-null value reaches ``float(None)`` and
    raises, taking the backend down at construction over an empty config line.
    """
    timeout = settings.get("timeout")
    return DEFAULT_TIMEOUT if timeout is None else float(timeout)


def duration_seconds(value: Union[str, int, float, None]) -> float:
    """Read a PromQL duration (``1h30m``) or a plain number of seconds.

    PromQL durations are what an operator writes in a query, so accepting the same
    spelling keeps the setting and the queries it feeds in one language.

    Raises:
        PrometheusError: if the value is not a duration this can read, naming it so
            the configuration mistake is reported rather than guessed at.
    """
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        msg = "The 'step' setting must be a duration such as '1h'"
        raise PrometheusError(msg)

    position = 0
    total = 0.0
    for match in _DURATION_PART.finditer(text):
        if match.start() != position:
            break
        total += float(match.group(1)) * _DURATION_UNITS[match.group(2)]
        position = match.end()
    if position != len(text) or not total:
        msg = f"The 'step' setting must be a PromQL duration such as '1h', got {text!r}"
        raise PrometheusError(msg)
    return total


def render_query(template: str, placeholders: dict[str, str]) -> str:
    """Fill the placeholders a query template names, leaving PromQL's braces alone."""
    for name, value in placeholders.items():
        template = template.replace("{" + name + "}", value)
    return template


def namespace_pattern(namespaces: Sequence[str]) -> str:
    """Fold the requested backend ids into one Prometheus regex alternation.

    Prometheus anchors a matcher's regex at both ends, so a bare alternation matches
    exactly the namespaces asked about. Each id is escaped: a namespace is an RFC 1123
    label today, but the selector must not become a pattern if that ever changes.
    """
    return "|".join(_REGEX_METACHARACTERS.sub(r"\\\1", n) for n in namespaces)


def _series_value(raw: object) -> Optional[float]:
    """Read one series' value, or None when it says nothing measurable.

    A counter that Prometheus could not evaluate comes back as ``NaN``; summing it
    would turn a whole namespace's figure into ``NaN`` and fail the report.
    """
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _parse_result(payload: object, namespaces: Sequence[str]) -> dict[str, float]:
    """Sum one query's answer per namespace.

    A namespace with no series is absent from the result rather than present as zero;
    the caller reports the zeros, because only it knows which namespaces were asked
    about.

    A series without a ``namespace`` label is attributed to the only namespace
    requested when there is exactly one -- that is the shape of an override written as
    a plain ``sum(...)``, which drops the label -- and is otherwise ignored with a
    warning, since there is no honest way to say which namespace it belongs to.
    """
    if not isinstance(payload, dict):
        msg = "Prometheus returned an unexpected payload"
        raise PrometheusError(msg)
    if payload.get("status") != "success":
        reason = payload.get("error") or payload.get("status")
        msg = f"Prometheus rejected the query: {reason}"
        raise PrometheusError(msg)

    data = payload.get("data")
    data = data if isinstance(data, dict) else {}
    result = data.get("result")
    result_type = data.get("resultType")

    if result_type == "scalar":
        if len(namespaces) != 1:
            msg = (
                "Prometheus answered with a bare scalar, which cannot be attributed "
                "to more than one namespace"
            )
            raise PrometheusError(msg)
        value = _series_value((result or [None, None])[1])
        return {namespaces[0]: value} if value is not None else {}
    if result_type != "vector":
        msg = f"Prometheus answered with an unexpected result type {result_type!r}"
        raise PrometheusError(msg)

    totals: dict[str, float] = {}
    for series in result or []:
        if not isinstance(series, dict):
            continue
        metric = series.get("metric")
        metric = metric if isinstance(metric, dict) else {}
        value = _series_value((series.get("value") or [None, None])[1])
        if value is None:
            continue
        namespace = metric.get("namespace")
        if not namespace:
            if len(namespaces) != 1:
                logger.warning(
                    "Ignoring a Prometheus series with no namespace label: it cannot "
                    "be attributed to one of %d requested namespaces",
                    len(namespaces),
                )
                continue
            namespace = namespaces[0]
        namespace = str(namespace)
        totals[namespace] = totals.get(namespace, 0.0) + value
    return totals


class PrometheusClient:
    """Reads period consumption from a Prometheus HTTP API."""

    def __init__(self, settings: dict) -> None:
        """Initialize the client from the offering's ``prometheus`` settings block."""
        url = settings.get("url")
        if not url:
            msg = "Prometheus billing requires 'url' in the prometheus settings"
            raise PrometheusError(msg)

        self.url = str(url).rstrip("/")
        self.verify_ssl = verify_ssl_setting(settings)
        self.timeout = timeout_setting(settings)
        self.step_seconds = duration_seconds(settings.get("step") or DEFAULT_STEP)
        # Merged over the defaults so an offering overrides the query for the one
        # component its cluster measures differently and inherits the rest.
        self.queries: dict[str, str] = {
            **DEFAULT_QUERIES,
            **{
                str(component_type): str(template)
                for component_type, template in (settings.get("queries") or {}).items()
            },
        }

        bearer_token = settings.get("bearer_token")
        username = settings.get("username")
        password = settings.get("password")
        if username or password:
            if not (username and password):
                msg = (
                    "Prometheus basic auth needs both 'username' and 'password'; "
                    "only one was given"
                )
                raise PrometheusError(msg)
            auth: Optional[httpx.Auth] = httpx.BasicAuth(str(username), str(password))
        else:
            auth = None
        headers = {"Accept": "application/json"}
        if bearer_token:
            # A bearer token wins over basic auth when an offering carries both: it
            # is the narrower credential, and it is the one an operator adds last.
            headers["Authorization"] = f"Bearer {bearer_token}"

        # One pooled client for the instance: a reporting pass runs one query per
        # component type, and a fresh client per query would pay a TCP and TLS
        # handshake for each.
        self.session = httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            headers=headers,
            auth=auth,
        )

    def close(self) -> None:
        """Release the pooled connections."""
        self.session.close()

    def ping(self) -> bool:
        """Return True when Prometheus answers a query.

        The query API rather than ``/-/healthy``: the latter is unauthenticated, so a
        probe against it would report the backend up with a token Prometheus rejects,
        and every usage report would then fail instead of the backend staying pending.
        """
        try:
            self._request("vector(1)", datetime.datetime.now(datetime.timezone.utc))
        except PrometheusError:
            logger.exception("Prometheus ping failed")
            return False
        return True

    def query(
        self,
        expression: str,
        at: datetime.datetime,
        namespaces: Sequence[str] = (),
    ) -> dict[str, float]:
        """Evaluate one instant query at ``at`` and sum its answer per namespace.

        Args:
            expression: the PromQL to run.
            at: the instant the query is evaluated at, normally the period's end.
            namespaces: the backend ids the expression is expected to report on,
                used to attribute a series that carries no ``namespace`` label.

        Raises:
            PrometheusError: on a transport failure, a refused query or a body this
                cannot read.
        """
        return _parse_result(self._request(expression, at), namespaces)

    def _request(self, expression: str, at: datetime.datetime) -> object:
        """Run one instant query and return its decoded body.

        Raises:
            PrometheusError: on a transport failure, a refused query or a body this
                cannot read.
        """
        try:
            response = self.session.post(
                f"{self.url}/api/v1/query",
                data={"query": expression, "time": f"{at.timestamp():.3f}"},
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            msg = f"Prometheus query failed: {exc.response.status_code} {exc.response.text}"
            raise PrometheusError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"Prometheus query request error: {exc}"
            raise PrometheusError(msg) from exc
        except ValueError as exc:
            msg = f"Prometheus returned invalid JSON: {exc}"
            raise PrometheusError(msg) from exc


def collect_usage(
    client: PrometheusClient,
    component_types: Sequence[str],
    namespaces: Sequence[str],
    start: datetime.datetime,
    end: datetime.datetime,
) -> tuple[dict[str, dict[str, float]], set[str]]:
    """Measure every component type for every namespace over one period.

    Returns ``({component_type: {namespace: native_value}}, failed_types)``.

    A component type with no query at all is left out of both: the caller reports it
    as zero, which is what an unmetered component consumed as far as this backend can
    tell. A query that *failed* is listed in ``failed_types``, because a zero would be
    a claim the backend cannot make -- a failed measurement and an idle namespace look
    identical otherwise, and reporting the first as the second would overwrite a
    period's accrued usage with nothing.

    One failure never aborts the pass: the remaining component types are still
    measured, and the caller decides what to do with the resources it cannot bill.

    Args:
        client: the Prometheus client to query through.
        component_types: the component types to measure, in any order.
        namespaces: the backend ids to measure them for.
        start: the beginning of the billing period, inclusive.
        end: the end of the period, exclusive, and the instant the queries run at.
    """
    window_seconds = max(int((end - start).total_seconds()), MIN_WINDOW_SECONDS)
    # Clamped to the window: a subquery shorter than its own step is one point, and
    # Prometheus refuses a range that does not contain a step.
    step_seconds = max(min(int(client.step_seconds), window_seconds), 1)
    placeholders = {
        "window": f"{window_seconds}s",
        "window_seconds": str(window_seconds),
        "step": f"{step_seconds}s",
        "namespaces": namespace_pattern(namespaces),
    }

    values: dict[str, dict[str, float]] = {}
    failed: set[str] = set()
    for component_type in component_types:
        template = client.queries.get(component_type)
        if template is None:
            logger.warning(
                "No Prometheus query is configured for component type '%s'; its usage "
                "will be reported as zero",
                component_type,
            )
            continue
        try:
            values[component_type] = client.query(
                render_query(template, placeholders), end, namespaces
            )
        except PrometheusError as exc:
            logger.error(
                "Prometheus could not measure component type '%s': %s",
                component_type,
                exc,
            )
            failed.add(component_type)
    return values, failed
