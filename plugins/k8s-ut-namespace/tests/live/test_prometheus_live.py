"""The built-in queries against a real Prometheus holding generate_openmetrics.py's data.

Skipped unless ``K8S_NAMESPACE_PROMETHEUS_URL`` names such a Prometheus: what is tested
here is PromQL's behaviour -- step points, period boundaries, joins, deduplication --
which no stub can stand in for. To run it::

    python tests/live/generate_openmetrics.py /tmp/synthetic.om
    promtool tsdb create-blocks-from openmetrics /tmp/synthetic.om ./data
    prometheus --storage.tsdb.path=./data --storage.tsdb.retention.time=400d
    K8S_NAMESPACE_PROMETHEUS_URL=http://localhost:9090 uv run pytest tests/live/
"""

from __future__ import annotations

import datetime
import os
from unittest.mock import MagicMock, patch

import pytest

from waldur_site_agent_k8s_ut_namespace.backend import K8sUtNamespaceBackend
from waldur_site_agent_k8s_ut_namespace.prometheus_billing import (
    PrometheusClient,
    PrometheusCoverageError,
    collect_usage,
)

URL = os.environ.get("K8S_NAMESPACE_PROMETHEUS_URL")
pytestmark = pytest.mark.skipif(
    not URL, reason="K8S_NAMESPACE_PROMETHEUS_URL is not set"
)

UTC = datetime.timezone.utc
OCTOBER = (datetime.datetime(2026, 10, 1, tzinfo=UTC), datetime.datetime(2026, 11, 1, tzinfo=UTC))
COMPONENTS = {
    name: {"type": name, "unit_factor": 1, "accounting_type": "usage"}
    for name in ("cpu", "ram", "storage", "gpu")
}
NAMESPACES = [
    "waldur-full",
    "waldur-half",
    "waldur-spoof",
    "waldur-rwx",
    "waldur-gpu",
    "waldur-burst",
    "waldur-straddle",
]


@pytest.fixture(scope="module")
def october():
    with patch("waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient") as k8s:
        k8s.return_value = MagicMock()
        backend = K8sUtNamespaceBackend(
            {"prometheus": {"url": URL, "closed_period_hours": 100_000}}, COMPONENTS
        )
    backend.timezone = "UTC"
    report = backend.get_usage_report_for_period(NAMESPACES, 2026, 10)
    return {ns: usage["TOTAL_ACCOUNT_USAGE"] for ns, usage in report.items()}


def test_whole_and_half_month_namespaces(october):
    assert october["waldur-full"] == {"cpu": 1488.0, "ram": 2976.0, "storage": 7440.0, "gpu": 0.0}
    assert october["waldur-half"] == {"cpu": 1536.0, "ram": 1536.0, "storage": 0.0, "gpu": 384.0}


def test_negative_series_cancel_nothing(october):
    """A tenant's own exporter is not the pinned job; a spoofed one is clamped at zero."""
    assert october["waldur-spoof"]["ram"] == 1488.0


def test_a_claim_mounted_on_two_nodes_counts_once(october):
    assert october["waldur-rwx"]["storage"] == 3720.0


def test_gpus_are_billed_per_pod_while_running(october):
    """train-0 holds 4 (its init container) all month, done-0 holds 2 for 240 h."""
    assert october["waldur-gpu"]["gpu"] == 4 * 744 + 2 * 240


def test_sub_step_jobs_are_billed_by_the_minute(october):
    """An hourly step bills the :10-:40 job nothing and the :55-:05 one six times over.

    The GPU figures are exact: the pod's phase turns from Running when the job ends.
    The memory series simply stop, and without the staleness marker a live scrape
    writes, Prometheus's 5-minute lookback adds 4 one-minute points per pod.
    """
    assert october["waldur-burst"]["gpu"] == 744 * 0.5
    assert october["waldur-straddle"]["gpu"] == round((744 * 10 - 5) / 60, 2)
    assert october["waldur-burst"]["ram"] == round(2 * 744 * 34 / 60, 2)
    assert october["waldur-straddle"]["ram"] == round((744 * 10 - 5 + 743 * 4) / 60, 2)


def test_every_default_query_parses_with_escaped_namespaces():
    client = PrometheusClient({"url": URL})
    values, failed = collect_usage(
        client, ["cpu", "ram", "storage", "gpu"], ["a.b", 'x"y', "waldur-full"], *OCTOBER
    )
    assert failed == set()
    assert values["cpu"] == {"waldur-full": 1488.0}


def test_a_month_before_the_oldest_sample_is_refused():
    client = PrometheusClient({"url": URL})
    floor, _ = client.covered_since(datetime.datetime.now(UTC))
    assert floor is not None
    assert floor >= OCTOBER[0].timestamp()
    with patch("waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"):
        backend = K8sUtNamespaceBackend(
            {"prometheus": {"url": URL, "closed_period_hours": 100_000}}, COMPONENTS
        )
    backend.timezone = "UTC"
    with pytest.raises(PrometheusCoverageError):
        backend.get_usage_report_for_period(NAMESPACES, 2026, 9)
