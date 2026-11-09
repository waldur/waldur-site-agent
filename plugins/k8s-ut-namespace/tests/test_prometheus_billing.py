"""Consumption billing from Prometheus, over a stubbed HTTP transport.

Nothing here touches the network: every request is answered by an
``httpx.MockTransport`` handler that records it, so each test can assert both the
figure reported and the PromQL that produced it. What PromQL itself does with the
queries -- step points, boundaries, joins -- is checked against a real Prometheus in
``tests/live/``.
"""

from __future__ import annotations

import copy
import datetime
import logging
import ssl
from typing import Any
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import certifi
import httpx
import pytest

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.structures import RootConfiguration
from waldur_site_agent_k8s_ut_namespace import prometheus_billing
from waldur_site_agent_k8s_ut_namespace.backend import K8sUtNamespaceBackend
from waldur_site_agent_k8s_ut_namespace.prometheus_billing import (
    DEFAULT_QUERIES,
    MATCH_ANY,
    PrometheusClient,
    PrometheusCoverageError,
    PrometheusError,
    PrometheusPeriodClosedError,
    PrometheusQueryError,
    PrometheusUnavailableError,
    collect_usage,
    duration_seconds,
    namespace_pattern,
    render_query,
)

TZ = "Europe/Tallinn"
# A fixed instant so the period bounds, and the ``time`` parameter derived from them,
# are asserted rather than observed.
NOW = datetime.datetime(2025, 3, 15, 12, 0, tzinfo=ZoneInfo(TZ))
NOW_SEAM = "waldur_site_agent_k8s_ut_namespace.backend.backend_utils.get_current_time_in_timezone"
URL = "https://prometheus.example.org"
# Long enough that every month of 2025 is still "within" both windows unless a test
# says otherwise.
FOREVER = {"retention": "100y", "closed_period_hours": 1_000_000}


def make_backend(settings, components):
    """Create a backend with mocked K8s and Keycloak clients."""
    with patch(
        "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
    ) as mock_k8s, patch("waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"):
        mock_k8s.return_value = MagicMock()
        return K8sUtNamespaceBackend(settings, components)


def _vector(*series: tuple) -> httpx.Response:
    """A Prometheus vector answer from ``(labels, value)`` pairs."""
    return httpx.Response(
        200,
        json={
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {"metric": dict(labels), "value": [1_742_000_000.0, str(value)]}
                    for labels, value in series
                ],
            },
        },
    )


def _ns(namespace: str, value: Any) -> tuple:
    return ({"namespace": namespace}, value)


def _form(request: httpx.Request) -> dict[str, str]:
    return dict(httpx.QueryParams(request.content.decode()))


class Prometheus:
    """A stand-in Prometheus whose answers each test sets up explicitly."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.routes: dict[str, httpx.Response] = {}
        self.paths: dict[str, httpx.Response] = {}
        self.default = _vector()

    def answer(self, needle: str, response: httpx.Response):
        """Answer every query whose PromQL contains ``needle`` with ``response``."""
        self.routes[needle] = response

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path in self.paths:
            return self.paths[request.url.path]
        if request.url.path != "/api/v1/query":
            return httpx.Response(404, text="404 page not found")
        expression = _form(request)["query"]
        for needle, response in self.routes.items():
            if needle in expression:
                return response
        return self.default

    def transport(self) -> httpx.MockTransport:
        # Through a lambda so a test can still replace ``handler``.
        return httpx.MockTransport(lambda request: self.handler(request))

    def queries(self) -> list[str]:
        return [_form(r)["query"] for r in self.requests if r.url.path == "/api/v1/query"]

    def query_for(self, needle: str) -> str:
        return next(q for q in self.queries() if needle in q)

    def times(self) -> list[float]:
        return [
            float(_form(r)["time"]) for r in self.requests if r.url.path == "/api/v1/query"
        ]


@pytest.fixture
def prometheus():
    return Prometheus()


def _client(prometheus, **settings) -> PrometheusClient:
    """A client wired to the stub, with ``settings`` laid over a working default."""
    return PrometheusClient(
        {"url": URL, "bearer_token": "t", **settings}, transport=prometheus.transport()
    )


def _components(**overrides):
    """The plugin's four component types billed by usage, ``overrides`` as unit factors."""
    components = {
        "cpu": {"type": "cpu", "unit_factor": 1, "accounting_type": "usage"},
        "ram": {"type": "ram", "unit_factor": 1, "accounting_type": "usage"},
        "storage": {"type": "storage", "unit_factor": 1, "accounting_type": "usage"},
        "gpu": {"type": "gpu", "unit_factor": 1, "accounting_type": "usage"},
    }
    for name, unit_factor in overrides.items():
        components[name]["unit_factor"] = unit_factor
    return components


@pytest.fixture
def backend_factory(backend_settings_no_keycloak):
    """Build a backend whose Prometheus client answers from the stub."""

    def build(prometheus, components=None, **settings):
        backend_settings = dict(backend_settings_no_keycloak)
        backend_settings["prometheus"] = {
            "url": URL,
            "bearer_token": "t",
            **FOREVER,
            **settings,
        }
        backend = make_backend(
            backend_settings, components if components is not None else _components()
        )
        backend.prometheus_client.transport = prometheus.transport()
        backend.timezone = TZ
        return backend

    return build


def _resource(backend_id="waldur-a"):
    resource = MagicMock(backend_id=backend_id, slug="a")
    resource.name = "a"
    return resource


class TestInert:
    """Without a ``prometheus`` block nothing is reported and nothing is queried."""

    def test_nothing_is_reported_or_queried(
        self, backend_settings_no_keycloak, backend_components
    ):
        with patch.object(prometheus_billing.httpx, "Client") as http_client:
            backend = make_backend(backend_settings_no_keycloak, backend_components)
            backend.timezone = TZ
            backend.k8s_client.ping.return_value = True

            assert backend.prometheus_client is None
            assert backend._get_usage_report(["waldur-a"]) == {}
            assert backend.get_usage_report_for_period(["waldur-a"], 2025, 3) == {}
            assert backend.ping() is True
        http_client.assert_not_called()

    def test_configured_block_is_built(self, backend_settings_no_keycloak, backend_components):
        settings = dict(backend_settings_no_keycloak)
        settings["prometheus"] = {"url": URL + "/"}
        backend = make_backend(settings, backend_components)

        assert backend.prometheus_client.url == URL

    @pytest.mark.parametrize("block", [{"bearer_token": "t"}, {}, None])
    def test_unusable_block_raises_rather_than_disabling_billing(
        self, backend_settings_no_keycloak, backend_components, block
    ):
        """A block with no ``url`` -- or no keys at all -- must not read as "billing off"."""
        settings = dict(backend_settings_no_keycloak)
        settings["prometheus"] = block
        with pytest.raises(PrometheusError, match="'url'"):
            make_backend(settings, backend_components)


class TestComponents:
    """Usage-billed components are measured by type and divided by ``unit_factor``."""

    def test_each_component_is_converted(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 7200)))
        prometheus.answer("container_memory_working_set_bytes", _vector(_ns("waldur-a", 100)))
        prometheus.answer("kubelet_volume_stats_used_bytes", _vector(_ns("waldur-a", 50)))
        prometheus.answer("kube_pod_container_resource_requests", _vector(_ns("waldur-a", 4)))
        backend = backend_factory(
            prometheus, components=_components(cpu=3600, ram=2, storage=0.5, gpu=1)
        )

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 2.0,
            "ram": 50.0,
            "storage": 100.0,
            "gpu": 4.0,
        }

    def test_default_unit_factor_is_one(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3.5)))
        backend = backend_factory(
            prometheus, components={"cpu": {"type": "cpu", "accounting_type": "usage"}}
        )

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {"cpu": 3.5}

    def test_component_name_and_type_are_distinguished(self, prometheus, backend_factory):
        """A renamed component is metered by its type and reported under its name."""
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3600)))
        prometheus.answer("kube_pod_container_resource_requests", _vector(_ns("waldur-a", 12)))
        backend = backend_factory(
            prometheus,
            components={
                "vCPU hours": {"type": "cpu", "unit_factor": 1, "accounting_type": "usage"},
                "accelerator": {"type": "gpu", "unit_factor": 4, "accounting_type": "usage"},
            },
        )

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {
            "vCPU hours": 3600.0,
            "accelerator": 3.0,
        }
        # One query per type, not per name.
        assert len(prometheus.queries()) == 2

    def test_limit_components_are_neither_measured_nor_reported(
        self, prometheus, backend_factory
    ):
        """1488 core-hours under a two-core limit component would only mislead."""
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 1488)))
        prometheus.answer("container_memory_working_set_bytes", _vector(_ns("waldur-a", 2)))
        backend = backend_factory(
            prometheus,
            components={
                "cpu": {"type": "cpu", "unit_factor": 1, "accounting_type": "limit"},
                "ram": {"type": "ram", "unit_factor": 1, "accounting_type": "usage"},
            },
        )

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {"ram": 2.0}
        assert not any("container_cpu_usage_seconds_total" in q for q in prometheus.queries())

    def test_offering_with_only_limit_components_queries_nothing(
        self, prometheus, backend_factory, backend_components
    ):
        backend = backend_factory(prometheus, components=backend_components)

        assert backend.get_usage_report_for_period(["waldur-a"], 2025, 2) == {}
        assert prometheus.requests == []

    def test_component_without_a_query_reports_zero(self, prometheus, backend_factory, caplog):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3600)))
        backend = backend_factory(
            prometheus,
            components={
                "cpu": {"type": "cpu", "accounting_type": "usage"},
                "mystery": {"type": "mystery", "accounting_type": "usage"},
            },
        )

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {"cpu": 3600.0, "mystery": 0.0}
        assert any(
            "No Prometheus query is configured for component type 'mystery'" in r.getMessage()
            for r in caplog.records
        )


class TestNoData:
    """No series means no report; a missing component beside others is a real zero."""

    def test_namespace_without_any_series_is_left_out(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3600)))
        backend = backend_factory(prometheus)

        report = backend.get_usage_report_for_period(["waldur-a", "waldur-quiet"], 2025, 2)

        assert "waldur-quiet" not in report
        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 3600.0,
            "ram": 0.0,
            "storage": 0.0,
            "gpu": 0.0,
        }

    def test_period_with_no_series_at_all_is_empty(self, prometheus, backend_factory):
        """The processor's "no historical data, skip" path, not a month of zeros."""
        backend = backend_factory(prometheus)

        assert backend.get_usage_report_for_period(["waldur-a"], 2025, 2) == {}
        assert len(prometheus.queries()) == 4

    def test_current_period_with_no_series_falls_back_to_an_empty_total(
        self, prometheus, backend_factory
    ):
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        with patch(NOW_SEAM, return_value=NOW):
            assert backend._get_usage_report(["waldur-a"]) == {}
            info = backend.pull_resource(_resource())

        assert info.usage == {"TOTAL_ACCOUNT_USAGE": {}}

    def test_a_zero_series_is_data(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 0)))
        backend = backend_factory(prometheus)

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 0.0

    def test_nan_is_dropped(self, prometheus, backend_factory):
        prometheus.answer(
            "container_cpu_usage_seconds_total",
            _vector(_ns("waldur-a", "NaN"), _ns("waldur-a", 3600), _ns("waldur-b", "NaN")),
        )
        backend = backend_factory(prometheus)

        report = backend.get_usage_report_for_period(["waldur-a", "waldur-b"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 3600.0
        assert "waldur-b" not in report

    def test_negative_values_are_dropped(self, prometheus, backend_factory, caplog):
        """A negative answer (from an override) would lower a bill."""
        prometheus.answer(
            "container_cpu_usage_seconds_total",
            _vector(_ns("waldur-a", 3600), _ns("waldur-a", -3600)),
        )
        backend = backend_factory(prometheus)

        report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 3600.0
        assert any("negative" in r.getMessage() for r in caplog.records)

    def test_unlabelled_series_are_dropped(self, prometheus, backend_factory, caplog):
        """Even with one namespace requested: guessing could bill someone else's usage."""
        prometheus.answer("container_cpu_usage_seconds_total", _vector(({}, 7)))
        backend = backend_factory(prometheus)

        assert backend.get_usage_report_for_period(["waldur-a"], 2025, 2) == {}
        assert any("no namespace label" in r.getMessage() for r in caplog.records)

    def test_scalar_answer_is_a_failed_query(self, prometheus, backend_factory):
        prometheus.answer(
            "container_cpu_usage_seconds_total",
            httpx.Response(
                200,
                json={"status": "success", "data": {"resultType": "scalar", "result": [0, "1"]}},
            ),
        )
        backend = backend_factory(prometheus)

        with pytest.raises(PrometheusQueryError, match="cpu"):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)


class TestQueries:
    """The built-in queries pin their source, clamp at zero and integrate the gauges."""

    @staticmethod
    def _render(component_type, client=None, namespaces=("waldur-a",)):
        client = client or PrometheusClient({"url": URL})
        return render_query(
            client.queries[component_type],
            {
                **client.placeholders(),
                "namespaces": namespace_pattern(namespaces),
                "window": "2419200s",
                "window_seconds": "2419200",
                "step": "60s",
                "step_seconds": "60",
            },
        )

    def test_cadvisor_queries_pin_the_source_and_drop_rollups(self):
        for component_type in ("cpu", "ram"):
            query = self._render(component_type)
            assert 'job="kubelet", metrics_path="/metrics/cadvisor"' in query
            assert 'container!=""' in query
            assert 'image!=""' in query

    def test_storage_counts_each_claim_once(self):
        query = self._render("storage")
        assert 'kubelet_volume_stats_used_bytes{namespace=~"waldur-a", job="kubelet", metrics_path="/metrics"}' in query
        assert "max by (namespace, persistentvolumeclaim)" in query

    def test_gpu_is_held_per_pod_while_running(self):
        query = self._render("gpu")
        assert "kube_pod_init_container_resource_requests" in query
        assert "max by (namespace, pod, container)" in query
        assert 'phase="Running"}) == 1)' in query
        assert query.count('job="kube-state-metrics"') == 3

    def test_every_input_series_is_clamped_at_zero(self):
        for component_type in DEFAULT_QUERIES:
            query = self._render(component_type)
            metrics = query.count("{namespace=~")
            # The GPU phase filter is a 0/1 comparison, not a figure.
            expected = metrics - (1 if component_type == "gpu" else 0)
            assert query.count("clamp_min(") == expected, component_type

    @pytest.mark.parametrize("component_type", ["ram", "storage", "gpu"])
    def test_gauges_are_integrated_over_steps(self, component_type):
        query = self._render(component_type)
        assert query.startswith("sum_over_time(")
        assert "avg_over_time" not in query
        # The points fall on [start, end): each stands for the step after it.
        assert "[2419200s:60s] offset 60s) * 60 / 3600" in query

    def test_selectors_can_be_overridden(self):
        client = PrometheusClient(
            {"url": URL, "selectors": {"kube_state_metrics": 'job="ksm", cluster="a"'}}
        )

        query = self._render("gpu", client)

        assert 'job="ksm", cluster="a"' in query
        assert 'job="kube-state-metrics"' not in query
        # The others keep their defaults.
        assert 'metrics_path="/metrics/cadvisor"' in self._render("cpu", client)

    def test_empty_selector_matches_any_source(self):
        client = PrometheusClient({"url": URL, "selectors": {"cadvisor": ""}})

        assert client.selectors["cadvisor"] == MATCH_ANY
        assert f'{{namespace=~"waldur-a", {MATCH_ANY}, container!=""' in self._render("cpu", client)

    def test_extra_selectors_become_placeholders(self):
        client = PrometheusClient(
            {
                "url": URL,
                "selectors": {"dcgm": 'job="dcgm"'},
                "queries": {"gpu": 'sum by (namespace) (x{namespace=~"{namespaces}", {dcgm_selector}})'},
            }
        )

        assert 'x{namespace=~"waldur-a", job="dcgm"}' in self._render("gpu", client)

    @pytest.mark.parametrize(
        ("queries", "match"),
        [
            ({"cpu": "sum by (namespace) (custom_cpu)"}, "{namespaces}"),
            ({"cpu": 'sum by (namespace) (x{namespace=~"{namespaces}", {nope_selector}})'}, "nope_selector"),
        ],
    )
    def test_unusable_templates_are_refused(self, queries, match):
        with pytest.raises(PrometheusError, match=match):
            PrometheusClient({"url": URL, "queries": queries})

    def test_bad_selector_name_is_refused(self):
        with pytest.raises(PrometheusError, match="Selector name"):
            PrometheusClient({"url": URL, "selectors": {"Bad-Name": 'job="x"'}})

    def test_override_composes_like_a_default(self, prometheus, backend_factory):
        backend = backend_factory(
            prometheus,
            components={"cpu": {"type": "cpu", "accounting_type": "usage"}},
            step="5m",
            queries={
                "cpu": (
                    'sum by (namespace) (custom_cpu{namespace=~"{namespaces}", {cadvisor_selector}}'
                    "[{window}:{step}]) * {step_seconds} / {window_seconds}"
                )
            },
        )

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert prometheus.query_for("custom_cpu") == (
            'sum by (namespace) (custom_cpu{namespace=~"waldur-a", job="kubelet", '
            'metrics_path="/metrics/cadvisor"}[2419200s:300s]) * 300 / 2419200'
        )

    def test_namespaces_are_escaped_for_re2_and_then_for_promql(self):
        """``\\.`` alone inside a PromQL string is an invalid escape, ``"`` ends it."""
        assert namespace_pattern(["waldur-a", "waldur.b"]) == r"waldur-a|waldur\\.b"
        assert namespace_pattern(['a"b']) == r"a\"b"

    def test_invalid_namespace_names_are_not_queried(self, prometheus, backend_factory, caplog):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 1)))
        backend = backend_factory(prometheus)

        report = backend.get_usage_report_for_period(
            ["waldur-a", 'x"}) or vector(1', "Upper", "a" * 64], 2025, 2
        )

        assert set(report) == {"waldur-a"}
        assert all('namespace=~"waldur-a"' in q for q in prometheus.queries())
        assert sum("not a valid namespace name" in r.getMessage() for r in caplog.records) == 3

    def test_render_leaves_promql_braces_alone(self):
        rendered = render_query(
            'x{namespace=~"{namespaces}", container!=""}[{window}:{step}] * {step_seconds}',
            {"namespaces": "a|b", "window": "60s", "step": "30s", "step_seconds": "30"},
        )

        assert rendered == 'x{namespace=~"a|b", container!=""}[60s:30s] * 30'


class TestStep:
    """A minute by default, never beyond the lookback, a minute for a young period."""

    def test_default_step_is_a_minute(self):
        assert PrometheusClient({"url": URL}).step_seconds == 60

    @pytest.mark.parametrize("step", ["5m", 300, "90s"])
    def test_steps_up_to_the_lookback_are_accepted(self, step):
        assert PrometheusClient({"url": URL, "step": step}).step_seconds <= 300

    @pytest.mark.parametrize("step", ["1h", "6m", 301])
    def test_steps_beyond_the_lookback_are_refused(self, step):
        with pytest.raises(PrometheusError, match="at most 5m"):
            PrometheusClient({"url": URL, "step": step})

    def test_young_period_is_measured_at_a_minute(self, prometheus):
        """Not at the window: that moves the grid and puts the one point in the last month."""
        client = _client(prometheus, step="5m")
        start = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))

        collect_usage(client, ["cpu", "ram"], ["waldur-a"], start, start + datetime.timedelta(seconds=150))

        assert "[150s]" in prometheus.query_for("container_cpu_usage_seconds_total")
        assert "[150s:60s] offset 60s) * 60 /" in prometheus.query_for(
            "container_memory_working_set_bytes"
        )

    def test_shortest_window_is_a_minute(self, prometheus):
        client = _client(prometheus)
        start = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))

        collect_usage(client, ["ram"], ["waldur-a"], start, start + datetime.timedelta(seconds=5))

        assert "[60s:60s] offset 60s" in prometheus.queries()[0]

    def test_durations(self):
        assert duration_seconds("1h30m") == 5400.0
        assert duration_seconds("15d", "retention") == 15 * 86400.0
        assert duration_seconds(90) == 90.0
        with pytest.raises(PrometheusError, match="'step'.*duration"):
            duration_seconds("hourly")
        with pytest.raises(PrometheusError, match="positive"):
            duration_seconds(0)


class TestWindowSelection:
    """The current month runs to now; a closed month runs to its end."""

    def test_current_month_ends_at_now(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        with patch(NOW_SEAM, return_value=NOW):
            backend._get_usage_report(["waldur-a"])

        start = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))
        window = int(NOW.timestamp() - start.timestamp())
        assert f"[{window}s]" in prometheus.query_for("container_cpu_usage_seconds_total")
        assert all(abs(t - NOW.timestamp()) < 1 for t in prometheus.times())

    def test_closed_month_ends_at_its_first_instant_after(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        end = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))
        assert all(abs(t - end.timestamp()) < 1 for t in prometheus.times())
        assert "[2419200s]" in prometheus.query_for("container_cpu_usage_seconds_total")

    def test_month_with_a_dst_change_is_measured_in_real_seconds(
        self, prometheus, backend_factory
    ):
        """October 2025 in Tallinn has 745 hours: the clocks went back on the 26th."""
        backend = backend_factory(prometheus)

        backend.get_usage_report_for_period(["waldur-a"], 2025, 10)

        assert "[2682000s]" in prometheus.query_for("container_cpu_usage_seconds_total")

    def test_december_rolls_into_the_next_year(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        backend.get_usage_report_for_period(["waldur-a"], 2024, 12)

        end = datetime.datetime(2025, 1, 1, tzinfo=ZoneInfo(TZ))
        assert all(abs(t - end.timestamp()) < 1 for t in prometheus.times())

    def test_unfinished_month_is_measured_up_to_now(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        with patch(NOW_SEAM, return_value=NOW):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 3)

        assert all(abs(t - NOW.timestamp()) < 1 for t in prometheus.times())

    def test_naive_now_is_read_as_local_time(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        backend.timezone = ""

        naive_now = datetime.datetime(2025, 3, 15, 12, 0)
        with patch(NOW_SEAM, return_value=naive_now):
            backend._get_usage_report(["waldur-a"])

        assert all(abs(t - naive_now.timestamp()) < 1 for t in prometheus.times())


class TestClosedPeriodWindow:
    """A closed month is measured again only while Waldur can still take it."""

    APRIL_2 = datetime.datetime(2025, 4, 2, 0, 0, tzinfo=ZoneInfo(TZ))  # March ended 24 h ago

    def test_default_window_is_a_day(self):
        assert PrometheusClient({"url": URL}).closed_period_seconds == 24 * 3600

    def test_within_the_window_the_month_is_measured(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 1)))
        backend = backend_factory(prometheus, closed_period_hours=None)

        with patch(NOW_SEAM, return_value=self.APRIL_2 - datetime.timedelta(minutes=1)):
            report = backend.get_usage_report_for_period(["waldur-a"], 2025, 3)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 1.0

    def test_after_the_window_the_cycle_gets_nothing_and_no_query(
        self, prometheus, backend_factory
    ):
        backend = backend_factory(prometheus, closed_period_hours=None)

        with patch(NOW_SEAM, return_value=self.APRIL_2 + datetime.timedelta(minutes=1)):
            assert backend.get_usage_report_for_period(["waldur-a"], 2025, 3) == {}

        assert prometheus.requests == []

    def test_after_the_window_the_historical_loader_gets_an_error(
        self, prometheus, backend_factory
    ):
        """An empty report there would be zeroed by --reconcile-stale."""
        backend = backend_factory(prometheus, closed_period_hours=None)
        backend.historical_load = True

        with patch(NOW_SEAM, return_value=self.APRIL_2 + datetime.timedelta(minutes=1)):
            with pytest.raises(PrometheusPeriodClosedError, match="closed_period_hours"):
                backend.get_usage_report_for_period(["waldur-a"], 2025, 3)

        assert prometheus.requests == []

    def test_zero_hours_never_measures_a_closed_month(self, prometheus, backend_factory):
        backend = backend_factory(prometheus, closed_period_hours=0)

        with patch(NOW_SEAM, return_value=NOW):
            assert backend.get_usage_report_for_period(["waldur-a"], 2025, 2) == {}

    def test_negative_hours_are_refused(self):
        with pytest.raises(PrometheusError, match="closed_period_hours"):
            PrometheusClient({"url": URL, "closed_period_hours": -1})


def _runtimeinfo(retention: str) -> httpx.Response:
    return httpx.Response(200, json={"status": "success", "data": {"storageRetention": retention}})


def _blocks(*min_times: datetime.datetime) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "status": "success",
            "data": {"blocks": [{"minTime": int(t.timestamp() * 1000)} for t in min_times]},
        },
    )


class TestCoverage:
    """A period Prometheus does not hold in full is not reported, current or closed."""

    JAN_1 = datetime.datetime(2025, 1, 1, tzinfo=ZoneInfo(TZ))

    def test_configured_retention_is_trusted(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 1)))
        # Mar 15 minus 45 days is Jan 29, before February starts.
        backend = backend_factory(prometheus, retention="45d")

        with patch(NOW_SEAM, return_value=NOW):
            report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 1.0
        # Nothing was asked of the status endpoints.
        assert all(r.url.path == "/api/v1/query" for r in prometheus.requests)

    def test_closed_month_before_the_retention_raises_without_a_query(
        self, prometheus, backend_factory
    ):
        """40 days reach back to Feb 3: some of February is gone, so none is reported."""
        backend = backend_factory(prometheus, retention="40d")

        with patch(NOW_SEAM, return_value=NOW), pytest.raises(PrometheusCoverageError):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert prometheus.queries() == []

    def test_retention_is_read_from_prometheus(self, prometheus, backend_factory):
        prometheus.paths["/api/v1/status/runtimeinfo"] = _runtimeinfo("40d")
        prometheus.paths["/api/v1/status/tsdb/blocks"] = _blocks(self.JAN_1)
        backend = backend_factory(prometheus, retention=None)

        with patch(NOW_SEAM, return_value=NOW), pytest.raises(
            PrometheusCoverageError, match="retention of 40d"
        ):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

    def test_size_retention_and_a_young_server_are_caught_by_the_oldest_sample(
        self, prometheus, backend_factory
    ):
        prometheus.paths["/api/v1/status/runtimeinfo"] = _runtimeinfo("400d or 10GiB")
        prometheus.paths["/api/v1/status/tsdb/blocks"] = _blocks(
            datetime.datetime(2025, 2, 3, tzinfo=ZoneInfo(TZ)), self.JAN_1.replace(month=3)
        )
        backend = backend_factory(prometheus, retention=None)

        with patch(NOW_SEAM, return_value=NOW), pytest.raises(
            PrometheusCoverageError, match="oldest sample from 2025-02-02T22:00"
        ):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

    def test_covered_month_is_measured_and_the_server_asked_once(
        self, prometheus, backend_factory
    ):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 1)))
        prometheus.paths["/api/v1/status/runtimeinfo"] = _runtimeinfo("400d")
        prometheus.paths["/api/v1/status/tsdb/blocks"] = _blocks(self.JAN_1)
        backend = backend_factory(prometheus, retention=None)

        with patch(NOW_SEAM, return_value=NOW):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)
            report = backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 1.0
        status_calls = [r for r in prometheus.requests if r.url.path.startswith("/api/v1/status")]
        assert len(status_calls) == 2

    def test_without_the_endpoints_the_fallbacks_apply(self, prometheus, backend_factory, caplog):
        """No runtimeinfo: Prometheus's default 15d; no block list: the TSDB metric."""
        prometheus.answer(
            "prometheus_tsdb_lowest_timestamp_seconds",
            _vector(({"namespace": "x"}, self.JAN_1.timestamp())),
        )
        backend = backend_factory(prometheus, retention=None)

        with patch(NOW_SEAM, return_value=NOW), pytest.raises(
            PrometheusCoverageError, match="retention of 15d"
        ):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)
        assert any("assuming 15d" in r.getMessage() for r in caplog.records)
        assert backend.prometheus_client.lowest_timestamp() == self.JAN_1.timestamp()

    def test_uncovered_current_period_is_not_reported_and_logged_once(
        self, prometheus, backend_factory, caplog
    ):
        """A retention shorter than the month would make it a sliding window."""
        backend = backend_factory(prometheus, retention="7d")
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        with patch(NOW_SEAM, return_value=NOW), caplog.at_level(logging.ERROR):
            backend.pull_resource(_resource())
            info = backend.pull_resource(_resource())

        assert info.usage == {"TOTAL_ACCOUNT_USAGE": {}}
        assert prometheus.queries() == []
        errors = [r for r in caplog.records if "No usage is reported for the current period" in r.getMessage()]
        assert len(errors) == 1

    def test_diagnostics_fail_when_the_current_period_is_not_covered(
        self, prometheus, backend_factory
    ):
        backend = backend_factory(prometheus, retention="7d")
        backend.k8s_client.ping.return_value = True
        backend.k8s_client.list_managed_namespaces.return_value = []

        with patch(NOW_SEAM, return_value=NOW):
            assert backend.diagnostics() is False


class TestFailures:
    """A failed query reports nothing; an unreachable Prometheus ends the cycle's queries."""

    def test_failed_query_raises_for_a_closed_month(self, prometheus, backend_factory, caplog):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3600)))
        prometheus.answer(
            "container_memory_working_set_bytes",
            httpx.Response(400, json={"status": "error", "errorType": "bad_data", "error": "parse error"}),
        )
        backend = backend_factory(prometheus)

        with pytest.raises(PrometheusQueryError, match="ram"):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert any(
            "could not measure component type 'ram'" in r.getMessage() for r in caplog.records
        )
        # A refused query does not stop the others, so every broken one is named.
        assert len(prometheus.queries()) == 4

    def test_failed_query_reports_an_empty_current_total(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3600)))
        prometheus.answer(
            "kubelet_volume_stats_used_bytes",
            httpx.Response(422, json={"status": "error", "errorType": "execution", "error": "x"}),
        )
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        with patch(NOW_SEAM, return_value=NOW):
            info = backend.pull_resource(_resource())

        assert info.usage == {"TOTAL_ACCOUNT_USAGE": {}}

    @pytest.mark.parametrize(
        "failure",
        [
            httpx.Response(401, text="unauthorized"),
            httpx.Response(502, text="<html>bad gateway</html>"),
            httpx.ConnectError("no route to host"),
            httpx.ReadTimeout("timed out"),
        ],
    )
    def test_unavailable_prometheus_stops_the_pass_and_the_cycle(
        self, prometheus, backend_factory, failure
    ):
        def handler(request):
            prometheus.requests.append(request)
            if isinstance(failure, Exception):
                raise failure
            return failure

        prometheus.handler = handler
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        with patch(NOW_SEAM, return_value=NOW):
            first = backend.pull_resource(_resource("waldur-a"))
            second = backend.pull_resource(_resource("waldur-b"))
            with pytest.raises(PrometheusUnavailableError, match="earlier in this cycle"):
                backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert first.usage == second.usage == {"TOTAL_ACCOUNT_USAGE": {}}
        # One request, then nothing: the other types and resources would each have
        # waited out the same timeout.
        assert len(prometheus.requests) == 1

    def test_unavailable_prometheus_raises_for_a_closed_month(self, prometheus, backend_factory):
        def explode(request):
            raise httpx.ConnectError("no route", request=request)

        prometheus.handler = explode
        backend = backend_factory(prometheus)

        with pytest.raises(PrometheusUnavailableError):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

    def test_long_error_body_is_truncated(self, prometheus):
        prometheus.default = httpx.Response(502, text="<html>" + "x" * 5000)
        client = _client(prometheus)

        with client.connect() as session, pytest.raises(PrometheusUnavailableError) as excinfo:
            client.query(session, "up", NOW)

        assert len(str(excinfo.value)) < 400

    def test_oversized_answer_is_refused(self, prometheus, monkeypatch):
        monkeypatch.setattr(prometheus_billing, "MAX_RESPONSE_BYTES", 1000)
        prometheus.default = _vector(*[_ns(f"waldur-{i}", 1) for i in range(100)])
        client = _client(prometheus)

        with client.connect() as session, pytest.raises(PrometheusQueryError, match="larger than"):
            client.query(session, "up", NOW)

    def test_slow_answer_is_abandoned(self, prometheus, monkeypatch):
        clock = iter([0.0, 100.0, 200.0, 300.0])
        monkeypatch.setattr(prometheus_billing.time, "monotonic", lambda: next(clock))
        client = _client(prometheus)

        with client.connect() as session, pytest.raises(PrometheusUnavailableError, match="longer than"):
            client.query(session, "up", NOW)

    def test_the_server_is_told_the_timeout(self, prometheus):
        client = _client(prometheus, timeout=12)

        with client.connect() as session:
            client.query(session, "up", NOW)

        assert _form(prometheus.requests[0])["timeout"] == "9s"

    def test_query_timeout_fails_only_that_namespace(self, prometheus, backend_factory):
        """A namespace whose own queries are slow does not stop the others' billing."""
        slow = httpx.Response(
            503, json={"status": "error", "errorType": "timeout", "error": "query timed out"}
        )

        def handler(request):
            prometheus.requests.append(request)
            if "waldur-slow" in _form(request)["query"]:
                return slow
            return _vector(_ns("waldur-b", 1.0))

        prometheus.handler = handler
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        with patch(NOW_SEAM, return_value=NOW):
            first = backend.pull_resource(_resource("waldur-slow"))
            second = backend.pull_resource(_resource("waldur-b"))

        assert first.usage == {"TOTAL_ACCOUNT_USAGE": {}}
        assert second.usage["TOTAL_ACCOUNT_USAGE"]["cpu"] == 1.0

    def test_timeouts_in_a_row_count_as_unavailable(self, prometheus):
        prometheus.default = httpx.Response(
            503, json={"status": "error", "errorType": "timeout", "error": "query timed out"}
        )
        client = _client(prometheus)

        with client.connect() as session:
            for _ in range(prometheus_billing.MAX_CONSECUTIVE_QUERY_TIMEOUTS - 1):
                with pytest.raises(PrometheusQueryError, match="timed out"):
                    client.query(session, "up", NOW)
            with pytest.raises(PrometheusUnavailableError, match="in a row"):
                client.query(session, "up", NOW)

    def test_coverage_fallback_uses_the_prometheus_selector(self, prometheus):
        prometheus.answer(
            "prometheus_tsdb_lowest_timestamp_seconds", _vector(({"namespace": "x"}, 1.0))
        )
        client = _client(prometheus, selectors={"prometheus": 'job="prometheus"'})

        client.lowest_timestamp()

        assert 'prometheus_tsdb_lowest_timestamp_seconds{job="prometheus"}' in (
            prometheus.query_for("prometheus_tsdb_lowest_timestamp_seconds")
        )


class TestConnectionLifetime:
    """Every pass closes the HTTP client it opened."""

    @staticmethod
    def _record_sessions(client: PrometheusClient) -> list[httpx.Client]:
        opened: list[httpx.Client] = []
        connect = client.connect

        def recording_connect():
            session = connect()
            opened.append(session)
            return session

        client.connect = recording_connect  # type: ignore[method-assign]
        return opened

    def test_a_pass_shares_one_client_and_closes_it(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        opened = self._record_sessions(backend.prometheus_client)

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert len(opened) == 1
        assert opened[0].is_closed
        assert len(prometheus.requests) == 4

    def test_a_failed_pass_closes_it_too(self, prometheus, backend_factory):
        def explode(request):
            raise httpx.ConnectError("no route", request=request)

        prometheus.handler = explode
        backend = backend_factory(prometheus)
        opened = self._record_sessions(backend.prometheus_client)

        with pytest.raises(PrometheusUnavailableError):
            backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert opened and all(session.is_closed for session in opened)

    def test_ping_closes_its_client(self, prometheus):
        client = _client(prometheus)
        opened = self._record_sessions(client)

        assert client.ping() is True
        assert opened[0].is_closed


class TestPing:
    """``ping`` probes the query API, so a rejected token shows up."""

    def test_reachable(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True

        assert backend.ping() is True

    def test_rejected_token_returns_false(self, prometheus, backend_factory):
        prometheus.default = httpx.Response(401, text="bad token")
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True

        assert backend.ping() is False

    def test_failure_raises_on_request(self, prometheus, backend_factory):
        prometheus.default = httpx.Response(500, text="boom")
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True

        with pytest.raises(BackendError, match="Prometheus"):
            backend.ping(raise_exception=True)

    def test_connection_error_returns_false(self, prometheus, backend_factory):
        def explode(request):
            raise httpx.ConnectError("no route", request=request)

        prometheus.handler = explode
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True

        assert backend.ping() is False

    def test_error_answer_returns_false(self, prometheus):
        prometheus.default = httpx.Response(200, json={"status": "error", "error": "no"})

        assert _client(prometheus).ping() is False

    def test_diagnostics_reports_prometheus(self, prometheus, backend_factory, caplog):
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True
        backend.k8s_client.list_managed_namespaces.return_value = []

        with patch(NOW_SEAM, return_value=NOW), caplog.at_level(logging.INFO):
            assert backend.diagnostics() is True
        messages = [r.getMessage() for r in caplog.records]
        assert any("Prometheus billing" in m and "Yes" in m for m in messages)
        assert any(URL in m for m in messages)
        assert any("Prometheus holds data from" in m for m in messages)

    def test_diagnostics_fails_when_prometheus_is_down(self, prometheus, backend_factory):
        prometheus.default = httpx.Response(503, text="down")
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True

        assert backend.diagnostics() is False


class TestAuthenticationAndUrl:
    """One credential, kept out of the URL; TLS on unless explicitly off."""

    def test_bearer_token(self, prometheus):
        _client(prometheus, bearer_token="s3cret").ping()

        assert prometheus.requests[0].headers["Authorization"] == "Bearer s3cret"

    def test_basic_auth(self, prometheus):
        _client(prometheus, bearer_token=None, username="u", password="p").ping()

        assert prometheus.requests[0].headers["Authorization"] == "Basic dTpw"

    def test_both_credentials_are_refused(self):
        """httpx would let basic auth overwrite the bearer header."""
        with pytest.raises(PrometheusError, match="not both"):
            PrometheusClient({"url": URL, "bearer_token": "t", "username": "u", "password": "p"})

    def test_half_a_basic_auth_pair_is_refused(self):
        with pytest.raises(PrometheusError, match="both a username and a password"):
            PrometheusClient({"url": URL, "username": "u"})

    def test_no_credentials_sends_none(self, prometheus):
        _client(prometheus, bearer_token=None).ping()

        assert "Authorization" not in prometheus.requests[0].headers

    def test_query_is_posted_with_a_form_body(self, prometheus):
        _client(prometheus).ping()

        request = prometheus.requests[0]
        assert request.method == "POST"
        assert request.url.path == "/api/v1/query"
        assert _form(request)["query"] == "vector(1)"

    def test_credentials_in_the_url_are_refused_without_repeating_them(self):
        with pytest.raises(PrometheusError, match="'username'") as excinfo:
            PrometheusClient({"url": "https://alice:pa%40ss@prometheus.example.org"})

        assert "pa%40ss" not in str(excinfo.value)
        assert "alice" not in str(excinfo.value)

    @pytest.mark.parametrize(
        "url", [URL + "/?x=1", URL + "/#frag", "prometheus.example.org:9090", "ftp://host/"]
    )
    def test_other_unusable_urls_are_refused(self, url):
        with pytest.raises(PrometheusError, match="'url'"):
            PrometheusClient({"url": url})

    def test_clear_text_credentials_are_warned_about(self, caplog):
        with caplog.at_level(logging.WARNING):
            PrometheusClient({"url": "http://prometheus:9090", "bearer_token": "t"})
            PrometheusClient({"url": "http://prometheus:9090"})

        warnings = [r.getMessage() for r in caplog.records if "clear text" in r.getMessage()]
        assert len(warnings) == 1

    def test_disabled_verification_is_warned_about(self, caplog):
        with caplog.at_level(logging.WARNING):
            client = PrometheusClient({"url": URL, "ssl_verify": False})

        assert client.verify_ssl is False
        assert any("verification is off" in r.getMessage() for r in caplog.records)

    def test_ca_bundle_path_is_accepted(self):
        client = PrometheusClient({"url": URL, "ssl_verify": certifi.where()})

        assert isinstance(client.verify_ssl, ssl.SSLContext)
        assert client.verify_ssl.verify_mode == ssl.CERT_REQUIRED

    def test_missing_ca_bundle_is_refused(self):
        with pytest.raises(PrometheusError, match="does not exist"):
            PrometheusClient({"url": URL, "ssl_verify": "/no/such/ca.pem"})


class TestExplicitNulls:
    """A present-but-empty YAML value keeps the default."""

    def test_null_ssl_verify_keeps_verification_on(self):
        assert PrometheusClient({"url": URL, "ssl_verify": None}).verify_ssl is True

    def test_null_timeout_falls_back_to_the_default(self):
        client = PrometheusClient({"url": URL, "timeout": None})

        assert client.timeout == prometheus_billing.DEFAULT_TIMEOUT

    def test_null_durations_fall_back_to_the_defaults(self):
        client = PrometheusClient(
            {"url": URL, "step": None, "retention": None, "closed_period_hours": None}
        )

        assert client.step_seconds == 60
        # Unset retention is read from Prometheus.
        assert client.retention_seconds is None
        assert client.closed_period_seconds == 24 * 3600

    def test_null_queries_and_selectors_keep_the_defaults(self):
        client = PrometheusClient({"url": URL, "queries": None, "selectors": None})

        assert client.queries == DEFAULT_QUERIES
        assert client.selectors == prometheus_billing.DEFAULT_SELECTORS

    def test_null_credentials_are_not_an_error(self):
        client = PrometheusClient(
            {"url": URL, "bearer_token": None, "username": None, "password": None}
        )

        assert "Authorization" not in client.headers
        assert client.auth is None


class TestPullWithoutUsage:
    """A pull that asked for no usage must not spend Prometheus queries on it."""

    def test_pull_without_usage_does_not_query_prometheus(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        report = backend.pull_resources([_resource()], include_usage=False)

        assert prometheus.requests == []
        assert report["waldur-a"][1].usage == {"TOTAL_ACCOUNT_USAGE": {}}

    def test_reporting_pull_still_queries_prometheus(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(_ns("waldur-a", 3600)))
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        with patch(NOW_SEAM, return_value=NOW):
            report = backend.pull_resources([_resource()])

        assert len(prometheus.queries()) == 4
        assert report["waldur-a"][1].usage["TOTAL_ACCOUNT_USAGE"]["cpu"] == 3600.0


class TestSettingsSchema:
    """The ``prometheus`` block is checked against its own model at load time."""

    VALID: dict[str, Any] = {
        "kubeconfig_path": "/etc/kube/config",
        "prometheus": {
            "url": URL,
            "bearer_token": "t",
            "ssl_verify": "/etc/ssl/ca.pem",
            "timeout": 30,
            "step": "1m",
            "retention": "90d",
            "closed_period_hours": 48,
            "selectors": {"cadvisor": 'job="kubelet"'},
            "queries": {"cpu": 'x{namespace=~"{namespaces}"}'},
        },
    }

    @staticmethod
    def _load(settings: dict[str, Any]):
        offering = {
            "name": "Schema test",
            "waldur_api_url": "https://waldur.example.com/api/",
            "waldur_api_token": "token",
            "waldur_offering_uuid": "0" * 32,
            "backend_type": "k8s-ut-namespace",
            "order_processing_backend": "k8s-ut-namespace",
            "backend_settings": settings,
        }
        return RootConfiguration(offerings=[offering]).to_agent_configuration()

    @staticmethod
    def _schema_warnings(caplog) -> list[str]:
        return [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING
            and "Plugin schema validation failed" in r.getMessage()
        ]

    def test_valid_block_loads_without_warnings(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._load(copy.deepcopy(self.VALID))
        assert self._schema_warnings(caplog) == []

    def test_numeric_durations_and_boolean_verify_are_accepted(self, caplog):
        settings = copy.deepcopy(self.VALID)
        settings["prometheus"].update(step=60, retention=7776000, ssl_verify=False)
        with caplog.at_level(logging.WARNING):
            self._load(settings)
        assert self._schema_warnings(caplog) == []

    def test_misspelt_key_in_the_block_is_reported(self, caplog):
        settings = copy.deepcopy(self.VALID)
        settings["prometheus"]["bearer_tokn"] = "t"
        with caplog.at_level(logging.WARNING):
            self._load(settings)
        assert any("bearer_tokn" in m for m in self._schema_warnings(caplog))
