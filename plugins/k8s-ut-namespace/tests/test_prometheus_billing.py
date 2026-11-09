"""Consumption billing from Prometheus, over a stubbed HTTP transport.

Nothing here touches the network: every query is answered by an
``httpx.MockTransport`` handler that records the request, so each test can assert
both the figure reported and the PromQL that produced it.
"""

from __future__ import annotations

import datetime
import logging
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import httpx
import pytest

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_k8s_ut_namespace import prometheus_billing
from waldur_site_agent_k8s_ut_namespace.backend import K8sUtNamespaceBackend
from waldur_site_agent_k8s_ut_namespace.prometheus_billing import (
    DEFAULT_QUERIES,
    PrometheusClient,
    PrometheusError,
    collect_usage,
    duration_seconds,
    namespace_pattern,
    render_query,
)

from conftest import make_backend

TZ = "Europe/Tallinn"
# A fixed instant so the period bounds, and the ``time`` parameter derived from them,
# are asserted rather than observed.
NOW = datetime.datetime(2025, 3, 15, 12, 0, tzinfo=ZoneInfo(TZ))


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


class Prometheus:
    """A stand-in Prometheus whose answers each test sets up explicitly."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.routes: dict[str, httpx.Response] = {}
        self.default = _vector()

    def answer(self, needle: str, response: httpx.Response):
        """Answer every query whose PromQL contains ``needle`` with ``response``."""
        self.routes[needle] = response

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        expression = dict(httpx.QueryParams(request.content.decode()))["query"]
        for needle, response in self.routes.items():
            if needle in expression:
                return response
        return self.default

    def queries(self) -> list[str]:
        return [dict(httpx.QueryParams(r.content.decode()))["query"] for r in self.requests]

    def times(self) -> list[float]:
        return [float(dict(httpx.QueryParams(r.content.decode()))["time"]) for r in self.requests]


@pytest.fixture
def prometheus():
    return Prometheus()


def _repoint(client: PrometheusClient, prometheus: Prometheus) -> PrometheusClient:
    """Answer the client's requests from the stub, keeping its built configuration.

    Only the transport is swapped, so the headers and auth the constructor derived
    from the settings are what the assertions see.
    """
    session = client.session
    client.session = httpx.Client(
        transport=httpx.MockTransport(prometheus.handler),
        headers=session.headers,
        auth=session.auth,
    )
    session.close()
    return client


def _client(prometheus, **settings) -> PrometheusClient:
    """A client wired to the stub, with ``settings`` laid over a working default."""
    client = PrometheusClient(
        {"url": "https://prometheus.example.org", "bearer_token": "t", **settings}
    )
    return _repoint(client, prometheus)


@pytest.fixture
def backend_factory(backend_components, backend_settings_no_keycloak):
    """Build a backend whose Prometheus client answers from the stub."""

    def build(prometheus, components=None, **settings):
        backend_settings = dict(backend_settings_no_keycloak)
        backend_settings["prometheus"] = {
            "url": "https://prometheus.example.org",
            "bearer_token": "t",
            **settings,
        }
        backend, _, _ = make_backend(
            backend_settings, components if components is not None else backend_components
        )
        _repoint(backend.prometheus_client, prometheus)
        backend.timezone = TZ
        return backend

    return build


def _components(**overrides):
    """The plugin's four component types, with ``overrides`` applied by name."""
    components = {
        "cpu": {"type": "cpu", "unit_factor": 1, "accounting_type": "usage"},
        "ram": {"type": "ram", "unit_factor": 1, "accounting_type": "usage"},
        "storage": {"type": "storage", "unit_factor": 1, "accounting_type": "usage"},
        "gpu": {"type": "gpu", "unit_factor": 1, "accounting_type": "usage"},
    }
    for name, unit_factor in overrides.items():
        components[name]["unit_factor"] = unit_factor
    return components


class TestInert:
    """Without a ``prometheus`` block nothing is reported and nothing is probed."""

    def test_usage_report_is_empty(self, backend_settings_no_keycloak, backend_components):
        backend, _, _ = make_backend(backend_settings_no_keycloak, backend_components)
        backend.timezone = TZ

        assert backend.prometheus_client is None
        assert backend._get_usage_report(["waldur-a"]) == {}
        assert backend.get_usage_report_for_period(["waldur-a"], 2025, 3) == {}

    def test_ping_does_not_probe_prometheus(self, backend_settings_no_keycloak, backend_components):
        backend, mock_k8s, _ = make_backend(
            backend_settings_no_keycloak, backend_components
        )
        mock_k8s.ping.return_value = True

        assert backend.ping() is True

    def test_configured_block_is_built(self, backend_settings_no_keycloak, backend_components):
        settings = dict(backend_settings_no_keycloak)
        settings["prometheus"] = {"url": "https://prometheus.example.org"}
        backend, _, _ = make_backend(settings, backend_components)

        assert backend.prometheus_client.url == "https://prometheus.example.org"

    def test_unusable_block_raises_rather_than_disabling_billing(
        self, backend_settings_no_keycloak, backend_components
    ):
        """A block with no ``url`` must not read as \"billing is off\"."""
        settings = dict(backend_settings_no_keycloak)
        settings["prometheus"] = {"bearer_token": "t"}
        with pytest.raises(PrometheusError, match="'url'"):
            make_backend(settings, backend_components)


class TestUnitFactor:
    """Native period units are divided by the component's ``unit_factor``."""

    def test_each_component_is_converted(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(({"namespace": "waldur-a"}, 7200)))
        prometheus.answer("container_memory_working_set_bytes", _vector(({"namespace": "waldur-a"}, 100)))
        prometheus.answer("kubelet_volume_stats_used_bytes", _vector(({"namespace": "waldur-a"}, 50)))
        prometheus.answer("kube_pod_container_resource_requests", _vector(({"namespace": "waldur-a"}, 4)))

        backend = backend_factory(
            prometheus,
            components=_components(cpu=3600, ram=2, storage=0.5, gpu=1),
        )

        report = backend._get_usage_report(["waldur-a"])

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 2.0,
            "ram": 50.0,
            "storage": 100.0,
            "gpu": 4.0,
        }

    def test_default_unit_factor_is_one(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(({"namespace": "waldur-a"}, 3.5)))
        backend = backend_factory(
            prometheus,
            components={"cpu": {"type": "cpu", "accounting_type": "usage"}},
        )

        assert backend._get_usage_report(["waldur-a"])["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 3.5
        }

    def test_component_name_and_type_are_distinguished(self, prometheus, backend_factory):
        """A renamed component is metered by its type and reported under its name."""
        prometheus.answer("container_cpu_usage_seconds_total", _vector(({"namespace": "waldur-a"}, 3600)))
        backend = backend_factory(
            prometheus,
            components={"vCPU hours": {"type": "cpu", "unit_factor": 1}},
        )

        report = backend._get_usage_report(["waldur-a"])

        # The figure is core-hours, as the default query divides seconds by 3600.
        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {"vCPU hours": 3600.0}


class TestEmptyNamespace:
    """A namespace with no series is billed zero, not skipped or failed."""

    def test_namespace_without_series_reports_zeros(self, prometheus, backend_factory):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(({"namespace": "waldur-a"}, 3600)))
        backend = backend_factory(prometheus)

        report = backend._get_usage_report(["waldur-a", "waldur-quiet"])

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 3600.0
        assert report["waldur-quiet"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 0.0,
            "ram": 0.0,
            "storage": 0.0,
            "gpu": 0.0,
        }

    def test_empty_result_is_not_an_error(self, prometheus, backend_factory):
        prometheus.default = _vector()
        backend = backend_factory(prometheus)

        report = backend._get_usage_report(["waldur-a"])

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 0.0

    def test_nan_value_is_not_summed(self, prometheus, backend_factory):
        prometheus.answer(
            "container_cpu_usage_seconds_total",
            _vector(({"namespace": "waldur-a"}, "NaN"), ({"namespace": "waldur-a"}, 3600)),
        )
        backend = backend_factory(
            prometheus, components={"cpu": {"type": "cpu", "unit_factor": 1}}
        )

        assert backend._get_usage_report(["waldur-a"])["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 3600.0
        }


class TestTemplateSubstitution:
    """``{window}``, ``{window_seconds}``, ``{step}`` and ``{namespaces}`` are filled."""

    def test_default_query_carries_period_and_namespaces(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        backend.get_usage_report_for_period(["waldur-a", "waldur-b"], 2025, 2)

        cpu_query = next(q for q in prometheus.queries() if "container_cpu_usage_seconds_total" in q)
        # February 2025 in Tallinn is 28 days.
        assert "increase(container_cpu_usage_seconds_total" in cpu_query
        assert '[2419200s]' in cpu_query
        assert 'namespace=~"waldur-a|waldur-b"' in cpu_query
        assert "{window}" not in cpu_query
        assert "{namespaces}" not in cpu_query

    def test_average_queries_carry_the_step(self, prometheus, backend_factory):
        backend = backend_factory(prometheus, step="15m")

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        ram_query = next(q for q in prometheus.queries() if "container_memory_working_set_bytes" in q)
        assert "[2419200s:900s]" in ram_query
        assert "* 2419200" in ram_query

    def test_override_composes_like_a_default(self, prometheus, backend_factory):
        backend = backend_factory(
            prometheus,
            queries={"cpu": 'sum by (namespace) (custom_cpu{namespace=~"{namespaces}"}[{window}])'},
        )

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        cpu_query = next(q for q in prometheus.queries() if "custom_cpu" in q)
        assert cpu_query == 'sum by (namespace) (custom_cpu{namespace=~"waldur-a"}[2419200s])'

    def test_override_replaces_only_its_own_type(self, prometheus, backend_factory):
        backend = backend_factory(prometheus, queries={"cpu": "custom_cpu"})

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        assert any("container_memory_working_set_bytes" in q for q in prometheus.queries())

    def test_render_leaves_promql_braces_alone(self):
        rendered = render_query(
            'x{namespace=~"{namespaces}", container!=""}[{window}]',
            {"namespaces": "a|b", "window": "60s"},
        )

        assert rendered == 'x{namespace=~"a|b", container!=""}[60s]'

    def test_namespace_pattern_escapes_regex_metacharacters(self):
        assert namespace_pattern(["waldur-a", "waldur.b"]) == r"waldur-a|waldur\.b"

    def test_step_accepts_a_promql_duration(self):
        assert duration_seconds("1h30m") == 5400.0
        assert duration_seconds(90) == 90.0
        with pytest.raises(PrometheusError, match="duration"):
            duration_seconds("hourly")


class TestWindowSelection:
    """The current month runs to now; a closed month runs to its end."""

    def test_current_month_ends_at_now(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        # The same seam the reporting processor reads, so the month boundary under
        # test is the one the processor would bill against.
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.backend_utils"
            ".get_current_time_in_timezone",
            return_value=NOW,
        ):
            backend._get_usage_report(["waldur-a"])

        start = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))
        window = int((NOW - start).total_seconds())
        assert "[%ds]" % window in next(
            q for q in prometheus.queries() if "container_cpu_usage_seconds_total" in q
        )
        # The query is evaluated at the period's end, not at "now" read separately.
        assert all(abs(t - NOW.timestamp()) < 1 for t in prometheus.times())

    def test_closed_month_ends_at_its_last_instant(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        backend.get_usage_report_for_period(["waldur-a"], 2025, 2)

        end = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))
        assert all(abs(t - end.timestamp()) < 1 for t in prometheus.times())
        assert "[2419200s]" in next(
            q for q in prometheus.queries() if "container_cpu_usage_seconds_total" in q
        )

    def test_december_rolls_into_the_next_year(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        backend.get_usage_report_for_period(["waldur-a"], 2024, 12)

        end = datetime.datetime(2025, 1, 1, tzinfo=ZoneInfo(TZ))
        assert all(abs(t - end.timestamp()) < 1 for t in prometheus.times())

    def test_fresh_period_uses_the_shortest_window(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        start = datetime.datetime(2025, 3, 1, tzinfo=ZoneInfo(TZ))

        collect_usage(
            backend.prometheus_client,
            ["cpu"],
            ["waldur-a"],
            start,
            start + datetime.timedelta(seconds=5),
        )

        assert "[60s]" in prometheus.queries()[0]


class TestFailures:
    """One failed query omits the affected resources and never aborts the pass."""

    def test_failed_query_is_logged_and_omitted(self, prometheus, backend_factory, caplog):
        prometheus.answer(
            "container_memory_working_set_bytes",
            httpx.Response(503, text="upstream unavailable"),
        )
        backend = backend_factory(prometheus)

        report = backend._get_usage_report(["waldur-a"])

        assert report == {}
        assert any(
            "could not measure component type 'ram'" in r.getMessage() for r in caplog.records
        )
        # The other component types were still asked for.
        assert any("container_cpu_usage_seconds_total" in q for q in prometheus.queries())

    def test_transport_failure_is_contained(self, prometheus, backend_factory):
        def explode(request):
            raise httpx.ConnectError("no route to host", request=request)

        prometheus.handler = explode
        backend = backend_factory(prometheus)

        assert backend._get_usage_report(["waldur-a"]) == {}

    def test_refused_query_is_contained(self, prometheus, backend_factory):
        prometheus.answer(
            "container_cpu_usage_seconds_total",
            httpx.Response(200, json={"status": "error", "error": "parse error"}),
        )
        backend = backend_factory(prometheus)

        assert backend._get_usage_report(["waldur-a"]) == {}

    def test_component_without_a_query_reports_zero(self, prometheus, backend_factory, caplog):
        prometheus.answer("container_cpu_usage_seconds_total", _vector(({"namespace": "waldur-a"}, 3600)))
        backend = backend_factory(
            prometheus, components={"cpu": {"type": "cpu"}, "mystery": {"type": "mystery"}}
        )

        report = backend._get_usage_report(["waldur-a"])

        assert report["waldur-a"]["TOTAL_ACCOUNT_USAGE"] == {"cpu": 3600.0, "mystery": 0.0}
        assert any(
            "No Prometheus query is configured for component type 'mystery'" in r.getMessage()
            for r in caplog.records
        )


class TestPing:
    """An unreachable Prometheus keeps orders pending instead of erroring them."""

    def test_reachable(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)

        assert backend.ping() is True

    def test_unreachable_returns_false(self, prometheus, backend_factory):
        prometheus.default = httpx.Response(401, text="bad token")
        backend = backend_factory(prometheus)

        assert backend.ping() is False

    def test_unreachable_raises_on_request(self, prometheus, backend_factory):
        prometheus.default = httpx.Response(500, text="boom")
        backend = backend_factory(prometheus)

        with pytest.raises(BackendError, match="Prometheus"):
            backend.ping(raise_exception=True)

    def test_connection_error_returns_false(self, prometheus, backend_factory):
        def explode(request):
            raise httpx.ConnectError("no route", request=request)

        prometheus.handler = explode
        backend = backend_factory(prometheus)

        assert backend.ping() is False

    def test_diagnostics_reports_prometheus(self, prometheus, backend_factory, caplog):
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True
        backend.k8s_client.list_managed_namespaces.return_value = []

        with caplog.at_level(logging.INFO):
            assert backend.diagnostics() is True
        assert any("Prometheus billing" in r.getMessage() for r in caplog.records)

    def test_diagnostics_fails_when_prometheus_is_down(self, prometheus, backend_factory):
        prometheus.default = httpx.Response(503, text="down")
        backend = backend_factory(prometheus)
        backend.k8s_client.ping.return_value = True

        assert backend.diagnostics() is False


class TestAuthentication:
    """The credential in the settings block reaches the request."""

    def test_bearer_token(self, prometheus):
        client = _client(prometheus, bearer_token="s3cret")

        client.ping()

        assert prometheus.requests[0].headers["Authorization"] == "Bearer s3cret"

    def test_basic_auth(self, prometheus):
        client = _client(prometheus, bearer_token=None, username="u", password="p")

        client.ping()

        assert prometheus.requests[0].headers["Authorization"].startswith("Basic ")

    def test_half_a_basic_auth_pair_is_refused(self):
        with pytest.raises(PrometheusError, match="both 'username' and 'password'"):
            PrometheusClient(
                {"url": "https://prometheus.example.org", "username": "u"}
            )

    def test_no_credentials_sends_none(self, prometheus):
        client = _client(prometheus, bearer_token=None)

        client.ping()

        assert "Authorization" not in prometheus.requests[0].headers

    def test_query_is_posted_with_a_form_body(self, prometheus):
        client = _client(prometheus)

        client.ping()

        request = prometheus.requests[0]
        assert request.method == "POST"
        assert request.url.path == "/api/v1/query"
        assert dict(httpx.QueryParams(request.content.decode()))["query"] == "vector(1)"


class TestExplicitNulls:
    """A present-but-empty YAML value must not change a default or crash."""

    def test_null_ssl_verify_keeps_verification_on(self):
        client = PrometheusClient(
            {"url": "https://prometheus.example.org", "ssl_verify": None}
        )

        assert client.verify_ssl is True

    def test_explicit_false_disables_verification(self):
        client = PrometheusClient(
            {"url": "https://prometheus.example.org", "ssl_verify": False}
        )

        assert client.verify_ssl is False

    def test_null_timeout_falls_back_to_the_default(self):
        client = PrometheusClient(
            {"url": "https://prometheus.example.org", "timeout": None}
        )

        assert client.timeout == prometheus_billing.DEFAULT_TIMEOUT

    def test_null_step_falls_back_to_the_default(self):
        client = PrometheusClient(
            {"url": "https://prometheus.example.org", "step": None}
        )

        assert client.step_seconds == duration_seconds(prometheus_billing.DEFAULT_STEP)

    def test_null_queries_keep_the_defaults(self):
        client = PrometheusClient(
            {"url": "https://prometheus.example.org", "queries": None}
        )

        assert client.queries == DEFAULT_QUERIES

    def test_null_credentials_are_not_an_error(self):
        client = PrometheusClient(
            {
                "url": "https://prometheus.example.org",
                "bearer_token": None,
                "username": None,
                "password": None,
            }
        )

        assert client.session.headers.get("Authorization") is None


class TestBackendClass:
    """Both entry-point names bill the same way."""

    def test_k8s_namespace_alias_inherits_billing(self, prometheus, backend_factory):
        from waldur_site_agent_k8s_ut_namespace.backend import K8sNamespaceBackend

        assert issubclass(K8sNamespaceBackend, K8sUtNamespaceBackend)
        assert K8sNamespaceBackend._get_usage_report is K8sUtNamespaceBackend._get_usage_report


class TestPullWithoutUsage:
    """A pull that asked for no usage must not spend Prometheus queries on it.

    Membership sync passes ``include_usage=False`` and never reads usage. Before the
    override honoured it, every sync cycle ran one query per component type per
    namespace for a figure it then discarded.
    """

    @staticmethod
    def _resource():
        resource = MagicMock()
        resource.backend_id = "waldur-a"
        resource.slug = "a"
        resource.name = "a"
        return resource

    def test_pull_without_usage_does_not_query_prometheus(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        backend.pull_resources([self._resource()], include_usage=False)

        assert prometheus.requests == []

    def test_reporting_pull_still_queries_prometheus(self, prometheus, backend_factory):
        backend = backend_factory(prometheus)
        backend.k8s_client.get_managed_namespace.return_value = {"status": {}}

        report = backend.pull_resources([self._resource()])

        assert prometheus.requests
        assert "TOTAL_ACCOUNT_USAGE" in report["waldur-a"][1].usage
