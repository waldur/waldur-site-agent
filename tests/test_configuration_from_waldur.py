"""Offerings whose configuration comes from Waldur (configuration_source: waldur).

The endpoint (GET marketplace-provider-offerings/{uuid}/site_agent_config/) is
mocked with respx; nothing here reaches a network.
"""

from __future__ import annotations

import copy
from pathlib import Path
from unittest import mock

import httpx
import pytest
import respx
import yaml

from waldur_site_agent.common import configuration_source, utils
from waldur_site_agent.common.configuration_source import ConfigurationSourceError
from waldur_site_agent.common.structures import AccountingType, RootConfiguration

API_URL = "https://waldur.example.com/api/"
UUID = "e2ef0000000000000000000000000009"
ENDPOINT = f"https://waldur.example.com/api/marketplace-provider-offerings/{UUID}/site_agent_config/"
TOKEN = "bootstrap-token"  # noqa: S105 - test value

SERVED = {
    "offering": {
        "name": "Cluster from Waldur",
        "backend_type": "slurm",
        "order_processing_backend": "slurm",
        "membership_sync_backend": "slurm",
        "reporting_backend": "slurm",
        "username_management_backend": "base",
        "stomp_enabled": False,
        "resource_import_enabled": True,
        "omit_anomalous_usage_components": True,
        "backend_settings": {
            "default_account": "root",
            "customer_prefix": "c_",
            "project_prefix": "p_",
            "allocation_prefix": "a_",
            "qos_default": "normal",
            "ldap": {"uri": "ldap://ldap.example.com", "bind_dn": "cn=admin"},
        },
        "backend_components": {
            "cpu": {
                "measured_unit": "Hours",
                "unit_factor": 60,
                "accounting_type": "usage",
                "label": "CPU",
                "limit": 100,
            },
            "mem": {
                "measured_unit": "GB-Hours",
                "unit_factor": 61440,
                "accounting_type": "limit",
                "label": "RAM",
                "unit_factor_reporting": 1024,
            },
        },
        "policy_settings": {"limit_type": "GrpTRESMins", "grace_ratio": 0.2},
    },
    "secrets": [
        {"field": "backend_settings.ldap.bind_password", "backend": "slurm", "requirement": "required_with_parent"},
    ],
    "version": "a" * 64,
}


def bootstrap(**extra):
    offering = {
        "waldur_api_url": API_URL,
        "waldur_api_token": TOKEN,
        "waldur_offering_uuid": UUID,
        "configuration_source": "waldur",
        # The served ldap block makes its bind password required.
        "backend_settings": {"ldap": {"bind_password": "s3cret"}},
    }
    offering.update(extra)
    return offering


def load(*offerings, user_agent="waldur-site-agent-test/1"):
    return RootConfiguration(offerings=list(offerings)).to_agent_configuration(user_agent=user_agent)


@pytest.fixture
def served():
    return copy.deepcopy(SERVED)


@pytest.fixture
def endpoint(served):
    with respx.mock(assert_all_called=False) as router:
        route = router.get(ENDPOINT).mock(side_effect=lambda request: httpx.Response(200, json=served))
        yield route


# --- Loading -----------------------------------------------------------------


def test_offering_loads_what_waldur_serves_with_local_secrets(endpoint, served):
    offering = load(bootstrap(backend_settings={"ldap": {"bind_password": "s3cret"}})).offerings[0]

    data = served["offering"]
    assert offering.name == data["name"]
    assert offering.configuration_source == "waldur"
    assert offering.configuration_version == served["version"]
    for role in ("backend_type", "order_processing_backend", "membership_sync_backend", "reporting_backend"):
        assert getattr(offering, role) == "slurm"
    assert offering.username_management_backend == "base"
    assert offering.stomp_enabled is False
    assert offering.resource_import_enabled is True
    assert offering.omit_anomalous_usage_components is True
    assert offering.policy_settings == data["policy_settings"]
    expected_settings = copy.deepcopy(data["backend_settings"])
    expected_settings["ldap"]["bind_password"] = "s3cret"
    assert offering.backend_settings == expected_settings
    assert offering.backend_components_dict == {
        "cpu": {"measured_unit": "Hours", "unit_factor": 60.0, "accounting_type": "usage", "label": "CPU", "limit": 100.0},
        "mem": {
            "measured_unit": "GB-Hours",
            "unit_factor": 61440.0,
            "accounting_type": "limit",
            "label": "RAM",
            "unit_factor_reporting": 1024.0,
        },
    }
    assert offering.backend_components["cpu"].accounting_type == AccountingType.USAGE
    # Bootstrap fields stay local.
    assert offering.waldur_api_url == API_URL
    assert offering.waldur_api_token == TOKEN

    request = endpoint.calls.last.request
    assert request.headers["Authorization"] == f"Token {TOKEN}"
    assert request.headers["User-Agent"] == "waldur-site-agent-test/1"


def test_local_name_and_verify_ssl_win(endpoint):
    offering = load(bootstrap(name="Local name", verify_ssl=False)).offerings[0]
    assert offering.name == "Local name"
    assert offering.verify_ssl is False


def test_offering_without_policy_has_none(endpoint, served):
    del served["offering"]["policy_settings"]
    assert load(bootstrap()).offerings[0].policy_settings is None


def test_oidc_bootstrap_fetches_a_token_first(served):
    with respx.mock() as router:
        router.post("https://idp.example.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "jwt", "expires_in": 300})
        )
        route = router.get(ENDPOINT).mock(return_value=httpx.Response(200, json=served))
        utils._OIDC_TOKEN_CACHE.clear()
        try:
            offering = load(
                {
                    "waldur_api_url": API_URL,
                    "waldur_offering_uuid": UUID,
                    "configuration_source": "waldur",
                    "oidc_token_url": "https://idp.example.com/token",
                    "oidc_client_id": "agent",
                    "oidc_client_secret": "client-secret",
                    "backend_settings": {"ldap": {"bind_password": "s3cret"}},
                }
            ).offerings[0]
        finally:
            utils._OIDC_TOKEN_CACHE.clear()
    assert route.calls.last.request.headers["Authorization"] == "Bearer jwt"
    assert offering.oidc_client_id == "agent"


def test_load_configuration_passes_the_user_agent(endpoint, tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"offerings": [bootstrap()]}))
    with mock.patch.object(utils, "configure_logger"):
        configuration = utils.load_configuration(str(path), user_agent_suffix="report")
    assert configuration.offerings[0].backend_type == "slurm"
    assert endpoint.calls.last.request.headers["User-Agent"] == configuration.waldur_user_agent
    assert configuration.waldur_user_agent.startswith("waldur-site-agent-report/")


def test_global_proxy_is_used(served):
    with mock.patch.object(utils, "get_client", wraps=utils.get_client) as get_client, respx.mock() as router:
        router.get(ENDPOINT).mock(return_value=httpx.Response(200, json=served))
        RootConfiguration(offerings=[bootstrap()], global_proxy="http://proxy.example.com:3128").to_agent_configuration()
    assert get_client.call_args.args[4] == "http://proxy.example.com:3128"


# --- Local keys ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("extra", "named"),
    [
        ({"backend_type": "slurm"}, "backend_type"),
        ({"backend_components": {}}, "backend_components"),
        ({"stomp_enabled": True, "reporting_backend": "slurm"}, "reporting_backend, stomp_enabled"),
    ],
)
def test_non_bootstrap_key_fails_before_calling_waldur(endpoint, extra, named):
    with pytest.raises(ConfigurationSourceError, match=f"Set these in Waldur instead: {named}$"):
        load(bootstrap(**extra))
    assert not endpoint.called


def test_non_secret_backend_setting_fails_naming_it(endpoint):
    with pytest.raises(ConfigurationSourceError, match=r"backend_settings\.default_account, backend_settings\.ldap\.uri"):
        load(bootstrap(backend_settings={"default_account": "other", "ldap": {"uri": "x", "bind_password": "s"}}))


def test_secret_declared_by_a_plugin_is_accepted_although_waldur_does_not_list_it(endpoint, served):
    served["secrets"] = []
    offering = load(bootstrap(backend_settings={"ldap": {"bind_password": "s3cret"}})).offerings[0]
    assert offering.backend_settings["ldap"]["bind_password"] == "s3cret"


# --- Required secrets --------------------------------------------------------------


def test_missing_required_secret_fails_naming_offering_and_field(endpoint, served):
    served["secrets"] = [{"field": "backend_settings.target_api_token", "backend": "waldur", "requirement": "required"}]
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(bootstrap(name="Federated", backend_settings={"ldap": {"bind_password": "s"}}))
    message = str(exc_info.value)
    assert f"Offering 'Federated' ({UUID})" in message
    assert "backend_settings.target_api_token (backend waldur)" in message


def test_required_secret_set_locally_loads(endpoint, served):
    served["secrets"] = [{"field": "backend_settings.target_api_token", "backend": "waldur", "requirement": "required"}]
    offering = load(bootstrap(backend_settings={"target_api_token": "t", "ldap": {"bind_password": "s"}})).offerings[0]
    assert offering.backend_settings["target_api_token"] == "t"


def test_empty_required_secret_counts_as_missing(endpoint, served):
    served["secrets"] = [{"field": "backend_settings.target_api_token", "backend": "waldur", "requirement": "required"}]
    with pytest.raises(ConfigurationSourceError, match="target_api_token"):
        load(bootstrap(backend_settings={"target_api_token": ""}))


def test_required_with_parent_is_required_when_the_block_is_served(endpoint):
    with pytest.raises(ConfigurationSourceError, match=r"backend_settings\.ldap\.bind_password \(backend slurm\)"):
        load(bootstrap(backend_settings={}))


def test_required_with_parent_is_not_required_without_the_block(endpoint, served):
    del served["offering"]["backend_settings"]["ldap"]
    offering = load(bootstrap(backend_settings={})).offerings[0]
    assert "ldap" not in offering.backend_settings


def test_secret_for_a_block_waldur_does_not_serve_fails_at_load(endpoint, served):
    # Merged in, it would make an ldap block holding only the password, and the
    # backend would fail on the missing uri at every cycle rather than at startup.
    del served["offering"]["backend_settings"]["ldap"]
    with pytest.raises(
        ConfigurationSourceError,
        match=r"secrets for blocks that Waldur does not configure: backend_settings\.ldap\.bind_password\.",
    ):
        load(bootstrap(backend_settings={"ldap": {"bind_password": "s3cret"}}))


def test_optional_secret_may_be_missing(endpoint, served):
    served["secrets"] = [{"field": "backend_settings.ldap.bind_password", "backend": "slurm", "requirement": "optional"}]
    assert load(bootstrap(backend_settings={})).offerings[0].backend_settings["ldap"] == (
        served["offering"]["backend_settings"]["ldap"]
    )


# --- Waldur failing ------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500])
def test_error_status_fails_with_offering_url_and_status(status):
    with respx.mock() as router:
        router.get(ENDPOINT).mock(return_value=httpx.Response(status, json={"detail": "Nope."}))
        with pytest.raises(ConfigurationSourceError) as exc_info:
            load(bootstrap(name="Cluster"))
    message = str(exc_info.value)
    assert f"Offering 'Cluster' ({UUID})" in message
    assert f"HTTP {status}" in message
    assert ENDPOINT in message
    assert "Nope." in message


def test_unreachable_waldur_fails_with_offering_and_url():
    with respx.mock() as router:
        router.get(ENDPOINT).mock(side_effect=httpx.ConnectError("Connection refused"))
        with pytest.raises(ConfigurationSourceError) as exc_info:
            load(bootstrap())
    message = str(exc_info.value)
    assert f"Offering {UUID}" in message
    assert ENDPOINT in message
    assert "Connection refused" in message


@pytest.mark.parametrize("body", [b"<html>", b'{"secrets": []}', b'{"offering": "x"}'])
def test_unusable_response_fails(body):
    with respx.mock() as router:
        router.get(ENDPOINT).mock(return_value=httpx.Response(200, content=body))
        with pytest.raises(ConfigurationSourceError, match=ENDPOINT):
            load(bootstrap())


@pytest.mark.parametrize(
    ("path", "value", "named"),
    [(("backend_components", "cpu", "accounting_type"), "bogus", "accounting_type"), (("stomp_enabled",), "maybe", "stomp_enabled")],
)
def test_invalid_served_configuration_fails_without_echoing_secrets(endpoint, served, path, value, named):
    node = served["offering"]
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(bootstrap())
    message = str(exc_info.value)
    assert named in message
    assert served["version"] in message
    assert ENDPOINT in message
    assert "s3cret" not in message
    assert value not in message


def test_init_configuration_exits_with_a_readable_error(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"offerings": [bootstrap(backend_type="slurm")]}))
    with mock.patch("sys.argv", ["waldur_site_agent", "-c", str(path)]), mock.patch.object(
        utils, "logger"
    ) as logger, pytest.raises(SystemExit) as exc_info:
        utils.init_configuration()
    assert exc_info.value.code == 1
    logged = logger.error.call_args.args
    assert "backend_type" in str(logged[2])


# --- Local offerings and drift --------------------------------------------------


def local_offering(**extra):
    offering = {
        "name": "Local cluster",
        "waldur_api_url": API_URL,
        "waldur_api_token": TOKEN,
        "waldur_offering_uuid": UUID,
        "backend_type": "slurm",
        "order_processing_backend": "slurm",
        "membership_sync_backend": "slurm",
        "reporting_backend": "slurm",
        "backend_settings": copy.deepcopy(SERVED["offering"]["backend_settings"]),
        "backend_components": copy.deepcopy(SERVED["offering"]["backend_components"]),
    }
    offering["backend_settings"]["ldap"]["bind_password"] = "s3cret"
    offering.update(extra)
    return offering


def test_local_offering_loads_without_calling_waldur(endpoint):
    configuration = load(local_offering(), local_offering(configuration_source="local"))
    assert not endpoint.called
    assert [o.configuration_source for o in configuration.offerings] == ["local", "local"]
    assert configuration.offerings[0].configuration_version == ""


def test_unknown_configuration_source_is_refused(endpoint):
    with pytest.raises(Exception, match="configuration_source"):
        load(local_offering(configuration_source="remote"))


def drift_warnings(configuration):
    with mock.patch.object(configuration_source, "logger") as logger:
        configuration_source.warn_on_configuration_drift(configuration)
    return logger


def test_matching_local_offering_logs_no_drift(endpoint):
    logger = drift_warnings(load(local_offering()))
    assert endpoint.called
    logger.warning.assert_not_called()


def test_differing_unit_factor_produces_one_warning(endpoint):
    offering = local_offering()
    offering["backend_components"]["cpu"]["unit_factor"] = 60000
    logger = drift_warnings(load(offering))
    assert logger.warning.call_count == 1
    args, kwargs = logger.warning.call_args
    message = args[0] % args[1:]
    assert "Local cluster" in message
    assert "component cpu unit_factor" in message
    assert "60000.0" in message and "60.0" in message
    assert kwargs == {"configuration_drift": "component cpu unit_factor", "local_value": 60000.0, "waldur_value": 60.0}


def test_drift_covers_component_fields_settings_and_roles(endpoint):
    offering = local_offering(reporting_backend="")
    offering["backend_components"]["cpu"].update(accounting_type="limit", limit=5)
    offering["backend_components"]["mem"].pop("unit_factor_reporting")
    offering["backend_components"]["gpu"] = {"measured_unit": "h", "accounting_type": "usage", "label": "GPU"}
    offering["backend_settings"]["qos_default"] = "high"
    offering["backend_settings"]["ldap"]["bind_password"] = "different-but-secret"
    subjects = [call.kwargs["configuration_drift"] for call in drift_warnings(load(offering)).warning.call_args_list]
    assert sorted(subjects) == sorted(
        [
            "reporting_backend",
            "component cpu accounting_type",
            "component cpu limit",
            "component mem unit_factor_reporting",
            "component gpu",
            "backend_settings.qos_default",
        ]
    )


@pytest.mark.parametrize(
    "response",
    [httpx.Response(403), httpx.Response(404), httpx.Response(400), httpx.ConnectError("refused")],
)
def test_drift_check_never_fails_startup(response):
    configuration = load(local_offering())
    with respx.mock() as router:
        route = router.get(ENDPOINT)
        if isinstance(response, Exception):
            route.mock(side_effect=response)
        else:
            route.mock(return_value=response)
        logger = drift_warnings(configuration)
    logger.warning.assert_not_called()
    assert logger.debug.called


def test_drift_check_skips_waldur_sourced_offerings(endpoint):
    configuration = load(bootstrap(backend_settings={"ldap": {"bind_password": "s"}}))
    calls = endpoint.call_count
    drift_warnings(configuration)
    assert endpoint.call_count == calls


# --- waldur_site_load_components --------------------------------------------------


def test_load_components_warns_and_skips_waldur_sourced_offerings(endpoint):
    configuration = load(bootstrap(backend_settings={"ldap": {"bind_password": "s"}}), local_offering())
    with mock.patch.object(utils, "init_configuration", return_value=configuration), mock.patch.object(
        utils, "get_client_for_offering"
    ) as get_client, mock.patch.object(utils, "load_components_to_waldur") as load_components, mock.patch.object(
        utils, "logger"
    ) as logger:
        utils.load_offering_components()
    assert "deprecated" in logger.warning.call_args_list[0].args[0]
    assert load_components.call_count == 1
    assert load_components.call_args.args[2] == "Local cluster"
    assert get_client.call_count == 1
    skipped = [call.args for call in logger.info.call_args_list if "Skipping" in call.args[0]]
    assert skipped and skipped[0][1] == SERVED["offering"]["name"]


# --- Review fixes ------------------------------------------------------------------


def test_drift_never_logs_backend_setting_values(endpoint):
    offering = local_offering()
    offering["backend_settings"]["my_api_password"] = "HUNTER2"  # no schema marks it secret
    offering["backend_settings"]["qos_default"] = "high"
    logger = drift_warnings(load(offering))
    calls = logger.warning.call_args_list
    subjects = sorted(call.kwargs["configuration_drift"] for call in calls)
    assert subjects == ["backend_settings.my_api_password", "backend_settings.qos_default"]
    for call in calls:
        rendered = call.args[0] % call.args[1:] + repr(call.kwargs)
        assert "HUNTER2" not in rendered
        assert "high" not in rendered and "normal" not in rendered
        assert "differs" in rendered


@pytest.mark.parametrize(
    "sent",
    [
        {"verify_ssl": False},
        {"waldur_api_token": "waldur-chosen-token"},
        {"waldur_api_url": "https://elsewhere.example.com/api/", "configuration_source": "local"},
        {"oidc_client_secret": "x", "waldur_offering_uuid": "other"},
    ],
)
def test_waldur_cannot_set_bootstrap_fields(endpoint, served, sent):
    served["offering"].update(sent)
    with mock.patch.object(configuration_source, "logger") as logger:
        offering = load(bootstrap()).offerings[0]
    assert offering.verify_ssl is True
    assert offering.waldur_api_token == TOKEN
    assert offering.waldur_api_url == API_URL
    assert offering.waldur_offering_uuid == UUID
    assert offering.configuration_source == "waldur"
    assert offering.oidc_client_secret is None
    warning = logger.warning.call_args
    assert warning.args[2] == ", ".join(sorted(sent))


def test_oidc_offering_does_not_get_a_token_from_waldur(served):
    served["offering"]["waldur_api_token"] = "waldur-chosen-token"
    with respx.mock() as router:
        router.post("https://idp.example.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "jwt", "expires_in": 300})
        )
        router.get(ENDPOINT).mock(return_value=httpx.Response(200, json=served))
        utils._OIDC_TOKEN_CACHE.clear()
        try:
            offering = load(
                bootstrap(
                    waldur_api_token="",
                    oidc_token_url="https://idp.example.com/token",
                    oidc_client_id="agent",
                    oidc_client_secret="client-secret",
                )
            ).offerings[0]
        finally:
            utils._OIDC_TOKEN_CACHE.clear()
    assert offering.waldur_api_token == ""


@pytest.mark.parametrize(
    ("block", "named"),
    [
        ({"waldur_offering_uuid": UUID, "waldur_api_token": "secret-token-value"}, "waldur_api_url"),
        (
            {
                "waldur_api_url": API_URL,
                "waldur_offering_uuid": UUID,
                "oidc_token_url": "https://idp.example.com/token",
                "oidc_client_secret": "secret-oidc-value",
            },
            "oidc",
        ),
    ],
)
def test_incomplete_bootstrap_block_is_a_configuration_source_error(endpoint, block, named):
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load({"configuration_source": "waldur", **block})
    message = str(exc_info.value)
    assert UUID in message
    assert named in message
    assert "secret-token-value" not in message and "secret-oidc-value" not in message
    assert not endpoint.called


# --- A group of offerings selected by backend ID ---------------------------------

SP_UUID = "5e000000000000000000000000000001"
GROUP_ENDPOINT = f"https://waldur.example.com/api/marketplace-service-providers/{SP_UUID}/site_agent_config/"
UUID_Q = "0f000000000000000000000000000001"
UUID_U = "0f000000000000000000000000000002"


def group_entry(uuid, name):
    entry = copy.deepcopy(SERVED)
    entry["offering"]["name"] = f"{name} as Waldur names it in the offering"
    return {"offering_uuid": uuid, "offering_name": name, **entry}


def group_block(**extra):
    block = {
        "name": "Cluster",
        "configuration_source": "waldur",
        "waldur_api_url": API_URL,
        "waldur_api_token": TOKEN,
        "waldur_service_provider_uuid": SP_UUID,
        "waldur_offering_backend_id_startswith": "cluster",
        "backend_settings": {"ldap": {"bind_password": "s3cret"}},
    }
    block.update(extra)
    return block


@pytest.fixture
def group_served():
    return [group_entry(UUID_Q, "cluster-q"), group_entry(UUID_U, "cluster-u")]


@pytest.fixture
def group_endpoint(group_served):
    with respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=GROUP_ENDPOINT).mock(
            side_effect=lambda request: httpx.Response(200, json=group_served)
        )
        router.get(ENDPOINT).mock(side_effect=lambda request: httpx.Response(200, json=SERVED))
        yield route


def test_group_block_loads_one_offering_per_matching_offering(group_endpoint, group_served):
    offerings = load(group_block()).offerings

    assert [(o.waldur_offering_uuid, o.name) for o in offerings] == [(UUID_Q, "cluster-q"), (UUID_U, "cluster-u")]
    for offering in offerings:
        assert offering.configuration_source == "waldur"
        assert offering.configuration_version == SERVED["version"]
        assert offering.backend_type == "slurm"
        assert offering.waldur_api_url == API_URL
        assert offering.waldur_api_token == TOKEN
        assert offering.backend_settings["ldap"]["bind_password"] == "s3cret"
        assert not hasattr(offering, "waldur_service_provider_uuid")
    # Each offering has its own copy of the settings.
    assert offerings[0].backend_settings is not offerings[1].backend_settings

    request = group_endpoint.calls.last.request
    assert group_endpoint.call_count == 1
    assert request.url.params["backend_id_startswith"] == "cluster"
    assert "backend_id" not in request.url.params
    assert request.headers["Authorization"] == f"Token {TOKEN}"
    assert request.headers["User-Agent"] == "waldur-site-agent-test/1"


def test_exact_backend_id_is_sent_as_backend_id(group_endpoint, group_served):
    del group_served[1]
    block = group_block(waldur_offering_backend_id="cluster-q")
    del block["waldur_offering_backend_id_startswith"]
    offerings = load(block).offerings
    assert [o.waldur_offering_uuid for o in offerings] == [UUID_Q]
    params = group_endpoint.calls.last.request.url.params
    assert params["backend_id"] == "cluster-q"
    assert "backend_id_startswith" not in params


def test_group_and_single_offering_blocks_load_together(group_endpoint):
    offerings = load(bootstrap(), group_block()).offerings
    assert [o.waldur_offering_uuid for o in offerings] == [UUID, UUID_Q, UUID_U]


@pytest.mark.parametrize(
    ("change", "named"),
    [
        ({"waldur_offering_uuid": UUID}, "sets both waldur_offering_uuid and waldur_service_provider_uuid"),
        ({"waldur_offering_backend_id": "cluster-q"}, "selects a group of offerings incompletely"),
        ({"waldur_service_provider_uuid": None}, "selects a group of offerings incompletely"),
        ({"waldur_offering_backend_id_startswith": None}, "selects a group of offerings incompletely"),
        ({"waldur_offering_backend_id_startswith": ""}, "waldur_offering_backend_id_startswith must be a non-empty"),
    ],
)
def test_malformed_group_block_fails_naming_the_block(group_endpoint, change, named):
    block = group_block()
    for key, value in change.items():
        if value is None:
            del block[key]
        else:
            block[key] = value
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(bootstrap(), block)
    message = str(exc_info.value)
    assert message.startswith("Offering block 2 ('Cluster')")
    assert named in message
    assert not group_endpoint.called


def test_block_naming_no_offering_fails(endpoint):
    block = bootstrap()
    del block["waldur_offering_uuid"]
    with pytest.raises(ConfigurationSourceError, match=r"^Offering block 1 names no offering"):
        load(block)
    assert not endpoint.called


def test_no_matching_offering_fails_naming_the_block(group_endpoint, group_served):
    group_served.clear()
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(group_block())
    message = str(exc_info.value)
    assert f"Offering group 'Cluster' (service provider {SP_UUID}, backend_id_startswith 'cluster')" in message
    assert "no offering of the service provider matches" in message
    assert GROUP_ENDPOINT in message


@pytest.mark.parametrize("status", [400, 403, 404, 500])
def test_group_error_status_fails_with_block_url_and_status(status):
    with respx.mock() as router:
        router.get(url__startswith=GROUP_ENDPOINT).mock(return_value=httpx.Response(status, json={"detail": "Nope."}))
        with pytest.raises(ConfigurationSourceError) as exc_info:
            load(group_block())
    message = str(exc_info.value)
    assert "Offering group 'Cluster'" in message
    assert f"HTTP {status}" in message
    assert f"{GROUP_ENDPOINT}?backend_id_startswith=cluster" in message
    assert "Nope." in message


def test_group_unreachable_waldur_fails():
    with respx.mock() as router:
        router.get(url__startswith=GROUP_ENDPOINT).mock(side_effect=httpx.ConnectError("Connection refused"))
        with pytest.raises(ConfigurationSourceError, match="Connection refused"):
            load(group_block())


@pytest.mark.parametrize("body", [b"<html>", b'{"offering": {}}', b'[{"offering": {}}]', b'[{"offering_uuid": "x"}]'])
def test_group_unusable_response_fails(body):
    with respx.mock() as router:
        router.get(url__startswith=GROUP_ENDPOINT).mock(return_value=httpx.Response(200, content=body))
        with pytest.raises(ConfigurationSourceError, match="Offering group 'Cluster'"):
            load(group_block())


def test_offering_listed_on_its_own_and_in_a_group_fails(group_endpoint, group_served):
    group_served[0]["offering_uuid"] = UUID
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(bootstrap(name="Single"), group_block())
    message = str(exc_info.value)
    assert f"Offering {UUID} is configured by more than one block" in message
    assert f"block 1 (Offering 'Single' ({UUID}))" in message
    assert "block 2 (Offering group 'Cluster'" in message


def test_offering_matched_by_two_group_blocks_fails(group_endpoint):
    with pytest.raises(ConfigurationSourceError, match=f"Offering {UUID_Q} is configured by more than one block"):
        load(group_block(), group_block(name="Cluster again"))


@pytest.mark.parametrize("value", ["not-a-uuid", f"{SP_UUID}/../../marketplace-provider-offerings/{UUID}"])
def test_service_provider_must_be_a_uuid(group_endpoint, value):
    with pytest.raises(ConfigurationSourceError, match="waldur_service_provider_uuid must be a UUID"):
        load(group_block(waldur_service_provider_uuid=value))
    assert group_endpoint.call_count == 0


def test_duplicate_is_found_whatever_the_uuid_spelling():
    dashed = "0f000000-0000-0000-0000-000000000001"
    with pytest.raises(ConfigurationSourceError, match=f"Offering {dashed} is configured by more than one block"):
        configuration_source.check_duplicate_offerings(
            [(dashed, "block 1", False), (UUID_Q, "block 2", True)]
        )


def test_group_block_without_configuration_source_waldur_fails_saying_so():
    block = group_block()
    del block["configuration_source"]
    with pytest.raises(ConfigurationSourceError, match="only a block with configuration_source: waldur"):
        load(block)


def test_local_blocks_sharing_a_uuid_load_as_before(endpoint):
    assert len(load(local_offering(), local_offering(name="Same offering again")).offerings) == 2


def test_missing_secret_for_a_matched_offering_fails_naming_block_and_offering(group_endpoint, group_served):
    group_served[1]["secrets"] = [
        {"field": "backend_settings.target_api_token", "backend": "waldur", "requirement": "required"}
    ]
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(group_block())
    message = str(exc_info.value)
    assert message.startswith("Offering group 'Cluster'")
    assert f"Offering 'cluster-u' ({UUID_U})" in message
    assert "backend_settings.target_api_token (backend waldur)" in message


def test_group_block_refuses_non_bootstrap_keys(group_endpoint):
    with pytest.raises(ConfigurationSourceError, match=r"Offering group 'Cluster'.*Set these in Waldur instead: backend_type$"):
        load(group_block(backend_type="slurm"))
    assert not group_endpoint.called


def test_invalid_served_configuration_in_a_group_names_block_and_offering(group_endpoint, group_served):
    group_served[0]["offering"]["stomp_enabled"] = "maybe"
    with pytest.raises(ConfigurationSourceError) as exc_info:
        load(group_block())
    message = str(exc_info.value)
    assert message.startswith("Offering group 'Cluster'")
    assert "cluster-q" in message and "stomp_enabled" in message
    assert "s3cret" not in message
