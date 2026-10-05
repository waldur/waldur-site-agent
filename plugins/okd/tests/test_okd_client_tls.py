"""TLS verification settings the OKD client hands to its HTTP and Kubernetes clients."""

from unittest import mock

import pytest
from waldur_site_agent_okd import client as okd_client


def _kubernetes_configuration(verify_cert):
    """Build an OkdClient and return the Configuration given to the Kubernetes client."""
    settings = {"api_url": "https://okd.example.com:6443", "token": "t"}
    if verify_cert is not None:
        settings["verify_cert"] = verify_cert
    with mock.patch.object(okd_client.k8s_client, "ApiClient") as api_client, mock.patch.object(
        okd_client.k8s_client, "CoreV1Api"
    ), mock.patch.object(okd_client.httpx, "Client"):
        okd_client.OkdClient({}, settings)
    return api_client.call_args.args[0]


def test_ca_bundle_path_is_used_for_kubernetes_calls():
    configuration = _kubernetes_configuration("/etc/pki/okd-ca.pem")
    assert configuration.verify_ssl is True
    assert configuration.ssl_ca_cert == "/etc/pki/okd-ca.pem"


def test_false_disables_verification():
    configuration = _kubernetes_configuration(False)
    assert configuration.verify_ssl is False
    assert not configuration.ssl_ca_cert


@pytest.mark.parametrize("verify_cert", [True, None])
def test_true_or_unset_verifies_with_the_system_trust_store(verify_cert):
    configuration = _kubernetes_configuration(verify_cert)
    assert configuration.verify_ssl is True
    assert not configuration.ssl_ca_cert
