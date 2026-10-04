"""The SLURM account diagnostics CLI must talk to SLURM the way the backend does.

It used to build ``SlurmClient(slurm_tres)``, ignoring the offering's
``slurm_bin_path``, ``cluster_name`` and ``execution_mode``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_slurm import diagnostic_cli
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.rest_client import SlurmRestClient


def _offering(**backend_settings: Any) -> Offering:
    return Offering(
        name="slurm",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_offering_uuid="12345678123412341234123456789abc",
        waldur_api_token="token",  # noqa: S106
        backend_type="slurm",
        backend_settings=backend_settings,
        backend_components={
            "cpu": {
                "limit": 10,
                "measured_unit": "k-Hours",
                "unit_factor": 60000,
                "accounting_type": "limit",
                "label": "CPU",
            }
        },
    )


def _client_used_by_cli(offering: Offering) -> object:
    configuration = SimpleNamespace(waldur_offerings=[offering], global_proxy="")
    service = mock.Mock()
    service.diagnose_account.side_effect = RuntimeError("stop")
    with (
        mock.patch("sys.argv", ["waldur_site_diagnose_slurm_account", "acct"]),
        mock.patch.object(diagnostic_cli, "configure_logger"),
        mock.patch.object(common_utils, "load_configuration", return_value=configuration),
        mock.patch.object(diagnostic_cli, "find_slurm_offering", return_value=offering),
        mock.patch.object(common_utils, "get_client_for_offering", return_value=mock.Mock()),
        mock.patch.object(
            diagnostic_cli, "SlurmAccountDiagnosticService", return_value=service
        ) as service_class,
        pytest.raises(RuntimeError, match="stop"),
    ):
        diagnostic_cli.main()
    return service_class.call_args.kwargs["slurm_client"]


def test_cli_uses_the_configured_binary_path_and_cluster() -> None:
    client = _client_used_by_cli(
        _offering(slurm_bin_path="/opt/slurm/bin", cluster_name="alpha")
    )

    assert isinstance(client, SlurmClient)
    assert client.cluster_name == "alpha"
    assert client.slurm_bin_path == "/opt/slurm/bin"
    assert "cluster=alpha" in client._inject_cluster_filter(
        ["show", "account", "x"], "sacctmgr"
    )


def test_cli_defaults_match_the_backend() -> None:
    client = _client_used_by_cli(_offering())

    assert isinstance(client, SlurmClient)
    assert client.slurm_bin_path == "/usr/bin"
    assert client.cluster_name is None


def test_cli_uses_the_rest_client_in_rest_mode() -> None:
    client = _client_used_by_cli(
        _offering(
            execution_mode="rest",
            cluster_name="alpha",
            rest_api={"url": "https://slurmrestd.example.com", "token_env": "SLURM_JWT"},
        )
    )

    assert isinstance(client, SlurmRestClient)
    assert client.cluster_name == "alpha"
