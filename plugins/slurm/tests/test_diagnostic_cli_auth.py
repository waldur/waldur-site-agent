"""The SLURM diagnostics CLI must authenticate OIDC-only offerings via OIDC."""

from types import SimpleNamespace
from unittest import mock

from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_slurm import diagnostic_cli


def test_diagnostics_cli_builds_client_with_oidc_bearer_token():
    offering = Offering(
        name="oidc-slurm",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_offering_uuid="12345678123412341234123456789abc",
        waldur_api_token="",
        oidc_token_url="https://idp.example.com/token",
        oidc_client_id="agent",
        oidc_client_secret="secret",  # noqa: S106
        backend_type="slurm",
    )
    configuration = SimpleNamespace(waldur_offerings=[offering], global_proxy="")
    with (
        mock.patch("sys.argv", ["waldur_site_diagnose_slurm_account", "acct"]),
        mock.patch.object(diagnostic_cli, "configure_logger"),
        mock.patch.object(common_utils, "load_configuration", return_value=configuration),
        mock.patch.object(diagnostic_cli, "find_slurm_offering", return_value=offering),
        mock.patch.object(common_utils, "fetch_oidc_token", return_value="jwt-from-idp"),
        mock.patch.object(
            common_utils, "get_client", side_effect=RuntimeError("stop")
        ) as get_client,
    ):
        assert diagnostic_cli.main() == 1

    args, kwargs = get_client.call_args
    token = kwargs.get("access_token", args[1] if len(args) > 1 else None)
    prefix = kwargs.get("token_prefix", args[5] if len(args) > 5 else "Token")
    assert token == "jwt-from-idp"
    assert prefix == "Bearer"
