"""Tests for the standalone create_homedirs_for_offering_users command."""

from unittest.mock import MagicMock, patch

import pytest

from waldur_site_agent.common import structures
from waldur_site_agent.common.utils import create_homedirs_for_offering_users

from tests.fixtures import ConcreteBackend


def _offering(backend_type, **backend_settings):
    return structures.Offering(
        name=f"{backend_type} offering",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="test-token",
        waldur_offering_uuid="11111111-1111-1111-1111-111111111111",
        backend_type=backend_type,
        backend_settings=backend_settings,
    )


def _declaring(supports):
    """A backend class declaring (or not) the homedir capability."""
    return type("DeclaredBackend", (ConcreteBackend,), {"supports_user_homedirs": supports})


@pytest.fixture
def backend():
    """An instance stub whose create_user_homedirs calls can be asserted on."""
    instance = ConcreteBackend({}, {})
    instance.create_user_homedirs = MagicMock()
    return instance


def _run(offerings, backend, supports=True, users_side_effect=None):
    configuration = MagicMock()
    configuration.waldur_offerings = offerings

    sync_all = (
        MagicMock(side_effect=users_side_effect)
        if users_side_effect
        else MagicMock(return_value=[MagicMock(username="alice")])
    )

    with (
        patch("waldur_site_agent.common.utils.init_configuration", return_value=configuration),
        patch("waldur_site_agent.common.utils.get_client", return_value=MagicMock()),
        patch(
            "waldur_site_agent.common.utils.get_backend_class_for_offering",
            return_value=_declaring(supports),
        ),
        patch(
            "waldur_site_agent.common.utils.get_backend_for_offering",
            return_value=(backend, "test"),
        ) as mock_build,
        patch(
            "waldur_site_agent.common.utils.marketplace_offering_users_list.sync_all",
            new=sync_all,
        ),
    ):
        create_homedirs_for_offering_users()

    return mock_build, sync_all


def test_non_slurm_backend_declaring_support_is_not_skipped(backend):
    """The gate follows the declared capability, not the backend type."""
    _run([_offering("custom-hpc")], backend)

    backend.create_user_homedirs.assert_called_once_with({"alice"}, "0077")


def test_backend_without_capability_is_skipped(backend):
    """A backend that cannot create homedirs is left alone."""
    _run([_offering("harbor")], backend, supports=False)

    backend.create_user_homedirs.assert_not_called()


def test_skipped_offering_backend_is_never_constructed(backend):
    """Skipping must not build the backend — construction opens clients and can raise."""
    mock_build, _ = _run([_offering("harbor")], backend, supports=False)

    mock_build.assert_not_called()


def test_offering_can_still_opt_out(backend):
    """enable_user_homedir_account_creation=False wins over the capability."""
    mock_build, _ = _run(
        [_offering("slurm", enable_user_homedir_account_creation=False)], backend
    )

    backend.create_user_homedirs.assert_not_called()
    mock_build.assert_not_called()


def test_configured_umask_is_passed_through(backend):
    """The offering's umask reaches the backend."""
    _run([_offering("slurm", default_homedir_umask="0750")], backend)

    backend.create_user_homedirs.assert_called_once_with({"alice"}, "0750")


def test_one_failing_offering_does_not_abort_the_rest(backend):
    """A broken offering is logged and skipped; later offerings still run."""
    seen = []

    def _users(*args, **kwargs):
        seen.append(kwargs["offering_uuid"])
        if len(seen) == 1:
            msg = "Waldur unreachable"
            raise RuntimeError(msg)
        return [MagicMock(username="alice")]

    _run(
        [_offering("slurm"), _offering("slurm")],
        backend,
        users_side_effect=_users,
    )

    assert len(seen) == 2
    backend.create_user_homedirs.assert_called_once_with({"alice"}, "0077")


class TestHomedirCapabilityContract:
    """Every backend opting into homedirs must validate the settings the same way."""

    @staticmethod
    def _homedir_backends():
        from waldur_site_agent.common.utils import BACKENDS

        return {
            name: cls
            for name, (cls, _dist, _version) in BACKENDS.items()
            if cls.supports_user_homedirs
        }

    def test_slurm_declares_support(self):
        """SLURM opts in. The gate is the flag, so any backend may join it."""
        names = set(self._homedir_backends())

        assert "slurm" in names

    def test_declaring_backends_validate_homedir_settings(self):
        """A backend that opts in registers a schema inheriting the shared fields."""
        from waldur_site_agent.common.plugin_schemas import (
            HomedirSettingsSchema,
            get_plugin_backend_settings_schemas,
        )

        schemas = get_plugin_backend_settings_schemas()

        for name in self._homedir_backends():
            assert name in schemas, f"{name} declares supports_user_homedirs but has no schema"
            assert issubclass(schemas[name], HomedirSettingsSchema), (
                f"{name}'s settings schema must inherit HomedirSettingsSchema"
            )

    def test_api_only_backends_do_not_declare_support(self):
        """Backends with no POSIX host must not shell out to mkhomedir_helper."""
        names = set(self._homedir_backends())

        assert not names & {"harbor", "digitalocean", "rancher", "waldur", "okd"}
