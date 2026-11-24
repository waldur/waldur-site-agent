"""ceph-s3 manages users through RGW Admin Ops only, and refuses croit management."""

from unittest.mock import patch

import pytest

from waldur_site_agent_ceph_s3.settings import (
    RADOSGW,
    resolve_flavour,
    validate_settings,
)

from conftest import CROIT_REPORTING, RADOSGW_SETTINGS

# An offering as the removed croit flavour configured it.
OLD_CROIT_SETTINGS = {
    "api_url": "https://croit.example.org",
    "token": "t",
    "s3_endpoint": "https://rgw.example.org",
}
COMPONENTS = {
    "s3_storage": {"accounting_type": "usage", "backend_name": "storage", "unit_factor": 10**9}
}


class TestFlavour:
    def test_radosgw_is_the_default(self):
        assert resolve_flavour({}) == RADOSGW
        assert resolve_flavour(RADOSGW_SETTINGS) == RADOSGW

    def test_flavour_is_case_insensitive(self):
        assert resolve_flavour({"flavour": "RadosGW"}) == RADOSGW

    def test_an_unknown_flavour_is_refused_by_name(self):
        """Naming the value matters: the likely cause is a typo."""
        with pytest.raises(ValueError, match="quobyte"):
            resolve_flavour({"flavour": "quobyte"})

    @pytest.mark.parametrize(
        ("settings", "named"),
        [
            ({"flavour": "croit"}, "flavour: croit"),
            ({"flavour": "CROIT"}, "flavour: croit"),
            (OLD_CROIT_SETTINGS, "api_url, token"),
            ({"username": "u", "password": "p"}, "password, username"),
            (dict(RADOSGW_SETTINGS, api_url="https://croit.example.org"), "api_url"),
        ],
    )
    def test_croit_management_is_refused_with_the_migration(self, settings, named):
        """An offering not yet migrated fails at start, saying what it still has and
        every step that moves it -- not just that something is wrong."""
        with pytest.raises(ValueError) as error:
            resolve_flavour(settings)
        message = str(error.value)
        assert "no longer manages users through croit" in message
        assert named in message
        for step in ("admin_access_key", "croit_reporting", "bucket_state_file", "users=*"):
            assert step in message


class TestValidation:
    @pytest.mark.parametrize("missing", ["s3_endpoint", "admin_access_key", "admin_secret_key"])
    def test_the_gateway_and_admin_credentials_are_required(self, missing):
        settings = {k: v for k, v in RADOSGW_SETTINGS.items() if k != missing}
        with pytest.raises(ValueError, match=missing):
            validate_settings(settings)

    def test_a_storage_class_needs_a_placement(self):
        """RGW applies default-storage-class only within the default-placement rule;
        alone it would be dropped without a word."""
        with pytest.raises(ValueError, match="default_placement"):
            validate_settings(dict(RADOSGW_SETTINGS, default_storage_class="COLD"))

    def test_a_storage_class_with_its_placement_passes(self):
        validate_settings(
            dict(
                RADOSGW_SETTINGS,
                default_placement="default-placement",
                default_storage_class="COLD",
            )
        )

    def test_a_complete_configuration_passes(self):
        validate_settings(RADOSGW_SETTINGS)
        validate_settings({k: v for k, v in RADOSGW_SETTINGS.items() if k != "flavour"})


class TestBackendWiring:
    def test_the_client_is_admin_ops_on_the_gateway(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend
        from waldur_site_agent_ceph_s3.clients.radosgw import RadosGWClient

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), COMPONENTS)
        assert isinstance(backend.client, RadosGWClient)
        assert backend.client.base_url == "https://rgw.example.org/admin"

    def test_a_renamed_rgw_admin_entry_is_honoured(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        settings = dict(RADOSGW_SETTINGS, admin_path="ceph-admin")
        backend = CephS3Backend(settings, COMPONENTS)
        assert backend.client.base_url == "https://rgw.example.org/ceph-admin"

    def test_an_old_croit_offering_never_reaches_a_client(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        with patch("waldur_site_agent_ceph_s3.backend.RadosGWClient") as client:
            with pytest.raises(ValueError, match="through croit"):
                CephS3Backend(dict(OLD_CROIT_SETTINGS), COMPONENTS)
        client.assert_not_called()

    def test_the_storage_class_reaches_user_creation(self):
        from waldur_site_agent_ceph_s3.clients.radosgw import RadosGWClient

        client = RadosGWClient(
            endpoint="https://rgw.example.org",
            access_key="ADMINACCESSKEY000000",
            secret_key="s" * 40,
        )
        with patch.object(client, "_request") as request:
            client.create_user(
                uid="t$root",
                name="root",
                default_placement="default-placement",
                default_storage_class="COLD",
            )
        params = request.call_args.kwargs["params"]
        assert params["default-placement"] == "default-placement"
        assert params["default-storage-class"] == "COLD"

    def test_diagnostics_name_the_gateway(self, caplog):
        import logging

        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), COMPONENTS)
        with patch.object(backend.client, "ping", return_value=True), patch.object(
            backend.client, "list_users", return_value=[]
        ), caplog.at_level(logging.INFO):
            backend.diagnostics()

        assert "https://rgw.example.org/admin" in caplog.text
        assert "AttributeError" not in caplog.text


class TestComponentValidation:
    """`unit_factor` carries a default, so an omitted one arrives as a real value."""

    @staticmethod
    def _storage(**overrides):
        component = {
            "accounting_type": "usage",
            "backend_name": "storage",
            "label": "Storage",
            "measured_unit": "GB-day",
            "unit_factor": 1000000000,
        }
        component.update(overrides)
        return {"s3_storage": component}

    def test_an_unconfigured_unit_factor_is_refused(self):
        """At 1 the ordered ceiling is sent as bytes and the usage is byte-days.

        A 5 GB order becomes a 5-byte quota -- the tenant can store nothing -- and
        the same factor divides the measured bytes, so the invoice is 10^9 times
        the real figure. Both are silent, which is why this fails at startup.
        """
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        with pytest.raises(ValueError, match="unit_factor"):
            CephS3Backend(dict(RADOSGW_SETTINGS), self._storage(unit_factor=1))

    def test_a_configured_unit_factor_is_accepted(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), self._storage())
        assert backend.flavour == RADOSGW

    def test_an_offering_with_no_storage_component_is_accepted(self):
        """Not every offering meters storage; only a storage component is checked."""
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(
            dict(RADOSGW_SETTINGS),
            {"s3_user": {"accounting_type": "limit", "backend_name": "user_quota"}},
        )
        assert backend.flavour == RADOSGW

    def test_the_reporting_backend_is_guarded_by_the_same_check(self):
        """CroitUsageBackend divides by the same factor, so it must not bypass it."""
        from waldur_site_agent_ceph_s3.reporting import CroitUsageBackend

        with pytest.raises(ValueError, match="unit_factor"):
            CroitUsageBackend(
                dict(RADOSGW_SETTINGS, croit_reporting=CROIT_REPORTING),
                self._storage(unit_factor=1),
            )
