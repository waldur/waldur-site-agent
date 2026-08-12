"""Flavour resolution and the settings each flavour demands."""

from unittest.mock import patch

import pytest

from waldur_site_agent_ceph_s3.settings import (
    CROIT,
    RADOSGW,
    resolve_flavour,
    validate_settings,
)

CROIT_SETTINGS = {
    "api_url": "https://croit.example.org",
    "token": "t",
    "s3_endpoint": "https://rgw.example.org",
}
RADOSGW_SETTINGS = {
    "flavour": "radosgw",
    "s3_endpoint": "https://rgw.example.org",
    "admin_access_key": "ADMINACCESSKEY000000",
    "admin_secret_key": "s" * 40,
}


def test_flavour_defaults_to_croit_so_existing_offerings_keep_working():
    assert resolve_flavour({}) == CROIT
    assert resolve_flavour(CROIT_SETTINGS) == CROIT


def test_radosgw_flavour_is_recognised():
    assert resolve_flavour(RADOSGW_SETTINGS) == RADOSGW


def test_flavour_is_case_insensitive():
    assert resolve_flavour({"flavour": "RadosGW"}) == RADOSGW


def test_unknown_flavour_is_refused_by_name():
    """Naming the value matters: the likely cause is a typo."""
    with pytest.raises(ValueError, match="quobyte"):
        resolve_flavour({"flavour": "quobyte"})


class TestCroitValidation:
    def test_credentials_are_required(self):
        with pytest.raises(ValueError, match="token"):
            validate_settings({"api_url": "https://croit.example.org"}, CROIT)

    def test_a_username_without_a_password_is_not_enough(self):
        with pytest.raises(ValueError, match="token"):
            validate_settings(
                {"api_url": "https://croit.example.org", "username": "u"}, CROIT
            )

    def test_radosgw_settings_are_refused(self):
        """Half-migrated offerings must fail loudly, not ignore the stray key."""
        settings = dict(CROIT_SETTINGS, admin_access_key="AK")
        with pytest.raises(ValueError, match="admin_access_key"):
            validate_settings(settings, CROIT)

    def test_a_complete_configuration_passes(self):
        validate_settings(CROIT_SETTINGS, CROIT)


class TestRadosGWValidation:
    def test_admin_credentials_are_required(self):
        settings = dict(RADOSGW_SETTINGS)
        del settings["admin_secret_key"]
        with pytest.raises(ValueError, match="admin_secret_key"):
            validate_settings(settings, RADOSGW)

    def test_the_gateway_address_is_required(self):
        """Admin Ops hangs off the gateway, so there is nowhere to send without it."""
        settings = dict(RADOSGW_SETTINGS)
        del settings["s3_endpoint"]
        with pytest.raises(ValueError, match="s3_endpoint"):
            validate_settings(settings, RADOSGW)

    def test_croit_settings_are_refused(self):
        settings = dict(RADOSGW_SETTINGS, api_url="https://croit.example.org")
        with pytest.raises(ValueError, match="api_url"):
            validate_settings(settings, RADOSGW)

    def test_every_stray_croit_setting_is_named_at_once(self):
        """One error listing all of them beats three rounds of trial and error."""
        settings = dict(RADOSGW_SETTINGS, api_url="x", token="y")
        with pytest.raises(ValueError) as excinfo:
            validate_settings(settings, RADOSGW)
        assert "api_url" in str(excinfo.value)
        assert "token" in str(excinfo.value)

    def test_radosgw_rejects_the_settings_it_cannot_honour(self):
        """Silently dropping a setting reads to an operator as configured."""
        with pytest.raises(ValueError, match="default_storage_class"):
            validate_settings(
                dict(RADOSGW_SETTINGS, default_storage_class="STANDARD_IA"), RADOSGW
            )
        with pytest.raises(ValueError, match="default_tenant"):
            validate_settings(dict(RADOSGW_SETTINGS, default_tenant="acme"), RADOSGW)

    def test_a_complete_configuration_passes(self):
        validate_settings(RADOSGW_SETTINGS, RADOSGW)


class TestBackendWiring:
    """The flavour must reach the client the backend actually uses."""

    @staticmethod
    def _components():
        return {
            "s3_storage": {
                "accounting_type": "usage",
                "backend_name": "storage",
                "unit_factor": 1000000000,
            }
        }

    def test_croit_settings_build_a_croit_client(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend
        from waldur_site_agent_ceph_s3.clients.croit import CroitClient

        backend = CephS3Backend(dict(CROIT_SETTINGS), self._components())
        assert isinstance(backend.client, CroitClient)
        assert backend.flavour == CROIT

    def test_radosgw_settings_build_an_admin_ops_client(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend
        from waldur_site_agent_ceph_s3.clients.radosgw import RadosGWClient

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), self._components())
        assert isinstance(backend.client, RadosGWClient)
        assert backend.flavour == RADOSGW

    def test_the_admin_ops_client_targets_the_gateway_not_a_management_host(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), self._components())
        assert backend.client.base_url == "https://rgw.example.org/admin"

    def test_a_renamed_rgw_admin_entry_is_honoured(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        settings = dict(RADOSGW_SETTINGS, admin_path="ceph-admin")
        backend = CephS3Backend(settings, self._components())
        assert backend.client.base_url == "https://rgw.example.org/ceph-admin"

    def test_a_mixed_configuration_never_reaches_a_client(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        settings = dict(RADOSGW_SETTINGS, api_url="https://croit.example.org")
        with pytest.raises(ValueError, match="api_url"):
            CephS3Backend(settings, self._components())


class TestAutoKeyCleanupIsCroitOnly:
    """generate-key=False means there is nothing to clean up on radosgw."""

    @staticmethod
    def _components():
        return {
            "s3_storage": {
                "accounting_type": "usage",
                "backend_name": "storage",
                "unit_factor": 1000000000,
            }
        }

    def test_radosgw_does_not_hunt_for_auto_generated_keys(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), self._components())
        with patch.object(backend.client, "list_user_keys") as list_user_keys, patch.object(
            backend.client, "delete_user_key"
        ) as delete_user_key:
            backend._remove_auto_generated_keys("user-1")

        list_user_keys.assert_not_called()
        delete_user_key.assert_not_called()

    def test_croit_still_removes_them(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(CROIT_SETTINGS), self._components())
        keys = [{"access_key": "AUTOGENERATED0000000", "secret_key": "s"}]
        with patch.object(backend.client, "list_user_keys", return_value=keys), patch.object(
            backend.client, "delete_user_key"
        ) as delete_user_key:
            backend._remove_auto_generated_keys("user-1")

        delete_user_key.assert_called_once_with("user-1", "AUTOGENERATED0000000")


class TestDiagnostics:
    """Diagnostics must describe whichever gateway is configured."""

    @staticmethod
    def _components():
        return {
            "s3_storage": {
                "accounting_type": "usage",
                "backend_name": "storage",
                "unit_factor": 1000000000,
            }
        }

    def test_radosgw_diagnostics_name_the_gateway(self, caplog):
        """Reading client.api_url here logged an AttributeError for radosgw."""
        import logging

        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(RADOSGW_SETTINGS), self._components())
        with patch.object(backend.client, "ping", return_value=True), patch.object(
            backend.client, "list_users", return_value=[]
        ), caplog.at_level(logging.INFO):
            backend.diagnostics()

        logged = caplog.text
        assert "https://rgw.example.org/admin" in logged
        assert "AttributeError" not in logged

    def test_croit_diagnostics_still_name_the_management_api(self, caplog):
        import logging

        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(CROIT_SETTINGS), self._components())
        with patch.object(backend.client, "ping", return_value=True), patch.object(
            backend.client, "list_users", return_value=[]
        ), caplog.at_level(logging.INFO):
            backend.diagnostics()

        assert "https://croit.example.org/api" in caplog.text


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
            CephS3Backend(dict(CROIT_SETTINGS), self._storage(unit_factor=1))

    def test_a_configured_unit_factor_is_accepted(self):
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(dict(CROIT_SETTINGS), self._storage())
        assert backend.flavour == CROIT

    def test_an_offering_with_no_storage_component_is_accepted(self):
        """Not every offering meters storage; only a storage component is checked."""
        from waldur_site_agent_ceph_s3.backend import CephS3Backend

        backend = CephS3Backend(
            dict(CROIT_SETTINGS),
            {"s3_user": {"accounting_type": "limit", "backend_name": "user_quota"}},
        )
        assert backend.flavour == CROIT

    def test_the_reporting_backend_is_guarded_by_the_same_check(self):
        """CroitUsageBackend divides by the same factor, so it must not bypass it."""
        from waldur_site_agent_ceph_s3.reporting import CroitUsageBackend

        with pytest.raises(ValueError, match="unit_factor"):
            CroitUsageBackend(dict(CROIT_SETTINGS), self._storage(unit_factor=1))
