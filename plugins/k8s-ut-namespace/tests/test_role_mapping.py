"""Tests for role mapping constants and configurable mappings."""

from unittest.mock import patch

from waldur_site_agent_k8s_ut_namespace.backend import (
    DEFAULT_ROLE_MAPPING,
    DEFAULT_COMPONENT_QUOTA_MAPPING,
    K8sUtNamespaceBackend,
    NS_ROLES,
    NS_ROLE_TO_CR_GROUP_FIELD,
    NS_ROLE_TO_CR_USER_FIELD,
)


class TestDefaultRoleMapping:
    """Tests for default Waldur role to namespace role mapping."""

    def test_manager_maps_to_admin(self):
        assert DEFAULT_ROLE_MAPPING["manager"] == "admin"

    def test_admin_maps_to_admin(self):
        assert DEFAULT_ROLE_MAPPING["admin"] == "admin"

    def test_member_maps_to_readwrite(self):
        assert DEFAULT_ROLE_MAPPING["member"] == "readwrite"

    def test_unknown_role_not_in_mapping(self):
        assert "unknown" not in DEFAULT_ROLE_MAPPING

    def test_ns_roles_tuple(self):
        assert NS_ROLES == ("admin", "readwrite", "readonly")


class TestConfigurableRoleMapping:
    """Tests for role mapping overridden via backend_settings."""

    def test_custom_role_mapping_overrides_defaults(
        self, backend_settings, backend_components
    ):
        backend_settings["role_mapping"] = {"observer": "readonly"}
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ), patch(
            "waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"
        ):
            backend = K8sUtNamespaceBackend(backend_settings, backend_components)

        # Custom mapping added
        assert backend.role_mapping["observer"] == "readonly"
        # Defaults preserved
        assert backend.role_mapping["manager"] == "admin"
        assert backend.role_mapping["member"] == "readwrite"

    def test_custom_role_mapping_can_override_default_entry(
        self, backend_settings, backend_components
    ):
        backend_settings["role_mapping"] = {"member": "readonly"}
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ), patch(
            "waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"
        ):
            backend = K8sUtNamespaceBackend(backend_settings, backend_components)

        assert backend.role_mapping["member"] == "readonly"

    def test_no_custom_mapping_uses_defaults(
        self, backend_settings, backend_components
    ):
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ), patch(
            "waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"
        ):
            backend = K8sUtNamespaceBackend(backend_settings, backend_components)

        assert backend.role_mapping == DEFAULT_ROLE_MAPPING


class TestConfigurableComponentQuotaMapping:
    """Tests for component quota mapping overridden via backend_settings."""

    def test_custom_component_mapping(
        self, backend_settings, backend_components
    ):
        backend_settings["component_quota_mapping"] = {"vram": "nvidia.com/vram"}
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ), patch(
            "waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"
        ):
            backend = K8sUtNamespaceBackend(backend_settings, backend_components)

        # Custom mapping added
        assert backend.component_quota_mapping["vram"] == "nvidia.com/vram"
        # Defaults preserved
        assert backend.component_quota_mapping["cpu"] == "cpu"
        assert backend.component_quota_mapping["ram"] == "memory"

    def test_no_custom_mapping_uses_defaults(
        self, backend_settings, backend_components
    ):
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ), patch(
            "waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"
        ):
            backend = K8sUtNamespaceBackend(backend_settings, backend_components)

        assert backend.component_quota_mapping == DEFAULT_COMPONENT_QUOTA_MAPPING


class TestCrFieldMapping:
    """Tests for NS role to CRD field name mapping."""

    def test_admin_group_field(self):
        assert NS_ROLE_TO_CR_GROUP_FIELD["admin"] == "adminGroups"

    def test_readwrite_group_field(self):
        assert NS_ROLE_TO_CR_GROUP_FIELD["readwrite"] == "rwGroups"

    def test_readonly_group_field(self):
        assert NS_ROLE_TO_CR_GROUP_FIELD["readonly"] == "roGroups"

    def test_admin_user_field(self):
        assert NS_ROLE_TO_CR_USER_FIELD["admin"] == "adminUsers"

    def test_readwrite_user_field(self):
        assert NS_ROLE_TO_CR_USER_FIELD["readwrite"] == "rwUsers"

    def test_readonly_user_field(self):
        assert NS_ROLE_TO_CR_USER_FIELD["readonly"] == "roUsers"

    def test_all_ns_roles_have_cr_fields(self):
        for role in NS_ROLES:
            assert role in NS_ROLE_TO_CR_GROUP_FIELD
            assert role in NS_ROLE_TO_CR_USER_FIELD
