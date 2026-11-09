"""The backend is registered under ``k8s-namespace`` and its former name ``k8s-ut-namespace``."""

from waldur_site_agent.common import plugin_schemas, utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_k8s_ut_namespace.backend import (
    K8sNamespaceBackend,
    K8sUtNamespaceBackend,
)
from waldur_site_agent_k8s_ut_namespace.schemas import K8sUtNamespaceBackendSettingsSchema

from conftest import make_backend


def _offering(name, settings, components):
    return Offering(
        name="Backend names",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="token",
        waldur_offering_uuid="0" * 32,
        backend_type=name,
        order_processing_backend=name,
        membership_sync_backend=name,
        reporting_backend=name,
        backend_settings=settings,
        backend_components={
            key: {"measured_unit": "units", "label": key, **value}
            for key, value in components.items()
        },
    )


class TestRegistration:
    def test_both_names_are_backends(self):
        assert utils.BACKENDS["k8s-namespace"][0] is K8sNamespaceBackend
        assert utils.BACKENDS["k8s-ut-namespace"][0] is K8sUtNamespaceBackend

    def test_class_path_per_name(self):
        """The class path is what the agent registers in Waldur as the processor's backend."""
        paths = {
            name: f"{cls.__module__}.{cls.__name__}"
            for name, (cls, _, _) in utils.BACKENDS.items()
            if name in ("k8s-namespace", "k8s-ut-namespace")
        }
        assert paths == {
            "k8s-namespace": "waldur_site_agent_k8s_ut_namespace.backend.K8sNamespaceBackend",
            "k8s-ut-namespace": "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceBackend",
        }

    def test_both_names_share_one_schema(self):
        schemas = plugin_schemas.get_plugin_backend_settings_schemas()
        assert schemas["k8s-namespace"] is K8sUtNamespaceBackendSettingsSchema
        assert schemas["k8s-ut-namespace"] is K8sUtNamespaceBackendSettingsSchema


class TestBackendType:
    def test_new_name_reports_new_type(self, backend_settings, backend_components):
        backend, _, _ = make_backend(
            backend_settings, backend_components, backend_class=K8sNamespaceBackend
        )
        assert backend.backend_type == "k8s-namespace"

    def test_former_name_still_reports_former_type(self, backend_settings, backend_components):
        backend, _, _ = make_backend(backend_settings, backend_components)
        assert backend.backend_type == "k8s-ut-namespace"

    def test_subclass_only_changes_backend_type(
        self, backend_settings, backend_components
    ):
        """The class defines nothing but ``__init__``; an instance differs only in ``backend_type``."""
        # Attributes every class body gets, varying by Python version (3.13 adds
        # __firstlineno__ and __static_attributes__; ABCMeta adds the abc ones).
        implicit = {
            "__module__",
            "__qualname__",
            "__doc__",
            "__firstlineno__",
            "__static_attributes__",
            "__abstractmethods__",
            "_abc_impl",
        }
        assert set(vars(K8sNamespaceBackend)) - implicit == {"__init__"}

        parent, _, _ = make_backend(backend_settings, backend_components)
        child, _, _ = make_backend(
            backend_settings, backend_components, backend_class=K8sNamespaceBackend
        )
        parent_state, child_state = dict(vars(parent)), dict(vars(child))
        assert parent_state.pop("backend_type") == "k8s-ut-namespace"
        assert child_state.pop("backend_type") == "k8s-namespace"
        assert set(parent_state) == set(child_state)
        # Clients are per-instance objects (the K8s and Keycloak ones are mocks
        # here): compare the core client by type, everything else by value.
        assert type(parent_state.pop("client")) is type(child_state.pop("client"))
        for key in ("k8s_client", "keycloak_client"):
            parent_state.pop(key)
            child_state.pop(key)
        assert parent_state == child_state

    def test_each_role_loads_through_the_core_loader(
        self, backend_settings_no_keycloak, backend_components, monkeypatch
    ):
        """``get_backend_for_offering`` resolves every role to the class its name registers."""
        monkeypatch.setattr(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient",
            lambda settings: object(),
        )
        for name, cls in (
            ("k8s-namespace", K8sNamespaceBackend),
            ("k8s-ut-namespace", K8sUtNamespaceBackend),
        ):
            offering = _offering(name, backend_settings_no_keycloak, backend_components)
            for role in ("order_processing_backend", "membership_sync_backend", "reporting_backend"):
                backend, _ = utils.get_backend_for_offering(offering, role)
                assert type(backend) is cls
                assert backend.backend_type == name


class TestBehaviour:
    def test_new_name_creates_the_same_cr(
        self, backend_settings, backend_components, waldur_resource
    ):
        specs = []
        for cls in (K8sUtNamespaceBackend, K8sNamespaceBackend):
            backend, mock_k8s, mock_kc = make_backend(
                backend_settings, backend_components, backend_class=cls
            )
            mock_kc.get_group_by_name.return_value = None
            mock_kc.create_group.side_effect = ["g-admin", "g-rw", "g-ro"]
            mock_k8s.create_managed_namespace.return_value = {"metadata": {"name": "x"}}
            backend.create_resource(waldur_resource)
            specs.append(mock_k8s.create_managed_namespace.call_args)
        assert specs[0] == specs[1]
