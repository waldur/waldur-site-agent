"""Tests for OpenNebula VDC backend."""

import logging
from unittest.mock import MagicMock, patch

import pyone
import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_opennebula.backend import OpenNebulaBackend
from waldur_site_agent_opennebula.client import OpenNebulaClient

_DEFAULT_PLAN_QUOTAS = {"vcpu": 1, "vm_ram": 512, "vm_disk": 2048}


# ── Client tests ─────────────────────────────────────────────────────


class TestOpenNebulaClientQuotaTemplate:
    """Test quota template building."""

    def test_build_quota_template_cpu_and_ram(self):
        tpl = OpenNebulaClient._build_quota_template({"cpu": 100, "ram": 2048})
        assert 'CPU="100"' in tpl
        assert 'MEMORY="2048"' in tpl
        assert 'VMS="-1"' in tpl
        assert tpl.startswith("VM=[")

    def test_build_quota_template_storage(self):
        tpl = OpenNebulaClient._build_quota_template({"storage": 10240})
        assert 'SIZE="10240"' in tpl
        assert 'ID="-1"' in tpl
        assert 'IMAGES="-1"' in tpl
        assert tpl.startswith("DATASTORE=[")

    def test_build_quota_template_all_components(self):
        tpl = OpenNebulaClient._build_quota_template(
            {"cpu": 50, "ram": 1024, "storage": 5120}
        )
        assert "VM=[" in tpl
        assert "DATASTORE=[" in tpl

    def test_build_quota_template_unknown_component_skipped(self):
        tpl = OpenNebulaClient._build_quota_template({"unknown_thing": 42})
        assert tpl == ""

    def test_build_quota_template_empty_dict(self):
        tpl = OpenNebulaClient._build_quota_template({})
        assert tpl == ""


class TestOpenNebulaClientParseQuotaUsage:
    """Test parsing quota usage from group info objects."""

    def _make_group_info(self, cpu_used=0, mem_used=0, size_used=0, leases_used=0):
        """Build a mock group info object mimicking pyone response.

        pyone returns VM_QUOTA.VM as a list of quota entries.
        """
        info = MagicMock()
        vm_entry = MagicMock()
        vm_entry.CPU_USED = str(cpu_used)
        vm_entry.MEMORY_USED = str(mem_used)
        info.VM_QUOTA.VM = [vm_entry]

        ds = MagicMock()
        ds.SIZE_USED = str(size_used)
        info.DATASTORE_QUOTA.DATASTORE = [ds]

        if leases_used > 0:
            nq = MagicMock()
            nq.LEASES_USED = str(leases_used)
            info.NETWORK_QUOTA.NETWORK = [nq]
        else:
            info.NETWORK_QUOTA.NETWORK = []

        return info

    def test_parse_usage_all_components(self):
        info = self._make_group_info(cpu_used=10, mem_used=512, size_used=2048)
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage == {"cpu": 10, "ram": 512, "storage": 2048}

    def test_parse_usage_no_vm_quota(self):
        info = MagicMock()
        info.VM_QUOTA.VM = []
        info.DATASTORE_QUOTA.DATASTORE = []
        info.NETWORK_QUOTA.NETWORK = []
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage == {}

    def test_parse_usage_zero_values(self):
        info = self._make_group_info(cpu_used=0, mem_used=0, size_used=0)
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage.get("cpu", 0) == 0
        assert usage.get("ram", 0) == 0

    def test_parse_usage_with_network_leases(self):
        info = self._make_group_info(
            cpu_used=5, mem_used=256, size_used=0, leases_used=3
        )
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage["floating_ip"] == 3


class TestOpenNebulaClientParseQuotaLimits:
    """Test parsing quota limits from group info objects."""

    def _make_group_info(self, cpu=100, mem=1024, size=10240, leases=0):
        info = MagicMock()
        vm_entry = MagicMock()
        vm_entry.CPU = str(cpu)
        vm_entry.MEMORY = str(mem)
        info.VM_QUOTA.VM = [vm_entry]

        ds = MagicMock()
        ds.SIZE = str(size)
        info.DATASTORE_QUOTA.DATASTORE = [ds]

        if leases > 0:
            nq = MagicMock()
            nq.LEASES = str(leases)
            info.NETWORK_QUOTA.NETWORK = [nq]
        else:
            info.NETWORK_QUOTA.NETWORK = []

        return info

    def test_parse_limits_all_components(self):
        info = self._make_group_info(cpu=100, mem=2048, size=5120)
        limits = OpenNebulaClient._parse_group_quota_limits(info)
        assert limits == {"cpu": 100, "ram": 2048, "storage": 5120}

    def test_parse_limits_with_network_leases(self):
        info = self._make_group_info(cpu=50, mem=1024, size=2048, leases=10)
        limits = OpenNebulaClient._parse_group_quota_limits(info)
        assert limits["floating_ip"] == 10


# ── Client integration tests (mocked pyone) ──────────────────────────


class TestOpenNebulaClientOperations:
    """Test client operations with mocked pyone.OneServer."""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    def test_list_resources(self, client, mock_one):
        vdc1 = MagicMock()
        vdc1.NAME = "waldur_test1"
        vdc1.ID = 100
        vdc2 = MagicMock()
        vdc2.NAME = "waldur_test2"
        vdc2.ID = 101

        mock_one.vdcpool.info.return_value.VDC = [vdc1, vdc2]

        resources = client.list_resources()
        assert len(resources) == 2
        assert resources[0].name == "waldur_test1"
        assert resources[1].name == "waldur_test2"

    def test_get_resource_found(self, client, mock_one):
        vdc = MagicMock()
        vdc.NAME = "waldur_test"
        vdc.ID = 100
        mock_one.vdcpool.info.return_value.VDC = [vdc]

        result = client.get_resource("waldur_test")
        assert result is not None
        assert result.name == "waldur_test"

    def test_get_resource_not_found(self, client, mock_one):
        mock_one.vdcpool.info.return_value.VDC = []

        result = client.get_resource("nonexistent")
        assert result is None

    def test_create_resource(self, client, mock_one):
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.return_value = 100

        # Mock cluster pool for auto-discovery
        cluster = MagicMock()
        cluster.ID = 0
        mock_one.clusterpool.info.return_value.CLUSTER = [cluster]

        result = client.create_resource("waldur_test", "Test", "org")

        assert result == "waldur_test"
        mock_one.group.allocate.assert_called_once_with("waldur_test")
        mock_one.vdc.allocate.assert_called_once_with('NAME="waldur_test"')
        mock_one.vdc.addgroup.assert_called_once_with(100, 200)
        mock_one.vdc.addcluster.assert_called_once_with(100, 0, 0)

    def test_create_resource_rollback_on_vdc_failure(self, client, mock_one):
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.side_effect = pyone.OneException("VDC creation failed")

        with pytest.raises(BackendError):
            client.create_resource("waldur_test", "Test", "org")

        mock_one.group.delete.assert_called_once_with(200)

    def test_delete_resource(self, client, mock_one):
        vdc = MagicMock()
        vdc.NAME = "waldur_test"
        vdc.ID = 100
        mock_one.vdcpool.info.return_value.VDC = [vdc]

        group = MagicMock()
        group.NAME = "waldur_test"
        group.ID = 200
        mock_one.grouppool.info.return_value.GROUP = [group]

        client.delete_resource("waldur_test")

        mock_one.vdc.delete.assert_called_once_with(100)
        mock_one.group.delete.assert_called_once_with(200)

    def test_set_resource_limits(self, client, mock_one):
        group = MagicMock()
        group.NAME = "waldur_test"
        group.ID = 200
        mock_one.grouppool.info.return_value.GROUP = [group]

        client.set_resource_limits("waldur_test", {"cpu": 50, "ram": 2048})

        mock_one.group.quota.assert_called_once()
        call_args = mock_one.group.quota.call_args
        assert call_args[0][0] == 200  # group_id
        quota_tpl = call_args[0][1]
        assert 'CPU="50"' in quota_tpl
        assert 'MEMORY="2048"' in quota_tpl

    def test_association_methods_are_noops(self, client):
        assert client.get_association("user", "resource") is None
        assert client.create_association("user", "resource") == "user"
        assert client.delete_association("user", "resource") == "user"
        assert client.list_resource_users("resource") == []

    def test_get_resource_user_limits_empty(self, client):
        assert client.get_resource_user_limits("resource") == {}


# ── Backend tests ────────────────────────────────────────────────────


class TestOpenNebulaBackendInit:
    """Test backend initialization."""

    def test_init_success(self, backend_settings, backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, backend_components)
            assert backend.backend_type == "opennebula"
            assert backend.supports_decreasing_usage is True

    def test_init_missing_api_url(self, backend_components):
        with pytest.raises(ValueError, match="api_url"):
            with patch("waldur_site_agent_opennebula.client.pyone"):
                OpenNebulaBackend({"credentials": "a:b"}, backend_components)

    def test_init_missing_credentials(self, backend_components):
        with pytest.raises(ValueError, match="credentials"):
            with patch("waldur_site_agent_opennebula.client.pyone"):
                OpenNebulaBackend(
                    {"api_url": "http://localhost:2633/RPC2"}, backend_components
                )

    def test_init_with_empty_backend_components(self, backend_settings):
        """Backend initializes with empty backend_components.

        Components are populated later by extend_backend_components() from Waldur.
        """
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, {})
            assert backend.backend_type == "opennebula"
            assert backend.backend_components == {}
            assert backend.list_components() == []

    def test_usage_report_with_empty_backend_components(self, backend_settings):
        """Usage report works with empty backend_components (returns empty usage)."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, {})
            backend.client = MagicMock(spec=OpenNebulaClient)

        backend.client.get_usage_report.return_value = [
            {"resource_id": "test", "usage": {"cpu": 5}}
        ]

        report = backend._get_usage_report(["test"])
        # With no backend_components, no conversion happens, empty usage
        assert report["test"]["TOTAL_ACCOUNT_USAGE"] == {}

    def test_collect_limits_with_empty_backend_components(self, backend_settings):
        """Limit collection works with empty backend_components."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, {})

        resource = MagicMock(spec=WaldurResource)
        resource.limits = {"cpu": 10, "ram": 512}

        backend_limits, waldur_limits = backend._collect_resource_limits(resource)
        assert backend_limits == {}
        assert waldur_limits == {}


class TestOpenNebulaBackendMethods:
    """Test backend abstract method implementations."""

    @pytest.fixture()
    def backend(self, backend_settings, backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_ping_success(self, backend):
        backend.client.list_resources.return_value = []
        assert backend.ping() is True

    def test_ping_failure(self, backend):
        backend.client.list_resources.side_effect = BackendError("Connection refused")
        assert backend.ping() is False

    def test_ping_failure_raises(self, backend):
        backend.client.list_resources.side_effect = BackendError("Connection refused")
        with pytest.raises(BackendError):
            backend.ping(raise_exception=True)

    def test_list_components(self, backend):
        components = backend.list_components()
        assert set(components) == {"cpu", "ram", "storage"}

    def test_collect_resource_limits(self, backend):
        resource = MagicMock(spec=WaldurResource)
        resource.limits = {"cpu": 10, "ram": 512, "storage": 2048}

        backend_limits, waldur_limits = backend._collect_resource_limits(resource)

        assert backend_limits == {"cpu": 10, "ram": 512, "storage": 2048}
        assert waldur_limits == {"cpu": 10, "ram": 512, "storage": 2048}

    def test_collect_resource_limits_with_unit_factor(self, backend_settings):
        components = {
            "cpu": {
                "limit": 100,
                "measured_unit": "cores",
                "unit_factor": 2,
                "accounting_type": "limit",
                "label": "CPU",
            },
        }
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, components)

        resource = MagicMock(spec=WaldurResource)
        resource.limits = {"cpu": 10}

        backend_limits, waldur_limits = backend._collect_resource_limits(resource)

        assert backend_limits["cpu"] == 20  # 10 * 2
        assert waldur_limits["cpu"] == 10

    def test_collect_resource_limits_empty(self, backend):
        resource = MagicMock(spec=WaldurResource)
        resource.limits = {}

        backend_limits, waldur_limits = backend._collect_resource_limits(resource)

        assert backend_limits == {}
        assert waldur_limits == {}

    def test_collect_resource_limits_none(self, backend):
        resource = MagicMock(spec=WaldurResource)
        resource.limits = None

        backend_limits, waldur_limits = backend._collect_resource_limits(resource)

        assert backend_limits == {}
        assert waldur_limits == {}

    def test_get_usage_report(self, backend):
        backend.client.get_usage_report.return_value = [
            {
                "resource_id": "waldur_test",
                "usage": {"cpu": 5, "ram": 256, "storage": 1024},
            }
        ]

        report = backend._get_usage_report(["waldur_test"])

        assert "waldur_test" in report
        assert "TOTAL_ACCOUNT_USAGE" in report["waldur_test"]
        assert report["waldur_test"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 5
        assert report["waldur_test"]["TOTAL_ACCOUNT_USAGE"]["ram"] == 256
        assert report["waldur_test"]["TOTAL_ACCOUNT_USAGE"]["storage"] == 1024

    def test_get_usage_report_with_unit_factor(self, backend_settings):
        components = {
            "cpu": {
                "limit": 100,
                "measured_unit": "cores",
                "unit_factor": 2,
                "accounting_type": "limit",
                "label": "CPU",
            },
        }
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, components)
            backend.client = MagicMock(spec=OpenNebulaClient)

        backend.client.get_usage_report.return_value = [
            {"resource_id": "test", "usage": {"cpu": 20}}
        ]

        report = backend._get_usage_report(["test"])

        # 20 backend units / 2 unit_factor = 10 Waldur units
        assert report["test"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 10

    def test_get_usage_report_empty(self, backend):
        backend.client.get_usage_report.return_value = []
        report = backend._get_usage_report(["waldur_test"])
        assert report == {}

    def test_get_usage_report_fills_missing_components(self, backend):
        backend.client.get_usage_report.return_value = [
            {"resource_id": "test", "usage": {"cpu": 5}}
        ]

        report = backend._get_usage_report(["test"])
        usage = report["test"]["TOTAL_ACCOUNT_USAGE"]

        # Missing components should default to 0
        assert usage["ram"] == 0
        assert usage["storage"] == 0

    def test_downscale_resource_noop(self, backend):
        assert backend.downscale_resource("test") is True

    def test_pause_resource_noop(self, backend):
        assert backend.pause_resource("test") is True

    def test_restore_resource_noop(self, backend):
        assert backend.restore_resource("test") is True

    def test_get_resource_metadata_empty(self, backend):
        assert backend.get_resource_metadata("test") == {}


# ── Networking tests ──────────────────────────────────────────────────


class TestOpenNebulaClientSubnetAllocation:
    """Test stateless subnet allocation from pool."""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    def _make_vnet(self, name, ip):
        vnet = MagicMock()
        vnet.NAME = name
        ar = MagicMock()
        ar.IP = ip
        vnet.AR_POOL.AR = [ar]
        return vnet

    def test_allocate_first_subnet(self, client, mock_one):
        """First allocation skips base network and returns second /24."""
        mock_one.vnpool.info.return_value.VNET = []

        result = client._allocate_next_subnet("10.0.0.0", 8, 24)
        assert result == "10.0.1.0"

    def test_allocate_skips_used(self, client, mock_one):
        """Allocation skips already-used subnets."""
        existing = self._make_vnet("waldur_proj1_internal", "10.0.1.5")
        mock_one.vnpool.info.return_value.VNET = [existing]

        result = client._allocate_next_subnet("10.0.0.0", 8, 24)
        assert result == "10.0.2.0"

    def test_allocate_skips_multiple_used(self, client, mock_one):
        """Allocation skips multiple used subnets."""
        vnets = [
            self._make_vnet("waldur_proj1_internal", "10.0.1.10"),
            self._make_vnet("waldur_proj2_internal", "10.0.2.10"),
        ]
        mock_one.vnpool.info.return_value.VNET = vnets

        result = client._allocate_next_subnet("10.0.0.0", 8, 24)
        assert result == "10.0.3.0"

    def test_allocate_ignores_non_waldur_vnets(self, client, mock_one):
        """Non-waldur VNets are ignored during allocation."""
        foreign = self._make_vnet("other_network", "10.0.1.5")
        mock_one.vnpool.info.return_value.VNET = [foreign]

        result = client._allocate_next_subnet("10.0.0.0", 8, 24)
        assert result == "10.0.1.0"

    def test_allocate_exhausted_pool(self, client, mock_one):
        """Raises BackendError when pool is exhausted."""
        # Use a tiny pool: 10.0.0.0/30 with /30 subnets = only 1 possible subnet
        # (10.0.0.0/30 base is skipped, no more left)
        mock_one.vnpool.info.return_value.VNET = []

        with pytest.raises(BackendError, match="No available subnets"):
            client._allocate_next_subnet("10.0.0.0", 30, 30)


class TestOpenNebulaClientNetworkOps:
    """Test VXLAN VNet, Virtual Router, and Security Group operations."""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    def test_create_vxlan_network(self, client, mock_one):
        """VXLAN VNet creation passes correct template."""
        mock_one.vn.allocate.return_value = 50

        vnet_id = client._create_vxlan_network(
            "waldur_test_internal", "10.0.1.0/24", "10.0.1.1", "eth0", "8.8.8.8", 0
        )

        assert vnet_id == 50
        call_args = mock_one.vn.allocate.call_args
        template = call_args[0][0]
        assert 'NAME="waldur_test_internal"' in template
        assert 'VN_MAD="vxlan"' in template
        assert 'PHYDEV="eth0"' in template
        assert 'IP="10.0.1.1"' in template  # gateway IP (first in AR)
        assert 'SIZE="254"' in template  # 256 - 2 (network + broadcast excluded)
        assert 'GATEWAY="10.0.1.1"' in template
        assert 'DNS="8.8.8.8"' in template
        assert call_args[0][1] == 0  # cluster_id

    def test_delete_vnet(self, client, mock_one):
        client._delete_vnet(50)
        mock_one.vn.delete.assert_called_once_with(50)

    def test_add_vnet_to_vdc(self, client, mock_one):
        client._add_vnet_to_vdc(100, 0, 50)
        mock_one.vdc.addvnet.assert_called_once_with(100, 0, 50)

    def test_create_virtual_router(self, client, mock_one):
        mock_one.vrouter.allocate.return_value = 30

        vr_id = client._create_virtual_router("waldur_test_router")

        assert vr_id == 30
        call_args = mock_one.vrouter.allocate.call_args
        assert 'NAME="waldur_test_router"' in call_args[0][0]

    def test_instantiate_vr(self, client, mock_one):
        mock_one.vrouter.instantiate.return_value = 500

        vm_id = client._instantiate_vr(
            30, 8, "waldur_test_router_vm", 'NIC=[NETWORK_ID="50"]'
        )

        assert vm_id == 500
        mock_one.vrouter.instantiate.assert_called_once_with(
            30, 1, 8, "waldur_test_router_vm", False, 'NIC=[NETWORK_ID="50"]'
        )

    def test_delete_virtual_router(self, client, mock_one):
        client._delete_virtual_router(30)
        mock_one.vrouter.delete.assert_called_once_with(30)

    def test_create_security_group(self, client, mock_one):
        mock_one.secgroup.allocate.return_value = 20

        rules = [
            {"direction": "INBOUND", "protocol": "TCP", "range": "22:22"},
            {"direction": "INBOUND", "protocol": "ICMP", "type": "8"},
        ]
        sg_id = client._create_security_group("waldur_test_default", rules)

        assert sg_id == 20
        call_args = mock_one.secgroup.allocate.call_args
        template = call_args[0][0]
        assert 'NAME="waldur_test_default"' in template
        assert 'PROTOCOL="TCP"' in template
        assert 'RULE_TYPE="inbound"' in template
        assert 'RANGE="22:22"' in template
        assert 'ICMP_TYPE="8"' in template

    def test_delete_security_group(self, client, mock_one):
        client._delete_security_group(20)
        mock_one.secgroup.delete.assert_called_once_with(20)

    def test_get_vnet_by_name_found(self, client, mock_one):
        vnet = MagicMock()
        vnet.NAME = "waldur_test_internal"
        mock_one.vnpool.info.return_value.VNET = [vnet]

        result = client._get_vnet_by_name("waldur_test_internal")
        assert result is vnet

    def test_get_vnet_by_name_not_found(self, client, mock_one):
        mock_one.vnpool.info.return_value.VNET = []

        result = client._get_vnet_by_name("nonexistent")
        assert result is None

    def test_get_vrouter_by_name_found(self, client, mock_one):
        vr = MagicMock()
        vr.NAME = "waldur_test_router"
        mock_one.vrouterpool.info.return_value.VROUTER = [vr]

        result = client._get_vrouter_by_name("waldur_test_router")
        assert result is vr

    def test_get_secgroup_by_name_found(self, client, mock_one):
        sg = MagicMock()
        sg.NAME = "waldur_test_default"
        mock_one.secgrouppool.info.return_value.SECURITY_GROUP = [sg]

        result = client._get_secgroup_by_name("waldur_test_default")
        assert result is sg


class TestVDCCreateWithNetworking:
    """Test full VDC creation with networking orchestration."""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    @pytest.fixture()
    def network_config(self):
        return {
            "zone_id": 0,
            "cluster_ids": [0],
            "external_network_id": 10,
            "vxlan_phydev": "eth0",
            "virtual_router_template_id": 8,
            "default_dns": "8.8.8.8",
            "internal_network_base": "10.0.0.0",
            "internal_network_prefix": 8,
            "subnet_prefix_length": 24,
            "security_group_defaults": [
                {"direction": "INBOUND", "protocol": "TCP", "range": "22:22"},
            ],
        }

    def _mock_vm_running(self, mock_one):
        """Set up vm.info to return ACTIVE/RUNNING."""
        from pyone import LCM_STATE, VM_STATE

        vm_info = MagicMock()
        vm_info.STATE = VM_STATE.ACTIVE
        vm_info.LCM_STATE = LCM_STATE.RUNNING
        mock_one.vm.info.return_value = vm_info

    def test_create_resource_with_networking(self, client, mock_one, network_config):
        """Full VDC + networking creation flow."""
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.return_value = 100
        mock_one.vnpool.info.return_value.VNET = []  # no existing nets
        mock_one.vn.allocate.return_value = 50
        mock_one.vrouter.allocate.return_value = 30
        mock_one.vrouter.instantiate.return_value = 500
        mock_one.secgroup.allocate.return_value = 20
        self._mock_vm_running(mock_one)

        result = client.create_resource(
            "waldur_test", "Test", "org", network_config=network_config
        )

        assert result == "waldur_test"
        # Group and VDC created
        mock_one.group.allocate.assert_called_once()
        mock_one.vdc.allocate.assert_called_once()
        # VNet created
        mock_one.vn.allocate.assert_called_once()
        # VNet added to VDC
        mock_one.vdc.addvnet.assert_called_once_with(100, 0, 50)
        # VR created and instantiated
        mock_one.vrouter.allocate.assert_called_once()
        mock_one.vrouter.instantiate.assert_called_once()
        # SG created
        mock_one.secgroup.allocate.assert_called_once()
        # Network metadata stored
        assert client._network_metadata["vnet_id"] == 50
        assert client._network_metadata["vr_id"] == 30
        assert client._network_metadata["sg_id"] == 20
        assert "10.0.1.0/24" in client._network_metadata["subnet_cidr"]

    def test_create_resource_without_networking(self, client, mock_one):
        """VDC creation without networking (no network_config)."""
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.return_value = 100
        cluster = MagicMock()
        cluster.ID = 0
        mock_one.clusterpool.info.return_value.CLUSTER = [cluster]

        result = client.create_resource("waldur_test", "Test", "org")

        assert result == "waldur_test"
        mock_one.vn.allocate.assert_not_called()
        mock_one.vrouter.allocate.assert_not_called()
        mock_one.secgroup.allocate.assert_not_called()

    def test_create_with_user_specified_subnet(self, client, mock_one, network_config):
        """User-specified subnet_cidr is used instead of auto-allocation."""
        network_config["subnet_cidr"] = "192.168.1.0/24"
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.return_value = 100
        mock_one.vn.allocate.return_value = 50
        mock_one.vrouter.allocate.return_value = 30
        mock_one.vrouter.instantiate.return_value = 500
        mock_one.secgroup.allocate.return_value = 20
        self._mock_vm_running(mock_one)

        client.create_resource(
            "waldur_test", "Test", "org", network_config=network_config
        )

        # VNet template should use user subnet
        vn_template = mock_one.vn.allocate.call_args[0][0]
        assert 'IP="192.168.1.1"' in vn_template
        assert 'GATEWAY="192.168.1.1"' in vn_template

    def test_networking_rollback_on_vr_failure(self, client, mock_one, network_config):
        """VR failure rolls back VNet and VDC."""
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.return_value = 100
        mock_one.vnpool.info.return_value.VNET = []
        mock_one.vn.allocate.return_value = 50
        mock_one.vrouter.allocate.side_effect = pyone.OneException("VR creation failed")

        with pytest.raises(BackendError):
            client.create_resource(
                "waldur_test", "Test", "org", network_config=network_config
            )

        # VNet should be rolled back
        mock_one.vn.delete.assert_called_once_with(50)
        # VDC and group should be rolled back
        mock_one.vdc.delete.assert_called_once_with(100)
        mock_one.group.delete.assert_called_once_with(200)

    def test_networking_rollback_on_vnet_failure(
        self, client, mock_one, network_config
    ):
        """VNet failure rolls back VDC and group."""
        mock_one.group.allocate.return_value = 200
        mock_one.vdc.allocate.return_value = 100
        mock_one.vnpool.info.return_value.VNET = []
        mock_one.vn.allocate.side_effect = pyone.OneException("VNet creation failed")

        with pytest.raises(BackendError):
            client.create_resource(
                "waldur_test", "Test", "org", network_config=network_config
            )

        # VDC and group should be rolled back
        mock_one.vdc.delete.assert_called_once_with(100)
        mock_one.group.delete.assert_called_once_with(200)


class TestVDCDeleteWithNetworking:
    """Test VDC deletion with networking teardown."""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    def test_delete_with_networking(self, client, mock_one):
        """Full teardown: VR → VNet → SG → VDC → Group."""
        # VR exists
        vr = MagicMock()
        vr.NAME = "waldur_test_router"
        vr.ID = 30
        mock_one.vrouterpool.info.return_value.VROUTER = [vr]

        # VR info returns a VM ID so _teardown_networking can wait for it
        vr_info = MagicMock()
        vr_info.VMS.ID = [99]
        mock_one.vrouter.info.return_value = vr_info

        # The VR VM reaches DONE state after deletion
        vr_vm_info = MagicMock()
        vr_vm_info.STATE = 6  # VM_STATE.DONE
        mock_one.vm.info.return_value = vr_vm_info

        # VNet exists
        vnet = MagicMock()
        vnet.NAME = "waldur_test_internal"
        vnet.ID = 50
        mock_one.vnpool.info.return_value.VNET = [vnet]

        # SG exists
        sg = MagicMock()
        sg.NAME = "waldur_test_default"
        sg.ID = 20
        mock_one.secgrouppool.info.return_value.SECURITY_GROUP = [sg]

        # VDC exists
        vdc = MagicMock()
        vdc.NAME = "waldur_test"
        vdc.ID = 100
        mock_one.vdcpool.info.return_value.VDC = [vdc]

        # Group exists
        group = MagicMock()
        group.NAME = "waldur_test"
        group.ID = 200
        mock_one.grouppool.info.return_value.GROUP = [group]

        client.delete_resource("waldur_test")

        # Verify deletion order via call sequence
        mock_one.vrouter.delete.assert_called_once_with(30)
        mock_one.vn.delete.assert_called_once_with(50)
        mock_one.secgroup.delete.assert_called_once_with(20)
        mock_one.vdc.delete.assert_called_once_with(100)
        mock_one.group.delete.assert_called_once_with(200)

    def test_delete_without_networking(self, client, mock_one):
        """Deletion when no networking resources exist."""
        mock_one.vrouterpool.info.return_value.VROUTER = []
        mock_one.vnpool.info.return_value.VNET = []
        mock_one.secgrouppool.info.return_value.SECURITY_GROUP = []

        vdc = MagicMock()
        vdc.NAME = "waldur_test"
        vdc.ID = 100
        mock_one.vdcpool.info.return_value.VDC = [vdc]

        group = MagicMock()
        group.NAME = "waldur_test"
        group.ID = 200
        mock_one.grouppool.info.return_value.GROUP = [group]

        client.delete_resource("waldur_test")

        mock_one.vrouter.delete.assert_not_called()
        mock_one.vn.delete.assert_not_called()
        mock_one.secgroup.delete.assert_not_called()
        mock_one.vdc.delete.assert_called_once_with(100)
        mock_one.group.delete.assert_called_once_with(200)


class TestOpenNebulaBackendNetworkConfig:
    """Test backend network configuration from Waldur resource."""

    def test_build_network_config_with_plugin_options(self):
        """Network config is built from offering_plugin_options."""
        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = {
            "zone_id": 0,
            "cluster_ids": [0, 100],
            "external_network_id": 10,
            "vxlan_phydev": "eth0",
            "virtual_router_template_id": 8,
            "default_dns": "8.8.8.8",
            "internal_network_base": "10.0.0.0",
            "internal_network_prefix": 8,
            "subnet_prefix_length": 24,
            "security_group_defaults": [
                {"direction": "INBOUND", "protocol": "TCP", "range": "22:22"},
            ],
        }
        resource.attributes = {}

        config = OpenNebulaBackend._build_network_config(resource)

        assert config is not None
        assert config["external_network_id"] == 10
        assert config["virtual_router_template_id"] == 8
        assert config["vxlan_phydev"] == "eth0"
        assert config["subnet_prefix_length"] == 24
        assert len(config["security_group_defaults"]) == 1

    def test_build_network_config_without_networking(self):
        """Returns None when networking keys are absent."""
        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = {"zone_id": 0}
        resource.attributes = {}

        config = OpenNebulaBackend._build_network_config(resource)
        assert config is None

    def test_build_network_config_with_user_subnet(self):
        """User-specified subnet_cidr is included."""
        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = {
            "external_network_id": 10,
            "virtual_router_template_id": 8,
        }
        resource.attributes = {"subnet_cidr": "192.168.1.0/24"}

        config = OpenNebulaBackend._build_network_config(resource)

        assert config is not None
        assert config["subnet_cidr"] == "192.168.1.0/24"

    def test_build_network_config_missing_vr_template(self):
        """Returns None when VR template is absent."""
        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = {"external_network_id": 10}
        resource.attributes = {}

        config = OpenNebulaBackend._build_network_config(resource)
        assert config is None

    def test_build_network_config_none_plugin_options(self):
        """Handles None offering_plugin_options gracefully."""
        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = None
        resource.attributes = {}

        config = OpenNebulaBackend._build_network_config(resource)
        assert config is None

    def test_pre_create_resource_stores_network_config(self, backend_settings):
        """_pre_create_resource stores network config for later use."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, {})

        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = {
            "external_network_id": 10,
            "virtual_router_template_id": 8,
        }
        resource.attributes = {}

        backend._pre_create_resource(resource)

        assert backend._pending_network_config is not None
        assert backend._pending_network_config["external_network_id"] == 10

    def test_pre_create_resource_no_networking(self, backend_settings):
        """_pre_create_resource sets None when no networking configured."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, {})

        resource = MagicMock(spec=WaldurResource)
        resource.offering_plugin_options = {}
        resource.attributes = {}

        backend._pre_create_resource(resource)

        assert backend._pending_network_config is None

    def test_get_resource_metadata_returns_network_info(self, backend_settings):
        """get_resource_metadata returns stored network metadata."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, {})

        metadata = {"vnet_id": 50, "subnet_cidr": "10.0.1.0/24", "vr_id": 30}
        backend._resource_network_metadata["waldur_test"] = metadata

        result = backend.get_resource_metadata("waldur_test")
        assert result["vnet_id"] == 50
        assert result["subnet_cidr"] == "10.0.1.0/24"


class TestQuotaTemplateWithFloatingIP:
    """Test quota template building with floating_ip component."""

    def test_build_quota_template_floating_ip(self):
        tpl = OpenNebulaClient._build_quota_template({"floating_ip": 10})
        assert "NETWORK=[" in tpl
        assert 'LEASES="10"' in tpl
        assert 'ID="-1"' in tpl

    def test_build_quota_template_all_with_floating_ip(self):
        tpl = OpenNebulaClient._build_quota_template(
            {"cpu": 50, "ram": 1024, "storage": 5120, "floating_ip": 5}
        )
        assert "VM=[" in tpl
        assert "DATASTORE=[" in tpl
        assert "NETWORK=[" in tpl
        assert 'LEASES="5"' in tpl


# ── VM client tests ──────────────────────────────────────────────────


class TestOpenNebulaClientVMOperations:
    """Test VM lifecycle methods on the client."""

    @pytest.fixture()
    def client(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_one = MagicMock()
            mock_pyone.OneServer.return_value = mock_one
            c = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
            c.one = mock_one
            return c

    def test_create_vm_instantiates_template(self, client):
        from pyone import LCM_STATE, VM_STATE

        # Setup: VNet exists, SG exists, group exists
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        mock_sg = MagicMock()
        mock_sg.ID = 10
        mock_group = MagicMock()
        mock_group.ID = 5

        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=mock_sg)
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        # Mock VM reaching RUNNING state
        vm_info = MagicMock()
        vm_info.STATE = VM_STATE.ACTIVE
        vm_info.LCM_STATE = LCM_STATE.RUNNING
        client.one.vm.info.return_value = vm_info

        vm_id = client.create_vm(
            template_id=101,
            vm_name="test_vm",
            parent_vdc_name="test_vdc",
            vcpu=2,
            ram_mb=1024,
        )

        assert vm_id == 100
        client.one.template.instantiate.assert_called_once()
        call_args = client.one.template.instantiate.call_args
        assert call_args[0][0] == 101  # template_id
        assert call_args[0][1] == "test_vm"  # vm_name
        # Verify chown was called
        client.one.vm.chown.assert_called_once_with(100, -1, 5)

    def test_create_vm_no_vnet_raises(self, client):
        client._get_vnet_by_name = MagicMock(return_value=None)

        with pytest.raises(BackendError, match="Internal network.*not found"):
            client.create_vm(
                template_id=101,
                vm_name="test_vm",
                parent_vdc_name="nonexistent_vdc",
            )

    def test_delete_vm_terminates(self, client):
        client.one.vm.action.return_value = True

        client.delete_vm(100)

        client.one.vm.action.assert_called_once_with("terminate-hard", 100)

    def test_delete_vm_not_found(self, client):
        from pyone import OneNoExistsException

        client.one.vm.action.side_effect = OneNoExistsException("not found")
        # Should not raise
        client.delete_vm(999)

    def test_get_vm_returns_info(self, client):
        mock_info = MagicMock()
        mock_info.NAME = "test_vm"
        mock_info.STATE = 3
        mock_info.LCM_STATE = 3
        client._get_vm_info = MagicMock(return_value=mock_info)

        info = client.get_vm(100)

        assert info is not None
        assert info["vm_id"] == 100
        assert info["name"] == "test_vm"

    def test_get_vm_not_found(self, client):
        client._get_vm_info = MagicMock(side_effect=BackendError("not found"))
        assert client.get_vm(999) is None

    def test_get_vm_usage(self, client):
        mock_info = MagicMock()
        mock_info.TEMPLATE.VCPU = "4"
        mock_info.TEMPLATE.MEMORY = "2048"
        mock_disk = MagicMock()
        mock_disk.SIZE = "10240"
        mock_info.TEMPLATE.DISK = [mock_disk]
        client._get_vm_info = MagicMock(return_value=mock_info)

        usage = client.get_vm_usage(100)

        assert usage is not None
        assert usage["vcpu"] == 4
        assert usage["vm_ram"] == 2048
        assert usage["vm_disk"] == 10240

    def test_get_vm_usage_not_found(self, client):
        client._get_vm_info = MagicMock(side_effect=BackendError("not found"))
        assert client.get_vm_usage(999) is None

    def test_get_vm_ip_address(self, client):
        mock_vm = MagicMock()
        mock_vm.ID = 100
        client._get_vm_by_name = MagicMock(return_value=mock_vm)

        mock_info = MagicMock()
        mock_nic = MagicMock()
        mock_nic.IP = "10.0.1.5"
        mock_info.TEMPLATE.NIC = [mock_nic]
        client._get_vm_info = MagicMock(return_value=mock_info)

        ip = client.get_vm_ip_address("test_vm")
        assert ip == "10.0.1.5"

    def test_chown_vm_to_group(self, client):
        mock_group = MagicMock()
        mock_group.ID = 5
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.vm.chown.return_value = True

        client._chown_vm_to_group(100, "test_group")
        client.one.vm.chown.assert_called_once_with(100, -1, 5)

    def test_chown_vm_group_not_found(self, client):
        client._get_group_by_name = MagicMock(return_value=None)
        with pytest.raises(BackendError, match="Group.*not found"):
            client._chown_vm_to_group(100, "nonexistent")


# ── VM backend tests ─────────────────────────────────────────────────


class TestOpenNebulaBackendVMInit:
    """Test backend initialization with resource_type=vm."""

    def test_vm_resource_type_from_settings(
        self, vm_backend_settings, vm_backend_components
    ):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
        assert backend.resource_type == "vm"

    def test_default_resource_type_is_vdc(self, backend_settings, backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, backend_components)
        assert backend.resource_type == "vdc"


class TestOpenNebulaBackendVMCreation:
    """Test VM creation flow through the backend."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_pre_create_vm_extracts_config(self, vm_backend):
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {
            "template_id": "101",
            "parent_backend_id": "my_vdc",
            "ssh_public_key": "ssh-rsa AAAA...",
        }

        user_context = {
            "plan_quotas": {"vcpu": 4, "vm_ram": 2048, "vm_disk": 20480},
            "ssh_keys": {},
        }

        vm_backend._pre_create_resource(resource, user_context=user_context)

        assert vm_backend._pending_vm_config is not None
        assert vm_backend._pending_vm_config["template_id"] == 101
        assert vm_backend._pending_vm_config["parent_backend_id"] == "my_vdc"
        assert vm_backend._pending_vm_config["vcpu"] == 4

    def test_pre_create_vm_missing_template_raises(self, vm_backend):
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"parent_backend_id": "my_vdc"}
        resource.offering_plugin_options = {}

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {},
        }

        with pytest.raises(BackendError, match="template_id"):
            vm_backend._pre_create_resource(resource, user_context=user_context)

    def test_pre_create_vm_missing_parent_raises(self, vm_backend):
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"template_id": "101"}
        resource.offering_plugin_options = {}

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {},
        }

        with pytest.raises(BackendError, match="parent_backend_id"):
            vm_backend._pre_create_resource(resource, user_context=user_context)

    def test_create_vm_resource_calls_client(self, vm_backend):
        vm_backend._pending_vm_config = {
            "template_id": 101,
            "parent_backend_id": "my_vdc",
            "ssh_public_key": "",
            "vcpu": 2,
            "vm_ram": 1024,
            "vm_disk": 10240,
        }
        vm_backend.client.create_vm.return_value = 42
        vm_backend.client.get_vm_ip_address.return_value = "10.0.1.5"

        vm_id = vm_backend._create_vm_resource("test_vm")

        assert vm_id == 42
        vm_backend.client.create_vm.assert_called_once_with(
            template_id=101,
            vm_name="test_vm",
            parent_vdc_name="my_vdc",
            ssh_key="",
            vcpu=2,
            ram_mb=1024,
            disk_mb=10240,
            cluster_ids=None,
            sched_requirements="",
        )

    def test_create_vm_stores_metadata(self, vm_backend):
        vm_backend._pending_vm_config = {
            "template_id": 101,
            "parent_backend_id": "my_vdc",
            "ssh_public_key": "",
            "vcpu": 2,
            "vm_ram": 1024,
            "vm_disk": 10240,
        }
        vm_backend.client.create_vm.return_value = 42
        vm_backend.client.get_vm_ip_address.return_value = "10.0.1.5"

        vm_backend._create_vm_resource("test_vm")

        metadata = vm_backend._resource_network_metadata["42"]
        assert metadata["vm_id"] == 42
        assert metadata["ip_address"] == "10.0.1.5"


class TestOpenNebulaBackendVMDeletion:
    """Test VM deletion flow."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_delete_vm_calls_client(self, vm_backend):
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "42"

        vm_backend.delete_resource(resource)

        vm_backend.client.delete_vm.assert_called_once_with(42)

    def test_delete_vm_empty_backend_id(self, vm_backend):
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = ""

        vm_backend.delete_resource(resource)

        vm_backend.client.delete_vm.assert_not_called()


class TestOpenNebulaBackendVMUsage:
    """Test VM usage reporting."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_vm_usage_report(self, vm_backend):
        vm_backend.client.get_vm_usage.return_value = {
            "vcpu": 4,
            "vm_ram": 2048,
            "vm_disk": 10240,
        }

        report = vm_backend._get_usage_report(["42"])

        assert "42" in report
        assert "TOTAL_ACCOUNT_USAGE" in report["42"]
        usage = report["42"]["TOTAL_ACCOUNT_USAGE"]
        assert usage["vcpu"] == 4
        assert usage["vm_ram"] == 2048
        assert usage["vm_disk"] == 10240
        vm_backend.client.get_vm_usage.assert_called_once_with(42)

    def test_vm_usage_not_found(self, vm_backend):
        vm_backend.client.get_vm_usage.return_value = None

        report = vm_backend._get_usage_report(["999"])

        assert "999" not in report

    def test_vm_usage_with_unit_factor(self, vm_backend):
        vm_backend.backend_components["vm_ram"]["unit_factor"] = 1024
        vm_backend.client.get_vm_usage.return_value = {
            "vcpu": 4,
            "vm_ram": 4096,
            "vm_disk": 10240,
        }

        report = vm_backend._get_usage_report(["42"])
        usage = report["42"]["TOTAL_ACCOUNT_USAGE"]
        assert usage["vm_ram"] == 4  # 4096 // 1024


class TestOpenNebulaBackendVMMetadata:
    """Test VM resource metadata retrieval."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_cached_metadata_returned(self, vm_backend):
        vm_backend._resource_network_metadata["test_vm"] = {
            "vm_id": 42,
            "ip_address": "10.0.1.5",
        }

        metadata = vm_backend.get_resource_metadata("test_vm")
        assert metadata["vm_id"] == 42
        assert metadata["ip_address"] == "10.0.1.5"

    def test_live_query_for_vm_metadata(self, vm_backend):
        vm_backend.client.get_vm.return_value = {
            "vm_id": 42,
            "name": "test_vm",
            "state": 3,
            "lcm_state": 3,
        }
        vm_backend.client.get_vm_ip_address.return_value = "10.0.1.5"

        metadata = vm_backend.get_resource_metadata("42")
        assert metadata["vm_id"] == 42
        assert metadata["ip_address"] == "10.0.1.5"
        vm_backend.client.get_vm.assert_called_once_with(42)
        vm_backend.client.get_vm_ip_address.assert_called_once_with(42)

    def test_vm_not_found_returns_empty(self, vm_backend):
        vm_backend.client.get_vm.return_value = None

        metadata = vm_backend.get_resource_metadata("999")
        assert metadata == {}


# ── User Management tests ────────────────────────────────────────────


class TestOpenNebulaClientUserManagement:
    """Test user CRUD operations on the OpenNebula client."""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    # ── create_user ──

    def test_create_user_success(self, client, mock_one):
        """Creates user, assigns to group, stores password in TEMPLATE."""
        mock_one.userpool.info.return_value.USER = []  # no existing user
        mock_one.user.allocate.return_value = 42

        group = MagicMock()
        group.ID = 10
        group.NAME = "my_vdc"
        mock_one.grouppool.info.return_value.GROUP = [group]

        user_id = client.create_user("my_vdc_admin", "s3cret", "my_vdc")

        assert user_id == 42
        mock_one.user.allocate.assert_called_once_with("my_vdc_admin", "s3cret", "core")
        mock_one.user.chgrp.assert_called_once_with(42, 10)
        mock_one.user.update.assert_called_once_with(
            42, 'WALDUR_PASSWORD="s3cret"', 1
        )

    def test_create_user_idempotent(self, client, mock_one):
        """Existing user with same name → returns existing ID."""
        existing = MagicMock()
        existing.ID = 42
        existing.NAME = "my_vdc_admin"
        mock_one.userpool.info.return_value.USER = [existing]

        user_id = client.create_user("my_vdc_admin", "s3cret", "my_vdc")

        assert user_id == 42
        mock_one.user.allocate.assert_not_called()

    def test_create_user_chgrp_failure_rolls_back(self, client, mock_one):
        """chgrp failure → user deleted."""
        mock_one.userpool.info.return_value.USER = []
        mock_one.user.allocate.return_value = 42

        group = MagicMock()
        group.ID = 10
        group.NAME = "my_vdc"
        mock_one.grouppool.info.return_value.GROUP = [group]
        mock_one.user.chgrp.side_effect = Exception("chgrp failed")

        with pytest.raises(BackendError, match="Failed to assign user"):
            client.create_user("my_vdc_admin", "s3cret", "my_vdc")

        mock_one.user.delete.assert_called_once_with(42)

    def test_create_user_group_not_found_rolls_back(self, client, mock_one):
        """Group not found → user deleted."""
        mock_one.userpool.info.return_value.USER = []
        mock_one.user.allocate.return_value = 42
        mock_one.grouppool.info.return_value.GROUP = []  # no groups

        with pytest.raises(BackendError, match="Group.*not found"):
            client.create_user("my_vdc_admin", "s3cret", "my_vdc")

        mock_one.user.delete.assert_called_once_with(42)

    # ── delete_user ──

    def test_delete_user_success(self, client, mock_one):
        """Deletes user by name."""
        user = MagicMock()
        user.ID = 42
        user.NAME = "my_vdc_admin"
        mock_one.userpool.info.return_value.USER = [user]

        client.delete_user("my_vdc_admin")

        mock_one.user.delete.assert_called_once_with(42)

    def test_delete_user_not_found(self, client, mock_one):
        """User not found → no-op."""
        mock_one.userpool.info.return_value.USER = []

        client.delete_user("nonexistent")  # should not raise
        mock_one.user.delete.assert_not_called()

    # ── get_user_credentials ──

    def test_get_user_credentials(self, client, mock_one):
        """Reads username and password from TEMPLATE."""
        user = MagicMock()
        user.ID = 42
        user.NAME = "my_vdc_admin"
        mock_one.userpool.info.return_value.USER = [user]

        user_info = MagicMock()
        user_info.TEMPLATE.WALDUR_PASSWORD = "s3cret"
        mock_one.user.info.return_value = user_info

        creds = client.get_user_credentials("my_vdc_admin")

        assert creds == {
            "opennebula_username": "my_vdc_admin",
            "opennebula_password": "s3cret",
        }

    def test_get_user_credentials_not_found(self, client, mock_one):
        """User not found → None."""
        mock_one.userpool.info.return_value.USER = []

        result = client.get_user_credentials("nonexistent")
        assert result is None

    # ── reset_user_password ──

    def test_reset_user_password(self, client, mock_one):
        """Resets auth password and updates TEMPLATE."""
        user = MagicMock()
        user.ID = 42
        user.NAME = "my_vdc_admin"
        mock_one.userpool.info.return_value.USER = [user]

        client.reset_user_password("my_vdc_admin", "n3w_pass")

        mock_one.user.passwd.assert_called_once_with(42, "n3w_pass")
        mock_one.user.update.assert_called_once_with(
            42, 'WALDUR_PASSWORD="n3w_pass"', 1
        )

    def test_reset_user_password_not_found(self, client, mock_one):
        """User not found → BackendError."""
        mock_one.userpool.info.return_value.USER = []

        with pytest.raises(BackendError, match="not found"):
            client.reset_user_password("nonexistent", "pass")


# ── VDC User Creation Integration ────────────────────────────────────


class TestOpenNebulaVDCUserCreation:
    """Test VDC user creation integration in the backend."""

    @pytest.fixture()
    def vdc_backend_with_user(self, backend_settings, backend_components):
        settings = {
            **backend_settings,
            "resource_type": "vdc",
            "create_opennebula_user": True,
        }
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(settings, backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    @pytest.fixture()
    def vdc_backend_no_user(self, backend_settings, backend_components):
        settings = {
            **backend_settings,
            "resource_type": "vdc",
            "create_opennebula_user": False,
        }
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(settings, backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    # ── username generation ──

    def test_generate_username(self):
        assert OpenNebulaBackend._generate_opennebula_username("my_vdc") == "my_vdc_admin"

    # ── post_create_resource ──

    def test_creates_user_when_enabled(self, vdc_backend_with_user):
        """Creates user and sets backend_metadata on resource."""
        from waldur_site_agent.backend.structures import BackendResourceInfo

        resource = BackendResourceInfo(backend_id="test_vdc")
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.uuid = "aaaa-bbbb-cccc"

        vdc_backend_with_user.client.create_user.return_value = 42

        vdc_backend_with_user.post_create_resource(resource, waldur_resource)

        vdc_backend_with_user.client.create_user.assert_called_once()
        call_args = vdc_backend_with_user.client.create_user.call_args
        assert call_args[0][0] == "test_vdc_admin"  # username
        assert len(call_args[0][1]) > 0  # password non-empty
        assert call_args[0][2] == "test_vdc"  # group_name

        # Check metadata was cached
        cached = vdc_backend_with_user._resource_network_metadata["test_vdc"]
        assert cached["opennebula_username"] == "test_vdc_admin"
        assert len(cached["opennebula_password"]) > 0

    def test_skips_when_disabled(self, vdc_backend_no_user):
        """Does not create user when flag is False."""
        from waldur_site_agent.backend.structures import BackendResourceInfo

        resource = BackendResourceInfo(backend_id="test_vdc")
        waldur_resource = MagicMock(spec=WaldurResource)

        vdc_backend_no_user.post_create_resource(resource, waldur_resource)

        vdc_backend_no_user.client.create_user.assert_not_called()

    def test_skips_for_vm_resource_type(
        self, vm_backend_settings, vm_backend_components
    ):
        """VM resource_type → no user creation."""
        from waldur_site_agent.backend.structures import BackendResourceInfo

        settings = {**vm_backend_settings, "create_opennebula_user": True}
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)

        resource = BackendResourceInfo(backend_id="test_vm")
        waldur_resource = MagicMock(spec=WaldurResource)

        backend.post_create_resource(resource, waldur_resource)

        backend.client.create_user.assert_not_called()

    def test_merges_with_network_metadata(self, vdc_backend_with_user):
        """Credentials are merged into existing network metadata."""
        from waldur_site_agent.backend.structures import BackendResourceInfo

        vdc_backend_with_user._resource_network_metadata["test_vdc"] = {
            "vnet_id": 50,
            "subnet_cidr": "10.0.1.0/24",
        }
        vdc_backend_with_user.client.create_user.return_value = 42

        resource = BackendResourceInfo(backend_id="test_vdc")
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.uuid = "aaaa"

        vdc_backend_with_user.post_create_resource(resource, waldur_resource)

        cached = vdc_backend_with_user._resource_network_metadata["test_vdc"]
        assert cached["vnet_id"] == 50
        assert cached["opennebula_username"] == "test_vdc_admin"

    def test_user_creation_failure_is_non_fatal(self, vdc_backend_with_user):
        """create_user raises → warning logged, no crash."""
        from waldur_site_agent.backend.structures import BackendResourceInfo

        vdc_backend_with_user.client.create_user.side_effect = BackendError(
            "API error"
        )

        resource = BackendResourceInfo(backend_id="test_vdc")
        waldur_resource = MagicMock(spec=WaldurResource)

        # Should not raise
        vdc_backend_with_user.post_create_resource(resource, waldur_resource)

    def test_pushes_metadata_to_waldur(self, vdc_backend_with_user):
        """Sets backend_metadata on the resource info object."""
        from waldur_site_agent.backend.structures import BackendResourceInfo

        vdc_backend_with_user.client.create_user.return_value = 42

        resource = BackendResourceInfo(backend_id="test_vdc")
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.uuid = "resource-uuid-123"

        vdc_backend_with_user.post_create_resource(resource, waldur_resource)

        assert resource.backend_metadata is not None
        assert resource.backend_metadata["opennebula_username"] == "test_vdc_admin"
        assert len(resource.backend_metadata["opennebula_password"]) > 0

    # ── _pre_delete_resource ──

    def test_pre_delete_removes_user(self, vdc_backend_with_user):
        """Deletes the OpenNebula user before VDC deletion."""
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "test_vdc"

        vdc_backend_with_user._pre_delete_resource(resource)

        vdc_backend_with_user.client.delete_user.assert_called_once_with(
            "test_vdc_admin"
        )

    def test_pre_delete_skips_when_disabled(self, vdc_backend_no_user):
        """Does not delete user when flag is False."""
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "test_vdc"

        vdc_backend_no_user._pre_delete_resource(resource)

        vdc_backend_no_user.client.delete_user.assert_not_called()

    def test_pre_delete_failure_is_non_fatal(self, vdc_backend_with_user, caplog):
        """delete_user raises → warning, no crash."""
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "test_vdc"

        vdc_backend_with_user.client.delete_user.side_effect = BackendError(
            "delete failed"
        )

        with caplog.at_level(logging.WARNING):
            vdc_backend_with_user._pre_delete_resource(resource)

        assert "Failed to delete OpenNebula user" in caplog.text

    # ── get_resource_metadata ──

    def test_get_metadata_returns_cached_creds(self, vdc_backend_with_user):
        """Cached credentials returned without API call."""
        vdc_backend_with_user._resource_network_metadata["test_vdc"] = {
            "opennebula_username": "test_vdc_admin",
            "opennebula_password": "cached_pass",
        }

        result = vdc_backend_with_user.get_resource_metadata("test_vdc")
        assert result["opennebula_password"] == "cached_pass"
        vdc_backend_with_user.client.get_user_credentials.assert_not_called()

    def test_get_metadata_refetches_on_cache_miss(self, vdc_backend_with_user):
        """No cache → reads from ONE user TEMPLATE."""
        vdc_backend_with_user.client.get_user_credentials.return_value = {
            "opennebula_username": "test_vdc_admin",
            "opennebula_password": "stored_pass",
        }

        result = vdc_backend_with_user.get_resource_metadata("test_vdc")
        assert result["opennebula_password"] == "stored_pass"
        vdc_backend_with_user.client.get_user_credentials.assert_called_once_with(
            "test_vdc_admin"
        )

    def test_get_metadata_empty_when_no_user(self, vdc_backend_with_user):
        """User not found → empty dict."""
        vdc_backend_with_user.client.get_user_credentials.return_value = None

        result = vdc_backend_with_user.get_resource_metadata("test_vdc")
        assert result == {}

    def test_get_metadata_no_refetch_when_disabled(self, vdc_backend_no_user):
        """Flag disabled → empty dict, no API call."""
        result = vdc_backend_no_user.get_resource_metadata("some_vdc")
        assert result == {}


# ── Password Reset Scaffold ──────────────────────────────────────────


class TestPasswordResetScaffold:
    """Test reset_vdc_user_password backend method."""

    @pytest.fixture()
    def vdc_backend(self, backend_settings, backend_components):
        settings = {
            **backend_settings,
            "resource_type": "vdc",
            "create_opennebula_user": True,
        }
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(settings, backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_reset_password_success(self, vdc_backend):
        """Resets password and returns new credentials."""
        result = vdc_backend.reset_vdc_user_password("test_vdc")

        vdc_backend.client.reset_user_password.assert_called_once()
        call_args = vdc_backend.client.reset_user_password.call_args
        assert call_args[0][0] == "test_vdc_admin"
        new_password = call_args[0][1]
        assert len(new_password) > 0

        assert result["opennebula_username"] == "test_vdc_admin"
        assert result["opennebula_password"] == new_password

        # Check cache was updated
        cached = vdc_backend._resource_network_metadata["test_vdc"]
        assert cached["opennebula_password"] == new_password

    def test_reset_password_user_not_found(self, vdc_backend):
        """User not found → BackendError from client propagates."""
        vdc_backend.client.reset_user_password.side_effect = BackendError(
            "User 'test_vdc_admin' not found"
        )

        with pytest.raises(BackendError, match="not found"):
            vdc_backend.reset_vdc_user_password("test_vdc")

    def test_reset_password_updates_existing_cache(self, vdc_backend):
        """Existing network metadata is preserved, credentials updated."""
        vdc_backend._resource_network_metadata["test_vdc"] = {
            "vnet_id": 50,
            "subnet_cidr": "10.0.1.0/24",
        }

        result = vdc_backend.reset_vdc_user_password("test_vdc")

        cached = vdc_backend._resource_network_metadata["test_vdc"]
        assert cached["vnet_id"] == 50
        assert cached["opennebula_username"] == "test_vdc_admin"
        assert cached["opennebula_password"] == result["opennebula_password"]


# ── Helpers ──────────────────────────────────────────────────────────


@pytest.fixture()
def mock_one_env():
    """Provide a patched pyone environment with exception classes."""
    with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
        mock_server = MagicMock()
        mock_pyone.OneServer.return_value = mock_server
        mock_pyone.OneException = Exception
        mock_pyone.OneInternalException = type(
            "OneInternalException", (Exception,), {}
        )
        mock_pyone.OneNoExistsException = type(
            "OneNoExistsException", (Exception,), {}
        )
        yield mock_server, mock_pyone


@pytest.fixture()
def one_client(mock_one_env):
    """Provide an OpenNebulaClient with mocked pyone."""
    mock_server, _ = mock_one_env
    client = OpenNebulaClient(
        api_url="http://localhost:2633/RPC2",
        credentials="oneadmin:testpass",
        zone_id=0,
    )
    return client, mock_server


# ── Stage 1: Idempotency Retry Paths ────────────────────────────────


class TestIdempotencyRetryPaths:
    """Tests for 'already taken' collision handling in create methods."""

    @pytest.fixture()
    def client(self, one_client):
        return one_client

    def test_create_vdc_reuses_existing_on_name_collision(self, client, mock_one_env):
        c, mock_pyone = client[0], client[1]
        _, pyone_mod = mock_one_env
        c.one.vdc.allocate.side_effect = pyone_mod.OneInternalException("already taken")
        existing_vdc = MagicMock()
        existing_vdc.ID = 99
        existing_vdc.NAME = "test_vdc"
        c.one.vdcpool.info.return_value.VDC = [existing_vdc]

        result = c._create_vdc("test_vdc")
        assert result == 99

    def test_create_group_reuses_existing_on_name_collision(self, client, mock_one_env):
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.group.allocate.side_effect = pyone_mod.OneInternalException("already taken")
        existing_group = MagicMock()
        existing_group.ID = 55
        existing_group.NAME = "test_group"
        c.one.grouppool.info.return_value.GROUP = [existing_group]

        result = c._create_group("test_group")
        assert result == 55

    def test_create_vdc_collision_but_not_found(self, client, mock_one_env):
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.vdc.allocate.side_effect = pyone_mod.OneInternalException("already taken")
        c.one.vdcpool.info.return_value.VDC = []  # not found

        with pytest.raises(BackendError, match="Failed to create VDC"):
            c._create_vdc("ghost_vdc")

    def test_create_group_collision_but_not_found(self, client, mock_one_env):
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.group.allocate.side_effect = pyone_mod.OneInternalException("already taken")
        c.one.grouppool.info.return_value.GROUP = []  # not found

        with pytest.raises(BackendError, match="Failed to create group"):
            c._create_group("ghost_group")

    def test_create_vdc_non_collision_error(self, client, mock_one_env):
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.vdc.allocate.side_effect = pyone_mod.OneInternalException("disk full")

        with pytest.raises(BackendError, match="disk full"):
            c._create_vdc("test_vdc")

    def test_add_group_to_vdc_already_assigned(self, client, mock_one_env):
        """'already assigned' in addgroup → tolerated (idempotent retry)."""
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.vdc.addgroup.side_effect = pyone_mod.OneInternalException(
            "Group 5 is already assigned to the VDC 10"
        )
        # Should not raise
        c._add_group_to_vdc(10, 5)

    def test_add_group_to_vdc_other_error(self, client, mock_one_env):
        """Non-'already assigned' error in addgroup → raises BackendError."""
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.vdc.addgroup.side_effect = pyone_mod.OneInternalException("access denied")
        with pytest.raises(BackendError, match="Failed to add group"):
            c._add_group_to_vdc(10, 5)

    def test_add_cluster_to_vdc_already_assigned(self, client, mock_one_env):
        """'already assigned' in addcluster → tolerated (idempotent retry)."""
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.vdc.addcluster.side_effect = pyone_mod.OneInternalException(
            "Cluster 0 is already assigned to the VDC 10"
        )
        # Should not raise
        c._add_clusters_to_vdc(10, [0])

    def test_add_cluster_to_vdc_other_error(self, client, mock_one_env):
        """Non-'already assigned' error in addcluster → raises BackendError."""
        c, _ = client
        _, pyone_mod = mock_one_env
        c.one.vdc.addcluster.side_effect = pyone_mod.OneInternalException("zone error")
        with pytest.raises(BackendError, match="Failed to add cluster"):
            c._add_clusters_to_vdc(10, [0])


# ── Stage 2: VDC Creation Edge Cases ────────────────────────────────


class TestVDCCreationEdgeCases:
    """Edge cases for VDC resource creation through the backend."""

    @pytest.fixture()
    def backend(self, backend_settings, backend_components, mock_one_env):
        mock_server, _ = mock_one_env
        backend = OpenNebulaBackend(backend_settings, backend_components)
        backend.client = MagicMock(spec=OpenNebulaClient)
        return backend

    def test_create_resource_with_id_existing_resource(self, backend):
        """get_resource finds existing VDC → _create_backend_resource returns False."""
        from waldur_site_agent.backend.structures import ClientResource

        backend.client.get_resource.return_value = ClientResource(
            name="existing_vdc", backend_id="existing_vdc"
        )

        result = backend._create_vdc_resource("existing_vdc", "Existing", "org")
        assert result is False
        backend.client.create_resource.assert_not_called()

    def test_crash_recovery_group_exists_vdc_created_fresh(self, mock_one_env):
        """Group 'already taken' (reused) but VDC created fresh → works."""
        mock_server, pyone_mod = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )

        # Group collision → reuse
        mock_server.group.allocate.side_effect = pyone_mod.OneInternalException(
            "already taken"
        )
        existing_group = MagicMock()
        existing_group.ID = 55
        existing_group.NAME = "test"
        mock_server.grouppool.info.return_value.GROUP = [existing_group]

        # VDC created fresh
        mock_server.vdc.allocate.return_value = 100
        cluster = MagicMock()
        cluster.ID = 0
        mock_server.clusterpool.info.return_value.CLUSTER = [cluster]

        result = client.create_resource("test", "Test", "org")
        assert result == "test"
        mock_server.vdc.addgroup.assert_called_once_with(100, 55)

    def test_crash_recovery_both_exist(self, mock_one_env):
        """Both group and VDC 'already taken' → clusters re-added, works."""
        mock_server, pyone_mod = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )

        # Group collision
        mock_server.group.allocate.side_effect = pyone_mod.OneInternalException(
            "already taken"
        )
        existing_group = MagicMock()
        existing_group.ID = 55
        existing_group.NAME = "test"
        mock_server.grouppool.info.return_value.GROUP = [existing_group]

        # VDC collision
        mock_server.vdc.allocate.side_effect = pyone_mod.OneInternalException(
            "already taken"
        )
        existing_vdc = MagicMock()
        existing_vdc.ID = 100
        existing_vdc.NAME = "test"
        mock_server.vdcpool.info.return_value.VDC = [existing_vdc]

        cluster = MagicMock()
        cluster.ID = 0
        mock_server.clusterpool.info.return_value.CLUSTER = [cluster]

        result = client.create_resource("test", "Test", "org")
        assert result == "test"


# ── Stage 3: Networking Edge Cases ───────────────────────────────────


class TestNetworkingEdgeCases:
    """Edge cases for networking orchestration."""

    @pytest.fixture()
    def client(self, mock_one_env):
        mock_server, _ = mock_one_env
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        ), mock_server

    @pytest.fixture()
    def network_config(self):
        return {
            "zone_id": 0,
            "cluster_ids": [0],
            "external_network_id": 10,
            "vxlan_phydev": "eth0",
            "virtual_router_template_id": 8,
            "default_dns": "8.8.8.8",
            "internal_network_base": "10.0.0.0",
            "internal_network_prefix": 8,
            "subnet_prefix_length": 24,
            "security_group_defaults": [
                {"direction": "INBOUND", "protocol": "TCP", "range": "22:22"},
            ],
        }

    @staticmethod
    def _mock_vm_running(mock_server):
        """Set up vm.info to return ACTIVE/RUNNING."""
        from pyone import LCM_STATE, VM_STATE

        vm_info = MagicMock()
        vm_info.STATE = VM_STATE.ACTIVE
        vm_info.LCM_STATE = LCM_STATE.RUNNING
        mock_server.vm.info.return_value = vm_info

    def test_empty_cluster_ids_falls_back_to_zero(self, client, network_config):
        """cluster_ids=[] → cluster_id=0 used for VNet placement."""
        c, mock_server = client
        network_config["cluster_ids"] = []
        network_config["subnet_cidr"] = "10.0.1.0/24"
        mock_server.vn.allocate.return_value = 50
        mock_server.vrouter.allocate.return_value = 30
        mock_server.vrouter.instantiate.return_value = 500
        mock_server.secgroup.allocate.return_value = 20
        self._mock_vm_running(mock_server)

        c._setup_networking("test", 100, network_config)
        # cluster_id = 0 passed to vn.allocate
        assert mock_server.vn.allocate.call_args[0][1] == 0

    def test_vnet_name_collision_reuses_existing(self, client, mock_one_env):
        """VNet 'already taken' → existing VNet reused."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        mock_server.vn.allocate.side_effect = pyone_mod.OneInternalException(
            "already taken"
        )
        existing_vnet = MagicMock()
        existing_vnet.ID = 77
        existing_vnet.NAME = "test_internal"
        mock_server.vnpool.info.return_value.VNET = [existing_vnet]

        result = c._create_vxlan_network(
            "test_internal", "10.0.1.0/24", "10.0.1.1", "eth0", "8.8.8.8", 0
        )
        assert result == 77

    def test_vrouter_name_collision_reuses_existing(self, client, mock_one_env):
        """VRouter 'already taken' → existing reused."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        mock_server.vrouter.allocate.side_effect = pyone_mod.OneInternalException(
            "already taken"
        )
        existing_vr = MagicMock()
        existing_vr.ID = 33
        existing_vr.NAME = "test_router"
        mock_server.vrouterpool.info.return_value.VROUTER = [existing_vr]

        result = c._create_virtual_router("test_router")
        assert result == 33

    def test_secgroup_name_collision_reuses_existing(self, client, mock_one_env):
        """SG 'already taken' → existing reused."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        mock_server.secgroup.allocate.side_effect = pyone_mod.OneInternalException(
            "already taken"
        )
        existing_sg = MagicMock()
        existing_sg.ID = 22
        existing_sg.NAME = "test_default"
        mock_server.secgrouppool.info.return_value.SECURITY_GROUP = [existing_sg]

        result = c._create_security_group("test_default", [])
        assert result == 22

    def test_empty_security_group_rules_skips_sg(self, client, network_config):
        """Empty security_group_defaults → no SG created."""
        c, mock_server = client
        network_config["security_group_defaults"] = []
        network_config["subnet_cidr"] = "10.0.1.0/24"
        mock_server.vn.allocate.return_value = 50
        mock_server.vrouter.allocate.return_value = 30
        mock_server.vrouter.instantiate.return_value = 500
        self._mock_vm_running(mock_server)

        metadata = c._setup_networking("test", 100, network_config)
        mock_server.secgroup.allocate.assert_not_called()
        assert "sg_id" not in metadata

    def test_vr_failure_triggers_vnet_rollback(self, client, network_config, mock_one_env):
        """VR instantiate fails → VNet deleted in rollback."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        network_config["subnet_cidr"] = "10.0.1.0/24"
        mock_server.vn.allocate.return_value = 50
        mock_server.vrouter.allocate.return_value = 30
        mock_server.vrouter.instantiate.side_effect = pyone_mod.OneException(
            "VR instantiate failed"
        )

        with pytest.raises(BackendError):
            c._setup_networking("test", 100, network_config)

        mock_server.vn.delete.assert_called_once_with(50)
        mock_server.vrouter.delete.assert_called_once_with(30)

    def test_rollback_failure_logs_warning(
        self, client, network_config, mock_one_env, caplog
    ):
        """VNet delete fails during rollback → warning logged, original error re-raised."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        network_config["subnet_cidr"] = "10.0.1.0/24"
        mock_server.vn.allocate.return_value = 50
        mock_server.vrouter.allocate.side_effect = pyone_mod.OneException(
            "VR creation failed"
        )
        # Rollback of VNet also fails
        mock_server.vn.delete.side_effect = pyone_mod.OneException("delete failed")

        with caplog.at_level(logging.WARNING), pytest.raises(BackendError):
            c._setup_networking("test", 100, network_config)

        assert "Failed to rollback VNet" in caplog.text

    def test_add_vnet_to_vdc_failure_triggers_rollback(
        self, client, network_config, mock_one_env
    ):
        """_add_vnet_to_vdc fails → VNet deleted."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        network_config["subnet_cidr"] = "10.0.1.0/24"
        mock_server.vn.allocate.return_value = 50
        mock_server.vdc.addvnet.side_effect = pyone_mod.OneException("addvnet failed")

        with pytest.raises(BackendError):
            c._setup_networking("test", 100, network_config)

        mock_server.vn.delete.assert_called_once_with(50)


# ── Stage 4: VDC Deletion Edge Cases ────────────────────────────────


class TestVDCDeletionEdgeCases:
    """Edge cases for VDC deletion."""

    @pytest.fixture()
    def client(self, mock_one_env):
        mock_server, _ = mock_one_env
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        ), mock_server

    def test_delete_orphaned_vdc_no_group(self, client):
        """VDC exists, group doesn't → VDC deleted, no group error."""
        c, mock_server = client
        vdc = MagicMock()
        vdc.NAME = "test"
        vdc.ID = 100
        mock_server.vdcpool.info.return_value.VDC = [vdc]
        mock_server.grouppool.info.return_value.GROUP = []
        mock_server.vrouterpool.info.return_value.VROUTER = []
        mock_server.vnpool.info.return_value.VNET = []
        mock_server.secgrouppool.info.return_value.SECURITY_GROUP = []

        c.delete_resource("test")
        mock_server.vdc.delete.assert_called_once_with(100)
        mock_server.group.delete.assert_not_called()

    def test_delete_orphaned_group_no_vdc(self, client):
        """Group exists, VDC doesn't → group deleted, no VDC error."""
        c, mock_server = client
        mock_server.vdcpool.info.return_value.VDC = []
        group = MagicMock()
        group.NAME = "test"
        group.ID = 200
        mock_server.grouppool.info.return_value.GROUP = [group]
        mock_server.vrouterpool.info.return_value.VROUTER = []
        mock_server.vnpool.info.return_value.VNET = []
        mock_server.secgrouppool.info.return_value.SECURITY_GROUP = []

        c.delete_resource("test")
        mock_server.vdc.delete.assert_not_called()
        mock_server.group.delete.assert_called_once_with(200)

    def test_delete_neither_exists(self, client):
        """Neither found → no error, clean return."""
        c, mock_server = client
        mock_server.vdcpool.info.return_value.VDC = []
        mock_server.grouppool.info.return_value.GROUP = []
        mock_server.vrouterpool.info.return_value.VROUTER = []
        mock_server.vnpool.info.return_value.VNET = []
        mock_server.secgrouppool.info.return_value.SECURITY_GROUP = []

        c.delete_resource("test")  # should not raise
        mock_server.vdc.delete.assert_not_called()
        mock_server.group.delete.assert_not_called()

    def test_delete_vr_failure_stops_teardown(self, client, mock_one_env):
        """VR delete fails → error propagates."""
        c, mock_server = client
        _, pyone_mod = mock_one_env
        vr = MagicMock()
        vr.NAME = "test_router"
        vr.ID = 30
        mock_server.vrouterpool.info.return_value.VROUTER = [vr]
        mock_server.vrouter.delete.side_effect = pyone_mod.OneException("VR delete fail")

        with pytest.raises(BackendError, match="Failed to delete virtual router"):
            c._teardown_networking("test")

    def test_delete_empty_backend_id(self, mock_one_env):
        """backend_id='' → skipped with warning via backend."""
        mock_server, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)

        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = ""

        backend.delete_resource(resource)
        backend.client.delete_vm.assert_not_called()

    def test_delete_whitespace_backend_id(self, mock_one_env):
        """backend_id='  ' → skipped with warning via backend."""
        mock_server, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)

        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "   "

        # VDC path delegates to super() which checks strip()
        backend.delete_resource(resource)

    def test_delete_base_class_skips_missing_resource(self, mock_one_env):
        """client.get_resource() returns None → base class logs warning, returns."""
        mock_server, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_resource.return_value = None

        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "nonexistent"

        # VDC path → super().delete_resource() → get_resource returns None → early return
        backend.delete_resource(resource)
        backend.client.delete_resource.assert_not_called()


# ── Stage 5: VDC Limit Update Edge Cases ────────────────────────────


class TestVDCLimitUpdateEdgeCases:
    """Edge cases for setting/collecting VDC limits."""

    @pytest.fixture()
    def client(self, mock_one_env):
        mock_server, _ = mock_one_env
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        ), mock_server

    def test_set_limits_all_zeros(self, client):
        """All-zero limits → quota template with '0' values."""
        c, mock_server = client
        group = MagicMock()
        group.NAME = "test"
        group.ID = 200
        mock_server.grouppool.info.return_value.GROUP = [group]

        c.set_resource_limits("test", {"cpu": 0, "ram": 0, "storage": 0})
        call_args = mock_server.group.quota.call_args
        tpl = call_args[0][1]
        assert 'CPU="0"' in tpl
        assert 'MEMORY="0"' in tpl
        assert 'SIZE="0"' in tpl

    def test_set_limits_negative_values_means_unlimited(self, client):
        """Negative values → '-1' in template (OpenNebula convention for unlimited)."""
        c, mock_server = client
        group = MagicMock()
        group.NAME = "test"
        group.ID = 200
        mock_server.grouppool.info.return_value.GROUP = [group]

        c.set_resource_limits("test", {"cpu": -1})
        tpl = mock_server.group.quota.call_args[0][1]
        assert 'CPU="-1"' in tpl

    def test_set_limits_group_not_found(self, client):
        """Group lookup returns None → BackendError."""
        c, mock_server = client
        mock_server.grouppool.info.return_value.GROUP = []

        with pytest.raises(BackendError, match="Group.*not found"):
            c.set_resource_limits("nonexistent", {"cpu": 10})

    def test_set_limits_empty_dict(self, client):
        """Empty limits → empty quota template, no API call."""
        c, mock_server = client
        group = MagicMock()
        group.NAME = "test"
        group.ID = 200
        mock_server.grouppool.info.return_value.GROUP = [group]

        c.set_resource_limits("test", {})
        mock_server.group.quota.assert_not_called()

    def test_set_limits_unknown_components_only(self, client):
        """Only unknown components → all skipped, empty template, no API call."""
        c, mock_server = client
        group = MagicMock()
        group.NAME = "test"
        group.ID = 200
        mock_server.grouppool.info.return_value.GROUP = [group]

        c.set_resource_limits("test", {"gpu": 4, "tpu": 2})
        mock_server.group.quota.assert_not_called()

    def test_collect_limits_unit_factor_zero_clamped(self, mock_one_env):
        """unit_factor=0 usage division → clamped to 1 via max()."""
        _, _ = mock_one_env
        components = {
            "cpu": {
                "limit": 100,
                "measured_unit": "cores",
                "unit_factor": 0,
                "accounting_type": "limit",
                "label": "CPU",
            },
        }
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            components,
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_usage_report.return_value = [
            {"resource_id": "test", "usage": {"cpu": 10}}
        ]

        report = backend._get_vdc_usage_report(["test"])
        # max(0, 1) = 1, so 10 // 1 = 10
        assert report["test"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 10

    def test_collect_limits_none_limits(self, mock_one_env):
        """waldur_resource.limits is None → empty dict."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {"cpu": {"unit_factor": 1}},
        )

        resource = MagicMock(spec=WaldurResource)
        resource.limits = None

        backend_limits, waldur_limits = backend._collect_resource_limits(resource)
        assert backend_limits == {}
        assert waldur_limits == {}


# ── Stage 6: VDC Usage Reporting Edge Cases ──────────────────────────


class TestVDCUsageReportEdgeCases:
    """Edge cases for VDC usage reporting."""

    def _make_group_info(self, cpu_used=0, mem_used=0, ds_entries=None, net_entries=None):
        """Build mock group info with optional multiple DS/NET entries."""
        info = MagicMock()
        vm_entry = MagicMock()
        vm_entry.CPU_USED = str(cpu_used)
        vm_entry.MEMORY_USED = str(mem_used)
        info.VM_QUOTA.VM = [vm_entry]

        if ds_entries is not None:
            ds_list = []
            for size_used in ds_entries:
                ds = MagicMock()
                ds.SIZE_USED = str(size_used)
                ds_list.append(ds)
            info.DATASTORE_QUOTA.DATASTORE = ds_list
        else:
            info.DATASTORE_QUOTA.DATASTORE = []

        if net_entries is not None:
            net_list = []
            for leases_used in net_entries:
                nq = MagicMock()
                nq.LEASES_USED = str(leases_used)
                net_list.append(nq)
            info.NETWORK_QUOTA.NETWORK = net_list
        else:
            info.NETWORK_QUOTA.NETWORK = []

        return info

    def test_usage_fresh_group_no_quotas(self):
        """Group has no quota set → all zeros."""
        info = MagicMock()
        info.VM_QUOTA.VM = []
        info.DATASTORE_QUOTA.DATASTORE = []
        info.NETWORK_QUOTA.NETWORK = []
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage == {}

    def test_usage_multiple_datastores_summed(self):
        """Two datastore quotas → SIZE_USED summed."""
        info = self._make_group_info(ds_entries=[1024, 2048])
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage["storage"] == 3072

    def test_usage_multiple_networks_summed(self):
        """Two network quotas → LEASES_USED summed."""
        info = self._make_group_info(net_entries=[3, 7])
        usage = OpenNebulaClient._parse_group_quota_usage(info)
        assert usage["floating_ip"] == 10

    def test_usage_unit_factor_zero_clamped(self, mock_one_env):
        """unit_factor=0 → max(0,1)=1, no division by zero."""
        _, _ = mock_one_env
        components = {
            "cpu": {"unit_factor": 0},
        }
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            components,
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_usage_report.return_value = [
            {"resource_id": "t", "usage": {"cpu": 5}}
        ]
        report = backend._get_vdc_usage_report(["t"])
        assert report["t"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 5

    def test_usage_resource_not_found_skipped(self, mock_one_env):
        """Group not in pool → warning, skipped in report."""
        _, _ = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )
        mock_server = client.one
        mock_server.grouppool.info.return_value.GROUP = []

        results = client.get_usage_report(["missing_resource"])
        assert results == []

    def test_usage_empty_resource_ids(self, mock_one_env):
        """Empty list → empty report, no API calls."""
        _, _ = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )
        results = client.get_usage_report([])
        assert results == []

    def test_usage_group_info_api_failure(self, mock_one_env):
        """_get_group_info raises → BackendError propagates."""
        mock_server, pyone_mod = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )
        group = MagicMock()
        group.NAME = "test"
        group.ID = 200
        mock_server.grouppool.info.return_value.GROUP = [group]
        mock_server.group.info.side_effect = pyone_mod.OneException("API error")

        with pytest.raises(BackendError, match="Failed to get group info"):
            client.get_usage_report(["test"])


# ── Stage 7: VM Creation Edge Cases ─────────────────────────────────


class TestVMCreationEdgeCases:
    """Edge cases for VM creation."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components, mock_one_env):
        _, _ = mock_one_env
        backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
        backend.client = MagicMock(spec=OpenNebulaClient)
        return backend

    def test_template_id_zero_treated_as_missing(self, vm_backend):
        """template_id=0 → BackendError 'requires template_id'."""
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"template_id": "0", "parent_backend_id": "vdc"}
        resource.offering_plugin_options = {}

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {},
        }

        with pytest.raises(BackendError, match="template_id"):
            vm_backend._pre_create_resource(resource, user_context=user_context)

    def test_template_id_string_converted(self, vm_backend):
        """template_id='101' → int(101) works."""
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"template_id": "101", "parent_backend_id": "vdc"}
        resource.offering_plugin_options = {}

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {},
        }

        vm_backend._pre_create_resource(resource, user_context=user_context)
        assert vm_backend._pending_vm_config["template_id"] == 101

    def test_template_id_non_numeric_raises(self, vm_backend):
        """template_id='abc' → ValueError."""
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"template_id": "abc", "parent_backend_id": "vdc"}
        resource.limits = {}

        with pytest.raises(ValueError):
            vm_backend._pre_create_resource(resource)

    def test_parent_backend_id_empty_string(self, vm_backend):
        """parent_backend_id='' → BackendError."""
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"template_id": "101", "parent_backend_id": ""}

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {},
        }

        with pytest.raises(BackendError, match="parent_backend_id"):
            vm_backend._pre_create_resource(resource, user_context=user_context)

    def test_plan_quotas_determine_vm_specs(self, vm_backend):
        """Plan quotas define VM specs (vcpu, vm_ram, vm_disk)."""
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {"template_id": "101", "parent_backend_id": "vdc"}

        user_context = {
            "plan_quotas": {"vcpu": 1, "vm_ram": 512, "vm_disk": 2048},
            "ssh_keys": {},
        }

        vm_backend._pre_create_resource(resource, user_context=user_context)
        assert vm_backend._pending_vm_config["vcpu"] == 1
        assert vm_backend._pending_vm_config["vm_ram"] == 512
        assert vm_backend._pending_vm_config["vm_disk"] == 2048

    def test_pending_vm_config_none_raises(self, vm_backend):
        """_create_vm_resource called without _pre_create → BackendError."""
        vm_backend._pending_vm_config = None
        with pytest.raises(BackendError, match="VM config not set"):
            vm_backend._create_vm_resource("test_vm")

    def test_parent_vdc_network_missing(self):
        """VNet not found → BackendError."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
        client._get_vnet_by_name = MagicMock(return_value=None)

        with pytest.raises(BackendError, match="Internal network.*not found"):
            client.create_vm(101, "vm1", "missing_vdc")

    def test_parent_vdc_sg_missing_vm_still_created(self):
        """SG not found → NIC without SG, VM still created."""
        from pyone import LCM_STATE, VM_STATE

        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=None)
        mock_group = MagicMock()
        mock_group.ID = 5
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        vm_info = MagicMock()
        vm_info.STATE = VM_STATE.ACTIVE
        vm_info.LCM_STATE = LCM_STATE.RUNNING
        client.one.vm.info.return_value = vm_info

        vm_id = client.create_vm(101, "vm1", "vdc1")
        assert vm_id == 100
        # Verify NIC template doesn't have SECURITY_GROUPS
        tpl = client.one.template.instantiate.call_args[0][3]
        assert "SECURITY_GROUPS" not in tpl

    def test_chown_fails_vm_terminated(self):
        """chown raises → VM terminate-hard called as rollback."""
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_pyone.OneException = Exception
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=MagicMock(ID=10))
        client._get_group_by_name = MagicMock(return_value=None)  # chown will fail
        client.one.template.instantiate.return_value = 100

        with pytest.raises(BackendError, match="Group.*not found"):
            client.create_vm(101, "vm1", "vdc1")
        client.one.vm.action.assert_called_once_with("terminate-hard", 100)

    def test_chown_fails_rollback_also_fails(self, mock_one_env):
        """Both chown and terminate fail → original error raised."""
        mock_server, pyone_mod = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=MagicMock(ID=10))
        client._get_group_by_name = MagicMock(return_value=None)
        mock_server.template.instantiate.return_value = 100
        mock_server.vm.action.side_effect = pyone_mod.OneException("terminate failed")

        with pytest.raises(BackendError, match="Group.*not found"):
            client.create_vm(101, "vm1", "vdc1")

    def test_ssh_key_with_double_quotes_escaped(self):
        """Double quotes in SSH key → escaped in template."""
        from pyone import LCM_STATE, VM_STATE

        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=None)
        mock_group = MagicMock()
        mock_group.ID = 5
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        vm_info = MagicMock()
        vm_info.STATE = VM_STATE.ACTIVE
        vm_info.LCM_STATE = LCM_STATE.RUNNING
        client.one.vm.info.return_value = vm_info

        client.create_vm(101, "vm1", "vdc1", ssh_key='ssh-rsa "key" comment')
        tpl = client.one.template.instantiate.call_args[0][3]
        assert r'SSH_PUBLIC_KEY="ssh-rsa \"key\" comment"' in tpl

    def test_ssh_key_empty_string_omitted(self):
        """Empty SSH key → no SSH_PUBLIC_KEY in CONTEXT."""
        from pyone import LCM_STATE, VM_STATE

        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=None)
        mock_group = MagicMock()
        mock_group.ID = 5
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        vm_info = MagicMock()
        vm_info.STATE = VM_STATE.ACTIVE
        vm_info.LCM_STATE = LCM_STATE.RUNNING
        client.one.vm.info.return_value = vm_info

        client.create_vm(101, "vm1", "vdc1", ssh_key="")
        tpl = client.one.template.instantiate.call_args[0][3]
        assert "SSH_PUBLIC_KEY" not in tpl

    def test_ip_address_none_stored_as_empty(self, vm_backend):
        """No NIC → metadata ip_address: ''."""
        vm_backend._pending_vm_config = {
            "template_id": 101,
            "parent_backend_id": "vdc",
            "ssh_public_key": "",
            "vcpu": 1,
            "vm_ram": 512,
            "vm_disk": 10240,
        }
        vm_backend.client.create_vm.return_value = 42
        vm_backend.client.get_vm_ip_address.return_value = None

        vm_backend._create_vm_resource("test_vm")
        assert vm_backend._resource_network_metadata["42"]["ip_address"] == ""


# ── Stage 8: VM Deletion Edge Cases ──────────────────────────────────


class TestVMDeletionEdgeCases:
    """Edge cases for VM deletion."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components, mock_one_env):
        _, _ = mock_one_env
        backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
        backend.client = MagicMock(spec=OpenNebulaClient)
        return backend

    def test_delete_vm_not_found_idempotent(self, mock_one_env):
        """VM ID not found → warning, no error."""
        mock_server, pyone_mod = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )
        mock_server.vm.action.side_effect = pyone_mod.OneNoExistsException("not found")
        client.delete_vm(999)  # should not raise

    def test_delete_vm_terminate_raises(self, mock_one_env):
        """vm.action fails → BackendError."""
        mock_server, pyone_mod = mock_one_env
        client = OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
        )
        mock_server.vm.action.side_effect = pyone_mod.OneException("action failed")

        with pytest.raises(BackendError, match="Failed to terminate VM"):
            client.delete_vm(100)

    def test_delete_vdc_path_not_taken_for_vm(self, vm_backend):
        """resource_type='vm' → does NOT call super().delete_resource()."""
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = "42"

        vm_backend.delete_resource(resource)
        vm_backend.client.delete_vm.assert_called_once_with(42)
        # Should NOT have called get_resource (VDC path)
        vm_backend.client.get_resource.assert_not_called()

    def test_delete_vm_none_backend_id_vm_path(self, vm_backend):
        """backend_id=None → not None is True, but empty str check catches it."""
        resource = MagicMock(spec=WaldurResource)
        resource.backend_id = None

        vm_backend.delete_resource(resource)
        vm_backend.client.delete_vm.assert_not_called()


# ── Stage 9: VM Usage Reporting Edge Cases ───────────────────────────


class TestVMUsageReportEdgeCases:
    """Edge cases for VM usage reporting."""

    def test_vm_deleted_between_list_and_query(self, mock_one_env):
        """get_vm_usage returns None → skipped with warning."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {
                "api_url": "http://localhost:2633/RPC2",
                "credentials": "a:b",
                "resource_type": "vm",
            },
            {"vcpu": {"unit_factor": 1}},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_vm_usage.return_value = None

        report = backend._get_vm_usage_report(["999"])
        assert "999" not in report
        backend.client.get_vm_usage.assert_called_once_with(999)

    def test_vm_multiple_disks_summed(self):
        """Multiple DISK entries → sizes summed."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )

        mock_info = MagicMock()
        mock_info.TEMPLATE.VCPU = "2"
        mock_info.TEMPLATE.MEMORY = "1024"
        disk1 = MagicMock()
        disk1.SIZE = "5120"
        disk2 = MagicMock()
        disk2.SIZE = "10240"
        mock_info.TEMPLATE.DISK = [disk1, disk2]
        client._get_vm_info = MagicMock(return_value=mock_info)

        usage = client.get_vm_usage(100)
        assert usage["vm_disk"] == 15360

    def test_vm_no_disk_in_template(self):
        """No DISK → vm_disk not in usage."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )

        mock_info = MagicMock()
        mock_info.TEMPLATE.VCPU = "2"
        mock_info.TEMPLATE.MEMORY = "1024"
        del mock_info.TEMPLATE.DISK  # no DISK attribute
        client._get_vm_info = MagicMock(return_value=mock_info)

        usage = client.get_vm_usage(100)
        assert "vm_disk" not in usage

    def test_vm_unit_factor_floor_division(self, mock_one_env):
        """1023 // 1024 = 0 (floor rounds down)."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {
                "api_url": "http://localhost:2633/RPC2",
                "credentials": "a:b",
                "resource_type": "vm",
            },
            {"vm_ram": {"unit_factor": 1024}},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_vm_usage.return_value = {"vm_ram": 1023}

        report = backend._get_vm_usage_report(["100"])
        assert report["100"]["TOTAL_ACCOUNT_USAGE"]["vm_ram"] == 0

    def test_vm_partial_report_on_mixed_results(self, mock_one_env):
        """2 VMs, 1 found, 1 missing → partial report."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {
                "api_url": "http://localhost:2633/RPC2",
                "credentials": "a:b",
                "resource_type": "vm",
            },
            {"vcpu": {"unit_factor": 1}},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_vm_usage.side_effect = [
            {"vcpu": 4},
            None,
        ]

        report = backend._get_vm_usage_report(["100", "200"])
        assert "100" in report
        assert "200" not in report


# ── Stage 10: VM Metadata Edge Cases ────────────────────────────────


class TestVMMetadataEdgeCases:
    """Edge cases for VM metadata retrieval."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components, mock_one_env):
        _, _ = mock_one_env
        backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
        backend.client = MagicMock(spec=OpenNebulaClient)
        return backend

    def test_cached_metadata_returned_even_if_stale(self, vm_backend):
        """Cached vm_id for deleted VM → stale data returned."""
        vm_backend._resource_network_metadata["deleted_vm"] = {
            "vm_id": 42,
            "ip_address": "10.0.1.5",
        }
        # Client would say VM doesn't exist, but cache is used first
        vm_backend.client.get_vm.return_value = None

        metadata = vm_backend.get_resource_metadata("deleted_vm")
        assert metadata["vm_id"] == 42
        vm_backend.client.get_vm.assert_not_called()

    def test_live_query_no_nic_empty_ip(self, vm_backend):
        """VM found, no NIC → ip_address: ''."""
        vm_backend.client.get_vm.return_value = {
            "vm_id": 42,
            "name": "test",
            "state": 3,
            "lcm_state": 3,
        }
        vm_backend.client.get_vm_ip_address.return_value = None

        metadata = vm_backend.get_resource_metadata("42")
        assert metadata["ip_address"] == ""

    def test_vdc_metadata_no_cache_returns_empty(self, mock_one_env):
        """VDC path, no cache → {}."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        assert backend.get_resource_metadata("some_vdc") == {}

    def test_vm_multiple_nics_first_ip_returned(self):
        """2 NICs → first NIC's IP."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )

        mock_info = MagicMock()
        nic1 = MagicMock()
        nic1.IP = "10.0.1.5"
        nic2 = MagicMock()
        nic2.IP = "192.168.1.5"
        mock_info.TEMPLATE.NIC = [nic1, nic2]
        client._get_vm_info = MagicMock(return_value=mock_info)

        ip = client.get_vm_ip_address(100)
        assert ip == "10.0.1.5"


# ── Stage 11: Ping/Diagnostics/Config Edge Cases ────────────────────


class TestPingAndConfigEdgeCases:
    """Edge cases for ping, diagnostics, and config handling."""

    def test_ping_unreachable_returns_false(self, mock_one_env):
        """API error → False."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.list_resources.side_effect = BackendError("unreachable")
        assert backend.ping() is False

    def test_ping_empty_pool_returns_true(self, mock_one_env):
        """Empty VDC pool → True."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.list_resources.return_value = []
        assert backend.ping() is True

    def test_ping_unreachable_raise_true(self, mock_one_env):
        """raise_exception=True → re-raises."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.list_resources.side_effect = BackendError("unreachable")
        with pytest.raises(BackendError):
            backend.ping(raise_exception=True)

    def test_unknown_resource_type_takes_vdc_path(self, mock_one_env):
        """resource_type='container' → else branch (VDC path)."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {
                "api_url": "http://localhost:2633/RPC2",
                "credentials": "a:b",
                "resource_type": "container",
            },
            {"cpu": {"unit_factor": 1}},
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_usage_report.return_value = [
            {"resource_id": "test", "usage": {"cpu": 5}}
        ]

        report = backend._get_usage_report(["test"])
        assert report["test"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 5

    def test_missing_unit_factor_defaults_to_one(self, mock_one_env):
        """Component without unit_factor → 1."""
        _, _ = mock_one_env
        backend = OpenNebulaBackend(
            {"api_url": "http://localhost:2633/RPC2", "credentials": "a:b"},
            {"cpu": {}},  # no unit_factor
        )
        backend.client = MagicMock(spec=OpenNebulaClient)
        backend.client.get_usage_report.return_value = [
            {"resource_id": "test", "usage": {"cpu": 10}}
        ]

        report = backend._get_vdc_usage_report(["test"])
        assert report["test"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 10


# ── SSH Key UUID Resolution ──────────────────────────────────────────


class TestSSHKeyResolution:
    """Test SSH key resolution from pre-resolved ssh_keys dict."""

    @pytest.fixture()
    def backend(self, vm_backend_settings, vm_backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            b = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            b.client = MagicMock(spec=OpenNebulaClient)
            return b

    def test_ssh_key_uuid_resolved_from_context(self):
        """UUID value is resolved to public key text via ssh_keys dict."""
        key_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        ssh_keys = {key_uuid: "ssh-rsa AAAA... user@host"}

        result = OpenNebulaBackend._resolve_ssh_key_from_context(key_uuid, ssh_keys)

        assert result == "ssh-rsa AAAA... user@host"

    def test_ssh_key_uuid_not_found_returns_empty(self):
        """UUID not in ssh_keys dict → empty string."""
        key_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        ssh_keys = {}

        result = OpenNebulaBackend._resolve_ssh_key_from_context(key_uuid, ssh_keys)

        assert result == ""

    def test_ssh_key_raw_text_passthrough(self):
        """Non-UUID text is passed through as-is (backward compatibility)."""
        raw_key = "ssh-rsa AAAA... user@host"
        ssh_keys = {}

        result = OpenNebulaBackend._resolve_ssh_key_from_context(raw_key, ssh_keys)

        assert result == raw_key

    def test_ssh_key_empty_string_no_lookup(self):
        """Empty string → empty string returned."""
        result = OpenNebulaBackend._resolve_ssh_key_from_context("", {})

        assert result == ""

    def test_ssh_key_uuid_hex_format_resolved(self):
        """UUID matched by hex (no dashes) format."""
        key_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        # Store with hex key (no dashes)
        hex_key = key_uuid.replace("-", "")
        ssh_keys = {hex_key: "ssh-rsa HEX_RESOLVED"}

        result = OpenNebulaBackend._resolve_ssh_key_from_context(key_uuid, ssh_keys)

        assert result == "ssh-rsa HEX_RESOLVED"

    def test_build_vm_config_resolves_uuid(self, backend):
        """_build_vm_config passes SSH key UUID through _resolve_ssh_key_from_context."""
        resource = MagicMock(spec=WaldurResource)
        key_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        resource.attributes = {
            "template_id": "101",
            "parent_backend_id": "my_vdc",
            "ssh_public_key": key_uuid,
        }

        user_context = {
            "plan_quotas": {"vcpu": 2, "vm_ram": 1024, "vm_disk": 10240},
            "ssh_keys": {key_uuid: "ssh-rsa RESOLVED"},
        }

        config = backend._build_vm_config(resource, user_context)

        assert config["ssh_public_key"] == "ssh-rsa RESOLVED"

    def test_build_vm_config_raw_key_backward_compat(self, backend):
        """_build_vm_config with raw SSH key passes it through unchanged."""
        resource = MagicMock(spec=WaldurResource)
        resource.attributes = {
            "template_id": "101",
            "parent_backend_id": "my_vdc",
            "ssh_public_key": "ssh-rsa AAAA... user@host",
        }

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {},
        }

        config = backend._build_vm_config(resource, user_context)
        assert config["ssh_public_key"] == "ssh-rsa AAAA... user@host"

    def test_pre_create_resource_threads_user_context(
        self, vm_backend_settings, vm_backend_components
    ):
        """_pre_create_resource threads user_context to _build_vm_config."""
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)

        resource = MagicMock(spec=WaldurResource)
        key_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        resource.attributes = {
            "template_id": "101",
            "parent_backend_id": "my_vdc",
            "ssh_public_key": key_uuid,
        }

        user_context = {
            "plan_quotas": _DEFAULT_PLAN_QUOTAS,
            "ssh_keys": {key_uuid: "ssh-rsa RESOLVED"},
        }

        backend._pre_create_resource(resource, user_context=user_context)

        assert backend._pending_vm_config["ssh_public_key"] == "ssh-rsa RESOLVED"


# ── VM provisioning polling tests ─────────────────────────────────────


class TestWaitForVMRunning:
    """Test _wait_for_vm_running polling logic."""

    @pytest.fixture()
    def client(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_one = MagicMock()
            mock_pyone.OneServer.return_value = mock_one
            c = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
            c.one = mock_one
            return c

    def _make_vm_info(self, state, lcm_state):
        info = MagicMock()
        info.STATE = state
        info.LCM_STATE = lcm_state
        return info

    def test_vm_already_running(self, client):
        """VM is ACTIVE/RUNNING on first poll — returns immediately."""
        from pyone import LCM_STATE, VM_STATE

        client.one.vm.info.return_value = self._make_vm_info(
            VM_STATE.ACTIVE, LCM_STATE.RUNNING
        )

        # Should not raise
        client._wait_for_vm_running(100, timeout=10, poll_interval=1)
        client.one.vm.info.assert_called_once_with(100)

    @patch("waldur_site_agent_opennebula.client.time")
    def test_vm_pending_then_running(self, mock_time, client):
        """VM transitions from PENDING to ACTIVE/RUNNING."""
        from pyone import LCM_STATE, VM_STATE

        mock_time.monotonic.side_effect = [0, 0, 5, 5, 10]
        mock_time.sleep = MagicMock()

        client.one.vm.info.side_effect = [
            self._make_vm_info(VM_STATE.PENDING, LCM_STATE.LCM_INIT),
            self._make_vm_info(VM_STATE.ACTIVE, LCM_STATE.RUNNING),
        ]

        client._wait_for_vm_running(100, timeout=30, poll_interval=5)
        assert client.one.vm.info.call_count == 2

    def test_vm_failed_state(self, client):
        """VM enters FAILED state — raises BackendError."""
        from pyone import LCM_STATE, VM_STATE

        client.one.vm.info.return_value = self._make_vm_info(
            VM_STATE.FAILED, LCM_STATE.LCM_INIT
        )

        with pytest.raises(BackendError, match="failure state"):
            client._wait_for_vm_running(100, timeout=10, poll_interval=1)

    def test_vm_boot_failure_lcm(self, client):
        """VM enters ACTIVE/BOOT_FAILURE — raises BackendError."""
        from pyone import LCM_STATE, VM_STATE

        client.one.vm.info.return_value = self._make_vm_info(
            VM_STATE.ACTIVE, LCM_STATE.BOOT_FAILURE
        )

        with pytest.raises(BackendError, match="failure LCM state"):
            client._wait_for_vm_running(100, timeout=10, poll_interval=1)

    @patch("waldur_site_agent_opennebula.client.time")
    def test_vm_timeout(self, mock_time, client):
        """VM stays PENDING until timeout — raises BackendError."""
        from pyone import LCM_STATE, VM_STATE

        # monotonic: first call sets deadline, then each loop iteration
        mock_time.monotonic.side_effect = [0, 5, 10, 15, 20, 25, 30, 35]
        mock_time.sleep = MagicMock()

        client.one.vm.info.return_value = self._make_vm_info(
            VM_STATE.PENDING, LCM_STATE.LCM_INIT
        )

        with pytest.raises(BackendError, match="did not reach RUNNING within 30s"):
            client._wait_for_vm_running(100, timeout=30, poll_interval=5)

    def test_create_vm_polls_for_running(self, client):
        """create_vm calls _wait_for_vm_running after instantiation."""
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        mock_sg = MagicMock()
        mock_sg.ID = 10
        mock_group = MagicMock()
        mock_group.ID = 5

        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=mock_sg)
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        client._wait_for_vm_running = MagicMock()

        vm_id = client.create_vm(
            template_id=101,
            vm_name="test_vm",
            parent_vdc_name="test_vdc",
        )

        assert vm_id == 100
        client._wait_for_vm_running.assert_called_once_with(100)

    def test_create_vm_rollback_on_poll_failure(self, client):
        """Poll failure in create_vm terminates the VM."""
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        mock_sg = MagicMock()
        mock_sg.ID = 10
        mock_group = MagicMock()
        mock_group.ID = 5

        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=mock_sg)
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        client._wait_for_vm_running = MagicMock(
            side_effect=BackendError("VM 100 entered failure state")
        )

        with pytest.raises(BackendError, match="failure state"):
            client.create_vm(
                template_id=101,
                vm_name="test_vm",
                parent_vdc_name="test_vdc",
            )

        client.one.vm.action.assert_called_once_with("terminate-hard", 100)

    def test_vr_instantiation_polls_for_running(self, client):
        """VR instantiation in _setup_networking triggers polling."""
        client._allocate_next_subnet = MagicMock(return_value="10.0.1.0")
        client._create_vxlan_network = MagicMock(return_value=50)
        client._add_vnet_to_vdc = MagicMock()
        client._create_virtual_router = MagicMock(return_value=30)
        client._instantiate_vr = MagicMock(return_value=500)
        client._wait_for_vm_running = MagicMock()
        client._create_security_group = MagicMock(return_value=20)

        network_config = {
            "zone_id": 0,
            "cluster_ids": [0],
            "external_network_id": 10,
            "vxlan_phydev": "eth0",
            "virtual_router_template_id": 8,
            "default_dns": "8.8.8.8",
            "internal_network_base": "10.0.0.0",
            "internal_network_prefix": 8,
            "subnet_prefix_length": 24,
            "security_group_defaults": [
                {"direction": "INBOUND", "protocol": "TCP", "range": "22:22"},
            ],
        }

        client._setup_networking("waldur_test", 100, network_config)

        client._wait_for_vm_running.assert_called_once_with(500)


# ── Scheduling requirements tests ─────────────────────────────────


class TestSchedRequirements:
    """Tests for SCHED_REQUIREMENTS generation and propagation."""

    def test_build_sched_from_cluster_ids(self):
        result = OpenNebulaClient._build_sched_requirements([0, 100])
        assert result == "CLUSTER_ID=0 | CLUSTER_ID=100"

    def test_build_sched_single_cluster(self):
        result = OpenNebulaClient._build_sched_requirements([5])
        assert result == "CLUSTER_ID=5"

    def test_build_sched_custom_override(self):
        result = OpenNebulaClient._build_sched_requirements(
            [0, 100], custom_expression='HYPERVISOR="kvm"'
        )
        assert result == 'HYPERVISOR="kvm"'

    def test_build_sched_empty(self):
        result = OpenNebulaClient._build_sched_requirements([])
        assert result == ""

    @pytest.fixture()
    def mock_one(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_server = MagicMock()
            mock_pyone.OneServer.return_value = mock_server
            mock_pyone.OneException = Exception
            mock_pyone.OneInternalException = type(
                "OneInternalException", (Exception,), {}
            )
            mock_pyone.OneNoExistsException = type(
                "OneNoExistsException", (Exception,), {}
            )
            mock_pyone.LCM_STATE = pyone.LCM_STATE
            mock_pyone.VM_STATE = pyone.VM_STATE
            yield mock_server

    @pytest.fixture()
    def client(self, mock_one):
        return OpenNebulaClient(
            api_url="http://localhost:2633/RPC2",
            credentials="oneadmin:testpass",
            zone_id=0,
        )

    def test_create_vm_includes_sched_requirements(self, client):
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        mock_sg = MagicMock()
        mock_sg.ID = 10
        mock_group = MagicMock()
        mock_group.ID = 5

        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=mock_sg)
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        vm_info = MagicMock()
        vm_info.STATE = pyone.VM_STATE.ACTIVE
        vm_info.LCM_STATE = pyone.LCM_STATE.RUNNING
        client.one.vm.info.return_value = vm_info

        client.create_vm(
            template_id=101,
            vm_name="test_vm",
            parent_vdc_name="test_vdc",
            cluster_ids=[0, 100],
        )

        call_args = client.one.template.instantiate.call_args
        extra_template = call_args[0][3]
        assert 'SCHED_REQUIREMENTS="CLUSTER_ID=0 | CLUSTER_ID=100"' in extra_template

    def test_create_vm_no_sched_when_empty(self, client):
        mock_vnet = MagicMock()
        mock_vnet.ID = 42
        mock_group = MagicMock()
        mock_group.ID = 5

        client._get_vnet_by_name = MagicMock(return_value=mock_vnet)
        client._get_secgroup_by_name = MagicMock(return_value=None)
        client._get_group_by_name = MagicMock(return_value=mock_group)
        client.one.template.instantiate.return_value = 100
        client.one.vm.chown.return_value = True
        vm_info = MagicMock()
        vm_info.STATE = pyone.VM_STATE.ACTIVE
        vm_info.LCM_STATE = pyone.LCM_STATE.RUNNING
        client.one.vm.info.return_value = vm_info

        client.create_vm(
            template_id=101,
            vm_name="test_vm",
            parent_vdc_name="test_vdc",
        )

        call_args = client.one.template.instantiate.call_args
        extra_template = call_args[0][3]
        assert "SCHED_REQUIREMENTS" not in extra_template

    def test_vr_instantiation_includes_sched_requirements(self, client):
        # Mock all the prerequisites for _setup_networking
        client._allocate_next_subnet = MagicMock(return_value="10.0.1.0")
        client._create_vxlan_network = MagicMock(return_value=50)
        client._add_vnet_to_vdc = MagicMock()
        client._create_virtual_router = MagicMock(return_value=60)
        client._instantiate_vr = MagicMock(return_value=70)
        client._wait_for_vm_running = MagicMock()
        client._create_security_group = MagicMock(return_value=80)

        network_config = {
            "zone_id": 0,
            "cluster_ids": [0, 100],
            "external_network_id": 1,
            "vxlan_phydev": "eth0",
            "virtual_router_template_id": 10,
            "default_dns": "8.8.8.8",
            "internal_network_base": "10.0.0.0",
            "internal_network_prefix": 8,
            "subnet_prefix_length": 24,
            "security_group_defaults": [],
        }

        client._setup_networking("test_vdc", 1, network_config)

        call_args = client._instantiate_vr.call_args
        extra_template = call_args[1]["extra_template"] if "extra_template" in (call_args[1] or {}) else call_args[0][3]
        assert 'SCHED_REQUIREMENTS="CLUSTER_ID=0 | CLUSTER_ID=100"' in extra_template


# ---------------------------------------------------------------------------
# VM Resize Tests
# ---------------------------------------------------------------------------


class TestOpenNebulaClientVMResize:
    """Test VM resize method on the client."""

    @pytest.fixture()
    def client(self):
        with patch("waldur_site_agent_opennebula.client.pyone") as mock_pyone:
            mock_one = MagicMock()
            mock_pyone.OneServer.return_value = mock_one
            c = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
            c.one = mock_one
            return c

    def _mock_vm(self, state, lcm_state=0):
        from pyone import LCM_STATE, VM_STATE

        vm = MagicMock()
        vm.ID = 42
        vm.NAME = "test_vm"
        vm.STATE = state
        vm.LCM_STATE = lcm_state
        return vm

    def _mock_vm_info(self, state, lcm_state=0, disk_size=2048):
        vm_info = MagicMock()
        vm_info.STATE = state
        vm_info.LCM_STATE = lcm_state
        disk = MagicMock()
        disk.SIZE = str(disk_size)
        vm_info.TEMPLATE.DISK = [disk]
        return vm_info

    def test_resize_running_vm_poweroff_resize_resume(self, client):
        from pyone import LCM_STATE, VM_STATE

        # First call: ACTIVE (triggers poweroff), second: POWEROFF, third: check disk,
        # fourth: ACTIVE/RUNNING after resume
        info_active = self._mock_vm_info(VM_STATE.ACTIVE, LCM_STATE.RUNNING, 2048)
        info_poweroff = self._mock_vm_info(VM_STATE.POWEROFF, 0, 2048)
        info_running = self._mock_vm_info(VM_STATE.ACTIVE, LCM_STATE.RUNNING, 5120)
        client.one.vm.info.side_effect = [
            info_active,   # initial state check
            info_poweroff, # _wait_for_vm_state
            info_poweroff, # after resize, disk check
            info_running,  # _wait_for_vm_running
        ]

        client.resize_vm(42, vcpu=2, ram_mb=1024, disk_mb=5120)

        # Verify poweroff was called
        client.one.vm.action.assert_any_call("poweroff-hard", 42)
        # Verify resize was called
        client.one.vm.resize.assert_called_once()
        resize_args = client.one.vm.resize.call_args
        assert '2' in resize_args[0][1]  # template contains vcpu
        assert '1024' in resize_args[0][1]  # template contains ram
        assert resize_args[0][2] is False  # enforce=False (Waldur is authority)
        # Verify disk resize (5120 > 2048)
        client.one.vm.diskresize.assert_called_once_with(42, 0, "5120")
        # Verify resume was called
        client.one.vm.action.assert_any_call("resume", 42)

    def test_resize_poweroff_vm_no_poweroff_needed(self, client):
        from pyone import VM_STATE

        info_poweroff = self._mock_vm_info(VM_STATE.POWEROFF, 0, 1024)
        client.one.vm.info.side_effect = [
            info_poweroff,  # initial state
            info_poweroff,  # after resize, disk check
        ]

        client.resize_vm(42, vcpu=1, ram_mb=512, disk_mb=1024)

        # No poweroff or resume should be called
        client.one.vm.action.assert_not_called()
        # Resize still called
        client.one.vm.resize.assert_called_once()
        # No disk resize (same size)
        client.one.vm.diskresize.assert_not_called()

    def test_resize_disk_shrink_skipped(self, client):
        from pyone import VM_STATE

        info = self._mock_vm_info(VM_STATE.POWEROFF, 0, 5120)
        client.one.vm.info.side_effect = [info, info]

        client.resize_vm(42, vcpu=1, ram_mb=512, disk_mb=2048)

        # Disk shrink should be skipped
        client.one.vm.diskresize.assert_not_called()

    def test_resize_vm_not_found_raises(self, client):
        client.one.vm.info.side_effect = BackendError("Failed to get VM info 999")

        with pytest.raises(BackendError, match="Failed to get VM info"):
            client.resize_vm(999, vcpu=1, ram_mb=512, disk_mb=1024)

    def test_resize_wrong_state_raises(self, client):
        from pyone import VM_STATE

        info = self._mock_vm_info(VM_STATE.STOPPED)
        client.one.vm.info.return_value = info

        with pytest.raises(BackendError, match="cannot resize"):
            client.resize_vm(42, vcpu=1, ram_mb=512, disk_mb=1024)

    def test_resize_failure_resumes_vm(self, client):
        from pyone import LCM_STATE, VM_STATE

        info_active = self._mock_vm_info(VM_STATE.ACTIVE, LCM_STATE.RUNNING)
        info_poweroff = self._mock_vm_info(VM_STATE.POWEROFF)
        client.one.vm.info.side_effect = [info_active, info_poweroff]
        client.one.vm.resize.side_effect = pyone.OneException("resize failed")

        with pytest.raises(BackendError, match="Failed to resize"):
            client.resize_vm(42, vcpu=2, ram_mb=1024, disk_mb=2048)

        # Should attempt resume after failure
        client.one.vm.action.assert_any_call("resume", 42)


class TestOpenNebulaBackendVMResize:
    """Test set_resource_limits override for VM resize."""

    @pytest.fixture()
    def vm_backend(self, vm_backend_settings, vm_backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(vm_backend_settings, vm_backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)
            return backend

    def test_vm_resize_calls_client(self, vm_backend):
        vm_backend.set_resource_limits(
            "42", {"vcpu": 2, "vm_ram": 2048, "vm_disk": 10240}
        )

        vm_backend.client.resize_vm.assert_called_once_with(
            42, 2, 2048, 10240
        )

    def test_vm_resize_skips_when_no_specs(self, vm_backend):
        vm_backend.set_resource_limits("my_vm", {})

        vm_backend.client.resize_vm.assert_not_called()

    def test_vdc_mode_sets_group_quotas(self, backend_settings, backend_components):
        with patch("waldur_site_agent_opennebula.client.pyone"):
            backend = OpenNebulaBackend(backend_settings, backend_components)
            backend.client = MagicMock(spec=OpenNebulaClient)

        backend.set_resource_limits("my_vdc", {"cpu": 4, "ram": 2048})

        backend.client.set_resource_limits.assert_called_once_with(
            "my_vdc", {"cpu": 4, "ram": 2048}
        )
