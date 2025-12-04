"""Pytest configuration and fixtures for historical usage tests."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

# Import emulator components - conditional import to handle missing package gracefully
try:
    from emulator.commands.sacct import SacctEmulator
    from emulator.commands.sacctmgr import SacctmgrEmulator
    from emulator.core.database import Account, SlurmDatabase, UsageRecord, User
    from emulator.core.time_engine import TimeEngine

    EMULATOR_AVAILABLE = True
except ImportError:
    EMULATOR_AVAILABLE = False


@pytest.fixture
def emulator_available():
    """Check if SLURM emulator is available."""
    if not EMULATOR_AVAILABLE:
        pytest.skip("slurm-emulator not installed")
    return True


@pytest.fixture
def time_engine():
    """Create a time engine for testing."""
    engine = TimeEngine()
    # Start at a known date
    engine.set_time(datetime(2024, 1, 1))
    return engine


@pytest.fixture
def slurm_database():
    """Create a clean SLURM database for testing."""
    database = SlurmDatabase()

    # Add test accounts
    test_account = Account(
        name="test_account_123",
        description="Test Account for Historical Usage",
        organization="test_org",
        allocation=1000,
    )
    database.accounts["test_account_123"] = test_account

    # Add test users
    user1 = User(name="testuser1", default_account="test_account_123")
    user2 = User(name="testuser2", default_account="test_account_123")
    database.users["testuser1"] = user1
    database.users["testuser2"] = user2

    return database


@pytest.fixture
def sacct_emulator(slurm_database, time_engine):
    """Create a sacct emulator instance."""
    return SacctEmulator(slurm_database, time_engine)


@pytest.fixture
def sacctmgr_emulator(slurm_database, time_engine):
    """Create a sacctmgr emulator instance."""
    return SacctmgrEmulator(slurm_database, time_engine)


@pytest.fixture
def historical_usage_data(slurm_database, time_engine):
    """Setup historical usage data across multiple months."""
    # January 2024 usage
    time_engine.set_time(datetime(2024, 1, 15))
    usage_records = [
        UsageRecord(
            account="test_account_123",
            user="testuser1",
            node_hours=150.0,
            billing_units=150.0,
            timestamp=datetime(2024, 1, 15),
            period="2024-01",
            raw_tres={
                "CPU": 9600,
                "Mem": 153600,
                "GRES/gpu": 9000,
            },  # 150h * 64 cores, 150h * 1024MB, 150h * 60min
        ),
        UsageRecord(
            account="test_account_123",
            user="testuser2",
            node_hours=100.0,
            billing_units=100.0,
            timestamp=datetime(2024, 1, 20),
            period="2024-01",
            raw_tres={"CPU": 6400, "Mem": 102400, "GRES/gpu": 6000},  # 100h * 64 cores, etc.
        ),
    ]

    # February 2024 usage
    time_engine.set_time(datetime(2024, 2, 10))
    usage_records.extend(
        [
            UsageRecord(
                account="test_account_123",
                user="testuser1",
                node_hours=200.0,
                billing_units=200.0,
                timestamp=datetime(2024, 2, 10),
                period="2024-02",
                raw_tres={"CPU": 12800, "Mem": 204800, "GRES/gpu": 12000},
            ),
            UsageRecord(
                account="test_account_123",
                user="testuser2",
                node_hours=150.0,
                billing_units=150.0,
                timestamp=datetime(2024, 2, 15),
                period="2024-02",
                raw_tres={"CPU": 9600, "Mem": 153600, "GRES/gpu": 9000},
            ),
        ]
    )

    # March 2024 usage
    time_engine.set_time(datetime(2024, 3, 5))
    usage_records.extend(
        [
            UsageRecord(
                account="test_account_123",
                user="testuser1",
                node_hours=100.0,
                billing_units=100.0,
                timestamp=datetime(2024, 3, 5),
                period="2024-03",
                raw_tres={"CPU": 6400, "Mem": 102400, "GRES/gpu": 6000},
            ),
            UsageRecord(
                account="test_account_123",
                user="testuser2",
                node_hours=250.0,
                billing_units=250.0,
                timestamp=datetime(2024, 3, 10),
                period="2024-03",
                raw_tres={"CPU": 16000, "Mem": 256000, "GRES/gpu": 15000},
            ),
        ]
    )

    # Add all usage records to database
    for record in usage_records:
        slurm_database.usage_records.append(record)

    return {
        "2024-01": {"testuser1": 150.0, "testuser2": 100.0, "total": 250.0},
        "2024-02": {"testuser1": 200.0, "testuser2": 150.0, "total": 350.0},
        "2024-03": {"testuser1": 100.0, "testuser2": 250.0, "total": 350.0},
    }


@pytest.fixture
def mock_slurm_backend_config():
    """Mock SLURM backend configuration for testing."""
    return {
        "default_account": "root",
        "customer_prefix": "test_customer_",
        "project_prefix": "test_project_",
        "allocation_prefix": "test_alloc_",
        "qos_default": "normal",
        "qos_downscaled": "slowdown",
        "qos_paused": "blocked",
    }


@pytest.fixture
def mock_slurm_tres():
    """Mock SLURM TRES configuration for testing."""
    return {
        "cpu": {
            "limit": 10000,
            "measured_unit": "k-Hours",
            "unit_factor": 60000,  # Convert CPU-minutes to k-Hours
            "accounting_type": "usage",
            "label": "CPU",
        },
        "mem": {
            "limit": 10000,
            "measured_unit": "gb-Hours",
            "unit_factor": 61440,  # Convert MB-minutes to gb-Hours
            "accounting_type": "usage",
            "label": "RAM",
        },
        "gres/gpu": {
            "limit": 1000,
            "measured_unit": "gpu-Hours",
            "unit_factor": 60,  # Convert GPU-minutes to gpu-Hours
            "accounting_type": "usage",
            "label": "GPU",
        },
    }


@pytest.fixture
def patched_slurm_client(sacct_emulator, sacctmgr_emulator):
    """Patch SlurmClient to use emulator instead of real SLURM commands."""

    def mock_execute_command(
        args, command_name="sacctmgr", immediate=True, parsable=True, silent=False
    ):
        """Mock execute command that routes to emulator."""
        if command_name == "sacct":
            return sacct_emulator.handle_command(args)
        if command_name == "sacctmgr":
            return sacctmgr_emulator.handle_command(args)
        if command_name == "sinfo":
            return "slurm-emulator 0.1.0"
        return f"Unknown command: {command_name}"

    with patch(
        "waldur_site_agent_slurm.client.SlurmClient._execute_command",
        side_effect=mock_execute_command,
    ):
        yield


class MockWaldurResource:
    """Mock Waldur resource for testing."""

    def __init__(self, name="test_resource", backend_id="test_account_123"):
        self.name = name
        self.backend_id = backend_id
        self.uuid = Mock()
        self.uuid.hex = "12345678-1234-1234-1234-123456789abc"


class MockOfferingUser:
    """Mock Waldur offering user for testing."""

    def __init__(self, username, url=None):
        self.username = username
        self.url = url or f"https://waldur.example.com/api/marketplace-offering-users/{username}/"


class MockComponentUsage:
    """Mock Waldur component usage for testing."""

    def __init__(self, type_, uuid_str=None):
        self.type_ = type_
        self.uuid = Mock()
        self.uuid.hex = uuid_str or f"component-{type_}-uuid"


@pytest.fixture
def mock_waldur_resources():
    """Mock Waldur resources for testing."""
    return [MockWaldurResource()]


@pytest.fixture
def mock_offering_users():
    """Mock Waldur offering users for testing."""
    return [
        MockOfferingUser("testuser1"),
        MockOfferingUser("testuser2"),
    ]


@pytest.fixture
def mock_component_usages():
    """Mock Waldur component usages for testing."""
    return [
        MockComponentUsage("cpu"),
        MockComponentUsage("mem"),
        MockComponentUsage("gres/gpu"),
    ]
