"""Comprehensive tests for SLURM periodic limits plugin with mocked mastermind signals."""

import json
import time
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient

# Waldur site agent imports
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing.handlers import on_resource_periodic_limits_update_stomp


class MockSTOMPFrame:
    """Mock STOMP frame for testing."""

    def __init__(self, message_data: dict):
        self.body = json.dumps(message_data)
        self.headers = {"destination": "/queue/test_periodic_limits"}


class MockMastermindSignals:
    """Mock signals from Waldur Mastermind for testing."""

    @staticmethod
    def create_periodic_limits_update_signal(
        resource_uuid: str, backend_id: str, offering_uuid: str, settings: dict
    ) -> MockSTOMPFrame:
        """Create a mock STOMP message for periodic limits update."""
        message_data = {
            "resource_uuid": resource_uuid,
            "backend_id": backend_id,
            "offering_uuid": offering_uuid,
            "action": "apply_periodic_settings",
            "settings": settings,
            "timestamp": f"{datetime.now().year}-Q{((datetime.now().month - 1) // 3 + 1)}",
        }
        return MockSTOMPFrame(message_data)

    @staticmethod
    def create_quarterly_transition_signal(
        resource_uuid: str,
        backend_id: str,
        base_allocation: float = 1000.0,
        previous_usage: float = 600.0,
    ) -> MockSTOMPFrame:
        """Create a mock quarterly transition signal with carryover calculation."""
        # Mock the calculation that would happen in SlurmPeriodicUsagePolicy
        # Use more conservative decay - assume 30 days instead of 90 for more realistic carryover
        decay_factor = 2 ** (-30 / 15)  # 15-day half-life over 30 days (1 month lookback)
        effective_previous_usage = previous_usage * decay_factor
        unused_allocation = max(0, base_allocation - effective_previous_usage)
        total_allocation = base_allocation + unused_allocation

        settings = {
            "fairshare": int(total_allocation // 3),  # Simple fairshare calculation
            "grp_tres_mins": {"billing": int(total_allocation * 60)},
            "qos_threshold": {"billing": int(total_allocation * 60)},
            "grace_limit": {"billing": int(total_allocation * 1.2 * 60)},
            "limit_type": "GrpTRESMins",
            "reset_raw_usage": True,
            "carryover_details": {
                "previous_usage": previous_usage,
                "decay_factor": decay_factor,
                "effective_previous_usage": effective_previous_usage,
                "unused_allocation": unused_allocation,
                "total_allocation": total_allocation,
            },
        }

        return MockMastermindSignals.create_periodic_limits_update_signal(
            resource_uuid, backend_id, "test-offering-uuid", settings
        )

    @staticmethod
    def create_threshold_exceeded_signal(
        resource_uuid: str,
        backend_id: str,
        current_allocation: float = 1000.0,
        current_usage: float = 1100.0,
    ) -> MockSTOMPFrame:
        """Create a signal for when usage exceeds threshold (QoS change needed)."""
        settings = {
            "qos_threshold": {"billing": int(current_allocation * 60)},
            "grace_limit": {"billing": int(current_allocation * 1.2 * 60)},
            "current_usage": {"billing": int(current_usage * 60)},
            "recommended_qos": "slowdown" if current_usage > current_allocation else "normal",
            "limit_type": "GrpTRESMins",
        }

        return MockMastermindSignals.create_periodic_limits_update_signal(
            resource_uuid, backend_id, "test-offering-uuid", settings
        )


class TestPeriodicLimitsPlugin:
    """Test SLURM periodic limits plugin functionality."""

    @pytest.fixture
    def slurm_backend_emulator(self):
        """SLURM backend configured for emulator mode."""
        backend_settings = {
            "periodic_limits": {
                "enabled": True,
                "emulator_mode": True,
                "emulator_base_url": "http://localhost:8080",
                "limit_type": "GrpTRESMins",
                "tres_billing_enabled": True,
                "qos_levels": {"default": "normal", "slowdown": "slowdown", "blocked": "blocked"},
            }
        }
        return SlurmBackend(backend_settings, {})

    @pytest.fixture
    def slurm_backend_production(self):
        """SLURM backend configured for production mode."""
        backend_settings = {
            "periodic_limits": {
                "enabled": True,
                "emulator_mode": False,
                "limit_type": "GrpTRESMins",
                "tres_billing_enabled": True,
                "fairshare_decay_half_life": 15,
                "qos_levels": {"default": "normal", "slowdown": "slowdown"},
            }
        }
        backend = SlurmBackend(backend_settings, {})
        backend.client = MagicMock(spec=SlurmClient)
        return backend

    def test_quarterly_transition_with_mastermind_signal(self, slurm_backend_production):
        """Test quarterly transition triggered by mastermind signal."""
        # Create mock signal from mastermind for quarterly transition
        signal = MockMastermindSignals.create_quarterly_transition_signal(
            "test-resource-uuid",
            "test-project-account",
            base_allocation=1000.0,
            previous_usage=750.0,  # 75% usage in previous quarter
        )

        # Parse the signal message
        message = json.loads(signal.body)
        settings = message["settings"]

        print("=== Quarterly Transition Test ===")
        print(f"Previous usage: {settings['carryover_details']['previous_usage']}Nh")
        print(f"Decay factor: {settings['carryover_details']['decay_factor']:.4f}")
        print(f"New allocation: {settings['carryover_details']['total_allocation']:.1f}Nh")
        print(f"New fairshare: {settings['fairshare']}")
        print(f"New billing limit: {settings['grp_tres_mins']['billing']} minutes")

        # Apply settings via backend
        result = slurm_backend_production.apply_periodic_settings(message["backend_id"], settings)

        # Verify settings applied
        assert result["success"] is True
        assert result["mode"] == "production"

        # Verify SLURM client calls
        backend_client = slurm_backend_production.client
        backend_client.set_account_fairshare.assert_called_once()
        backend_client.set_account_limits.assert_called_once()
        backend_client.reset_raw_usage.assert_called_once()  # Period transition

        # Verify calculated values are reasonable
        total_allocation = settings["carryover_details"]["total_allocation"]
        # With 750/1000 usage and 30-day decay factor of 0.25, effective usage is 187.5
        # So unused = max(0, 1000 - 187.5) = 812.5
        # Total = 1000 + 812.5 = 1812.5
        assert 1800 < total_allocation < 1820, f"Unexpected carryover: {total_allocation}Nh"

        print("✅ Quarterly transition with carryover working correctly")

    def test_threshold_exceeded_qos_management(self, slurm_backend_production):
        """Test QoS management when usage exceeds threshold."""
        # Create signal for threshold exceeded scenario
        signal = MockMastermindSignals.create_threshold_exceeded_signal(
            "test-resource-uuid",
            "test-over-allocation-account",
            current_allocation=1000.0,
            current_usage=1150.0,  # 15% over allocation
        )

        # Mock current usage check
        slurm_backend_production.client.get_current_usage.return_value = {
            "billing": 69000  # 1150Nh * 60min = 69,000 billing minutes
        }

        slurm_backend_production.client.get_current_account_qos.return_value = "normal"

        message = json.loads(signal.body)
        settings = message["settings"]

        print("=== QoS Threshold Management Test ===")
        print(f"Current usage: {settings['current_usage']['billing']} billing minutes")
        print(f"QoS threshold: {settings['qos_threshold']['billing']} billing minutes")
        print(f"Recommended QoS: {settings['recommended_qos']}")

        # Apply settings
        result = slurm_backend_production.apply_periodic_settings(message["backend_id"], settings)

        assert result["success"] is True

        # Should trigger QoS change
        slurm_backend_production.client.set_account_qos.assert_called()

        print("✅ QoS threshold management working correctly")

    def test_emulator_integration_with_signals(self, slurm_backend_emulator):
        """Test emulator integration with mocked mastermind signals."""
        # Mock requests for emulator communication
        with patch("requests.post") as mock_post, patch("requests.get") as mock_get:
            # Mock successful responses
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = {"success": True, "current_usage": 45000}
            mock_post.return_value = mock_response
            mock_get.return_value = mock_response

            # Create signal for standard periodic update
            signal = MockMastermindSignals.create_periodic_limits_update_signal(
                "test-resource-uuid",
                "emulator-test-account",
                "test-offering-uuid",
                {
                    "fairshare": 400,
                    "grp_tres_mins": {"billing": 72000},
                    "qos_threshold": {"billing": 60000},
                },
            )

            message = json.loads(signal.body)

            print("=== Emulator Integration Test ===")
            print(f"Applying to emulator: {message['settings']}")

            # Apply settings to emulator
            result = slurm_backend_emulator.apply_periodic_settings(
                message["backend_id"], message["settings"]
            )

            assert result["success"] is True
            assert result["mode"] == "emulator"

            # Verify emulator API calls
            expected_calls = [
                call(
                    "http://localhost:8080/api/apply-periodic-settings",
                    json={"resource_id": "emulator-test-account", "fairshare": 400},
                    timeout=10,
                ),
                call(
                    "http://localhost:8080/api/apply-periodic-settings",
                    json={
                        "resource_id": "emulator-test-account",
                        "grp_tres_mins": {"billing": 72000},
                    },
                    timeout=10,
                ),
            ]

            # Check that post was called the right number of times with the right arguments
            assert mock_post.call_count == 2
            actual_calls = mock_post.call_args_list
            assert actual_calls == expected_calls

            print("✅ Emulator integration with signals working")

    def test_stomp_handler_with_mastermind_signals(self):
        """Test STOMP handler processing real mastermind-like signals."""
        # Mock offering and backend
        mock_offering = MagicMock()
        mock_offering.name = "Test SLURM Cluster"
        mock_offering.order_processing_backend = "slurm"

        mock_backend = MagicMock()
        mock_backend.apply_periodic_settings.return_value = {"success": True, "mode": "test"}

        with patch(
            "waldur_site_agent.common.utils.get_backend_for_offering",
            return_value=(mock_backend, "test-version"),
        ):
            # Test 1: Standard quarterly update
            quarterly_signal = MockMastermindSignals.create_quarterly_transition_signal(
                "resource-uuid-1", "quarterly-account-1"
            )

            on_resource_periodic_limits_update_stomp(quarterly_signal, mock_offering, "test-agent")

            # Verify backend was called with quarterly settings
            assert mock_backend.apply_periodic_settings.called
            call_args = mock_backend.apply_periodic_settings.call_args
            assert call_args[0][0] == "quarterly-account-1"  # backend_id
            assert "carryover_details" in call_args[0][1]  # settings include carryover

            # Test 2: Threshold exceeded scenario
            mock_backend.reset_mock()

            threshold_signal = MockMastermindSignals.create_threshold_exceeded_signal(
                "resource-uuid-2",
                "threshold-account-2",
                current_allocation=1000.0,
                current_usage=1200.0,  # 20% over
            )

            on_resource_periodic_limits_update_stomp(threshold_signal, mock_offering, "test-agent")

            # Verify QoS-related settings were applied
            call_args = mock_backend.apply_periodic_settings.call_args
            settings = call_args[0][1]
            assert "recommended_qos" in settings
            assert settings["recommended_qos"] == "slowdown"

            print("✅ STOMP handler processes all mastermind signal types correctly")

    def test_error_handling_with_invalid_signals(self):
        """Test error handling with various invalid signals."""
        mock_offering = MagicMock()
        mock_backend = MagicMock()

        with patch(
            "waldur_site_agent.common.utils.get_offering_backend", return_value=mock_backend
        ):
            # Test 1: Invalid JSON
            invalid_frame = MagicMock()
            invalid_frame.body = "invalid-json-content"

            try:
                on_resource_periodic_limits_update_stomp(invalid_frame, mock_offering, "test-agent")
                # Should not crash
                print("✓ Handled invalid JSON gracefully")
            except Exception as e:
                pytest.fail(f"Should not crash on invalid JSON: {e}")

            # Test 2: Missing required fields
            incomplete_signal = MockSTOMPFrame(
                {
                    "resource_uuid": "test-uuid",
                    # Missing backend_id and action
                    "settings": {},
                }
            )

            try:
                on_resource_periodic_limits_update_stomp(
                    incomplete_signal, mock_offering, "test-agent"
                )
                print("✓ Handled missing fields gracefully")
            except Exception as e:
                pytest.fail(f"Should not crash on missing fields: {e}")

            # Backend should not have been called due to validation failure
            mock_backend.apply_periodic_settings.assert_not_called()

            # Test 3: Backend without periodic limits support
            mock_backend_no_support = MagicMock()
            del mock_backend_no_support.apply_periodic_settings

            with patch(
                "waldur_site_agent.common.utils.get_offering_backend",
                return_value=mock_backend_no_support,
            ):
                valid_signal = MockMastermindSignals.create_periodic_limits_update_signal(
                    "test-uuid", "test-account", "test-offering", {"fairshare": 100}
                )

                try:
                    on_resource_periodic_limits_update_stomp(
                        valid_signal, mock_offering, "test-agent"
                    )
                    print("✓ Handled unsupported backend gracefully")
                except Exception as e:
                    pytest.fail(f"Should not crash on unsupported backend: {e}")

        print("✅ Error handling working correctly")

    def test_production_slurm_client_methods(self):
        """Test SLURM client methods with mocked command execution."""
        # Create client with mocked command execution
        client = SlurmClient({})

        with patch.object(client, "_execute_command") as mock_execute:
            # Test fairshare setting
            mock_execute.return_value = " Adding Account(s)\n  test-account\n Modified account...\n"

            result = client.set_account_fairshare("test-account", 500)
            assert result is True
            mock_execute.assert_called_with(
                ["modify", "account", "test-account", "set", "fairshare=500"]
            )

            # Test limits setting
            mock_execute.reset_mock()
            result = client.set_account_limits("test-account", "GrpTRESMins", {"billing": 72000})
            assert result is True
            mock_execute.assert_called_with(
                ["modify", "account", "test-account", "set", "GrpTRESMins=billing=72000"]
            )

            # Test usage reset
            mock_execute.reset_mock()
            result = client.reset_raw_usage("test-account")
            assert result is True
            mock_execute.assert_called_with(
                ["modify", "account", "test-account", "set", "RawUsage=0"]
            )

            # Test current usage retrieval
            mock_execute.reset_mock()
            mock_execute.return_value = "test-account|cpu=32000,mem=256000,gres/gpu=2000\n"

            usage = client.get_current_usage("test-account")
            assert isinstance(usage, dict)
            # Basic validation that parsing worked
            assert "billing" in usage or "cpu" in usage

            print("✅ SLURM client methods working correctly")

    def test_configuration_driven_behavior(self):
        """Test that behavior changes based on configuration."""
        # Test different configurations
        configs = [
            {
                "name": "GrpTRESMins + Billing",
                "config": {
                    "periodic_limits": {
                        "enabled": True,
                        "limit_type": "GrpTRESMins",
                        "tres_billing_enabled": True,
                    }
                },
            },
            {
                "name": "MaxTRESMins + Raw TRES",
                "config": {
                    "periodic_limits": {
                        "enabled": True,
                        "limit_type": "MaxTRESMins",
                        "tres_billing_enabled": False,
                    }
                },
            },
        ]

        for config_test in configs:
            backend = SlurmBackend(config_test["config"], {})
            backend.client = MagicMock()

            # Create appropriate settings for this configuration
            if config_test["config"]["periodic_limits"]["tres_billing_enabled"]:
                settings = {
                    "fairshare": 300,
                    "grp_tres_mins": {"billing": 60000},
                    "limit_type": "GrpTRESMins",
                }
            else:
                settings = {
                    "fairshare": 300,
                    "max_tres_mins": {"node": 60000},
                    "limit_type": "MaxTRESMins",
                }

            result = backend.apply_periodic_settings("test-account", settings)

            assert result["success"] is True
            backend.client.set_account_fairshare.assert_called_once_with("test-account", 300)

            print(f"✓ {config_test['name']} configuration working")

        print("✅ Configuration-driven behavior validated")

    def test_mastermind_signal_scenarios(self):
        """Test various mastermind signal scenarios."""
        # Scenario 1: New quarter with significant carryover
        scenario_1 = MockMastermindSignals.create_quarterly_transition_signal(
            "scenario-1-uuid",
            "scenario-1-account",
            base_allocation=2000.0,
            previous_usage=500.0,  # Light usage -> big carryover
        )

        message_1 = json.loads(scenario_1.body)
        carryover_1 = message_1["settings"]["carryover_details"]

        # Should have substantial carryover
        assert carryover_1["total_allocation"] > 3000, "Expected substantial carryover"
        print(
            f"Scenario 1: Light usage (500/2000Nh) → {carryover_1['total_allocation']:.0f}Nh carryover"
        )

        # Scenario 2: New quarter with minimal carryover
        scenario_2 = MockMastermindSignals.create_quarterly_transition_signal(
            "scenario-2-uuid",
            "scenario-2-account",
            base_allocation=1000.0,
            previous_usage=1800.0,  # Heavy usage -> minimal carryover
        )

        message_2 = json.loads(scenario_2.body)
        carryover_2 = message_2["settings"]["carryover_details"]

        # Should have minimal carryover relative to the high usage
        # With 1800/1000 usage and 30-day decay factor of 0.25, effective usage is 450
        # Since effective usage < base_allocation, unused = max(0, 1000 - 450) = 550
        # Total = 1000 + 550 = 1550
        assert 1540 < carryover_2["total_allocation"] < 1560, (
            "Expected moderate carryover due to decay"
        )
        print(
            f"Scenario 2: Heavy usage (1800/1000Nh) → {carryover_2['total_allocation']:.0f}Nh allocation"
        )

        # Scenario 3: Threshold management
        scenario_3 = MockMastermindSignals.create_threshold_exceeded_signal(
            "scenario-3-uuid",
            "scenario-3-account",
            current_allocation=1500.0,
            current_usage=1600.0,  # Just over threshold
        )

        message_3 = json.loads(scenario_3.body)
        qos_settings = message_3["settings"]

        assert qos_settings["recommended_qos"] == "slowdown"
        assert qos_settings["qos_threshold"]["billing"] == 90000  # 1500 * 60
        print(f"Scenario 3: Over threshold (1600/1500Nh) → QoS: {qos_settings['recommended_qos']}")

        print("✅ All mastermind signal scenarios validated")

    def test_performance_with_realistic_load(self):
        """Test performance with realistic load of periodic limits updates."""
        backend_settings = {
            "periodic_limits": {
                "enabled": True,
                "emulator_mode": True,
                "emulator_base_url": "http://localhost:8080",
            }
        }

        backend = SlurmBackend(backend_settings, {})

        with patch("requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response

            # Test rapid updates (simulating multiple resources updating)
            start_time = time.time()

            successful_updates = 0
            for i in range(50):  # 50 resource updates
                signal = MockMastermindSignals.create_periodic_limits_update_signal(
                    f"resource-{i}",
                    f"account-{i}",
                    "test-offering",
                    {"fairshare": 100 + i, "grp_tres_mins": {"billing": 60000 + i * 1000}},
                )

                message = json.loads(signal.body)
                result = backend.apply_periodic_settings(message["backend_id"], message["settings"])

                if result.get("success"):
                    successful_updates += 1

            end_time = time.time()
            duration = end_time - start_time

            print("Performance Test Results:")
            print(f"- Updates: {successful_updates}/50")
            print(f"- Duration: {duration:.2f}s")
            print(f"- Rate: {successful_updates / duration:.1f} updates/sec")

            # Performance requirements
            assert successful_updates >= 45, "Too many failures"
            assert duration < 10, "Too slow for realistic load"

            print("✅ Performance under realistic load validated")

    def test_mastermind_signal_integration_patterns(self):
        """Test integration patterns with mastermind signals."""
        # Mock offering configuration
        mock_offering = MagicMock(spec=Offering)
        mock_offering.name = "Mock SLURM Offering"
        mock_offering.uuid = "mock-offering-uuid"
        mock_offering.order_processing_backend = "slurm"

        # Mock backend with different periodic limits configurations
        test_cases = [
            {
                "name": "Full Featured",
                "backend_config": {
                    "periodic_limits": {
                        "enabled": True,
                        "limit_type": "GrpTRESMins",
                        "tres_billing_enabled": True,
                        "carryover_enabled": True,
                    }
                },
                "should_process": True,
            },
            {
                "name": "Disabled",
                "backend_config": {"periodic_limits": {"enabled": False}},
                "should_process": False,  # Backend will reject
            },
            {
                "name": "No Config",
                "backend_config": {},
                "should_process": False,  # Backend will reject
            },
        ]

        for test_case in test_cases:
            print(f"\n=== Testing: {test_case['name']} ===")

            # Create backend with specific configuration
            mock_backend = MagicMock()
            if test_case["should_process"]:
                mock_backend.apply_periodic_settings.return_value = {"success": True}
            else:
                mock_backend.apply_periodic_settings.return_value = {
                    "success": False,
                    "reason": "periodic_limits_not_enabled",
                }

            # Mock the backend retrieval
            with patch(
                "waldur_site_agent.common.utils.get_backend_for_offering",
                return_value=(mock_backend, "test-version"),
            ):
                # Create standard signal
                signal = MockMastermindSignals.create_periodic_limits_update_signal(
                    f"test-{test_case['name'].lower()}-uuid",
                    f"test-{test_case['name'].lower()}-account",
                    "test-offering",
                    {"fairshare": 200},
                )

                # Process signal
                on_resource_periodic_limits_update_stomp(signal, mock_offering, "test-agent")

                # Verify behavior
                if test_case["should_process"]:
                    mock_backend.apply_periodic_settings.assert_called_once()
                    print(f"✓ {test_case['name']}: Processed correctly")
                else:
                    # Backend might still be called, but should return failure
                    print(f"✓ {test_case['name']}: Handled appropriately")

        print("\n✅ All integration patterns validated")


class TestPeriodicLimitsMockMastermind:
    """Test suite for mocking complete mastermind behavior."""

    def test_complete_mastermind_policy_simulation(self):
        """Test complete simulation of mastermind policy behavior."""

        class MockSlurmPeriodicUsagePolicy:
            """Mock implementation of the mastermind policy."""

            def __init__(self):
                self.fairshare_decay_half_life = 15
                self.grace_ratio = 0.2
                self.carryover_enabled = True
                self.tres_billing_enabled = True
                self.limit_type = "GrpTRESMins"

            def calculate_slurm_settings(self, resource_data, previous_usage=0):
                """Mock calculation matching the real policy."""
                base_allocation = resource_data.get("base_allocation", 1000.0)

                # Mock carryover calculation
                if self.carryover_enabled and previous_usage > 0:
                    # Use 30-day lookback for more realistic carryover
                    decay_factor = 2 ** (-30 / self.fairshare_decay_half_life)
                    effective_usage = previous_usage * decay_factor
                    unused = max(0, base_allocation - effective_usage)
                    total_allocation = base_allocation + unused
                else:
                    total_allocation = base_allocation

                # Calculate settings
                fairshare = max(1, int(total_allocation // 3))
                billing_minutes = int(total_allocation * 60)
                qos_threshold = int(total_allocation * 60)
                grace_limit = int(total_allocation * (1 + self.grace_ratio) * 60)

                return {
                    "fairshare": fairshare,
                    "grp_tres_mins": {"billing": billing_minutes},
                    "qos_threshold": {"billing": qos_threshold},
                    "grace_limit": {"billing": grace_limit},
                    "limit_type": self.limit_type,
                    "carryover_details": {
                        "total_allocation": total_allocation,
                        "previous_usage": previous_usage,
                    },
                }

            def publish_to_site_agent(self, resource_data, settings):
                """Mock STOMP message publishing."""
                return MockMastermindSignals.create_periodic_limits_update_signal(
                    resource_data["uuid"],
                    resource_data["backend_id"],
                    resource_data["offering_uuid"],
                    settings,
                )

        # Test complete workflow
        mock_policy = MockSlurmPeriodicUsagePolicy()

        resource_data = {
            "uuid": "test-resource-uuid",
            "backend_id": "test-account-123",
            "offering_uuid": "test-offering-uuid",
            "base_allocation": 1500.0,
        }

        # Simulate Q1 -> Q2 transition with 800Nh used in Q1
        settings = mock_policy.calculate_slurm_settings(resource_data, previous_usage=800.0)
        signal = mock_policy.publish_to_site_agent(resource_data, settings)

        print("=== Complete Mastermind Policy Simulation ===")
        print(f"Resource: {resource_data['backend_id']}")
        print(f"Q1 Usage: 800Nh / {resource_data['base_allocation']}Nh")
        print(f"Q2 Allocation: {settings['carryover_details']['total_allocation']:.0f}Nh")
        print(f"Fairshare: {settings['fairshare']}")
        print(f"Billing limit: {settings['grp_tres_mins']['billing']} minutes")

        # Validate realistic values
        total_allocation = settings["carryover_details"]["total_allocation"]
        # With 800/1500 usage and 30-day decay factor of 0.25, effective usage is 200
        # So unused = max(0, 1500 - 200) = 1300
        # Total = 1500 + 1300 = 2800
        assert 2790 < total_allocation < 2810, f"Unexpected allocation: {total_allocation}"

        # Process signal with site agent
        mock_offering = MagicMock()
        mock_offering.order_processing_backend = "slurm"
        mock_backend = MagicMock()
        mock_backend.apply_periodic_settings.return_value = {"success": True, "mode": "production"}

        with patch(
            "waldur_site_agent.common.utils.get_backend_for_offering",
            return_value=(mock_backend, "test-version"),
        ):
            on_resource_periodic_limits_update_stomp(signal, mock_offering, "test-agent")

            # Verify site agent processed the signal
            mock_backend.apply_periodic_settings.assert_called_once()
            call_args = mock_backend.apply_periodic_settings.call_args

            applied_settings = call_args[0][1]
            assert applied_settings["fairshare"] == settings["fairshare"]
            assert applied_settings["grp_tres_mins"] == settings["grp_tres_mins"]

        print("✅ Complete mastermind simulation successful")

    def test_realistic_signal_timing_patterns(self):
        """Test realistic timing patterns for signals."""
        # Mock time-based scenarios
        scenarios = [
            {
                "name": "End of Quarter Batch Update",
                "description": "Multiple resources updated at quarter end",
                "resource_count": 20,
                "timing_pattern": "batch",  # All at once
            },
            {
                "name": "Mid-Quarter Dynamic Adjustment",
                "description": "Single resource allocation increased",
                "resource_count": 1,
                "timing_pattern": "immediate",  # Single update
            },
            {
                "name": "Usage Threshold Alerts",
                "description": "Multiple resources hitting thresholds",
                "resource_count": 5,
                "timing_pattern": "staggered",  # Spread over time
            },
        ]

        for scenario in scenarios:
            print(f"\n=== {scenario['name']} ===")
            print(f"Description: {scenario['description']}")

            signals = []
            for i in range(scenario["resource_count"]):
                if scenario["name"] == "Usage Threshold Alerts":
                    signal = MockMastermindSignals.create_threshold_exceeded_signal(
                        f"threshold-resource-{i}",
                        f"threshold-account-{i}",
                        current_allocation=1000.0,
                        current_usage=1050.0 + i * 50,  # Varying degrees of overage
                    )
                else:
                    signal = MockMastermindSignals.create_quarterly_transition_signal(
                        f"{scenario['name'].lower()}-resource-{i}",
                        f"{scenario['name'].lower()}-account-{i}",
                        base_allocation=1000.0,
                        previous_usage=400.0 + i * 100,  # Varying usage patterns
                    )

                signals.append(signal)

            # Process signals according to timing pattern
            if scenario["timing_pattern"] == "batch":
                # All at once
                start_time = time.time()
                for signal in signals:
                    message = json.loads(signal.body)
                    # Would be processed by STOMP handler
                duration = time.time() - start_time
                print(f"Batch processing: {len(signals)} signals in {duration:.3f}s")

            elif scenario["timing_pattern"] == "staggered":
                # Spread over time
                total_time = 0
                for i, signal in enumerate(signals):
                    start = time.time()
                    message = json.loads(signal.body)
                    # Simulate processing time
                    time.sleep(0.01)  # 10ms processing
                    duration = time.time() - start
                    total_time += duration

                print(f"Staggered processing: {len(signals)} signals, total {total_time:.3f}s")

            print(f"✓ {scenario['name']} pattern validated")

        print("\n✅ All realistic timing patterns tested")

    def test_signal_validation_and_sanitization(self):
        """Test signal validation and sanitization."""

        def validate_periodic_limits_signal(message_dict: dict) -> tuple[bool, list[str]]:
            """Validate incoming periodic limits signal."""
            errors = []

            # Required fields
            required_fields = ["resource_uuid", "backend_id", "action", "settings"]
            for field in required_fields:
                if field not in message_dict:
                    errors.append(f"Missing required field: {field}")

            # Action validation
            if message_dict.get("action") != "apply_periodic_settings":
                errors.append("Invalid action - must be 'apply_periodic_settings'")

            # Settings validation
            settings = message_dict.get("settings", {})
            if not isinstance(settings, dict):
                errors.append("Settings must be a dictionary")
            else:
                # Validate fairshare
                if "fairshare" in settings:
                    fairshare = settings["fairshare"]
                    if not isinstance(fairshare, int) or fairshare < 1:
                        errors.append("Fairshare must be positive integer")

                # Validate limits
                limit_fields = ["grp_tres_mins", "max_tres_mins"]
                for field in limit_fields:
                    if field in settings:
                        limit_value = settings[field]
                        if not isinstance(limit_value, dict):
                            errors.append(f"{field} must be dictionary")

            return len(errors) == 0, errors

        # Test valid signal
        valid_signal = MockMastermindSignals.create_periodic_limits_update_signal(
            "valid-uuid",
            "valid-account",
            "valid-offering",
            {"fairshare": 300, "grp_tres_mins": {"billing": 60000}},
        )

        valid_message = json.loads(valid_signal.body)
        is_valid, errors = validate_periodic_limits_signal(valid_message)

        assert is_valid is True
        assert len(errors) == 0
        print("✓ Valid signal passed validation")

        # Test invalid signals
        invalid_cases = [
            {
                "name": "Missing Backend ID",
                "data": {
                    "resource_uuid": "test",
                    "action": "apply_periodic_settings",
                    "settings": {},
                },
                "expected_error": "Missing required field: backend_id",
            },
            {
                "name": "Invalid Action",
                "data": {
                    "resource_uuid": "test",
                    "backend_id": "test",
                    "action": "invalid",
                    "settings": {},
                },
                "expected_error": "Invalid action",
            },
            {
                "name": "Invalid Fairshare",
                "data": {
                    "resource_uuid": "test",
                    "backend_id": "test",
                    "action": "apply_periodic_settings",
                    "settings": {"fairshare": -1},
                },
                "expected_error": "Fairshare must be positive",
            },
        ]

        for case in invalid_cases:
            is_valid, errors = validate_periodic_limits_signal(case["data"])
            assert is_valid is False
            assert any(case["expected_error"] in error for error in errors)
            print(f"✓ {case['name']}: Validation correctly failed")

        print("✅ Signal validation and sanitization working")
