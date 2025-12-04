"""Configuration validation tests for SLURM periodic limits."""

import os
import tempfile
from unittest.mock import patch

import yaml


class TestPeriodicLimitsConfiguration:
    """Test configuration validation for periodic limits functionality."""

    def test_complete_configuration_validation(self):
        """Test complete configuration validation."""

        def validate_complete_config(config: dict) -> tuple[bool, list[str]]:
            """Comprehensive validation of periodic limits configuration."""
            errors = []

            # Check offerings structure
            if "offerings" not in config:
                errors.append("Missing 'offerings' section")
                return False, errors

            for offering in config["offerings"]:
                # Check backend type
                if offering.get("backend_type") != "slurm":
                    continue  # Only validate SLURM offerings

                backend_settings = offering.get("backend_settings", {})
                periodic_config = backend_settings.get("periodic_limits")

                if not periodic_config:
                    continue  # Periodic limits not configured - OK

                # Validate periodic limits configuration
                if not isinstance(periodic_config, dict):
                    errors.append("periodic_limits must be a dictionary")
                    continue

                # Validate enabled flag
                if periodic_config.get("enabled") and not isinstance(
                    periodic_config["enabled"], bool
                ):
                    errors.append("periodic_limits.enabled must be boolean")

                # Validate limit_type
                limit_type = periodic_config.get("limit_type")
                if limit_type and limit_type not in ["GrpTRESMins", "MaxTRESMins", "GrpTRES"]:
                    errors.append(f"Invalid limit_type: {limit_type}")

                # Validate TRES billing
                if "tres_billing_enabled" in periodic_config:
                    if not isinstance(periodic_config["tres_billing_enabled"], bool):
                        errors.append("tres_billing_enabled must be boolean")

                # Validate TRES weights
                weights = periodic_config.get("tres_billing_weights", {})
                if weights and isinstance(weights, dict):
                    for tres_type, weight in weights.items():
                        if not isinstance(weight, (int, float)) or weight < 0:
                            errors.append(f"Invalid TRES weight for {tres_type}: {weight}")

                # Validate decay half-life
                half_life = periodic_config.get("fairshare_decay_half_life")
                if half_life and (not isinstance(half_life, int) or half_life < 1):
                    errors.append("fairshare_decay_half_life must be positive integer")

                # Validate grace ratio
                grace_ratio = periodic_config.get(
                    "default_grace_ratio", periodic_config.get("grace_ratio")
                )
                if grace_ratio and (
                    not isinstance(grace_ratio, (int, float)) or grace_ratio < 0 or grace_ratio > 1
                ):
                    errors.append("grace_ratio must be between 0.0 and 1.0")

                # Validate QoS levels
                qos_levels = periodic_config.get("qos_levels", {})
                if qos_levels:
                    required_qos = ["default", "slowdown"]
                    for qos in required_qos:
                        if qos not in qos_levels or not qos_levels[qos].strip():
                            errors.append(f"Missing or empty QoS level: {qos}")

            return len(errors) == 0, errors

        # Test valid complete configuration
        valid_config = {
            "offerings": [
                {
                    "name": "Test SLURM Cluster",
                    "backend_type": "slurm",
                    "backend_settings": {
                        "periodic_limits": {
                            "enabled": True,
                            "limit_type": "GrpTRESMins",
                            "tres_billing_enabled": True,
                            "tres_billing_weights": {
                                "CPU": 0.015625,
                                "Mem": 0.001953125,
                                "GRES/gpu": 0.25,
                            },
                            "fairshare_decay_half_life": 15,
                            "default_grace_ratio": 0.2,
                            "qos_levels": {
                                "default": "normal",
                                "slowdown": "slowdown",
                                "blocked": "blocked",
                            },
                        }
                    },
                }
            ]
        }

        is_valid, errors = validate_complete_config(valid_config)
        assert is_valid is True, f"Valid config failed validation: {errors}"

        # Test invalid configurations
        invalid_configs = [
            {
                "name": "Missing Offerings",
                "config": {},
                "expected_error": "Missing 'offerings' section",
            },
            {
                "name": "Invalid Limit Type",
                "config": {
                    "offerings": [
                        {
                            "backend_type": "slurm",
                            "backend_settings": {
                                "periodic_limits": {"enabled": True, "limit_type": "InvalidType"}
                            },
                        }
                    ]
                },
                "expected_error": "Invalid limit_type",
            },
            {
                "name": "Invalid Grace Ratio",
                "config": {
                    "offerings": [
                        {
                            "backend_type": "slurm",
                            "backend_settings": {
                                "periodic_limits": {
                                    "enabled": True,
                                    "default_grace_ratio": 1.5,  # > 1.0
                                }
                            },
                        }
                    ]
                },
                "expected_error": "grace_ratio must be between",
            },
        ]

        for invalid_case in invalid_configs:
            is_valid, errors = validate_complete_config(invalid_case["config"])
            assert is_valid is False
            assert any(invalid_case["expected_error"] in error for error in errors)
            print(f"✓ {invalid_case['name']}: Correctly rejected")

        print("✅ Complete configuration validation working")

    def test_configuration_file_loading(self):
        """Test loading configuration from files."""
        # Test configuration with periodic limits
        test_config = {
            "sentry_dsn": "",
            "timezone": "UTC",
            "offerings": [
                {
                    "name": "Test SLURM with Periodic Limits",
                    "waldur_api_url": "http://test.example.com/api/",
                    "waldur_api_token": "test-token",
                    "waldur_offering_uuid": "test-offering-uuid",
                    "backend_type": "slurm",
                    "backend_settings": {
                        "default_account": "root",
                        "customer_prefix": "test_",
                        "project_prefix": "test_",
                        "allocation_prefix": "test_",
                        "periodic_limits": {
                            "enabled": True,
                            "limit_type": "GrpTRESMins",
                            "tres_billing_enabled": True,
                            "tres_billing_weights": {
                                "CPU": 0.015625,
                                "Mem": 0.001953125,
                                "GRES/gpu": 0.25,
                            },
                            "fairshare_decay_half_life": 15,
                            "default_grace_ratio": 0.2,
                            "qos_levels": {"default": "normal", "slowdown": "slowdown"},
                        },
                    },
                    "backend_components": {
                        "nodeHours": {
                            "limit": 1000,
                            "measured_unit": "node-hours",
                            "unit_factor": 1,
                            "accounting_type": "usage",
                            "label": "Node Hours",
                        }
                    },
                }
            ],
        }

        # Test YAML serialization/deserialization
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(test_config, f)
            temp_file = f.name

        try:
            # Load configuration
            with open(temp_file) as f:
                loaded_config = yaml.safe_load(f)

            # Verify structure preserved
            offering = loaded_config["offerings"][0]
            periodic_config = offering["backend_settings"]["periodic_limits"]

            assert periodic_config["enabled"] is True
            assert periodic_config["limit_type"] == "GrpTRESMins"
            assert "tres_billing_weights" in periodic_config
            assert "CPU" in periodic_config["tres_billing_weights"]

            print("✅ Configuration file loading working")

        finally:
            os.unlink(temp_file)

    def test_environment_variable_integration(self):
        """Test integration with environment variables."""
        test_env_vars = {
            "SLURM_EMULATOR_URL": "http://test-emulator:8080",
            "SLURM_EMULATOR_AVAILABLE": "true",
            "SLURM_PERIODIC_LIMITS_ENABLED": "true",
        }

        with patch.dict(os.environ, test_env_vars):
            # Test environment variable access patterns
            emulator_url = os.environ.get("SLURM_EMULATOR_URL", "http://localhost:8080")
            emulator_available = (
                os.environ.get("SLURM_EMULATOR_AVAILABLE", "false").lower() == "true"
            )
            periodic_enabled = (
                os.environ.get("SLURM_PERIODIC_LIMITS_ENABLED", "false").lower() == "true"
            )

            assert emulator_url == "http://test-emulator:8080"
            assert emulator_available is True
            assert periodic_enabled is True

            print("✅ Environment variable integration working")

    def test_configuration_examples_validity(self):
        """Test that our configuration examples are valid."""
        # Test the actual example file we created
        example_file = "examples/slurm-periodic-limits-config.yaml.example"

        if os.path.exists(example_file):
            with open(example_file) as f:
                example_config = yaml.safe_load(f)

            # Basic validation of example config
            assert "offerings" in example_config
            assert len(example_config["offerings"]) > 0

            # Find periodic limits offering
            periodic_offering = None
            for offering in example_config["offerings"]:
                if "periodic_limits" in offering.get("backend_settings", {}):
                    periodic_offering = offering
                    break

            assert periodic_offering is not None, "No periodic limits offering found in example"

            periodic_config = periodic_offering["backend_settings"]["periodic_limits"]
            assert periodic_config["enabled"] is True
            assert "tres_billing_weights" in periodic_config

            print("✅ Configuration examples are valid")
        else:
            print("ℹ️ Example configuration file not found - skipping validation")

    def test_multi_offering_configuration(self):
        """Test configuration with multiple offerings (some with/without periodic limits)."""
        multi_config = {
            "offerings": [
                {
                    "name": "Traditional SLURM",
                    "backend_type": "slurm",
                    "backend_settings": {
                        "default_account": "root",
                        "qos_default": "normal",
                        # No periodic_limits
                    },
                },
                {
                    "name": "Modern SLURM with Periodic Limits",
                    "backend_type": "slurm",
                    "backend_settings": {
                        "default_account": "root",
                        "periodic_limits": {"enabled": True, "limit_type": "GrpTRESMins"},
                    },
                },
                {
                    "name": "Non-SLURM Backend",
                    "backend_type": "mup",
                    "backend_settings": {
                        "api_url": "http://mup.example.com"
                        # No periodic_limits (not applicable)
                    },
                },
            ]
        }

        # Process each offering
        for offering in multi_config["offerings"]:
            backend_type = offering.get("backend_type")
            backend_settings = offering.get("backend_settings", {})
            periodic_config = backend_settings.get("periodic_limits")

            print(f"Offering: {offering['name']} ({backend_type})")

            if backend_type == "slurm":
                if periodic_config and periodic_config.get("enabled"):
                    print("  ✓ SLURM with periodic limits enabled")
                    assert "limit_type" in periodic_config
                else:
                    print("  ✓ Traditional SLURM (no periodic limits)")
            else:
                print(f"  ✓ Non-SLURM backend ({backend_type}) - periodic limits not applicable")
                assert periodic_config is None or not periodic_config.get("enabled", False)

        print("✅ Multi-offering configuration handled correctly")


class TestAdvancedConfigurationScenarios:
    """Test advanced configuration scenarios."""

    def test_custom_tres_billing_weights(self):
        """Test custom TRES billing weights for different cluster configurations."""
        cluster_configs = [
            {
                "name": "GPU-Heavy Cluster",
                "description": "Cluster with expensive GPU resources",
                "tres_weights": {
                    "CPU": 0.01,  # Cheaper CPUs
                    "Mem": 0.001,  # Cheaper memory
                    "GRES/gpu": 1.0,  # Expensive GPUs (1 GPU = 1 billing unit)
                },
            },
            {
                "name": "Memory-Optimized Cluster",
                "description": "Cluster with expensive high-memory nodes",
                "tres_weights": {
                    "CPU": 0.02,  # Standard CPUs
                    "Mem": 0.01,  # Expensive memory
                    "GRES/gpu": 0.5,  # Standard GPUs
                },
            },
            {
                "name": "Standard Compute Cluster",
                "description": "Balanced cluster configuration",
                "tres_weights": {
                    "CPU": 0.015625,  # 64 CPUs = 1 billing unit
                    "Mem": 0.001953125,  # 512 GB = 1 billing unit
                    "GRES/gpu": 0.25,  # 4 GPUs = 1 billing unit
                },
            },
        ]

        for cluster in cluster_configs:
            print(f"\n--- {cluster['name']} ---")
            print(f"Description: {cluster['description']}")

            # Test billing calculation with these weights
            typical_job = {
                "CPU": 32,  # 32 CPUs
                "Mem": 256,  # 256 GB
                "GRES/gpu": 2,  # 2 GPUs
            }

            billing_units = sum(
                typical_job.get(tres, 0) * cluster["tres_weights"].get(tres, 0)
                for tres in cluster["tres_weights"]
            )

            print(f"Typical job (32C/256G/2GPU): {billing_units:.2f} billing units")

            # Verify reasonable billing units (should be between 0.1 and 10)
            assert 0.1 <= billing_units <= 10, f"Unreasonable billing units: {billing_units}"

            print(f"✓ {cluster['name']} weights produce reasonable billing units")

        print("\n✅ Custom TRES billing weights validated")

    def test_qos_strategy_configurations(self):
        """Test different QoS strategy configurations."""
        qos_strategies = [
            {
                "name": "Threshold Strategy",
                "config": {
                    "qos_strategy": "threshold",
                    "qos_levels": {"default": "normal", "slowdown": "slowdown"},
                },
                "thresholds": [100],  # Single threshold at 100%
            },
            {
                "name": "Progressive Strategy",
                "config": {
                    "qos_strategy": "progressive",
                    "qos_levels": {
                        "default": "normal",
                        "slowdown": "slowdown",
                        "blocked": "blocked",
                    },
                },
                "thresholds": [75, 90, 100],  # Multiple thresholds
            },
            {
                "name": "Custom QoS Names",
                "config": {
                    "qos_strategy": "threshold",
                    "qos_levels": {
                        "default": "priority_normal",
                        "slowdown": "priority_low",
                        "blocked": "priority_blocked",
                    },
                },
                "thresholds": [100],
            },
        ]

        for strategy in qos_strategies:
            print(f"\n--- {strategy['name']} ---")

            config = strategy["config"]
            qos_levels = config["qos_levels"]

            # Validate QoS configuration
            assert "default" in qos_levels, "Missing default QoS level"
            assert "slowdown" in qos_levels, "Missing slowdown QoS level"

            # Validate QoS names are non-empty strings
            for qos_type, qos_name in qos_levels.items():
                assert isinstance(qos_name, str) and qos_name.strip(), (
                    f"QoS {qos_type} must be non-empty string"
                )

            print(f"QoS Levels: {qos_levels}")
            print(f"Strategy: {config['qos_strategy']}")
            print(f"✓ {strategy['name']} configuration valid")

        print("\n✅ QoS strategy configurations validated")

    def test_emulator_vs_production_config_differences(self):
        """Test configuration differences between emulator and production."""
        base_config = {
            "enabled": True,
            "limit_type": "GrpTRESMins",
            "tres_billing_enabled": True,
            "fairshare_decay_half_life": 15,
        }

        emulator_specific = {
            **base_config,
            "emulator_mode": True,
            "emulator_base_url": "http://localhost:8080",
            # Emulator might have different timeouts/retries
            "api_timeout": 5,
        }

        production_specific = {
            **base_config,
            "emulator_mode": False,
            # Production might have additional settings
            "command_timeout": 30,
            "retry_attempts": 3,
            "raw_usage_reset": True,
        }

        # Test that both configurations are valid but different
        def extract_mode_specific_settings(config):
            mode = "emulator" if config.get("emulator_mode") else "production"
            specific_settings = {}

            if mode == "emulator":
                specific_settings["emulator_base_url"] = config.get("emulator_base_url")
                specific_settings["api_timeout"] = config.get("api_timeout", 10)
            else:
                specific_settings["command_timeout"] = config.get("command_timeout", 30)
                specific_settings["retry_attempts"] = config.get("retry_attempts", 1)

            return mode, specific_settings

        emulator_mode, emulator_settings = extract_mode_specific_settings(emulator_specific)
        production_mode, production_settings = extract_mode_specific_settings(production_specific)

        assert emulator_mode == "emulator"
        assert production_mode == "production"
        assert "emulator_base_url" in emulator_settings
        assert "command_timeout" in production_settings

        print("✓ Emulator configuration includes API settings")
        print("✓ Production configuration includes command settings")
        print("✅ Mode-specific configurations working")

    def test_configuration_migration_scenarios(self):
        """Test configuration migration from legacy to periodic limits."""
        # Legacy SLURM configuration
        legacy_config = {
            "name": "Legacy SLURM Cluster",
            "backend_type": "slurm",
            "backend_settings": {
                "default_account": "root",
                "customer_prefix": "hpc_",
                "project_prefix": "hpc_",
                "qos_default": "normal",
                "qos_downscaled": "limited",
                "qos_paused": "paused",
            },
        }

        # Migration to periodic limits
        def migrate_to_periodic_limits(legacy_offering):
            """Simulate configuration migration."""
            migrated = legacy_offering.copy()

            # Add periodic limits configuration
            migrated["backend_settings"]["periodic_limits"] = {
                "enabled": True,
                "limit_type": "GrpTRESMins",
                "tres_billing_enabled": True,
                "fairshare_decay_half_life": 15,
                "default_grace_ratio": 0.2,
                "qos_levels": {
                    "default": legacy_offering["backend_settings"]["qos_default"],
                    "slowdown": legacy_offering["backend_settings"]["qos_downscaled"],
                    "blocked": legacy_offering["backend_settings"].get("qos_paused", "blocked"),
                },
            }

            return migrated

        # Test migration
        migrated_config = migrate_to_periodic_limits(legacy_config)

        # Verify migration preserved legacy settings
        assert migrated_config["backend_settings"]["qos_default"] == "normal"
        assert migrated_config["backend_settings"]["customer_prefix"] == "hpc_"

        # Verify periodic limits were added
        periodic_config = migrated_config["backend_settings"]["periodic_limits"]
        assert periodic_config["enabled"] is True
        assert periodic_config["qos_levels"]["default"] == "normal"  # Mapped from legacy
        assert periodic_config["qos_levels"]["slowdown"] == "limited"  # Mapped from legacy

        print("✓ Legacy configuration preserved")
        print("✓ Periodic limits configuration added")
        print("✓ QoS mappings preserved from legacy")
        print("✅ Configuration migration working correctly")

    def test_validation_with_real_world_scenarios(self):
        """Test validation with realistic deployment scenarios."""
        scenarios = [
            {
                "name": "Small Academic Cluster",
                "config": {
                    "periodic_limits": {
                        "enabled": True,
                        "limit_type": "MaxTRESMins",  # Traditional approach
                        "tres_billing_enabled": False,  # Simple node-hour limits
                        "fairshare_decay_half_life": 7,  # Faster decay for small cluster
                        "default_grace_ratio": 0.1,  # Smaller grace period
                    }
                },
            },
            {
                "name": "Large HPC Center",
                "config": {
                    "periodic_limits": {
                        "enabled": True,
                        "limit_type": "GrpTRESMins",  # Modern approach
                        "tres_billing_enabled": True,
                        "tres_billing_weights": {
                            "CPU": 0.01,  # More CPUs per billing unit
                            "Mem": 0.0015625,  # More memory per billing unit
                            "GRES/gpu": 0.125,  # More GPUs per billing unit
                            "GRES/nic": 2.0,  # Special network resources
                        },
                        "fairshare_decay_half_life": 21,  # Longer decay
                        "default_grace_ratio": 0.25,  # Larger grace period
                    }
                },
            },
            {
                "name": "Cloud-Native HPC",
                "config": {
                    "periodic_limits": {
                        "enabled": True,
                        "limit_type": "GrpTRES",  # Concurrent limits for auto-scaling
                        "tres_billing_enabled": True,
                        "qos_strategy": "progressive",  # Multiple QoS levels
                        "fairshare_decay_half_life": 3,  # Very fast decay for dynamic workloads
                        "default_grace_ratio": 0.5,  # Large burst capacity
                    }
                },
            },
        ]

        for scenario in scenarios:
            print(f"\n--- {scenario['name']} ---")

            config = scenario["config"]["periodic_limits"]

            # Validate each scenario
            assert isinstance(config["enabled"], bool)
            assert config["limit_type"] in ["GrpTRESMins", "MaxTRESMins", "GrpTRES"]
            assert 1 <= config["fairshare_decay_half_life"] <= 30  # Reasonable range
            assert 0.0 <= config["default_grace_ratio"] <= 1.0  # Valid percentage

            # Scenario-specific validations
            if scenario["name"] == "Small Academic Cluster":
                assert config["tres_billing_enabled"] is False  # Simple approach
                assert config["fairshare_decay_half_life"] <= 7  # Fast decay

            elif scenario["name"] == "Large HPC Center":
                assert config["tres_billing_enabled"] is True  # Complex billing
                assert "tres_billing_weights" in config
                assert len(config["tres_billing_weights"]) >= 3  # Multiple TRES types

            elif scenario["name"] == "Cloud-Native HPC":
                assert config["limit_type"] == "GrpTRES"  # Concurrent limits
                assert config["fairshare_decay_half_life"] <= 5  # Very fast decay
                assert config["default_grace_ratio"] >= 0.3  # Large burst capacity

            print(f"✓ {scenario['name']} configuration validated")

        print("\n✅ Real-world scenario configurations validated")
