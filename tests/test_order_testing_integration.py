"""Integration tests for the complete order testing workflow."""

import json
import tempfile
from pathlib import Path

import pytest

from waldur_site_agent.testing.order_test_harness import OrderTestHarness


class TestOrderTestingIntegration:
    """Integration tests for complete order testing workflows."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()

        # Create comprehensive test config
        config_content = """
offerings:
  - name: slurm-test-offering
    waldur_api_url: https://test.waldur.com/api/
    waldur_api_token: test-token-123
    waldur_offering_uuid: d629d5e4-5567-425d-a9cd-bdc1af67b32c
    backend_type: slurm
    order_processing_backend: slurm
    membership_sync_backend: slurm
    reporting_backend: slurm
    backend_settings:
      default_account: root
      customer_prefix: hpc_
      project_prefix: hpc_
      allocation_prefix: hpc_
      check_backend_id_uniqueness: true
      homedir_umask: "0750"
    backend_components:
      cpu:
        limit: 10000
        measured_unit: k-Hours
        unit_factor: 60000
        accounting_type: limit
        label: CPU
      mem:
        limit: 20480
        measured_unit: gb-Hours
        unit_factor: 61440
        accounting_type: usage
        label: RAM
      gpu:
        limit: 8
        measured_unit: Hours
        unit_factor: 1
        accounting_type: limit
        label: GPU
      storage:
        limit: 1000
        measured_unit: GB
        unit_factor: 1
        accounting_type: limit
        label: Storage

  - name: moab-test-offering
    waldur_api_url: https://test.waldur.com/api/
    waldur_api_token: test-token-456
    waldur_offering_uuid: a1b2c3d4-5678-90ab-cdef-123456789abc
    backend_type: moab
    order_processing_backend: moab
    backend_settings:
      default_account: root
    backend_components:
      cpu:
        limit: 5000
        measured_unit: Hours
        unit_factor: 1
        accounting_type: limit
        label: CPU
"""
        self.config_path = Path(self.temp_dir) / "integration_config.yaml"
        self.config_path.write_text(config_content)

        # Create custom templates for testing
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir(parents=True)

        # Create test scenario templates
        self._create_test_templates()

        self.harness = OrderTestHarness(
            config_path=self.config_path,
            template_dir=self.template_dir,
            use_mock_backend=True,
        )

    def _create_test_templates(self) -> None:
        """Create comprehensive test templates."""
        # Lifecycle test template (CREATE)
        lifecycle_create = """
{
  "uuid": "{{ order_uuid | default('' | uuid4()) }}",
  "type": "Create",
  "resource_name": "{{ resource_name }}",
  "project_slug": "{{ project_slug }}",
  "customer_slug": "{{ customer_slug }}",
  "offering_uuid": "{{ offering_uuid }}",
  "marketplace_resource_uuid": "{{ marketplace_resource_uuid }}",
  "state": "executing",
  "limits": {
    "cpu": {{ cpu_limit }},
    "mem": {{ mem_limit }}{% if gpu_limit is defined %},
    "gpu": {{ gpu_limit }}{% endif %}
  },
  "attributes": {
    "slurm_account": "{{ slurm_account }}",
    "partition": "{{ partition | default('compute') }}"
  }
}
"""
        create_dir = self.template_dir / "lifecycle"
        create_dir.mkdir()
        (create_dir / "create.json").write_text(lifecycle_create)

        # Lifecycle test template (UPDATE)
        lifecycle_update = """
{
  "uuid": "{{ order_uuid | default('' | uuid4()) }}",
  "type": "Update",
  "resource_name": "{{ resource_name }}",
  "marketplace_resource_uuid": "{{ marketplace_resource_uuid }}",
  "state": "executing",
  "limits": {
    "cpu": {{ new_cpu_limit }},
    "mem": {{ new_mem_limit }}
  }
}
"""
        (create_dir / "update.json").write_text(lifecycle_update)

        # Lifecycle test template (TERMINATE)
        lifecycle_terminate = """
{
  "uuid": "{{ order_uuid | default('' | uuid4()) }}",
  "type": "Terminate",
  "resource_name": "{{ resource_name }}",
  "marketplace_resource_uuid": "{{ marketplace_resource_uuid }}",
  "state": "executing"
}
"""
        (create_dir / "terminate.json").write_text(lifecycle_terminate)

    def test_complete_resource_lifecycle(self) -> None:
        """Test complete CREATE -> UPDATE -> TERMINATE lifecycle."""
        # Shared variables for the lifecycle
        base_vars = {
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "marketplace_resource_uuid": "12345678-1234-5678-9012-123456789abc",
            "resource_name": "lifecycle-test-001",
            "project_slug": "test-project",
            "customer_slug": "test-customer",
            "slurm_account": "test_account",
        }

        # Step 1: CREATE
        create_result = self.harness.test_order_from_template(
            "lifecycle/create.json", cpu_limit=2000, mem_limit=4096, **base_vars
        )
        assert create_result.success
        assert create_result.order_type == "Create"
        assert create_result.resource_name == "lifecycle-test-001"

        # Step 2: UPDATE
        update_result = self.harness.test_order_from_template(
            "lifecycle/update.json", new_cpu_limit=4000, new_mem_limit=8192, **base_vars
        )
        assert update_result.success
        assert update_result.order_type == "Update"

        # Step 3: TERMINATE
        terminate_result = self.harness.test_order_from_template(
            "lifecycle/terminate.json", **base_vars
        )
        assert terminate_result.success
        assert terminate_result.order_type == "Terminate"

    def test_multiple_offerings_configuration(self) -> None:
        """Test that harness can work with multiple offerings in config."""
        # Test with first offering (SLURM)
        result1 = self.harness.test_order_from_template(
            "lifecycle/create.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",  # SLURM offering
            marketplace_resource_uuid="12345678-1234-5678-9012-123456789abc",
            resource_name="slurm-test",
            project_slug="test-project",
            customer_slug="test-customer",
            slurm_account="slurm_test",
            cpu_limit=1000,
            mem_limit=2048,
        )
        assert result1.success

        # Test with second offering (MOAB)
        result2 = self.harness.test_order_from_template(
            "lifecycle/create.json",
            offering_uuid="a1b2c3d4-5678-90ab-cdef-123456789abc",  # MOAB offering
            marketplace_resource_uuid="87654321-4321-8765-2109-987654321abc",
            resource_name="moab-test",
            project_slug="test-project",
            customer_slug="test-customer",
            slurm_account="moab_test",
            cpu_limit=500,
            mem_limit=1024,
        )
        assert result2.success

    def test_template_variable_override_precedence(self) -> None:
        """Test that template variables are properly overridden."""
        # Template with defaults
        template_with_defaults = """
{
  "type": "Create",
  "resource_name": "{{ resource_name | default('default-name') }}",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing",
  "limits": {
    "cpu": {{ cpu_limit | default(1000) }},
    "mem": {{ mem_limit | default(2048) }}
  }
}
"""
        (self.template_dir / "defaults_test.json").write_text(template_with_defaults)

        # Test with no overrides (should use defaults)
        result_defaults = self.harness.test_order_from_template(
            "defaults_test.json", offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c"
        )
        assert result_defaults.success
        assert result_defaults.resource_name == "default-name"

        # Test with overrides (should use provided values)
        result_overrides = self.harness.test_order_from_template(
            "defaults_test.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="override-name",
            cpu_limit=5000,
            mem_limit=10240,
        )
        assert result_overrides.success
        assert result_overrides.resource_name == "override-name"

    def test_json_file_order_processing(self) -> None:
        """Test processing orders from raw JSON files."""
        # Create test order files
        create_order_data = {
            "type": "Create",
            "resource_name": "json-file-test",
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "marketplace_resource_uuid": "11111111-2222-3333-4444-555555555555",
            "state": "executing",
            "limits": {"cpu": 1500, "mem": 3072},
        }

        order_file = Path(self.temp_dir) / "test_order.json"
        with order_file.open("w") as f:
            json.dump(create_order_data, f, indent=2)

        # Test processing the file
        result = self.harness.test_order_from_file(order_file)

        assert result.success
        assert result.order_type == "Create"
        assert result.resource_name == "json-file-test"

    def test_error_handling_workflow(self) -> None:
        """Test error handling in various scenarios."""
        # Test with invalid template that has syntax errors
        broken_template = """
{
  "type": "Create",
  "resource_name": "{{ broken_var
}
"""
        (self.template_dir / "broken.json").write_text(broken_template)

        result = self.harness.test_order_from_template("broken.json")
        assert not result.success
        assert len(result.errors) > 0

        # Test with valid template but invalid variables
        valid_template = """
{
  "type": "Update",
  "marketplace_resource_uuid": "{{ marketplace_resource_uuid }}",
  "resource_name": "{{ resource_name }}"
}
"""
        (self.template_dir / "valid_structure.json").write_text(valid_template)

        result_invalid_uuid = self.harness.test_order_from_template(
            "valid_structure.json",
            marketplace_resource_uuid="not-a-uuid",
            resource_name="error-test",
        )
        assert not result_invalid_uuid.success
        assert any(
            "UUID format" in error or "badly formed" in error
            for error in result_invalid_uuid.errors
        )

    def test_template_inheritance_simulation(self) -> None:
        """Test simulated template inheritance through includes."""
        # Base template with common fields
        base_template = """
{
  "uuid": "{{ order_uuid | default('' | uuid4()) }}",
  "state": "{{ state | default('executing') }}",
  "created": "{{ created | default('' | timestamp()) }}"
}
"""
        (self.template_dir / "base.json").write_text(base_template)

        # Extended template that builds on base
        extended_template = """
{
  "uuid": "{{ order_uuid | default('' | uuid4()) }}",
  "state": "{{ state | default('executing') }}",
  "created": "{{ created | default('' | timestamp()) }}",
  "type": "Create",
  "resource_name": "{{ resource_name }}",
  "offering_uuid": "{{ offering_uuid }}"
}
"""
        (self.template_dir / "extended.json").write_text(extended_template)

        # Test the extended template
        result = self.harness.test_order_from_template(
            "extended.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="inheritance-test",
        )

        assert result.success
        assert result.resource_name == "inheritance-test"

    def test_batch_order_validation(self) -> None:
        """Test validating multiple orders in batch."""
        # Create multiple order files
        orders = [
            {
                "name": "create_order.json",
                "data": {
                    "type": "Create",
                    "resource_name": "batch-create",
                    "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
                    "state": "executing",
                },
            },
            {
                "name": "update_order.json",
                "data": {
                    "type": "Update",
                    "resource_name": "batch-update",
                    "marketplace_resource_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
                    "limits": {"cpu": 2000},
                },
            },
            {
                "name": "terminate_order.json",
                "data": {
                    "type": "Terminate",
                    "resource_name": "batch-terminate",
                    "marketplace_resource_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
                },
            },
        ]

        results = []
        for order_info in orders:
            order_path = Path(self.temp_dir) / str(order_info["name"])
            with order_path.open("w") as f:
                json.dump(order_info["data"], f)

            result = self.harness.test_order_from_file(order_path)
            results.append((order_info["name"], result))

        # All should succeed
        for name, result in results:
            assert result.success, f"Order {name} should succeed"

    def test_performance_with_large_templates(self) -> None:
        """Test performance with templates containing many variables."""
        # Create template with many conditional fields
        large_template_parts = [
            '{\n  "type": "Create",',
            '  "resource_name": "{{ resource_name }}",',
            '  "offering_uuid": "{{ offering_uuid }}",',
            '  "state": "executing",',
            '  "attributes": {',
        ]

        # Add many conditional attributes
        for i in range(50):
            large_template_parts.append(
                f'    {{% if attr_{i} is defined %}}"attr_{i}": "{{{{ attr_{i} }}}}",{{% endif %}}'
            )

        large_template_parts.extend(['    "base_attr": "fixed_value"', "  }", "}"])

        large_template = "\n".join(large_template_parts)
        (self.template_dir / "large.json").write_text(large_template)

        # Test with subset of variables
        variables = {
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "performance-test",
        }

        # Add some of the conditional variables
        for i in range(0, 50, 5):  # Every 5th variable
            variables[f"attr_{i}"] = f"value_{i}"

        import time

        start_time = time.time()
        result = self.harness.test_order_from_template("large.json", **variables)
        end_time = time.time()

        assert result.success
        assert result.resource_name == "performance-test"
        # Should complete reasonably quickly (under 5 seconds)
        assert (end_time - start_time) < 5.0

    def test_concurrent_harness_usage(self) -> None:
        """Test using multiple harness instances concurrently."""
        # Create second harness with different template directory
        template_dir2 = Path(self.temp_dir) / "templates2"
        template_dir2.mkdir()

        template2_content = """
{
  "type": "Create",
  "resource_name": "concurrent-test-2",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing"
}
"""
        (template_dir2 / "concurrent.json").write_text(template2_content)

        harness2 = OrderTestHarness(
            config_path=self.config_path,
            template_dir=template_dir2,
            use_mock_backend=True,
        )

        # Use both harnesses
        result1 = self.harness.test_order_from_template(
            "lifecycle/create.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            marketplace_resource_uuid="11111111-1111-1111-1111-111111111111",
            resource_name="concurrent-1",
            project_slug="test",
            customer_slug="test",
            slurm_account="test",
            cpu_limit=1000,
            mem_limit=2048,
        )

        result2 = harness2.test_order_from_template(
            "concurrent.json", offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c"
        )

        assert result1.success
        assert result2.success
        assert result1.resource_name == "concurrent-1"
        assert result2.resource_name == "concurrent-test-2"

    def test_mock_vs_real_backend_configuration(self) -> None:
        """Test switching between mock and real backend modes."""
        # Test with mock backend
        harness_mock = OrderTestHarness(
            config_path=self.config_path,
            template_dir=self.template_dir,
            use_mock_backend=True,
        )

        result_mock = harness_mock.test_order_from_template(
            "lifecycle/create.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            marketplace_resource_uuid="22222222-2222-2222-2222-222222222222",
            resource_name="mock-backend-test",
            project_slug="test",
            customer_slug="test",
            slurm_account="test",
            cpu_limit=1000,
            mem_limit=2048,
        )
        assert result_mock.success
        assert len(result_mock.backend_operations) > 0

        # Test with real backend (should try to load actual SLURM backend)
        harness_real = OrderTestHarness(
            config_path=self.config_path,
            template_dir=self.template_dir,
            use_mock_backend=False,
        )

        # This might fail if SLURM backend isn't available, but that's expected
        result_real = harness_real.test_order_from_template(
            "lifecycle/create.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            marketplace_resource_uuid="33333333-3333-3333-3333-333333333333",
            resource_name="real-backend-test",
            project_slug="test",
            customer_slug="test",
            slurm_account="test",
            cpu_limit=1000,
            mem_limit=2048,
        )

        # Either succeeds with real backend or fails gracefully
        if not result_real.success:
            # Should have meaningful error message about backend initialization
            assert len(result_real.errors) > 0

    def test_configuration_loading_edge_cases(self) -> None:
        """Test edge cases in configuration loading."""
        # Test with malformed YAML
        bad_config_path = Path(self.temp_dir) / "bad_config.yaml"
        bad_config_path.write_text("offerings: [invalid: yaml: syntax")

        with pytest.raises(Exception):
            OrderTestHarness(
                config_path=bad_config_path,
                template_dir=self.template_dir,
                use_mock_backend=True,
            )

        # Test with missing backend configuration
        minimal_config = """
offerings:
  - name: minimal-offering
    waldur_api_url: https://test.waldur.com/api/
    waldur_api_token: test-token
    waldur_offering_uuid: d629d5e4-5567-425d-a9cd-bdc1af67b32c
    backend_type: unknown_backend_type
"""
        minimal_config_path = Path(self.temp_dir) / "minimal_config.yaml"
        minimal_config_path.write_text(minimal_config)

        # Should still create harness but may fail when trying to get backend
        harness_minimal = OrderTestHarness(
            config_path=minimal_config_path,
            template_dir=self.template_dir,
            use_mock_backend=False,  # Try to use real backend that doesn't exist
        )

        result = harness_minimal.test_order_from_template(
            "lifecycle/create.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            marketplace_resource_uuid="44444444-4444-4444-4444-444444444444",
            resource_name="minimal-test",
            project_slug="test",
            customer_slug="test",
            slurm_account="test",
            cpu_limit=1000,
            mem_limit=2048,
        )

        # Should succeed but may have warnings about unknown backend type
        # The harness falls back to mock backend when real backend fails to load
        assert result.success

    def test_template_reusability(self) -> None:
        """Test that templates can be reused with different variables."""
        # Use the same template multiple times with different variables
        test_cases = [
            {
                "resource_name": "reuse-test-1",
                "cpu_limit": 1000,
                "mem_limit": 2048,
                "partition": "compute",
            },
            {
                "resource_name": "reuse-test-2",
                "cpu_limit": 5000,
                "mem_limit": 8192,
                "partition": "gpu",
            },
            {
                "resource_name": "reuse-test-3",
                "cpu_limit": 500,
                "mem_limit": 1024,
                "partition": "debug",
            },
        ]

        base_vars = {
            "marketplace_resource_uuid": "55555555-5555-5555-5555-555555555555",
            "project_slug": "reuse-project",
            "customer_slug": "reuse-customer",
            "slurm_account": "reuse_account",
        }

        offering_uuid = "d629d5e4-5567-425d-a9cd-bdc1af67b32c"

        for i, test_case in enumerate(test_cases):
            variables = {**base_vars, **test_case}
            variables["marketplace_resource_uuid"] = f"55555555-5555-5555-5555-55555555555{i}"

            result = self.harness.test_order_from_template(
                "lifecycle/create.json", offering_uuid, **variables
            )

            assert result.success
            assert result.resource_name == test_case["resource_name"]
