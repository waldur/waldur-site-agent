"""Tests for template validation and edge cases."""

import json
import tempfile
from pathlib import Path

import pytest

from waldur_site_agent.testing.template_engine import (
    OrderTemplateEngine,
    TemplateRenderError,
    TemplateValidationError,
    ValidationResult,
)
from waldur_site_agent.testing.mock_backend import MockBackend


class TestTemplateValidation:
    """Tests for comprehensive template validation scenarios."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir()
        self.engine = OrderTemplateEngine(self.template_dir)

    def test_invalid_uuid_validation(self) -> None:
        """Test validation catches invalid UUID formats."""
        template_content = """
{
  "type": "Create",
  "offering_uuid": "{{ offering_uuid }}",
  "marketplace_resource_uuid": "invalid-uuid-format",
  "resource_name": "test"
}
"""
        template_path = self.template_dir / "invalid_uuid.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template("invalid_uuid.json", offering_uuid="also-invalid")

        assert not result.is_valid
        assert any(
            "Invalid UUID format for field 'offering_uuid': also-invalid" in error
            for error in result.errors
        )
        assert any(
            "Invalid UUID format for field 'marketplace_resource_uuid': invalid-uuid-format"
            in error
            for error in result.errors
        )

    def test_missing_required_fields_create(self) -> None:
        """Test validation catches missing required fields for CREATE orders."""
        template_content = """
{
  "type": "Create"
}
"""
        template_path = self.template_dir / "missing_fields.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template("missing_fields.json")

        assert not result.is_valid
        assert any("Offering UUID required for CREATE order" in error for error in result.errors)

    def test_missing_required_fields_update(self) -> None:
        """Test validation catches missing required fields for UPDATE orders."""
        template_content = """
{
  "type": "Update",
  "resource_name": "test"
}
"""
        template_path = self.template_dir / "update_missing.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template("update_missing.json")

        assert not result.is_valid
        assert any("Marketplace resource UUID required" in error for error in result.errors)

    def test_missing_required_fields_terminate(self) -> None:
        """Test validation catches missing required fields for TERMINATE orders."""
        template_content = """
{
  "type": "Terminate",
  "resource_name": "test"
}
"""
        template_path = self.template_dir / "terminate_missing.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template("terminate_missing.json")

        assert not result.is_valid
        assert any("Marketplace resource UUID required" in error for error in result.errors)

    def test_valid_complex_template(self) -> None:
        """Test validation of complex template with all fields."""
        template_content = """
{
  "uuid": "{{ '' | uuid4() }}",
  "type": "Create",
  "resource_name": "{{ resource_name | default('complex-test') }}",
  "project_slug": "{{ project_slug | default('test-project') }}",
  "customer_slug": "{{ customer_slug | default('test-customer') }}",
  "offering_uuid": "{{ offering_uuid }}",
  "marketplace_resource_uuid": "{{ '' | uuid4() }}",
  "project_uuid": "{{ '' | uuid4() }}",
  "customer_uuid": "{{ '' | uuid4() }}",
  "state": "executing",
  "created": "{{ '' | timestamp() }}",
  "limits": {
    "cpu": {{ cpu_limit | default(1000) }},
    "mem": {{ mem_limit | default(2048) }}
  },
  "attributes": {
    "partition": "{{ partition | default('compute') }}",
    "qos": "{{ qos | default('normal') }}"
  }
}
"""
        template_path = self.template_dir / "complex.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template(
            "complex.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            cpu_limit=2000,
            partition="gpu",
        )

        assert result.is_valid
        assert len(result.errors) == 0

    def test_template_with_conditional_fields(self) -> None:
        """Test template with conditional fields using Jinja2 conditionals."""
        template_content = """
{
  "type": "Update",
  "marketplace_resource_uuid": "{{ marketplace_resource_uuid }}",
  "resource_name": "{{ resource_name }}",
  "limits": {
    {%- set items = [] -%}
    {%- if cpu_limit is defined -%}
      {%- set _ = items.append('"cpu": ' ~ cpu_limit) -%}
    {%- endif -%}
    {%- if mem_limit is defined -%}
      {%- set _ = items.append('"mem": ' ~ mem_limit) -%}
    {%- endif -%}
    {{ items | join(', ') }}
  }
}
"""
        template_path = self.template_dir / "conditional.json"
        template_path.write_text(template_content)

        # Test with CPU limit only
        result_cpu = self.engine.validate_template(
            "conditional.json",
            marketplace_resource_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="test",
            cpu_limit=2000,
        )
        assert result_cpu.is_valid

        # Test with both limits
        result_both = self.engine.validate_template(
            "conditional.json",
            marketplace_resource_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="test",
            cpu_limit=2000,
            mem_limit=4096,
        )
        assert result_both.is_valid

    def test_template_json_syntax_errors(self) -> None:
        """Test various JSON syntax errors in templates."""
        # Missing comma
        bad_template1 = """
{
  "type": "Create"
  "resource_name": "test"
}
"""
        template_path1 = self.template_dir / "bad1.json"
        template_path1.write_text(bad_template1)

        result1 = self.engine.validate_template("bad1.json")
        assert not result1.is_valid
        assert any("invalid JSON" in error for error in result1.errors)

        # Trailing comma
        bad_template2 = """
{
  "type": "Create",
  "resource_name": "test",
}
"""
        template_path2 = self.template_dir / "bad2.json"
        template_path2.write_text(bad_template2)

        result2 = self.engine.validate_template("bad2.json")
        assert not result2.is_valid
        assert any("invalid JSON" in error for error in result2.errors)

    def test_template_jinja_syntax_errors(self) -> None:
        """Test Jinja2 syntax errors in templates."""
        # Unclosed variable
        bad_jinja = """
{
  "type": "Create",
  "resource_name": "{{ unclosed_var"
}
"""
        template_path = self.template_dir / "bad_jinja.json"
        template_path.write_text(bad_jinja)

        result = self.engine.validate_template("bad_jinja.json")
        assert not result.is_valid
        assert any("Failed to load template" in error for error in result.errors)

    def test_template_variable_extraction(self) -> None:
        """Test extraction of template variables."""
        template_content = """
{
  "type": "Create",
  "resource_name": "{{ resource_name }}",
  "offering_uuid": "{{ offering_uuid }}",
  "limits": {
    "cpu": {{ cpu_limit | default(1000) }},
    "mem": {{ mem_limit }}
  }
}
"""
        template_path = self.template_dir / "extract_vars.json"
        template_path.write_text(template_content)

        variables = self.engine.get_template_variables("extract_vars.json")

        # Should find the variables used in the template
        assert "resource_name" in variables
        assert "offering_uuid" in variables
        assert "cpu_limit" in variables  # Even with default
        assert "mem_limit" in variables

    def test_order_type_case_sensitivity(self) -> None:
        """Test that order types are case-sensitive."""
        # Wrong case
        wrong_case_template = """
{
  "type": "create",
  "resource_name": "test",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "executing"
}
"""
        template_path = self.template_dir / "wrong_case.json"
        template_path.write_text(wrong_case_template)

        result = self.engine.validate_template("wrong_case.json")
        # Should fail because "create" is not a valid RequestType (should be "Create")
        assert not result.is_valid

    def test_state_validation(self) -> None:
        """Test validation of order states."""
        # Invalid state
        invalid_state_template = """
{
  "type": "Create",
  "resource_name": "test",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "INVALID_STATE"
}
"""
        template_path = self.template_dir / "invalid_state.json"
        template_path.write_text(invalid_state_template)

        result = self.engine.validate_template("invalid_state.json")
        assert not result.is_valid
        assert any("OrderDetails" in error for error in result.errors)


class TestMockBackendEdgeCases:
    """Tests for edge cases in MockBackend."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.backend = MockBackend()

    def test_operations_on_nonexistent_resource(self) -> None:
        """Test operations on resources that don't exist."""
        from waldur_api_client.models.resource import Resource
        import uuid

        nonexistent_resource = Resource(
            uuid=uuid.uuid4(),
            name="nonexistent",
            project_uuid=uuid.uuid4(),
            backend_id="nonexistent-id",
        )

        # These should not crash even if resource doesn't exist
        self.backend.set_resource_limits("nonexistent-id", {"cpu": 1000})
        info = self.backend.pull_resource(nonexistent_resource)
        assert info is None

        limits = self.backend.get_resource_limits("nonexistent-id")
        assert limits == {}

        metadata = self.backend.get_resource_metadata("nonexistent-id")
        assert metadata == {}

    def test_duplicate_user_operations(self) -> None:
        """Test adding/removing the same user multiple times."""
        from waldur_api_client.models.resource import Resource
        import uuid

        # Create a resource first
        resource = Resource(
            uuid=uuid.uuid4(),
            name="user-test",
            project_uuid=uuid.uuid4(),
            backend_id="user-test-id",
        )

        self.backend.create_resource_with_id(resource, "user-test-id", {"team": []})

        # Add user multiple times
        self.backend.add_user(resource, "testuser")
        self.backend.add_user(resource, "testuser")  # Duplicate add

        info = self.backend.pull_resource(resource)
        assert info is not None
        # Should only have one instance of the user
        assert info.users.count("testuser") == 1

        # Remove user multiple times
        self.backend.remove_user(resource, "testuser")
        self.backend.remove_user(resource, "testuser")  # Duplicate remove

        info_after_remove = self.backend.pull_resource(resource)
        assert info_after_remove is not None
        assert "testuser" not in info_after_remove.users

    def test_bulk_user_operations(self) -> None:
        """Test bulk user add/remove operations."""
        from waldur_api_client.models.resource import Resource
        import uuid

        resource = Resource(
            uuid=uuid.uuid4(),
            name="bulk-test",
            project_uuid=uuid.uuid4(),
            backend_id="bulk-test-id",
        )

        self.backend.create_resource_with_id(resource, "bulk-test-id", {"team": []})

        # Add multiple users
        users_to_add = {"user1", "user2", "user3", "user4"}
        added_users = self.backend.add_users_to_resource(resource, users_to_add)
        assert added_users == users_to_add

        info = self.backend.pull_resource(resource)
        assert info is not None
        assert set(info.users) == users_to_add

        # Remove some users
        users_to_remove = {"user1", "user3"}
        self.backend.remove_users_from_resource(resource, users_to_remove)

        info_after_remove = self.backend.pull_resource(resource)
        assert info_after_remove is not None
        assert set(info_after_remove.users) == {"user2", "user4"}

    def test_backend_status_operations(self) -> None:
        """Test resource status operations (pause, downscale, restore)."""
        # These should all return True for mock backend
        assert self.backend.pause_resource("test-id") == True
        assert self.backend.downscale_resource("test-id") == True
        assert self.backend.restore_resource("test-id") == True

        # Verify operations were logged
        operations = self.backend.get_operations_log()
        assert len(operations) == 3
        assert operations[0]["operation"] == "pause_resource"
        assert operations[1]["operation"] == "downscale_resource"
        assert operations[2]["operation"] == "restore_resource"

    def test_backend_diagnostics(self) -> None:
        """Test backend diagnostics functionality."""
        # Diagnostics should return True for mock backend
        result = self.backend.diagnostics()
        assert result is True

    def test_list_components(self) -> None:
        """Test component listing functionality."""
        components = self.backend.list_components()
        assert len(components) == 2
        assert "cpu" in components
        assert "mem" in components


class TestOrderValidationEdgeCases:
    """Tests for edge cases in order validation."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir()

        # Create test config
        config_content = """
offerings:
  - name: test-offering
    waldur_api_url: https://test.waldur.com/api/
    waldur_api_token: test-token
    waldur_offering_uuid: d629d5e4-5567-425d-a9cd-bdc1af67b32c
    backend_type: slurm
    order_processing_backend: slurm
    backend_settings:
      default_account: root
    backend_components:
      cpu:
        limit: 1000
        measured_unit: Hours
        unit_factor: 1
        accounting_type: limit
        label: CPU
"""
        self.config_path = Path(self.temp_dir) / "config.yaml"
        self.config_path.write_text(config_content)

        from waldur_site_agent.testing.order_test_harness import OrderTestHarness

        self.harness = OrderTestHarness(
            config_path=self.config_path,
            template_dir=self.template_dir,
            use_mock_backend=True,
        )

    def test_order_with_empty_fields(self) -> None:
        """Test order validation with empty/null fields."""
        order_data = {
            "type": "Create",
            "resource_name": "",  # Empty string
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "state": "executing",
        }

        result = self.harness.validate_order_structure(order_data)

        # Should have warnings about empty fields
        assert len(result.warnings) > 0

    def test_order_with_extra_fields(self) -> None:
        """Test order validation with extra unexpected fields."""
        order_data = {
            "type": "Create",
            "resource_name": "test",
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "state": "executing",
            "extra_field": "unexpected",
            "another_extra": {"nested": "data"},
        }

        result = self.harness.validate_order_structure(order_data)

        # Should still be valid (OrderDetails allows additional properties)
        assert result.success

    def test_order_limits_validation(self) -> None:
        """Test validation of order limits."""
        # Order with limits
        order_with_limits = {
            "type": "Update",
            "marketplace_resource_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "test",
            "limits": {
                "cpu": 2000,
                "mem": 4096,
                "invalid_component": -100,  # Negative limit
            },
        }

        result = self.harness.validate_order_structure(order_with_limits)
        assert result.success  # Structure is valid even with negative values

    def test_order_with_complex_attributes(self) -> None:
        """Test orders with complex nested attributes."""
        order_data = {
            "type": "Create",
            "resource_name": "complex-test",
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "state": "executing",
            "attributes": {
                "slurm_config": {
                    "partition": "gpu",
                    "qos": "high",
                    "tres_weights": ["CPU=1.0", "Mem=0.25G"],
                },
                "metadata": {"created_by": "test_system", "tags": ["testing", "development"]},
            },
        }

        result = self.harness.validate_order_structure(order_data)
        assert result.success

    def test_file_not_found_handling(self) -> None:
        """Test handling of missing order files."""
        result = self.harness.test_order_from_file("nonexistent_file.json")

        assert not result.success
        assert any("not found" in error for error in result.errors)

    def test_malformed_json_file(self) -> None:
        """Test handling of malformed JSON files."""
        # Create malformed JSON file
        bad_json_path = Path(self.temp_dir) / "bad.json"
        bad_json_path.write_text('{"type": "Create", "invalid": json}')

        result = self.harness.test_order_from_file(bad_json_path)

        assert not result.success
        assert any("Failed to load order file" in error for error in result.errors)

    def test_no_offerings_in_config(self) -> None:
        """Test behavior when config has no offerings."""
        # Create config with empty offerings
        empty_config = """
offerings: []
"""
        empty_config_path = Path(self.temp_dir) / "empty_config.yaml"
        empty_config_path.write_text(empty_config)

        from waldur_site_agent.testing.order_test_harness import OrderTestHarness

        harness_empty = OrderTestHarness(
            config_path=empty_config_path,
            template_dir=self.template_dir,
            use_mock_backend=True,
        )

        result = harness_empty.test_order_from_template("test.json")

        assert not result.success
        assert any("No offerings found" in error for error in result.errors)

    def test_invalid_offering_uuid_in_config(self) -> None:
        """Test behavior with specific offering UUID that doesn't exist in config."""
        result = self.harness.test_order_from_template(
            "test.json",
            offering_uuid="00000000-0000-0000-0000-000000000000",  # UUID not in config
        )

        assert not result.success
        assert any("not found in configuration" in error for error in result.errors)
