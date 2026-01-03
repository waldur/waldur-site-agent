"""Tests for the order testing framework."""

import json
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pytest
from waldur_api_client.models.order_details import OrderDetails

from waldur_site_agent.testing.mock_backend import MockBackend
from waldur_site_agent.testing.order_test_harness import OrderTestHarness
from waldur_site_agent.testing.template_engine import (
    OrderTemplateEngine,
    TemplateNotFound,
    TemplateRenderError,
)


class TestOrderTemplateEngine:
    """Tests for the OrderTemplateEngine class."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir()
        self.engine = OrderTemplateEngine(self.template_dir)

    def test_render_basic_template(self) -> None:
        """Test rendering a basic template."""
        # Create a simple template
        template_content = """
        {
          "uuid": "{{ '' | uuid4() }}",
          "type": "Create",
          "resource_name": "{{ resource_name }}",
          "offering_uuid": "{{ offering_uuid | default('d629d5e4-5567-425d-a9cd-bdc1af67b32c') }}",
          "state": "executing"
        }
        """
        template_path = self.template_dir / "test.json"
        template_path.write_text(template_content)

        # Render template
        order = self.engine.render_template("test.json", resource_name="test-resource")

        # Verify result
        assert order.type_ == "Create"
        assert order.resource_name == "test-resource"
        assert order.uuid is not None

    def test_render_with_defaults(self) -> None:
        """Test template rendering with default values."""
        template_content = """
        {
          "type": "Create",
          "resource_name": "{{ resource_name | default('default-resource') }}",
          "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
          "state": "executing"
        }
        """
        template_path = self.template_dir / "defaults.json"
        template_path.write_text(template_content)

        # Render without providing resource_name
        order = self.engine.render_template("defaults.json")

        assert order.resource_name == "default-resource"

    def test_custom_filters(self) -> None:
        """Test custom Jinja2 filters."""
        template_content = """
        {
          "uuid": "{{ '' | uuid4() }}",
          "created": "{{ '' | timestamp() }}",
          "attributes": {{ '{"key": "value"}' | from_json | to_json }}
        }
        """
        template_path = self.template_dir / "filters.json"
        template_path.write_text(template_content)

        order_dict = self.engine.render_template_to_dict("filters.json")

        # UUID should be generated
        assert isinstance(order_dict["uuid"], str)
        assert len(order_dict["uuid"]) == 36  # UUID format

        # Timestamp should be generated
        assert isinstance(order_dict["created"], str)

        # JSON parsing should work
        assert order_dict["attributes"] == {"key": "value"}

    def test_template_not_found(self) -> None:
        """Test handling of missing templates."""
        with pytest.raises(TemplateNotFound):
            self.engine.render_template("nonexistent.json")

    def test_invalid_json_template(self) -> None:
        """Test handling of templates that produce invalid JSON."""
        template_content = """
        {
          "invalid": {{ unclosed_dict
        }
        """
        template_path = self.template_dir / "invalid.json"
        template_path.write_text(template_content)

        with pytest.raises(TemplateRenderError):
            self.engine.render_template("invalid.json")

    def test_list_templates(self) -> None:
        """Test listing available templates."""
        # Create some templates
        (self.template_dir / "template1.json").write_text("{}")
        (self.template_dir / "subdir").mkdir()
        (self.template_dir / "subdir" / "template2.json").write_text("{}")

        templates = self.engine.list_templates()

        assert "template1.json" in templates
        assert "subdir/template2.json" in templates
        assert len(templates) == 2

    def test_validate_template_success(self) -> None:
        """Test successful template validation."""
        template_content = """
        {
          "type": "Create",
          "resource_name": "{{ resource_name }}",
          "marketplace_resource_uuid": "{{ '' | uuid4() }}",
          "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
          "state": "executing"
        }
        """
        template_path = self.template_dir / "valid.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template("valid.json", resource_name="test")

        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_template_errors(self) -> None:
        """Test template validation with errors."""
        template_content = """
        {
          "type": "Update"
        }
        """
        template_path = self.template_dir / "invalid.json"
        template_path.write_text(template_content)

        result = self.engine.validate_template("invalid.json")

        assert not result.is_valid
        assert any("Marketplace resource UUID required" in error for error in result.errors)


class TestMockBackend:
    """Tests for the MockBackend class."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.backend = MockBackend()

    def test_create_resource(self) -> None:
        """Test resource creation in mock backend."""
        from waldur_api_client.models.resource import Resource

        # Create test resource
        waldur_resource = Resource(
            uuid=uuid.uuid4(),
            name="test-resource",
            project_uuid=uuid.uuid4(),
        )

        user_context: dict[str, Any] = {"team": []}
        backend_id = "test-backend-id"

        result = self.backend.create_resource_with_id(waldur_resource, backend_id, user_context)

        assert result.backend_id == backend_id
        assert "create_resource" in str(self.backend.get_operations_log())

    def test_resource_lifecycle(self) -> None:
        """Test complete resource lifecycle in mock backend."""
        from waldur_api_client.models.resource import Resource

        waldur_resource = Resource(
            uuid=uuid.uuid4(),
            name="lifecycle-test",
            project_uuid=uuid.uuid4(),
            backend_id="lifecycle-test-id",
        )

        # Create resource
        result = self.backend.create_resource_with_id(
            waldur_resource, "lifecycle-test-id", {"team": []}
        )
        assert result.backend_id == "lifecycle-test-id"

        # Update limits
        self.backend.set_resource_limits("lifecycle-test-id", {"cpu": 2000, "mem": 4096})

        # Add users
        added = self.backend.add_users_to_resource(waldur_resource, {"user1", "user2"})
        assert added == {"user1", "user2"}

        # Remove users
        self.backend.remove_users_from_resource(waldur_resource, {"user1"})

        # Get resource info
        info = self.backend.pull_resource(waldur_resource)
        assert info is not None
        assert info.backend_id == "lifecycle-test-id"
        assert "user2" in info.users
        assert "user1" not in info.users

        # Delete resource
        self.backend.delete_resource(waldur_resource)

        # Verify deletion
        info_after_delete = self.backend.pull_resource(waldur_resource)
        assert info_after_delete is None

    def test_operations_log(self) -> None:
        """Test that operations are properly logged."""
        from waldur_api_client.models.resource import Resource

        waldur_resource = Resource(
            uuid=uuid.uuid4(),
            name="log-test",
            project_uuid=uuid.uuid4(),
            backend_id="log-test-id",
        )

        # Perform some operations
        self.backend.create_resource_with_id(waldur_resource, "log-test-id", {"team": []})
        self.backend.set_resource_limits("log-test-id", {"cpu": 1000})
        self.backend.ping()

        operations = self.backend.get_operations_log()

        assert len(operations) == 3
        assert operations[0]["operation"] == "create_resource"
        assert operations[1]["operation"] == "set_resource_limits"
        assert operations[2]["operation"] == "ping"

        # Test log clearing
        self.backend.clear_operations_log()
        assert len(self.backend.get_operations_log()) == 0


class TestOrderTestHarness:
    """Tests for the OrderTestHarness class."""

    def setup_method(self) -> None:
        """Set up test environment."""
        # Create temporary config
        self.temp_dir = tempfile.mkdtemp()
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

        # Create template directory
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir()

        # Create a basic template
        template_content = """
{
  "type": "Create",
  "resource_name": "{{ resource_name | default('test-resource') }}",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing"
}
"""
        (self.template_dir / "test.json").write_text(template_content)

        self.harness = OrderTestHarness(
            config_path=self.config_path,
            template_dir=self.template_dir,
            use_mock_backend=True,
        )

    def test_test_from_template(self) -> None:
        """Test order execution from template."""
        result = self.harness.test_order_from_template(
            "test.json",
            resource_name="template-test",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
        )

        assert result.success
        assert result.order_type == "Create"
        assert result.resource_name == "template-test"

    def test_test_from_file(self) -> None:
        """Test order execution from JSON file."""
        # Create test order file
        order_data = {
            "type": "Create",
            "resource_name": "file-test",
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "state": "executing",
        }
        order_path = Path(self.temp_dir) / "order.json"
        with order_path.open("w") as f:
            json.dump(order_data, f)

        result = self.harness.test_order_from_file(order_path)

        assert result.success
        assert result.order_type == "Create"
        assert result.resource_name == "file-test"

    def test_validate_order_structure(self) -> None:
        """Test order structure validation."""
        valid_order = {
            "type": "Create",
            "resource_name": "validation-test",
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "state": "executing",
        }

        result = self.harness.validate_order_structure(valid_order)

        assert result.success
        assert result.order_type == "Create"

    def test_validate_order_structure_invalid(self) -> None:
        """Test validation of invalid order structure."""
        invalid_order = {
            "type": "Update",
            # Missing required marketplace_resource_uuid
        }

        result = self.harness.validate_order_structure(invalid_order)

        assert not result.success
        assert "Marketplace resource UUID required" in str(result.errors)

    def test_list_templates(self) -> None:
        """Test template listing."""
        templates = self.harness.list_templates()

        assert "test.json" in templates

    def test_validate_template(self) -> None:
        """Test template validation."""
        result = self.harness.validate_template(
            "test.json", offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c"
        )

        assert result.success

    def test_backend_operations_logging(self) -> None:
        """Test that mock backend logs operations correctly."""
        result = self.harness.test_order_from_template(
            "test.json",
            resource_name="ops-test",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
        )

        assert result.success
        # Should have at least one ping operation logged
        assert len(result.backend_operations) >= 1
        assert any(op["operation"] == "ping" for op in result.backend_operations)

    def test_multiple_template_validation(self) -> None:
        """Test validation of different template types."""
        # CREATE template
        create_template = """
{
  "type": "Create",
  "resource_name": "{{ resource_name }}",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing"
}
"""
        (self.template_dir / "create_test.json").write_text(create_template)

        # UPDATE template
        update_template = """
{
  "type": "Update",
  "marketplace_resource_uuid": "{{ marketplace_resource_uuid }}",
  "resource_name": "{{ resource_name }}"
}
"""
        (self.template_dir / "update_test.json").write_text(update_template)

        # Test CREATE validation
        create_result = self.harness.validate_template(
            "create_test.json",
            resource_name="test",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
        )
        assert create_result.success

        # Test UPDATE validation
        update_result = self.harness.validate_template(
            "update_test.json",
            marketplace_resource_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="test",
        )
        assert update_result.success

    def test_invalid_template_variables(self) -> None:
        """Test template with missing required variables."""
        missing_vars_template = """
{
  "type": "Create",
  "resource_name": "{{ required_var }}",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "executing"
}
"""
        (self.template_dir / "missing_vars.json").write_text(missing_vars_template)

        # Should fail validation due to missing required_var
        result = self.harness.validate_template("missing_vars.json")
        assert not result.success
