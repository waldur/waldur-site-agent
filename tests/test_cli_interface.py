"""Tests for the CLI interface of the order testing system."""

import json
import subprocess
import tempfile
from pathlib import Path

import pytest

from waldur_site_agent.testing.template_engine import OrderTemplateEngine


class TestCLIInterface:
    """Tests for the command-line interface."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()

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

        # Create test templates
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir()

        basic_template = """
{
  "type": "Create",
  "resource_name": "{{ resource_name | default('cli-test') }}",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing"
}
"""
        (self.template_dir / "cli_test.json").write_text(basic_template)

    def _run_cli(self, args: list[str]) -> tuple[int, str, str]:
        """Run CLI command and return exit code, stdout, stderr."""
        cmd = ["uv", "run", "waldur_site_test_order"] + args

        result = subprocess.run(cmd, capture_output=True, text=True)

        return result.returncode, result.stdout, result.stderr

    def test_list_templates_command(self) -> None:
        """Test --list-templates command."""
        exit_code, stdout, stderr = self._run_cli(["--list-templates"])

        assert exit_code == 0
        assert "Available templates:" in stdout
        # Should show built-in templates
        assert "create/basic.json" in stdout
        assert "update/limits-only.json" in stdout

    def test_list_templates_custom_dir(self) -> None:
        """Test --list-templates with custom template directory."""
        exit_code, stdout, stderr = self._run_cli(
            ["--list-templates", "--template-dir", str(self.template_dir)]
        )

        assert exit_code == 0
        assert "cli_test.json" in stdout

    def test_validate_template_success(self) -> None:
        """Test successful template validation via CLI."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--validate-only",
                "--var",
                "offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            ]
        )

        assert exit_code == 0
        assert "Test Result: SUCCESS" in stdout

    def test_validate_template_failure(self) -> None:
        """Test template validation failure via CLI."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--validate-only",
                "--var",
                "offering_uuid=invalid-uuid",
            ]
        )

        assert exit_code == 1
        assert "Test Result: FAILED" in stdout
        assert "Invalid UUID format" in stdout

    def test_generate_only_mode(self) -> None:
        """Test --generate-only mode."""
        output_file = Path(self.temp_dir) / "generated.json"

        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--generate-only",
                "--var",
                "offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c",
                "--var",
                "resource_name=cli-generated",
                "--output",
                str(output_file),
            ]
        )

        assert exit_code == 0
        assert output_file.exists()

        # Verify generated content
        with output_file.open() as f:
            generated_data = json.load(f)

        assert generated_data["type"] == "Create"
        assert generated_data["resource_name"] == "cli-generated"
        assert generated_data["offering_uuid"] == "d629d5e4-5567-425d-a9cd-bdc1af67b32c"

    def test_generate_only_stdout(self) -> None:
        """Test --generate-only mode with stdout output."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--generate-only",
                "--var",
                "offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            ]
        )

        assert exit_code == 0

        # Should output valid JSON
        generated_data = json.loads(stdout)
        assert generated_data["type"] == "Create"
        assert generated_data["offering_uuid"] == "d629d5e4-5567-425d-a9cd-bdc1af67b32c"

    def test_dry_run_with_config(self) -> None:
        """Test --dry-run mode with configuration file."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--config",
                str(self.config_path),
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--dry-run",
            ]
        )

        assert exit_code == 0
        assert "Test Result: SUCCESS" in stdout

    def test_missing_config_file(self) -> None:
        """Test behavior when config file is missing."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--config",
                "nonexistent.yaml",
                "--template",
                "cli_test.json",
                "--dry-run",
            ]
        )

        assert exit_code == 1
        assert "Configuration file not found" in stderr or "Configuration file not found" in stdout

    def test_missing_template_file(self) -> None:
        """Test behavior when template file is missing."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "nonexistent.json",
                "--validate-only",
            ]
        )

        assert exit_code == 1
        assert "not found" in stdout or "does not exist" in stdout

    def test_invalid_variable_format(self) -> None:
        """Test behavior with invalid variable format."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--validate-only",
                "--var",
                "invalid_format_no_equals",
            ]
        )

        assert exit_code == 1
        assert "Invalid variable format" in stderr or "Invalid variable format" in stdout

    def test_json_output_mode(self) -> None:
        """Test --json-output mode."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--validate-only",
                "--var",
                "offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c",
                "--json-output",
            ]
        )

        assert exit_code == 0

        # Should output valid JSON
        result_data = json.loads(stdout)
        assert result_data["success"] == True
        assert "order_uuid" in result_data

    def test_verbose_mode(self) -> None:
        """Test --verbose mode."""
        exit_code, stdout, stderr = self._run_cli(
            [
                "--config",
                str(self.config_path),
                "--template",
                "cli_test.json",
                "--template-dir",
                str(self.template_dir),
                "--dry-run",
                "--verbose",
            ]
        )

        assert exit_code == 0
        assert "Backend Operations:" in stdout

    def test_no_arguments(self) -> None:
        """Test CLI behavior with no arguments."""
        exit_code, stdout, stderr = self._run_cli([])

        assert exit_code == 1
        assert (
            "Configuration file is required" in stderr
            or "Must specify either --template or --order-file" in stderr
        )

    def test_conflicting_arguments(self) -> None:
        """Test CLI behavior with conflicting arguments."""
        # Create a test order file
        order_file = Path(self.temp_dir) / "test_order.json"
        order_file.write_text('{"type": "Create", "resource_name": "test"}')

        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "cli_test.json",
                "--order-file",
                str(order_file),  # Conflicting arguments
                "--validate-only",
            ]
        )

        assert exit_code == 2  # argparse error for mutually exclusive arguments

    def test_complex_variable_parsing(self) -> None:
        """Test parsing of complex variable values."""
        # Template that uses JSON variables
        json_template = """
{
  "type": "Create",
  "resource_name": "json-test",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing",
  "attributes": {{ attributes | to_json }}
}
"""
        template_path = self.template_dir / "json_vars.json"
        template_path.write_text(json_template)

        exit_code, stdout, stderr = self._run_cli(
            [
                "--template",
                "json_vars.json",
                "--template-dir",
                str(self.template_dir),
                "--generate-only",
                "--var",
                "offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c",
                "--var",
                'attributes={"partition": "gpu", "qos": "high"}',
            ]
        )

        assert exit_code == 0

        # Verify JSON parsing worked
        generated_data = json.loads(stdout)
        assert generated_data["attributes"]["partition"] == "gpu"
        assert generated_data["attributes"]["qos"] == "high"


class TestTemplateEngineErrorHandling:
    """Tests for error handling in the template engine."""

    def setup_method(self) -> None:
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.template_dir = Path(self.temp_dir) / "templates"
        self.template_dir.mkdir()
        from waldur_site_agent.testing.template_engine import OrderTemplateEngine

        self.engine = OrderTemplateEngine(self.template_dir)

    def test_template_with_undefined_variables(self) -> None:
        """Test behavior when template uses undefined variables."""
        template_content = """
{
  "type": "Create",
  "resource_name": "{{ undefined_variable }}",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "executing"
}
"""
        template_path = self.template_dir / "undefined.json"
        template_path.write_text(template_content)

        # Should raise error when undefined variable is used
        with pytest.raises(Exception):
            self.engine.render_template("undefined.json")

    def test_template_with_filter_errors(self) -> None:
        """Test behavior when template filters fail."""
        template_content = """
{
  "type": "Create",
  "resource_name": "test",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "executing",
  "invalid_json": {{ "malformed json" | from_json }}
}
"""
        template_path = self.template_dir / "filter_error.json"
        template_path.write_text(template_content)

        # Should raise error when filter fails
        with pytest.raises(Exception):
            self.engine.render_template("filter_error.json")

    def test_empty_template_directory(self) -> None:
        """Test behavior with empty template directory."""
        empty_dir = Path(self.temp_dir) / "empty"
        empty_dir.mkdir()

        empty_engine = OrderTemplateEngine(empty_dir)
        templates = empty_engine.list_templates()

        assert len(templates) == 0

    def test_template_directory_does_not_exist(self) -> None:
        """Test behavior when template directory doesn't exist."""
        nonexistent_dir = Path(self.temp_dir) / "nonexistent"

        # Should create the directory
        engine = OrderTemplateEngine(nonexistent_dir)
        assert nonexistent_dir.exists()
        assert nonexistent_dir.is_dir()

    def test_nested_template_structure(self) -> None:
        """Test templates in nested directory structure."""
        # Create nested structure
        nested_dir = self.template_dir / "scenarios" / "complex"
        nested_dir.mkdir(parents=True)

        nested_template = """
{
  "type": "Create",
  "resource_name": "nested-test",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "executing"
}
"""
        (nested_dir / "nested.json").write_text(nested_template)

        templates = self.engine.list_templates()
        assert "scenarios/complex/nested.json" in templates

        # Should be able to render nested template
        order = self.engine.render_template("scenarios/complex/nested.json")
        assert order.resource_name == "nested-test"

    def test_large_template_variables(self) -> None:
        """Test template with many variables."""
        template_content = """
{
  "type": "Create",
  "resource_name": "{{ resource_name }}",
  "offering_uuid": "{{ offering_uuid }}",
  "state": "executing",
  "attributes": {
    {% for i in range(10) %}
    "attr_{{ i }}": "{{ vars['attr_' + i|string] | default('default_' + i|string) }}"{% if not loop.last %},{% endif %}
    {% endfor %}
  }
}
"""
        template_path = self.template_dir / "many_vars.json"
        template_path.write_text(template_content)

        # Provide variables
        variables = {
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "many-vars-test",
            "vars": {f"attr_{i}": f"value_{i}" for i in range(5)},  # Only provide half
        }

        order = self.engine.render_template("many_vars.json", **variables)
        assert order.resource_name == "many-vars-test"

    def test_template_with_comments(self) -> None:
        """Test that JSON comments are not supported (as expected)."""
        template_with_comments = """
{
  // This is a comment - should cause JSON parsing to fail
  "type": "Create",
  "resource_name": "test",
  "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
  "state": "executing"
}
"""
        template_path = self.template_dir / "comments.json"
        template_path.write_text(template_with_comments)

        result = self.engine.validate_template("comments.json")
        assert not result.is_valid
        assert any("invalid JSON" in error for error in result.errors)


class TestBuiltInTemplates:
    """Tests for the built-in templates to ensure they work correctly."""

    def setup_method(self) -> None:
        """Set up test environment."""
        # Use the actual built-in templates
        template_dir = Path(__file__).parent.parent / "waldur_site_agent" / "testing" / "templates"
        from waldur_site_agent.testing.template_engine import OrderTemplateEngine

        self.engine = OrderTemplateEngine(template_dir)

    def test_all_create_templates(self) -> None:
        """Test all CREATE templates validate correctly."""
        create_templates = [
            "create/basic.json",
            "create/with-limits.json",
            "create/slurm-full.json",
        ]

        base_vars = {
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "template-test",
        }

        for template_name in create_templates:
            result = self.engine.validate_template(template_name, **base_vars)
            assert result.is_valid, f"Template {template_name} should be valid"

    def test_all_update_templates(self) -> None:
        """Test all UPDATE templates validate correctly."""
        update_templates = ["update/limits-only.json", "update/attributes-and-limits.json"]

        base_vars = {
            "marketplace_resource_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "template-test",
            "new_cpu_limit": 2000,
            "new_mem_limit": 4096,
        }

        for template_name in update_templates:
            result = self.engine.validate_template(template_name, **base_vars)
            assert result.is_valid, f"Template {template_name} should be valid"

    def test_terminate_template(self) -> None:
        """Test TERMINATE template validates correctly."""
        base_vars = {
            "marketplace_resource_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "template-test",
        }

        result = self.engine.validate_template("terminate/basic.json", **base_vars)
        assert result.is_valid

    def test_template_rendering_produces_valid_orders(self) -> None:
        """Test that all built-in templates produce valid OrderDetails objects."""
        # Test CREATE template
        create_order = self.engine.render_template(
            "create/basic.json",
            offering_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="render-test",
        )
        assert create_order.type_.value == "Create"
        assert create_order.resource_name == "render-test"
        assert create_order.uuid is not None

        # Test UPDATE template
        update_order = self.engine.render_template(
            "update/limits-only.json",
            marketplace_resource_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="update-test",
            new_cpu_limit=3000,
            new_mem_limit=6144,
        )
        assert update_order.type_.value == "Update"
        assert update_order.resource_name == "update-test"

        # Test TERMINATE template
        terminate_order = self.engine.render_template(
            "terminate/basic.json",
            marketplace_resource_uuid="d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            resource_name="terminate-test",
        )
        assert terminate_order.type_.value == "Terminate"
        assert terminate_order.resource_name == "terminate-test"

    def test_slurm_full_template_with_all_variables(self) -> None:
        """Test the comprehensive SLURM template with all possible variables."""
        variables = {
            "offering_uuid": "d629d5e4-5567-425d-a9cd-bdc1af67b32c",
            "resource_name": "slurm-comprehensive",
            "project_slug": "hpc-project-123",
            "customer_slug": "research-org",
            "cpu_limit": 10000,
            "mem_limit": 32768,
            "gpu_limit": 8,
            "storage_limit": 1000,
            "slurm_account": "research_account",
            "partition": "gpu_v100",
            "qos": "premium",
            "tres_billing_weights": "CPU=2.0,Mem=0.5G,GRES/gpu=8.0",
            "account_description": "Research allocation for ML workloads",
        }

        order = self.engine.render_template("create/slurm-full.json", **variables)

        assert order.type_.value == "Create"
        assert order.resource_name == "slurm-comprehensive"
        assert order.limits.additional_properties["cpu"] == 10000
        assert order.limits.additional_properties["mem"] == 32768
        assert order.limits.additional_properties["gpu"] == 8
        assert order.limits.additional_properties["storage"] == 1000
        assert order.attributes["slurm_account"] == "research_account"
        assert order.attributes["partition"] == "gpu_v100"
        assert order.attributes["qos"] == "premium"
