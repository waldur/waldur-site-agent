"""Bundled order templates keep UUIDs the caller supplies and generate the rest."""

import uuid
from pathlib import Path

import pytest

from waldur_site_agent.testing.template_engine import OrderTemplateEngine

TEMPLATES = Path(__file__).parent.parent / "waldur_site_agent" / "testing" / "templates"
OFFERING = "d629d5e4-5567-425d-a9cd-bdc1af67b32c"
ORDER = "11111111-2222-4333-8444-555555555555"
PROJECT = "66666666-7777-4888-9999-aaaaaaaaaaaa"


@pytest.fixture
def engine() -> OrderTemplateEngine:
    return OrderTemplateEngine(TEMPLATES)


@pytest.mark.parametrize(
    "template", ["create/basic.json", "create/with-limits.json", "create/slurm-full.json"]
)
def test_supplied_order_and_project_uuids_are_kept(engine, template):
    order = engine.render_template(
        template, offering_uuid=OFFERING, order_uuid=ORDER, project_uuid=PROJECT
    )
    assert str(order.uuid) == ORDER
    assert str(order.project_uuid) == PROJECT


@pytest.mark.parametrize(
    "template", ["create/basic.json", "create/with-limits.json", "create/slurm-full.json"]
)
def test_missing_order_uuid_is_generated(engine, template):
    first = engine.render_template(template, offering_uuid=OFFERING)
    second = engine.render_template(template, offering_uuid=OFFERING)
    assert isinstance(first.uuid, uuid.UUID)
    assert first.uuid != second.uuid


def test_empty_input_still_generates_a_uuid(engine):
    rendered = engine.jinja_env.from_string("{{ '' | uuid4 }}").render()
    assert uuid.UUID(rendered)


def test_generated_uuid_differs_between_renders_of_a_cached_template(engine):
    """'' | uuid4 must not be folded into a constant when the template compiles."""
    template = engine.jinja_env.from_string("{{ '' | uuid4 }}")
    assert template.render() != template.render()
