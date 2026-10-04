"""A retried order must reuse the Harbor project it already created.

If the project was created but recording the backend id in Waldur failed, the
order is retried with the same project name. The project carries a
project-scoped label naming the Waldur resource, so the retry adopts it instead
of raising ``DuplicateResourceError`` (which errs the order and orphans the
project, or creates a second project under the next candidate name).
"""

import uuid
from typing import Optional
from unittest.mock import patch

import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.exceptions import BackendError, DuplicateResourceError
from waldur_site_agent_harbor.backend import HarborBackend
from waldur_site_agent_harbor.client import HarborClient
from waldur_site_agent_harbor.exceptions import HarborAPIError, HarborQuotaError


class FakeHarborClient(HarborClient):
    """Holds projects, labels and group members in memory instead of calling Harbor."""

    def __init__(  # noqa: D107
        self,
        label_error: Optional[Exception] = None,
        label_errors_before_success: int = 0,
        list_error: Optional[Exception] = None,
        quota_error: Optional[Exception] = None,
    ) -> None:
        self.label_errors_before_success = label_errors_before_success
        self.list_error = list_error
        self.quota_error = quota_error
        self.label_posts = 0
        self.projects: dict[str, dict] = {}
        self.labels: dict[str, list[str]] = {}
        self.members: dict[str, set[str]] = {}
        self.quotas: dict[str, int] = {}
        self.label_error = label_error

    def add_project(self, name: str, labels: Optional[list[str]] = None) -> None:
        self.projects[name] = {"name": name, "project_id": len(self.projects) + 1}
        self.labels[name] = list(labels or [])
        self.members[name] = set()

    def get_project(self, project_name: str) -> Optional[dict]:
        return self.projects.get(project_name)

    def create_project(self, project_name: str, storage_quota_gb: int) -> bool:
        if project_name in self.projects:
            return False
        self.add_project(project_name)
        self.quotas[project_name] = storage_quota_gb
        return True

    def update_project_quota(self, project_name: str, new_quota_gb: int) -> bool:
        if self.quota_error:
            raise self.quota_error
        self.quotas[project_name] = new_quota_gb
        return True

    def create_user_group(self, group_name: str) -> int:
        return 1

    def assign_group_to_project(self, group_name: str, project_name: str, role_id: int = 2) -> bool:
        self.members[project_name].add(group_name)
        return True

    def add_project_label(self, project_name: str, label_name: str) -> None:
        self.label_posts += 1
        if self.label_error:
            raise self.label_error
        if self.label_errors_before_success:
            self.label_errors_before_success -= 1
            raise HarborAPIError("API request failed: 502 Bad Gateway")
        self.labels[project_name].append(label_name)

    def list_project_label_names(self, project_name: str) -> list[str]:
        if self.list_error:
            raise self.list_error
        return list(self.labels.get(project_name, []))


def _backend(client: FakeHarborClient) -> HarborBackend:
    with patch("waldur_site_agent_harbor.backend.HarborClient"):
        backend = HarborBackend(
            {
                "harbor_url": "https://harbor.example.com",
                "robot_username": "robot$test",
                "robot_password": "secret",
                "default_storage_quota_gb": 10,
                "oidc_group_prefix": "waldur-",
                "project_role_id": 2,
            },
            {"storage": {"measured_unit": "GB", "unit_factor": 1, "accounting_type": "limit", "label": "Storage"}},
        )
    backend.client = client
    return backend


def _resource() -> WaldurResource:
    return WaldurResource.from_dict(
        {
            "uuid": uuid.uuid4().hex,
            "name": "Registry",
            "slug": "registry",
            "project_slug": "test-project",
            "limits": {"storage": 25},
        }
    )


def test_new_project_is_marked_for_its_resource() -> None:
    client = FakeHarborClient()
    resource = _resource()

    _backend(client).create_resource_with_id(resource, "waldur-registry")

    assert client.labels["waldur-registry"] == [f"waldur-resource-{resource.uuid.hex}"]


def test_retry_after_partial_order_adopts_the_project() -> None:
    client = FakeHarborClient()
    backend = _backend(client)
    resource = _resource()

    first = backend.create_resource_with_id(resource, "waldur-registry")
    # set_backend_id failed; the retry computes the same project name.
    second = backend.create_resource_with_id(resource, "waldur-registry")

    assert second.backend_id == first.backend_id == "waldur-registry"
    assert list(client.projects) == ["waldur-registry"]
    assert client.members["waldur-registry"] == {"waldur-test-project"}
    assert client.quotas["waldur-registry"] == 25


def test_adopted_project_gets_access_even_if_the_first_attempt_stopped_early() -> None:
    """The first attempt created and marked the project but died before granting access."""
    client = FakeHarborClient()
    resource = _resource()
    client.add_project("waldur-registry", labels=[f"waldur-resource-{resource.uuid.hex}"])

    result = _backend(client).create_resource_with_id(resource, "waldur-registry")

    assert result.backend_id == "waldur-registry"
    assert client.members["waldur-registry"] == {"waldur-test-project"}
    assert client.quotas["waldur-registry"] == 25


def test_project_marked_for_another_resource_is_a_duplicate() -> None:
    client = FakeHarborClient()
    client.add_project("waldur-registry", labels=[f"waldur-resource-{uuid.uuid4().hex}"])

    with pytest.raises(DuplicateResourceError):
        _backend(client).create_resource_with_id(_resource(), "waldur-registry")
    assert client.members["waldur-registry"] == set()


def test_unmarked_existing_project_is_a_duplicate() -> None:
    client = FakeHarborClient()
    client.add_project("waldur-registry")

    with pytest.raises(DuplicateResourceError):
        _backend(client).create_resource_with_id(_resource(), "waldur-registry")
    assert client.members["waldur-registry"] == set()


def test_failing_to_mark_the_project_does_not_fail_the_order() -> None:
    """A robot account without label rights still gets a working project, just no retry safety."""
    client = FakeHarborClient(label_error=HarborAPIError("403 Forbidden"))

    result = _backend(client).create_resource_with_id(_resource(), "waldur-registry")

    assert result.backend_id == "waldur-registry"
    assert client.members["waldur-registry"] == {"waldur-test-project"}


def test_label_read_failure_retries_the_order_instead_of_creating_another_project() -> None:
    """Treating an unreadable label as "not ours" sends the processor to {slug}-0."""
    client = FakeHarborClient(list_error=HarborAPIError("API request failed: 503"))
    resource = _resource()
    client.add_project("waldur-registry", labels=[f"waldur-resource-{resource.uuid.hex}"])

    with pytest.raises(BackendError) as excinfo:
        _backend(client).create_resource_with_id(resource, "waldur-registry")
    assert not isinstance(excinfo.value, DuplicateResourceError)
    assert list(client.projects) == ["waldur-registry"]


def test_label_read_forbidden_names_the_missing_permission() -> None:
    client = FakeHarborClient(
        list_error=HarborAPIError("API request failed: 403 | Status: 403 | Body: forbidden")
    )
    client.add_project("waldur-registry")

    with pytest.raises(BackendError, match="label") as excinfo:
        _backend(client).create_resource_with_id(_resource(), "waldur-registry")
    assert not isinstance(excinfo.value, DuplicateResourceError)


def test_label_post_is_retried_once_on_a_transient_failure() -> None:
    client = FakeHarborClient(label_errors_before_success=1)
    resource = _resource()

    _backend(client).create_resource_with_id(resource, "waldur-registry")

    assert client.label_posts == 2
    assert client.labels["waldur-registry"] == [f"waldur-resource-{resource.uuid.hex}"]


def test_quota_failure_on_adoption_is_a_backend_error() -> None:
    client = FakeHarborClient(quota_error=HarborQuotaError("No quota found for project"))
    resource = _resource()
    client.add_project("waldur-registry", labels=[f"waldur-resource-{resource.uuid.hex}"])

    with pytest.raises(BackendError, match="quota"):
        _backend(client).create_resource_with_id(resource, "waldur-registry")


def test_label_listing_follows_pages() -> None:
    """A project with more labels than one page still finds the marker on a later page."""
    from unittest.mock import Mock

    client = HarborClient.__new__(HarborClient)
    client.get_project = Mock(return_value={"name": "p", "project_id": 7})  # type: ignore[method-assign]
    pages = [
        [{"name": f"other-{i}"} for i in range(100)],
        [{"name": "waldur-resource-abc"}],
    ]
    client._make_request = Mock(side_effect=lambda *a, **k: pages.pop(0))  # type: ignore[method-assign]
    client._parse_json_response = lambda response: response  # type: ignore[method-assign]

    names = client.list_project_label_names("p")

    assert "waldur-resource-abc" in names
    assert len(names) == 101
