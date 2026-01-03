"""Mock backend implementation for safe order testing."""

from __future__ import annotations

from typing import Any, Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.backends import BaseBackend
from waldur_site_agent.backend.structures import BackendResourceInfo


class MockBackend(BaseBackend):
    """Mock backend implementation for testing without affecting real systems."""

    def __init__(
        self,
        backend_settings: dict[str, Any] | None = None,
        backend_components: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Initialize the mock backend."""
        if backend_settings is None:
            backend_settings = {}
        if backend_components is None:
            backend_components = {}

        super().__init__(
            backend_settings=backend_settings, backend_components=backend_components, **kwargs
        )
        self.backend_type = "mock"
        self._created_resources: dict[str, BackendResourceInfo] = {}
        self._operations_log: list[dict[str, Any]] = []

    def _log_operation(self, operation: str, **details: Any) -> None:  # noqa: ANN401
        """Log backend operations for inspection during testing."""
        log_entry = {"operation": operation, **details}
        self._operations_log.append(log_entry)
        logger.info("Mock backend operation: %s", log_entry)

    def get_operations_log(self) -> list[dict[str, Any]]:
        """Get log of all operations performed by this mock backend."""
        return self._operations_log.copy()

    def clear_operations_log(self) -> None:
        """Clear the operations log."""
        self._operations_log.clear()

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: dict[str, Any] | None = None,
    ) -> BackendResourceInfo:
        """Create a mock resource with the specified backend ID."""
        if user_context is None:
            user_context = {}

        self._log_operation(
            "create_resource",
            backend_id=resource_backend_id,
            resource_name=waldur_resource.name,
            user_context_users=len(user_context.get("team", [])),
        )

        # Simulate resource creation
        resource_info = BackendResourceInfo(
            backend_id=resource_backend_id,
            parent_id=f"mock_project_{waldur_resource.project_uuid}",
            users=[],
            usage={
                "TOTAL_ACCOUNT_USAGE": {"cpu": 0.0, "mem": 0.0},
            },
            limits={"cpu": 1000, "mem": 2048},  # Default mock limits
        )

        self._created_resources[resource_backend_id] = resource_info
        return resource_info

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: Any) -> None:  # noqa: ANN401, ARG002
        """Delete a mock resource."""
        backend_id = waldur_resource.backend_id
        self._log_operation("delete_resource", backend_id=backend_id)

        if backend_id in self._created_resources:
            del self._created_resources[backend_id]

    def set_resource_limits(self, backend_id: str, limits: dict[str, Any]) -> None:
        """Update resource limits in the mock backend."""
        self._log_operation("set_resource_limits", backend_id=backend_id, limits=limits)

        if backend_id in self._created_resources:
            self._created_resources[backend_id].limits.update(limits)

    def pull_resource(self, waldur_resource: WaldurResource) -> Optional[BackendResourceInfo]:
        """Retrieve resource information from the mock backend."""
        backend_id = waldur_resource.backend_id
        self._log_operation("pull_resource", backend_id=backend_id)

        return self._created_resources.get(backend_id)

    def pull_resources(
        self, waldur_resources: list[WaldurResource]
    ) -> dict[str, tuple[WaldurResource, BackendResourceInfo]]:
        """Pull multiple resources from the mock backend."""
        result = {}
        for resource in waldur_resources:
            backend_info = self.pull_resource(resource)
            if backend_info is not None:
                result[resource.backend_id] = (resource, backend_info)
        return result

    def list_resources(self) -> list[BackendResourceInfo]:
        """List all resources in the mock backend."""
        self._log_operation("list_resources", count=len(self._created_resources))
        return list(self._created_resources.values())

    def add_user(self, waldur_resource: WaldurResource, username: str) -> bool:
        """Add a user to a mock resource."""
        backend_id = waldur_resource.backend_id
        self._log_operation("add_user", backend_id=backend_id, username=username)

        if backend_id in self._created_resources:
            resource_info = self._created_resources[backend_id]
            if username not in resource_info.users:
                resource_info.users.append(username)
        return True

    def remove_user(self, waldur_resource: WaldurResource, username: str) -> bool:
        """Remove a user from a mock resource."""
        backend_id = waldur_resource.backend_id
        self._log_operation("remove_user", backend_id=backend_id, username=username)

        if backend_id in self._created_resources:
            resource_info = self._created_resources[backend_id]
            if username in resource_info.users:
                resource_info.users.remove(username)
        return True

    def add_users_to_resource(
        self,
        waldur_resource: WaldurResource,
        user_ids: set[str],
        **kwargs: dict[Any, Any],  # noqa: ARG002
    ) -> set[str]:
        """Add multiple users to a mock resource."""
        backend_id = waldur_resource.backend_id
        self._log_operation(
            "add_users_to_resource", backend_id=backend_id, usernames=list(user_ids)
        )

        if backend_id in self._created_resources:
            resource_info = self._created_resources[backend_id]
            for username in user_ids:
                if username not in resource_info.users:
                    resource_info.users.append(username)

        return user_ids  # Mock backend adds all users successfully

    def remove_users_from_resource(
        self, waldur_resource: WaldurResource, usernames: set[str]
    ) -> list[str]:
        """Remove multiple users from a mock resource."""
        backend_id = waldur_resource.backend_id
        self._log_operation(
            "remove_users_from_resource", backend_id=backend_id, usernames=list(usernames)
        )

        removed_users = []
        if backend_id in self._created_resources:
            resource_info = self._created_resources[backend_id]
            for username in usernames:
                if username in resource_info.users:
                    resource_info.users.remove(username)
                    removed_users.append(username)
        return removed_users

    def get_resource_limits(self, backend_id: str) -> dict[str, Any]:
        """Get resource limits from the mock backend."""
        self._log_operation("get_resource_limits", backend_id=backend_id)

        if backend_id in self._created_resources:
            return self._created_resources[backend_id].limits.copy()
        return {}

    def get_resource_metadata(self, backend_id: str) -> dict[str, Any]:
        """Get resource metadata from the mock backend."""
        self._log_operation("get_resource_metadata", backend_id=backend_id)

        if backend_id in self._created_resources:
            resource_info = self._created_resources[backend_id]
            return {
                "backend_type": "mock",
                "users": resource_info.users.copy(),
                "limits": resource_info.limits.copy(),
                "created_at": "2024-01-01T00:00:00Z",
            }
        return {}

    def ping(self, raise_exception: bool = False) -> bool:  # noqa: ARG002
        """Mock backend is always available."""
        self._log_operation("ping")
        return True

    # Additional methods for compatibility with BaseBackend interface

    def pause_resource(self, backend_id: str) -> bool:
        """Mock resource pausing."""
        self._log_operation("pause_resource", backend_id=backend_id)
        return True

    def downscale_resource(self, backend_id: str) -> bool:
        """Mock resource downscaling."""
        self._log_operation("downscale_resource", backend_id=backend_id)
        return True

    def restore_resource(self, backend_id: str) -> bool:
        """Mock resource restoration."""
        self._log_operation("restore_resource", backend_id=backend_id)
        return True

    def get_resource_user_limits(self, backend_id: str) -> dict[str, dict[str, Any]]:
        """Get mock user limits for a resource."""
        self._log_operation("get_resource_user_limits", backend_id=backend_id)
        return {}

    def set_resource_user_limits(
        self, backend_id: str, username: str, limits: dict[str, Any]
    ) -> None:
        """Set mock user limits for a resource."""
        self._log_operation(
            "set_resource_user_limits", backend_id=backend_id, username=username, limits=limits
        )

    def _collect_resource_limits(self, backend_id: str) -> tuple[dict[str, int], dict[str, int]]:
        """Collect resource limits from mock backend."""
        self._log_operation("_collect_resource_limits", backend_id=backend_id)
        if backend_id in self._created_resources:
            limits = self._created_resources[backend_id].limits
            int_limits = {k: int(v) for k, v in limits.items() if isinstance(v, (int, float))}
            return int_limits, {}
        return {}, {}

    def _get_usage_report(self, backend_ids: list[str]) -> dict[str, dict[str, float]]:
        """Get usage report from mock backend."""
        self._log_operation("_get_usage_report", backend_ids=backend_ids)
        result = {}
        for backend_id in backend_ids:
            if backend_id in self._created_resources:
                result[backend_id] = self._created_resources[backend_id].usage.copy()
            else:
                result[backend_id] = {"TOTAL_ACCOUNT_USAGE": {"cpu": 0.0, "mem": 0.0}}
        return result

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: dict[Any, Any] | None = None
    ) -> None:
        """Pre-creation setup for mock resources."""
        self._log_operation(
            "_pre_create_resource",
            resource_name=waldur_resource.name,
            user_count=len(user_context.get("team", []) if user_context else []),
        )

    def diagnostics(self) -> bool:
        """Return mock backend diagnostics."""
        self._log_operation("diagnostics")
        return True

    def list_components(self) -> list[str]:
        """List mock backend components."""
        self._log_operation("list_components")
        return ["cpu", "mem"]
