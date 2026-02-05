"""Tests for WaldurClient with mocked waldur_api_client responses."""

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_waldur.client import WaldurClient

# Valid test UUIDs
ORDER_UUID = UUID("12345678-1234-1234-1234-123456789abc")
RESOURCE_UUID = UUID("abcdef01-1234-1234-1234-123456789abc")
PROJECT_UUID = UUID("aabbccdd-1234-1234-1234-123456789abc")
USER_UUID = UUID("11223344-1234-1234-1234-123456789abc")


@pytest.fixture()
def client():
    return WaldurClient(
        api_url="https://waldur-b.example.com/api/",
        api_token="test-token",
        offering_uuid="abcdef01-0000-0000-0000-000000000001",
    )


class TestMarketplaceOrders:
    def test_create_marketplace_order(self, client):
        mock_order = MagicMock()
        mock_order.uuid = ORDER_UUID

        with patch(
            "waldur_api_client.api.marketplace_orders.marketplace_orders_create.sync",
            return_value=mock_order,
        ) as mock_create:
            result = client.create_marketplace_order(
                project_url="/api/projects/proj-uuid/",
                offering_url="/api/marketplace-provider-offerings/off-uuid/",
                limits={"cpu": 100, "mem": 200},
                attributes={"name": "test-resource"},
            )

            assert result == mock_order
            mock_create.assert_called_once()

    def test_get_order(self, client):
        mock_order = MagicMock()
        mock_order.state = "done"

        with patch(
            "waldur_api_client.api.marketplace_orders.marketplace_orders_retrieve.sync",
            return_value=mock_order,
        ) as mock_retrieve:
            result = client.get_order(ORDER_UUID)
            assert result == mock_order
            mock_retrieve.assert_called_once()

    def test_poll_order_completion_success(self, client):
        from waldur_api_client.models.order_state import OrderState

        mock_order = MagicMock()
        mock_order.state = OrderState.DONE
        mock_order.marketplace_resource_uuid = RESOURCE_UUID

        with patch.object(client, "get_order", return_value=mock_order):
            result = client.poll_order_completion(
                order_uuid=ORDER_UUID,
                timeout=5,
                interval=1,
            )
            assert result == mock_order

    def test_poll_order_completion_erred(self, client):
        from waldur_api_client.models.order_state import OrderState

        mock_order = MagicMock()
        mock_order.state = OrderState.ERRED
        mock_order.error_message = "Something went wrong"

        with patch.object(client, "get_order", return_value=mock_order):
            with pytest.raises(BackendError, match="erred"):
                client.poll_order_completion(
                    order_uuid=ORDER_UUID,
                    timeout=5,
                    interval=1,
                )

    def test_poll_order_completion_timeout(self, client):
        from waldur_api_client.models.order_state import OrderState

        mock_order = MagicMock()
        mock_order.state = OrderState.EXECUTING

        with patch.object(client, "get_order", return_value=mock_order):
            with pytest.raises(BackendError, match="timed out"):
                client.poll_order_completion(
                    order_uuid=ORDER_UUID,
                    timeout=2,
                    interval=1,
                )


class TestProjectOperations:
    def test_find_project_by_backend_id_found(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "uuid": str(PROJECT_UUID),
                "url": "/api/projects/proj-uuid/",
                "name": "Test Project",
            }
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.get.return_value = mock_response

        with patch(
            "waldur_api_client.client.AuthenticatedClient.get_httpx_client",
            return_value=mock_httpx_client,
        ):
            result = client.find_project_by_backend_id("customer_project")
            assert result is not None
            assert result["name"] == "Test Project"
            mock_httpx_client.get.assert_called_once_with(
                "/api/projects/",
                params={"backend_id": "customer_project"},
            )

    def test_find_project_by_backend_id_not_found(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.get.return_value = mock_response

        with patch(
            "waldur_api_client.client.AuthenticatedClient.get_httpx_client",
            return_value=mock_httpx_client,
        ):
            result = client.find_project_by_backend_id("nonexistent")
            assert result is None

    def test_find_or_create_project_existing(self, client):
        existing_project = {
            "uuid": "proj-uuid",
            "url": "/api/projects/proj-uuid/",
            "name": "Existing",
        }

        with patch.object(
            client, "find_project_by_backend_id", return_value=existing_project
        ):
            result = client.find_or_create_project(
                customer_url="/api/customers/cust-uuid/",
                name="New Project",
                backend_id="test_backend_id",
            )
            assert result["uuid"] == "proj-uuid"

    def test_find_or_create_project_creates_new(self, client):
        new_project = {
            "uuid": "new-proj-uuid",
            "url": "/api/projects/new-proj-uuid/",
            "name": "New Project",
        }

        with (
            patch.object(client, "find_project_by_backend_id", return_value=None),
            patch.object(client, "create_project", return_value=new_project),
        ):
            result = client.find_or_create_project(
                customer_url="/api/customers/cust-uuid/",
                name="New Project",
                backend_id="test_backend_id",
            )
            assert result["uuid"] == "new-proj-uuid"


class TestUserResolution:
    def test_resolve_user_by_cuid(self, client):
        mock_result = MagicMock()
        mock_result.uuid = USER_UUID

        with patch(
            "waldur_api_client.api.remote_eduteams.remote_eduteams.sync",
            return_value=mock_result,
        ):
            result = client.resolve_user_by_cuid("user-cuid-123")
            assert result == USER_UUID

    def test_resolve_user_by_cuid_not_found(self, client):
        with patch(
            "waldur_api_client.api.remote_eduteams.remote_eduteams.sync",
            side_effect=Exception("Not found"),
        ):
            result = client.resolve_user_by_cuid("nonexistent-cuid")
            assert result is None

    def test_resolve_user_by_email(self, client):
        mock_user = MagicMock()
        mock_user.uuid = USER_UUID

        with patch(
            "waldur_api_client.api.users.users_list.sync",
            return_value=[mock_user],
        ):
            result = client.resolve_user_by_field("user@example.com", "email")
            assert result == USER_UUID


class TestBaseClientMethods:
    def test_list_resources(self, client):
        mock_resource = MagicMock()
        mock_resource.uuid = RESOURCE_UUID
        mock_resource.name = "Test Resource"
        mock_resource.project_uuid = PROJECT_UUID

        with patch.object(
            client, "list_marketplace_resources", return_value=[mock_resource]
        ):
            result = client.list_resources()
            assert len(result) == 1
            assert result[0].name == str(RESOURCE_UUID)

    def test_get_resource_found(self, client):
        mock_resource = MagicMock()
        mock_resource.uuid = RESOURCE_UUID
        mock_resource.name = "Test Resource"
        mock_resource.project_uuid = PROJECT_UUID

        with patch.object(
            client, "get_marketplace_resource", return_value=mock_resource
        ):
            result = client.get_resource(str(RESOURCE_UUID))
            assert result is not None
            assert result.description == "Test Resource"

    def test_get_resource_not_found(self, client):
        with patch.object(
            client, "get_marketplace_resource", side_effect=Exception("Not found")
        ):
            result = client.get_resource("nonexistent-uuid")
            assert result is None

    def test_list_resource_users(self, client):
        mock_member = MagicMock()
        mock_member.username = "user1"

        with patch.object(client, "get_resource_team", return_value=[mock_member]):
            result = client.list_resource_users(str(RESOURCE_UUID))
            assert result == ["user1"]

    def test_ping_success(self, client):
        with patch.object(client, "list_marketplace_resources", return_value=[]):
            assert client.ping() is True

    def test_ping_failure(self, client):
        with patch.object(
            client,
            "list_marketplace_resources",
            side_effect=Exception("Connection refused"),
        ):
            assert client.ping() is False

    def test_get_offering_url(self, client):
        url = client.get_offering_url()
        assert "marketplace-public-offerings" in url
        assert "abcdef01000000000000000000000001" in url

    def test_get_customer_url(self, client):
        url = client.get_customer_url("aabbccdd-1234-1234-1234-123456789abc")
        assert "customers/aabbccdd12341234123412345678" in url
