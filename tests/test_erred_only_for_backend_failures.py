"""ERRED is reserved for backend failures.

A resource or order is marked ERRED when the backend could not do its part.
A non-transient error from this Waldur's own API during bookkeeping (reading
per-user limits, listing service accounts, refreshing last sync, post-processing
an order that is already done) says nothing about the backend, so it must not
flip a working resource or a finished order to ERRED.
"""

from unittest import mock
from uuid import UUID

import httpx
import pytest
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import utils
from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingOrderProcessor,
)

WALDUR_BASE_URL = "https://waldur.example.com"


def _status(status_code: int, url: str) -> UnexpectedStatus:
    return UnexpectedStatus(status_code=status_code, content=b"{}", url=httpx.URL(url))


def _own_waldur_error(status_code: int = 403, path: str = "/api/component-user-usage-limits/"):
    return _status(status_code, WALDUR_BASE_URL + path)


def _resource(index: int):
    resource = mock.Mock()
    resource.uuid = UUID(int=index)
    resource.backend_id = f"allocation-{index}"
    resource.name = f"resource-{index}"
    resource.state = ResourceState.OK
    return resource


def _report(*resources):
    return {
        r.backend_id: (r, BackendResourceInfo(backend_id=r.backend_id)) for r in resources
    }


@pytest.fixture()
def membership_processor():
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = utils.get_client(WALDUR_BASE_URL + "/api/", "token")
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.offering.username_reconciliation_enabled = False
    processor.resource_backend = mock.Mock()
    processor.resource_backend.remote_waldur_base_urls.return_value = []
    processor.expose_backend_error_details = True
    processor._refresh_local_offering_users = mock.Mock(return_value=[])
    processor._sync_user_profiles_to_backend = mock.Mock()
    processor._fetch_source_project = mock.Mock(return_value=None)
    processor._sync_resource_users = mock.Mock(return_value=set())
    processor._sync_resource_service_accounts = mock.Mock()
    processor._sync_resource_course_accounts = mock.Mock()
    processor._sync_resource_status = mock.Mock()
    processor._sync_resource_limits = mock.Mock()
    processor._sync_resource_user_limits = mock.Mock()
    return processor


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_own_waldur_4xx_in_bookkeeping_does_not_erred_resource(
    mock_mark_erred, mock_refresh, membership_processor
):
    membership_processor._sync_resource_user_limits.side_effect = _own_waldur_error(403)

    membership_processor._process_resources(_report(_resource(1)))

    mock_mark_erred.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_own_waldur_4xx_skips_only_that_step(
    mock_mark_erred, mock_refresh, membership_processor
):
    membership_processor._sync_resource_service_accounts.side_effect = _own_waldur_error(
        403, "/api/marketplace-provider-offerings/x/list_project_service_accounts/"
    )

    membership_processor._process_resources(_report(_resource(1)))

    membership_processor._sync_resource_course_accounts.assert_called_once()
    membership_processor._sync_resource_status.assert_called_once()
    membership_processor._sync_resource_limits.assert_called_once()
    membership_processor._sync_resource_user_limits.assert_called_once()
    mock_mark_erred.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_set_as_ok")
@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_partial_sync_clears_erred_but_withholds_last_sync(
    mock_mark_erred, mock_refresh, mock_set_ok, membership_processor
):
    """Skipped bookkeeping says nothing about the backend: an ERRED resource whose
    backend steps all ran goes back to OK, but last_sync is not claimed."""
    resource = _resource(1)
    resource.state = ResourceState.ERRED
    membership_processor._sync_resource_user_limits.side_effect = _own_waldur_error(403)

    membership_processor._process_resources(_report(resource))

    mock_mark_erred.assert_not_called()
    mock_refresh.sync_detailed.assert_not_called()
    mock_set_ok.sync_detailed.assert_called_once()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_remote_waldur_4xx_still_marks_resource_erred(
    mock_mark_erred, mock_refresh, membership_processor
):
    """For Waldur-to-Waldur backends the remote Waldur is the backend."""
    membership_processor._sync_resource_users.side_effect = _status(
        403, "https://remote-waldur.example.org/api/marketplace-resources/x/team/"
    )

    membership_processor._process_resources(_report(_resource(1)))

    mock_mark_erred.assert_called_once()


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_backend_error_still_marks_resource_erred(
    mock_mark_erred, mock_refresh, membership_processor
):
    membership_processor._sync_resource_status.side_effect = BackendError("sacctmgr failed")

    membership_processor._process_resources(_report(_resource(1)))

    mock_mark_erred.assert_called_once()


@mock.patch("waldur_site_agent.common.utils.marketplace_provider_resources_set_as_erred")
def test_transport_error_while_marking_erred_does_not_abort_loop(mock_set_as_erred):
    first, second = _resource(1), _resource(2)
    mock_set_as_erred.sync_detailed.side_effect = [
        httpx.RemoteProtocolError("Server disconnected without sending a response."),
        mock.Mock(status_code=200),
    ]

    utils.mark_waldur_resources_as_erred(
        mock.Mock(), [first, second], {"error_message": "boom", "error_traceback": ""}
    )

    assert mock_set_as_erred.sync_detailed.call_count == 2


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.utils.marketplace_provider_resources_set_as_erred")
def test_failed_mark_erred_does_not_skip_remaining_resources(
    mock_set_as_erred, mock_refresh, membership_processor
):
    first, second = _resource(1), _resource(2)
    membership_processor._sync_resource_status.side_effect = [BackendError("down"), None]
    mock_set_as_erred.sync_detailed.side_effect = httpx.RemoteProtocolError("dropped")

    membership_processor._process_resources(_report(first, second))

    assert membership_processor._sync_resource_status.call_count == 2


@pytest.fixture()
def order_processor():
    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = utils.get_client(WALDUR_BASE_URL + "/api/", "token")
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.resource_backend = mock.Mock()
    processor.expose_backend_error_details = True
    processor._process_create_order = mock.Mock(return_value=True)
    processor._post_process_order = mock.Mock()
    return processor


@pytest.fixture()
def executing_order():
    order = mock.Mock()
    order.uuid = UUID("22222222-2222-2222-2222-222222222222")
    order.state = OrderState.EXECUTING
    order.type_ = RequestTypes.CREATE
    order.resource_name = "test-resource"
    return order


def _order_with_state(state):
    refreshed = mock.Mock()
    refreshed.state = state
    return refreshed


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_done")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_post_processing_failure_after_done_does_not_erred_order(
    mock_erred, mock_retrieve, mock_done, order_processor, executing_order
):
    # Waldur keeps answering EXECUTING (e.g. set_state_done not yet visible), so only
    # the post-processing guard keeps the order from being erred.
    mock_retrieve.sync.return_value = _order_with_state(OrderState.EXECUTING)
    order_processor._post_process_order.side_effect = _own_waldur_error(
        404, "/api/marketplace-provider-resources/abc/"
    )

    order_processor.process_order(executing_order)

    mock_done.sync_detailed.assert_called_once()
    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_order_already_done_in_waldur_is_not_erred(
    mock_erred, mock_retrieve, order_processor, executing_order
):
    """The local order object is stale; the guard must use Waldur's current state."""
    order_processor._process_create_order.side_effect = BackendError("late failure")
    mock_retrieve.sync.return_value = _order_with_state(OrderState.DONE)

    order_processor.process_order(executing_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_backend_failure_on_executing_order_still_erred(
    mock_erred, mock_retrieve, order_processor, executing_order
):
    order_processor._process_create_order.side_effect = BackendError("sacctmgr failed")
    mock_retrieve.sync.return_value = _order_with_state(OrderState.EXECUTING)

    order_processor.process_order(executing_order)

    mock_erred.sync_detailed.assert_called_once()


@pytest.mark.parametrize(
    "fresh_state",
    [OrderState.DONE, OrderState.CANCELED, OrderState.REJECTED, OrderState.ERRED],
)
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_order_no_longer_executing_is_not_erred(
    mock_erred, mock_retrieve, order_processor, executing_order, fresh_state
):
    order_processor._process_create_order.side_effect = BackendError("late failure")
    mock_retrieve.sync.return_value = _order_with_state(fresh_state)

    order_processor.process_order(executing_order)

    mock_erred.sync_detailed.assert_not_called()


@mock.patch("waldur_site_agent.common.processors.marketplace_orders_retrieve")
@mock.patch("waldur_site_agent.common.processors.marketplace_orders_set_state_erred")
def test_unreadable_order_state_falls_back_to_local_executing(
    mock_erred, mock_retrieve, order_processor, executing_order
):
    order_processor._process_create_order.side_effect = BackendError("sacctmgr failed")
    mock_retrieve.sync.side_effect = httpx.ConnectError("refused")

    order_processor.process_order(executing_order)

    mock_erred.sync_detailed.assert_called_once()


def _processor_for(api_url, remote_waldur_urls=()):
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = utils.get_client(api_url, "token")
    processor.resource_backend = mock.Mock()
    processor.resource_backend.remote_waldur_base_urls.return_value = list(remote_waldur_urls)
    return processor


@pytest.mark.parametrize(
    ("api_url", "error_url"),
    [
        ("https://waldur.example.com/api/", "https://waldur.example.com/api/x/"),
        ("https://Waldur.Example.COM/api/", "https://waldur.example.com/api/x/"),
        ("https://waldur.example.com:443/api/", "https://waldur.example.com/api/x/"),
        ("http://waldur.example.com:80/api/", "http://waldur.example.com/api/x/"),
        ("https://waldur.example.com/api/", "https://waldur.example.com:443/api/x/"),
        ("https://h.example.com/my%20waldur/api/", "https://h.example.com/my waldur/api/x/"),
        ("https://h.example.com/my waldur/api/", "https://h.example.com/my%20waldur/api/x/"),
    ],
)
def test_own_waldur_url_matching_is_normalised(api_url, error_url):
    processor = _processor_for(api_url)

    assert processor._is_own_waldur_api_error(_status(403, error_url))


@pytest.mark.parametrize(
    ("api_url", "error_url"),
    [
        ("https://waldur.example.com/api/", "https://other.example.com/api/x/"),
        ("https://waldur.example.com/api/", "http://waldur.example.com/api/x/"),
        ("https://waldur.example.com:8443/api/", "https://waldur.example.com/api/x/"),
        ("https://h.example.com/a/api/", "https://h.example.com/ab/api/x/"),
    ],
)
def test_other_origins_are_not_own_waldur(api_url, error_url):
    processor = _processor_for(api_url)

    assert not processor._is_own_waldur_api_error(_status(403, error_url))


@pytest.mark.parametrize("status_code", [301, 302, 307, 500, 503, 429])
def test_only_client_errors_count_as_own_waldur(status_code):
    processor = _processor_for("https://waldur.example.com/api/")

    assert not processor._is_own_waldur_api_error(
        _status(status_code, "https://waldur.example.com/api/x/")
    )


@pytest.mark.parametrize(
    ("api_url", "remote_url", "error_url"),
    [
        # Waldur B served under a path on Waldur A's origin
        ("https://h.example.com/api/", "https://h.example.com/b", "https://h.example.com/b/api/x/"),
        # loopback test setup: A and B are the same origin
        ("http://localhost:8000/api/", "http://localhost:8000", "http://localhost:8000/api/x/"),
    ],
)
def test_federation_target_on_same_origin_is_not_own_waldur(api_url, remote_url, error_url):
    processor = _processor_for(api_url, [remote_url])

    assert not processor._is_own_waldur_api_error(_status(403, error_url))


@mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_refresh_last_sync")
@mock.patch("waldur_site_agent.common.processors.utils.mark_waldur_resources_as_erred")
def test_federation_target_4xx_on_same_origin_marks_erred(
    mock_mark_erred, mock_refresh, membership_processor
):
    membership_processor.waldur_rest_client = utils.get_client("http://localhost:8000/api/", "t")
    membership_processor.resource_backend.remote_waldur_base_urls.return_value = [
        "http://localhost:8000"
    ]
    membership_processor._sync_resource_users.side_effect = _status(
        403, "http://localhost:8000/api/marketplace-resources/x/team/"
    )

    membership_processor._process_resources(_report(_resource(1)))

    mock_mark_erred.assert_called_once()


def test_base_backend_declares_no_remote_waldur():
    from waldur_site_agent.backend.backends import BaseBackend  # noqa: PLC0415

    assert BaseBackend.remote_waldur_base_urls(mock.Mock()) == []
