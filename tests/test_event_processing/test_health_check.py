"""Agent health checks for offerings with and without a resource backend."""

from unittest import mock

from waldur_site_agent.common import structures
from waldur_site_agent.event_processing import utils


def _offering(**overrides) -> structures.Offering:
    fields = dict(
        name="ldap-only",
        waldur_offering_uuid="test-uuid",
        waldur_api_url="https://example.com/api/",
        waldur_api_token="token",
        backend_type="ldap",
        username_management_backend="ldap",
        stomp_enabled=False,
    )
    fields.update(overrides)
    return structures.Offering(**fields)


@mock.patch.object(utils, "marketplace_orders_list")
@mock.patch.object(utils, "get_client_for_offering")
def test_an_offering_without_a_resource_backend_reports_without_an_error(get_client, orders):
    with mock.patch.object(utils.common_processors, "OfferingOrderProcessor") as processor, (
        mock.patch.object(utils, "logger")
    ) as logger:
        utils.send_agent_health_checks([_offering()], "agent")
    processor.assert_not_called()
    orders.sync.assert_called_once_with(client=get_client.return_value, offering_uuid="test-uuid")
    logger.error.assert_not_called()


@mock.patch.object(utils, "marketplace_orders_list")
@mock.patch.object(utils, "get_client_for_offering")
def test_an_offering_with_an_order_backend_still_builds_the_processor(get_client, orders):
    with mock.patch.object(utils.common_processors, "OfferingOrderProcessor") as processor:
        utils.send_agent_health_checks([_offering(order_processing_backend="slurm")], "agent")
    processor.assert_called_once()
    orders.sync.assert_called_once()


@mock.patch.object(utils, "marketplace_orders_list")
@mock.patch.object(utils, "get_client_for_offering")
def test_a_failure_is_logged(get_client, orders):
    orders.sync.side_effect = RuntimeError("down")
    with mock.patch.object(utils, "logger") as logger:
        utils.send_agent_health_checks([_offering()], "agent")
    logger.error.assert_called_once()
