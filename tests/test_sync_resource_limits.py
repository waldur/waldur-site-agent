"""sync_resource_limits reads the offering's resources through the provider API."""

from unittest import mock

from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent.common import utils


@mock.patch.object(utils.marketplace_provider_resources_list, "sync_all")
@mock.patch.object(utils, "get_client_for_offering")
@mock.patch.object(utils, "get_backend_for_offering")
@mock.patch.object(utils, "init_configuration")
def test_sync_resource_limits_lists_provider_resources(
    init_configuration, get_backend_for_offering, get_client_for_offering, provider_list
):
    offering = mock.Mock(uuid="offering-uuid", membership_sync_backend="slurm")
    init_configuration.return_value = mock.Mock(waldur_offerings=[offering])
    backend = mock.Mock()
    get_backend_for_offering.return_value = (backend, None)
    client = get_client_for_offering.return_value
    with_backend_id = mock.Mock(backend_id="acc-1")
    provider_list.return_value = [with_backend_id, mock.Mock(backend_id="")]

    utils.sync_resource_limits()

    provider_list.assert_called_once_with(
        client=client,
        offering_uuid=["offering-uuid"],
        state=[ResourceState.OK, ResourceState.ERRED],
    )
    backend.sync_resource_limits.assert_called_once_with(with_backend_id, client)
