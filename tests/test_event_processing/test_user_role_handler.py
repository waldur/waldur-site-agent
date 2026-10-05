"""on_user_role_message_stomp must report the real error, not one raised by its own logging."""

import json
import unittest
from unittest import mock

from waldur_site_agent.event_processing import handlers

_PREFIX = "waldur_site_agent.event_processing.handlers"


def _frame(message):
    return mock.Mock(body=json.dumps(message), headers={"destination": "/amq/queue/q"})


@mock.patch(f"{_PREFIX}.common_processors.OfferingMembershipProcessor")
@mock.patch(f"{_PREFIX}.common_utils.get_backend_for_offering", return_value=(mock.Mock(), "1"))
@mock.patch(f"{_PREFIX}.register_event_process_service")
@mock.patch(f"{_PREFIX}.common_utils.get_client_for_offering")
class TestUserRoleHandlerErrors(unittest.TestCase):
    def test_missing_username_is_reported_not_unbound_local(self, *_mocks):
        message = {
            "user_uuid": "u-1",
            "project_uuid": "p-1",
            "project_name": "Project",
            "granted": True,
        }

        with self.assertLogs(level="ERROR") as logs:
            handlers.on_user_role_message_stomp(_frame(message), mock.Mock(), "ua")

        self.assertTrue(any("user_username" in line for line in logs.output))

    def test_early_failure_logs_original_error(self, mock_client, *_mocks):
        mock_client.side_effect = RuntimeError("cannot build client")
        message = {
            "user_uuid": "u-1",
            "user_username": "alice",
            "project_uuid": "p-1",
            "project_name": "Project",
            "granted": True,
        }

        with self.assertLogs(level="ERROR") as logs:
            handlers.on_user_role_message_stomp(_frame(message), mock.Mock(), "ua")

        self.assertTrue(any("cannot build client" in line for line in logs.output))
