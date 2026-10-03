"""Tests for provider project group events: subscription, routing and coalescing."""

import json
import threading
import unittest
from unittest import mock

import httpx
import stomp.utils
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.backend.backends import AbstractUsernameManagementBackend
from waldur_site_agent.common import structures
from waldur_site_agent.event_processing import event_subscription_manager, handlers
from waldur_site_agent.event_processing.utils import (
    _determine_observable_object_types,
    _register_queue,
)

PROJECT_GROUP = ObservableObjectTypeEnum.SERVICE_PROVIDER_PROJECT_GROUP
_URL = httpx.URL("https://example.com/api/marketplace-site-agent-identities/x/register_queue/")


def _make_offering(**overrides):
    defaults = {
        "name": "test-offering",
        "waldur_offering_uuid": "11111111111111111111111111111111",
        "waldur_api_url": "https://example.com/api/",
        "waldur_api_token": "token",
        "backend_type": "slurm",
        "membership_sync_backend": "",
        "stomp_enabled": True,
    }
    defaults.update(overrides)
    return structures.Offering(**defaults)


class _UsernameBackend(AbstractUsernameManagementBackend):
    """A username backend that writes project groups or not, and nothing else."""

    def __init__(self, writes_project_groups):
        self.writes_project_groups = writes_project_groups

    def generate_username(self, offering_user):
        return ""

    def get_username(self, offering_user):
        return None

    def reconciles_project_groups(self):
        return self.writes_project_groups


def _username_backend(writes_project_groups):
    backend = mock.Mock()
    backend.reconciles_project_groups.return_value = writes_project_groups
    return backend


def _frame(**payload):
    payload.setdefault("object_type", PROJECT_GROUP.value)
    return stomp.utils.Frame(cmd="MESSAGE", headers={}, body=json.dumps(payload))


@mock.patch("waldur_site_agent.event_processing.utils.common_utils.get_username_management_backend")
class TestSubscription(unittest.TestCase):
    def test_subscribes_when_the_username_backend_writes_project_groups(self, get_backend):
        get_backend.return_value = (_UsernameBackend(True), "1.0")
        self.assertIn(PROJECT_GROUP, _determine_observable_object_types(_make_offering()))

    def test_subscribes_alongside_membership_sync(self, get_backend):
        get_backend.return_value = (_UsernameBackend(True), "1.0")
        offering = _make_offering(membership_sync_backend="slurm")
        object_types = _determine_observable_object_types(offering)
        self.assertIn(PROJECT_GROUP, object_types)
        self.assertIn(ObservableObjectTypeEnum.USER_ROLE, object_types)

    def test_no_subscription_without_project_groups(self, get_backend):
        get_backend.return_value = (_UsernameBackend(False), "1.0")
        self.assertNotIn(PROJECT_GROUP, _determine_observable_object_types(_make_offering()))

    def test_no_subscription_when_stomp_membership_is_opted_out(self, get_backend):
        get_backend.return_value = (_UsernameBackend(True), "1.0")
        offering = _make_offering(stomp_membership_sync_enabled=False)
        self.assertNotIn(PROJECT_GROUP, _determine_observable_object_types(offering))

    def test_an_unresolvable_backend_means_no_subscription(self, get_backend):
        get_backend.side_effect = RuntimeError("no such backend")
        self.assertNotIn(PROJECT_GROUP, _determine_observable_object_types(_make_offering()))


class TestQueueRegistration(unittest.TestCase):
    def test_a_server_refusing_project_group_events_keeps_the_other_types(self):
        manager = mock.Mock()
        manager.register_queue.side_effect = [UnexpectedStatus(400, b"unknown choice", _URL), "queue"]
        types = [ObservableObjectTypeEnum.ORDER, PROJECT_GROUP]

        self.assertEqual(_register_queue(manager, "identity", types), "queue")
        self.assertEqual(
            manager.register_queue.call_args_list[-1].args,
            ("identity", [ObservableObjectTypeEnum.ORDER]),
        )

    def test_other_failures_are_not_retried(self):
        for failure in (RuntimeError("down"), UnexpectedStatus(502, b"bad gateway", _URL)):
            manager = mock.Mock()
            manager.register_queue.side_effect = failure
            with self.assertRaises(type(failure)):
                _register_queue(manager, "identity", [ObservableObjectTypeEnum.ORDER, PROJECT_GROUP])
            manager.register_queue.assert_called_once()


class TestRouting(unittest.TestCase):
    def test_project_group_messages_reach_their_handler(self):
        handler = mock.Mock()
        with mock.patch.dict(
            event_subscription_manager.OBJECT_TYPE_TO_HANDLER_STOMP, {PROJECT_GROUP: handler}
        ):
            event_subscription_manager.route_message(
                _frame(action="create", name="proj", gid=20004), _make_offering(), "agent"
            )
        handler.assert_called_once()

    @mock.patch.object(handlers, "schedule_project_group_reconcile")
    def test_handler_schedules_a_pass(self, schedule):
        offering = _make_offering()
        handlers.on_project_group_message_stomp(
            _frame(action="create", name="proj", gid=20004), offering, "agent"
        )
        schedule.assert_called_once_with(offering, "agent")


class TestCoalescing(unittest.TestCase):
    def tearDown(self):
        with handlers._project_group_timers_lock:
            for timer in handlers._project_group_timers.values():
                timer.cancel()
            handlers._project_group_timers.clear()

    def test_a_burst_becomes_one_pending_pass(self):
        offering = _make_offering()
        self.assertTrue(handlers.schedule_project_group_reconcile(offering, "agent", delay=60))
        self.assertFalse(handlers.schedule_project_group_reconcile(offering, "agent", delay=60))
        self.assertFalse(handlers.schedule_project_group_reconcile(offering, "agent", delay=60))

    def test_offerings_are_scheduled_independently(self):
        first = _make_offering()
        second = _make_offering(waldur_offering_uuid="22222222222222222222222222222222")
        self.assertTrue(handlers.schedule_project_group_reconcile(first, "agent", delay=60))
        self.assertTrue(handlers.schedule_project_group_reconcile(second, "agent", delay=60))

    @mock.patch.object(handlers, "register_event_process_service")
    @mock.patch.object(handlers.common_utils, "get_client_for_offering")
    @mock.patch.object(handlers.common_utils, "get_username_management_backend")
    def test_the_pass_runs_once_after_the_delay(self, get_backend, get_client, _register):
        backend = _username_backend(True)
        get_backend.return_value = (backend, "1.0")
        ran = threading.Event()
        backend.reconcile_project_groups.side_effect = lambda client: ran.set()
        offering = _make_offering()

        for _ in range(5):
            handlers.schedule_project_group_reconcile(offering, "agent", delay=0.05)

        self.assertTrue(ran.wait(5))
        backend.reconcile_project_groups.assert_called_once_with(get_client.return_value)

    @mock.patch.object(handlers, "register_event_process_service")
    @mock.patch.object(handlers.common_utils, "get_client_for_offering")
    @mock.patch.object(handlers.common_utils, "get_username_management_backend")
    def test_an_event_during_a_pass_schedules_another(self, get_backend, _client, _register):
        offering = _make_offering()
        scheduled_during_pass = []

        def reconcile(client):
            scheduled_during_pass.append(
                handlers.schedule_project_group_reconcile(offering, "agent", delay=60)
            )

        backend = _username_backend(True)
        backend.reconcile_project_groups.side_effect = reconcile
        get_backend.return_value = (backend, "1.0")
        handlers.schedule_project_group_reconcile(offering, "agent", delay=60)

        handlers.run_project_group_reconcile(offering, "agent")

        self.assertEqual(scheduled_during_pass, [True])

    @mock.patch.object(handlers, "register_event_process_service")
    @mock.patch.object(handlers.common_utils, "get_client_for_offering")
    @mock.patch.object(handlers.common_utils, "get_username_management_backend")
    def test_a_backend_without_project_groups_is_left_alone(self, get_backend, _client, _register):
        backend = _username_backend(False)
        get_backend.return_value = (backend, "1.0")
        handlers.run_project_group_reconcile(_make_offering(), "agent")
        backend.reconcile_project_groups.assert_not_called()

    @mock.patch.object(handlers, "register_event_process_service")
    @mock.patch.object(handlers.common_utils, "get_client_for_offering")
    @mock.patch.object(handlers.common_utils, "get_username_management_backend")
    def test_a_failing_pass_is_logged_not_raised(self, get_backend, _client, _register):
        backend = _username_backend(True)
        backend.reconcile_project_groups.side_effect = RuntimeError("ldap down")
        get_backend.return_value = (backend, "1.0")
        with self.assertLogs(level="ERROR"):
            handlers.run_project_group_reconcile(_make_offering(), "agent")
