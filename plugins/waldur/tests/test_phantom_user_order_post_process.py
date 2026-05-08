from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from waldur_api_client.models.offering_user_state import OfferingUserState

from waldur_site_agent.common import structures
from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent_waldur.backend import WaldurBackend


PROJECT_UUID_ON_B = UUID("aabbccdd-1234-1234-1234-123456789abc")
RESOURCE_UUID = UUID("abcdef01-1234-1234-1234-123456789abc")
GHOST_USER_UUID = UUID("11223344-1234-1234-1234-123456789abc")
SANTORO_USER_UUID = UUID("99999999-1234-1234-1234-123456789abc")

OFFERING_USER_USERNAME = "santnagi"  # Waldur B-assigned local name
USER_CUID = "santoro@idp.example.org"  # Real CUID on Waldur A
OFFERING_UUID = "eeee0000-1111-2222-3333-444455556666"


def _make_offering() -> structures.Offering:
    return structures.Offering(
        name="EuroHPC to Puhuri Federation",
        waldur_offering_uuid=OFFERING_UUID,
        waldur_api_url="https://waldur-a.example.com/api/",
        waldur_api_token="token",
        backend_type="waldur",
        order_processing_backend="waldur",
        membership_sync_backend="waldur",
        backend_settings={
            "target_api_url": "https://waldur-b.example.com/api/",
            "target_api_token": "test-token",
            "target_offering_uuid": "ffff0000-1111-2222-3333-444455556666",
            "target_customer_uuid": "cccc0000-1111-2222-3333-444455556666",
            "user_match_field": "cuid",
            "identity_bridge_source": "isd:efp",
        },
    )


def _make_offering_user(*, username: str, user_username: str, state):
    """Mock OfferingUser with the given offering username, CUID and state."""
    ou = MagicMock()
    ou.username = username
    ou.user_username = user_username
    ou.state = state
    return ou


def _make_waldur_resource() -> MagicMock:
    resource = MagicMock()
    resource.uuid = RESOURCE_UUID
    resource.backend_id = str(RESOURCE_UUID)
    return resource


@pytest.fixture()
def mock_waldur_b_client():
    client = MagicMock()
    client.api_url = "https://waldur-b.example.com/api/"

    # Identity bridge POST is idempotent: it CREATES the user if absent
    # and returns the (new or existing) UUID. With only {username, source}
    # in the payload (no attributes), a new "phantom" user is created.
    client.resolve_user_via_identity_bridge.return_value = GHOST_USER_UUID

    target_resource = MagicMock()
    target_resource.project_uuid = PROJECT_UUID_ON_B
    client.get_marketplace_resource.return_value = target_resource

    return client


@pytest.fixture()
def waldur_backend(mock_waldur_b_client):
    backend_settings = {
        "target_api_url": "https://waldur-b.example.com/api/",
        "target_api_token": "test-token",
        "target_offering_uuid": "ffff0000-1111-2222-3333-444455556666",
        "target_customer_uuid": "cccc0000-1111-2222-3333-444455556666",
        "user_match_field": "cuid",
        "identity_bridge_source": "isd:efp",
    }
    backend = WaldurBackend(backend_settings, {})
    backend.client = mock_waldur_b_client
    return backend


@pytest.fixture()
def order_processor(waldur_backend):
    """Construct an OfferingOrderProcessor without running its full __init__.

    We only need the attributes used by ``_add_users_to_resource``:
        - self.offering (for name / uuid / backend_settings)
        - self.resource_backend (the WaldurBackend instance)

    ``_update_offering_users``, ``_fetch_user_context_for_resource`` and
    ``_get_exposed_fields`` are mocked because they require a live
    Waldur API client we don't need for this regression test. Tests that
    care about user_attributes override the ``_get_exposed_fields`` mock.
    """
    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.offering = _make_offering()
    processor.resource_backend = waldur_backend
    processor.waldur_rest_client = MagicMock()
    processor._get_exposed_fields = MagicMock(return_value=[])
    return processor


def _make_team_member(*, offering_user_username: str, role: str):
    """Mock ProjectUser for a team-list response on Waldur A."""
    member = MagicMock()
    member.offering_user_username = offering_user_username
    member.role = role
    return member


class TestOrderPostProcessThreadsIdentityKwargsThrough:
    """Lock in the post-fix behaviour of ``_add_users_to_resource``.

    Scenario — user 'Santoro Nagishumi' on Waldur A:
      * user.username (CUID)              = 'santoro@idp.example.org'
      * offering_user.user_username       = 'santoro@idp.example.org'
      * offering_user.username (B local)  = 'santnagi'
      * project role on A                 = PROJECT.MEMBER

    After a CREATE order completes, ``_post_process_order`` calls
    ``_add_users_to_resource``. The fixed implementation must thread
    ``user_cuids`` / ``user_attributes`` / ``user_roles`` through to the
    backend so identity bridge resolves the existing user (no phantom)
    and the user keeps their real role.
    """

    def test_identity_bridge_receives_real_cuid_not_offering_username(
        self, order_processor, mock_waldur_b_client
    ):
        """identity bridge must be called with the CUID, not 'santnagi'.

        Regression guard for the original phantom-user bug.
        """
        santoro_ou = _make_offering_user(
            username=OFFERING_USER_USERNAME,
            user_username=USER_CUID,
            state=OfferingUserState.OK,
        )
        team_member = _make_team_member(
            offering_user_username=OFFERING_USER_USERNAME,
            role="PROJECT.MEMBER",
        )
        user_context = {
            "team": [team_member],
            "offering_users": [santoro_ou],
            "user_mappings": {},
            "offering_user_mappings": {},
        }

        with patch.object(
            order_processor, "_update_offering_users", return_value=False
        ), patch.object(
            order_processor, "_fetch_user_context_for_resource", return_value={}
        ):
            order_processor._add_users_to_resource(
                _make_waldur_resource(), user_context
            )

        mock_waldur_b_client.resolve_user_via_identity_bridge.assert_called_once()
        identity_passed_to_bridge = (
            mock_waldur_b_client.resolve_user_via_identity_bridge.call_args.args[0]
        )
        assert identity_passed_to_bridge == USER_CUID, (
            "Expected identity bridge to receive the real CUID "
            f"({USER_CUID!r}); got {identity_passed_to_bridge!r}. If this "
            "is the offering_user_username, the phantom-user bug has "
            "regressed."
        )

    def test_user_attributes_are_forwarded_to_identity_bridge(
        self, order_processor, mock_waldur_b_client
    ):
        """User profile attributes must be forwarded to identity bridge.

        Without attributes, identity bridge creates a bare username-only
        user. The offering's ``OfferingUserAttributeConfig.exposed_fields``
        drives which attributes are forwarded.
        """
        order_processor._get_exposed_fields = MagicMock(
            return_value=["first_name", "last_name", "email"]
        )

        santoro_ou = _make_offering_user(
            username=OFFERING_USER_USERNAME,
            user_username=USER_CUID,
            state=OfferingUserState.OK,
        )
        santoro_ou.user_first_name = "Santoro"
        santoro_ou.user_last_name = "Nagishumi"
        santoro_ou.user_email = "santoro@example.org"

        user_context = {
            "team": [
                _make_team_member(
                    offering_user_username=OFFERING_USER_USERNAME,
                    role="PROJECT.MEMBER",
                )
            ],
            "offering_users": [santoro_ou],
            "user_mappings": {},
            "offering_user_mappings": {},
        }

        with patch.object(
            order_processor, "_update_offering_users", return_value=False
        ), patch.object(
            order_processor, "_fetch_user_context_for_resource", return_value={}
        ):
            order_processor._add_users_to_resource(
                _make_waldur_resource(), user_context
            )

        attributes_kwarg = (
            mock_waldur_b_client.resolve_user_via_identity_bridge.call_args.kwargs.get(
                "attributes"
            )
        )
        assert attributes_kwarg is not None, (
            "Expected user_attributes to be forwarded to identity bridge; "
            "got attributes=None. Without these the resolved/created "
            "user has no first_name / last_name / email."
        )
        assert attributes_kwarg.get("first_name") == "Santoro"
        assert attributes_kwarg.get("last_name") == "Nagishumi"
        assert attributes_kwarg.get("email") == "santoro@example.org"

    def test_user_added_with_real_project_role_not_default_admin(
        self, order_processor, mock_waldur_b_client
    ):
        """add_user_to_project must use the user's real role from the team.

        The default ``DEFAULT_PROJECT_ROLE_NAME = PROJECT.ADMIN`` must
        only be a fallback for users we cannot resolve a role for —
        not the role of every user added during order post-processing.
        """
        santoro_ou = _make_offering_user(
            username=OFFERING_USER_USERNAME,
            user_username=USER_CUID,
            state=OfferingUserState.OK,
        )
        team_member = _make_team_member(
            offering_user_username=OFFERING_USER_USERNAME,
            role="PROJECT.MEMBER",
        )
        user_context = {
            "team": [team_member],
            "offering_users": [santoro_ou],
            "user_mappings": {},
            "offering_user_mappings": {},
        }

        # identity bridge resolves to the real Santoro UUID
        mock_waldur_b_client.resolve_user_via_identity_bridge.return_value = (
            SANTORO_USER_UUID
        )

        with patch.object(
            order_processor, "_update_offering_users", return_value=False
        ), patch.object(
            order_processor, "_fetch_user_context_for_resource", return_value={}
        ):
            order_processor._add_users_to_resource(
                _make_waldur_resource(), user_context
            )

        mock_waldur_b_client.add_user_to_project.assert_called_once()
        call_kwargs = mock_waldur_b_client.add_user_to_project.call_args.kwargs
        assert call_kwargs.get("user_uuid") == SANTORO_USER_UUID
        assert call_kwargs.get("role_name") == "PROJECT.MEMBER", (
            "Expected the user's real Waldur A role (PROJECT.MEMBER) to "
            "be applied; got "
            f"{call_kwargs.get('role_name')!r}. If this is "
            "'PROJECT.ADMIN' the user_roles kwarg is no longer being "
            "threaded through."
        )

    def test_only_OK_offering_users_are_processed(
        self, order_processor, mock_waldur_b_client
    ):
        """REQUESTED/CREATING offering users must not be added.

        Sanity check: only OK offering users (those with usernames
        assigned by Waldur B) are passed to the backend.
        """
        requested_ou = _make_offering_user(
            username="some-other-user",
            user_username="other@idp.example.org",
            state=OfferingUserState.REQUESTED,
        )
        user_context = {
            "team": [],
            "offering_users": [requested_ou],
            "user_mappings": {},
            "offering_user_mappings": {},
        }

        with patch.object(
            order_processor, "_update_offering_users", return_value=False
        ), patch.object(
            order_processor, "_fetch_user_context_for_resource", return_value={}
        ):
            order_processor._add_users_to_resource(
                _make_waldur_resource(), user_context
            )

        mock_waldur_b_client.resolve_user_via_identity_bridge.assert_not_called()
        mock_waldur_b_client.add_user_to_project.assert_not_called()


class TestMembershipSyncPathIsCorrect:
    """Control test: the membership-sync path passes user_cuids/user_roles.

    Demonstrates that the same backend method behaves correctly when the
    caller (``OfferingMembershipProcessor._sync_resource_users``) passes
    the proper kwargs. This isolates the bug to the post-process-order
    path and confirms the backend itself is fine.
    """

    def test_membership_sync_uses_cuid_and_real_role(
        self, waldur_backend, mock_waldur_b_client
    ):
        """When user_cuids and user_roles are passed, identity bridge gets the
        CUID and the user's real project role is used."""
        mock_waldur_b_client.resolve_user_via_identity_bridge.return_value = (
            SANTORO_USER_UUID
        )

        waldur_backend.add_users_to_resource(
            _make_waldur_resource(),
            {OFFERING_USER_USERNAME},
            user_cuids={OFFERING_USER_USERNAME: USER_CUID},
            user_roles={OFFERING_USER_USERNAME: "PROJECT.MEMBER"},
            user_attributes={
                OFFERING_USER_USERNAME: {
                    "first_name": "Santoro",
                    "last_name": "Nagishumi",
                    "email": "santoro@example.org",
                }
            },
        )

        positional_args = (
            mock_waldur_b_client.resolve_user_via_identity_bridge.call_args.args
        )
        assert positional_args[0] == USER_CUID, (
            "Membership-sync path correctly resolves via CUID — proves "
            "the backend itself is fine; the bug is the caller."
        )

        call_kwargs = mock_waldur_b_client.add_user_to_project.call_args.kwargs
        assert call_kwargs.get("role_name") == "PROJECT.MEMBER"
        assert call_kwargs.get("user_uuid") == SANTORO_USER_UUID
