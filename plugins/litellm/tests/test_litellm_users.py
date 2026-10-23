"""Tests for per-person provisioning: LiteLLM users and Open WebUI accounts."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest
from waldur_site_agent_litellm import ownership
from waldur_site_agent_litellm.backend import LiteLLMBackend
from waldur_site_agent_litellm.client import META_OWNERSHIP, META_RESOURCE, META_USERNAME
from waldur_site_agent_litellm.openwebui import ROLE_ACTIVE, ROLE_DISABLED

from waldur_site_agent.backend.exceptions import BackendError

COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
}
SETTINGS = {
    "api_url": "https://litellm.example.com/",
    "api_token": "sk-master",
    "openwebui": {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "url": "https://chat.example.com",
        "account_provisioning": "managed_password",
        "initial_password": "ChangeMe-123",
    },
}
RID = "abc123"
EMAIL = "ada@example.com"
USERNAME = "ada"
TODAY = "2026-10-15"


@pytest.fixture(autouse=True)
def _today():
    with mock.patch.object(ownership, "today", return_value=TODAY):
        yield


def _make_backend(settings: Optional[dict] = None) -> LiteLLMBackend:
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
    ):
        backend = LiteLLMBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = []
    if backend.openwebui_client is not None:
        backend.openwebui_client = mock.MagicMock()
        backend.openwebui_client.find_user.return_value = None
    return backend


def _make_resource(backend_id: str = RID) -> SimpleNamespace:
    return SimpleNamespace(
        uuid=uuid.uuid4(), name="Inference", slug="inference", backend_id=backend_id, limits=None
    )


def _managed_user(email: str = EMAIL, username: str = USERNAME, resource: str = RID) -> dict:
    """A record as written before ownership had dates: the current owner, nothing else."""
    return {
        "user_id": email,
        "user_email": email,
        "metadata": {META_RESOURCE: resource, META_USERNAME: username},
    }


def _period(resource: str, start: Optional[str], end: Optional[str]) -> dict:
    return {"resource": resource, "from": start, "to": end}


def _owned_user(*periods: dict, email: str = EMAIL, username: str = USERNAME) -> dict:
    """A record carrying dated ownership periods, oldest first."""
    current = next((p["resource"] for p in periods if p["to"] is None), None)
    return {
        "user_id": email,
        "user_email": email,
        "metadata": {
            META_RESOURCE: current,
            META_USERNAME: username,
            META_OWNERSHIP: list(periods),
        },
    }


def _written_metadata(backend: LiteLLMBackend) -> dict:
    """The metadata of the last ``/user/update`` the backend sent."""
    _, fields = backend.litellm_client.update_user.call_args[0]
    return fields["metadata"]


def _add(backend: LiteLLMBackend, **kwargs) -> set:
    return backend.add_users_to_resource(
        _make_resource(),
        {USERNAME},
        user_emails={USERNAME: EMAIL},
        user_attributes={USERNAME: {"full_name": "Ada Lovelace"}},
        **kwargs,
    )


# --- provisioning -----------------------------------------------------------


def test_a_new_member_gets_a_litellm_user_keyed_by_their_email():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    assert _add(backend) == {USERNAME}

    backend.litellm_client.create_user.assert_called_once()
    args, kwargs = backend.litellm_client.create_user.call_args
    assert args[0] == EMAIL, "the user_id must be the address the proxy sees in the header"
    assert kwargs["user_email"] == EMAIL
    assert kwargs["metadata"] == {
        META_RESOURCE: RID,
        META_USERNAME: USERNAME,
        META_OWNERSHIP: [_period(RID, TODAY, None)],
    }


def test_the_address_is_normalized_so_a_capitalized_email_is_one_user_not_two():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    backend.add_users_to_resource(
        _make_resource(), {USERNAME}, user_emails={USERNAME: "Ada@Example.COM "}
    )

    assert backend.litellm_client.create_user.call_args[0][0] == EMAIL


def test_a_member_with_no_email_is_skipped_rather_than_provisioned_blind():
    backend = _make_backend()

    added = backend.add_users_to_resource(_make_resource(), {USERNAME}, user_emails={})

    assert added == set()
    backend.litellm_client.create_user.assert_not_called()


def test_a_user_owned_by_another_resource_is_refused_not_stolen():
    # The load-bearing per-person constraint: an address bills to exactly one resource,
    # so moving it would silently re-point the other resource's chat billing.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _managed_user(resource="other-resource")

    assert _add(backend) == set()
    backend.litellm_client.create_user.assert_not_called()
    backend.litellm_client.update_user.assert_not_called()


def test_an_unowned_user_is_adopted_so_a_hand_made_account_does_not_block_provisioning():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {"user_id": EMAIL, "metadata": {}}

    assert _add(backend) == {USERNAME}

    # From today: whatever the address used before Waldur managed it is not this
    # resource's to pay for.
    assert _written_metadata(backend) == {
        META_RESOURCE: RID,
        META_USERNAME: USERNAME,
        META_OWNERSHIP: [_period(RID, TODAY, None)],
    }


def test_the_offering_username_is_restamped_so_the_membership_diff_cannot_go_stale():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _managed_user(username="old-name")

    _add(backend)

    _, fields = backend.litellm_client.update_user.call_args[0]
    assert fields["metadata"][META_USERNAME] == USERNAME


# --- the chat surface -------------------------------------------------------


def test_managed_password_mode_creates_the_chat_account():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    _add(backend)

    backend.openwebui_client.create_user.assert_called_once_with(
        EMAIL, "Ada Lovelace", "ChangeMe-123", role=ROLE_ACTIVE
    )


def test_sso_mode_creates_no_account_because_the_idp_does_it_on_first_login():
    settings = dict(SETTINGS)
    settings["openwebui"] = {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "account_provisioning": "sso",
    }
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = None

    assert _add(backend) == {USERNAME}
    backend.openwebui_client.create_user.assert_not_called()


def test_a_previously_disabled_account_is_re_enabled_on_re_add():
    # An SSO login into a demoted account does not restore it, so the re-enable has to
    # happen here even in the mode that never creates anything.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_DISABLED}

    _add(backend)

    backend.openwebui_client.set_role.assert_called_once_with(
        {"id": "owui-1", "role": ROLE_DISABLED}, ROLE_ACTIVE
    )


def test_managed_password_without_a_password_is_refused_at_construction():
    settings = dict(SETTINGS)
    settings["openwebui"] = {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "account_provisioning": "managed_password",
    }
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
        pytest.raises(BackendError, match="initial_password"),
    ):
        LiteLLMBackend(settings, dict(COMPONENTS))


def test_an_api_only_offering_never_touches_open_webui():
    backend = _make_backend({"api_url": "https://litellm.example.com", "api_token": "sk-master"})
    assert backend.openwebui_client is None
    backend.litellm_client.get_user.return_value = None

    assert _add(backend) == {USERNAME}


# --- membership diff --------------------------------------------------------


def test_pull_reports_members_by_offering_username_which_is_what_waldur_diffs_on():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [
        _managed_user(),
        _managed_user("bob@example.com", "bob", resource="another"),
        {"user_id": "unmanaged@example.com", "metadata": {}},
    ]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]

    info = backend._pull_backend_resource(RID)

    assert info.users == [USERNAME]


def test_a_resource_with_no_members_is_still_a_resource():
    # Existence is decided by the keys; reporting it as missing would re-provision it.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]

    assert backend._pull_backend_resource(RID).users == []


# --- removal ----------------------------------------------------------------


def test_removal_revokes_chat_access_and_releases_the_billing_record():
    # Releasing only the LiteLLM user leaves the person chatting through the shared key.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.get_user.return_value = _managed_user()
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_ACTIVE}

    removed = backend.remove_users_from_resource(_make_resource(), {USERNAME})

    assert removed == [USERNAME]
    backend.openwebui_client.set_role.assert_called_once_with(
        {"id": "owui-1", "role": ROLE_ACTIVE}, ROLE_DISABLED
    )
    # Kept, with the period closed today: deleting it took the chat usage the person
    # already had this month off the resource's bill. A record from before ownership had
    # dates keeps its open start, so every earlier day still bills here.
    backend.litellm_client.delete_users.assert_not_called()
    assert _written_metadata(backend)[META_OWNERSHIP] == [_period(RID, None, TODAY)]
    assert _written_metadata(backend)[META_RESOURCE] is None


def test_removal_can_delete_the_account_when_the_offering_asks_for_it():
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], delete_accounts_on_removal=True)
    backend = _make_backend(settings)
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_ACTIVE}

    backend.remove_users_from_resource(_make_resource(), {USERNAME})

    backend.openwebui_client.delete_user.assert_called_once_with("owui-1")


def test_removing_someone_already_gone_reports_success_so_it_is_not_retried_forever():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = []

    assert backend.remove_users_from_resource(_make_resource(), {USERNAME}) == [USERNAME]


def test_terminating_a_resource_removes_its_members_before_its_keys():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.get_user.return_value = _managed_user()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    calls = []
    backend.litellm_client.update_user.side_effect = lambda *args: calls.append("users")
    backend.litellm_client.delete_keys.side_effect = lambda tokens: calls.append("keys")

    backend.delete_resource(_make_resource())

    assert calls == ["users", "keys"], (
        "a member left behind after the keys are gone can still chat, and their spend "
        "accrues against a resource Waldur has terminated"
    )


# --- pause / restore --------------------------------------------------------


def test_pausing_also_closes_the_chat_surface():
    # Blocking the keys only closes the API surface: chat runs on Open WebUI's shared
    # key, which this plugin neither owns nor may block.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.block.return_value = True
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_ACTIVE}

    assert backend.pause_resource(RID) is True

    backend.openwebui_client.set_role.assert_called_once_with(
        {"id": "owui-1", "role": ROLE_ACTIVE}, ROLE_DISABLED
    )
    backend.litellm_client.block.assert_called_once_with("h1")


def test_a_pause_that_could_not_close_the_chat_surface_is_not_reported_as_paused():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.block.return_value = True
    backend.openwebui_client.find_user.side_effect = BackendError("chat is down")

    assert backend.pause_resource(RID) is False


def test_restoring_re_enables_the_chat_account():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.unblock.return_value = True
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_DISABLED}

    assert backend.restore_resource(RID) is True

    backend.openwebui_client.set_role.assert_called_once_with(
        {"id": "owui-1", "role": ROLE_DISABLED}, ROLE_ACTIVE
    )


# --- portal surface ---------------------------------------------------------


def test_the_chat_url_is_surfaced_on_the_resource_only_when_one_is_configured():
    backend = _make_backend()
    endpoints = backend.create_resource_with_id(_make_resource(""), RID).endpoints
    assert {"name": "Chat", "url": "https://chat.example.com"} in endpoints

    api_only = _make_backend({"api_url": "https://litellm.example.com", "api_token": "sk"})
    names = [e["name"] for e in api_only.create_resource_with_id(_make_resource(""), RID).endpoints]
    assert names == ["OpenAI API"]


# --- pause is not removal ----------------------------------------------------


def test_pausing_never_deletes_the_account_even_when_removal_would():
    # A pause is temporary -- an unpaid invoice, an exhausted quota. Deleting the
    # account there destroys the person's conversations, and under SSO restore creates
    # nothing back, so the loss would be permanent.
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], delete_accounts_on_removal=True)
    backend = _make_backend(settings)
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.block.return_value = True
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_ACTIVE}

    assert backend.pause_resource(RID) is True

    backend.openwebui_client.delete_user.assert_not_called()
    backend.openwebui_client.set_role.assert_called_once_with(
        {"id": "owui-1", "role": ROLE_ACTIVE}, ROLE_DISABLED
    )


def test_restoring_does_not_create_an_account_that_was_never_there():
    # Restore knows the offering username, not the person's name, and an account that
    # is absent was not paused by this plugin -- there is nothing to restore.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.unblock.return_value = True
    backend.openwebui_client.find_user.return_value = None

    assert backend.restore_resource(RID) is True

    backend.openwebui_client.create_user.assert_not_called()


# --- a failing user listing must not corrupt the paths above ----------------


def test_email_is_taken_from_user_attributes_when_the_caller_passes_no_emails():
    # The order processor and the account syncs pass addresses in user_attributes only.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    added = backend.add_users_to_resource(
        _make_resource(),
        {USERNAME},
        user_attributes={USERNAME: {"full_name": "Ada Lovelace", "email": EMAIL}},
    )

    assert added == {USERNAME}
    assert backend.litellm_client.create_user.call_args.args[0] == EMAIL


def test_a_failed_user_listing_reports_the_resource_as_existing_not_missing():
    # Otherwise the order processor re-creates a live resource and mints a second key set.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.list_users.side_effect = BackendError("user list is down")

    info = backend._pull_backend_resource(RID)

    assert info is not None
    assert info.backend_id == RID
    assert info.users == []


def test_a_failed_user_listing_still_deletes_every_key_on_terminate():
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.list_users.side_effect = BackendError("user list is down")

    backend.delete_resource(_make_resource())

    backend.litellm_client.delete_keys.assert_called_once_with(["h1"])


def test_a_failed_user_listing_still_blocks_every_key_on_pause():
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.list_users.side_effect = BackendError("user list is down")
    backend.litellm_client.block.return_value = True

    # False, because chat access could not be revoked -- but the key is blocked.
    assert backend.pause_resource(RID) is False
    backend.litellm_client.block.assert_called_once_with("h1")


def test_an_admin_chat_account_is_never_demoted():
    backend = _make_backend()
    backend.openwebui_client.find_user.return_value = {"id": "u1", "role": "admin"}

    backend._revoke_chat_access(EMAIL)

    backend.openwebui_client.set_role.assert_not_called()
    backend.openwebui_client.delete_user.assert_not_called()


# --- dated ownership --------------------------------------------------------
#
# A pass recomputes the whole month, so a removed member's record has to survive with
# the days it owned, or their chat usage leaves the resource's bill.


def test_a_removed_member_is_no_longer_listed_so_the_diff_does_not_remove_them_again():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [
        _owned_user(_period(RID, "2026-10-01", "2026-10-14"))
    ]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]

    assert backend._pull_backend_resource(RID).users == []


def test_pause_does_not_touch_a_member_whose_period_has_closed():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [
        _owned_user(_period(RID, "2026-10-01", "2026-10-14"))
    ]
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.block.return_value = True

    assert backend.pause_resource(RID) is True
    backend.openwebui_client.find_user.assert_not_called()


def test_terminating_a_resource_releases_its_members_rather_than_deleting_them():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [
        _owned_user(_period(RID, "2026-10-01", None))
    ]
    backend.litellm_client.get_user.return_value = _owned_user(_period(RID, "2026-10-01", None))
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]

    backend.delete_resource(_make_resource())

    backend.litellm_client.delete_users.assert_not_called()
    assert _written_metadata(backend)[META_OWNERSHIP] == [_period(RID, "2026-10-01", TODAY)]


def test_another_resource_can_claim_an_address_once_the_first_has_released_it():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _owned_user(
        _period("other", None, "2026-10-14")
    )

    assert _add(backend) == {USERNAME}

    # The closed period stays, so the days before the move still bill to "other".
    assert _written_metadata(backend)[META_OWNERSHIP] == [
        _period("other", None, "2026-10-14"),
        _period(RID, TODAY, None),
    ]
    assert _written_metadata(backend)[META_RESOURCE] == RID


def test_an_address_another_resource_still_holds_open_is_refused():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _owned_user(
        _period(RID, None, "2026-09-30"), _period("other", "2026-10-01", None)
    )

    assert _add(backend) == set()
    backend.litellm_client.update_user.assert_not_called()


def test_a_record_from_before_dated_ownership_keeps_its_open_start_on_re_add():
    # Read as one open period with no start; writing it back with a start would take
    # every earlier day of the month off the resource.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _managed_user()

    assert _add(backend) == {USERNAME}

    assert _written_metadata(backend)[META_OWNERSHIP] == [_period(RID, None, None)]


def test_re_adding_a_removed_member_opens_a_new_period_on_the_same_resource():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _owned_user(
        _period(RID, "2026-10-01", "2026-10-05")
    )

    assert _add(backend) == {USERNAME}

    assert _written_metadata(backend)[META_OWNERSHIP] == [
        _period(RID, "2026-10-01", "2026-10-05"),
        _period(RID, TODAY, None),
    ]


def test_the_record_keeps_metadata_keys_this_plugin_does_not_own():
    # /user/update replaces metadata wholesale, so anything not carried over is lost.
    backend = _make_backend()
    record = _managed_user()
    record["metadata"]["note"] = "set by an admin"
    backend.litellm_client.list_users.return_value = [record]
    backend.litellm_client.get_user.return_value = record

    backend.remove_users_from_resource(_make_resource(), {USERNAME})

    assert _written_metadata(backend)["note"] == "set by an admin"


def test_releasing_an_address_this_resource_no_longer_holds_writes_nothing():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_managed_user()]
    # Moved on between the listing and the removal.
    backend.litellm_client.get_user.return_value = _owned_user(
        _period(RID, None, "2026-10-10"), _period("other", "2026-10-11", None)
    )

    assert backend.remove_users_from_resource(_make_resource(), {USERNAME}) == [USERNAME]
    backend.litellm_client.update_user.assert_not_called()


def test_periods_older_than_the_retention_window_are_trimmed_on_write():
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=2))
    backend.litellm_client.get_user.return_value = _owned_user(
        _period(RID, "2026-07-01", "2026-08-31"), _period(RID, "2026-09-01", "2026-09-10")
    )

    _add(backend)

    # The window is September and October: August can no longer be reported.
    assert _written_metadata(backend)[META_OWNERSHIP] == [
        _period(RID, "2026-09-01", "2026-09-10"),
        _period(RID, TODAY, None),
    ]


# --- pruning released records -----------------------------------------------


def test_a_released_record_is_deleted_once_no_reportable_month_needs_it():
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=2))
    expired = _owned_user(_period(RID, None, "2026-08-31"), email="gone@example.com")
    recent = _owned_user(_period(RID, None, "2026-09-01"), email="recent@example.com")
    hand_made = {"user_id": "admin@example.com", "metadata": {}}
    current = _owned_user(_period(RID, None, None))
    backend.litellm_client.list_users.return_value = [expired, recent, hand_made, current]
    backend.litellm_client.get_user.side_effect = lambda email: {
        "gone@example.com": expired
    }.get(email)

    users = backend._managed_users()

    backend.litellm_client.delete_users.assert_called_once_with(["gone@example.com"])
    assert sorted(users) == [EMAIL, "admin@example.com", "recent@example.com"]


def test_the_default_window_keeps_a_released_record_for_a_year():
    backend = _make_backend()
    kept = _owned_user(_period(RID, None, "2025-11-01"), email="kept@example.com")
    gone = _owned_user(_period(RID, None, "2025-10-31"), email="gone@example.com")
    backend.litellm_client.list_users.return_value = [kept, gone]
    backend.litellm_client.get_user.side_effect = lambda email: {
        "kept@example.com": kept,
        "gone@example.com": gone,
    }[email]

    backend._managed_users()

    backend.litellm_client.delete_users.assert_called_once_with(["gone@example.com"])


def test_an_address_re_claimed_since_the_listing_is_not_pruned():
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=1))
    backend.litellm_client.list_users.return_value = [_owned_user(_period(RID, None, "2026-08-31"))]
    backend.litellm_client.get_user.return_value = _owned_user(
        _period(RID, None, "2026-08-31"), _period("other", TODAY, None)
    )

    users = backend._managed_users()

    backend.litellm_client.delete_users.assert_not_called()
    assert EMAIL in users


def test_a_failed_prune_does_not_fail_the_listing():
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=1))
    record = _owned_user(_period(RID, None, "2026-08-31"))
    backend.litellm_client.list_users.return_value = [record]
    backend.litellm_client.get_user.return_value = record
    backend.litellm_client.delete_users.side_effect = BackendError("proxy said no")

    assert EMAIL in backend._managed_users()


def test_a_retention_window_shorter_than_the_current_month_is_refused():
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
        pytest.raises(BackendError, match="ownership_retention_months"),
    ):
        LiteLLMBackend(dict(SETTINGS, ownership_retention_months=0), dict(COMPONENTS))
