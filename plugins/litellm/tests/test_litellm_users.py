"""Tests for per-person provisioning: LiteLLM users and Open WebUI accounts."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest import mock

import pytest
from waldur_api_client.models.resource_state import ResourceState
from waldur_site_agent_litellm import chat_accounts, ownership
from waldur_site_agent_litellm.backend import LiteLLMBackend
from waldur_site_agent_litellm.client import (
    META_CHAT_ACCOUNT,
    META_CREATED,
    META_OWNERSHIP,
    META_RESOURCE,
    META_USERNAME,
)
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
        "password_secret": "s3cret-never-shown",
    },
}
RID = "abc123"
EMAIL = "ada@example.com"
USERNAME = "ada"
TODAY = "2026-10-15"
NEW_ACCOUNT = "owui-new"


@pytest.fixture(autouse=True)
def _today():
    with mock.patch.object(ownership, "today", return_value=TODAY):
        yield


def _make_backend(settings: dict | None = None) -> LiteLLMBackend:
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
    ):
        backend = LiteLLMBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = []
    # A user the backend creates or updates reads back as written -- the chat account is
    # recorded on it right after; an address never written answers with
    # ``get_user.return_value``, which each test sets.
    written: dict = {}

    def create_user(user_id: str, **fields) -> dict:
        written[user_id] = {"user_id": user_id, "metadata": dict(fields.get("metadata") or {})}
        return written[user_id]

    def update_user(user_id: str, fields: dict) -> None:
        current = written.get(user_id) or backend.litellm_client.get_user.return_value
        current = current if isinstance(current, dict) else {"user_id": user_id}
        written[user_id] = {**current, **fields}

    backend.litellm_client.create_user.side_effect = create_user
    backend.litellm_client.update_user.side_effect = update_user
    backend.litellm_client.get_user.side_effect = lambda user_id: written.get(
        user_id, mock.DEFAULT
    )
    if backend.openwebui_client is not None:
        backend.openwebui_client = mock.MagicMock()
        backend.openwebui_client.find_user.return_value = None
        backend.openwebui_client.create_user.side_effect = (
            lambda email, name, password, role: {"id": NEW_ACCOUNT, "email": email, "role": role}
        )
    return backend


def _sso_settings() -> dict:
    settings = dict(SETTINGS)
    settings["openwebui"] = {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "account_provisioning": "sso",
    }
    return settings


def _make_resource(backend_id: str = RID) -> SimpleNamespace:
    return SimpleNamespace(
        uuid=uuid.uuid4(), name="Inference", slug="inference", backend_id=backend_id, limits=None
    )


def _managed_user(
    email: str = EMAIL,
    username: str = USERNAME,
    resource: str = RID,
    account: str | None = None,
) -> dict:
    """A record as written before ownership had dates: the current owner, nothing else.

    ``account`` is the Open WebUI account recorded as the person's, if any.
    """
    metadata = {META_RESOURCE: resource, META_USERNAME: username}
    if account is not None:
        metadata[META_CHAT_ACCOUNT] = account
    return {"user_id": email, "user_email": email, "metadata": metadata}


def _period(resource: str, start: str | None, end: str | None) -> dict:
    return {"resource": resource, "from": start, "to": end}


def _owned_user(*periods: dict, email: str = EMAIL, username: str = USERNAME) -> dict:
    """A record the plugin created, carrying dated ownership periods, oldest first."""
    current = next((p["resource"] for p in periods if p["to"] is None), None)
    return {
        "user_id": email,
        "user_email": email,
        "metadata": {
            META_CREATED: True,
            META_RESOURCE: current,
            META_USERNAME: username,
            META_OWNERSHIP: list(periods),
        },
    }


def _written_metadata(backend: LiteLLMBackend) -> dict:
    """The metadata of the last ``/user/update``, less the chat account recorded on it."""
    _, fields = backend.litellm_client.update_user.call_args[0]
    return {k: v for k, v in fields["metadata"].items() if k != META_CHAT_ACCOUNT}


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
        META_CREATED: True,
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


def test_a_user_waldur_never_stamped_is_refused_not_adopted():
    # A hand-made user, adopted, was released with the member and deleted once the
    # retention window passed: the plugin destroyed a record it never made.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {"user_id": EMAIL, "metadata": {}}

    assert _add(backend) == set()

    backend.litellm_client.create_user.assert_not_called()
    backend.litellm_client.update_user.assert_not_called()
    backend.openwebui_client.find_user.assert_not_called()


def test_the_offering_username_is_restamped_so_the_membership_diff_cannot_go_stale():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _managed_user(username="old-name")

    _add(backend)

    assert _written_metadata(backend)[META_USERNAME] == USERNAME


# --- the chat surface -------------------------------------------------------


def test_managed_password_mode_creates_the_chat_account_with_the_persons_own_password():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    _add(backend)

    backend.openwebui_client.create_user.assert_called_once_with(
        EMAIL, "Ada Lovelace", chat_accounts.derived_password("s3cret-never-shown", EMAIL),
        role=ROLE_ACTIVE,
    )
    # Recorded as the person's, so it is the one account the plugin later acts on.
    assert backend.litellm_client.get_user(EMAIL)["metadata"][META_CHAT_ACCOUNT] == NEW_ACCOUNT


def test_no_two_members_get_the_same_chat_password_and_none_gets_the_shared_secret():
    # The shared initial password let any member sign in as any other who had not
    # changed it.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    backend.add_users_to_resource(
        _make_resource(),
        {"ada", "bob"},
        user_emails={"ada": EMAIL, "bob": "bob@example.com"},
    )

    passwords = [call.args[2] for call in backend.openwebui_client.create_user.call_args_list]
    assert len(passwords) == 2
    assert len(set(passwords)) == 2
    assert "s3cret-never-shown" not in passwords


def test_managed_password_still_set_up_with_only_the_old_shared_password_is_refused():
    # Every member was told initial_password and the derivation is public: keyed with
    # it, the derived passwords would be ones any member could compute.
    settings = dict(SETTINGS)
    settings["openwebui"] = {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "account_provisioning": "managed_password",
        "initial_password": "ChangeMe-123",
    }
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
        pytest.raises(BackendError, match="password_secret"),
    ):
        LiteLLMBackend(settings, dict(COMPONENTS))


def test_a_chat_account_that_cannot_be_recorded_is_deleted_again():
    # Unrecorded, it would read as someone else's account from then on and the person
    # could never be given access; deleting it lets the next pass create it afresh.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None
    backend.litellm_client.update_user.side_effect = BackendError("proxy is down")

    assert _add(backend) == set()

    backend.openwebui_client.delete_user.assert_called_once_with(NEW_ACCOUNT)


def test_a_failed_chat_half_releases_the_claim_so_the_next_pass_adds_the_member_again():
    # Kept, the open period would make the membership diff count the member as
    # provisioned, and nothing would ever create their chat account.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None
    backend.openwebui_client.create_user.side_effect = BackendError("chat is down")

    assert _add(backend) == set()

    # Created by this add and left with nothing: deleted, not kept with no ownership,
    # which the next add would refuse as hand-made.
    backend.litellm_client.delete_users.assert_called_once_with([EMAIL])


def test_failed_adds_on_a_released_record_add_no_period_per_retry():
    # A pass every few minutes for the length of an outage would otherwise pile up a
    # closed period each time, every one of them sent back on every metadata write.
    backend = _make_backend()
    released = _owned_user(_period(RID, "2026-09-01", "2026-09-30"))
    backend.litellm_client.get_user.return_value = released
    backend.openwebui_client.create_user.side_effect = BackendError("chat is down")

    for _ in range(3):
        assert _add(backend) == set()

    record = backend.litellm_client.get_user(EMAIL)
    assert ownership.periods_of(record["metadata"]) == [
        _period(RID, "2026-09-01", "2026-09-30")
    ]
    backend.litellm_client.delete_users.assert_not_called()


def test_a_failed_chat_half_keeps_a_member_the_resource_already_held():
    # The add did not open their period, so it must not close it: they stay a member.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _managed_user(account="owui-1")
    backend.openwebui_client.find_user.side_effect = BackendError("chat is down")

    assert _add(backend) == set()

    record = backend.litellm_client.get_user(EMAIL)
    assert ownership.current_owner(ownership.periods_of(record["metadata"])) == RID


def test_a_pending_account_someone_registered_under_the_address_is_not_promoted():
    # With ENABLE_SIGNUP on, anyone can register a member's address before the member
    # is provisioned; promoting that account handed them the member's entitlement.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None
    squatter = {"id": "owui-squat", "email": EMAIL, "role": ROLE_DISABLED, "created_at": 1}
    backend.openwebui_client.find_user.return_value = squatter

    # Not reported as added: the membership diff then retries on the next pass, which
    # is what gives the member chat once the squatter's account is dealt with.
    assert _add(backend) == set()

    backend.openwebui_client.set_role.assert_not_called()
    backend.openwebui_client.create_user.assert_not_called()
    backend.litellm_client.delete_users.assert_called_once_with([EMAIL])


def test_an_account_the_identity_provider_created_is_bound_and_promoted():
    backend = _make_backend(_sso_settings())
    backend.litellm_client.get_user.return_value = None
    account = {"id": "owui-sso", "email": EMAIL, "role": ROLE_DISABLED,
               "oauth": {"oidc": {"sub": "f00"}}}
    backend.openwebui_client.find_user.return_value = account

    _add(backend)

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)
    assert backend.litellm_client.get_user(EMAIL)["metadata"][META_CHAT_ACCOUNT] == "owui-sso"


def test_a_new_sso_account_is_bound_after_the_old_one_was_deleted():
    # delete_accounts_on_removal took the old account; the person's next login made a
    # new one, which is theirs as much as the first.
    backend = _make_backend(_sso_settings())
    backend.litellm_client.get_user.return_value = _managed_user(account="owui-old")
    account = {"id": "owui-sso", "email": EMAIL, "role": ROLE_DISABLED, "oauth_sub": "oidc@f00"}
    backend.openwebui_client.find_user.return_value = account

    _add(backend)

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)


# Trusted-header and LDAP sign-in: the account carries no oauth, oauth_sub or scim.
TRUSTED_HEADER_ACCOUNT = {"id": "owui-hdr", "email": EMAIL, "created_at": 1_900_000_000}


def _upper_case_sso_settings() -> dict:
    # Not a mode name the plugin knows; like every other check, it reads as sso.
    settings = _sso_settings()
    settings["openwebui"] = dict(settings["openwebui"], account_provisioning="SSO")
    return settings


@pytest.mark.parametrize(
    ("settings", "promoted"),
    [(_sso_settings(), True), (_upper_case_sso_settings(), True), (SETTINGS, False)],
    ids=["sso", "unknown-mode", "managed_password"],
)
def test_an_account_with_no_idp_mark_is_the_members_only_under_sso(settings, promoted):
    # Under sso the IdP makes every account (signup is off), and one from a trusted
    # header or LDAP has nothing to show it; under managed_password the same account
    # could be a squatter's, so it is refused.
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = _managed_user()
    account = dict(TRUSTED_HEADER_ACCOUNT, role=ROLE_DISABLED)
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == ({USERNAME} if promoted else set())

    assert backend.openwebui_client.set_role.called is promoted
    recorded = backend.litellm_client.get_user(EMAIL)
    assert (recorded["metadata"].get(META_CHAT_ACCOUNT) == "owui-hdr") is promoted


def test_an_sso_member_re_added_gets_their_demoted_trusted_header_account_back():
    # Added before their first sign-in, so nothing was recorded; the account their
    # sign-in then created was demoted on removal, and the re-add must promote it.
    backend = _make_backend(_sso_settings())
    backend.litellm_client.get_user.return_value = None
    assert _add(backend) == {USERNAME}
    account = dict(TRUSTED_HEADER_ACCOUNT, role=ROLE_DISABLED)
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == {USERNAME}

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)
    backend.litellm_client.delete_users.assert_not_called()


def test_a_new_trusted_header_account_is_bound_after_the_recorded_one_was_deleted():
    # delete_accounts_on_removal took the recorded account; the next sign-in made a new
    # one with no IdP mark, which under sso is the member's as much as the first.
    backend = _make_backend(_sso_settings())
    backend.litellm_client.get_user.return_value = _managed_user(account="owui-old")
    account = dict(TRUSTED_HEADER_ACCOUNT, role=ROLE_DISABLED)
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == {USERNAME}

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)
    assert backend.litellm_client.get_user(EMAIL)["metadata"][META_CHAT_ACCOUNT] == "owui-hdr"


def test_restoring_an_sso_resource_re_enables_an_unrecorded_trusted_header_account():
    backend = _make_backend(_sso_settings())
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.get_user.return_value = _managed_user()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.unblock.return_value = True
    account = dict(TRUSTED_HEADER_ACCOUNT, role=ROLE_DISABLED)
    backend.openwebui_client.find_user.return_value = account

    assert backend.restore_resource(RID) is True

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)


def test_sso_removal_deletes_an_unrecorded_trusted_header_account_when_asked():
    settings = _sso_settings()
    settings["openwebui"] = dict(settings["openwebui"], delete_accounts_on_removal=True)
    backend = _make_backend(settings)
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.get_user.return_value = _managed_user()
    backend.openwebui_client.find_user.return_value = dict(TRUSTED_HEADER_ACCOUNT, role=ROLE_ACTIVE)

    assert backend.remove_users_from_resource(_make_resource(), {USERNAME}) == [USERNAME]

    backend.openwebui_client.delete_user.assert_called_once_with("owui-hdr")


@pytest.mark.parametrize(
    ("created_at", "promoted"),
    [
        (1_700_000_000, True),  # 2023: made by the plugin before it recorded accounts
        (1_800_000_000, False),  # 2027: made after the upgrade, by someone else
    ],
)
def test_the_cutoff_binds_only_accounts_older_than_the_upgrade(created_at, promoted):
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], bind_accounts_created_before="2026-11-14")
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = _managed_user()
    account = {"id": "owui-1", "email": EMAIL, "role": ROLE_DISABLED, "created_at": created_at}
    backend.openwebui_client.find_user.return_value = account

    _add(backend)

    assert backend.openwebui_client.set_role.called is promoted


def test_an_account_older_than_the_cutoff_is_not_bound_for_someone_provisioned_after_it():
    # Registered under the address before the upgrade, by someone else: the plugin only
    # made accounts by then for people it had provisioned by then.
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], bind_accounts_created_before="2026-10-01")
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = None  # provisioned today, 2026-10-15
    squatter = {"id": "owui-squat", "email": EMAIL, "role": ROLE_DISABLED,
                "created_at": 1_700_000_000}
    backend.openwebui_client.find_user.return_value = squatter

    _add(backend)

    backend.openwebui_client.set_role.assert_not_called()


def test_a_member_an_older_agent_provisioned_counts_as_before_the_cutoff_however_long_ago():
    # Their record's periods were trimmed long ago; that it carries no waldur_created
    # marker is what says an agent from before the upgrade stamped it.
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], bind_accounts_created_before="2026-10-01")
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "metadata": {META_RESOURCE: None, META_OWNERSHIP: [_period("other", "2026-09-20", "2026-09-25")]},
    }
    account = {"id": "owui-1", "email": EMAIL, "role": ROLE_DISABLED, "created_at": 1_700_000_000}
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == {USERNAME}

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)


@pytest.mark.parametrize("settings", [SETTINGS, _sso_settings()], ids=["managed_password", "sso"])
def test_an_admin_who_is_a_member_is_added_and_their_account_left_alone(settings):
    # Elevated roles are set outside the plugin; failing the add over one would have the
    # member created and deleted on every pass and their chat never attributed.
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = None
    backend.openwebui_client.find_user.return_value = {"id": "owui-admin", "role": "admin"}

    assert _add(backend) == {USERNAME}

    backend.openwebui_client.set_role.assert_not_called()
    backend.litellm_client.delete_users.assert_not_called()


def test_a_failed_re_add_after_the_retention_window_keeps_the_record_naming_the_account():
    # Kept past retention because its account lives; the claim trimmed its old periods.
    # Deleting it on a failed add would lose the only link to the person's account.
    backend = _make_backend()
    kept = _owned_user(_period(RID, "2025-01-01", "2025-02-01"))
    kept["metadata"][META_CHAT_ACCOUNT] = "owui-ada"
    backend.litellm_client.get_user.return_value = kept
    backend.openwebui_client.find_user.return_value = {"id": "owui-ada", "role": ROLE_DISABLED}
    backend.openwebui_client.set_role.side_effect = BackendError("chat is down")

    assert _add(backend) == set()

    backend.litellm_client.delete_users.assert_not_called()
    record = backend.litellm_client.get_user(EMAIL)
    assert record["metadata"][META_CHAT_ACCOUNT] == "owui-ada"
    assert ownership.current_owner(ownership.periods_of(record["metadata"])) is None

    # The next pass claims it again -- waldur_created, not hand-made -- and recognises
    # the account.
    backend.openwebui_client.set_role.side_effect = None
    assert _add(backend) == {USERNAME}
    backend.openwebui_client.set_role.assert_called_with(
        {"id": "owui-ada", "role": ROLE_DISABLED}, ROLE_ACTIVE
    )


def test_a_failed_re_add_leaves_an_older_agents_record_claimable():
    # Stamped before the upgrade (no waldur_created), its one period long past the
    # retention window: the claim trims it, the failed add drops today's, and what is
    # left is an empty ownership list -- still the plugin's, not a hand-made user.
    backend = _make_backend()
    legacy = {
        "user_id": EMAIL,
        "metadata": {
            META_RESOURCE: None,
            META_USERNAME: USERNAME,
            META_OWNERSHIP: [_period(RID, "2025-01-01", "2025-02-01")],
        },
    }
    backend.litellm_client.get_user.return_value = legacy
    backend.openwebui_client.find_user.side_effect = BackendError("chat is down")

    assert _add(backend) == set()
    assert ownership.periods_of(backend.litellm_client.get_user(EMAIL)["metadata"]) == []

    backend.openwebui_client.find_user.side_effect = None
    assert _add(backend) == {USERNAME}
    assert _written_metadata(backend)[META_OWNERSHIP] == [_period(RID, TODAY, None)]


def test_an_emptied_older_agents_record_still_counts_as_provisioned_before_the_cutoff():
    # The same emptied record: still the old agent's, so the member's pre-cutoff account
    # is theirs. Reading "provisioned before" off the periods alone would refuse it.
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], bind_accounts_created_before="2026-10-01")
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "metadata": {META_RESOURCE: None, META_USERNAME: USERNAME, META_OWNERSHIP: []},
    }
    account = {"id": "owui-1", "email": EMAIL, "role": ROLE_DISABLED, "created_at": 1_700_000_000}
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == {USERNAME}

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)


def test_a_member_whose_record_an_older_agent_deleted_is_refused_until_bound_by_hand():
    # The old agent deleted their record and demoted their account. With no record,
    # their own pre-cutoff account looks like a squatter's, so the agent refuses it; the
    # README's manual binding -- an empty ownership list naming the account -- is the way
    # back that keeps their conversations.
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], bind_accounts_created_before="2026-10-01")
    backend = _make_backend(settings)
    backend.litellm_client.get_user.return_value = None
    account = {"id": "owui-ada", "email": EMAIL, "role": ROLE_DISABLED, "created_at": 1_700_000_000}
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == set()
    backend.openwebui_client.set_role.assert_not_called()

    bound_by_hand = _make_backend(settings)
    bound_by_hand.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "metadata": {META_OWNERSHIP: [], META_USERNAME: USERNAME, META_CHAT_ACCOUNT: "owui-ada"},
    }
    bound_by_hand.openwebui_client.find_user.return_value = account

    assert _add(bound_by_hand) == {USERNAME}
    bound_by_hand.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)


def test_binding_an_existing_record_by_hand_keeps_its_billed_periods():
    # The README's update recipe adds only the account id; the closed period that bills
    # the member's October chat survives the claim, and the account is re-enabled.
    backend = _make_backend()
    record = _owned_user(_period(RID, "2026-09-01", "2026-10-10"))
    record["metadata"][META_CHAT_ACCOUNT] = "owui-ldap"
    backend.litellm_client.get_user.return_value = record
    account = {"id": "owui-ldap", "email": EMAIL, "role": ROLE_DISABLED}
    backend.openwebui_client.find_user.return_value = account

    assert _add(backend) == {USERNAME}

    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_ACTIVE)
    periods = ownership.periods_of(backend.litellm_client.get_user(EMAIL)["metadata"])
    assert ownership.owner_on(periods, "2026-10-05") == RID
    assert periods[-1] == _period(RID, TODAY, None)


def test_a_failed_add_keeps_a_record_another_pass_recorded_an_account_on():
    # Two passes adding the same new member at once: this one created the record, the
    # other created and recorded the account; this one's failure must not wipe that.
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    def taken(email, name, password, role):
        record = backend.litellm_client.get_user(email)
        backend.litellm_client.update_user(
            email, {"metadata": {**record["metadata"], META_CHAT_ACCOUNT: "owui-other"}}
        )
        raise BackendError("400 EMAIL_TAKEN")

    backend.openwebui_client.create_user.side_effect = taken

    assert _add(backend) == set()

    backend.litellm_client.delete_users.assert_not_called()
    assert backend.litellm_client.get_user(EMAIL)["metadata"][META_CHAT_ACCOUNT] == "owui-other"


def test_an_unreadable_cutoff_is_refused_at_construction():
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], bind_accounts_created_before="soon")
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
        pytest.raises(BackendError, match="bind_accounts_created_before"),
    ):
        LiteLLMBackend(settings, dict(COMPONENTS))


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
    backend.litellm_client.get_user.return_value = _managed_user(account="owui-1")
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
        pytest.raises(BackendError, match="password_secret"),
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


# --- keyless resources on an offering that manages keys one by one -----------
#
# Such a resource starts with no keys and can lose all of them. The membership sync
# still has to see it, or its members get no account and departed ones keep theirs.


def _managed_resource(managed: bool = True) -> SimpleNamespace:
    resource = _make_resource()
    resource.offering_plugin_options = SimpleNamespace(enable_api_key_provisioning=managed)
    return resource


def _pull(backend: LiteLLMBackend, resource: SimpleNamespace, strict: bool = False) -> dict:
    """Pull the way the membership processor does."""
    return backend.pull_resources([resource], include_usage=False, strict=strict)


def test_a_keyless_resource_whose_keys_are_managed_is_reported_with_its_members():
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.list_users.return_value = [_managed_user()]
    resource = _managed_resource()

    report = _pull(backend, resource)

    _, info = report[RID]
    assert info.backend_id == RID
    assert info.users == [USERNAME]


def test_a_keyless_resource_without_managed_keys_is_still_missing():
    # None is what makes the order processor re-provision a restored resource's keys.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.list_users.return_value = [_managed_user()]

    assert backend.pull_resource(_managed_resource(managed=False)) is None
    assert backend.pull_resource(_make_resource()) is None


@pytest.mark.parametrize("state", [ResourceState.ERRED, ResourceState.TERMINATING])
def test_a_keyless_managed_resource_winding_down_is_still_missing(state):
    # A terminate that deleted the keys and then failed leaves it ERRED. Reporting it
    # would re-add its members and reset it to OK, hiding the failed terminate.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = []
    backend.litellm_client.list_users.return_value = [_managed_user()]
    resource = _managed_resource()
    resource.state = state

    assert backend.pull_resource(resource) is None
    assert _pull(backend, resource) == {}


def test_a_managed_resource_winding_down_with_keys_is_still_reported():
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    resource = _managed_resource()
    resource.state = ResourceState.ERRED

    assert backend.pull_resource(resource).backend_id == RID


def test_a_managed_resource_is_reported_without_listing_its_keys():
    backend = _make_backend()

    assert backend.pull_resource(_managed_resource()) is not None
    backend.litellm_client.list_keys.assert_not_called()


def test_a_failed_user_listing_on_a_keyless_managed_resource_reads_as_no_members():
    backend = _make_backend()
    backend.litellm_client.list_users.side_effect = BackendError("user list is down")

    _, info = _pull(backend, _managed_resource())[RID]

    assert info.users == []


def test_a_failed_user_listing_fails_a_strict_pull():
    # The teardown reads "no members" as "no association" and releases the account.
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [{"key_alias": f"{RID}-1", "token": "h1"}]
    backend.litellm_client.list_users.side_effect = BackendError("user list is down")

    for resource in (_managed_resource(), _make_resource()):
        with pytest.raises(BackendError, match="user list is down"):
            _pull(backend, resource, strict=True)



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
    backend.litellm_client.list_users.return_value = [_managed_user(account="owui-1")]
    backend.litellm_client.get_user.return_value = _managed_user(account="owui-1")
    backend.openwebui_client.find_user.return_value = {"id": "owui-1", "role": ROLE_ACTIVE}

    backend.remove_users_from_resource(_make_resource(), {USERNAME})

    backend.openwebui_client.delete_user.assert_called_once_with("owui-1")


def test_removal_demotes_but_never_deletes_an_account_waldur_did_not_create():
    # An account an operator made by hand under the member's address still loses the
    # access the membership gave, but the plugin destroys only what it made.
    settings = dict(SETTINGS)
    settings["openwebui"] = dict(SETTINGS["openwebui"], delete_accounts_on_removal=True)
    backend = _make_backend(settings)
    backend.litellm_client.list_users.return_value = [_managed_user()]
    backend.litellm_client.get_user.return_value = _managed_user()
    account = {"id": "owui-hand", "email": EMAIL, "role": ROLE_ACTIVE}
    backend.openwebui_client.find_user.return_value = account

    assert backend.remove_users_from_resource(_make_resource(), {USERNAME}) == [USERNAME]

    backend.openwebui_client.delete_user.assert_not_called()
    backend.openwebui_client.set_role.assert_called_once_with(account, ROLE_DISABLED)


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
    backend.litellm_client.list_users.return_value = [_managed_user(account="owui-1")]
    backend.litellm_client.get_user.return_value = _managed_user(account="owui-1")
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
    # Hand-made too, but an older agent adopted it, so it carries periods: released,
    # never deleted, because the plugin did not create it.
    adopted = _owned_user(_period(RID, None, "2026-08-31"), email="ops@example.com")
    del adopted["metadata"][META_CREATED]
    current = _owned_user(_period(RID, None, None))
    backend.litellm_client.list_users.return_value = [expired, recent, hand_made, adopted, current]
    backend.litellm_client.get_user.side_effect = lambda email: {
        "gone@example.com": expired,
        "ops@example.com": adopted,
    }.get(email)

    users = backend._managed_users()

    backend.litellm_client.delete_users.assert_called_once_with(["gone@example.com"])
    assert sorted(users) == [
        EMAIL, "admin@example.com", "ops@example.com", "recent@example.com"
    ]


def test_an_expired_record_whose_chat_account_still_exists_is_kept():
    # Removal demoted the account rather than deleting it; the record is the only thing
    # saying the account is the person's, and a re-add needs it to re-enable it.
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=2))
    expired = _owned_user(_period(RID, None, "2026-08-31"), email="gone@example.com")
    expired["metadata"][META_CHAT_ACCOUNT] = "owui-gone"
    backend.litellm_client.list_users.return_value = [expired]
    backend.litellm_client.get_user.side_effect = lambda email: expired
    backend.openwebui_client.find_user.return_value = {"id": "owui-gone", "role": ROLE_DISABLED}

    backend._managed_users()

    backend.litellm_client.delete_users.assert_not_called()


def test_an_expired_record_is_kept_when_its_chat_account_cannot_be_looked_up():
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=2))
    expired = _owned_user(_period(RID, None, "2026-08-31"), email="gone@example.com")
    expired["metadata"][META_CHAT_ACCOUNT] = "owui-gone"
    backend.litellm_client.list_users.return_value = [expired]
    backend.litellm_client.get_user.side_effect = lambda email: expired
    backend.openwebui_client.find_user.side_effect = BackendError("ambiguous")

    backend._managed_users()

    backend.litellm_client.delete_users.assert_not_called()


def test_an_expired_record_whose_chat_account_is_gone_is_pruned():
    backend = _make_backend(dict(SETTINGS, ownership_retention_months=2))
    expired = _owned_user(_period(RID, None, "2026-08-31"), email="gone@example.com")
    expired["metadata"][META_CHAT_ACCOUNT] = "owui-gone"
    backend.litellm_client.list_users.return_value = [expired]
    backend.litellm_client.get_user.side_effect = lambda email: expired
    backend.openwebui_client.find_user.return_value = None

    backend._managed_users()

    backend.litellm_client.delete_users.assert_called_once_with(["gone@example.com"])


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


# --- member role ------------------------------------------------------------
#
# internal_user can mint its own uncapped keys from the LiteLLM UI; internal_user_viewer
# cannot. Members are created as viewers, and records created before that are demoted.


def _role_writes(backend: LiteLLMBackend) -> list:
    return [
        call.args
        for call in backend.litellm_client.update_user.call_args_list
        if call.args[1] == {"user_role": "internal_user_viewer"}
    ]


def test_a_member_created_as_internal_user_is_demoted_on_the_next_listing():
    backend = _make_backend()
    record = dict(_owned_user(_period(RID, "2026-09-01", None)), user_role="internal_user")
    backend.litellm_client.list_users.return_value = [record]

    users = backend._managed_users()

    # The write carries the role alone, so it cannot overwrite newer metadata.
    assert _role_writes(backend) == [(EMAIL, {"user_role": "internal_user_viewer"})]
    assert users[EMAIL]["user_role"] == "internal_user_viewer"
    # Still listed, so the membership diff does not re-add or remove them.
    assert backend.list_resource_users(RID) == [USERNAME]


def test_a_record_from_before_dated_ownership_is_demoted_too():
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [
        dict(_managed_user(), user_role="internal_user")
    ]

    backend._managed_users()

    assert _role_writes(backend) == [(EMAIL, {"user_role": "internal_user_viewer"})]


def test_a_removed_member_is_demoted_because_their_record_outlives_them():
    # A released record stays for the retention window, and so does the sign-in.
    backend = _make_backend()
    record = dict(_owned_user(_period(RID, "2026-09-01", "2026-10-01")), user_role="internal_user")
    backend.litellm_client.list_users.return_value = [record]

    backend._managed_users()

    assert _role_writes(backend) == [(EMAIL, {"user_role": "internal_user_viewer"})]


@pytest.mark.parametrize(
    "record",
    [
        # A user an admin made by hand: not the plugin's to change.
        {"user_id": EMAIL, "user_role": "internal_user", "metadata": {}},
        # An admin who is also a member keeps the role the operator gave them.
        dict(_owned_user(_period(RID, "2026-09-01", None)), user_role="proxy_admin"),
        # Already a viewer: nothing to write.
        dict(_owned_user(_period(RID, "2026-09-01", None)), user_role="internal_user_viewer"),
    ],
    ids=["hand-made", "proxy-admin", "viewer"],
)
def test_only_stamped_internal_users_are_demoted(record):
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [record]

    backend._managed_users()

    backend.litellm_client.update_user.assert_not_called()


def test_a_failed_demotion_does_not_fail_the_listing_and_is_retried():
    backend = _make_backend()
    record = dict(_owned_user(_period(RID, "2026-09-01", None)), user_role="internal_user")
    backend.litellm_client.list_users.return_value = [record]
    backend.litellm_client.update_user.side_effect = BackendError("proxy said no")

    assert backend.list_resource_users(RID) == [USERNAME]
    assert backend._managed_users()[EMAIL]["user_role"] == "internal_user"

    backend._invalidate_users_cache()
    backend.litellm_client.update_user.side_effect = None
    backend._managed_users()

    assert _role_writes(backend)[-1] == (EMAIL, {"user_role": "internal_user_viewer"})


# Adopted means stamped by an agent from before members were viewers: a user Waldur
# never stamped is refused outright (``test_a_user_waldur_never_stamped_is_refused_not_adopted``).
_OLDER_AGENTS_METADATA = {META_RESOURCE: RID, META_USERNAME: USERNAME}


def test_an_adopted_internal_user_is_switched_to_the_viewer_role():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "user_role": "internal_user",
        "metadata": _OLDER_AGENTS_METADATA,
    }

    assert _add(backend) == {USERNAME}

    # Not necessarily the last write: the chat account is recorded on the user after it.
    roles = [
        call.args[1]["user_role"]
        for call in backend.litellm_client.update_user.call_args_list
        if "user_role" in call.args[1]
    ]
    assert roles == ["internal_user_viewer"]


def test_an_adopted_admin_keeps_their_role():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "user_role": "proxy_admin",
        "metadata": _OLDER_AGENTS_METADATA,
    }

    assert _add(backend) == {USERNAME}

    assert all(
        "user_role" not in call.args[1]
        for call in backend.litellm_client.update_user.call_args_list
    )
    assert backend.litellm_client.update_user.called


def test_an_adopted_viewer_is_written_without_a_role():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "user_role": "internal_user_viewer",
        "metadata": _OLDER_AGENTS_METADATA,
    }

    assert _add(backend) == {USERNAME}

    assert all(
        "user_role" not in call.args[1]
        for call in backend.litellm_client.update_user.call_args_list
    )
    assert backend.litellm_client.update_user.called


# --- keys a member minted while they could -----------------------------------
#
# The role switch stops new keys only: a viewer's existing key still calls models.


def test_a_demotion_reports_the_keys_the_member_minted(caplog):
    backend = _make_backend()
    record = dict(_owned_user(_period(RID, "2026-09-01", None)), user_role="internal_user")
    backend.litellm_client.list_users.return_value = [record]
    backend.litellm_client.list_user_keys.return_value = [
        {"token": "hash-1", "key_alias": "my-key", "key_name": "sk-...abcd"}
    ]

    with caplog.at_level("WARNING", logger="waldur_site_agent_litellm.backend"):
        backend._managed_users()

    backend.litellm_client.list_user_keys.assert_called_once_with(EMAIL)
    assert "hash-1" in caplog.text
    assert "sk-...abcd" in caplog.text
    # Reported, never revoked: the key was not Waldur's to delete.
    backend.litellm_client.delete_keys.assert_not_called()
    backend.litellm_client.block.assert_not_called()
    backend.litellm_client.update_key.assert_not_called()


def test_no_key_lookup_without_a_demotion():
    backend = _make_backend()
    record = dict(
        _owned_user(_period(RID, "2026-09-01", None)), user_role="internal_user_viewer"
    )
    backend.litellm_client.list_users.return_value = [record]

    backend._managed_users()

    backend.litellm_client.list_user_keys.assert_not_called()


def test_a_failed_key_lookup_does_not_fail_the_listing():
    backend = _make_backend()
    record = dict(_owned_user(_period(RID, "2026-09-01", None)), user_role="internal_user")
    backend.litellm_client.list_users.return_value = [record]
    backend.litellm_client.list_user_keys.side_effect = BackendError("proxy said no")

    assert backend.list_resource_users(RID) == [USERNAME]
    assert backend._managed_users()[EMAIL]["user_role"] == "internal_user_viewer"


def test_an_adopted_internal_user_has_their_keys_reported():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "user_role": "internal_user",
        "metadata": _OLDER_AGENTS_METADATA,
    }

    assert _add(backend) == {USERNAME}

    backend.litellm_client.list_user_keys.assert_called_once_with(EMAIL)


def test_an_adopted_admin_has_no_key_lookup():
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = {
        "user_id": EMAIL,
        "user_role": "proxy_admin",
        "metadata": _OLDER_AGENTS_METADATA,
    }

    assert _add(backend) == {USERNAME}

    backend.litellm_client.list_user_keys.assert_not_called()
