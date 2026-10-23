"""Tests for chat that follows the key: access and model groups by the assigned key."""

from __future__ import annotations

import datetime
import threading
import uuid
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest

from waldur_site_agent_litellm import ownership
from waldur_site_agent_litellm.backend import (
    META_HOLDER_CHAT,
    META_KEY_LIMITS,
    META_KEY_PAUSED,
    META_RESOURCE_LIMITS,
    NO_MODELS,
    _KEY_METADATA_LOCK,
    LiteLLMBackend,
)
from waldur_site_agent_litellm.client import META_KEY, META_RESOURCE, META_USERNAME
from waldur_site_agent_litellm.openwebui import ROLE_ACTIVE, ROLE_DISABLED

COMPONENTS = {
    "input_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
    "output_tokens": {"measured_unit": "tokens", "accounting_type": "usage"},
}
SETTINGS = {
    "api_url": "https://litellm.example.com/",
    "api_token": "sk-master",
    "models": ["gpt-4o", "gpt-4o-mini"],
    "openwebui": {
        "api_url": "https://chat.example.com",
        "api_token": "owui-admin",
        "url": "https://chat.example.com",
        "account_provisioning": "managed_password",
        "initial_password": "ChangeMe-123",
    },
}
# A budget that resets on the 1st, the only kind the holder's chat is taken off.
MONTHLY = {**SETTINGS, "budget_duration": "1mo"}
RID = "abc123"
ADA = "ada@example.com"
BOB = "bob@example.com"
GROUPS = {"gpt-4o": "g-4o", "gpt-4o-mini": "g-mini"}


def _make_backend(settings: Optional[dict] = None) -> LiteLLMBackend:
    with (
        mock.patch("waldur_site_agent_litellm.backend.LiteLLMClient"),
        mock.patch("waldur_site_agent_litellm.backend.OpenWebUIClient"),
    ):
        backend = LiteLLMBackend(dict(settings or SETTINGS), dict(COMPONENTS))
    backend.litellm_client = mock.MagicMock()
    backend.litellm_client.list_users.return_value = []
    backend.litellm_client.list_keys.return_value = []
    if backend.usage_client is not None:
        backend.usage_client = mock.MagicMock()
        backend.usage_client.get_user_usage_rows.return_value = []
    if backend.openwebui_client is not None:
        owui = mock.MagicMock()
        owui.find_user.side_effect = lambda email: _account(email, ROLE_DISABLED)
        owui.list_groups.return_value = [
            {"id": gid, "name": f"waldur-model-{model}"} for model, gid in GROUPS.items()
        ]
        owui.user_group_ids.return_value = set()
        backend.openwebui_client = owui
    # /key/info answers from the listing, as the proxy does when nothing changed since.
    backend.litellm_client.get_key.side_effect = lambda token: next(
        (k for k in backend.litellm_client.list_keys.return_value if k["token"] == token), None
    )
    return backend


def _account(email: str, role: str) -> dict:
    return {"id": f"owui-{email.split('@')[0]}", "email": email, "name": email, "role": role}


def _resource(*, managed: bool = True, paused: bool = False) -> SimpleNamespace:
    options = SimpleNamespace(enable_api_key_provisioning=True) if managed else None
    return SimpleNamespace(
        uuid=uuid.uuid4(),
        name="Inference",
        backend_id=RID,
        offering_plugin_options=options,
        paused=paused,
        downscaled=False,
    )


def _member(email: str, key: Optional[str] = "") -> dict:
    metadata = {META_RESOURCE: RID, META_USERNAME: email.split("@")[0]}
    if key is not None:
        metadata[META_KEY] = key
    return {"user_id": email, "user_alias": email.split("@")[0].title(), "metadata": metadata}


def _key(alias: str, *, blocked: bool = False, models: Optional[list] = None, **meta) -> dict:
    record = {"key_alias": alias, "token": f"h-{alias}", "blocked": blocked}
    if models is not None:
        record["models"] = models
    if meta:
        record["metadata"] = dict(meta)
    return record


def _stamps(backend: LiteLLMBackend) -> dict:
    """``{email: alias}`` of every key stamp written on a user record."""
    return {
        call.args[0]: call.args[1]["metadata"][META_KEY]
        for call in backend.litellm_client.update_user.call_args_list
        if META_KEY in call.args[1].get("metadata", {})
    }


def _roles(backend: LiteLLMBackend) -> dict:
    """``{email: role}`` of every chat account whose role was changed."""
    return {
        call.args[0]["email"]: call.args[1]
        for call in backend.openwebui_client.set_role.call_args_list
    }


def _groups_added(backend: LiteLLMBackend) -> list:
    return [call.args for call in backend.openwebui_client.add_user_to_group.call_args_list]


def _groups_removed(backend: LiteLLMBackend) -> list:
    return [call.args for call in backend.openwebui_client.remove_user_from_group.call_args_list]


def test_only_an_offering_with_chat_asks_who_holds_its_keys() -> None:
    # Each ask is a Waldur key listing per resource per pass; an API-only offering has
    # no use for the answer.
    assert _make_backend().requires_resource_key_assignees is True
    api_only = {key: value for key, value in SETTINGS.items() if key != "openwebui"}
    assert _make_backend(api_only).requires_resource_key_assignees is False


# --- joining the project ------------------------------------------------------


def test_a_new_member_gets_no_chat_until_a_key_is_assigned() -> None:
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None

    backend.add_users_to_resource(
        _resource(), {"ada"}, user_emails={"ada": ADA}, user_attributes={}
    )

    # Their record says chat follows a key, and none is assigned yet.
    metadata = backend.litellm_client.create_user.call_args.kwargs["metadata"]
    assert metadata[META_KEY] == ""
    backend.openwebui_client.create_user.assert_not_called()


def test_without_key_management_a_new_member_chats_by_membership_as_before() -> None:
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = None
    backend.openwebui_client.find_user.side_effect = None
    backend.openwebui_client.find_user.return_value = None

    backend.add_users_to_resource(
        _resource(managed=False), {"ada"}, user_emails={"ada": ADA}, user_attributes={}
    )

    assert META_KEY not in backend.litellm_client.create_user.call_args.kwargs["metadata"]
    backend.openwebui_client.create_user.assert_called_once()


def test_a_re_add_keeps_the_key_the_person_holds() -> None:
    backend = _make_backend()
    backend.litellm_client.get_user.return_value = _member(ADA, key=f"{RID}-2")

    backend.add_users_to_resource(
        _resource(), {"ada"}, user_emails={"ada": ADA}, user_attributes={}
    )

    assert backend.litellm_client.update_user.call_args.args[1]["metadata"][META_KEY] == f"{RID}-2"


# --- the assignee sync --------------------------------------------------------


def test_assigning_a_key_opens_the_holders_chat_with_its_models() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=["gpt-4o-mini"])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert _stamps(backend) == {ADA: f"{RID}-1"}
    assert _roles(backend) == {ADA: ROLE_ACTIVE}
    assert _groups_added(backend) == [("g-mini", "owui-ada")]


def test_a_member_with_no_key_has_their_chat_closed() -> None:
    # Someone who chatted by membership before key management was turned on.
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.openwebui_client.user_group_ids.return_value = {"g-4o", "someone-elses"}
    backend.litellm_client.list_users.return_value = [_member(ADA, key=None)]

    backend.sync_resource_key_assignees(_resource(), {})

    assert _stamps(backend) == {ADA: ""}
    assert _roles(backend) == {ADA: ROLE_DISABLED}
    # Only the plugin's own groups are touched.
    assert _groups_removed(backend) == [("g-4o", "owui-ada")]


def test_reassigning_a_key_moves_the_chat_with_it() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(
        email, ROLE_ACTIVE if email == ADA else ROLE_DISABLED
    )
    backend.litellm_client.list_users.return_value = [
        _member(ADA, key=f"{RID}-1"),
        _member(BOB),
    ]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "bob"})

    assert _stamps(backend) == {ADA: "", BOB: f"{RID}-1"}
    assert _roles(backend) == {ADA: ROLE_DISABLED, BOB: ROLE_ACTIVE}


def test_a_paused_key_keeps_its_holders_chat_closed() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True})
    ]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    backend.openwebui_client.set_role.assert_not_called()
    backend.litellm_client.update_user.assert_not_called()


def test_a_paused_resource_keeps_chat_closed_before_its_keys_are_blocked() -> None:
    # The status sync that blocks the keys runs after this in the same pass.
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.sync_resource_key_assignees(_resource(paused=True), {f"{RID}-1": "ada"})

    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_a_downscaled_resource_keeps_chat_closed_too() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    resource = _resource()
    resource.downscaled = True

    backend.sync_resource_key_assignees(resource, {f"{RID}-1": "ada"})

    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_a_key_paused_since_the_listing_does_not_get_its_chat_reopened() -> None:
    # The pass read key 1 serving; a pause at its limit closed Ada's chat since.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.litellm_client.get_key.side_effect = lambda token: _key(
        f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}
    )

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert _roles(backend) == {}
    assert _groups_added(backend) == []


def test_of_two_keys_a_person_holds_the_oldest_carries_their_chat_even_paused() -> None:
    # Toy story: Ada holds keys 1 and 2, key 1 paused at its limit. Moving her chat to key
    # 2 would drop key 1's usage back under its limit, Waldur would resume it, key 2 would
    # fill and pause, and her chat would alternate between them and never stop.
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}),
        _key(f"{RID}-3"),
        _key(f"{RID}-2"),
    ]

    backend.sync_resource_key_assignees(
        _resource(), {f"{RID}-1": "ada", f"{RID}-2": "ada", f"{RID}-3": "ada"}
    )

    assert _stamps(backend) == {}
    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_the_first_pass_after_a_restore_keeps_the_holders_models() -> None:
    # The user sync runs before the status sync, so the keys are still blocked from the
    # pause when Waldur already shows the resource restored. Reading that as "key paused"
    # would strip Ada's model groups, which the restore does not give back.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, models=["gpt-4o"])
    ]

    backend.sync_resource_key_assignees(_resource(paused=False), {f"{RID}-1": "ada"})

    assert _roles(backend) == {ADA: ROLE_ACTIVE}
    assert _groups_added(backend) == [("g-4o", "owui-ada")]


def test_a_key_for_every_model_joins_every_model_group() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    # The proxy stores an empty list for every model.
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=[])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert sorted(_groups_added(backend)) == [("g-4o", "owui-ada"), ("g-mini", "owui-ada")]


def test_an_account_not_signed_in_yet_is_left_to_the_next_pass() -> None:
    backend = _make_backend(
        {**SETTINGS, "openwebui": {**SETTINGS["openwebui"], "account_provisioning": "sso"}}
    )
    backend.openwebui_client.find_user.side_effect = None
    backend.openwebui_client.find_user.return_value = None
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    # Stamped now, so the first sync after their sign-in opens it.
    assert _stamps(backend) == {ADA: f"{RID}-1"}
    backend.openwebui_client.add_user_to_group.assert_not_called()


def test_one_persons_failure_does_not_stop_the_rest() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA), _member(BOB)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1"), _key(f"{RID}-2")]
    from waldur_site_agent_litellm.openwebui import OpenWebUIError

    def find_user(email: str) -> dict:
        if email == ADA:
            raise OpenWebUIError("down")
        return _account(email, ROLE_DISABLED)

    backend.openwebui_client.find_user.side_effect = find_user

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada", f"{RID}-2": "bob"})

    assert _roles(backend) == {BOB: ROLE_ACTIVE}


def test_a_failed_group_change_leaves_the_account_closed() -> None:
    # Opened first, a failure here would leave the person chatting in the groups an
    # earlier, wider key left them in.
    from waldur_site_agent_litellm.openwebui import OpenWebUIError  # noqa: PLC0415

    backend = _make_backend()
    backend.openwebui_client.user_group_ids.return_value = {"g-4o"}
    backend.openwebui_client.add_user_to_group.side_effect = OpenWebUIError("down")
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=["gpt-4o-mini"])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert _roles(backend) == {}
    # The wider group went before the failing add.
    assert _groups_removed(backend) == [("g-4o", "owui-ada")]


def test_groups_are_narrowed_before_widened_and_before_the_account_opens() -> None:
    backend = _make_backend()
    backend.openwebui_client.user_group_ids.return_value = {"g-4o"}
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=["gpt-4o-mini"])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    writes = [
        call[0]
        for call in backend.openwebui_client.mock_calls
        if call[0] in ("add_user_to_group", "remove_user_from_group", "set_role")
    ]
    assert writes == ["remove_user_from_group", "add_user_to_group", "set_role"]


def test_a_new_account_is_made_closed_and_opened_once_in_its_groups() -> None:
    backend = _make_backend()
    created: dict = {}

    def find_user(email: str) -> Optional[dict]:
        return created.get(email)

    def create_user(email: str, name: str, password: str, role: str) -> dict:
        created[email] = {**_account(email, role), "name": name}
        return created[email]

    backend.openwebui_client.find_user.side_effect = find_user
    backend.openwebui_client.create_user.side_effect = create_user
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=["gpt-4o"])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert backend.openwebui_client.create_user.call_args.kwargs["role"] == ROLE_DISABLED
    assert _groups_added(backend) == [("g-4o", "owui-ada")]
    assert _roles(backend) == {ADA: ROLE_ACTIVE}
    writes = [
        call[0]
        for call in backend.openwebui_client.mock_calls
        if call[0] in ("create_user", "add_user_to_group", "set_role")
    ]
    assert writes == ["create_user", "add_user_to_group", "set_role"]


def test_the_sync_does_nothing_without_key_management() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=None)]

    backend.sync_resource_key_assignees(_resource(managed=False), {})

    backend.litellm_client.update_user.assert_not_called()
    backend.openwebui_client.set_role.assert_not_called()


def test_the_sync_does_nothing_on_an_api_only_offering() -> None:
    settings = {key: value for key, value in SETTINGS.items() if key != "openwebui"}
    backend = _make_backend(settings)
    backend.litellm_client.list_users.return_value = [_member(ADA)]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    backend.litellm_client.update_user.assert_not_called()


# --- model groups -------------------------------------------------------------


def test_missing_groups_are_created_and_every_grant_is_reasserted() -> None:
    backend = _make_backend()
    backend.openwebui_client.list_groups.return_value = [
        {"id": "g-4o", "name": "waldur-model-gpt-4o"},
        {"id": "x", "name": "operators"},
    ]
    backend.openwebui_client.create_group.return_value = {"id": "g-new"}

    groups = backend._chat_model_groups()

    assert groups == {"gpt-4o": "g-4o", "gpt-4o-mini": "g-new"}
    backend.openwebui_client.create_group.assert_called_once()
    # An admin who set a model to Public replaced its grants; every pass puts them back.
    assert sorted(
        call.args for call in backend.openwebui_client.grant_model_to_group.call_args_list
    ) == [("gpt-4o", "g-4o"), ("gpt-4o-mini", "g-new")]


def test_groups_are_read_once_per_pass() -> None:
    backend = _make_backend()
    backend._chat_model_groups()
    backend._chat_model_groups()
    backend.openwebui_client.list_groups.assert_called_once()


def test_without_an_offering_model_list_groups_are_left_to_the_operator() -> None:
    backend = _make_backend({**SETTINGS, "models": []})
    backend._set_model_groups(ADA, ["gpt-4o"])
    backend.openwebui_client.list_groups.assert_not_called()


def test_a_model_dropped_from_the_offering_is_closed_in_chat() -> None:
    # The offering's models shrank from both to gpt-4o; gpt-4o-mini's group is still in
    # Open WebUI with the person in it, and would keep the model open to them.
    backend = _make_backend({**SETTINGS, "models": ["gpt-4o"]})
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.openwebui_client.user_group_ids.return_value = {"g-4o", "g-mini", "operators"}
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    # A key for every model: in chat, every model the offering has now.
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=[])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert _groups_removed(backend) == [("g-mini", "owui-ada")]
    backend.openwebui_client.add_user_to_group.assert_not_called()
    # The retired model is no longer granted to anyone afresh.
    assert [
        call.args for call in backend.openwebui_client.grant_model_to_group.call_args_list
    ] == [("gpt-4o", "g-4o")]


# --- the per-key verbs --------------------------------------------------------


def test_pausing_a_key_closes_its_holders_chat() -> None:
    # Waldur pauses a key at its limit; without this the limit would hold for API calls
    # only.
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [
        _member(ADA, key=f"{RID}-1"),
        _member(BOB, key=f"{RID}-2"),
    ]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1"), _key(f"{RID}-2")]

    backend.pause_resource_key(f"{RID}-1", RID)

    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_pausing_a_key_with_chat_unreachable_keeps_the_key_blocked_and_fails() -> None:
    # The block is written first and is not undone; the failure reaches the command, so
    # Waldur sees the pause as failed, and the next membership sync closes the chat.
    from waldur_site_agent_litellm.openwebui import OpenWebUIError  # noqa: PLC0415

    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = OpenWebUIError("down")
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    with pytest.raises(OpenWebUIError):
        backend.pause_resource_key(f"{RID}-1", RID)

    (token, fields), _ = backend.litellm_client.update_key.call_args
    assert token == f"h-{RID}-1"
    assert fields["blocked"] is True
    assert fields["metadata"][META_KEY_PAUSED] is True
    backend.litellm_client.update_key.assert_called_once()


def test_resuming_a_key_reopens_its_holders_chat_with_its_models() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}),
        _key(f"{RID}-2"),
    ]

    backend.resume_resource_key(f"{RID}-1", RID, allowed_models=["gpt-4o"])

    assert _roles(backend) == {ADA: ROLE_ACTIVE}
    assert _groups_added(backend) == [("g-4o", "owui-ada")]


def test_resuming_a_key_of_a_paused_resource_leaves_the_chat_closed() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}),
        _key(f"{RID}-2", blocked=True),
    ]

    backend.resume_resource_key(f"{RID}-1", RID)

    # The account stays demoted, and gains no model.
    assert _roles(backend) == {}
    assert _groups_added(backend) == []


def test_narrowing_a_keys_models_narrows_its_holders_chat() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.openwebui_client.user_group_ids.return_value = {"g-4o", "g-mini"}
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.update_resource_key(f"{RID}-1", RID, allowed_models=["gpt-4o-mini"])

    assert _groups_removed(backend) == [("g-4o", "owui-ada")]
    assert _groups_added(backend) == []


def test_a_key_left_with_no_models_leaves_its_holder_no_model_in_chat() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.openwebui_client.user_group_ids.return_value = {"g-4o", "g-mini"}
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=[NO_MODELS])]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert sorted(_groups_removed(backend)) == [("g-4o", "owui-ada"), ("g-mini", "owui-ada")]
    assert _groups_added(backend) == []


def test_deleting_a_key_closes_its_holders_chat_and_unlinks_it() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.delete_resource_key(f"{RID}-1", RID)

    assert _roles(backend) == {ADA: ROLE_DISABLED}
    # Otherwise a resource restore would reopen chat on a key that is gone.
    assert _stamps(backend) == {ADA: ""}


def test_deleting_a_key_already_gone_still_closes_its_holders_chat() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]

    backend.delete_resource_key(f"{RID}-1", RID)

    assert _roles(backend) == {ADA: ROLE_DISABLED}


# --- the resource around the keys ---------------------------------------------


def test_restoring_the_resource_reopens_chat_only_on_keys_that_come_back() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [
        _member(ADA, key=f"{RID}-1"),
        _member(BOB, key=f"{RID}-2"),
        _member("carol@example.com"),
        _member("dan@example.com", key=None),
    ]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True),
        _key(f"{RID}-2", blocked=True, **{META_KEY_PAUSED: True}),
    ]
    backend.litellm_client.unblock.return_value = True

    backend.restore_resource(RID)

    # Ada's key comes back; Bob's is paused on its own; Carol holds no key. Dan is on
    # membership chat (an offering without key management) and comes back as before.
    assert _roles(backend) == {ADA: ROLE_ACTIVE, "dan@example.com": ROLE_ACTIVE}


def test_pausing_the_resource_closes_every_holders_chat() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.litellm_client.block.return_value = True

    backend.pause_resource(RID)

    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_pausing_a_key_the_proxy_lost_still_closes_its_holders_chat() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]

    backend.pause_resource_key(f"{RID}-1", RID)

    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_a_key_deleted_since_the_listing_does_not_get_its_chat_reopened() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1"), _member(BOB)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1"), _key(f"{RID}-2")]
    backend.litellm_client.get_key.side_effect = lambda token: (
        None if token == f"h-{RID}-1" else _key(f"{RID}-2")
    )

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada", f"{RID}-2": "bob"})

    # Ada's key is gone: her chat stays closed. Bob, after her, is still reconciled.
    assert _roles(backend) == {BOB: ROLE_ACTIVE}


def test_the_models_are_read_with_the_key_just_before_opening() -> None:
    # An update narrowed key 1 to gpt-4o-mini after the pass listed it.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1", models=["gpt-4o"])]
    backend.litellm_client.get_key.side_effect = lambda token: _key(
        f"{RID}-1", models=["gpt-4o-mini"]
    )

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada"})

    assert _groups_added(backend) == [("g-mini", "owui-ada")]


def test_assignees_are_matched_by_offering_username_not_address() -> None:
    # Ada's record and chat account carry the address she was added under; the core
    # sends her offering username, so an address she changed in Waldur since does not
    # matter. Her record here has a username unrelated to her address.
    backend = _make_backend()
    member = _member(ADA)
    member["metadata"][META_USERNAME] = "u-1042"
    backend.litellm_client.list_users.return_value = [member]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "u-1042"})

    assert _stamps(backend) == {ADA: f"{RID}-1"}
    assert _roles(backend) == {ADA: ROLE_ACTIVE}


def test_a_key_the_proxy_lost_does_not_carry_chat() -> None:
    # Ada holds keys 1 and 2; key 1 was removed out of band. Her chat follows key 2.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-2")]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "ada", f"{RID}-2": "ada"})

    assert _stamps(backend) == {ADA: f"{RID}-2"}
    assert _roles(backend) == {ADA: ROLE_ACTIVE}


def test_unassigned_keys_carry_nobodys_chat() -> None:
    # The core maps a key assigned to nobody (the two default keys, say) to None.
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA)]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1"), _key(f"{RID}-2")]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": None, f"{RID}-2": None})

    backend.litellm_client.update_user.assert_not_called()
    backend.openwebui_client.set_role.assert_not_called()


def test_an_assignee_with_no_record_on_the_resource_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _make_backend()
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    with caplog.at_level("INFO"):
        backend.sync_resource_key_assignees(_resource(), {f"{RID}-1": "zed"})

    assert any("zed" in record.getMessage() for record in caplog.records)


def test_rotation_lifting_a_stale_pause_reopens_its_holders_chat() -> None:
    backend = _make_backend()
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}),
        _key(f"{RID}-2"),
    ]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(f"{RID}-1", RID)

    assert _roles(backend) == {ADA: ROLE_ACTIVE}


def test_rotation_lifting_a_stale_pause_on_a_paused_resource_keeps_chat_closed() -> None:
    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = lambda email: _account(email, ROLE_ACTIVE)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}),
        _key(f"{RID}-2", blocked=True),
    ]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(f"{RID}-1", RID)

    assert _roles(backend) == {ADA: ROLE_DISABLED}


def test_a_chat_failure_does_not_fail_the_rotation() -> None:
    from waldur_site_agent_litellm.openwebui import OpenWebUIError  # noqa: PLC0415

    backend = _make_backend()
    backend.openwebui_client.find_user.side_effect = OpenWebUIError("down")
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", blocked=True, **{META_KEY_PAUSED: True}),
        _key(f"{RID}-2"),
    ]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    assert backend.rotate_resource_key(f"{RID}-1", RID) == "sk-rotated"


# --- the key's budget counts its holder's chat --------------------------------


def _chat(
    email: str, spend: float, *, alias: Optional[str] = None, date: Optional[str] = None
) -> dict:
    """One per-person usage row, as ``get_user_usage_rows`` returns it: today's by default."""
    return {
        "date": date or datetime.datetime.now(tz=datetime.timezone.utc).date().isoformat(),
        "user_id": email,
        "key_alias": alias,
        "input_tokens": 10,
        "output_tokens": 10,
        "token_cost": spend,
    }


def _budget_writes(backend: LiteLLMBackend) -> dict:
    """``{token: (max_budget, metadata)}`` of every key update that wrote a budget."""
    return {
        call.args[0]: (call.args[1]["max_budget"], call.args[1].get("metadata", {}))
        for call in backend.litellm_client.update_key.call_args_list
        if "max_budget" in call.args[1]
    }


def test_the_holders_chat_comes_off_the_keys_budget() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 1.0), _chat(ADA, 2.0)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 7.0
    assert metadata[META_HOLDER_CHAT] == 3.0


def _days_ago(days: int) -> str:
    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    return (today - datetime.timedelta(days=days)).isoformat()


def _moved_here(email: str, key: str, moved_on: str) -> dict:
    """A holder whose address belonged to resource ``other`` until ``moved_on``."""
    member = _member(email, key=key)
    periods = ownership.release(ownership.claim([], "other", _days_ago(30)), "other", moved_on)
    periods = ownership.claim(periods, RID, moved_on)
    member["metadata"] = ownership.stamp(member["metadata"], periods)
    return member


def test_chat_from_before_the_holder_moved_here_stays_on_the_keys_budget() -> None:
    # The report bills the day of the move to the resource Ada left, so Waldur shows the
    # key at 0. Taking it off here would refuse the key with nothing in Waldur saying why.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_moved_here(ADA, f"{RID}-1", _days_ago(0))]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 5.0)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 10
    assert META_HOLDER_CHAT not in metadata


def test_only_chat_from_the_days_the_holder_was_here_comes_off() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_moved_here(ADA, f"{RID}-1", _days_ago(1))]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _chat(ADA, 5.0, date=_days_ago(2)),
        # The day of the move stays with the resource left, as in the report.
        _chat(ADA, 1.0, date=_days_ago(1)),
        _chat(ADA, 2.0),
    ]

    backend.set_resource_limits(RID, {"token_cost": 10})

    assert _budget_writes(backend)[f"h-{RID}-1"][0] == 8.0


def test_a_rotation_takes_the_holders_chat_off_the_keys_budget() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1"),
        _key(f"{RID}-2", **{META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 3.0)]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(
        f"{RID}-1", RID, limits={"token_cost": 10}, allowed_models=["gpt-4o"]
    )

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 7.0
    assert metadata[META_HOLDER_CHAT] == 3.0


def test_a_key_the_proxy_lost_is_minted_back_net_of_its_holders_chat() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-2", **{META_RESOURCE_LIMITS: {"token_cost": 50}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 3.0)]

    backend.rotate_resource_key(
        f"{RID}-1", RID, limits={"token_cost": 10}, allowed_models=["gpt-4o"]
    )

    minted = backend.litellm_client.generate_key.call_args.kwargs
    assert minted["max_budget"] == 7.0
    assert minted["metadata"][META_HOLDER_CHAT] == 3.0


def test_the_deduction_applies_to_the_tighter_of_the_key_and_the_resource() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(BOB, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_KEY_LIMITS: {"token_cost": 7}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(BOB, 3.78)]

    backend.set_resource_limits(RID, {"token_cost": 30})

    assert _budget_writes(backend)[f"h-{RID}-1"][0] == 3.22


def test_chat_past_the_limit_leaves_a_budget_of_zero() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 12.5)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    update = backend.litellm_client.update_key.call_args.args[1]
    # Still a budget, and still one that resets: 0 refuses the key's next call.
    assert update["max_budget"] == 0.0
    assert update["budget_duration"] == "1mo"


def test_api_calls_under_the_holders_address_are_not_counted_as_chat() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    # Served on a key this plugin minted: the key's own spend, which the proxy counts.
    backend.usage_client.get_user_usage_rows.return_value = [
        _chat(ADA, 2.0),
        _chat(ADA, 5.0, alias=f"{RID}-1"),
    ]

    backend.set_resource_limits(RID, {"token_cost": 10})

    assert _budget_writes(backend)[f"h-{RID}-1"][0] == 8.0


def test_each_key_loses_only_its_own_holders_chat() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [
        _member(ADA, key=f"{RID}-1"),
        _member(BOB, key=f"{RID}-2"),
    ]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1"), _key(f"{RID}-2")]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 1.0), _chat(BOB, 4.0)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    writes = _budget_writes(backend)
    assert writes[f"h-{RID}-1"][0] == 9.0
    assert writes[f"h-{RID}-2"][0] == 6.0


def test_a_key_with_no_holder_keeps_its_whole_budget_and_nothing_is_read() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key="")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    backend.set_resource_limits(RID, {"token_cost": 10})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 10
    assert META_HOLDER_CHAT not in metadata
    backend.usage_client.get_user_usage_rows.assert_not_called()


def test_an_unreadable_chat_spend_keeps_the_amount_last_taken_off() -> None:
    from waldur_site_agent_litellm.usage_client import LiteLLMUsageBackendError  # noqa: PLC0415

    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_HOLDER_CHAT: 2.0, META_RESOURCE_LIMITS: {"token_cost": 10}}),
    ]
    backend.usage_client.get_user_usage_rows.side_effect = LiteLLMUsageBackendError("down")

    backend.set_resource_limits(RID, {"token_cost": 20})

    # The resource's new limit still lands; the chat already taken off stays off.
    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 18.0
    assert metadata[META_HOLDER_CHAT] == 2.0


def test_an_unchanged_deduction_is_not_written_again() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(
        f"{RID}-1", **{META_HOLDER_CHAT: 3.0, META_RESOURCE_LIMITS: {"token_cost": 10}}
    )
    record.update(max_budget=7.0, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 3.0)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    backend.litellm_client.update_key.assert_not_called()


def test_new_chat_since_the_last_pass_lowers_the_budget() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(
        f"{RID}-1", **{META_HOLDER_CHAT: 3.0, META_RESOURCE_LIMITS: {"token_cost": 10}}
    )
    record.update(max_budget=7.0, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 3.0), _chat(ADA, 1.5)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 5.5
    assert metadata[META_HOLDER_CHAT] == 4.5


def test_a_key_update_takes_its_holders_chat_off_too() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 30}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.5)]

    backend.update_resource_key(f"{RID}-1", RID, limits={"token_cost": 7})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 4.5
    assert metadata[META_HOLDER_CHAT] == 2.5
    assert metadata[META_KEY_LIMITS] == {"token_cost": 7}


def test_the_chat_spend_is_read_once_per_pass() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 1.0)]

    backend.set_resource_limits(RID, {"token_cost": 10})
    backend.set_resource_limits(RID, {"token_cost": 10})

    backend.usage_client.get_user_usage_rows.assert_called_once()


def test_an_api_only_offering_reads_no_chat_spend() -> None:
    api_only = {key: value for key, value in MONTHLY.items() if key != "openwebui"}
    assert _make_backend(api_only).usage_client is None


@pytest.mark.parametrize("duration", ["1mo", "7d", None])
def test_chat_is_read_whatever_budget_duration_says(duration: Optional[str]) -> None:
    # The backstop always resets on the 1st, so a month of chat always fits it.
    backend = _make_backend({**SETTINGS, "budget_duration": duration})

    assert backend.usage_client is not None


@pytest.mark.parametrize(
    ("now", "period"),
    [
        (datetime.datetime(2026, 10, 31, 23, 59, tzinfo=datetime.timezone.utc),
         ("2026-10-01", "2026-10-31")),
        # A Sunday: the week's window reaches back to Monday, in October.
        (datetime.datetime(2026, 11, 1, 0, 0, 30, tzinfo=datetime.timezone.utc),
         ("2026-10-26", "2026-11-01")),
    ],
)
def test_the_chat_read_is_this_utc_months(now: datetime.datetime, period: tuple) -> None:
    # The proxy resets a monthly budget on the 1st at 00:00 UTC, so the chat taken off
    # it is the UTC month's: on an agent east of UTC, a local date would read the new
    # month for hours while the proxy still counts the old one. The read starts at the
    # month or the week, whichever is earlier, for the weekly window.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]

    with mock.patch("waldur_site_agent_litellm.backend.datetime") as clock:
        clock.timezone.utc = datetime.timezone.utc
        clock.timedelta = datetime.timedelta
        clock.datetime.now.return_value = now
        backend.set_resource_limits(RID, {"token_cost": 10})

    clock.datetime.now.assert_called_with(tz=datetime.timezone.utc)
    backend.usage_client.get_user_usage_rows.assert_called_once_with(*period)


def test_a_new_month_reads_the_chat_again() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    times = [
        datetime.datetime(2026, 10, 31, 23, 59, tzinfo=datetime.timezone.utc),
        datetime.datetime(2026, 11, 1, 0, 1, tzinfo=datetime.timezone.utc),
    ]

    with mock.patch("waldur_site_agent_litellm.backend.datetime") as clock:
        clock.timezone.utc = datetime.timezone.utc
        clock.timedelta = datetime.timedelta
        clock.datetime.now.side_effect = times
        backend.set_resource_limits(RID, {"token_cost": 10})
        backend.set_resource_limits(RID, {"token_cost": 10})

    # Within the cache's lifetime, but a cached October is not November's chat.
    assert [call.args for call in backend.usage_client.get_user_usage_rows.call_args_list] == [
        ("2026-10-01", "2026-10-31"),
        ("2026-10-26", "2026-11-01"),
    ]


def test_a_key_without_a_budget_records_no_deduction() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"tpm": 1000}})
    record.update(max_budget=None, tpm_limit=1000, rpm_limit=None, budget_duration=None)
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 3.0)]

    backend.set_resource_limits(RID, {"tpm": 1000})

    # Nothing was taken off, so nothing is noted, and the key is not rewritten each pass.
    backend.litellm_client.update_key.assert_not_called()


def test_a_reassigned_key_loses_its_new_holders_chat() -> None:
    backend = _make_backend(MONTHLY)
    # The key was Ada's, with her 1.0 of chat taken off; it is stamped on Bob now.
    backend.litellm_client.list_users.return_value = [
        _member(ADA, key=""),
        _member(BOB, key=f"{RID}-1"),
    ]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_HOLDER_CHAT: 1.0, META_RESOURCE_LIMITS: {"token_cost": 10}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 1.0), _chat(BOB, 4.0)]

    backend.set_resource_limits(RID, {"token_cost": 10})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 6.0
    assert metadata[META_HOLDER_CHAT] == 4.0


def test_a_resumed_key_takes_its_holders_chat_off() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_KEY_PAUSED: True, META_RESOURCE_LIMITS: {"token_cost": 10}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.0)]

    backend.resume_resource_key(f"{RID}-1", RID)

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 8.0
    assert metadata[META_HOLDER_CHAT] == 2.0
    assert metadata[META_KEY_PAUSED] is False


def test_an_update_with_unreadable_chat_keeps_the_amount_last_taken_off() -> None:
    from waldur_site_agent_litellm.usage_client import LiteLLMUsageBackendError  # noqa: PLC0415

    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_HOLDER_CHAT: 2.0, META_RESOURCE_LIMITS: {"token_cost": 30}}),
    ]
    backend.usage_client.get_user_usage_rows.side_effect = LiteLLMUsageBackendError("down")

    backend.update_resource_key(f"{RID}-1", RID, limits={"token_cost": 7})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 5.0
    assert metadata[META_HOLDER_CHAT] == 2.0


def test_the_status_pass_puts_last_months_chat_back_on_the_1st() -> None:
    # Under STOMP no limits pass runs on a schedule; the hourly status pass restores
    # every serving resource, and that recomputes the chat taken off from the stamps.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(
        f"{RID}-1", **{META_HOLDER_CHAT: 6.5, META_RESOURCE_LIMITS: {"token_cost": 7}}
    )
    record.update(max_budget=0.5, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = []

    backend.restore_resource(RID)

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 7
    assert META_HOLDER_CHAT not in metadata
    assert metadata[META_RESOURCE_LIMITS] == {"token_cost": 7}


def test_the_status_pass_takes_new_chat_off() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 10}})
    record.update(max_budget=10, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.0)]

    backend.restore_resource(RID)

    assert _budget_writes(backend)[f"h-{RID}-1"][0] == 8.0


def test_the_status_pass_leaves_a_key_never_reconciled_to_the_limits_sync() -> None:
    # No stamped resource limits to start from: writing would drop the key's budget.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(f"{RID}-1")
    record.update(max_budget=10, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.0)]

    backend.restore_resource(RID)

    assert _budget_writes(backend) == {}


def test_the_status_pass_writes_no_budget_where_chat_is_not_taken_off() -> None:
    api_only = {key: value for key, value in MONTHLY.items() if key != "openwebui"}
    backend = _make_backend(api_only)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 10}}),
    ]

    backend.restore_resource(RID)

    assert _budget_writes(backend) == {}


def test_a_budget_write_on_the_1st_leaves_the_reset_alone() -> None:
    # Naming budget_duration moves the key's budget_reset_at to the next reset; on the
    # 1st, before the proxy's reset job, that would skip this month's reset.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(
        f"{RID}-1", **{META_HOLDER_CHAT: 3.78, META_RESOURCE_LIMITS: {"token_cost": 7}}
    )
    record.update(max_budget=3.22, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]

    backend.restore_resource(RID)

    update = backend.litellm_client.update_key.call_args.args[1]
    assert update["max_budget"] == 7
    assert "budget_duration" not in update


def test_a_key_update_names_the_budget_duration_only_when_it_changes() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    record = _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 30}})
    record.update(max_budget=30, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]

    backend.update_resource_key(f"{RID}-1", RID, limits={"token_cost": 7})
    assert "budget_duration" not in backend.litellm_client.update_key.call_args.args[1]

    # A key that had no budget gets one, and with it a reset schedule.
    record.update(max_budget=None, budget_duration=None)
    backend.update_resource_key(f"{RID}-1", RID, limits={"token_cost": 7})
    assert backend.litellm_client.update_key.call_args.args[1]["budget_duration"] == "1mo"


def _writes_under_the_lock(backend: LiteLLMBackend) -> list:
    """Record, for each ``/key/update``, whether another thread was kept out of the lock."""
    held_by_writer: list = []

    def update_key(token: str, payload: dict) -> None:
        def try_acquire() -> None:
            acquired = _KEY_METADATA_LOCK.acquire(blocking=False)
            held_by_writer.append(not acquired)
            if acquired:
                _KEY_METADATA_LOCK.release()

        other = threading.Thread(target=try_acquire)
        other.start()
        other.join()

    backend.litellm_client.update_key.side_effect = update_key
    return held_by_writer


@pytest.mark.parametrize(
    ("self_paused", "write"),
    [
        (False, lambda backend: backend.restore_resource(RID)),
        (False, lambda backend: backend.pause_resource_key(f"{RID}-1", RID)),
        (False, lambda backend: backend.update_resource_key(f"{RID}-1", RID, limits={})),
        (True, lambda backend: backend.resume_resource_key(f"{RID}-1", RID)),
        (True, lambda backend: backend.rotate_resource_key(f"{RID}-1", RID)),
    ],
    ids=["status pass", "pause", "update", "resume", "rotate"],
)
def test_every_metadata_write_holds_the_lock(self_paused: bool, write) -> None:
    # The status pass rewrites budgets on the main thread while STOMP handlers pause
    # and update keys on theirs: a read-modify-write on either side that skipped the
    # lock could drop the other's metadata, a key's own pause included.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    meta = {META_RESOURCE_LIMITS: {"token_cost": 10}}
    if self_paused:
        meta[META_KEY_PAUSED] = True
    record = _key(f"{RID}-1", **meta)
    record.update(max_budget=10, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.litellm_client.regenerate_key.return_value = "sk-new"
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.0)]
    held_by_writer = _writes_under_the_lock(backend)

    write(backend)

    assert held_by_writer
    assert all(held_by_writer)


def test_a_key_minted_for_someone_loses_their_chat_once_it_is_stamped() -> None:
    # After a mint the core syncs assignees at once; the stamp is what makes the key
    # the holder's, so the budget is netted there rather than an hour later.
    backend = _make_backend(MONTHLY)
    users = [_member(ADA, key="")]
    backend.litellm_client.list_users.side_effect = lambda *args, **kwargs: list(users)

    def stamp(email: str, payload: dict) -> None:
        users[0] = {**users[0], "metadata": payload["metadata"]}

    backend.litellm_client.update_user.side_effect = stamp
    record = _key(f"{RID}-2", **{META_RESOURCE_LIMITS: {"token_cost": 7}})
    record.update(max_budget=7, tpm_limit=None, rpm_limit=None, budget_duration="1mo")
    backend.litellm_client.list_keys.return_value = [record]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 6.5)]

    backend.sync_resource_key_assignees(_resource(), {f"{RID}-2": "ada"})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-2"]
    assert budget == 0.5
    assert metadata[META_HOLDER_CHAT] == 6.5


def test_a_rotation_with_settings_takes_the_holders_chat_off_too() -> None:
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 30}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.5)]
    backend.litellm_client.regenerate_key.return_value = "sk-rotated"

    backend.rotate_resource_key(f"{RID}-1", RID, limits={"token_cost": 7})

    budget, metadata = _budget_writes(backend)[f"h-{RID}-1"]
    assert budget == 4.5
    assert metadata[META_HOLDER_CHAT] == 2.5


def test_a_delete_and_mint_rotation_keeps_the_budget_net_of_chat() -> None:
    from waldur_site_agent_litellm.client import LiteLLMEnterpriseFeatureError  # noqa: PLC0415

    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [
        _key(f"{RID}-1", **{META_RESOURCE_LIMITS: {"token_cost": 30}}),
    ]
    backend.usage_client.get_user_usage_rows.return_value = [_chat(ADA, 2.5)]
    backend.litellm_client.regenerate_key.side_effect = LiteLLMEnterpriseFeatureError("ee")
    backend.litellm_client.generate_key.return_value = {"key": "sk-new"}

    backend.rotate_resource_key(f"{RID}-1", RID, limits={"token_cost": 7})

    kwargs = backend.litellm_client.generate_key.call_args.kwargs
    assert kwargs["max_budget"] == 4.5
    assert kwargs["metadata"][META_HOLDER_CHAT] == 2.5


def test_chat_with_no_per_key_split_on_a_day_keys_served_is_not_taken_off() -> None:
    # The usage report bills such a row as nobody's chat, and its address is whatever
    # the request's header named: taking it off would let a key holder shrink
    # another person's budget.
    backend = _make_backend(MONTHLY)
    backend.litellm_client.list_users.return_value = [_member(ADA, key=f"{RID}-1")]
    backend.litellm_client.list_keys.return_value = [_key(f"{RID}-1")]
    backend.usage_client.get_user_usage_rows.return_value = [
        _chat(ADA, 2.0),
        {**_chat(ADA, 5.0), "day_key_aliases": [f"{RID}-2", None]},
        {**_chat(ADA, 1.0), "day_key_aliases": [None]},
    ]

    backend.set_resource_limits(RID, {"token_cost": 10})

    assert _budget_writes(backend)[f"h-{RID}-1"][0] == 7.0
