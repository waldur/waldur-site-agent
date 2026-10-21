"""Tests for the Open WebUI admin HTTP client."""

from __future__ import annotations

from unittest import mock

import httpx
import pytest
from waldur_site_agent_litellm.openwebui import ROLE_ACTIVE, OpenWebUIClient, OpenWebUIError

SETTINGS = {"api_url": "https://chat.example.com/", "api_token": "owui-admin"}


def _client() -> OpenWebUIClient:
    return OpenWebUIClient(dict(SETTINGS))


def _response(status: int = 200, json_body: object = None, text: str = "") -> mock.Mock:
    response = mock.Mock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.text = text
    response.json.return_value = json_body
    if response.is_success:
        response.raise_for_status = mock.Mock()
    else:
        response.raise_for_status = mock.Mock(
            side_effect=httpx.HTTPStatusError("boom", request=mock.Mock(), response=response)
        )
    return response


def test_requires_api_url() -> None:
    with pytest.raises(OpenWebUIError):
        OpenWebUIClient({"api_token": "owui-admin"})


def test_requires_api_token() -> None:
    with pytest.raises(OpenWebUIError):
        OpenWebUIClient({"api_url": "https://chat.example.com"})


def test_set_role_sends_the_whole_account_because_update_is_a_replace() -> None:
    # Open WebUI's UserUpdateForm requires role, name, email and profile_image_url on
    # every released version (they became optional only after v0.6.34), and the handler
    # writes all four unconditionally. A body carrying just the role is a 422 there,
    # and on a build that accepts it the other three would be nulled out.
    client = _client()
    client.session = mock.MagicMock()
    client.session.request.return_value = _response(json_body={"id": "owui-1"})

    client.set_role(
        {
            "id": "owui-1",
            "name": "Ada Lovelace",
            "email": "Ada@Example.com",
            "profile_image_url": "/cache/ada.png",
            "role": "pending",
        },
        ROLE_ACTIVE,
    )

    _, kwargs = client.session.request.call_args
    assert kwargs["json"] == {
        "role": ROLE_ACTIVE,
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "profile_image_url": "/cache/ada.png",
    }


def test_set_role_falls_back_to_a_default_avatar_when_the_record_has_none() -> None:
    # profile_image_url is non-nullable on the released form, so an account record
    # without one still has to carry a value or the update is refused.
    client = _client()
    client.session = mock.MagicMock()
    client.session.request.return_value = _response(json_body={"id": "owui-1"})

    client.set_role({"id": "owui-1", "name": "Ada", "email": "ada@example.com"}, ROLE_ACTIVE)

    assert client.session.request.call_args.kwargs["json"]["profile_image_url"] == "/user.png"


def test_find_user_matches_the_address_exactly_not_as_a_substring() -> None:
    # The listing query is a substring match on name and email, so a loose match would
    # hand ann@x.org's account to joann@x.org.
    client = _client()
    client.session = mock.MagicMock()
    client.session.request.return_value = _response(
        json_body={
            "users": [
                {"id": "owui-2", "email": "joann@x.org"},
                {"id": "owui-1", "email": "Ann@x.org"},
            ],
            "total": 2,
        }
    )

    assert client.find_user("ann@x.org")["id"] == "owui-1"
    assert client.find_user("zoe@x.org") is None


def test_a_bare_list_from_the_user_endpoint_is_still_searched() -> None:
    # Pre-UserListResponse builds answer with a list rather than an envelope.
    client = _client()
    client.session = mock.MagicMock()
    client.session.request.return_value = _response(
        json_body=[{"id": "u1", "email": "ada@example.com"}]
    )
    assert client.find_user("ada@example.com") == {"id": "u1", "email": "ada@example.com"}


def test_an_unreadable_user_payload_raises_rather_than_reading_as_no_such_account() -> None:
    # Returning None would turn every revoke into a silent no-op.
    client = _client()
    client.session = mock.MagicMock()
    client.session.request.return_value = _response(json_body="nonsense")
    with pytest.raises(OpenWebUIError):
        client.find_user("ada@example.com")


# --- groups and model access ----------------------------------------------------


def _client_with(json_body: object) -> OpenWebUIClient:
    client = _client()
    client.session = mock.MagicMock()
    client.session.request.return_value = _response(json_body=json_body)
    return client


def test_a_user_is_added_to_a_group_one_per_call() -> None:
    # On 0.11.3 a batch with an existing member rolls back the new ones and answers 200.
    client = _client_with({"id": "g-1", "member_count": 1})

    client.add_user_to_group("g-1", "owui-1")

    method, url = client.session.request.call_args.args
    assert (method, url) == ("POST", "https://chat.example.com/api/v1/groups/id/g-1/users/add")
    assert client.session.request.call_args.kwargs["json"] == {"user_ids": ["owui-1"]}


def test_a_user_is_removed_from_a_group() -> None:
    client = _client_with({"id": "g-1", "member_count": 0})

    client.remove_user_from_group("g-1", "owui-1")

    assert client.session.request.call_args.args[1].endswith("/api/v1/groups/id/g-1/users/remove")
    assert client.session.request.call_args.kwargs["json"] == {"user_ids": ["owui-1"]}


def test_a_model_is_granted_to_its_one_group_only() -> None:
    client = _client_with({"id": "gpt-4o"})

    client.grant_model_to_group("gpt-4o", "g-1")

    assert client.session.request.call_args.args[1].endswith("/api/v1/models/model/access/update")
    assert client.session.request.call_args.kwargs["json"] == {
        "id": "gpt-4o",
        "name": "gpt-4o",
        "access_grants": [{"principal_type": "group", "principal_id": "g-1", "permission": "read"}],
    }


def test_a_users_groups_are_read_by_id() -> None:
    client = _client_with([{"id": "g-1", "name": "waldur-model-gpt-4o"}, {"name": "no id"}])

    assert client.user_group_ids("owui-1") == {"g-1"}
    assert client.session.request.call_args.args[1].endswith("/api/v1/users/owui-1/groups")


def test_a_group_listing_of_the_wrong_shape_is_an_error() -> None:
    with pytest.raises(OpenWebUIError):
        _client_with({"detail": "nope"}).list_groups()


def test_a_created_group_without_an_id_is_an_error() -> None:
    with pytest.raises(OpenWebUIError):
        _client_with({"name": "waldur-model-gpt-4o"}).create_group("waldur-model-gpt-4o", "")
