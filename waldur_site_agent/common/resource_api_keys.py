"""Commands Waldur sends the agent about one resource API key, and per-key usage.

Every command follows rotation's shape: the agent applies the change to the backend
first and acknowledges it to Waldur second, through the provider endpoint that answers
that command, so a key Waldur shows as settled is one the backend already reflects.
A failure is reported with ``set_erred``, which Waldur accepts only while the command
is in flight.

| Command  | Backend call           | Acknowledgement |
|----------|------------------------|-----------------|
| create   | ``mint_resource_key``  | ``set_key``     |
| rotate   | ``rotate_resource_key``| ``set_key``     |
| pause    | ``pause_resource_key`` | ``set_paused``  |
| resume   | ``resume_resource_key``| ``set_ok``      |
| update   | ``update_resource_key``| ``set_ok``      |
| delete   | ``delete_resource_key``| ``set_deleted`` |

Rotation is available to every backend with ``supports_resource_api_keys``; the other
commands need ``supports_resource_api_key_lifecycle`` as well.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
from waldur_api_client import AuthenticatedClient, errors
from waldur_api_client.api.marketplace_offering_users import marketplace_offering_users_list
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.api.marketplace_resource_api_keys import (
    marketplace_resource_api_keys_list,
    marketplace_resource_api_keys_report_usage,
    marketplace_resource_api_keys_retrieve,
    marketplace_resource_api_keys_set_deleted,
    marketplace_resource_api_keys_set_erred,
    marketplace_resource_api_keys_set_key,
    marketplace_resource_api_keys_set_ok,
    marketplace_resource_api_keys_set_paused,
)
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.resource_api_key_set_erred_request import (
    ResourceApiKeySetErredRequest,
)
from waldur_api_client.models.resource_api_key_set_key_request import (
    ResourceApiKeySetKeyRequest,
)
from waldur_api_client.models.resource_api_key_state import ResourceApiKeyState
from waldur_api_client.models.resource_api_key_status import ResourceApiKeyStatus
from waldur_api_client.models.resource_api_key_usage_request import ResourceApiKeyUsageRequest
from waldur_api_client.models.resource_api_key_usage_request_usages import (
    ResourceApiKeyUsageRequestUsages,
)
from waldur_api_client.models.resource_field_enum import ResourceFieldEnum
from waldur_api_client.types import UNSET, Unset

from waldur_site_agent.backend import UNASSIGNED_USAGE_SUFFIX, logger
from waldur_site_agent.common import spending_limits, structures, utils
from waldur_site_agent.common.healthz import touch_heartbeat

# The command vocabulary, mirroring mastermind's ResourceApiKeyActions.
CREATE = "create"
ROTATE = "rotate"
PAUSE = "pause"
RESUME = "resume"
DELETE = "delete"
UPDATE = "update"
ACTIONS = (CREATE, ROTATE, PAUSE, RESUME, DELETE, UPDATE)

# Naming every state lists deleted keys too, which Waldur leaves out otherwise.
ALL_STATES = list(ResourceApiKeyState)

HTTP_BAD_REQUEST = 400
HTTP_SERVER_ERROR = 500

# Offering users in these states hold an entitlement. The membership pass filters on
# the same set (``OfferingMembershipProcessor._get_waldur_offering_users``); a key
# assigned to anyone else -- someone who left the project, say -- has no assignee the
# backend may act for.
_ACTIONABLE_OFFERING_USER_STATES = (
    OfferingUserState.OK,
    OfferingUserState.REQUESTED,
    OfferingUserState.CREATING,
    OfferingUserState.ERROR_CREATING,
    OfferingUserState.PENDING_ACCOUNT_LINKING,
    OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
)


class _NotYet(Exception):  # noqa: N818 - a signal, not an error
    """Waldur could not be read before anything was applied; leave the command pending.

    The next pass over pending commands replays it. Reporting it erred instead would
    end a key request for good: Waldur only lets a key that erred before it had a
    client_id be deleted.
    """


class _Withdrawn(Exception):  # noqa: N818 - a signal, not an error
    """Waldur refused a created key, which was withdrawn from the backend.

    Waldur answered, so the key there is settled — typically the request was
    deleted while the agent created it — and reporting the command erred would only
    be refused in turn.
    """


@dataclass
class ApiKeyCommand:
    """One command about one key, from a STOMP message or a stuck key's listing row."""

    action: str
    api_key_uuid: str
    resource_uuid: str
    resource_backend_id: str
    client_id: str = ""
    limits: Optional[dict] = None
    allowed_models: Optional[list] = field(default=None)
    # Per period, ``{component: {period: amount}}``; None from a Waldur whose keys
    # have only the month limits in ``limits``, or for a key with no limits.
    spending_limits: Optional[dict] = None

    @classmethod
    def from_message(cls, message: dict) -> ApiKeyCommand:
        """Build a command from a STOMP payload (``api_key_uuid`` and ``action``)."""
        return cls(
            action=message.get("action") or "",
            api_key_uuid=message.get("api_key_uuid") or "",
            resource_uuid=message.get("resource_uuid") or "",
            resource_backend_id=message.get("resource_backend_id") or "",
            client_id=message.get("client_id") or "",
            limits=message.get("limits"),
            allowed_models=message.get("allowed_models"),
            spending_limits=spending_limits.key_spending_limits(message.get("spending_limits")),
        )

    @classmethod
    def from_listing(cls, row: ResourceApiKeyStatus, action: str) -> ApiKeyCommand:
        """Build the command a listed key is waiting on."""
        return cls(
            action=action,
            api_key_uuid=str(row.uuid),
            resource_uuid=str(row.resource_uuid),
            resource_backend_id=row.resource_backend_id or "",
            client_id=_value(row.client_id) or "",
            limits=row.limits.additional_properties if row.limits else None,
            allowed_models=_effective_models(row),
            spending_limits=spending_limits.key_spending_limits(
                spending_limits.generated_field(row, "spending_limits")
            ),
        )


def _key_settings(backend, command: ApiKeyCommand) -> dict:  # noqa: ANN001
    """The settings a create, resume or update hands the backend.

    ``spending_limits`` only to a backend that takes limits per period: the others
    keep the signature they were written against.
    """
    settings: dict = {"limits": command.limits, "allowed_models": command.allowed_models}
    if getattr(backend, "supports_spending_limit_periods", False) is True:
        settings["spending_limits"] = command.spending_limits
    return settings


def _value(field: Any) -> Any:  # noqa: ANN401 - any optional field of a generated model
    return None if isinstance(field, Unset) else field


def _effective_models(row: ResourceApiKeyStatus) -> Optional[list]:
    """The model list a command for this key would have carried.

    Waldur stores a key's own list as ``allowed_models`` and sends the one its
    resource's selection narrows it to, which the listing gives as
    ``effective_models``. A Waldur that predates it narrows nothing, so its
    ``allowed_models`` is what it would have sent. Read through the generated
    model when the client knows the field, and from the extra JSON when not.
    """
    effective = getattr(row, "effective_models", UNSET)
    if isinstance(effective, Unset):
        effective = row.additional_properties.get("effective_models", UNSET)
    if isinstance(effective, Unset):
        return row.allowed_models
    return effective


def list_all_api_keys(
    waldur_rest_client: AuthenticatedClient, resource_uuid: str
) -> list[ResourceApiKeyStatus]:
    """Every key Waldur holds for the resource, deleted keys included."""
    return marketplace_resource_api_keys_list.sync_all(
        client=waldur_rest_client,
        resource_uuid=resource_uuid,  # type: ignore[arg-type]
        state=ALL_STATES,
    )


def _reserved_client_ids(waldur_rest_client: AuthenticatedClient, resource_uuid: str) -> list[str]:
    """Every client_id Waldur holds for the resource, deleted keys included.

    Raises when the listing fails: minting against a partial set could hand out a
    deleted key's identifier, which Waldur refuses — after the key already went live.
    """
    keys = list_all_api_keys(waldur_rest_client, resource_uuid)
    client_ids = (_value(key.client_id) for key in keys)
    return sorted({client_id for client_id in client_ids if client_id})


def _is_refusal(exc: Exception) -> bool:
    """Whether Waldur answered and refused, as opposed to never answering."""
    return (
        isinstance(exc, errors.UnexpectedStatus)
        and HTTP_BAD_REQUEST <= exc.status_code < HTTP_SERVER_ERROR
    )


def _apply_resource_pause(
    backend,  # noqa: ANN001
    resource: object,
    resource_backend_id: str,
) -> None:
    """Bring the backend's resource-wide pause up to date before a key is added.

    The membership sync applies it too, but only to resources the backend reports as
    existing — and a backend can report a resource with no keys left as missing. A
    key minted onto such a resource after Waldur paused it would otherwise go live.
    """
    if getattr(resource, "paused", None) is True:
        backend.pause_resource(resource_backend_id)
    elif getattr(resource, "downscaled", None) is True:
        backend.downscale_resource(resource_backend_id)
    else:
        # Clears a pause recorded while the resource had no keys, which the
        # membership sync — skipping keyless resources — never got to lift.
        backend.restore_resource(resource_backend_id)


def _read_resource(waldur_rest_client: AuthenticatedClient, command: ApiKeyCommand) -> object:
    """The key's resource, with what a command needs before it touches the backend."""
    try:
        return marketplace_provider_resources_retrieve.sync(
            uuid=command.resource_uuid,  # type: ignore[arg-type]
            client=waldur_rest_client,
            field=[
                ResourceFieldEnum.PAUSED,
                ResourceFieldEnum.DOWNSCALED,
                # The resource's share of a new key's limits, for a resource with no
                # keys yet to read them back from.
                ResourceFieldEnum.LIMITS,
                # And its day/week/month windows, for the same reason: without them a
                # first key would serve past the resource's day limit until the next
                # membership sync stamped it.
                *spending_limits.resource_fields(),
                # For the backend's assignee hook, after a key is created.
                ResourceFieldEnum.UUID,
                ResourceFieldEnum.BACKEND_ID,
                ResourceFieldEnum.OFFERING_UUID,
                ResourceFieldEnum.OFFERING_PLUGIN_OPTIONS,
            ],
        )
    except (httpx.TransportError, errors.UnexpectedStatus) as exc:
        if isinstance(exc, errors.UnexpectedStatus) and exc.status_code < HTTP_SERVER_ERROR:
            raise
        # Waldur did not answer and nothing is applied yet: try again later.
        raise _NotYet(str(exc)) from exc


def _resource_limits(resource: object) -> Optional[dict]:
    """The resource's limits as Waldur holds them; None when the read carried none."""
    limits = _value(getattr(resource, "limits", None))
    if limits is None:
        return None
    if isinstance(limits, dict):
        return dict(limits)
    to_dict = getattr(limits, "to_dict", None)
    data = to_dict() if callable(to_dict) else None
    return dict(data) if isinstance(data, dict) else None


def _resource_key_limits(backend, resource: object) -> dict:  # noqa: ANN001
    """The resource's limits a mint or rotation hands the backend.

    ``resource_spending_limits`` only to a backend that takes limits per period, as
    ``spending_limits`` in ``_key_settings``: the others keep the signature they were
    written against. None in either means the read did not carry it.
    """
    settings: dict = {"resource_limits": _resource_limits(resource)}
    if getattr(backend, "supports_spending_limit_periods", False) is True:
        settings["resource_spending_limits"] = spending_limits.resource_spending_limits(
            resource
        )
    return settings


def _create(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001 - BaseBackend would be a circular import
    command: ApiKeyCommand,
) -> None:
    try:
        reserved = _reserved_client_ids(waldur_rest_client, command.resource_uuid)
    except (httpx.TransportError, errors.UnexpectedStatus) as exc:
        if isinstance(exc, errors.UnexpectedStatus) and exc.status_code < HTTP_SERVER_ERROR:
            raise
        # Waldur did not answer and nothing is applied yet: try again later.
        raise _NotYet(str(exc)) from exc
    resource = _read_resource(waldur_rest_client, command)
    _apply_resource_pause(backend, resource, command.resource_backend_id)
    key = backend.mint_resource_key(
        command.resource_backend_id,
        reserved,
        **_resource_key_limits(backend, resource),
        **_key_settings(backend, command),
    )
    try:
        marketplace_resource_api_keys_set_key.sync(
            uuid=command.api_key_uuid,
            client=waldur_rest_client,
            body=ResourceApiKeySetKeyRequest(api_key=key["api_key"], client_id=key["client_id"]),
        )
    except Exception as exc:
        # A refused report means Waldur holds no row for a key that is already live,
        # and nobody else knows its value: withdraw it. A report that never got an
        # answer may have landed, so Waldur is asked: withdrawing a key it holds
        # would break one it shows as working, but keeping one it does not hold
        # leaves a live key nobody knows, and the retried request mints another.
        if _is_refusal(exc):
            _withdraw(backend, key["client_id"], command, "Waldur refused it")
            raise _Withdrawn(str(exc)) from exc
        if _set_key_landed(waldur_rest_client, command, key["client_id"]) is False:
            _withdraw(backend, key["client_id"], command, "its report did not reach Waldur")
        raise
    _sync_key_assignees_now(waldur_rest_client, backend, resource)


def _withdraw(backend, client_id: str, command: ApiKeyCommand, why: str) -> None:  # noqa: ANN001
    logger.warning("Withdrawing key %s of API key %s: %s", client_id, command.api_key_uuid, why)
    try:
        backend.delete_resource_key(client_id, command.resource_backend_id)
    except Exception:
        logger.exception("Could not withdraw key %s after %s", client_id, why)


def _set_key_landed(
    waldur_rest_client: AuthenticatedClient, command: ApiKeyCommand, client_id: str
) -> Optional[bool]:
    """Whether Waldur holds ``client_id`` for the key; None when it cannot be told."""
    try:
        key = marketplace_resource_api_keys_retrieve.sync(
            uuid=command.api_key_uuid,  # type: ignore[arg-type]
            client=waldur_rest_client,
        )
    except Exception:
        logger.exception(
            "Cannot tell whether key %s of API key %s reached Waldur; leaving it in place",
            client_id,
            command.api_key_uuid,
        )
        return None
    return _value(key.client_id) == client_id


def _acknowledge(
    endpoint: Any,  # noqa: ANN401 - one of the generated set_* modules
    waldur_rest_client: AuthenticatedClient,
    command: ApiKeyCommand,
) -> None:
    endpoint.sync(uuid=command.api_key_uuid, client=waldur_rest_client)


def _run(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    command: ApiKeyCommand,
) -> None:
    action = command.action
    if action == CREATE:
        _create(waldur_rest_client, backend, command)
        return
    if action == DELETE and not command.client_id:
        # A requested key that never got a client_id never reached the backend.
        _acknowledge(marketplace_resource_api_keys_set_deleted, waldur_rest_client, command)
        return
    if not command.client_id:
        msg = f"{action} command for API key {command.api_key_uuid} carries no client_id"
        raise ValueError(msg)
    if action == PAUSE:
        backend.pause_resource_key(command.client_id, command.resource_backend_id)
        _acknowledge(marketplace_resource_api_keys_set_paused, waldur_rest_client, command)
    elif action == RESUME:
        # A key of a paused resource stays held, and a backend can only tell a key
        # paused on its own from one paused with its resource if it knows the latter.
        # It cannot always tell from its keys -- not when every one of them is paused
        # on its own -- so Waldur's flag is applied first, as before a create.
        _apply_resource_pause(
            backend, _read_resource(waldur_rest_client, command), command.resource_backend_id
        )
        backend.resume_resource_key(
            command.client_id, command.resource_backend_id, **_key_settings(backend, command)
        )
        _acknowledge(marketplace_resource_api_keys_set_ok, waldur_rest_client, command)
    elif action == UPDATE:
        # An update settles the key OK and so lifts a stale pause of its own; the same
        # reason as for a resume to apply the resource's pause first.
        _apply_resource_pause(
            backend, _read_resource(waldur_rest_client, command), command.resource_backend_id
        )
        backend.update_resource_key(
            command.client_id, command.resource_backend_id, **_key_settings(backend, command)
        )
        _acknowledge(marketplace_resource_api_keys_set_ok, waldur_rest_client, command)
    elif action == DELETE:
        backend.delete_resource_key(command.client_id, command.resource_backend_id)
        _acknowledge(marketplace_resource_api_keys_set_deleted, waldur_rest_client, command)


def _set_erred(
    waldur_rest_client: AuthenticatedClient, api_key_uuid: str, error_message: str
) -> None:
    marketplace_resource_api_keys_set_erred.sync(
        uuid=api_key_uuid,
        client=waldur_rest_client,
        body=ResourceApiKeySetErredRequest(error_message=error_message),
    )


def _rotate(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    command: ApiKeyCommand,
    expose_backend_error_details: bool,
) -> None:
    """Rotate one key; rotation reports its own outcome to Waldur."""
    if not command.client_id:
        logger.error("rotate command for key %s has no client_id", command.api_key_uuid)
        return
    resource = None
    if getattr(backend, "supports_resource_api_key_lifecycle", False) is True:
        # A rotation can mint or unblock a key too, so a backend whose keys may be
        # paused one by one learns Waldur's resource pause first, as for a create.
        try:
            resource = _read_resource(waldur_rest_client, command)
            _apply_resource_pause(backend, resource, command.resource_backend_id)
        except _NotYet as exc:
            logger.warning(
                "Could not read Waldur before the rotate of API key %s, leaving it pending: %s",
                command.api_key_uuid,
                exc,
            )
            return
        except Exception as exc:
            logger.error("Failed to rotate API key %s: %s", command.api_key_uuid, exc)
            error_message, _ = utils.format_waldur_error_details(exc, expose_backend_error_details)
            _set_erred(waldur_rest_client, command.api_key_uuid, error_message)
            return
        key_settings = _rotation_settings(waldur_rest_client, backend, command)
        resource_settings = {
            name: value
            for name, value in _resource_key_limits(backend, resource).items()
            if value is not None
        }
        if resource_settings:
            # The resource's limits and windows from the read above, as on a create: a
            # backend minting back a key it lost has no sibling to read them from when
            # that key was the resource's only one.
            key_settings = {**(key_settings or {}), **resource_settings}
    else:
        # A backend without the per-key lifecycle has no settings of its own per key,
        # and keeps the rotate signature it always had.
        key_settings = None
    rotated = utils.rotate_resource_api_key(
        waldur_rest_client,
        command.api_key_uuid,
        command.client_id,
        backend,
        command.resource_backend_id,
        command.resource_uuid or None,
        expose_backend_error_details=expose_backend_error_details,
        key_settings=key_settings,
    )
    if rotated and resource is not None:
        # Waldur rotates a key that changes hands, so the new assignee's chat moves
        # onto it now rather than at the next membership pass.
        _sync_key_assignees_now(waldur_rest_client, backend, resource)


def _rotation_settings(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    command: ApiKeyCommand,
) -> Optional[dict]:
    """The key's own limits and model list, applied to the rotated key.

    Waldur rotates a key that changes hands, possibly with new settings, and a
    backend that lost the key re-creates it on rotation, as restricted as the key
    Waldur shows. A sweep's command carries the settings from the listing row, and a
    STOMP rotate from a Waldur that rotates a key changing hands carries them too; an
    older Waldur's carries none, so the key is read. ``limits`` is always a dict here,
    so a backend can tell settings Waldur gave (``{}`` for no limits of its own) from
    none at all. None when the key cannot be read: the rotation still goes ahead,
    since a key the backend still holds keeps its own settings, and a backend minting
    a lost one has to fail closed instead. A backend that takes limits per period gets
    ``spending_limits`` too (``_key_settings``), so a rotation keeps the key's day and
    week limits.
    """
    if command.limits is None and command.allowed_models is None:
        try:
            row = marketplace_resource_api_keys_retrieve.sync(
                uuid=command.api_key_uuid,  # type: ignore[arg-type]
                client=waldur_rest_client,
            )
        except Exception as exc:
            logger.warning(
                "Could not read the settings of API key %s before rotating it: %s",
                command.api_key_uuid,
                exc,
            )
            return None
        command = ApiKeyCommand.from_listing(row, ROTATE)
    return {**_key_settings(backend, command), "limits": command.limits or {}}


def execute_api_key_command(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    command: ApiKeyCommand,
    expose_backend_error_details: bool = True,
) -> None:
    """Carry out one command on the backend and acknowledge it to Waldur.

    An action outside the vocabulary is refused without touching the backend or
    Waldur: it can only come from a newer Waldur, and guessing at it is worse than
    leaving the key pending. A known action the backend cannot
    perform, or one that fails, is reported with ``set_erred`` so the portal stops
    waiting on it.
    """
    if command.action not in ACTIONS:
        logger.error(
            "Unknown API key action %r for key %s; not acting on it",
            command.action,
            command.api_key_uuid,
        )
        return
    if not command.api_key_uuid or not command.resource_backend_id:
        logger.error(
            "API key %s command is missing the key uuid or the resource backend id",
            command.action,
        )
        return

    if command.action == ROTATE:
        _rotate(waldur_rest_client, backend, command, expose_backend_error_details)
        return

    if not getattr(backend, "supports_resource_api_key_lifecycle", False):
        logger.error(
            "Backend %s cannot %s a single API key (key %s)",
            type(backend).__name__,
            command.action,
            command.api_key_uuid,
        )
        _set_erred(
            waldur_rest_client,
            command.api_key_uuid,
            f"The backend does not support the {command.action} command for a single key.",
        )
        return

    logger.info(
        "Applying API key %s to key %s (%s)",
        command.action,
        command.api_key_uuid,
        command.client_id or "no client_id yet",
    )
    try:
        _run(waldur_rest_client, backend, command)
    except _NotYet as exc:
        logger.warning(
            "Could not read Waldur before the %s of API key %s, leaving it pending: %s",
            command.action,
            command.api_key_uuid,
            exc,
        )
    except _Withdrawn as exc:
        logger.info(
            "Waldur refused the key created for API key %s, which was withdrawn: %s",
            command.api_key_uuid,
            exc,
        )
    except Exception as exc:
        # The acknowledgement is inside the try, as for rotation: a key whose
        # acknowledgement failed must not stay in flight, or the sweep replays the
        # command on every tick.
        logger.error(
            "Failed to %s API key %s: %s", command.action, command.api_key_uuid, exc
        )
        error_message, _ = utils.format_waldur_error_details(exc, expose_backend_error_details)
        _set_erred(waldur_rest_client, command.api_key_uuid, error_message)


def find_pending_api_key_commands(
    waldur_rest_client: AuthenticatedClient,
    offering_uuid: str,
    cutoff: Optional[datetime.datetime],
    lifecycle: bool,
) -> list[ApiKeyCommand]:
    """Return the command each key in a transitional state awaits.

    With ``cutoff``, only keys that entered that state before it; without, every one.
    The command is the one the key's ``pending_action`` names. Deleted keys are
    settled and never listed, so a sweep cannot bring one back.

    Creating and Deleting keys come from per-key commands, so they are looked for
    only when the backend can carry those out (``lifecycle``).
    """
    states = [ResourceApiKeyState.UPDATING]
    if lifecycle:
        states += [ResourceApiKeyState.CREATING, ResourceApiKeyState.DELETING]
    rows = marketplace_resource_api_keys_list.sync_all(
        client=waldur_rest_client,
        offering_uuid=offering_uuid,  # type: ignore[arg-type]
        state=states,
        modified_before=cutoff if cutoff else UNSET,
    )
    commands: list[ApiKeyCommand] = []
    for row in rows:
        action = row.pending_action.value
        if not action:
            logger.warning(
                "API key %s is %s with no pending command; not replaying anything",
                row.uuid,
                row.state,
            )
            continue
        commands.append(ApiKeyCommand.from_listing(row, action))
    return commands


def process_pending_api_key_commands(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    offering: structures.Offering,
    cutoff: Optional[datetime.datetime] = None,
    expose_backend_error_details: bool = True,
) -> None:
    """Carry out the commands the offering's keys are waiting on.

    Key commands are not orders, so order processing never sees them. Two loops take
    them from Waldur instead of from STOMP: polling order processing takes every one,
    and the event_process reconciliation sweep, which must not race the STOMP handler,
    only those older than ``cutoff``. One failing command does not stop the others.
    A backend that does not manage resource API keys is left alone.
    """
    if not getattr(backend, "supports_resource_api_keys", False):
        return
    commands = find_pending_api_key_commands(
        waldur_rest_client,
        offering.waldur_offering_uuid,
        cutoff,
        lifecycle=getattr(backend, "supports_resource_api_key_lifecycle", False),
    )
    if not commands:
        return

    logger.info(
        "Found %d pending API key command(s) for %s%s",
        len(commands),
        offering.name,
        f" (modified before {cutoff.isoformat()})" if cutoff else "",
    )
    for command in commands:
        touch_heartbeat()
        # The STOMP handler rejects these, and a listing row can lack them as
        # well. An empty backend id makes envoy's pause check see no siblings
        # and re-provision a key active.
        if not command.resource_backend_id:
            logger.warning(
                "Skipping API key %s: the resource has no backend id",
                command.api_key_uuid,
            )
            continue
        try:
            execute_api_key_command(
                waldur_rest_client,
                backend,
                command,
                expose_backend_error_details=expose_backend_error_details,
            )
        except Exception:
            logger.exception(
                "Failed to carry out the %s of API key %s",
                command.action,
                command.api_key_uuid,
            )


def _plugin_option(waldur_resource: object, name: str) -> object:
    plugin_options = getattr(waldur_resource, "offering_plugin_options", None)
    value = getattr(plugin_options, name, None)
    if value is None:
        props = getattr(plugin_options, "additional_properties", None)
        if isinstance(props, dict):
            value = props.get(name)
    return value


def _current_period() -> datetime.date:
    """The month the backend's per-key report covers: the current UTC month, first day."""
    return datetime.datetime.now(tz=datetime.timezone.utc).date().replace(day=1)


def manages_api_keys(waldur_resource: object) -> bool:
    """Whether the resource's offering has per-key governance turned on in Waldur."""
    return bool(_plugin_option(waldur_resource, "enable_api_key_provisioning"))


def wants_key_assignees(backend, waldur_resource: object) -> bool:  # noqa: ANN001
    """Whether the backend is to be told who the resource's keys are assigned to."""
    return getattr(backend, "requires_resource_key_assignees", False) is True and (
        manages_api_keys(waldur_resource)
    )


def _uuid_hex(value: object) -> str:
    value = _value(value)
    if value is None:
        return ""
    return str(getattr(value, "hex", None) or str(value).replace("-", ""))


def key_assignments(
    keys: list[ResourceApiKeyStatus], offering_users: list
) -> dict[str, Optional[str]]:
    """Map the client_id of every key not deleted to its assignee's offering username.

    The listing carries the assignee's user uuid; the offering users carry the uuid
    and the username the backend knows the person by, so the join is a lookup. A key
    assigned to nobody, or to someone with no actionable offering user or no username
    yet, maps to None: the backend then acts for nobody, rather than for whoever held
    the key before. A key with no client_id yet has not reached the backend and is
    left out.
    """
    usernames: dict[str, str] = {}
    for offering_user in offering_users:
        if getattr(offering_user, "state", None) not in _ACTIONABLE_OFFERING_USER_STATES:
            continue
        user = _uuid_hex(getattr(offering_user, "user_uuid", None))
        username = _value(getattr(offering_user, "username", None))
        if user and isinstance(username, str) and username:
            usernames[user] = username
    assignments: dict[str, Optional[str]] = {}
    for key in keys:
        client_id = _value(key.client_id)
        if not client_id or key.state == ResourceApiKeyState.DELETED:
            continue
        assignments[client_id] = usernames.get(_uuid_hex(key.user_uuid))
    return assignments


def sync_key_assignees(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    waldur_resource: object,
    offering_users: list,
) -> None:
    """Tell the backend who the resource's keys are assigned to (membership pass).

    An assignee change sends the agent no command, so this pass is what carries a
    reassignment to the backend. One key listing per resource, and only for a backend
    that asks and an offering that manages its keys.
    """
    if not wants_key_assignees(backend, waldur_resource):
        return
    keys = list_all_api_keys(waldur_rest_client, _uuid_hex(getattr(waldur_resource, "uuid", "")))
    backend.sync_resource_key_assignees(waldur_resource, key_assignments(keys, offering_users))


def _sync_key_assignees_now(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    resource: object,
) -> None:
    """Hand the backend the assignees right after a key is created or rotated.

    Without it a new key's assignee -- or the new assignee of a key Waldur rotated
    because it changed hands -- waits for the next membership pass. The key is live
    and acknowledged by now, so a failure here is logged and left to that pass rather
    than reported against the key.
    """
    if not wants_key_assignees(backend, resource):
        return
    resource_uuid = _uuid_hex(getattr(resource, "uuid", ""))
    try:
        keys = list_all_api_keys(waldur_rest_client, resource_uuid)
        assignees = sorted(
            {
                _uuid_hex(key.user_uuid)
                for key in keys
                if key.state != ResourceApiKeyState.DELETED and _value(key.client_id)
            }
            - {""}
        )
        offering_users: list = []
        for user_uuid in assignees:
            offering_users.extend(
                marketplace_offering_users_list.sync_all(
                    client=waldur_rest_client,
                    offering_uuid=[getattr(resource, "offering_uuid")],  # noqa: B009
                    user_uuid=user_uuid,  # type: ignore[arg-type]
                    is_restricted=False,
                )
            )
        backend.sync_resource_key_assignees(resource, key_assignments(keys, offering_users))
    except Exception:
        logger.exception(
            "Could not hand the key assignees of resource %s to the backend; the next "
            "membership sync will",
            resource_uuid,
        )


def report_api_key_usages(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001
    resource_uuid: str,
    resource_backend_id: str,
    component_types: list[str],
) -> None:
    """Report the current month's usage of each of the resource's keys to Waldur.

    Every key is reported, deleted ones included (their usage this month still
    counts), and a key with no usage this month is reported as zero — otherwise a key
    keeps last month's figure, and a key paused at its limit stays over it for good. A
    resource whose usage the backend cannot attribute to its keys is not reported per
    key at all. Waldur pauses a key whose reported usage reaches its limit.

    Each report names its month (``billing_period``): left out, Waldur files the
    figures under its own current month, and a September total that reaches it
    after midnight on the first would become October's usage and pause keys near
    their limit. The backend reports the current UTC month, so a collection that
    straddles the turn of a month cannot say which month it covers and is dropped;
    the next round reports the new month.

    Where the backend reports per day and Waldur takes a key's usage per day, every
    day of the month so far goes out (``daily_usages``), so that day and week limits
    hold; Waldur derives the month from them. The month figures still decide whether
    a key is reported at all.
    """
    period = _current_period()
    daily: Optional[dict] = None
    if getattr(backend, "supports_daily_usage", False) is True:
        # The month figures are the sum of the days, so one collection gives both.
        daily = backend.get_resource_key_daily_usage_report([resource_backend_id]).get(
            resource_backend_id, {}
        )
        report = (
            None
            if daily is None
            else {
                client_id: spending_limits.month_totals(days) for client_id, days in daily.items()
            }
        )
    else:
        report = backend.get_resource_key_usage_report([resource_backend_id]).get(
            resource_backend_id, {}
        )
    if _current_period() != period:
        logger.info(
            "The month turned while collecting per-key usage of %s; reporting it next round",
            resource_backend_id,
        )
        return
    if report is None:
        logger.info(
            "Usage of %s cannot be attributed to its keys; skipping per-key usage",
            resource_backend_id,
        )
        return
    known = set()
    for key in list_all_api_keys(waldur_rest_client, resource_uuid):
        client_id = _value(key.client_id)
        if not client_id:
            continue
        known.add(client_id)
        key_usage = report.get(client_id, {})
        # Rounded as the resource total is: Waldur keeps two decimals.
        usages = {
            component: round(float(key_usage.get(component, 0)), 2)
            for component in component_types
        }
        # Waldur keeps one month per key: figures it holds for an earlier month say
        # nothing about this one, so the first report of a month always goes out, even
        # when it repeats them (typically zero). Reported per day, Waldur never lowers a
        # key's month figure within the month, so a key whose usage moved elsewhere is
        # sent again every round until the month turns; the days then stay right.
        current = key.current_usages.additional_properties if key.current_usages else {}
        if _value(key.usage_period) == period and all(
            float(current.get(component, -1)) == value for component, value in usages.items()
        ):
            continue
        body_usages = ResourceApiKeyUsageRequestUsages()
        body = ResourceApiKeyUsageRequest(usages=body_usages, billing_period=period)
        if daily is not None and spending_limits.key_accepts_daily_usage(key):
            # No month totals beside the days, which Waldur would refuse; a client that
            # predates daily_usages sends usages all the same, so it goes out empty.
            days = spending_limits.fill_days(
                daily.get(client_id, {}),
                period.year,
                period.month,
                set(component_types),
                spending_limits.utc_today(),
            )
            body.additional_properties["daily_usages"] = spending_limits.daily_usage_items(days)
        else:
            body_usages.additional_properties = usages
        try:
            marketplace_resource_api_keys_report_usage.sync(
                uuid=key.uuid,
                client=waldur_rest_client,
                body=body,
            )
        except Exception as exc:
            logger.warning("Failed to report the usage of API key %s: %s", client_id, exc)
    unknown = sorted(
        client_id
        for client_id in set(report) - known
        if not client_id.endswith(UNASSIGNED_USAGE_SUFFIX)
    )
    if unknown:
        logger.warning(
            "Usage of %s came through keys Waldur does not hold: %s",
            resource_backend_id,
            ", ".join(unknown),
        )
