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

from waldur_site_agent.backend import logger
from waldur_site_agent.common import structures, utils
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
            allowed_models=row.allowed_models,
        )


def _value(field: Any) -> Any:  # noqa: ANN401 - any optional field of a generated model
    return None if isinstance(field, Unset) else field


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


def _create(
    waldur_rest_client: AuthenticatedClient,
    backend,  # noqa: ANN001 - BaseBackend would be a circular import
    command: ApiKeyCommand,
) -> None:
    try:
        reserved = _reserved_client_ids(waldur_rest_client, command.resource_uuid)
        resource = marketplace_provider_resources_retrieve.sync(
            uuid=command.resource_uuid,  # type: ignore[arg-type]
            client=waldur_rest_client,
            field=[ResourceFieldEnum.PAUSED, ResourceFieldEnum.DOWNSCALED],
        )
    except (httpx.TransportError, errors.UnexpectedStatus) as exc:
        if isinstance(exc, errors.UnexpectedStatus) and exc.status_code < HTTP_SERVER_ERROR:
            raise
        # Waldur did not answer and nothing is applied yet: try again later.
        raise _NotYet(str(exc)) from exc
    _apply_resource_pause(backend, resource, command.resource_backend_id)
    key = backend.mint_resource_key(
        command.resource_backend_id,
        reserved,
        limits=command.limits,
        allowed_models=command.allowed_models,
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
        backend.resume_resource_key(
            command.client_id,
            command.resource_backend_id,
            limits=command.limits,
            allowed_models=command.allowed_models,
        )
        _acknowledge(marketplace_resource_api_keys_set_ok, waldur_rest_client, command)
    elif action == UPDATE:
        backend.update_resource_key(
            command.client_id,
            command.resource_backend_id,
            limits=command.limits,
            allowed_models=command.allowed_models,
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
        if not command.client_id:
            logger.error("rotate command for key %s has no client_id", command.api_key_uuid)
            return
        utils.rotate_resource_api_key(
            waldur_rest_client,
            command.api_key_uuid,
            command.client_id,
            backend,
            command.resource_backend_id,
            command.resource_uuid or None,
            expose_backend_error_details=expose_backend_error_details,
        )
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
            "Could not read Waldur before creating API key %s, leaving it pending: %s",
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
    """
    period = _current_period()
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
        # when it repeats them (typically zero).
        current = key.current_usages.additional_properties if key.current_usages else {}
        if _value(key.usage_period) == period and all(
            float(current.get(component, -1)) == value for component, value in usages.items()
        ):
            continue
        body_usages = ResourceApiKeyUsageRequestUsages()
        body_usages.additional_properties = usages
        try:
            marketplace_resource_api_keys_report_usage.sync(
                uuid=key.uuid,
                client=waldur_rest_client,
                body=ResourceApiKeyUsageRequest(usages=body_usages, billing_period=period),
            )
        except Exception as exc:
            logger.warning("Failed to report the usage of API key %s: %s", client_id, exc)
    unknown = sorted(set(report) - known)
    if unknown:
        logger.warning(
            "Usage of %s came through keys Waldur does not hold: %s",
            resource_backend_id,
            ", ".join(unknown),
        )
