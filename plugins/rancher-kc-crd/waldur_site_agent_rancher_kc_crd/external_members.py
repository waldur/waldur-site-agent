"""Import people added by hand to the operator's Keycloak groups into Waldur.

With ``externalMemberPattern`` set, the operator keeps a group member the
CR does not name, provided the CR never had them, and reports them in
``status.*RoleBindings[].externalMembers``. This module turns each report
into a Waldur grant: the Identity Bridge finds the person's Waldur account
(by username, or by civil number when they signed up under another
username) or creates it, and the role the group stands for is granted on
the resource project (project groups) or the resource (cluster groups).
On the next cycle the person is an ordinary member of the CR.

Waldur stays authoritative for removals: someone it removes was synced
before, so the operator removes them and never reports them here.
"""

from __future__ import annotations

import logging
import re
import uuid as uuid_lib
from dataclasses import dataclass
from types import SimpleNamespace

from waldur_api_client.api.identity_bridge import (
    identity_bridge,
    identity_bridge_allowed_fields_retrieve,
)
from waldur_api_client.api.marketplace_offering_roles import marketplace_offering_roles_list
from waldur_api_client.api.marketplace_provider_resource_projects import (
    marketplace_provider_resource_projects_add_user,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_add_user,
)
from waldur_api_client.api.users import users_retrieve
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.identity_bridge_request_request import (
    IdentityBridgeRequestRequest,
)
from waldur_api_client.models.user_field_enum import UserFieldEnum
from waldur_api_client.models.user_role_create_request import UserRoleCreateRequest

from .translator import DEFAULT_IDENTITY_TEMPLATE, IdentitySettings, mask_civil_code

logger = logging.getLogger(__name__)

PROJECT_SCOPE = "resource_project"
CLUSTER_SCOPE = "resource"
# validate_role_grant's wording: "User has already the same role in this scope."
_ALREADY_GRANTED = "already the same role in this scope"
_HTTP_OK = 200
_HTTP_CREATED = 201
_HTTP_BAD_REQUEST = 400
_ROLE_PAGE_SIZE = 100
# Accounts created in civil_number mode are named prefix + 20 random hex
# characters; Waldur's USERNAME_REGEX allows letters, digits, "_", "." and
# "-", not starting with "-".
DEFAULT_USERNAME_PREFIX = "rancher-"
USERNAME_PREFIX_RE = re.compile(r"^([a-zA-Z0-9_.][a-zA-Z0-9_.-]{0,63})?$")


def validate_username_prefix(prefix: str) -> None:
    """Raise ``ValueError`` unless ``prefix`` can start a Waldur username."""
    if not USERNAME_PREFIX_RE.fullmatch(prefix):
        msg = (
            "external_member_username_prefix may hold letters, digits, '_', '.' and '-' "
            "(not first), at most 64 characters"
        )
        raise ValueError(msg)


def validate_external_import(identity: IdentitySettings, bridge_source: str | None) -> None:
    """Raise ``ValueError`` unless external members can be imported with these settings.

    The operator reports a Keycloak username, which maps back to a Waldur
    account only when it is the member's username or civil number as is.
    """
    if identity.source not in {"civil_number", "username"} or identity.lookup != "username":
        msg = (
            "import_external_members needs keycloak_user_lookup: username with "
            "keycloak_user_identity_source civil_number or username"
        )
        raise ValueError(msg)
    if identity.template != DEFAULT_IDENTITY_TEMPLATE:
        msg = "import_external_members cannot reverse keycloak_user_identity_template"
        raise ValueError(msg)
    if not bridge_source:
        msg = "import_external_members needs identity_bridge_source"
        raise ValueError(msg)
    if identity.source == "civil_number" and not identity.identity_provider:
        msg = (
            "import_external_members in civil_number mode needs keycloak_identity_provider: "
            "only members linked to that provider are imported"
        )
        raise ValueError(msg)


def _call(endpoint, **kwargs):  # noqa: ANN001, ANN003, ANN202
    """Call a generated client endpoint, answering every status with a response.

    The client raises ``UnexpectedStatus`` for a status its schema does not
    list, with the whole response body in the message; that message would
    reach the logs, and Waldur's answers can echo what was sent. The
    exception is turned into an ordinary response here, so the callers'
    status handling and masking apply to it too.
    """
    try:
        return endpoint(**kwargs)
    except UnexpectedStatus as exc:
        return SimpleNamespace(status_code=exc.status_code, content=exc.content, parsed=None)


@dataclass(frozen=True)
class ExternalGrant:
    """One Waldur role an external member is to receive."""

    scope_type: str  # PROJECT_SCOPE or CLUSTER_SCOPE
    scope_uuid: str
    role_name: str
    username: str
    first_name: str
    last_name: str
    identifier: str  # Waldur username, or civil number in civil_number mode


def collect_external_grants(
    cr_status: dict,
    group_roles: dict[str, str],
    cluster_group_roles: dict[str, str],
    resource_project_uuid: str,
    resource_uuid: str,
    identity: IdentitySettings,
) -> list[ExternalGrant]:
    """The grants a CR's reported external members call for.

    A binding whose group the agent did not build (renamed template, role
    dropped from the map) is skipped: there is no Waldur role to map it to.
    """
    grants: list[ExternalGrant] = []
    for key, roles, scope_type, scope_uuid in (
        ("keycloakRoleBindings", group_roles, PROJECT_SCOPE, resource_project_uuid),
        ("clusterKeycloakRoleBindings", cluster_group_roles, CLUSTER_SCOPE, resource_uuid),
    ):
        for rb in cr_status.get(key) or []:
            role_name = roles.get(rb.get("groupName") or "")
            if not role_name:
                continue
            for member in rb.get("externalMembers") or []:
                username = member.get("username") or ""
                identifier = waldur_identifier(member, identity)
                if not username or not identifier:
                    civil = identity.source == "civil_number"
                    shown = mask_civil_code(username) if civil else username
                    logger.warning(
                        "rancher-kc-crd: external member %s of %s has no link to %s matching its "
                        "username; not imported (the operator removes them after its grace period)",
                        shown,
                        rb.get("groupName"),
                        identity.identity_provider,
                    )
                    continue
                grants.append(
                    ExternalGrant(
                        scope_type=scope_type,
                        scope_uuid=scope_uuid,
                        role_name=role_name,
                        username=username,
                        first_name=member.get("firstName") or "",
                        last_name=member.get("lastName") or "",
                        identifier=identifier,
                    )
                )
    return grants


def waldur_identifier(member: dict, identity: IdentitySettings) -> str | None:
    """The Waldur value an external member is matched on.

    In civil_number mode the civil number, and only from the member's link
    to the shared identity provider (``keycloak_identity_provider``), which
    keeps the case the provider issued. A member without that link is not
    imported: a local Keycloak account named like a civil code proves
    nothing about whose code it is, and importing it would grant the role
    to that person's Waldur account. In username mode the username.
    """
    username = member.get("username") or ""
    if identity.source == "username":
        return username or None
    if identity.source != "civil_number" or not identity.identity_provider or not username:
        return None
    for link in member.get("federatedIdentities") or []:
        user_id = link.get("userId") or ""
        # The link must name the same person as the username: the CR and the
        # operator address the member by username, so a local account linked
        # to someone else's identity would get that person the Waldur role
        # (and, through the username, possibly their Keycloak account).
        if (
            link.get("identityProvider") == identity.identity_provider
            and user_id.casefold() == username.casefold()
        ):
            return user_id
    return None


class ExternalMemberImporter:
    """Grants Waldur roles to external members, once per cycle each."""

    def __init__(
        self,
        client: AuthenticatedClient,
        offering_uuid: str,
        identity: IdentitySettings,
        bridge_source: str,
        username_prefix: str = DEFAULT_USERNAME_PREFIX,
    ) -> None:
        """Store the Waldur client and the settings the import reads."""
        self.client = client
        self.offering_uuid = offering_uuid
        self.identity = identity
        self.bridge_source = bridge_source
        self.username_prefix = username_prefix
        self._role_uuids: dict[tuple[str, str], str] | None = None
        # One bridge call per person per cycle, however many grants they get.
        self._user_uuids: dict[str, uuid_lib.UUID] = {}
        self._ordinary: dict[uuid_lib.UUID, bool] = {}

    def import_grants(self, grants: list[ExternalGrant]) -> int:
        """Apply the grants; return how many roles were granted.

        A cluster member is reported by every CR on the cluster, so the
        same grant arrives several times and is applied once.
        """
        unique = list(dict.fromkeys(grants))
        if not unique:
            return 0
        if not self._bridge_stores_civil_number():
            return 0
        granted = 0
        for grant in unique:
            try:
                if self._import(grant):
                    granted += 1
            except Exception as exc:
                logger.warning(
                    "rancher-kc-crd: cannot import external member %s: %s",
                    self._display(grant.username),
                    exc,
                )
        return granted

    def _bridge_stores_civil_number(self) -> bool:
        """In civil_number mode, refuse to create accounts the TARA login cannot find."""
        if self.identity.source != "civil_number":
            return True
        allowed = identity_bridge_allowed_fields_retrieve.sync(client=self.client)
        if allowed is not None and "civil_number" in allowed.allowed_fields:
            return True
        logger.error(
            "rancher-kc-crd: external members are not imported: the Identity Bridge does "
            "not store civil_number (add it to FEDERATED_IDENTITY_SYNC_ALLOWED_ATTRIBUTES), "
            "so an account it created would not be the one the person signs in to"
        )
        return False

    def _import(self, grant: ExternalGrant) -> bool:
        role_uuid = self._role_uuid(grant.scope_type, grant.role_name)
        if role_uuid is None:
            logger.warning(
                "rancher-kc-crd: offering has no %s role %r; external member %s not imported",
                grant.scope_type,
                grant.role_name,
                self._display(grant.username),
            )
            return False
        user_uuid = self._resolve_user(grant)
        if not self._is_ordinary(user_uuid):
            logger.warning(
                "rancher-kc-crd: external member %s resolves to a staff, support or identity "
                "manager account, or one the agent cannot read; not imported",
                self._display(grant.username),
            )
            return False
        body = UserRoleCreateRequest(role=role_uuid, user=user_uuid)
        if grant.scope_type == PROJECT_SCOPE:
            response = _call(
                marketplace_provider_resource_projects_add_user.sync_detailed,
                uuid=uuid_lib.UUID(grant.scope_uuid),
                client=self.client,
                body=body,
            )
        else:
            response = _call(
                marketplace_provider_resources_add_user.sync_detailed,
                uuid=uuid_lib.UUID(grant.scope_uuid),
                client=self.client,
                body=body,
            )
        if response.status_code == _HTTP_CREATED:
            logger.info(
                "rancher-kc-crd: imported external member %s as %s on %s %s",
                self._display(grant.username),
                grant.role_name,
                grant.scope_type,
                grant.scope_uuid,
            )
            return True
        text = response.content.decode(errors="replace")
        if response.status_code == _HTTP_BAD_REQUEST and _ALREADY_GRANTED in text:
            # Granted on an earlier cycle; the provider listing hides it (a
            # user who has not accepted the offering's terms) or the CR has
            # not caught up yet.
            return False
        msg = f"granting {grant.role_name} answered {response.status_code}{self._detail(response)}"
        raise RuntimeError(msg)

    def _resolve_user(self, grant: ExternalGrant) -> uuid_lib.UUID:
        """The Waldur account for the person, through the Identity Bridge."""
        cached = self._user_uuids.get(grant.identifier)
        if cached is not None:
            return cached
        if self.identity.source == "civil_number":
            # The Keycloak username is the civil code; it must not become the
            # Waldur username. A new account gets a neutral one, and the
            # bridge finds an existing account by the civil number instead,
            # so a repeated push lands on the same account.
            body = IdentityBridgeRequestRequest(
                username=f"{self.username_prefix}{uuid_lib.uuid4().hex[:20]}",
                source=self.bridge_source,
                civil_number=grant.identifier,
            )
        else:
            body = IdentityBridgeRequestRequest(username=grant.username, source=self.bridge_source)
        # Empty names are left out: they would clear the names of an
        # existing account the bridge updates.
        if grant.first_name:
            body.first_name = grant.first_name
        if grant.last_name:
            body.last_name = grant.last_name
        response = _call(identity_bridge.sync_detailed, client=self.client, body=body)
        if response.parsed is None:
            msg = f"Identity Bridge answered {response.status_code}{self._detail(response)}"
            raise RuntimeError(msg)
        result = response.parsed
        if result.created:
            logger.info(
                "rancher-kc-crd: created Waldur account for external member %s",
                self._display(grant.username),
            )
        self._user_uuids[grant.identifier] = result.uuid
        return result.uuid

    def _is_ordinary(self, user_uuid: uuid_lib.UUID) -> bool:
        """Whether the account may receive a role through the import.

        Staff, support and identity manager accounts never do: a Keycloak
        admin must not be able to hand them roles by naming them, and a staff
        token gets them back from the bridge. An account the agent cannot
        read is refused too.
        """
        if user_uuid in self._ordinary:
            return self._ordinary[user_uuid]
        response = _call(
            users_retrieve.sync_detailed,
            uuid=user_uuid,
            client=self.client,
            field=[
                UserFieldEnum.IS_STAFF,
                UserFieldEnum.IS_SUPPORT,
                UserFieldEnum.IS_IDENTITY_MANAGER,
            ],
        )
        user = response.parsed
        ordinary = (
            response.status_code == _HTTP_OK
            and user is not None
            and not any(
                getattr(user, flag, False) is True
                for flag in ("is_staff", "is_support", "is_identity_manager")
            )
        )
        self._ordinary[user_uuid] = ordinary
        return ordinary

    def _detail(self, response) -> str:  # noqa: ANN001
        """Waldur's answer for a log line; left out in civil_number mode.

        Waldur's messages can echo what was sent, and there that is a civil
        number.
        """
        if self.identity.source == "civil_number":
            return ""
        return ": " + response.content.decode(errors="replace")[:200]

    def _role_uuid(self, scope_type: str, role_name: str) -> str | None:
        if self._role_uuids is None:
            roles: list = []
            page = 1
            while True:
                batch = (
                    marketplace_offering_roles_list.sync(
                        client=self.client,
                        offering_uuid=self.offering_uuid,
                        page=page,
                        page_size=_ROLE_PAGE_SIZE,
                    )
                    or []
                )
                roles.extend(batch)
                if len(batch) < _ROLE_PAGE_SIZE:
                    break
                page += 1
            # Role names are unique per offering and content type only.
            self._role_uuids = {
                (str(role.content_type), role.name): role.uuid.hex
                for role in roles
                if getattr(role, "is_active", True)
            }
        return self._role_uuids.get((scope_type, role_name))

    def _display(self, username: str) -> str:
        return mask_civil_code(username) if self.identity.source == "civil_number" else username
