"""LDAP plugin Pydantic schemas for configuration validation."""

from __future__ import annotations

import uuid
from enum import Enum
from typing import Optional

from pydantic import ConfigDict, Field, field_validator, model_validator

from waldur_site_agent.common.plugin_schemas import PluginBackendSettingsSchema


# All of these derive from `str` on purpose. The backend compares them against
# plain strings, and pydantic hands back enum *instances* whenever a validated
# model is dumped back into the settings dict — with a bare Enum those
# comparisons silently become False and every account takes the wrong branch.
class UsernameFormat(str, Enum):
    """Username generation strategies."""

    FIRST_INITIAL_LASTNAME = "first_initial_lastname"
    FIRST_LETTER_FULL_LASTNAME = "first_letter_full_lastname"
    FIRSTNAME_DOT_LASTNAME = "firstname_dot_lastname"
    FIRSTNAME_LASTNAME = "firstname_lastname"
    WALDUR_USERNAME = "waldur_username"


class AccountSource(str, Enum):
    """Which side owns username, uidNumber, gidNumber, homeDirectory and loginShell."""

    # The directory owns them: usernames are derived from the user's name and
    # ids are allocated by scanning the directory. Historical default.
    LDAP = "ldap"
    # Waldur owns them: the agent writes the offering user's values into the
    # directory and never allocates. Required when several offerings of one
    # service provider share a directory.
    WALDUR = "waldur"


class MissingPosixIdsPolicy(str, Enum):
    """What to do when Waldur holds no UID/GID for an account."""

    ERROR = "error"  # log an actionable error and skip the account
    SKIP = "skip"  # skip quietly, for staged rollouts


class PosixMismatchPolicy(str, Enum):
    """What to do when a directory entry's ids disagree with Waldur's."""

    # Log a before/after diff and change nothing. Renumbering a live account
    # orphans every file it owns, so this is a human decision by default.
    REPORT = "report"
    # Rewrite the entry (and its personal group) to Waldur's values. For a
    # one-shot migration, after which the filesystem needs chown -R.
    ADOPT = "adopt"
    FAIL = "fail"  # raise and abort the cycle


class DeparturePolicy(str, Enum):
    """What happens to the directory entry when the person's last access ends."""

    # Keep the entry and its ids, but make it unusable: no-login shell,
    # shadowExpire in the past, group memberships dropped, marker set. The
    # identity stays reserved while files owned by it exist, and a returning
    # user gets the same DN and uid back.
    DISABLE = "disable"
    # Remove the entry and its personal group.
    DELETE = "delete"


class AccessGroupConfig(PluginBackendSettingsSchema):
    """Configuration for an LDAP access group that users can be added to."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="LDAP group name (e.g., 'vpnusrgroup', 'plus')")
    attribute: str = Field(
        default="memberUid",
        description="Membership attribute: 'memberUid' (UID-based) or 'member' (DN-based)",
    )

    @field_validator("attribute")
    @classmethod
    def validate_attribute(cls, v: str) -> str:
        """Validate that attribute is memberUid or member."""
        allowed = {"memberUid", "member"}
        if v not in allowed:
            msg = f"attribute must be one of {allowed}"
            raise ValueError(msg)
        return v


class ProjectGroupMemberAttribute(str, Enum):
    """How a project group lists its members."""

    MEMBER_UID = "memberUid"  # bare usernames (RFC 2307 posixGroup)
    MEMBER = "member"  # user DNs (rfc2307bis, groupOfNames)


class ProjectGroupMembership(str, Enum):
    """How far the agent goes in making a project group's members match Waldur."""

    SYNC = "sync"  # add missing members and remove the ones Waldur does not list
    ADD_ONLY = "add_only"  # only add; a member Waldur does not list stays


class GidMismatchPolicy(str, Enum):
    """What to do with an existing group of the same name but another GID."""

    # Log it and leave the entry alone: renumbering a group orphans every file
    # its old GID owns.
    REPORT = "report"
    # Rewrite the entry's gidNumber to Waldur's, unless another entry holds it.
    ADOPT = "adopt"


class ParentGroupConfig(PluginBackendSettingsSchema):
    """An entry that lists the DNs of project groups (a cluster's groupOfNames)."""

    model_config = ConfigDict(extra="forbid")

    dn: str = Field(..., description="Full DN of the entry, e.g. 'cn=alps,ou=clusters,dc=...'")
    attribute: str = Field(
        default="member",
        description="Attribute that holds the project group DNs ('member', 'uniqueMember')",
    )
    offering_uuids: Optional[list[str]] = Field(
        default=None,
        description="Offerings whose projects this entry lists. Unset, only this "
        "agent's offering. Set it when several offerings share one entry, or each "
        "agent would remove the groups the others add.",
    )

    @field_validator("offering_uuids")
    @classmethod
    def validate_offering_uuids(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        """Each must be a UUID, with or without dashes."""
        for value in v or []:
            try:
                uuid.UUID(str(value))
            except ValueError as e:
                msg = f"offering_uuids: {value!r} is not a UUID"
                raise ValueError(msg) from e
        return v


class ProjectGroupsConfig(PluginBackendSettingsSchema):
    """Provider project groups written from Waldur, one entry per project."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=False, description="Write Waldur's project groups")
    ou: str = Field(
        default="ou=projects",
        description="OU, relative to base_dn, the project groups are written under",
    )
    object_classes: list[str] = Field(
        default_factory=lambda: ["top", "posixGroup"],
        description="Object classes of a newly created project group",
    )
    member_attribute: ProjectGroupMemberAttribute = Field(
        default=ProjectGroupMemberAttribute.MEMBER_UID,
        description="'memberUid' writes usernames, 'member' writes user DNs "
        "(uid=<name>,<people_ou>,<base_dn>)",
    )
    membership: ProjectGroupMembership = Field(
        default=ProjectGroupMembership.SYNC,
        description="'sync' adds and removes members to match Waldur; 'add_only' "
        "never removes one",
    )
    on_gid_mismatch: GidMismatchPolicy = Field(
        default=GidMismatchPolicy.REPORT,
        description="An existing group of the same name with another gidNumber: "
        "'report' leaves it alone, 'adopt' renumbers it to Waldur's GID unless "
        "another entry holds that GID",
    )
    managed_marker: str = Field(
        default="waldur-managed",
        min_length=1,
        description="Extra description value written on every project group the agent "
        "creates or adopts. Only marked groups are taken out of a parent once Waldur "
        "no longer lists them; unmarked groups under the OU are the operator's",
    )
    parents: list[ParentGroupConfig] = Field(
        default_factory=list,
        description="Entries that list the DN of each project group whose project "
        "has a resource on the offering",
    )
    organization_description: Optional[str] = Field(
        default=None,
        description="Template of a description value naming the project's "
        "organization, e.g. 'organization={slug}'. Unset, nothing is written. Kept in "
        "sync on every pass; with literal text around {slug} a changed slug replaces "
        "the old value, a bare '{slug}' can only be added",
    )

    @field_validator("organization_description")
    @classmethod
    def validate_organization_description(cls, v: Optional[str]) -> Optional[str]:
        """Exactly one ``{slug}`` placeholder, so the value can be found again."""
        if v is not None and v.count("{slug}") != 1:
            msg = "organization_description must contain {slug} exactly once"
            raise ValueError(msg)
        return v


    @model_validator(mode="after")
    def validate_member_attribute(self) -> ProjectGroupsConfig:
        """``member`` holds DNs, which an RFC 2307 posixGroup does not allow.

        groupOfNames or groupOfMembers (the rfc2307bis structural classes) has
        to be among the object classes for the directory to accept the members.
        """
        if self.member_attribute != ProjectGroupMemberAttribute.MEMBER:
            return self
        classes = {c.lower() for c in self.object_classes}
        if not classes & {"groupofnames", "groupofmembers"}:
            msg = (
                "member_attribute 'member' needs groupOfNames or groupOfMembers in "
                "object_classes; an RFC 2307 posixGroup allows memberUid only"
            )
            raise ValueError(msg)
        return self


class LdapSettingsSchema(PluginBackendSettingsSchema):
    """LDAP connection and provisioning settings.

    Nested under backend_settings.ldap in the offering configuration.
    """

    model_config = ConfigDict(extra="allow")

    # Connection
    uri: str = Field(..., description="LDAP server URI (e.g., 'ldap://ldap.example.com')")
    bind_dn: str = Field(..., description="DN to bind as (e.g., 'cn=admin,dc=example,dc=com')")
    bind_password: str = Field(..., description="Password for bind DN")
    base_dn: str = Field(..., description="Base DN for the directory (e.g., 'dc=example,dc=com')")
    use_starttls: Optional[bool] = Field(default=False, description="Use STARTTLS for connection")

    # Directory structure
    people_ou: str = Field(default="ou=People", description="OU for user entries")
    groups_ou: str = Field(default="ou=Groups", description="OU for group entries")

    # ID allocation ranges
    uid_range_start: int = Field(default=10000, description="Start of UID allocation range")
    uid_range_end: int = Field(default=65000, description="End of UID allocation range")
    gid_range_start: int = Field(default=10000, description="Start of GID allocation range")
    gid_range_end: int = Field(default=65000, description="End of GID allocation range")

    # User defaults
    default_login_shell: str = Field(
        default="/bin/bash", description="Default login shell for users"
    )
    default_home_base: str = Field(default="/home", description="Base path for home directories")

    # Identity authority
    account_source: AccountSource = Field(
        default=AccountSource.LDAP,
        description="Which side owns username/uidNumber/gidNumber/homeDirectory/"
        "loginShell. 'ldap' (default) keeps the historical behaviour: the agent "
        "derives usernames and allocates ids from the ranges below. 'waldur' takes "
        "all five from the offering user and writes them into the directory.",
    )
    on_missing_posix_ids: MissingPosixIdsPolicy = Field(
        default=MissingPosixIdsPolicy.ERROR,
        description="What to do when Waldur holds no UID/GID for an account. Only "
        "consulted when account_source is 'waldur'.",
    )
    on_posix_mismatch: PosixMismatchPolicy = Field(
        default=PosixMismatchPolicy.REPORT,
        description="What to do when an existing entry's uidNumber/gidNumber "
        "disagree with Waldur's. Only consulted when account_source is 'waldur'.",
    )

    # Username generation
    username_format: Optional[UsernameFormat] = Field(
        default=UsernameFormat.FIRST_INITIAL_LASTNAME,
        description="Strategy for generating usernames from user profiles. "
        "Not permitted when account_source is 'waldur' - Waldur names the accounts.",
    )
    waldur_username_attribute: Optional[str] = Field(
        default=None,
        description="LDAP attribute to store the Waldur username (e.g. a CUID) in, "
        "alongside the POSIX login name. Left unset, it is not written.",
    )

    # User lifecycle
    remove_user_on_deactivate: Optional[bool] = Field(
        default=None,
        description="Delete the directory entry once the person holds no live account "
        "on any of the provider's offerings that share it. Unset, this follows "
        "account_source: off under 'ldap' (the entry is kept, as it always was), on "
        "under 'waldur' (Waldur owns the identity and keeps the ids, so the entry "
        "is reproducible and a departed user must not keep a resolvable login).",
    )
    on_departure: Optional[DeparturePolicy] = Field(
        default=None,
        description="What to do with the entry once it is released: 'disable' parks it "
        "(no-login shell, shadowExpire=1, groups dropped, marker set; same DN and uid "
        "come back on return) or 'delete' removes it. Unset, this follows "
        "account_source: 'disable' under 'waldur', 'delete' under 'ldap' (the "
        "historical meaning of remove_user_on_deactivate there).",
    )
    generate_vpn_password: Optional[bool] = Field(
        default=False,
        description="Generate a random password for VPN access on user creation",
    )

    # Groups
    personal_groups: bool = Field(
        default=True,
        description="Create a personal group (cn=<username> in groups_ou) with each "
        "account. Off, accounts carry the primary GID from Waldur and no group entry "
        "is written, so groups_ou is only needed for access_groups. Requires "
        "account_source 'waldur': without it there is no primary GID to write.",
    )
    project_groups: Optional[ProjectGroupsConfig] = Field(
        default=None,
        description="Write the service provider's project groups from Waldur",
    )

    # Access groups
    access_groups: Optional[list[AccessGroupConfig]] = Field(
        default=None,
        description="LDAP groups to add new users to (e.g., VPN access, GPU access)",
    )

    # Welcome email
    welcome_email: Optional[WelcomeEmailSchema] = Field(
        default=None,
        description="SMTP settings for sending a welcome email on account creation. "
        "Disabled when not configured.",
    )

    # Object classes
    user_object_classes: Optional[list[str]] = Field(
        default=None,
        description="Object classes for user entries",
    )
    user_group_object_classes: Optional[list[str]] = Field(
        default=None,
        description="Object classes for personal user groups",
    )
    project_group_object_classes: Optional[list[str]] = Field(
        default=None,
        description="Object classes for project groups",
    )

    @field_validator("uid_range_start", "uid_range_end", "gid_range_start", "gid_range_end")
    @classmethod
    def validate_id_range(cls, v: int) -> int:
        """Validate that ID range values are non-negative."""
        if v < 0:
            msg = "ID range values must be non-negative"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def validate_account_source(self) -> LdapSettingsSchema:
        """Reject settings that contradict a Waldur-authoritative configuration.

        Only ``username_format`` is rejected outright: leaving it accepted would
        let an operator believe the agent still names accounts, which it does not.
        The uid/gid ranges stay legal — the same ``ldap:`` block is shared verbatim
        with the SLURM backend's client, which keeps allocating *project* group
        GIDs from ``gid_range_*``. The backend warns about both at construction.

        The converse: ``personal_groups: false`` is rejected *without* Waldur
        authority, since only Waldur can supply a primary GID that no group holds.
        """
        if self.account_source != AccountSource.WALDUR:
            if not self.personal_groups:
                msg = (
                    "personal_groups: false requires account_source 'waldur': the "
                    "agent allocates primary GIDs from groups_ou otherwise, and an "
                    "account without its group would leave its GID free for reuse."
                )
                raise ValueError(msg)
            return self
        if "username_format" in self.model_fields_set:
            msg = (
                "username_format is not permitted when account_source is 'waldur': "
                "usernames come from the offering user, not from this agent. "
                "Remove the setting, or set account_source to 'ldap'."
            )
            raise ValueError(msg)
        return self


class WelcomeEmailSchema(PluginBackendSettingsSchema):
    """SMTP and template settings for welcome emails sent on account creation."""

    model_config = ConfigDict(extra="forbid")

    # SMTP connection
    smtp_host: str = Field(..., description="SMTP server hostname")
    smtp_port: int = Field(default=587, description="SMTP server port")
    smtp_username: Optional[str] = Field(default=None, description="SMTP auth username")
    smtp_password: Optional[str] = Field(default=None, description="SMTP auth password")
    use_tls: bool = Field(default=True, description="Use STARTTLS (port 587)")
    use_ssl: bool = Field(default=False, description="Use implicit SSL (port 465)")
    timeout: int = Field(default=30, description="SMTP connection timeout in seconds")

    # Sender
    from_address: str = Field(..., description="Sender email address")
    from_name: Optional[str] = Field(default=None, description="Sender display name")

    # Email content
    subject: str = Field(
        default="Your new account has been created",
        description="Email subject line (supports Jinja2 template variables)",
    )
    template_path: str = Field(
        ...,
        description="Path to Jinja2 email body template file (absolute or relative to CWD)",
    )


class LdapBackendSettingsSchema(PluginBackendSettingsSchema):
    """Top-level backend settings schema for LDAP username management.

    The LDAP settings are nested under the 'ldap' key.
    """

    model_config = ConfigDict(extra="allow")

    ldap: LdapSettingsSchema = Field(..., description="LDAP connection and provisioning settings")
