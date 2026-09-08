"""LDAP plugin Pydantic schemas for configuration validation."""

from __future__ import annotations

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
        default=False,
        description="Delete user from LDAP on deactivation (default: keep user)",
    )
    generate_vpn_password: Optional[bool] = Field(
        default=False,
        description="Generate a random password for VPN access on user creation",
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
        """
        if self.account_source != AccountSource.WALDUR:
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
