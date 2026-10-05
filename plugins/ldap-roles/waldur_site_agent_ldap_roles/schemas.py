"""Pydantic schemas for the ldap-roles backend configuration."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict, Field, field_validator

from waldur_site_agent.common.plugin_schemas import PluginBackendSettingsSchema


class LdapConnectionSchema(PluginBackendSettingsSchema):
    """LDAP connection settings (subset shared with the ldap plugin).

    Only the fields the ldap-roles backend actually reads are validated
    here. Extra keys are allowed because the underlying LdapClient
    consumes additional ones (uid_range_start, etc.) when used in
    other contexts.
    """

    model_config = ConfigDict(extra="allow")

    uri: str = Field(..., description="LDAP server URI (e.g. ldaps://ldap.example.com)")
    bind_dn: str = Field(..., description="DN to bind as")
    bind_password: str = Field(..., description="Password for bind DN")
    base_dn: str = Field(..., description="Base DN for the directory")
    people_ou: str = Field(default="ou=People", description="OU for user entries")
    groups_ou: str = Field(default="ou=Groups", description="OU for group entries")
    use_starttls: Optional[bool] = Field(default=False, description="Use STARTTLS")
    project_group_object_classes: Optional[list[str]] = Field(
        default=None,
        description=(
            "Object classes of the groups this backend creates. Defaults to "
            "posixGroup for memberUid and groupOfNames for member; the class that "
            "carries the membership attribute must be present"
        ),
    )
    empty_group_member_dn: Optional[str] = Field(
        default=None,
        description=(
            "Stand-in member that keeps a groupOfNames valid when no user holds its "
            "role (default cn=nobody,<base_dn>); must not be a uid= DN"
        ),
    )


class LdapRolesBackendSettingsSchema(PluginBackendSettingsSchema):
    """Top-level backend_settings schema for the ldap-roles backend."""

    model_config = ConfigDict(extra="allow")

    # Waldur API access -- the backend fetches Resource and
    # ResourceProject user roles via the SDK every cycle.
    waldur_api_url: str = Field(
        ...,
        description="Waldur API base URL (e.g. https://waldur.example.com/api/)",
    )
    waldur_api_token: str = Field(..., description="Waldur API token")
    waldur_verify_ssl: Optional[bool] = Field(default=True, description="Verify TLS for Waldur API")

    # Group naming -- string.Template syntax, same variables as
    # rancher-kc-crd: ${resource_slug}, ${rp_uuid}, ${rp_uuid_short},
    # ${role_name}, ${customer_slug}, ${project_slug}, ${project_name}.
    resource_group_template: str = Field(
        default="${resource_slug}_${role_name}",
        description="Group name template for Resource-scope roles",
    )
    resource_project_group_template: str = Field(
        default="${resource_slug}_${rp_uuid_short}_${role_name}",
        description="Group name template for ResourceProject-scope roles",
    )

    # Role mappings -- only roles present in the maps are emitted as
    # groups. Values become the ${role_name} substitution in the
    # templates above.
    resource_role_map: dict[str, str] = Field(
        default_factory=dict,
        description="Map of Waldur role name -> output role token for Resource-scope roles",
    )
    resource_project_role_map: dict[str, str] = Field(
        default_factory=dict,
        description="Map of Waldur role name -> output role token for ResourceProject-scope roles",
    )

    # LDAP membership type -- POSIX vs DN-based.
    membership_type: str = Field(
        default="memberUid",
        description="LDAP membership attribute: 'memberUid' (POSIX) or 'member' (DN-based)",
    )

    # Groups this backend creates carry "managed_by=<tag>;resource=<uuid>"
    # in their description. Only groups carrying that marker are ever
    # reconciled or emptied; a pre-existing group of the same name is
    # left alone.
    managed_by_tag: str = Field(
        default="waldur-site-agent",
        description=(
            "Tag in the ownership marker (managed_by=<tag>;resource=<uuid>) written "
            "to the description of groups this backend creates"
        ),
    )

    # If True, look up users by Waldur user_uuid instead of username.
    # Off by default because Waldur usernames are the universal join
    # key with LDAP uid in self-hosted deployments.
    lookup_by_user_uuid: Optional[bool] = Field(
        default=False,
        description="Look up LDAP users by Waldur UUID instead of username",
    )

    ldap: LdapConnectionSchema = Field(..., description="LDAP connection settings")

    @field_validator("membership_type")
    @classmethod
    def validate_membership_type(cls, v: str) -> str:
        """Validate that membership_type is one of the supported values."""
        allowed = {"memberUid", "member"}
        if v not in allowed:
            msg = f"membership_type must be one of {allowed}"
            raise ValueError(msg)
        return v
