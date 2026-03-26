"""MUP Backend for waldur site agent.

This module provides integration between Waldur Mastermind and MUP
(Portuguese project allocation portal). It implements the backend interface
for managing project allocations and user memberships.

Mapping:
- Waldur Project -> MUP Project (1:1, using grant number from project name)
- Waldur Resource -> MUP Allocation (multiple allocations per project)
- Waldur User -> MUP User
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional, cast

import pycountry

from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.project_user import ProjectUser
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits

from waldur_site_agent.backend import BackendType, backends
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.backend.exceptions import BackendError, BackendNotReadyError
from waldur_site_agent_mup.client import MUPClient, MUPError

logger = logging.getLogger(__name__)

GENDER_MAP = {
    0: "Don't want to reply",
    1: "Male",
    2: "Female",
    9: "Don't want to reply",
}

ORG_TYPE_MAPPING = {
    "university": "Academia",
    "research-institution": "Academia",
    "research": "Academia",
    "company": "Industry",
    "government": "Public Administration",
}

COUNTRIES = [(country.alpha_2, country.name) for country in pycountry.countries]
COUNTRIES_DICT = cast(dict[str, str], dict(COUNTRIES))

PROJECT_MANAGER_ROLE = "PROJECT.MANAGER"

class MUPBackend(backends.BaseBackend):
    """MUP backend implementation for Waldur Site Agent.

    This backend manages the lifecycle of project allocations in MUP based on
    Waldur marketplace orders and handles user membership synchronization.
    """

    def __init__(self, mup_settings: dict, mup_components: dict[str, dict]) -> None:
        """Init backend info and creates a corresponding client."""
        super().__init__(mup_settings, mup_components)
        self.backend_type = BackendType.MUP.value

        # Required settings
        required_settings = ["api_url", "username", "password"]
        for setting in required_settings:
            if setting not in mup_settings:
                raise ValueError(f"Missing required setting: {setting}")

        # Validate components - MUP only supports limit-based accounting
        for component_name, component_config in mup_components.items():
            accounting_type = component_config.get("accounting_type")
            if accounting_type != "limit":
                raise ValueError(
                    f"MUP backend only supports components with accounting_type='limit'. "
                    f"Component '{component_name}' has accounting_type='{accounting_type}'"
                )

        self.client: MUPClient = MUPClient(
            api_url=mup_settings["api_url"],
            username=mup_settings["username"],
            password=mup_settings["password"],
        )

        # Backend-specific settings with defaults
        self.default_research_field = mup_settings.get("default_research_field", 1)
        self.default_agency = mup_settings.get("default_agency", "FCT")
        self.project_prefix = mup_settings.get("project_prefix", "waldur_")
        self.allocation_prefix = mup_settings.get("allocation_prefix", "alloc_")
        self.default_storage_limit = mup_settings.get(
            "default_storage_limit", 1000
        )  # GB

        # User creation defaults - configurable to support different institutions/countries
        self.user_defaults = {
            "salutation": mup_settings.get("default_user_salutation", "Dr."),
            "gender": mup_settings.get("default_user_gender", "Other"),
            "year_of_birth": mup_settings.get("default_user_birth_year", 1990),
            "country": mup_settings.get("default_user_country", "Portugal"),
            "type_of_institution": mup_settings.get(
                "default_user_institution_type", "Academic"
            ),
            "affiliated_institution": mup_settings.get(
                "default_user_institution", "Research Institution"
            ),
            "biography": mup_settings.get(
                "default_user_biography",
                "Researcher using Waldur site agent for resource allocation",
            ),
            "funding_agency_prefix": mup_settings.get(
                "user_funding_agency_prefix", "WALDUR-SITE-AGENT-"
            ),
        }

        # Cache for research fields and user mappings
        self._research_fields_cache: Optional[list[dict]] = None
        self._user_cache: dict[str, int] = {}
        self._project_cache: dict[str, dict] = {}

    def ping(self, raise_exception: bool = False) -> bool:
        """Check if MUP backend is available and accessible."""
        try:
            # Try to get research fields as a simple connectivity test
            self.client.get_research_fields()
        except Exception as e:
            if raise_exception:
                raise BackendError(f"MUP backend not available: {e}") from e
            logger.exception("MUP backend not available")
            return False
        else:
            return True

    def diagnostics(self) -> bool:
        """Logs info about the MUP cluster."""
        return True

    def list_components(self) -> list[str]:
        """Return a list of components supported by MUP backend."""
        return list(self.backend_components.keys())

    def get_research_fields(self) -> list[dict]:
        """Get and cache research fields."""
        if self._research_fields_cache is None:
            self._research_fields_cache = self.client.get_research_fields()
        return self._research_fields_cache

    def _raise_no_allocations_error(self) -> None:
        """Raise error when no allocations were created."""
        msg = "No allocations were created - all components had zero limits or failed"
        raise BackendError(msg)

    def _map_gender(self, gender_code: Optional[int]) -> Optional[str]:

        if gender_code is None:
            return None

        return GENDER_MAP.get(gender_code, "Other")

    def _map_country(self, country_code: Optional[str]) -> Optional[str]:
        if not country_code:
            return None

        return COUNTRIES_DICT.get(country_code.upper(), country_code)

    def _extract_birth_year(self, birth_date) -> Optional[int]:
        if birth_date is None:
            return None

        if isinstance(birth_date, (date, datetime)):
            return birth_date.year

        return None

    def _map_organization_type_to_mup_type(self, org_type: Optional[str]) -> str:
        if not org_type:
            return "Other"

        # The field can be SCHAC URN or simple identifier
        if org_type.startswith("urn:schac:homeOrganizationType:"):
            parts = org_type.split(":")
            if len(parts) >= 4:
                org_type = parts[-1]

        org_type = org_type.lower()
        return ORG_TYPE_MAPPING.get(org_type, "Other")

    def _year_from_attr_birth_date(self, val: object) -> Optional[int]:
        """Extract birth year from serialized user_attributes birth_date."""
        if val is None:
            return None
        if isinstance(val, str) and len(val) >= 4:
            try:
                return int(val[:4])
            except ValueError:
                return None
        if isinstance(val, (date, datetime)):
            return val.year
        return None

    def _build_waldur_user_dict_from_attributes(
        self, username: str, attrs: dict[str, Any], email: str
    ) -> dict[str, Any]:
        """Build the waldur_user dict for _get_or_create_user from membership kwargs.

        Keys match what _get_or_create_user reads from OfferingUser-backed dicts.
        """
        full_name = str(attrs.get("full_name") or "")
        name_parts = full_name.split(" ", 1) if full_name else ["", ""]
        first_name = str(attrs.get("first_name") or name_parts[0])
        last_name = str(
            attrs.get("last_name") or (name_parts[1] if len(name_parts) > 1 else "")
        )

        gender_raw = attrs.get("gender")
        gender_out: Optional[str] = None
        if isinstance(gender_raw, int):
            gender_out = self._map_gender(gender_raw)
        elif isinstance(gender_raw, str) and gender_raw:
            gender_out = gender_raw

        country_raw = attrs.get("country_of_residence")
        country_out: Optional[str] = None
        if isinstance(country_raw, str) and country_raw:
            country_out = self._map_country(country_raw) or country_raw

        org_type_raw = attrs.get("organization_type")
        inst_out: Optional[str] = None
        if org_type_raw:
            inst_out = self._map_organization_type_to_mup_type(str(org_type_raw))

        affil_raw = attrs.get("organization")
        affil_str = str(affil_raw) if isinstance(affil_raw, str) else ""

        return {
            "username": username,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "personal_title": attrs.get("personal_title"),
            "gender": gender_out,
            "year_of_birth": self._year_from_attr_birth_date(attrs.get("birth_date")),
            "country": country_out,
            "type_of_institution": inst_out,
            "affiliated_institution": affil_str,
        }

    def _parse_backend_id(self, backend_id: str) -> tuple[int, Optional[int]]:
        """Parse a backend_id string into (project_id, allocation_id)."""
        if "_" in backend_id:
            parts = backend_id.split("_", 1)
            try:
                return int(parts[0]), int(parts[1])
            except ValueError:
                pass
        return int(backend_id), None

    @staticmethod
    def _to_waldur_units(raw: Any, unit_factor: int) -> int:
        """Convert MUP usage to Waldur units using unit_factor."""
        try:
            value = int(raw)
        except (TypeError, ValueError):
            try:
                value = int(float(raw))
            except (TypeError, ValueError):
                return 0
        return value // max(unit_factor, 1)

    def _extract_grant_from_project_name(self, project_name: str) -> str:
        if not project_name or not project_name.strip():
            msg = "Project name is required to extract grant number"
            raise BackendError(msg)

        parts = [part.strip() for part in project_name.split("/")]

        if len(parts) < 2:
            msg = (
                f"Project name '{project_name}' does not contain grant number. "
            )
            raise BackendError(msg)

        grant_number = parts[1]
        if not grant_number:
            msg = (
                f"Grant number is empty in project name '{project_name}'. "
            )
            raise BackendError(msg)

        logger.debug("Extracted grant number '%s' from project name '%s'", grant_number, project_name)
        return grant_number

    def _get_or_create_user(self, waldur_user: dict) -> Optional[int]:  # noqa: PLR0911
        """Get or create MUP user based on Waldur user information.

        Returns MUP user ID or None if creation fails.
        """
        email = waldur_user.get("email")
        username = waldur_user.get(
            "username", email.split("@")[0] if email else "unknown"
        )

        if not email:
            logger.error("User %s has no email address", username)
            return None

        # Check cache first
        if email in self._user_cache:
            logger.debug("Found cached user ID for %s", email)
            return self._user_cache[email]

        # Search for existing active user by email
        try:
            user = self.client.get_user_by_email(email)
            if user and user.get("id"):
                user_id = user["id"]
                self._user_cache[email] = user_id
                logger.info("Found existing MUP user %s for %s", user_id, email)
                return user_id
        except MUPError as e:
            logger.warning("Failed to search for existing user %s: %s", email, e)
            # Continue to attempt user creation
        except Exception as e:
            logger.exception("Unexpected error searching for user %s: %s", email, e)
            return None
        # Create new user request and attempt to find the created request
        logger.info("Attempting to create new MUP user for %s", email)
        try:
            # Complete payload - server cannot handle None values, needs real data
            user_data = {
                # Required fields
                "username": username,
                "email": email,
                "research_fields": [self.default_research_field],  # Must be a list
                # Personal information - use configurable defaults
                "first_name": waldur_user.get("first_name", ""),
                "last_name": waldur_user.get("last_name", ""),
                "salutation": waldur_user.get("personal_title") or self.user_defaults["salutation"],
                "gender": waldur_user.get("gender") or self.user_defaults["gender"],
                "year_of_birth": waldur_user.get("year_of_birth") or self.user_defaults["year_of_birth"],
                "country": waldur_user.get("country") or self.user_defaults["country"],
                "type_of_institution": waldur_user.get("type_of_institution") or self.user_defaults["type_of_institution"],
                "affiliated_institution": waldur_user.get("affiliated_institution") or self.user_defaults["affiliated_institution"],
                # Agency and funding information
                "agency": self.default_agency,
                "funding_agency": self.default_agency,
                "funding_agency_grant": self.user_defaults["funding_agency_prefix"]
                + username[:10].upper(),
                # Request details
                "request_reason": "User account creation via Waldur site agent for project access",
                "rejection_reason": "",  # Empty for new requests
                "biography": self.user_defaults["biography"],
                # Timestamps
                "last_login": None,  # null for new users is OK
                # Boolean flags - conservative defaults for new users
                "is_verified": False,  # Will be set by MUP workflow
                "is_approved": False,  # Will be set by admin approval
                "is_active": False,  # Will be activated after approval
                "is_staff": False,
                "is_admin": False,
                "is_totp_enabled": False,
                "has_subscribe_newsletter": False,
                "has_read_and_accepted_terms_of_service": True,
                "has_read_and_accepted_data_sharing_policy": True,
                # Permission arrays - empty for new users
                "groups": [],
                "user_permissions": [],
            }
            result = self.client.create_user_request(user_data)

            # The API should now return an ID directly, but keep fallback for robustness
            user_id = result.get("id")

            if not user_id:
                # Try to find the user request by email
                logger.info(
                    "User request created but no ID returned, searching for request by email"
                )
                user_id = self._find_user_request_by_email(email)

            if user_id:
                self._user_cache[email] = user_id
                logger.info("Created MUP user request %s for %s", user_id, email)
                return user_id
            logger.warning(
                "User request created for %s but unable to retrieve user ID. "
                "User may need manual approval in MUP admin interface.",
                email,
            )
            return None

        except MUPError as e:
            # Check if it's a server error (5xx) - common issue with MUP dev server
            if "500 Server Error" in str(e):
                logger.warning(
                    "MUP server error creating user %s - this is a known issue with dev server. "
                    "User may need to be created manually. Error: %s",
                    email,
                    str(e)[:200],
                )
            else:
                logger.error("MUP API error creating user %s: %s", email, e)
            return None
        except Exception as e:
            logger.exception("Unexpected error creating user %s: %s", email, e)
            return None

    def _find_user_request_by_email(self, email: str) -> Optional[int]:
        """Find a user request by email address.

        Returns the user request ID if found, None otherwise.
        """
        try:
            # Get list of user requests
            user_requests = self.client.get_user_requests()

            # Look for our email in recent requests (last 10)
            for request in user_requests[-10:]:
                if request.get("email") == email:
                    request_id = request.get("id")
                    if request_id:
                        logger.info(
                            "Found user request %s for email %s", request_id, email
                        )
                        return request_id

            logger.warning("Could not find user request for email %s", email)
            return None

        except Exception as e:
            logger.warning("Error searching for user request by email %s: %s", email, e)
            return None

    def _get_project_by_grant(self, grant_number: str) -> Optional[dict]:
        """Find MUP project by grant number (from Waldur project name)."""
        if grant_number in self._project_cache:
            return self._project_cache[grant_number]

        try:
            project = self.client.get_project_by_grant(grant_number)
            if project:
                self._project_cache[grant_number] = project
                logger.debug("Found MUP project %s for grant %s", project.get("id"), grant_number)
                return project
        except MUPError:
            logger.debug("Project with grant %s not found in MUP", grant_number)
        except Exception as e:
            logger.exception("Unexpected error searching for project with grant %s: %s", grant_number, e)

        return None

    def _create_mup_project(
        self, project_name: str, grant_number: str, project_uuid: str, pi_user_email: str, description: Optional[str] = None
    ) -> Optional[dict]:
        """Create MUP project from Waldur project data.

        """
        try:
            # Calculate project dates (default to 1 year if not specified)
            start_date = datetime.now()
            end_date = start_date + timedelta(days=365)

            project_data = {
                "title": project_name,
                "description": description or f"Waldur project {project_name}",
                "pi": pi_user_email,  # PI email
                "co_pi": "",  # Could be mapped from project managers
                "science_field": self.default_research_field,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "agency": self.default_agency,
                "grant_number": grant_number,
                "max_storage": self.default_storage_limit,
                "ai_included": False,
            }

            result = self.client.create_project(project_data)
            logger.info(
                "Created MUP project %s for Waldur project %s (grant: %s)",
                result.get("id"),
                project_uuid,
                grant_number,
            )

            # Cache the result
            self._project_cache[grant_number] = result
            return result

        except MUPError:
            logger.exception(
                "Failed to create MUP project for grant %s (project %s)", grant_number, project_uuid
            )
            return None

    def _find_pi_from_context(
        self, user_context: Optional[dict]
    ) -> Optional[ProjectUser]:
        """Find Principal Investigator (PI) from user context by role.

        PI is identified as the user with PROJECT_MANAGER_ROLE in the project team.

        Args:
            user_context: User context with team members and offering users

        Returns:
            ProjectUser object for PI, or None if not found
        """
        if not user_context:
            logger.warning("No user context provided, using fallback PI email")
            return None

        # Look for users with PROJECT.MANAGER role (PI)
        team = user_context.get("team", [])
        for user in team:
            # Check if user has manager role (PI role)
            if hasattr(user, "role") and user.role == PROJECT_MANAGER_ROLE:
                logger.info("Found PI with role '%s': %s", user.role, user.email)
                return user

        logger.warning("No user with role '%s' found in team, Team roles: %s", PROJECT_MANAGER_ROLE, [getattr(user, "role", None) for user in team])
        return None

    def _build_user_data_from_context(
        self, team_user: ProjectUser, offering_user: Optional[OfferingUser] = None
    ) -> Optional[dict]:
        """Build user data dict from team user and offering user.

        Args:
            team_user: ProjectUser object with basic user info
            offering_user: Optional OfferingUser object with additional attributes

        Returns:
            User data dict ready for MUP API, or None if username/email is missing.
        """
        if not offering_user or not offering_user.username:
            logger.warning(
                "No offering user username for %s, cannot build MUP user data",
                getattr(team_user, "email", "unknown"),
            )
            return None

        # Get full name from offering user or team user
        full_name = offering_user.user_full_name or getattr(team_user, "full_name", "")
        name_parts = full_name.split(" ", 1) if full_name else ["", ""]

        user_data = {
            "username": offering_user.username,
            "email": offering_user.user_email if offering_user else team_user.email,
            "first_name": getattr(team_user, "first_name", name_parts[0]),
            "last_name": getattr(
                team_user,
                "last_name",
                name_parts[1] if len(name_parts) > 1 else "",
            ),
            "personal_title": (
                offering_user.user_personal_title if offering_user else None
            ),
            "gender": (
                self._map_gender(offering_user.user_gender) if offering_user else None
            ),
            "year_of_birth": (
                self._extract_birth_year(offering_user.user_birth_date)
                if offering_user
                else None
            ),
            "country": (
                self._map_country(offering_user.user_country_of_residence)
                if offering_user
                else None
            ),
            "affiliated_institution": (
                offering_user.customer_name or offering_user.user_organization
                if offering_user
                else None
            ),
            "type_of_institution": (
                self._map_organization_type_to_mup_type(offering_user.user_organization_type)
                if offering_user
                else None
            ),
        }

        if not user_data["email"]:
            return None

        return user_data

    def _create_mup_user(
        self, team_user: ProjectUser, offering_user: Optional[OfferingUser] = None
    ) -> Optional[int]:
        """Create a single user in MUP from team user and optional offering user.

        Args:
            team_user: ProjectUser object with basic user info
            offering_user: Optional OfferingUser object with additional attributes

        Returns:
            MUP user ID if created successfully, None otherwise
        """
        user_data = self._build_user_data_from_context(team_user, offering_user)
        if not user_data:
            logger.error("User has no email address, cannot create in MUP")
            return None

        mup_user_id = self._get_or_create_user(user_data)
        if mup_user_id:
            logger.debug("Created/found MUP user %s (ID: %s)", user_data["email"], mup_user_id)

        return mup_user_id

    def _create_and_add_users_from_context(
        self, project_id: int, user_context: dict
    ) -> None:
        """Create users and add them to the MUP project.

        Checks per-user if they're already members (doesn't skip entirely if project has members).
        Only adds users that are not already in the project.

        Args:
            project_id: MUP project ID
            user_context: User context with team members and offering users
        """
        offering_user_mappings: dict[str, OfferingUser] = user_context.get("offering_user_mappings", {})
        user_mappings: dict[str, ProjectUser] = user_context.get("user_mappings", {})

        # Get existing project members
        existing_member_emails = set()
        try:
            existing_members = self.client.get_project_members(project_id)
            for member in existing_members:
                email = member.get("email")
                if email:
                    existing_member_emails.add(email)
        except MUPError as e:
            logger.warning(
                "Failed to get existing project members, will attempt to add all users: %s", e
            )

        total_users = len(offering_user_mappings)
        successful_users = 0
        skipped_users = 0
        failed_users = 0

        logger.info("Processing %d users for MUP project %d", total_users, project_id)

        # Create and add users who have offering usernames
        for user_uuid, offering_user in offering_user_mappings.items():

            try:
                # Only process users whose offering account is fully approved
                if offering_user.state != OfferingUserState.OK:
                    logger.debug(
                        "Offering user %s is not in OK state (%s), skipping for now "
                        "(will be added by membership sync once approved)",
                        offering_user.user_email or user_uuid,
                        offering_user.state,
                    )
                    skipped_users += 1
                    continue

                # Get user info from team
                team_user = user_mappings.get(user_uuid)
                if not team_user:
                    logger.warning(
                        "No team user found for UUID %s, skipping", user_uuid
                    )
                    failed_users += 1
                    continue

                # Build user data
                user_data = self._build_user_data_from_context(team_user, offering_user)
                if not user_data:
                    logger.warning(
                        "User %s has no email, skipping", team_user.username
                    )
                    failed_users += 1
                    continue

                # Check if user is already a member
                if user_data["email"] in existing_member_emails:
                    logger.info(
                        "User %s is already a member of project %d, skipping",
                        user_data["email"],
                        project_id,
                    )
                    skipped_users += 1
                    continue

                # Create or get user in MUP
                mup_user_id = self._create_mup_user(team_user, offering_user)
                if mup_user_id:
                    try:
                        # Add user to project
                        member_data = {
                            "user_id": mup_user_id,
                            "email": user_data["email"],
                            "active": True,
                        }
                        self.client.add_project_member(project_id, member_data)
                        logger.info(
                            "Added user %s to MUP project %d",
                            user_data["email"],
                            project_id,
                        )
                        successful_users += 1
                    except Exception as e:
                        logger.error(
                            "Failed to add user %s (ID: %s) to project %d: %s",
                            user_data["email"],
                            mup_user_id,
                            project_id,
                            e,
                        )
                        failed_users += 1
                else:
                    logger.warning(
                        "Could not create/find MUP user for %s, skipping project membership",
                        user_data["email"],
                    )
                    failed_users += 1

            except Exception as e:
                logger.exception(
                    "Unexpected error processing user %s for project %d: %s",
                    user_uuid,
                    project_id,
                    e,
                )
                failed_users += 1

        # Summary logging
        if successful_users > 0:
            logger.info(
                "Successfully added %d/%d users to MUP project %d",
                successful_users,
                total_users,
                project_id,
            )
        if skipped_users > 0:
            logger.warning(
                "Skipped %d users (already members or PI)", skipped_users
            )
        if failed_users > 0:
            logger.warning(
                "Failed to add %d/%d users to MUP project %d (check logs above for details)",
                failed_users,
                total_users,
                project_id,
            )

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Create and activate MUP project (if needed) for Waldur project.
        Uses grant number from project name to map Waldur Project -> MUP Project (1:1).
        """
        # Get project information
        project_uuid = waldur_resource.project_uuid
        project_name = waldur_resource.project_name

        if not project_uuid:
            msg = "No project UUID found in resource data"
            raise BackendError(msg)

        if not project_name:
            msg = (
                f"Project name is required to extract grant number."
                f"Resource: {waldur_resource.name}, Project UUID: {project_uuid}"
            )
            raise BackendError(msg)

        grant_number = self._extract_grant_from_project_name(project_name)

        mup_project = self._get_project_by_grant(grant_number)
        if not mup_project:
            # Find and create PI user FIRST (before project creation)
            pi_user = None
            pi_email = None

            if user_context:
                pi_user = self._find_pi_from_context(user_context)
                if pi_user:
                    # Get offering user for PI if available (for attribute mapping)
                    offering_user_mappings = user_context.get("offering_user_mappings", {})
                    pi_offering_user = offering_user_mappings.get(pi_user.uuid)


                    if pi_offering_user is None or pi_offering_user.state != OfferingUserState.OK:
                        pi_state = (
                            pi_offering_user.state if pi_offering_user else "missing"
                        )
                        raise BackendNotReadyError(
                            f"PI {pi_user.email} offering user is not in OK state yet "
                            f"(current state: {pi_state}). "
                            f"Will retry on the next polling cycle."
                        )

                    # Create PI user in MUP
                    pi_mup_user_id = self._create_mup_user(pi_user, pi_offering_user)
                    if not pi_mup_user_id:
                        msg = (
                            f"Failed to create PI user for project {project_uuid}. "
                            f"PI email: {pi_user.email}"
                        )
                        raise BackendError(msg)

                    pi_email = pi_user.email
                    logger.info("Created PI user %s in MUP before project creation", pi_email)
                else:
                    msg = (
                        f"No user with role '{PROJECT_MANAGER_ROLE}' found in project team. "
                        f"Project: {project_uuid}, Grant: {grant_number}"
                    )
                    # PI is required for project creation, do not fail the order but wait for PI
                    raise BackendNotReadyError(msg)
            else:
                msg = (
                    f"No user context provided. Cannot identify PI for project {project_uuid}. "
                    f"Grant: {grant_number}"
                )
                raise BackendError(msg)

            # Create MUP project
            mup_project = self._create_mup_project(
                project_name=project_name,
                grant_number=grant_number,
                project_uuid=str(project_uuid),
                pi_user_email=pi_email,
                description=waldur_resource.project_description,
            )
            if not mup_project:
                msg = f"Failed to create MUP project for grant {grant_number}"
                raise BackendError(msg)

        # Activate project if needed
        if not mup_project.get("active", False):
            try:
                self.client.activate_project(mup_project["id"])
                logger.info("Activated MUP project %s", mup_project["id"])
            except MUPError as e:
                logger.warning(
                    "Failed to activate project %s: %s", mup_project["id"], e
                )

        # Create and add users to project if user context is available

        if user_context:
            self._create_and_add_users_from_context(
                mup_project["id"], user_context
            )

    def post_create_resource(
        self,
        backend_resource_info: BackendResourceInfo,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Perform actions after resource creation - create allocations."""
        # Extract grant number from project name
        if not waldur_resource.project_name:
            msg = (
                f"Project name is required to extract grant number. "
                f"Resource: {waldur_resource.name}, Project UUID: {waldur_resource.project_uuid}"
            )
            raise BackendError(msg)

        grant_number = self._extract_grant_from_project_name(waldur_resource.project_name)

        # Get the MUP project by grant (same project for all resources in Waldur project)
        mup_project = self._get_project_by_grant(grant_number)
        if not mup_project:
            logger.error(
                "No MUP project found for grant %s (resource %s)",
                grant_number,
                waldur_resource.uuid.hex
            )
            return

        # Create allocations for each configured backend component.
        # Iterate backend_components (not backend_resource_info.limits) so that
        # an allocation is always created even when the resource was ordered with
        # no limits (limits can be updated later via Update orders).
        limits = backend_resource_info.limits
        created_allocations = []

        for component_key, component_config in self.backend_components.items():
            # Use whatever limit was provided; default to 0 if not set.
            component_limit = limits.get(component_key, 0)

            allocation_type = component_config.get(
                "mup_allocation_type",
                "Deucalion x86_64",  # Default fallback
            )

            # Apply unit factor for the allocation size; size=0 is valid in MUP.
            unit_factor = component_config.get("unit_factor", 1)
            allocation_size = int(component_limit * unit_factor)

            allocation_data = {
                "type": allocation_type,
                "identifier": (
                    f"{self.allocation_prefix}{waldur_resource.uuid.hex}_{component_key}"
                ),
                "size": allocation_size,
                "used": 0,
                "active": True,
                "project": mup_project["id"],
            }

            try:
                result = self.client.create_allocation(
                    mup_project["id"], allocation_data
                )
                created_allocations.append(
                    {
                        "id": result["id"],
                        "component": component_key,
                        "type": allocation_type,
                        "identifier": allocation_data["identifier"],
                    }
                )

                logger.info(
                    "Created MUP allocation %s (%s) for resource %s",
                    result["id"],
                    allocation_type,
                    waldur_resource.uuid.hex,
                )

            except MUPError as e:
                logger.error(
                    "Failed to create allocation for component %s: %s",
                    component_key,
                    e,
                )

        logger.info(
            "Created %d allocations for resource %s",
            len(created_allocations),
            waldur_resource.uuid.hex,
        )

        if not created_allocations:
            raise BackendError(
                f"Failed to create any MUP allocations for resource "
                f"{waldur_resource.uuid.hex} (project {mup_project['id']}). "
                f"Check logs above for individual component errors."
            )

        # Update backend_id to combined "project_id_allocation_id" format.
        primary_alloc_id = created_allocations[0]["id"]
        combined_id = f"{mup_project['id']}_{primary_alloc_id}"
        backend_resource_info.backend_id = combined_id
        logger.info(
            "Set backend_id to '%s' for resource %s (project %s, allocation %s)",
            combined_id,
            waldur_resource.uuid.hex,
            mup_project["id"],
            primary_alloc_id,
        )

    def _setup_resource_limits(
        self, resource_backend_id: str, waldur_resource: WaldurResource
    ) -> dict[str, int]:
        """Setup resource limits from Waldur resource."""
        del resource_backend_id
        return waldur_resource.limits.to_dict()

    def _create_resource_in_backend(self, waldur_resource: WaldurResource) -> str:
        """Create MUP allocations for Waldur resource within the MUP project.

        Multiple resources in the same Waldur project create multiple allocations
        in the same MUP project.
        """
        # Extract grant number from project name
        if not waldur_resource.project_name:
            msg = (
                f"Project name is required to extract grant number. "
                f"Resource: {waldur_resource.name}, Project UUID: {waldur_resource.project_uuid}"
            )
            raise BackendError(msg)

        grant_number = self._extract_grant_from_project_name(waldur_resource.project_name)

        # Get the MUP project by grant (should exist from _pre_create_resource)
        mup_project = self._get_project_by_grant(grant_number)
        if not mup_project:
            msg = (
                f"MUP project is expected to be created in _pre_create_resource. "
                f"Grant: {grant_number}, resource: {waldur_resource.name}, "
                f"project: {waldur_resource.project_uuid}"
            )
            raise BackendError(msg)

        # Create allocations for each backend component
        limits = waldur_resource.limits
        created_allocations = []
        resource_limits = {}

        for component_key, component_config in self.backend_components.items():
            # Get limit value for this component
            if isinstance(limits, ResourceLimits):
                component_limit = (
                    limits[component_key] if component_key in limits else 0
                )  # noqa: SIM401
            else:
                component_limit = 0

            # Skip components with zero limits
            if component_limit <= 0:
                continue

            # Get MUP allocation type for this component from config
            allocation_type = component_config.get(
                "mup_allocation_type",
                "Deucalion x86_64",  # Default fallback
            )

            # Apply unit factor for the allocation size
            unit_factor = component_config.get("unit_factor", 1)
            allocation_size = int(component_limit * unit_factor)

            allocation_data = {
                "type": allocation_type,
                "identifier": (
                    f"{self.allocation_prefix}{waldur_resource.uuid.hex}_{component_key}"
                ),
                "size": allocation_size,
                "used": 0,
                "active": True,
                "project": mup_project["id"],
            }

            try:
                result = self.client.create_allocation(
                    mup_project["id"], allocation_data
                )
                created_allocations.append(
                    {
                        "id": result["id"],
                        "component": component_key,
                        "type": allocation_type,
                        "identifier": allocation_data["identifier"],
                    }
                )
                resource_limits[component_key] = int(component_limit)

                logger.info(
                    "Created MUP allocation %s (%s) for resource %s component %s",
                    result["id"],
                    allocation_type,
                    waldur_resource.uuid.hex,
                    component_key,
                )
            except Exception:
                logger.exception(
                    "Failed to create allocation for component %s", component_key
                )
                # Continue with other components even if one fails
                continue

        if not created_allocations:
            self._raise_no_allocations_error()

        return str(mup_project["id"])

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Collect MUP and Waldur limits separately."""
        allocation_limits: dict = {}
        waldur_resource_limits: dict = {}

        # Extract limits from resource
        limits = waldur_resource.limits
        if not limits:
            return allocation_limits, waldur_resource_limits

        for component_key, data in self.backend_components.items():
            if component_key in limits:
                limit_value = limits[component_key]
                # Apply unit factor
                unit_factor = data.get("unit_factor", 1)
                allocation_limits[component_key] = limit_value * unit_factor
                waldur_resource_limits[component_key] = limit_value

        return allocation_limits, waldur_resource_limits

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Downscale the account on the backend - not supported by MUP."""
        logger.warning(
            "Downscaling not supported for MUP account %s", resource_backend_id
        )
        return False

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Pause the account on the backend - not supported by MUP."""
        logger.warning("Pausing not supported for MUP account %s", resource_backend_id)
        return False

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Restore the account after downscaling or pausing - not supported by MUP."""
        logger.warning("Restore not supported for MUP account %s", resource_backend_id)
        return False

    def set_resource_limits(
        self, resource_backend_id: str, limits: dict[str, int]
    ) -> None:
        """Set limits for components in the MUP allocations."""
        logger.info("Setting resource limits for %s: %s", resource_backend_id, limits)

        # Convert limits with unit factors
        converted_limits = {
            key: value * self.backend_components[key]["unit_factor"]
            for key, value in limits.items()
            if key in self.backend_components
        }

        try:
            project_id, alloc_id = self._parse_backend_id(resource_backend_id)
            if alloc_id is None:
                raise BackendError(
                    f"backend_id '{resource_backend_id}' has no allocation ID – "
                    "cannot update limits."
                )

            # Fetch the specific allocation directly – no full list scan needed
            allocation = self.client.get_allocation(project_id, alloc_id)

            # Determine which component this allocation belongs to from its identifier
            alloc_identifier = allocation.get("identifier", "")
            component_key = None
            parts = alloc_identifier.split("_")
            if len(parts) >= 3:
                component_key = parts[-1]

            if not component_key or component_key not in converted_limits:
                logger.warning(
                    "No matching limit found for allocation %s (identifier: %s, "
                    "component: %s). Available limits: %s",
                    alloc_id,
                    alloc_identifier,
                    component_key,
                    list(converted_limits.keys()),
                )
                return

            new_size = converted_limits[component_key]
            allocation_data = {
                "type": allocation["type"],
                "identifier": allocation["identifier"],
                "size": new_size,
                "used": allocation.get("used", 0),
                "active": allocation.get("active", True),
                "project": project_id,
            }
            self.client.update_allocation(project_id, alloc_id, allocation_data)
            logger.info(
                "Updated MUP allocation %s (%s) size to %s for resource %s",
                alloc_id,
                component_key,
                new_size,
                resource_backend_id,
            )

        except BackendError:
            raise
        except Exception as e:
            logger.exception(
                "Failed to update resource limits for %s", resource_backend_id
            )
            raise BackendError(f"Failed to update resource limits: {e}") from e

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Get backend-specific resource metadata."""
        try:
            project_id, allocation_id = self._parse_backend_id(resource_backend_id)
            project = self.client.get_project(project_id)
            if not project:
                return {}
            allocations = self.client.get_project_allocations(project_id)
            if not allocations:
                return {}
            # Use the specific allocation if we have its ID, otherwise fall back to first
            allocation = next(
                (alloc for alloc in allocations if alloc.get("id") == allocation_id),
                allocations[0],
            )
            return {
                "mup_project_id": project["id"],
                "mup_allocation_id": allocation["id"],
                "allocation_type": allocation.get("type"),
                "allocation_size": allocation.get("size"),
                "allocation_used": allocation.get("used"),
            }
        except (ValueError, Exception):
            logger.exception(
                "Failed to get resource metadata for %s", resource_backend_id
            )
        return {}

    def _get_usage_report(
        self, resource_backend_ids: list[str]
    ) -> dict[str, dict[str, dict[str, int]]]:
        """Collect usage report for the specified accounts from MUP."""
        report: dict[str, dict[str, dict[str, int]]] = {}

        component_keys = list(self.backend_components.keys())
        comp_key = component_keys[0]
        if len(component_keys) > 1:
            logger.warning(
                "Warning, more than one component is configured for this MUP offering;"
                "attributing usage to the first component %r only (%d components configured)",
                comp_key,
                len(component_keys),
            )

        for backend_id in resource_backend_ids:
            try:
                project_id, allocation_id = self._parse_backend_id(backend_id)
            except ValueError:
                logger.warning(
                    "Cannot parse backend_id '%s', skipping usage report",
                    backend_id,
                )
                continue

            if allocation_id is None:
                logger.warning(
                    "backend_id '%s' has no allocation id, skipping usage report",
                    backend_id,
                )
                continue

            total_usage: dict[str, int] = dict.fromkeys(self.backend_components, 0)
            merged_users: dict[str, dict[str, int]] = {}

            unit_factor = self.backend_components[comp_key].get("unit_factor", None)
            if not unit_factor:
                logger.error("Unit factor not found for component %s", comp_key)
                raise BackendError(f"Unit factor not found for component {comp_key}")

            logger.info("Unit factor for %s: %s", comp_key, unit_factor)
            try:
                data = self.client.get_allocation_usage(project_id, allocation_id)
            except MUPError:
                logger.exception(
                    "Failed to fetch allocation usage for project=%s allocation=%s",
                    project_id,
                    allocation_id,
                )
                report[backend_id] = {"TOTAL_ACCOUNT_USAGE": total_usage}
                continue

            total_raw = data.get("total", 0)
            logger.info("Total raw usage from MUP for %s: %s", comp_key, total_raw)
            total_usage[comp_key] = self._to_waldur_units(total_raw, unit_factor)

            users_raw = data.get("users") or {}
            if isinstance(users_raw, dict):
                for username, raw_val in users_raw.items():
                    if not username:
                        continue
                    waldur_val = self._to_waldur_units(raw_val, unit_factor)
                    user_row = dict.fromkeys(self.backend_components, 0)
                    user_row[comp_key] = waldur_val
                    merged_users[username] = user_row

            entry: dict[str, dict[str, int]] = {
                "TOTAL_ACCOUNT_USAGE": total_usage,
            }
            entry.update(merged_users)
            report[backend_id] = entry

        return report

    def _pull_backend_resource(
        self, resource_backend_id: str
    ) -> Optional[BackendResourceInfo]:
        """Pull resource data from MUP. Necessary for membership sync."""
        logger.info("Pulling MUP resource %s", resource_backend_id)
        project = self._find_project_by_resource_id(resource_backend_id)

        if not project:
            logger.warning(
                "There is no MUP project for resource %s", resource_backend_id
            )
            return None

        # Collect current project members as the user list for this resource.
        try:
            members = self.client.get_project_members(project["id"])
            users = [
                member.get("username")
                for member in members
                if member.get("username")
            ]
        except Exception:
            logger.exception(
                "Failed to fetch project members for resource %s", resource_backend_id
            )
            users = []

        report = self._get_usage_report([resource_backend_id])
        usage = report.get(resource_backend_id)
        if usage is None:
            usage = {"TOTAL_ACCOUNT_USAGE": dict.fromkeys(self.backend_components, 0)}

        return BackendResourceInfo(
            backend_id=resource_backend_id,
            users=users,
            usage=usage,
        )

    def _find_project_by_resource_id(self, resource_backend_id: str) -> Optional[dict]:
        """Find MUP project by resource backend ID.


        """
        try:
            project_id, _ = self._parse_backend_id(resource_backend_id)
            project = self.client.get_project(project_id)
            return project or None
        except (ValueError, Exception):
            logger.exception(
                "Failed to find project for resource %s", resource_backend_id
            )
        return None

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set[str], **kwargs: dict
    ) -> set[str]:
        """Add specified users to the MUP project.

        Handles both users that already exist in MUP (looked up by username or
        email) and users that need to be created first (using ``user_attributes``
        and ``offering_user_states`` from the membership sync processor).
        """
        resource_backend_id = waldur_resource.backend_id
        logger.info(
            "Adding users to MUP project for resource %s: %s",
            resource_backend_id,
            ", ".join(user_ids),
        )
        added_users: set[str] = set()

        target_project = self._find_project_by_resource_id(resource_backend_id)
        if not target_project:
            logger.error("No MUP project found for resource %s", resource_backend_id)
            return added_users

        user_emails: dict[str, str] = kwargs.get("user_emails", {})
        user_attributes: dict[str, dict] = kwargs.get("user_attributes") or {}
        offering_user_states_raw: Any = kwargs.get("offering_user_states", {})
        offering_user_states: dict[str, OfferingUserState] = (
            offering_user_states_raw
            if isinstance(offering_user_states_raw, dict)
            else {}
        )

        # Existing project members — used to skip users already in the project.
        existing_members = self.client.get_project_members(target_project["id"])
        existing_usernames = {
            member.get("username")
            for member in existing_members
            if member.get("username")
        }

        for user_id in user_ids:
            try:
                if user_id in existing_usernames:
                    logger.info(
                        "User %s is already a member of MUP project %s",
                        user_id,
                        target_project["id"],
                    )
                    added_users.add(user_id)
                    continue

                mup_user = self.client.get_user_by_username(user_id)

                if not mup_user:
                    email = user_emails.get(user_id)
                    if email:
                        mup_user = self.client.get_user_by_email(email)
                # If user not found we may need to create them.
                if not mup_user:
                    state = offering_user_states.get(user_id)
                    if state != OfferingUserState.OK:
                        logger.warning(
                            "User %s not found in MUP and offering user state is %s "
                            "(need OK to create), skipping",
                            user_id,
                            state,
                        )
                        continue
                    attrs = user_attributes.get(user_id) or {}
                    email = user_emails.get(user_id) or attrs.get("email")
                    if not email or not isinstance(email, str):
                        logger.warning(
                            "User %s not found in MUP and no email available, skipping",
                            user_id,
                        )
                        continue
                    waldur_user = self._build_waldur_user_dict_from_attributes(
                        user_id, attrs, email
                    )
                    mup_user_id = self._get_or_create_user(waldur_user)
                    if mup_user_id:
                        mup_user = self.client.get_user_by_username(user_id) or self.client.get_user_by_email(
                            email
                        )
                    else:
                        logger.warning(
                            "User %s not found in MUP and user creation failed, skipping",
                            user_id,
                        )
                        continue

                if not mup_user:
                    logger.warning(
                        "User %s could not be found or created in MUP, skipping.",
                        user_id,
                    )
                    continue

                member_data = {
                    "user_id": mup_user["id"],
                    "email": mup_user["email"],
                    "active": True,
                }
                try:
                    self.client.add_project_member(target_project["id"], member_data)
                    added_users.add(user_id)
                    logger.info(
                        "Added user %s to MUP project %s", user_id, target_project["id"]
                    )
                except MUPError as e:
                    if "already a member" in str(e).lower():
                        logger.info(
                            "User %s is already a member of MUP project %s (PI or pre-existing)",
                            user_id,
                            target_project["id"],
                        )
                        added_users.add(user_id)
                    else:
                        raise

            except MUPError:
                logger.exception("Failed to add user %s to MUP project", user_id)

        return added_users

    def remove_users_from_resource(
        self,
        waldur_resource: WaldurResource,
        usernames: set[str],
        **kwargs: dict[Any, Any],
    ) -> list[str]:
        """Remove specified users from the MUP project."""
        del kwargs
        resource_backend_id = waldur_resource.backend_id
        logger.info(
            "Removing users from MUP project for resource %s: %s",
            resource_backend_id,
            ", ".join(usernames),
        )
        removed_users: list[str] = []

        # Find the project by resource backend ID
        target_project = self._find_project_by_resource_id(resource_backend_id)

        if not target_project:
            logger.error("No MUP project found for resource %s", resource_backend_id)
            return removed_users

        # Get current project members
        try:
            members = self.client.get_project_members(target_project["id"])

            for username in usernames:
                # Find member by username or email.
                for member in members:
                    if (
                        member.get("username") == username
                        or member.get("email") == username
                    ):
                        # Deactivate member instead of deleting
                        status_data = {"active": False}
                        self.client.toggle_member_status(
                            target_project["id"], member["id"], status_data
                        )
                        removed_users.append(username)
                        logger.info(
                            "Deactivated user %s in MUP project %s",
                            username,
                            target_project["id"],
                        )
                        break
                else:
                    logger.warning(
                        "User %s not found in MUP project %s",
                        username,
                        target_project["id"],
                    )

        except Exception:
            logger.exception("Failed to remove users from MUP project")

        return removed_users
