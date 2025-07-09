"""MUP Backend for waldur site agent.

This module provides integration between Waldur Mastermind and MUP
(Portuguese project allocation portal). It implements the backend interface
for managing project allocations and user memberships.

Mapping:
- Waldur Project -> MUP Project
- Waldur Resource -> MUP Allocation
- Waldur User -> MUP User
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits

from waldur_site_agent.backends import BackendType, backend
from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.backends.mup_backend.client import MUPClient, MUPError

logger = logging.getLogger(__name__)


class MUPBackend(backend.BaseBackend):
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
        self.default_storage_limit = mup_settings.get("default_storage_limit", 1000)  # GB

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

    def _get_or_create_user(self, waldur_user: dict) -> Optional[int]:  # noqa: PLR0911
        """Get or create MUP user based on Waldur user information.

        Returns MUP user ID or None if creation fails.
        """
        email = waldur_user.get("email")
        if not email:
            logger.error("User %s has no email address", waldur_user.get("uuid"))
            return None

        # Check cache first
        if email in self._user_cache:
            return self._user_cache[email]

        # Search for existing user by email
        try:
            users = self.client.get_users()
            for user in users:
                if user.get("email") == email:
                    self._user_cache[email] = user["id"]
                    return user["id"]
        except MUPError:
            logger.exception("Failed to search for user %s", email)
            return None

        # Create new user if not found
        try:
            user_data = {
                "username": waldur_user.get("username", email.split("@")[0]),
                "email": email,
                "first_name": waldur_user.get("first_name", ""),
                "last_name": waldur_user.get("last_name", ""),
                "research_fields": self.default_research_field,
                "agency": self.default_agency,
                "has_read_and_accepted_terms_of_service": True,
                "has_read_and_accepted_data_sharing_policy": True,
                "has_subscribe_newsletter": False,
            }

            result = self.client.create_user_request(user_data)
            user_id = result.get("id")
            if user_id:
                self._user_cache[email] = user_id
                logger.info("Created MUP user %s for %s", user_id, email)
                return user_id

        except MUPError:
            logger.exception("Failed to create user %s", email)
            return None

        return None

    def _get_project_by_waldur_id(self, waldur_resource_uuid: str) -> Optional[dict]:
        """Find MUP project by Waldur resource UUID."""
        if waldur_resource_uuid in self._project_cache:
            return self._project_cache[waldur_resource_uuid]

        try:
            projects = self.client.get_projects()
            for project in projects:
                # Look for our project by grant_number (we store Waldur Resource UUID there)
                if project.get("grant_number") == f"{self.project_prefix}{waldur_resource_uuid}":
                    self._project_cache[waldur_resource_uuid] = project
                    return project
        except MUPError:
            logger.exception("Failed to search for resource %s", waldur_resource_uuid)

        return None

    def _create_mup_project(self, waldur_project: dict, pi_user_email: str) -> Optional[dict]:
        """Create MUP project from Waldur project data."""
        try:
            # Extract project information
            project_name = waldur_project.get("name", f"Project {waldur_project['uuid']}")
            resource_uuid = waldur_project["uuid"]  # This is actually resource UUID now

            # Calculate project dates (default to 1 year if not specified)
            start_date = datetime.now()
            end_date = start_date + timedelta(days=365)

            project_data = {
                "title": project_name,
                "description": waldur_project.get("description", f"Waldur project {project_name}"),
                "pi": pi_user_email,  # PI email
                "co_pi": None,  # Could be mapped from project managers
                "science_field": self.default_research_field,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "agency": self.default_agency,
                "grant_number": (
                    f"{self.project_prefix}{resource_uuid}"  # Store Waldur Resource UUID
                ),
                "max_storage": self.default_storage_limit,
                "ai_included": False,
            }

            result = self.client.create_project(project_data)
            logger.info(
                "Created MUP project %s for Waldur resource %s", result.get("id"), resource_uuid
            )

            # Cache the result
            self._project_cache[resource_uuid] = result
            return result

        except MUPError:
            logger.exception("Failed to create MUP project for resource %s", resource_uuid)
            return None

    def _get_pi_email_from_context(self, user_context: Optional[dict], project_uuid: str) -> str:
        """Get PI email from user context or return a fallback.

        Args:
            user_context: User context with team members and offering users
            project_uuid: Project UUID for fallback generation

        Returns:
            PI email address
        """
        if not user_context:
            logger.warning("No user context provided, using fallback PI email")
            return f"admin@{project_uuid}.example.com"

        # Try to get the first available user email from team members
        team = user_context.get("team", [])
        offering_user_mappings = user_context.get("offering_user_mappings", {})
        # Look for users with offering usernames (these are the active ones)
        for user in team:
            user_uuid = user.uuid
            if user_uuid in offering_user_mappings:
                # This user has an offering username, use their email
                email = user.email
                if email and "@" in email and not email.endswith(".example.com"):
                    logger.info("Using team member %s as PI for project %s", email, project_uuid)
                    return email

        # Fallback to first team member email
        for user in team:
            email = user.email
            if email and "@" in email and not email.endswith(".example.com"):
                logger.info("Using first team member %s as PI for project %s", email, project_uuid)
                return email

        # Final fallback
        logger.warning(
            "No suitable PI email found in user context, using fallback for project %s",
            project_uuid,
        )
        return f"admin@{project_uuid}.example.com"

    def _create_and_add_users_from_context(self, project_id: int, user_context: dict) -> None:
        """Create users and add them to the MUP project during resource creation.

        Args:
            project_id: MUP project ID
            user_context: User context with team members and offering users
        """
        offering_user_mappings = user_context.get("offering_user_mappings", {})

        # Create and add users who have offering usernames
        for user_uuid, offering_user in offering_user_mappings.items():
            try:
                # Get user info from team
                team_user = user_context.get("user_mappings", {}).get(user_uuid)
                if not team_user:
                    continue

                # Create user data combining team and offering info

                # NB! first_name and last_name do not exist on ProjectUser, only full_name
                # needs improvement
                user_data = {
                    "username": offering_user.username,
                    "email": team_user.email,
                    "first_name": team_user.first_name if hasattr(team_user, "first_name") else "",
                    "last_name": team_user.last_name if hasattr(team_user, "last_name") else "",
                }

                if not user_data["email"]:
                    logger.warning("User %s has no email, skipping", user_data["username"])
                    continue

                # Create or get user in MUP
                mup_user_id = self._get_or_create_user(user_data)
                if mup_user_id:
                    # Add user to project
                    member_data = {"user_id": mup_user_id, "active": True}
                    self.client.add_project_member(project_id, member_data)
                    logger.info(
                        "Added user %s to MUP project %s during creation",
                        user_data["email"],
                        project_id,
                    )

            except Exception:
                logger.exception("Failed to add user %s to project during creation", user_uuid)

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        """Create and activate MUP project."""
        # Get project information
        project_uuid = waldur_resource.project_uuid
        project_name = waldur_resource.project_name
        if not project_name:
            project_name = f"Project {project_uuid}"

        if not project_uuid:
            msg = "No project UUID found in resource data"
            raise BackendError(msg)

        # Get or create MUP project (Resource = Project in MUP)
        mup_project = self._get_project_by_waldur_id(waldur_resource.uuid.hex)
        if not mup_project:
            # Get PI email from user context or use a fallback
            pi_email = self._get_pi_email_from_context(user_context, project_uuid)

            project_data = {
                "uuid": waldur_resource.uuid.hex,  # Use resource UUID for MUP project
                "name": project_name,
                "description": f"Waldur project {project_name}",
            }
            mup_project = self._create_mup_project(project_data, pi_email)
            if not mup_project:
                msg = "Failed to create MUP project"
                raise BackendError(msg)

        # Activate project if needed
        if not mup_project.get("active", False):
            try:
                self.client.activate_project(mup_project["id"])
                logger.info("Activated MUP project %s", mup_project["id"])
            except MUPError as e:
                logger.warning("Failed to activate project %s: %s", mup_project["id"], e)

        # Create users and add them to project if user context is available
        if user_context:
            self._create_and_add_users_from_context(mup_project["id"], user_context)

    def _setup_resource_limits(
        self, allocation_account: str, waldur_resource: WaldurResource
    ) -> None:
        """Skip this step for MUP."""
        del allocation_account, waldur_resource

    def _create_resource_in_backend(self, waldur_resource: WaldurResource) -> str:
        """Create MUP resource within the MUP project."""
        mup_project = self._get_project_by_waldur_id(waldur_resource.uuid.hex)
        if not mup_project:
            msg = (
                f"MUP project is expected to be created upon MUP resource creation, "
                f"resource {waldur_resource['name']}, project {waldur_resource['project_uuid']}"
            )
            raise BackendError(msg)

        # Create allocations for each backend component
        limits = waldur_resource.limits
        created_allocations = []
        resource_limits = {}

        for component_key, component_config in self.backend_components.items():
            # Get limit value for this component
            if isinstance(limits, ResourceLimits):
                component_limit = limits[component_key] if component_key in limits else 0  # noqa: SIM401
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
                result = self.client.create_allocation(mup_project["id"], allocation_data)
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
                logger.exception("Failed to create allocation for component %s", component_key)
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

    def downscale_resource(self, account: str) -> bool:
        """Downscale the account on the backend - not supported by MUP."""
        logger.warning("Downscaling not supported for MUP account %s", account)
        return False

    def pause_resource(self, account: str) -> bool:
        """Pause the account on the backend - not supported by MUP."""
        logger.warning("Pausing not supported for MUP account %s", account)
        return False

    def restore_resource(self, account: str) -> bool:
        """Restore the account after downscaling or pausing - not supported by MUP."""
        logger.warning("Restore not supported for MUP account %s", account)
        return False

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """Set limits for components in the MUP allocations."""
        logger.info("Setting resource limits for %s: %s", resource_backend_id, limits)

        # Convert limits with unit factors
        converted_limits = {
            key: value * self.backend_components[key]["unit_factor"]
            for key, value in limits.items()
            if key in self.backend_components
        }

        # resource_backend_id is now the MUP project ID
        try:
            project_id = int(resource_backend_id)
            updated_allocations = 0

            # Get all allocations for this project directly
            allocations = self.client.get_project_allocations(project_id)

            # Update each allocation based on component limits
            for allocation in allocations:
                allocation_id = allocation.get("identifier", "")
                # Extract component from allocation identifier (format: alloc_{uuid}_{component})
                if "_" in allocation_id:
                    parts = allocation_id.split("_")
                    min_parts = 3
                    if len(parts) >= min_parts:
                        component_key = parts[-1]  # Last part is the component

                        # Update allocation if we have a limit for this component
                        if component_key in converted_limits:
                            new_size = converted_limits[component_key]
                            allocation_data = {
                                "type": allocation["type"],
                                "identifier": allocation["identifier"],
                                "size": new_size,
                                "used": allocation.get("used", 0),
                                "active": allocation.get("active", True),
                                "project": project_id,
                            }

                            self.client.update_allocation(
                                project_id, allocation["id"], allocation_data
                            )
                            updated_allocations += 1
                            logger.info(
                                "Updated MUP allocation %s (%s) size to %s",
                                allocation["id"],
                                component_key,
                                new_size,
                            )

            if updated_allocations == 0:
                logger.warning("No allocations were updated for resource %s", resource_backend_id)
            else:
                logger.info(
                    "Updated %d allocations for resource %s",
                    updated_allocations,
                    resource_backend_id,
                )

        except Exception as e:
            logger.exception("Failed to update resource limits for %s", resource_backend_id)
            raise BackendError(f"Failed to update resource limits: {e}") from e

    def get_resource_metadata(self, account: str) -> dict:
        """Get backend-specific resource metadata."""
        # Find project and allocation by account name (project ID)
        try:
            project_id = int(account)
            projects = self.client.get_projects()
            for project in projects:
                if project.get("id") == project_id:
                    allocations = self.client.get_project_allocations(project["id"])
                    if allocations:
                        allocation = allocations[0]
                        return {
                            "mup_project_id": project["id"],
                            "mup_allocation_id": allocation["id"],
                            "allocation_type": allocation.get("type"),
                            "allocation_size": allocation.get("size"),
                            "allocation_used": allocation.get("used"),
                        }
        except (ValueError, Exception):
            logger.exception("Failed to get resource metadata for %s", account)
        return {}

    def _get_usage_report(self, accounts: list[str]) -> dict[str, dict[str, dict[str, int]]]:
        """Collect usage report for the specified accounts from MUP."""
        report: dict[str, dict[str, dict[str, int]]] = {}

        try:
            projects = self.client.get_projects()

            for project in projects:
                project_id_str = str(project.get("id"))
                if project_id_str in accounts:
                    allocations = self.client.get_project_allocations(project["id"])

                    # Initialize account usage
                    report[project_id_str] = {}
                    total_usage: dict[str, int] = {}

                    for allocation in allocations:
                        # Map allocation usage to component usage based on allocation identifier
                        allocation_id = allocation.get("identifier", "")
                        allocation_type = allocation.get("type", "")
                        used = allocation.get("used", 0)

                        # Extract component from identifier (format: alloc_{uuid}_{component})
                        component_key = None
                        if "_" in allocation_id:
                            parts = allocation_id.split("_")
                            min_parts = 3
                            if len(parts) >= min_parts:
                                component_key = parts[-1]  # Last part is the component

                        # If we can't extract component from identifier, map by allocation type
                        if not component_key:
                            # Build reverse mapping from allocation type to component from config
                            type_to_component = {
                                config.get("mup_allocation_type"): comp_key
                                for comp_key, config in self.backend_components.items()
                                if config.get("mup_allocation_type")
                            }
                            component_key = type_to_component.get(allocation_type, "cpu")

                        # Add usage for this component
                        if component_key and component_key in self.backend_components:
                            # Apply reverse unit factor to get Waldur units
                            unit_factor = self.backend_components[component_key].get(
                                "unit_factor", 1
                            )
                            waldur_usage = used // max(unit_factor, 1)
                            total_usage[component_key] = (
                                total_usage.get(component_key, 0) + waldur_usage
                            )

                    # Set total account usage
                    report[project_id_str]["TOTAL_ACCOUNT_USAGE"] = total_usage

                    # Get project members and create per-user usage (placeholder)
                    members = self.client.get_project_members(project["id"])
                    for member in members:
                        if member.get("active", False):
                            username = member.get("member", {}).get("username", "")
                            if username:
                                # For now, assign equal share of usage to active users
                                user_count = len([m for m in members if m.get("active", False)])
                                # Distribute usage across all components for this user
                                user_usage = {}
                                for component_key, component_total in total_usage.items():
                                    user_usage[component_key] = component_total // max(
                                        user_count, 1
                                    )
                                report[project_id_str][username] = user_usage

        except Exception:
            logger.exception("Failed to get usage report")

        return report

    def _find_project_by_resource_id(self, resource_backend_id: str) -> Optional[dict]:
        """Find MUP project by resource backend ID (project ID)."""
        try:
            project_id = int(resource_backend_id)
            projects = self.client.get_projects()

            # Find project by ID
            for project in projects:
                if project.get("id") == project_id:
                    return project

        except (ValueError, Exception):
            logger.exception("Failed to find project for resource %s", resource_backend_id)

        return None

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: set[str], **_kwargs: dict
    ) -> set[str]:
        """Add specified users to the MUP project.

        Note: Most users are already added during resource creation. This method
        handles additional users that may be added later.
        """
        logger.info(
            "Adding users to MUP project for resource %s: %s",
            resource_backend_id,
            ", ".join(user_ids),
        )
        added_users: set[str] = set()

        # Find the project by resource backend ID
        target_project = self._find_project_by_resource_id(resource_backend_id)

        if not target_project:
            logger.error("No MUP project found for resource %s", resource_backend_id)
            return added_users

        # Get existing project members to avoid duplicates
        existing_members = self.client.get_project_members(target_project["id"])
        existing_emails = {
            member.get("member", {}).get("email")
            for member in existing_members
            if member.get("active", False)
        }

        # Add each user to the project (if not already a member)
        for user_id in user_ids:
            try:
                # Assume user_id is email for simplicity
                if user_id in existing_emails:
                    logger.info(
                        "User %s is already a member of project %s", user_id, target_project["id"]
                    )
                    added_users.add(user_id)
                    continue

                user_data = {"username": user_id, "email": user_id}
                mup_user_id = self._get_or_create_user(user_data)
                if mup_user_id:
                    member_data = {"user_id": mup_user_id, "active": True}
                    self.client.add_project_member(target_project["id"], member_data)
                    added_users.add(user_id)
                    logger.info("Added user %s to MUP project %s", user_id, target_project["id"])

            except Exception:
                logger.exception("Failed to add user %s to MUP project", user_id)

        return added_users

    def remove_users_from_resource(
        self, resource_backend_id: str, usernames: set[str]
    ) -> list[str]:
        """Remove specified users from the MUP project."""
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
                # Find member by username or email
                for member in members:
                    member_info = member.get("member", {})
                    if (
                        member_info.get("username") == username
                        or member_info.get("email") == username
                    ):
                        # Deactivate member instead of deleting
                        status_data = {"active": False}
                        self.client.toggle_member_status(
                            target_project["id"], member["id"], status_data
                        )
                        removed_users.append(username)
                        logger.info(
                            "Deactivated user %s in MUP project %s", username, target_project["id"]
                        )
                        break
                else:
                    logger.warning(
                        "User %s not found in MUP project %s", username, target_project["id"]
                    )

        except Exception:
            logger.exception("Failed to remove users from MUP project")

        return removed_users
