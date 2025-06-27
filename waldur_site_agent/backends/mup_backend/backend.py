"""MUP Backend for waldur site agent.

This module provides integration between Waldur Mastermind and MUP
(Portuguese project allocation portal). It implements the backend interface
for managing project allocations and user memberships.

Mapping:
- Waldur Project -> MUP Project
- Waldur Resource -> MUP Allocation
- Waldur User -> MUP User
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from waldur_site_agent.backends import BackendType, backend
from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.backends.mup_backend.client import MUPClient, MUPError
from waldur_site_agent.backends.structures import Resource

logger = logging.getLogger(__name__)


class MUPBackend(backend.BaseBackend):
    """MUP backend implementation for Waldur Site Agent.

    This backend manages the lifecycle of project allocations in MUP based on
    Waldur marketplace orders and handles user membership synchronization.
    """

    def __init__(self, mup_settings: Dict, mup_components: Dict[str, Dict]) -> None:
        """Init backend info and creates a corresponding client."""
        super().__init__(mup_settings, mup_components)
        self.backend_type = BackendType.MUP.value

        # Required settings
        required_settings = ["api_url", "username", "password"]
        for setting in required_settings:
            if setting not in mup_settings:
                raise ValueError(f"Missing required setting: {setting}")

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
        self.default_allocation_type = mup_settings.get("default_allocation_type", "compute")
        self.default_storage_limit = mup_settings.get("default_storage_limit", 1000)  # GB

        # Cache for research fields and user mappings
        self._research_fields_cache: Optional[List[Dict]] = None
        self._user_cache: Dict[str, int] = {}
        self._project_cache: Dict[str, Dict] = {}

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

    def list_components(self) -> List[str]:
        """Return a list of components supported by MUP backend."""
        return list(self.backend_components.keys())

    def get_research_fields(self) -> List[Dict]:
        """Get and cache research fields."""
        if self._research_fields_cache is None:
            self._research_fields_cache = self.client.get_research_fields()
        return self._research_fields_cache

    def _get_or_create_user(self, waldur_user: Dict) -> Optional[int]:  # noqa: PLR0911
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

    def _get_project_by_waldur_id(self, waldur_project_uuid: str) -> Optional[Dict]:
        """Find MUP project by Waldur project UUID."""
        if waldur_project_uuid in self._project_cache:
            return self._project_cache[waldur_project_uuid]

        try:
            projects = self.client.get_projects()
            for project in projects:
                # Look for our project by grant_number (we store Waldur UUID there)
                if project.get("grant_number") == f"{self.project_prefix}{waldur_project_uuid}":
                    self._project_cache[waldur_project_uuid] = project
                    return project
        except MUPError:
            logger.exception("Failed to search for project %s", waldur_project_uuid)

        return None

    def _create_mup_project(self, waldur_project: Dict, pi_user_email: str) -> Optional[Dict]:
        """Create MUP project from Waldur project data."""
        try:
            # Extract project information
            project_name = waldur_project.get("name", f"Project {waldur_project['uuid']}")
            project_uuid = waldur_project["uuid"]

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
                "grant_number": f"{self.project_prefix}{project_uuid}",  # Store Waldur UUID
                "max_storage": self.default_storage_limit,
                "ai_included": False,
            }

            result = self.client.create_project(project_data)
            logger.info(
                "Created MUP project %s for Waldur project %s", result.get("id"), project_uuid
            )

            # Cache the result
            self._project_cache[project_uuid] = result
            return result  # noqa: TRY300

        except MUPError:
            logger.exception("Failed to create MUP project for %s", waldur_project["uuid"])
            return None

    def _get_pi_email_from_context(self, user_context: Optional[Dict], project_uuid: str) -> str:
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
            user_uuid = user.get("uuid")
            if user_uuid in offering_user_mappings:
                # This user has an offering username, use their email
                email = user.get("email")
                if email and "@" in email and not email.endswith(".example.com"):
                    logger.info("Using team member %s as PI for project %s", email, project_uuid)
                    return email

        # Fallback to first team member email
        for user in team:
            email = user.get("email")
            if email and "@" in email and not email.endswith(".example.com"):
                logger.info("Using first team member %s as PI for project %s", email, project_uuid)
                return email

        # Final fallback
        logger.warning(
            "No suitable PI email found in user context, using fallback for project %s",
            project_uuid,
        )
        return f"admin@{project_uuid}.example.com"

    def _create_and_add_users_from_context(self, project_id: int, user_context: Dict) -> None:
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
                user_data = {
                    "username": offering_user.get("username"),
                    "email": team_user.get("email"),
                    "first_name": team_user.get("first_name", ""),
                    "last_name": team_user.get("last_name", ""),
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

    def create_resource(
        self, waldur_resource: Dict, user_context: Optional[Dict] = None
    ) -> Resource:
        """Create resource on MUP backend - creates project and allocation.

        Args:
            waldur_resource: Waldur resource data from marketplace order
            user_context: User context with team members and offering users

        Returns:
            Resource object with backend metadata
        """
        try:
            logger.info("Creating MUP allocation for resource %s", waldur_resource["uuid"])

            # Get project information
            project_uuid = waldur_resource.get("project_uuid")
            project_name = waldur_resource.get("project_name", f"Project {project_uuid}")

            if not project_uuid:
                msg = "No project UUID found in resource data"
                raise BackendError(msg)  # noqa: TRY301

            # Get or create MUP project
            mup_project = self._get_project_by_waldur_id(project_uuid)
            if not mup_project:
                # Get PI email from user context or use a fallback
                pi_email = self._get_pi_email_from_context(user_context, project_uuid)

                project_data = {
                    "uuid": project_uuid,
                    "name": project_name,
                    "description": f"Waldur project {project_name}",
                }
                mup_project = self._create_mup_project(project_data, pi_email)
                if not mup_project:
                    msg = "Failed to create MUP project"
                    raise BackendError(msg)  # noqa: TRY301

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

            # Create allocation based on resource limits/plan
            limits = waldur_resource.get("limits", {})

            # Extract CPU cores (assume this is the main allocation metric)
            cpu_cores = limits.get("cpu", 1)
            if isinstance(cpu_cores, dict):
                cpu_cores = cpu_cores.get("value", 1)

            allocation_data = {
                "type": self.default_allocation_type,
                "identifier": f"{self.allocation_prefix}{waldur_resource['uuid']}",
                "size": int(cpu_cores),
                "used": 0,
                "active": True,
                "project": mup_project["id"],
            }

            result = self.client.create_allocation(mup_project["id"], allocation_data)

            logger.info(
                "Created MUP allocation %s for resource %s", result["id"], waldur_resource["uuid"]
            )

            # Return Resource object with backend metadata
            return Resource(
                backend_type=self.backend_type,
                name=waldur_resource["name"],
                marketplace_uuid=waldur_resource["uuid"],
                backend_id=f"{self.allocation_prefix}{waldur_resource['uuid']}",
                limits={"cpu": int(cpu_cores)},
                users=[],
                usage={},
            )

        except Exception as e:
            logger.exception("Failed to create allocation")
            raise BackendError(f"Failed to create allocation: {e}") from e

    def _collect_limits(
        self, waldur_resource: Dict[str, Dict]
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Collect MUP and Waldur limits separately."""
        allocation_limits = {}
        waldur_resource_limits = {}

        # Extract limits from resource
        limits = waldur_resource.get("limits", {})

        for component_key, data in self.backend_components.items():
            if component_key in limits:
                limit_value = limits[component_key]
                if isinstance(limit_value, dict):
                    limit_value = limit_value.get("value", 0)

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

    def set_resource_limits(self, resource_backend_id: str, limits: Dict[str, int]) -> None:
        """Set limits for components in the MUP allocation."""
        logger.info("Setting resource limits for %s: %s", resource_backend_id, limits)

        # Convert limits with unit factors
        converted_limits = {
            key: value * self.backend_components[key]["unit_factor"]
            for key, value in limits.items()
            if key in self.backend_components
        }

        # Find the project by allocation identifier
        try:
            projects = self.client.get_projects()
            target_project = None

            for project in projects:
                allocations = self.client.get_project_allocations(project["id"])
                for allocation in allocations:
                    if allocation.get("identifier") == resource_backend_id:
                        target_project = project
                        target_allocation = allocation
                        break
                if target_project:
                    break

            if not target_project:
                logger.error("No MUP project found for resource %s", resource_backend_id)
                return

            # Update allocation size based on CPU limit (primary resource for MUP)
            if "cpu" in converted_limits:
                new_size = converted_limits["cpu"]
                allocation_data = {
                    "type": target_allocation["type"],
                    "identifier": target_allocation["identifier"],
                    "size": new_size,
                    "used": target_allocation.get("used", 0),
                    "active": target_allocation.get("active", True),
                    "project": target_project["id"],
                }

                self.client.update_allocation(
                    target_project["id"], target_allocation["id"], allocation_data
                )
                logger.info(
                    "Updated MUP allocation %s size to %s", target_allocation["id"], new_size
                )

        except Exception as e:
            logger.exception("Failed to update resource limits for %s", resource_backend_id)
            raise BackendError(f"Failed to update resource limits: {e}") from e

    def get_resource_metadata(self, account: str) -> dict:
        """Get backend-specific resource metadata."""
        # Find project and allocation by account name
        projects = self.client.get_projects()
        for project in projects:
            if project.get("grant_number") == account:
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
        return {}

    def _get_usage_report(self, accounts: List[str]) -> Dict[str, Dict[str, Dict[str, int]]]:
        """Collect usage report for the specified accounts from MUP."""
        report: Dict[str, Dict[str, Dict[str, int]]] = {}

        try:
            projects = self.client.get_projects()

            for project in projects:
                grant_number = project.get("grant_number")
                if grant_number in accounts:
                    allocations = self.client.get_project_allocations(project["id"])

                    # Initialize account usage
                    report[grant_number] = {}
                    total_usage: Dict[str, int] = {}

                    for allocation in allocations:
                        # Map allocation usage to component usage
                        allocation_type = allocation.get("type", "compute")
                        used = allocation.get("used", 0)

                        # Map to CPU usage for now (could be extended for other types)
                        if allocation_type == "compute":
                            total_usage["cpu"] = total_usage.get("cpu", 0) + used

                    # Set total account usage
                    report[grant_number]["TOTAL_ACCOUNT_USAGE"] = total_usage

                    # Get project members and create per-user usage (placeholder)
                    members = self.client.get_project_members(project["id"])
                    for member in members:
                        if member.get("active", False):
                            username = member.get("member", {}).get("username", "")
                            if username:
                                # For now, assign equal share of usage to active users
                                user_count = len([m for m in members if m.get("active", False)])
                                user_usage = {
                                    "cpu": total_usage.get("cpu", 0) // max(user_count, 1)
                                }
                                report[grant_number][username] = user_usage

        except Exception:
            logger.exception("Failed to get usage report")

        return report

    def add_users_to_resource(
        self, resource_backend_id: str, user_ids: Set[str], **_kwargs: dict
    ) -> Set[str]:
        """Add specified users to the MUP project.

        Note: Most users are already added during resource creation. This method
        handles additional users that may be added later.
        """
        logger.info(
            "Adding users to MUP project for resource %s: %s",
            resource_backend_id,
            ", ".join(user_ids),
        )
        added_users: Set[str] = set()

        # Find the project by allocation identifier
        projects = self.client.get_projects()
        target_project = None

        for project in projects:
            allocations = self.client.get_project_allocations(project["id"])
            for allocation in allocations:
                if allocation.get("identifier") == resource_backend_id:
                    target_project = project
                    break
            if target_project:
                break

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

    def remove_users_from_account(self, resource_backend_id: str, usernames: Set[str]) -> List[str]:
        """Remove specified users from the MUP project."""
        logger.info(
            "Removing users from MUP project for resource %s: %s",
            resource_backend_id,
            ", ".join(usernames),
        )
        removed_users: List[str] = []

        # Find the project by allocation identifier
        projects = self.client.get_projects()
        target_project = None

        for project in projects:
            allocations = self.client.get_project_allocations(project["id"])
            for allocation in allocations:
                if allocation.get("identifier") == resource_backend_id:
                    target_project = project
                    break
            if target_project:
                break

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
