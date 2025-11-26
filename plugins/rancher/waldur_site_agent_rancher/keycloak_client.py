"""Keycloak client for managing user groups and memberships."""

from typing import Optional

from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from keycloak.exceptions import KeycloakError

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError


class KeycloakClient:
    """Keycloak client for managing groups and user memberships."""

    def __init__(self, keycloak_settings: dict) -> None:
        """Initialize Keycloak client with connection settings."""
        self.keycloak_settings = keycloak_settings

        # Keycloak connection configuration (matching waldur-mastermind format)
        self.server_url = keycloak_settings.get("keycloak_url", "https://localhost/auth/")
        self.realm_name = keycloak_settings.get("keycloak_realm", "waldur")
        self.user_realm = keycloak_settings.get("keycloak_user_realm", "master")
        self.client_id = keycloak_settings.get("client_id", "admin-cli")
        self.username = keycloak_settings.get("keycloak_username", "")
        self.password = keycloak_settings.get("keycloak_password", "")
        self.verify_cert = keycloak_settings.get("keycloak_ssl_verify", True)

        # Initialize Keycloak admin connection
        try:
            keycloak_connection = KeycloakOpenIDConnection(
                server_url=self.server_url,
                username=self.username,
                password=self.password,
                realm_name=self.realm_name,
                client_id=self.client_id,
                verify=self.verify_cert,
            )

            self.keycloak_admin = KeycloakAdmin(connection=keycloak_connection)
            logger.info(f"Initialized Keycloak client for realm: {self.realm_name}")

        except Exception as e:
            logger.error(f"Failed to initialize Keycloak client: {e}")
            raise BackendError(f"Failed to initialize Keycloak client: {e}") from e

    def ping(self) -> bool:
        """Check Keycloak connectivity."""
        try:
            # Test connectivity by getting realm info
            realm_info = self.keycloak_admin.get_realm(self.realm_name)
            return realm_info is not None and "realm" in realm_info
        except Exception as e:
            logger.error(f"Failed to ping Keycloak: {e}")
            return False

    def find_user_by_username(self, username: str) -> Optional[dict]:
        """Find user by username."""
        try:
            users = self.keycloak_admin.get_users({"username": username, "exact": True})
            return users[0] if users else None
        except KeycloakError as e:
            logger.warning(f"Failed to find user {username}: {e}")
            return None

    def find_user_by_id(self, user_id: str) -> Optional[dict]:
        """Find user by ID."""
        try:
            # In Keycloak, the user ID might be stored as username
            # First try to get user by ID directly
            user = self.keycloak_admin.get_user(user_id)
            if user:
                return user
        except KeycloakError:
            pass

        try:
            # If not found by ID, try searching by username (in case ID is stored as username)
            users = self.keycloak_admin.get_users({"username": user_id, "exact": True})
            return users[0] if users else None
        except KeycloakError as e:
            logger.warning(f"Failed to find user {user_id}: {e}")
            return None

    def find_user(self, user_identifier: str, use_id: bool = True) -> Optional[dict]:
        """Find user by ID or username based on configuration."""
        if use_id:
            return self.find_user_by_id(user_identifier)
        return self.find_user_by_username(user_identifier)

    def create_group(
        self, group_name: str, description: str = "", parent_id: Optional[str] = None
    ) -> str:
        """Create a new group."""
        try:
            group_data = {
                "name": group_name,
                "attributes": {
                    "description": [description] if description else [],
                    "managed_by": ["waldur-site-agent"],
                },
            }

            if parent_id:
                # Create as subgroup
                group_id = self.keycloak_admin.create_group(group_data, parent=parent_id)
            else:
                # Create as top-level group
                group_id = self.keycloak_admin.create_group(group_data)

            logger.info(f"Created Keycloak group: {group_name} (ID: {group_id})")
            return group_id

        except KeycloakError as e:
            logger.error(f"Failed to create group {group_name}: {e}")
            raise BackendError(f"Failed to create group {group_name}: {e}") from e

    def get_group_by_name(self, group_name: str) -> Optional[dict]:
        """Get group by name."""
        try:
            groups = self.keycloak_admin.get_groups()
            for group in groups:
                if group.get("name") == group_name:
                    return group
                # Also check subgroups
                subgroups = group.get("subGroups", [])
                for subgroup in subgroups:
                    if subgroup.get("name") == group_name:
                        return subgroup
            return None
        except KeycloakError as e:
            logger.warning(f"Failed to find group {group_name}: {e}")
            return None

    def get_group_by_id(self, group_id: str) -> Optional[dict]:
        """Get group by ID."""
        try:
            return self.keycloak_admin.get_group(group_id)
        except KeycloakError as e:
            logger.warning(f"Failed to get group {group_id}: {e}")
            return None

    def delete_group(self, group_id: str) -> None:
        """Delete a group."""
        try:
            self.keycloak_admin.delete_group(group_id)
            logger.info(f"Deleted Keycloak group: {group_id}")
        except KeycloakError as e:
            logger.error(f"Failed to delete group {group_id}: {e}")
            raise BackendError(f"Failed to delete group {group_id}: {e}") from e

    def add_user_to_group(self, user_id: str, group_id: str) -> None:
        """Add user to group."""
        try:
            self.keycloak_admin.group_user_add(user_id, group_id)
            logger.info(f"Added user {user_id} to group {group_id}")
        except KeycloakError as e:
            logger.error(f"Failed to add user {user_id} to group {group_id}: {e}")
            raise BackendError(f"Failed to add user to group: {e}") from e

    def remove_user_from_group(self, user_id: str, group_id: str) -> None:
        """Remove user from group."""
        try:
            self.keycloak_admin.group_user_remove(user_id, group_id)
            logger.info(f"Removed user {user_id} from group {group_id}")
        except KeycloakError as e:
            logger.error(f"Failed to remove user {user_id} from group {group_id}: {e}")
            raise BackendError(f"Failed to remove user from group: {e}") from e

    def get_group_members(self, group_id: str) -> list[dict]:
        """Get all members of a group."""
        try:
            return self.keycloak_admin.get_group_members(group_id)
        except KeycloakError as e:
            logger.warning(f"Failed to get members for group {group_id}: {e}")
            return []

    def get_user_groups(self, user_id: str) -> list[dict]:
        """Get all groups a user belongs to."""
        try:
            return self.keycloak_admin.get_user_groups(user_id)
        except KeycloakError as e:
            logger.warning(f"Failed to get groups for user {user_id}: {e}")
            return []

    def is_user_in_group(self, user_id: str, group_id: str) -> bool:
        """Check if user is a member of group."""
        try:
            user_groups = self.get_user_groups(user_id)
            return any(group.get("id") == group_id for group in user_groups)
        except Exception as e:
            logger.warning(f"Failed to check group membership for user {user_id}: {e}")
            return False

    def create_project_groups(self, project_slug: str, description: str = "") -> tuple[str, str]:
        """Create parent and child groups for a project.

        Returns:
            Tuple of (parent_group_id, child_group_id)
        """
        try:
            # Create parent group for the project
            parent_group_name = f"project-{project_slug}"
            parent_description = f"Parent group for project {project_slug}"

            # Check if parent group already exists
            parent_group = self.get_group_by_name(parent_group_name)
            if parent_group:
                parent_group_id = parent_group["id"]
                logger.info(f"Using existing parent group: {parent_group_name}")
            else:
                parent_group_id = self.create_group(parent_group_name, parent_description)

            # Create child group for project members
            child_group_name = project_slug  # Use project slug as group name
            child_description = description or f"Members of project {project_slug}"

            # Check if child group already exists
            child_group = self.get_group_by_name(child_group_name)
            if child_group:
                child_group_id = child_group["id"]
                logger.info(f"Using existing child group: {child_group_name}")
            else:
                child_group_id = self.create_group(
                    child_group_name, child_description, parent_group_id
                )

            return parent_group_id, child_group_id

        except Exception as e:
            logger.error(f"Failed to create project groups for {project_slug}: {e}")
            raise BackendError(f"Failed to create project groups: {e}") from e

    def delete_project_groups(self, project_slug: str) -> None:
        """Delete both parent and child groups for a project."""
        try:
            # Delete child group (project members)
            child_group = self.get_group_by_name(project_slug)
            if child_group:
                self.delete_group(child_group["id"])

            # Delete parent group
            parent_group_name = f"project-{project_slug}"
            parent_group = self.get_group_by_name(parent_group_name)
            if parent_group:
                self.delete_group(parent_group["id"])

            logger.info(f"Deleted project groups for: {project_slug}")

        except Exception as e:
            logger.error(f"Failed to delete project groups for {project_slug}: {e}")
            raise BackendError(f"Failed to delete project groups: {e}") from e

    def ensure_project_group_exists(self, project_slug: str, description: str = "") -> str:
        """Ensure project group exists and return the group ID.

        This method creates the group structure if it doesn't exist,
        or returns the existing child group ID.
        """
        try:
            # Check if child group exists
            child_group = self.get_group_by_name(project_slug)
            if child_group:
                return child_group["id"]

            # Create the group structure
            _, child_group_id = self.create_project_groups(project_slug, description)
            return child_group_id

        except Exception as e:
            logger.error(f"Failed to ensure project group exists for {project_slug}: {e}")
            raise BackendError(f"Failed to ensure project group exists: {e}") from e
