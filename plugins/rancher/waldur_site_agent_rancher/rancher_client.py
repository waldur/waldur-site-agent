"""Rancher client for managing projects and resources."""

import ssl
from typing import Any, Optional

import requests
import yaml
from requests.adapters import HTTPAdapter

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import ClientResource


class SSLAdapter(HTTPAdapter):
    """HTTPAdapter with custom SSL configuration."""

    def __init__(self, verify_cert: bool = True, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize SSL adapter."""
        self.verify_cert = verify_cert
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Initialize pool manager with SSL context."""
        ctx = ssl.create_default_context()
        if not self.verify_cert:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


class RancherClient(BaseClient):
    """Rancher client for managing projects and resources."""

    def __init__(self, rancher_settings: dict) -> None:
        """Initialize Rancher client with connection settings."""
        super().__init__()
        self.rancher_settings = rancher_settings

        # Rancher API configuration (matching waldur-mastermind format)
        backend_url = rancher_settings.get("backend_url", "https://localhost")
        self.api_url = f"{backend_url}/v3"  # Add /v3 to backend_url
        self.access_key = rancher_settings.get(
            "username", ""
        )  # username = access_key in waldur-mastermind
        self.secret_key = rancher_settings.get(
            "password", ""
        )  # password = secret_key in waldur-mastermind
        self.verify_cert = rancher_settings.get("verify_cert", True)
        self.cluster_id = rancher_settings.get("cluster_id", "")
        self.project_prefix = rancher_settings.get("project_prefix", "waldur-")

        # Initialize session with custom SSL adapter
        self.session = requests.Session()
        adapter = SSLAdapter(verify_cert=self.verify_cert)
        self.session.mount("https://", adapter)
        self.session.verify = self.verify_cert
        self.session.headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )

        # Perform login similar to waldur-mastermind
        if self.access_key and self.secret_key:
            try:
                self.login(self.access_key, self.secret_key)
                logger.info(f"Successfully logged in to Rancher at {self.api_url}")
            except Exception as e:
                logger.warning(f"Login failed, continuing without authentication: {e}")

        logger.info(f"Initialized Rancher client for {self.api_url}")

    def login(self, access_key: str, secret_key: str) -> None:
        """Login to Rancher server using access_key and secret_key.

        This follows the same pattern as waldur-mastermind RancherClient.
        """
        try:
            # First make a POST request to authenticate (like waldur-mastermind)
            response = self.session.post(self.api_url, auth=(access_key, secret_key))
            response.raise_for_status()

            # Then set the session auth for subsequent requests
            self.session.auth = (access_key, secret_key)
            logger.debug(f"Successfully logged in as {access_key}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Login failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise BackendError(f"Rancher login failed: {e}") from e

    def _make_request(self, method: str, endpoint: str, data: Optional[dict] = None) -> dict:
        """Make HTTP request to Rancher API."""
        # Ensure endpoint doesn't start with / to avoid urljoin issues
        endpoint = endpoint.lstrip("/")
        url = f"{self.api_url}/{endpoint}"

        try:
            if method == "GET":
                response = self.session.get(url)
            elif method == "POST":
                response = self.session.post(url, json=data)
            elif method == "DELETE":
                response = self.session.delete(url)
            elif method == "PUT":
                response = self.session.put(url, json=data)
            else:
                raise BackendError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()

            if response.content:
                return response.json()
            return {}

        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {url} failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise BackendError(f"Rancher API request failed: {e}") from e

    def ping(self) -> bool:
        """Check Rancher connectivity."""
        try:
            # Test connectivity by getting cluster info
            response = self._make_request("GET", f"clusters/{self.cluster_id}")
            return "id" in response and response["id"] == self.cluster_id
        except Exception as e:
            logger.error(f"Failed to ping Rancher: {e}")
            return False

    def list_projects(self) -> list[ClientResource]:
        """List all projects in the cluster managed by this agent."""
        try:
            endpoint = f"projects?clusterId={self.cluster_id}"
            response = self._make_request("GET", endpoint)

            projects = []
            for item in response.get("data", []):
                name = item.get("name", "")
                # Only include projects with our prefix
                if name.startswith(self.project_prefix):
                    project = ClientResource(
                        name=name,
                        organization=item.get("annotations", {}).get("waldur/organization", ""),
                        description=item.get("description", ""),
                        backend_id=item.get("id", ""),
                    )
                    projects.append(project)

            return projects

        except Exception as e:
            logger.error(f"Failed to list Rancher projects: {e}")
            raise BackendError(f"Failed to list Rancher projects: {e}") from e

    def get_project(self, project_id: str) -> Optional[ClientResource]:
        """Get information about a specific project."""
        try:
            endpoint = f"projects/{project_id}"
            response = self._make_request("GET", endpoint)

            if response:
                return ClientResource(
                    name=response.get("name", ""),
                    organization=response.get("annotations", {}).get("waldur/organization", ""),
                    description=response.get("description", ""),
                    backend_id=response.get("id", ""),
                )
            return None

        except Exception as e:
            logger.debug(f"Failed to get project {project_id}: {e}")
            return None

    def create_project(
        self, name: str, description: str, organization: str, project_slug: str = ""
    ) -> str:
        """Create a new Rancher project."""
        try:
            project_data = {
                "type": "project",
                "clusterId": self.cluster_id,
                "name": name,
                "description": description,
                "annotations": {
                    "waldur/organization": organization,
                    "waldur/managed": "true",
                    "waldur/project_slug": project_slug or "unknown",
                },
            }

            response = self._make_request("POST", "projects", project_data)
            project_id = response.get("id", "")

            logger.info(f"Created Rancher project: {name} (ID: {project_id})")
            return project_id

        except Exception as e:
            logger.error(f"Failed to create project {name}: {e}")
            raise BackendError(f"Failed to create project {name}: {e}") from e

    def delete_project(self, project_id: str) -> None:
        """Delete a Rancher project."""
        try:
            endpoint = f"projects/{project_id}"
            self._make_request("DELETE", endpoint)

            logger.info(f"Deleted Rancher project: {project_id}")

        except Exception as e:
            logger.error(f"Failed to delete project {project_id}: {e}")
            raise BackendError(f"Failed to delete project {project_id}: {e}") from e

    def create_namespace(self, project_id: str, namespace: str) -> None:
        """Create a namespace in a Rancher project with optional resource quotas.

        Args:
            project_id: The ID of the project to create the namespace in
            namespace: The name of the namespace to create
            quotas: Optional dictionary of resource quotas (e.g., {"cpu": 4.0, "memory": 8.0})

        Raises:
            BackendError: If namespace creation fails
        """
        try:
            logger.info("Creating namespace '%s' in project %s", namespace, project_id)

            # Extract cluster ID from project ID (format: "c-xxx:p-yyy")
            cluster_id = project_id.split(":")[0] if ":" in project_id else self.cluster_id

            namespace_data = {
                "type": "namespace",
                "name": namespace,
                "projectId": project_id,
                "annotations": {
                    "waldur/managed": "true",
                },
            }

            response = self._make_request(
                "POST", f"clusters/{cluster_id}/namespaces", namespace_data
            )
            namespace_id = response.get("id", "")

            logger.info(
                "Created namespace '%s' in project %s (ID: %s)", namespace, project_id, namespace_id
            )

        except Exception as e:
            logger.error(f"Failed to create namespace '{namespace}' in project {project_id}: {e}")
            raise BackendError(
                f"Failed to create namespace '{namespace}' in project {project_id}: {e}"
            ) from e

    def get_project_namespaces(self, project_id: str) -> list[str]:
        """List namespaces in the Rancher project."""
        try:
            endpoint = f"clusters/{self.cluster_id}/namespaces?projectId={project_id}"
            response = self._make_request("GET", endpoint)

            namespaces = []
            for item in response.get("data", []):
                namespace_name = item.get("name", "")
                namespaces.append(namespace_name)

            return namespaces

        except Exception as e:
            logger.error("Failed to list namespaces for project %s: %s", project_id, e)
            raise BackendError(f"Failed to list namespaces for project {project_id}: {e}") from e

    def get_project_quotas(self, project_id: str) -> dict[str, float]:
        """Get resource quotas for the project."""
        try:
            # Get the project object which contains quota information
            endpoint = f"projects/{project_id}"
            response = self._make_request("GET", endpoint)

            quotas = {}
            if response:
                # Check resourceQuota field
                resource_quota = response.get("resourceQuota", {})
                limits = resource_quota.get("limit", {})

                # Parse CPU limit (e.g., "2000m" -> 2)
                if "limitsCpu" in limits:
                    cpu_str = limits["limitsCpu"]
                    if cpu_str.endswith("m"):
                        quotas["cpu"] = int(cpu_str[:-1]) / 1000
                    else:
                        quotas["cpu"] = int(cpu_str)

                # Parse memory limit (e.g., "2000Mi" -> 2)
                if "limitsMemory" in limits:
                    memory_str = limits["limitsMemory"]
                    if memory_str.endswith("Mi"):
                        quotas["memory"] = int(memory_str[:-2]) / 1024
                    elif memory_str.endswith("Gi"):
                        quotas["memory"] = int(memory_str[:-2])
                    else:
                        quotas["memory"] = int(memory_str)

            return quotas

        except Exception as e:
            logger.debug(f"No quotas found for project {project_id}: {e}")
            return {}

    def get_project_usage(self, project_id: str) -> dict[str, float]:
        """Get actual allocated resources for the project (total allocated CPU, memory, storage)."""
        try:
            # Get all workloads in the project to calculate total allocated resources
            workloads_endpoint = f"projects/{project_id}/workloads"
            workloads_response = self._make_request("GET", workloads_endpoint)

            total_usage = {"cpu": 0.0, "memory": 0.0, "storage": 0.0, "pods": 0}

            for workload in workloads_response.get("data", []):
                containers = workload.get("containers", [])

                for container in containers:
                    resources = container.get("resources", {})
                    requests = resources.get("requests", {})

                    # Sum up CPU requests (allocated CPU)
                    if "cpu" in requests:
                        cpu_str = requests["cpu"]
                        if cpu_str.endswith("m"):
                            total_usage["cpu"] += int(cpu_str[:-1]) / 1000
                        else:
                            total_usage["cpu"] += float(cpu_str)

                    # Sum up memory requests (allocated memory)
                    if "memory" in requests:
                        memory_str = requests["memory"]
                        if memory_str.endswith("Gi"):
                            total_usage["memory"] += float(memory_str[:-2])
                        elif memory_str.endswith("Mi"):
                            total_usage["memory"] += float(memory_str[:-2]) / 1024
                        elif memory_str.endswith("Ki"):
                            total_usage["memory"] += float(memory_str[:-2]) / (1024 * 1024)

                # Count pods (replicas)
                replicas = workload.get("scale", 1)
                total_usage["pods"] += replicas

            # Get storage usage from persistent volume claims
            try:
                pvc_endpoint = f"projects/{project_id}/persistentvolumeclaims"
                pvc_response = self._make_request("GET", pvc_endpoint)

                for pvc in pvc_response.get("data", []):
                    spec = pvc.get("spec", {})
                    resources = spec.get("resources", {})
                    requests = resources.get("requests", {})
                    if "storage" in requests:
                        storage_str = requests["storage"]
                        if storage_str.endswith("Gi"):
                            total_usage["storage"] += float(storage_str[:-2])
                        elif storage_str.endswith("Mi"):
                            total_usage["storage"] += float(storage_str[:-2]) / 1024

            except Exception as e:
                logger.debug(f"Could not get storage usage for {project_id}: {e}")

            return total_usage

        except Exception as e:
            logger.warning(f"Failed to get allocated resources for project {project_id}: {e}")
            # Return zero usage if we can't get metrics
            return {"cpu": 0.0, "memory": 0.0, "storage": 0.0, "pods": 0}

    def list_project_users(self, project_id: str) -> list[str]:
        """List users with access to the project."""
        try:
            endpoint = f"projects/{project_id}/projectroletemplatebindings"
            response = self._make_request("GET", endpoint)

            users = []
            for binding in response.get("data", []):
                user_id = binding.get("userId")
                if user_id and user_id not in users:
                    users.append(user_id)

            return users

        except Exception as e:
            logger.warning(f"Failed to list users for project {project_id}: {e}")
            return []

    def remove_project_user(self, project_id: str, user_id: str) -> None:
        """Remove user from project."""
        try:
            # Find the binding for this user
            endpoint = f"projects/{project_id}/projectroletemplatebindings"
            response = self._make_request("GET", endpoint)

            for binding in response.get("data", []):
                if binding.get("userId") == user_id:
                    binding_id = binding.get("id")
                    if binding_id:
                        self._make_request("DELETE", f"/projectroletemplatebindings/{binding_id}")
                        logger.info(f"Removed user {user_id} from project {project_id}")
                        return

            logger.warning(f"No binding found for user {user_id} in project {project_id}")

        except Exception as e:
            logger.error(f"Failed to remove user {user_id} from project {project_id}: {e}")
            raise BackendError(f"Failed to remove user from project: {e}") from e

    def get_project_group_role(self, group_id: str, project_id: str, role: str) -> list[dict]:
        """Get project role template bindings for a group."""
        try:
            endpoint = "projectroletemplatebindings"
            params = {"roleTemplateId": role, "projectId": project_id, "groupPrincipalId": group_id}
            response = self._make_request(
                "GET", endpoint + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
            )
            return response.get("data", [])
        except Exception as e:
            logger.warning(f"Failed to get group role binding for {group_id}: {e}")
            return []

    def create_project_group_role(self, group_id: str, project_id: str, role: str) -> None:
        """Create project role template binding for a group."""
        try:
            binding_data = {
                "type": "projectRoleTemplateBinding",
                "roleTemplateId": role,
                "projectId": project_id,
                "groupPrincipalId": group_id,
            }

            self._make_request("POST", "projectroletemplatebindings", binding_data)
            logger.info(f"Created group role binding: {group_id} → {project_id} with role {role}")

        except Exception as e:
            logger.error(f"Failed to create group role binding for {group_id}: {e}")
            raise BackendError(f"Failed to create group role binding: {e}") from e

    def delete_project_group_role(self, group_id: str, project_id: str, role: str) -> None:
        """Delete project role template binding for a group."""
        try:
            # Find the binding
            bindings = self.get_project_group_role(group_id, project_id, role)
            for binding in bindings:
                binding_id = binding.get("id")
                if binding_id:
                    self._make_request("DELETE", f"projectroletemplatebindings/{binding_id}")
                    logger.info(f"Deleted group role binding: {group_id} from {project_id}")
                    return

            logger.warning(f"No group role binding found for {group_id} in {project_id}")

        except Exception as e:
            logger.error(f"Failed to delete group role binding for {group_id}: {e}")
            raise BackendError(f"Failed to delete group role binding: {e}") from e

    # Backend interface methods for membership sync
    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get resource (project) information for membership sync."""
        try:
            project = self.get_project(resource_id)
            if project:
                return project
            return None
        except Exception as e:
            logger.debug(f"Resource {resource_id} not found: {e}")
            return None

    def list_resource_users(self, resource_id: str) -> list[str]:  # noqa: ARG002
        """List users with access to the resource (project) for membership sync."""
        # This method should return users currently in the Keycloak group
        # so membership sync can detect stale users for removal
        # However, we need the parent backend to provide the Keycloak client
        # For now, return empty list - the backend will override this
        return []

    def set_namespace_custom_resource_quotas(
        self, namespace: str, waldur_limits: dict[str, int]
    ) -> None:
        """Set resource quotas for a specific namespace.

        The method excepts a quota dictionary with the Waldur resource limits
        and applies it as resource quotas to the given namespace within the cluster.
        """
        logger.info(
            "Setting resource quota for namespace '%s' in cluster %s: %s",
            namespace,
            self.cluster_id,
            waldur_limits,
        )

        url = f"/clusters/{self.cluster_id}?action=importYaml"
        hard_quotas = {}

        # Map component quotas to Rancher format
        for component, value in waldur_limits.items():
            if component == "cpu":
                # Convert to millicores (e.g., 4 cores -> "4000m")
                hard_quotas["limits.cpu"] = f"{int(value * 1000)}m"
            elif component == "memory":
                # Convert to Mi (e.g., 8 GB -> "8192Mi")
                hard_quotas["limits.memory"] = f"{int(value * 1024)}Mi"
            elif component == "storage":
                # Convert to Gi (e.g., 100 GB -> "100Gi")
                hard_quotas["requests.storage"] = f"{int(value) * 1024}Mi"
            elif component == "gpu":
                hard_quotas["requests.nvidia.com/gpu"] = str(int(value))

        if not hard_quotas:
            logger.info("Custom quotas to set for namespace '%s'. Skipping.", namespace)
            return

        logger.info("Prepared hard quotas for namespace '%s': %s", namespace, hard_quotas)

        quota_manifest = {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {
                "name": "custom-resource-quota",
                "namespace": namespace,
            },
            "spec": {
                "hard": hard_quotas,
            },
        }
        quota_manifest_yaml = yaml.dump(quota_manifest)

        payload = {"yaml": quota_manifest_yaml, "namespace": namespace}

        response_data = self._make_request("POST", url, payload)
        logger.info(
            "Successfully set resource quota for namespace '%s': %s", namespace, response_data
        )
