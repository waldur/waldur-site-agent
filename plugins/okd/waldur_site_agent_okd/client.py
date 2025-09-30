"""OKD/OpenShift client for interacting with the cluster API."""

import ssl
from typing import Any, Optional

import requests
from kubernetes import client as k8s_client
from requests.adapters import HTTPAdapter
from waldur_site_agent_okd.token_manager import TokenRefreshMixin

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Association, ClientResource


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


class OkdClient(TokenRefreshMixin, BaseClient):
    """OKD/OpenShift client for managing projects and resources."""

    def __init__(self, okd_components: dict[str, dict], okd_settings: dict) -> None:
        """Initialize OKD client with connection settings."""
        self.okd_components = okd_components
        self.okd_settings = okd_settings

        # OKD API configuration
        self.api_url = okd_settings.get("api_url", "https://localhost:8443")
        self.token = okd_settings.get("token", "")
        self.verify_cert = okd_settings.get("verify_cert", True)
        self.namespace_prefix = okd_settings.get("namespace_prefix", "waldur-")

        # Initialize TokenRefreshMixin
        super().__init__()

        # Initialize session with custom SSL adapter
        self.session = requests.Session()
        adapter = SSLAdapter(verify_cert=self.verify_cert)
        self.session.mount("https://", adapter)
        self.session.verify = self.verify_cert  # Also set verify directly on session
        # Initialize with basic headers - auth headers will be managed by TokenRefreshMixin
        self.session.headers.update({"Content-Type": "application/json"})

        # Set up authentication headers using token manager
        if hasattr(self, "_get_auth_headers"):
            auth_headers = self._get_auth_headers()
            self.session.headers.update(auth_headers)

        # Initialize Kubernetes client for advanced operations
        configuration = k8s_client.Configuration()
        configuration.host = self.api_url
        configuration.verify_ssl = self.verify_cert
        configuration.api_key = {"authorization": f"Bearer {self.token}"}
        configuration.api_key_prefix = {"authorization": "Bearer"}

        self.core_v1 = k8s_client.CoreV1Api(k8s_client.ApiClient(configuration))
        self.project_v1 = None  # Will be initialized for project operations

        logger.info(f"Initialized OKD client for {self.api_url}")

    def _make_request(self, method: str, endpoint: str, data: Optional[dict] = None) -> dict:
        """Make HTTP request to OKD API with automatic token refresh."""
        # Use authenticated request if token manager is available
        if hasattr(self, "token_manager") and self.token_manager:
            return self._make_authenticated_request(method, endpoint, data)

        # Fallback to basic request
        return self._make_basic_request(method, endpoint, data)

    def _make_basic_request(self, method: str, endpoint: str, data: Optional[dict] = None) -> dict:
        """Make basic HTTP request to OKD API (fallback method)."""
        url = f"{self.api_url}{endpoint}"

        try:
            if method == "GET":
                response = self.session.get(url)
            elif method == "POST":
                response = self.session.post(url, json=data)
            elif method == "DELETE":
                response = self.session.delete(url)
            elif method == "PUT":
                response = self.session.put(url, json=data)
            elif method == "PATCH":
                self.session.headers["Content-Type"] = "application/strategic-merge-patch+json"
                response = self.session.patch(url, json=data)
                self.session.headers["Content-Type"] = "application/json"
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
            raise BackendError(f"OKD API request failed: {e}") from e

    def ping(self) -> bool:
        """Check OKD cluster connectivity."""
        try:
            # Try to get API resource list
            response = self._make_request("GET", "/api/v1")
            return "kind" in response and response["kind"] == "APIResourceList"
        except Exception as e:
            logger.error(f"Failed to ping OKD cluster: {e}")
            return False

    def list_resources(self) -> list[ClientResource]:
        """List all projects/namespaces managed by this agent."""
        try:
            # Use project.openshift.io API for OpenShift projects
            endpoint = "/apis/project.openshift.io/v1/projects"
            response = self._make_request("GET", endpoint)

            resources = []
            for item in response.get("items", []):
                name = item["metadata"]["name"]
                # Only include projects with our prefix
                if name.startswith(self.namespace_prefix):
                    resource = ClientResource(
                        name=name,
                        organization=item["metadata"]
                        .get("annotations", {})
                        .get("waldur/organization", ""),
                        description=item["metadata"]
                        .get("annotations", {})
                        .get("openshift.io/description", ""),
                    )
                    resources.append(resource)

            return resources

        except Exception as e:
            logger.error(f"Failed to list OKD projects: {e}")
            raise BackendError(f"Failed to list OKD projects: {e}") from e

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Get information about a specific project."""
        try:
            endpoint = f"/apis/project.openshift.io/v1/projects/{resource_id}"
            response = self._make_request("GET", endpoint)

            if response:
                return ClientResource(
                    name=response["metadata"]["name"],
                    organization=response["metadata"]
                    .get("annotations", {})
                    .get("waldur/organization", ""),
                    description=response["metadata"]
                    .get("annotations", {})
                    .get("openshift.io/description", ""),
                )
            return None

        except BackendError:
            return None
        except Exception as e:
            logger.error(f"Failed to get project {resource_id}: {e}")
            return None

    def create_resource(
        self, name: str, description: str, organization: str, parent_name: Optional[str] = None
    ) -> str:
        """Create a new OpenShift project."""
        try:
            # Create ProjectRequest for OpenShift
            project_request: dict[str, Any] = {
                "kind": "ProjectRequest",
                "apiVersion": "project.openshift.io/v1",
                "metadata": {
                    "name": name,
                    "annotations": {
                        "openshift.io/description": description,
                        "openshift.io/display-name": description,
                        "waldur/organization": organization,
                    },
                },
            }

            if parent_name:
                project_request["metadata"]["annotations"]["waldur/parent"] = parent_name

            endpoint = "/apis/project.openshift.io/v1/projectrequests"
            _ = self._make_request("POST", endpoint, project_request)

            logger.info(f"Created OKD project: {name}")
            return name

        except Exception as e:
            logger.error(f"Failed to create project {name}: {e}")
            raise BackendError(f"Failed to create project {name}: {e}") from e

    def delete_resource(self, name: str) -> str:
        """Delete an OpenShift project."""
        try:
            endpoint = f"/apis/project.openshift.io/v1/projects/{name}"
            self._make_request("DELETE", endpoint)

            logger.info(f"Deleted OKD project: {name}")
            return f"Project {name} deleted"

        except Exception as e:
            logger.error(f"Failed to delete project {name}: {e}")
            raise BackendError(f"Failed to delete project {name}: {e}") from e

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set resource quotas for the project."""
        try:
            # Create or update ResourceQuota
            resource_quota: dict[str, Any] = {
                "apiVersion": "v1",
                "kind": "ResourceQuota",
                "metadata": {"name": "waldur-quota", "namespace": resource_id},
                "spec": {"hard": {}},
            }

            # Map component limits to Kubernetes resource quotas
            for component, value in limits_dict.items():
                if component == "cpu":
                    resource_quota["spec"]["hard"]["requests.cpu"] = str(value)
                    resource_quota["spec"]["hard"]["limits.cpu"] = str(value * 2)  # Allow burst
                elif component == "memory":
                    resource_quota["spec"]["hard"]["requests.memory"] = f"{value}Gi"
                    resource_quota["spec"]["hard"]["limits.memory"] = f"{value * 2}Gi"
                elif component == "storage":
                    resource_quota["spec"]["hard"]["requests.storage"] = f"{value}Gi"
                elif component == "pods":
                    resource_quota["spec"]["hard"]["pods"] = str(value)

            # Check if quota exists
            quota_endpoint = f"/api/v1/namespaces/{resource_id}/resourcequotas/waldur-quota"
            try:
                _ = self._make_request("GET", quota_endpoint)
                # Update existing quota
                self._make_request("PUT", quota_endpoint, resource_quota)
                logger.info(f"Updated resource quota for project {resource_id}")
            except Exception:
                # Create new quota
                create_endpoint = f"/api/v1/namespaces/{resource_id}/resourcequotas"
                self._make_request("POST", create_endpoint, resource_quota)
                logger.info(f"Created resource quota for project {resource_id}")

            return "Resource limits set"

        except Exception as e:
            logger.error(f"Failed to set resource limits for {resource_id}: {e}")
            raise BackendError(f"Failed to set resource limits: {e}") from e

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Get resource quotas for the project."""
        try:
            endpoint = f"/api/v1/namespaces/{resource_id}/resourcequotas/waldur-quota"
            response = self._make_request("GET", endpoint)

            limits = {}
            if response and "spec" in response and "hard" in response["spec"]:
                hard = response["spec"]["hard"]

                # Map Kubernetes quotas back to components
                if "requests.cpu" in hard:
                    limits["cpu"] = int(hard["requests.cpu"])
                if "requests.memory" in hard:
                    mem_str = hard["requests.memory"].replace("Gi", "")
                    limits["memory"] = int(mem_str)
                if "requests.storage" in hard:
                    storage_str = hard["requests.storage"].replace("Gi", "")
                    limits["storage"] = int(storage_str)
                if "pods" in hard:
                    limits["pods"] = int(hard["pods"])

            return limits

        except Exception as e:
            logger.debug(f"No resource quota found for {resource_id}: {e}")
            return {}

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:  # noqa: ARG002
        """Get per-user limits (not applicable for OKD projects)."""
        # OKD doesn't have per-user limits within a project
        return {}

    def set_resource_user_limits(
        self,
        resource_id: str,  # noqa: ARG002
        username: str,
        limits_dict: dict[str, int],  # noqa: ARG002
    ) -> str:
        """Set per-user limits (not applicable for OKD projects)."""
        # OKD doesn't support per-user limits within a project
        logger.info(f"Per-user limits not supported in OKD, ignoring for {username}")
        return "Per-user limits not supported"

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Get user's role binding in the project."""
        try:
            # Check RoleBindings in the namespace
            endpoint = f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_id}/rolebindings"
            response = self._make_request("GET", endpoint)

            for binding in response.get("items", []):
                for subject in binding.get("subjects", []):
                    if subject.get("kind") == "User" and subject.get("name") == user:
                        return Association(
                            account=resource_id,
                            user=user,
                            value=1,  # Dummy value, not used in OKD
                        )

            return None

        except Exception as e:
            logger.debug(f"No association found for {user} in {resource_id}: {e}")
            return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Grant user access to the project."""
        try:
            # Determine role based on default_account or settings
            role = default_account or self.okd_settings.get("default_role", "edit")

            # Create RoleBinding
            role_binding = {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": f"waldur-{username}", "namespace": resource_id},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": role,  # Can be 'admin', 'edit', or 'view'
                },
                "subjects": [
                    {"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": username}
                ],
            }

            endpoint = f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_id}/rolebindings"
            self._make_request("POST", endpoint, role_binding)

            logger.info(
                f"Created association for {username} in project {resource_id} with role {role}"
            )
            return f"User {username} added to project"

        except Exception as e:
            logger.error(f"Failed to create association for {username} in {resource_id}: {e}")
            raise BackendError(f"Failed to create association: {e}") from e

    def delete_association(self, username: str, resource_id: str) -> str:
        """Remove user access from the project."""
        try:
            endpoint = (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_id}"
                f"/rolebindings/waldur-{username}"
            )
            self._make_request("DELETE", endpoint)

            logger.info(f"Removed association for {username} from project {resource_id}")
            return f"User {username} removed from project"

        except Exception as e:
            logger.error(f"Failed to remove association for {username} from {resource_id}: {e}")
            raise BackendError(f"Failed to remove association: {e}") from e

    def get_usage_report(self, resource_ids: list[str]) -> list:
        """Get resource usage metrics for projects."""
        usage_reports = []

        for resource_id in resource_ids:
            try:
                # Get current usage from ResourceQuota status
                quota_endpoint = f"/api/v1/namespaces/{resource_id}/resourcequotas/waldur-quota"
                quota_response = self._make_request("GET", quota_endpoint)

                usage = {}
                if (
                    quota_response
                    and "status" in quota_response
                    and "used" in quota_response["status"]
                ):
                    used = quota_response["status"]["used"]

                    if "requests.cpu" in used:
                        # Convert CPU from millicores to cores
                        cpu_str = used["requests.cpu"]
                        if cpu_str.endswith("m"):
                            usage["cpu"] = int(cpu_str[:-1]) / 1000
                        else:
                            usage["cpu"] = float(cpu_str)

                    if "requests.memory" in used:
                        # Convert memory to GB
                        mem_str = used["requests.memory"]
                        if mem_str.endswith("Gi"):
                            usage["memory"] = float(mem_str[:-2])
                        elif mem_str.endswith("Mi"):
                            usage["memory"] = float(mem_str[:-2]) / 1024
                        else:
                            usage["memory"] = float(mem_str) / (1024**3)

                    if "requests.storage" in used:
                        # Convert storage to GB
                        storage_str = used["requests.storage"]
                        if storage_str.endswith("Gi"):
                            usage["storage"] = float(storage_str[:-2])
                        elif storage_str.endswith("Mi"):
                            usage["storage"] = float(storage_str[:-2]) / 1024
                        else:
                            usage["storage"] = float(storage_str) / (1024**3)

                    if "pods" in used:
                        usage["pods"] = int(used["pods"])

                usage_reports.append({"resource_id": resource_id, "usage": usage})

            except Exception as e:
                logger.warning(f"Failed to get usage for project {resource_id}: {e}")
                usage_reports.append({"resource_id": resource_id, "usage": {}})

        return usage_reports

    def list_resource_users(self, resource_id: str) -> list[str]:
        """List users with access to the project."""
        users = []

        try:
            # Get all RoleBindings in the namespace
            endpoint = f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_id}/rolebindings"
            response = self._make_request("GET", endpoint)

            for binding in response.get("items", []):
                for subject in binding.get("subjects", []):
                    if subject.get("kind") == "User":
                        username = subject.get("name")
                        if username and username not in users:
                            users.append(username)

            return users

        except Exception as e:
            logger.warning(f"Failed to list users for project {resource_id}: {e}")
            return []

    def create_linux_user_homedir(self, username: str, umask: str = "") -> str:  # noqa: ARG002
        """Not applicable for OKD - users are managed externally."""
        logger.debug(f"Homedir creation not applicable for OKD user {username}")
        return "Not applicable for OKD"
