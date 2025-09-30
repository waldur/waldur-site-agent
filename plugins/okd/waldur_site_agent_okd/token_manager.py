"""Token management for OKD authentication."""

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from waldur_site_agent.backend.exceptions import BackendError

if TYPE_CHECKING:
    import requests

logger = logging.getLogger(__name__)


class TokenManager:
    """Manages OKD authentication tokens with automatic refresh capabilities."""

    def __init__(self, token_config: dict) -> None:
        """Initialize token manager with configuration.

        Args:
            token_config: Configuration dict with token settings
        """
        self.config = token_config
        self.token_type = token_config.get("token_type", "static")
        self.current_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        self.service_account_path: str = ""
        self.token_file_path: Optional[str] = None
        self.oauth_config: dict = {}

        # Initialize based on token type
        if self.token_type == "static":  # noqa: S105
            self.current_token = token_config.get("token")
        elif self.token_type == "service_account":  # noqa: S105
            self.service_account_path = token_config.get(
                "service_account_path", "/var/run/secrets/kubernetes.io/serviceaccount"
            )
        elif self.token_type == "file":  # noqa: S105
            self.token_file_path = token_config.get("token_file_path")
        elif self.token_type == "oauth":  # noqa: S105
            self.oauth_config = token_config.get("oauth_config", {})

        logger.info("Initialized token manager with type: %s", self.token_type)

    def get_token(self) -> str:
        """Get current valid token, refreshing if necessary."""
        try:
            if self._is_token_expired():
                self._refresh_token()

            if not self.current_token:
                msg = "No valid authentication token available"
                raise BackendError(msg)  # noqa: TRY301

            return self.current_token

        except Exception as e:
            logger.exception("Failed to get authentication token: %s", e)  # noqa: TRY401
            raise BackendError(f"Authentication token error: {e}") from e

    def _is_token_expired(self) -> bool:
        """Check if current token is expired or about to expire."""
        if not self.current_token:
            return True

        if self.token_type == "static":  # noqa: S105
            # Static tokens don't expire in our management
            return False

        if self.token_expires_at:
            # Refresh 5 minutes before expiration
            return time.time() + 300 > self.token_expires_at

        # For service account tokens, check periodically
        return False

    def _refresh_token(self) -> None:
        """Refresh the authentication token based on configured method."""
        logger.info("Refreshing authentication token using method: %s", self.token_type)

        if self.token_type == "service_account":  # noqa: S105
            self._refresh_service_account_token()
        elif self.token_type == "file":  # noqa: S105
            self._refresh_file_token()
        elif self.token_type == "oauth":  # noqa: S105
            self._refresh_oauth_token()
        else:
            logger.warning("Token refresh not supported for type: %s", self.token_type)

    def _refresh_service_account_token(self) -> None:
        """Refresh token from service account mount."""
        token_file = Path(self.service_account_path) / "token"

        try:
            if token_file.exists():
                new_token = token_file.read_text().strip()
                if new_token and new_token != self.current_token:
                    self.current_token = new_token
                    logger.info("Successfully refreshed service account token")
                else:
                    logger.debug("Service account token unchanged")
            else:
                raise BackendError(f"Service account token file not found: {token_file}")  # noqa: TRY301

        except Exception as e:
            logger.exception("Failed to refresh service account token: %s", e)  # noqa: TRY401
            raise BackendError(f"Service account token refresh failed: {e}") from e

    def _refresh_file_token(self) -> None:
        """Refresh token from specified file path."""
        if not hasattr(self, "token_file_path") or not self.token_file_path:
            msg = "Token file path not configured"
            raise BackendError(msg)
        token_file = Path(self.token_file_path)

        try:
            if token_file.exists():
                new_token = token_file.read_text().strip()
                if new_token:
                    self.current_token = new_token
                    logger.info("Successfully refreshed token from file")
                else:
                    msg = "Token file is empty"
                    raise BackendError(msg)  # noqa: TRY301
            else:
                raise BackendError(f"Token file not found: {token_file}")  # noqa: TRY301

        except Exception as e:
            logger.exception("Failed to refresh token from file: %s", e)  # noqa: TRY401
            raise BackendError(f"File token refresh failed: {e}") from e

    def _refresh_oauth_token(self) -> None:
        """Refresh token using OAuth flow."""
        # This would implement OAuth token refresh
        # For now, raise not implemented
        msg = "OAuth token refresh not yet implemented"
        raise BackendError(msg)

    def invalidate_token(self) -> None:
        """Invalidate current token to force refresh on next request."""
        logger.info("Invalidating current authentication token")
        self.current_token = None
        self.token_expires_at = None


class TokenRefreshMixin:
    """Mixin to add automatic token refresh to HTTP requests."""

    # Type hints for attributes expected from the mixed-in class
    session: "requests.Session"
    okd_settings: dict
    token: str

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize the token refresh mixin."""
        super().__init__(*args, **kwargs)
        self.token_manager: Optional[TokenManager] = None
        self._setup_token_manager()

    def _make_basic_request(self, method: str, endpoint: str, data: Optional[dict] = None) -> dict:
        """Abstract method - should be implemented by the class using this mixin."""
        msg = "This method should be implemented by the class using this mixin"
        raise NotImplementedError(msg)

    def _setup_token_manager(self) -> None:
        """Setup token manager based on configuration."""
        if hasattr(self, "okd_settings") and self.okd_settings:
            # Extract token configuration
            if "token_config" in self.okd_settings:
                token_config = self.okd_settings["token_config"]
            else:
                # Fallback to simple static token
                token_config = {"token_type": "static", "token": self.okd_settings.get("token", "")}

            self.token_manager = TokenManager(token_config)

    def _get_auth_headers(self) -> dict[str, str]:
        """Get current authentication headers with valid token."""
        if self.token_manager:
            token = self.token_manager.get_token()
            return {"Authorization": f"Bearer {token}"}

        # Fallback to static token from settings
        token = getattr(self, "token", "")
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _make_authenticated_request(
        self, method: str, endpoint: str, data: Optional[dict] = None, retry_auth: bool = True
    ) -> dict:
        """Make HTTP request with automatic token refresh on auth failures."""
        headers = self._get_auth_headers()
        headers["Content-Type"] = "application/json"

        try:
            # Use the basic _make_request method but with updated headers
            original_headers = getattr(self.session, "headers", {}).copy()
            self.session.headers.update(headers)

            return self._make_basic_request(method, endpoint, data)

        except BackendError as e:
            # Check if it's an authentication error
            if "401" in str(e) or ("403" in str(e) and retry_auth):
                logger.warning("Authentication failed, attempting token refresh")

                if self.token_manager:
                    try:
                        # Invalidate and refresh token
                        self.token_manager.invalidate_token()
                        new_headers = self._get_auth_headers()
                        self.session.headers.update(new_headers)

                        # Retry the request once with basic method
                        return self._make_basic_request(method, endpoint, data)

                    except Exception as refresh_error:
                        logger.exception("Token refresh failed: %s", refresh_error)  # noqa: TRY401
                        raise BackendError(
                            f"Authentication failed and token refresh unsuccessful: {refresh_error}"
                        ) from e

            # Re-raise original error if not auth-related or retry failed
            raise

        finally:
            # Restore original headers
            if "original_headers" in locals():
                self.session.headers.clear()
                self.session.headers.update(original_headers)
