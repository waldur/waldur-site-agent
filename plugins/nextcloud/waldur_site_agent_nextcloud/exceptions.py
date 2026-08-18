"""Nextcloud-specific exceptions."""


class NextcloudError(Exception):
    """Base exception for Nextcloud-related errors."""


class NextcloudAPIError(NextcloudError):
    """Exception raised when a Nextcloud API request fails."""


class NextcloudAuthError(NextcloudError):
    """Exception raised when authentication with Nextcloud fails."""


class NextcloudGroupError(NextcloudError):
    """Exception raised when group operations fail."""


class NextcloudGroupFolderError(NextcloudError):
    """Exception raised when group folder operations fail."""
