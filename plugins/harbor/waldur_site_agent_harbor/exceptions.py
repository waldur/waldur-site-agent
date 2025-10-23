"""Harbor-specific exceptions."""


class HarborError(Exception):
    """Base exception for Harbor-related errors."""


class HarborAPIError(HarborError):
    """Exception raised when Harbor API request fails."""


class HarborAuthenticationError(HarborError):
    """Exception raised when authentication with Harbor fails."""


class HarborProjectError(HarborError):
    """Exception raised when project operations fail."""


class HarborQuotaError(HarborError):
    """Exception raised when quota operations fail."""


class HarborOIDCError(HarborError):
    """Exception raised when OIDC group operations fail."""
