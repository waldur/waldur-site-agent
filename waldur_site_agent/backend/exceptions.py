"""Generic exceptions and errors for backends."""

from typing import Union


class BackendError(Exception):
    """Error happened on the backend."""


class ConfigurationError(Exception):
    """Agent configuration is incorrect."""


class OfferingUserAccountLinkingRequiredError(Exception):
    """Linking a user to the offering user is required."""

    def __init__(self, message: str, comment_url: Union[str, None] = None) -> None:
        """Initialize exception with message and optional comment URL.

        Args:
            message: The error message explaining what is required
            comment_url: Optional URL for additional information or actions
        """
        super().__init__(message)
        self.comment_url = comment_url


class OfferingUserAdditionalValidationRequiredError(Exception):
    """Additional validation for the offering user is required."""

    def __init__(self, message: str, comment_url: Union[str, None] = None) -> None:
        """Initialize exception with message and optional comment URL.

        Args:
            message: The error message explaining what validation is required
            comment_url: Optional URL for additional information or actions
        """
        super().__init__(message)
        self.comment_url = comment_url
