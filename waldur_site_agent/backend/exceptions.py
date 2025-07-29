"""Generic exceptions and errors for backends."""


class BackendError(Exception):
    """Error happened on the backend."""


class ConfigurationError(Exception):
    """Agent configuration is incorrect."""


class OfferingUserAccountLinkingRequiredError(Exception):
    """Linking a user to the offering user is required."""


class OfferingUserAdditionalValidationRequiredError(Exception):
    """Additional validation for the offering user is required."""
