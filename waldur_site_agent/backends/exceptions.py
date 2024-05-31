"""Generic exceptions and errors for backends."""


class BackendError(Exception):
    """Error happened on the backend."""


class ConfigurationError(Exception):
    """Agent configuration is incorrect."""
