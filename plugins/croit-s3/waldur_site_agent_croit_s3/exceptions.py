"""Custom exceptions for Croit S3 plugin."""


class CroitS3Error(Exception):
    """Base exception for Croit S3 operations."""


class CroitS3AuthenticationError(CroitS3Error):
    """Authentication failed with Croit API."""


class CroitS3UserNotFoundError(CroitS3Error):
    """S3 user not found."""


class CroitS3UserExistsError(CroitS3Error):
    """S3 user already exists."""


class CroitS3APIError(CroitS3Error):
    """General Croit API error."""
