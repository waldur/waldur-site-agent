"""Custom exceptions for the Ceph S3 plugin.

Named for Ceph rather than croit because both flavours raise them: the croit
management API and RadosGW Admin Ops report the same conditions, only with
different status codes and payloads. The one exception that stays croit-named is
the graph error, which is a croit concept with no Ceph equivalent.
"""


class CephS3Error(Exception):
    """Base exception for Ceph S3 operations."""


class CephS3AuthenticationError(CephS3Error):
    """Authentication failed against the storage management API."""


class CephS3UserNotFoundError(CephS3Error):
    """S3 user, or a key belonging to it, does not exist.

    Also raised for RadosGW's 403/InvalidAccessKeyId: an access key the gateway
    does not know is, for every caller here, a key that is already gone.
    """


class CephS3UserExistsError(CephS3Error):
    """S3 user already exists, or the access key belongs to another user."""


class CroitS3GraphNotFoundError(CephS3Error):
    """A croit statistics graph does not exist.

    Distinct from CephS3UserNotFoundError because the reporting loop reads a
    missing user as a terminated resource. Graph names live in croit's
    resources/statistics/graphite-queries.yml rather than in a documented API
    contract, so an upgrade can rename one — and that must fail loudly instead of
    looking like a resource that went away.
    """


class CephS3APIError(CephS3Error):
    """General storage management API error."""
