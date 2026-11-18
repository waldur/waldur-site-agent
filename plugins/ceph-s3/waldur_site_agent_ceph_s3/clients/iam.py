"""IAM calls made as an RGW account's root user.

Admin Ops manages accounts, users and keys, but not what an account's own IAM API
creates: roles, groups and OIDC providers. RGW refuses to delete an account that
still holds any of them, and the agent's admin user cannot see them. So before an
account is removed, the agent signs IAM requests with a temporary key of the
account's root user and removes them itself.
"""

from __future__ import annotations

import logging
from typing import Iterator
from urllib.parse import urlencode
from xml.etree import ElementTree  # nosec B405 - answers from the operator's own RGW

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from ..exceptions import CephS3APIError

logger = logging.getLogger(__name__)

IAM_VERSION = "2010-05-08"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _texts(root: ElementTree.Element, name: str) -> list[str]:
    return [node.text or "" for node in root.iter() if _local(node.tag) == name]


def _first(root: ElementTree.Element, name: str) -> str:
    values = _texts(root, name)
    return values[0] if values else ""


class AccountIamClient:
    """IAM Query API of one RGW account, signed as one of its users."""

    def __init__(
        self,
        gateway_url: str,
        access_key: str,
        secret_key: str,
        region: str,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        self.url = gateway_url.rstrip("/") + "/"
        self._credentials = Credentials(access_key, secret_key)
        self._region = region
        self.session = httpx.Client(timeout=timeout, verify=verify_ssl)

    def close(self) -> None:
        self.session.close()

    def _call(self, action: str, **params: str) -> ElementTree.Element:
        body = urlencode({"Action": action, "Version": IAM_VERSION, **params})
        request = AWSRequest(
            method="POST",
            url=self.url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        # RGW checks x-amz-content-sha256 on IAM requests too, which only the
        # S3 flavour of the signer adds.
        S3SigV4Auth(self._credentials, "iam", self._region).add_auth(request)
        try:
            response = self.session.post(
                self.url, content=body, headers=dict(request.headers)
            )
        except httpx.HTTPError as e:
            raise CephS3APIError(f"IAM {action} failed: {e}") from e
        if response.status_code >= 300:
            code = ""
            try:
                code = _first(ElementTree.fromstring(response.content), "Code")
            except ElementTree.ParseError:
                pass
            raise CephS3APIError(
                f"IAM {action} answered {response.status_code} {code}".strip()
            )
        return ElementTree.fromstring(response.content)

    def _list(self, action: str, name: str, **params: str) -> Iterator[str]:
        """Every ``name`` value of a paginated List* action."""
        marker = ""
        while True:
            page = self._call(action, **params, **({"Marker": marker} if marker else {}))
            yield from _texts(page, name)
            marker = _first(page, "Marker")
            if _first(page, "IsTruncated").lower() != "true" or not marker:
                return

    def clear(self) -> dict[str, int]:
        """Remove the account's roles, groups and OIDC providers, with the
        policies attached to them. Users and their keys are left to Admin Ops.

        Returns:
            How many of each were removed.
        """
        removed = {"roles": 0, "groups": 0, "oidc_providers": 0}
        # RGW lists a tenanted account's roles as "tenant$name" but takes only
        # the bare name everywhere else (400 InvalidInput otherwise).
        roles = [name.rsplit("$", 1)[-1] for name in self._list("ListRoles", "RoleName")]
        for role in roles:
            for policy in list(self._list("ListRolePolicies", "member", RoleName=role)):
                self._call("DeleteRolePolicy", RoleName=role, PolicyName=policy)
            for arn in list(
                self._list("ListAttachedRolePolicies", "PolicyArn", RoleName=role)
            ):
                self._call("DetachRolePolicy", RoleName=role, PolicyArn=arn)
            self._call("DeleteRole", RoleName=role)
            removed["roles"] += 1
        for group in list(self._list("ListGroups", "GroupName")):
            for user in list(self._list("GetGroup", "UserName", GroupName=group)):
                self._call("RemoveUserFromGroup", GroupName=group, UserName=user)
            for policy in list(
                self._list("ListGroupPolicies", "member", GroupName=group)
            ):
                self._call("DeleteGroupPolicy", GroupName=group, PolicyName=policy)
            for arn in list(
                self._list("ListAttachedGroupPolicies", "PolicyArn", GroupName=group)
            ):
                self._call("DetachGroupPolicy", GroupName=group, PolicyArn=arn)
            self._call("DeleteGroup", GroupName=group)
            removed["groups"] += 1
        for arn in _texts(self._call("ListOpenIDConnectProviders"), "Arn"):
            self._call("DeleteOpenIDConnectProvider", OpenIDConnectProviderArn=arn)
            removed["oidc_providers"] += 1
        return removed
