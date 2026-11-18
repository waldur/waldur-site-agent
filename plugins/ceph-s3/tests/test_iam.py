"""The account IAM cleanup that precedes deleting an RGW account."""

from urllib.parse import parse_qs

import httpx
import pytest

from waldur_site_agent_ceph_s3.clients.iam import AccountIamClient
from waldur_site_agent_ceph_s3.exceptions import CephS3APIError

NS = 'xmlns="https://iam.amazonaws.com/doc/2010-05-08/"'

ANSWERS = {
    "ListRoles": f"<ListRolesResponse {NS}><ListRolesResult><Roles><member><RoleName>t1$r1</RoleName></member></Roles><IsTruncated>false</IsTruncated></ListRolesResult></ListRolesResponse>",
    "ListRolePolicies": f"<ListRolePoliciesResponse {NS}><ListRolePoliciesResult><PolicyNames><member>inline</member></PolicyNames><IsTruncated>false</IsTruncated></ListRolePoliciesResult></ListRolePoliciesResponse>",
    "ListAttachedRolePolicies": f"<R {NS}><AttachedPolicies/><IsTruncated>false</IsTruncated></R>",
    "ListGroups": f"<R {NS}><Groups><member><GroupName>g1</GroupName></member></Groups><IsTruncated>false</IsTruncated></R>",
    "GetGroup": f"<R {NS}><Users><member><UserName>u1</UserName></member></Users><IsTruncated>false</IsTruncated></R>",
    "ListGroupPolicies": f"<R {NS}><PolicyNames/><IsTruncated>false</IsTruncated></R>",
    "ListAttachedGroupPolicies": f"<R {NS}><AttachedPolicies/><IsTruncated>false</IsTruncated></R>",
    "ListOpenIDConnectProviders": f"<R {NS}><OpenIDConnectProviderList><member><Arn>arn:aws:iam::RGW1:oidc-provider/idp</Arn></member></OpenIDConnectProviderList></R>",
}


def client_with(handler):
    client = AccountIamClient("https://rgw.example.org", "AK", "SK", region="default")
    client.session = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def test_clear_removes_policies_before_roles_and_members_before_groups():
    calls = []

    def handler(request):
        form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        calls.append((form["Action"], form.get("RoleName") or form.get("GroupName")))
        assert request.headers["authorization"].startswith("AWS4-HMAC-SHA256")
        return httpx.Response(200, text=ANSWERS.get(form["Action"], f"<R {NS}/>"))

    removed = client_with(handler).clear()

    actions = [action for action, _ in calls]
    # The tenant prefix ListRoles adds is dropped before the role is named again.
    assert ("DeleteRole", "r1") in calls
    assert actions.index("DeleteRolePolicy") < actions.index("DeleteRole")
    assert actions.index("RemoveUserFromGroup") < actions.index("DeleteGroup")
    assert "DeleteOpenIDConnectProvider" in actions
    assert removed == {"roles": 1, "groups": 1, "oidc_providers": 1}


def test_a_refusal_is_an_api_error_naming_the_action():
    def handler(request):
        return httpx.Response(403, text=f"<ErrorResponse {NS}><Error><Code>AccessDenied</Code></Error></ErrorResponse>")

    with pytest.raises(CephS3APIError, match="ListRoles answered 403 AccessDenied"):
        client_with(handler).clear()
