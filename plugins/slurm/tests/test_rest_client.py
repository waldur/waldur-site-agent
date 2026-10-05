"""Tests for the slurmrestd-backed SLURM client.

The REST analogue of test_command_construction.py: every test asserts the
exact request path, query parameters and JSON payload sent to slurmrestd,
using an httpx.MockTransport — no network, no SLURM.
"""

import json
from unittest import mock

import httpx
import pytest
from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.rest_client import (
    FAIRSHARE_USE_PARENT,
    SlurmRestClient,
    _parse_walltime_minutes,
)

API = "v0.0.43"


def envelope(**kwargs):
    """Build a slurmrestd response body with an empty error envelope."""
    body = {"meta": {}, "errors": [], "warnings": []}
    body.update(kwargs)
    return body


class RecordingHandler:
    """MockTransport handler that records requests and replays canned responses."""

    def __init__(self, responses=None):
        self.requests: list[httpx.Request] = []
        # Map of "METHOD path" -> response body dict; default empty envelope.
        self.responses = responses or {}
        self.status_codes = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        key = f"{request.method} {request.url.path}"
        status = self.status_codes.get(key, 200)
        return httpx.Response(status, json=self.responses.get(key, envelope()))

    def body(self, index=0) -> dict:
        return json.loads(self.requests[index].content)


@pytest.fixture
def handler():
    return RecordingHandler()


@pytest.fixture
def client(handler, monkeypatch):
    monkeypatch.setenv("SLURM_JWT", "test-token")
    return SlurmRestClient(
        slurm_tres={},
        rest_settings={
            "url": "http://localhost:6820",
            "api_version": API,
            "username": "waldur-agent",
            "token_env": "SLURM_JWT",
        },
        cluster_name="testcluster",
        transport=httpx.MockTransport(handler),
    )


class TestTransport:
    def test_auth_headers_are_sent(self, client, handler):
        client.list_resources()
        request = handler.requests[0]
        assert request.headers["X-SLURM-USER-TOKEN"] == "test-token"
        assert request.headers["X-SLURM-USER-NAME"] == "waldur-agent"

    def test_body_level_errors_raise_backend_error_despite_http_200(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/accounts/"] = envelope(
            errors=[{"description": "Unable to connect to database", "error_number": 7000}]
        )
        with pytest.raises(BackendError, match="Unable to connect to database"):
            client.list_resources()

    def test_http_error_status_raises_backend_error(self, client, handler):
        handler.status_codes[f"GET /slurmdb/{API}/accounts/"] = 500
        with pytest.raises(BackendError, match="HTTP 500"):
            client.list_resources()

    def test_401_triggers_token_reload_and_retry(self, handler, monkeypatch, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("stale-token")

        def auth_handler(request: httpx.Request) -> httpx.Response:
            handler.requests.append(request)
            if request.headers["X-SLURM-USER-TOKEN"] == "stale-token":
                return httpx.Response(401, json=envelope())
            return httpx.Response(200, json=envelope(accounts=[]))

        client = SlurmRestClient(
            slurm_tres={},
            rest_settings={
                "url": "http://localhost:6820",
                "api_version": API,
                "username": "waldur-agent",
                "token_file": str(token_file),
            },
            cluster_name="testcluster",
            transport=httpx.MockTransport(auth_handler),
        )
        # Prime the cached token, then rotate the file on disk.
        client._get_token()
        token_file.write_text("fresh-token")
        client.list_resources()
        assert handler.requests[-1].headers["X-SLURM-USER-TOKEN"] == "fresh-token"

    def test_requests_are_tracked(self, client, handler):
        client.list_resources()
        assert client.executed_commands == [f"GET /slurmdb/{API}/accounts/"]

    def test_missing_cluster_name_raises(self):
        with pytest.raises(BackendError, match="cluster_name is required"):
            SlurmRestClient(
                slurm_tres={},
                rest_settings={"url": "http://localhost:6820", "token_env": "X"},
                cluster_name="",
            )


class TestAccounts:
    def test_list_resources(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/accounts/"] = envelope(
            accounts=[{"name": "acc1", "description": "Account 1", "organization": "org1"}]
        )
        resources = client.list_resources()
        assert len(resources) == 1
        assert resources[0].name == "acc1"
        assert resources[0].description == "Account 1"
        assert resources[0].organization == "org1"

    def test_get_resource_returns_none_when_absent(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/account/missing"] = envelope(accounts=[])
        assert client.get_resource("missing") is None

    def test_create_resource_uses_accounts_association_with_parent(self, client, handler):
        client.create_resource("acc1", "My project", "proj1", parent_name="parent1")
        request = handler.requests[0]
        assert request.method == "POST"
        assert request.url.path == f"/slurmdb/{API}/accounts_association/"
        assert handler.body() == {
            "association_condition": {
                "accounts": ["acc1"],
                "clusters": ["testcluster"],
                "association": {"parent": "parent1"},
            },
            "account": {"description": "My project", "organization": "proj1"},
        }

    def test_create_resource_without_parent_omits_association(self, client, handler):
        client.create_resource("acc1", "desc", "org")
        assert "association" not in handler.body()["association_condition"]

    def test_delete_resource(self, client, handler):
        client.delete_resource("acc1")
        request = handler.requests[0]
        assert request.method == "DELETE"
        assert request.url.path == f"/slurmdb/{API}/account/acc1"

    def test_get_account_parent_reads_account_level_association(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": "user1", "parent_account": "wrong"},
                {"account": "acc1", "user": "", "parent_account": "parent1"},
            ]
        )
        assert client.get_account_parent("acc1") == "parent1"
        assert dict(handler.requests[0].url.params) == {
            "account": "acc1",
            "cluster": "testcluster",
        }

    def test_set_account_parent(self, client, handler):
        client.set_account_parent("acc1", "parent2")
        # "user" is a required ASSOC field; "" addresses the account-level
        # association (parsers.c add_parse_req / associations.c matching).
        assert handler.body() == {
            "associations": [
                {
                    "account": "acc1",
                    "cluster": "testcluster",
                    "user": "",
                    "parent_account": "parent2",
                }
            ]
        }

    def test_delete_all_users_from_account(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": ""},
                {"account": "acc1", "user": "user1"},
                {"account": "acc1", "user": "user2"},
            ]
        )
        client.delete_all_users_from_account("acc1")
        delete_request = handler.requests[-1]
        assert delete_request.method == "DELETE"
        assert delete_request.url.path == f"/slurmdb/{API}/associations/"
        assert dict(delete_request.url.params) == {
            "account": "acc1",
            "user": "user1,user2",
            "cluster": "testcluster",
        }

    def test_delete_all_users_repoints_a_default_account_first(self, client, handler):
        """slurmdbd refuses to drop a default association while others remain."""
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": "user1"},
                {"account": "acc2", "user": "user1"},
            ]
        )
        handler.responses[f"GET /slurmdb/{API}/user/user1"] = envelope(
            users=[{"name": "user1", "default": {"account": "acc1"}}]
        )

        client.delete_all_users_from_account("acc1")

        posts = [
            r
            for r in handler.requests
            if r.method == "POST" and r.url.path == f"/slurmdb/{API}/users/"
        ]
        assert len(posts) == 1, "the default account should be re-pointed exactly once"
        assert json.loads(posts[0].content) == {
            "users": [{"name": "user1", "default": {"account": "acc2"}}]
        }
        # ...and the re-pointing happens before the associations are dropped.
        assert handler.requests[-1].method == "DELETE"

    def test_delete_all_users_leaves_an_unrelated_default_alone(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": "user1"},
                {"account": "acc2", "user": "user1"},
            ]
        )
        handler.responses[f"GET /slurmdb/{API}/user/user1"] = envelope(
            users=[{"name": "user1", "default": {"account": "acc2"}}]
        )

        client.delete_all_users_from_account("acc1")

        assert not [
            r
            for r in handler.requests
            if r.method == "POST" and r.url.path == f"/slurmdb/{API}/users/"
        ]


class TestLimits:
    def test_set_resource_limits_builds_grp_tres_mins_payload(self, client, handler):
        client.set_resource_limits("acc1", {"cpu": 60000, "mem": 61440, "gres/gpu": 600})
        assert handler.body() == {
            "associations": [
                {
                    "account": "acc1",
                    "cluster": "testcluster",
                    "user": "",
                    "max": {
                        "tres": {
                            "group": {
                                "minutes": [
                                    {"count": 60000, "type": "cpu"},
                                    {"count": 600, "type": "gres", "name": "gpu"},
                                    {"count": 61440, "type": "mem"},
                                ]
                            }
                        }
                    },
                }
            ]
        }

    def test_get_resource_limits_parses_tres_and_tristate_counts(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {
                    "account": "acc1",
                    "user": "",
                    "max": {
                        "tres": {
                            "group": {
                                "minutes": [
                                    {"type": "cpu", "name": "", "count": 60000},
                                    {
                                        "type": "mem",
                                        "count": {"set": True, "infinite": False, "number": 1024},
                                    },
                                    {"type": "node", "count": {"set": False, "infinite": False}},
                                ]
                            }
                        }
                    },
                }
            ]
        )
        assert client.get_resource_limits("acc1") == {"cpu": 60000, "mem": 1024}

    def test_get_resource_user_limits_skips_account_association(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": ""},
                {
                    "account": "acc1",
                    "user": "user1",
                    "max": {"tres": {"minutes": {"per": {"job": [{"type": "cpu", "count": 100}]}}}},
                },
            ]
        )
        assert client.get_resource_user_limits("acc1") == {"user1": {"cpu": 100}}

    def test_set_account_limits_unsupported_type_raises(self, client):
        with pytest.raises(BackendError, match="Unsupported limit type"):
            client.set_account_limits("acc1", "GrpWall", {"cpu": 1})

    def test_set_account_limits_grp_tres(self, client, handler):
        client.set_account_limits("acc1", "GrpTRES", {"cpu": 100})
        # A GET (read-for-merge) precedes the POST; the payload is the last request.
        assoc = handler.body(-1)["associations"][0]
        assert assoc["max"]["tres"]["total"] == [{"count": 100, "type": "cpu"}]

    def test_set_account_limits_merges_with_existing_tres(self, client, handler):
        # CLI parity: a partial {cpu} payload must not wipe an existing mem limit.
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {
                    "account": "acc1",
                    "user": "",
                    "max": {
                        "tres": {
                            "group": {
                                "minutes": [
                                    {"type": "cpu", "count": 100},
                                    {"type": "mem", "count": 2048},
                                ]
                            }
                        }
                    },
                }
            ]
        )
        client.set_account_limits("acc1", "GrpTRESMins", {"cpu": 500})
        posted = handler.body(-1)["associations"][0]["max"]["tres"]["group"]["minutes"]
        assert posted == [
            {"count": 500, "type": "cpu"},
            {"count": 2048, "type": "mem"},
        ]


class TestAssociations:
    def test_create_association_posts_users_association(self, client, handler):
        client.create_association("user1", "acc1", default_account="defacc")
        request = handler.requests[0]
        assert request.url.path == f"/slurmdb/{API}/users_association/"
        assert handler.body() == {
            "association_condition": {
                "users": ["user1"],
                "accounts": ["acc1"],
                "clusters": ["testcluster"],
                # Share=parent equivalent
                "association": {"fairshare": FAIRSHARE_USE_PARENT},
            },
            "user": {"default": {"account": "defacc"}},
        }

    def test_create_association_with_partition(self, client, handler):
        client.create_association_with_partition("user1", "acc1", "gpu", "defacc")
        condition = handler.body()["association_condition"]
        assert condition["partitions"] == ["gpu"]
        # Single-partition variant does not force parent fairshare (CLI parity)
        assert "association" not in condition

    def test_create_association_with_partitions_sorts_and_validates(self, client, handler):
        client.create_association_with_partitions("user1", "acc1", ["gpu", "cn"], "defacc")
        condition = handler.body()["association_condition"]
        assert condition["partitions"] == ["cn", "gpu"]
        assert condition["association"] == {"fairshare": FAIRSHARE_USE_PARENT}

    def test_create_association_with_qos_single_request(self, client, handler):
        client.create_association_with_qos(
            "user1",
            "acc1",
            ["normal", "boost"],
            default_qos="boost",
            partitions=["gpu", "cn"],
            default_account="defacc",
        )
        request = handler.requests[0]
        assert request.url.path == f"/slurmdb/{API}/users_association/"
        condition = handler.body()["association_condition"]
        # Partitions sorted; one row per partition, each carrying the qos.
        assert condition["partitions"] == ["cn", "gpu"]
        # Flat ASSOC_REC_SET template keys (qoslevel/defaultqos), plus parent fairshare.
        assert condition["association"] == {
            "fairshare": FAIRSHARE_USE_PARENT,
            "qoslevel": "normal,boost",
            "defaultqos": "boost",
        }
        assert handler.body()["user"] == {"default": {"account": "defacc"}}

    def test_create_association_with_qos_no_partition(self, client, handler):
        client.create_association_with_qos("user1", "acc1", ["normal"])
        condition = handler.body()["association_condition"]
        assert "partitions" not in condition
        assert condition["association"]["qoslevel"] == "normal"
        assert "defaultqos" not in condition["association"]

    def test_create_association_with_qos_empty_rejected(self, client):
        with pytest.raises(BackendError, match="non-empty"):
            client.create_association_with_qos("user1", "acc1", [])

    def test_create_association_with_qos_invalid_name_rejected(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM QoS name"):
            client.create_association_with_qos("user1", "acc1", ["a; rm -rf /"])

    def test_create_association_with_qos_invalid_partition_rejected(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM partition name"):
            client.create_association_with_qos("user1", "acc1", ["normal"], partitions=["bad name"])

    def test_set_account_grp_submit_jobs(self, client, handler):
        client.set_account_grp_submit_jobs("acc1", 0)
        request = handler.requests[0]
        assert request.url.path == f"/slurmdb/{API}/associations/"
        assoc = handler.body()["associations"][0]
        # Account-level row (user="") with the nested grp_submit_jobs path.
        assert assoc["account"] == "acc1"
        assert assoc["user"] == ""
        assert assoc["max"] == {"jobs": {"per": {"submitted": 0}}}

    def test_clear_account_grp_submit_jobs(self, client, handler):
        client.set_account_grp_submit_jobs("acc1", -1)
        assoc = handler.body()["associations"][0]
        assert assoc["max"] == {"jobs": {"per": {"submitted": -1}}}

    def test_create_association_with_invalid_partition_raises(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM partition name"):
            client.create_association_with_partitions("user1", "acc1", ["bad name"], "")

    def test_create_association_with_partition_singular_invalid_raises(self, client):
        with pytest.raises(BackendError, match="Invalid SLURM partition name"):
            client.create_association_with_partition("user1", "acc1", "bad name", "")

    def test_delete_association(self, client, handler):
        client.delete_association("user1", "acc1")
        # The user's associations are listed first; the delete is the last request.
        request = handler.requests[-1]
        assert request.method == "DELETE"
        assert request.url.path == f"/slurmdb/{API}/associations/"
        assert dict(request.url.params) == {
            "account": "acc1",
            "user": "user1",
            "cluster": "testcluster",
        }


    def _user_with(self, handler, accounts, default):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[{"account": a, "user": "user1"} for a in accounts]
            + [{"account": "root", "user": "root"}, {"account": accounts[0], "user": ""}]
        )
        handler.responses[f"GET /slurmdb/{API}/user/user1"] = envelope(
            users=[{"name": "user1", "default": {"account": default}}]
        )

    def test_delete_default_association_repoints_the_default_first(self, client, handler):
        """slurmdbd refuses to drop a default association while others remain."""
        self._user_with(handler, ["acc1", "acc2"], default="acc1")
        client.delete_association("user1", "acc1")
        methods = [(r.method, r.url.path) for r in handler.requests]
        assert methods[-2] == ("POST", f"/slurmdb/{API}/users/")
        assert methods[-1] == ("DELETE", f"/slurmdb/{API}/associations/")
        repoint = json.loads(handler.requests[-2].content)
        assert repoint == {"users": [{"name": "user1", "default": {"account": "acc2"}}]}

    def test_delete_non_default_association_leaves_the_default_alone(self, client, handler):
        self._user_with(handler, ["acc1", "acc2"], default="acc1")
        client.delete_association("user1", "acc2")
        assert all(r.method != "POST" for r in handler.requests)
        assert handler.requests[-1].method == "DELETE"
        assert handler.requests[-1].url.path == f"/slurmdb/{API}/associations/"

    def test_delete_last_association_removes_the_user(self, client, handler):
        self._user_with(handler, ["acc1"], default="acc1")
        client.delete_association("user1", "acc1")
        last = handler.requests[-1]
        assert (last.method, last.url.path) == ("DELETE", f"/slurmdb/{API}/user/user1")
        assert all(r.url.path != f"/slurmdb/{API}/associations/" or r.method == "GET"
                   for r in handler.requests)
    def test_get_association(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {
                    "account": "acc1",
                    "user": "user1",
                    "max": {"tres": {"group": {"minutes": [{"type": "cpu", "count": 42}]}}},
                }
            ]
        )
        association = client.get_association("user1", "acc1")
        assert association is not None
        assert association.account == "acc1"
        assert association.user == "user1"
        assert association.value == 42

    def test_list_resource_users(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": ""},
                {"account": "acc1", "user": "user1"},
                {"account": "acc1", "user": "user2"},
            ]
        )
        assert client.list_resource_users("acc1") == ["user1", "user2"]


class TestQos:
    def test_create_qos_single_request_with_mapped_flags_and_limits(self, client, handler):
        client.create_qos(
            "qos1",
            flags="DenyOnLimit,NoDecay",
            grp_tres="cpu=25600,node=100",
            max_jobs=100,
            max_submit=200,
            max_wall="30-00:00:00",
            min_tres_per_job="gres/gpu=1",
        )
        qos = handler.body()["qos"][0]
        assert qos["name"] == "qos1"
        assert qos["flags"] == ["DENY_LIMIT", "NO_DECAY"]
        assert qos["limits"]["max"]["tres"]["total"] == [
            {"count": 25600, "type": "cpu"},
            {"count": 100, "type": "node"},
        ]
        # MaxJobsPerUser lives under max/jobs/active_jobs in every supported
        # API version (data_parser v0.0.43-v0.0.46 parsers.c).
        assert qos["limits"]["max"]["jobs"]["active_jobs"]["per"]["user"] == 100
        assert qos["limits"]["max"]["jobs"]["per"]["user"] == 200
        assert qos["limits"]["max"]["wall_clock"]["per"]["job"] == 30 * 24 * 60
        assert qos["limits"]["min"]["tres"]["per"]["job"] == [
            {"count": 1, "type": "gres", "name": "gpu"}
        ]

    def test_create_qos_invalid_tres_value_raises_backend_error(self, client):
        # A non-integer TRES value must surface as BackendError, not a bare
        # ValueError that bypasses every `except BackendError` handler.
        with pytest.raises(BackendError, match="Invalid TRES value"):
            client.create_qos("qos1", grp_tres="cpu=unlimited")

    def test_qos_exists(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/qos/qos1"] = envelope(qos=[{"name": "qos1"}])
        assert client.qos_exists("qos1") is True
        assert client.qos_exists("other") is False

    def test_delete_qos(self, client, handler):
        client.delete_qos("qos1")
        assert handler.requests[0].method == "DELETE"
        assert handler.requests[0].url.path == f"/slurmdb/{API}/qos/qos1"

    def test_set_account_qos_splits_csv(self, client, handler):
        client.set_account_qos("acc1", "qos1,qos2")
        assoc = handler.body()["associations"][0]
        assert assoc["qos"] == ["qos1", "qos2"]
        assert "default" not in assoc

    def test_set_account_qos_with_default_posts_default_qos(self, client, handler):
        client.set_account_qos("acc1", "stop", default_qos="stop")
        assoc = handler.body()["associations"][0]
        assert assoc["qos"] == ["stop"]
        assert assoc["default"] == {"qos": "stop"}

    def test_get_current_account_default_qos(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": "", "qos": ["normal"], "default": {"qos": "normal"}}
            ]
        )
        assert client.get_current_account_default_qos("acc1") == "normal"

    def test_get_current_account_default_qos_missing(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[{"account": "acc1", "user": "", "qos": ["normal"]}]
        )
        assert client.get_current_account_default_qos("acc1") == ""

    def test_get_current_account_qos(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[{"account": "acc1", "user": "", "qos": ["qos1", "qos2"]}]
        )
        assert client.get_current_account_qos("acc1") == "qos1,qos2"

    def test_add_account_qos_merges_with_current_list(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[{"account": "acc1", "user": "", "qos": ["qos1"]}]
        )
        client.add_account_qos("acc1", "qos2")
        assert handler.body(-1)["associations"][0]["qos"] == ["qos1", "qos2"]

    def test_add_account_qos_is_idempotent(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[{"account": "acc1", "user": "", "qos": ["qos1"]}]
        )
        client.add_account_qos("acc1", "qos1")
        assert len(handler.requests) == 1  # read only, no write

    def test_set_account_default_qos(self, client, handler):
        client.set_account_default_qos("acc1", "qos1")
        assert handler.body()["associations"][0]["default"] == {"qos": "qos1"}

    def test_set_qos_grp_tres_mins_posts_qos_limits(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/qos/qos1"] = envelope(qos=[])
        client.set_qos_grp_tres_mins("qos1", {"billing": 19854000, "gres/gpu": 300000})
        qos = handler.body(-1)["qos"][0]
        assert qos["name"] == "qos1"
        minutes = qos["limits"]["max"]["tres"]["group"]["minutes"]
        assert {"count": 19854000, "type": "billing"} in minutes
        assert {"count": 300000, "type": "gres", "name": "gpu"} in minutes

    def test_set_qos_grp_tres_mins_merges_existing(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/qos/qos1"] = envelope(
            qos=[
                {
                    "name": "qos1",
                    "limits": {
                        "max": {
                            "tres": {
                                "group": {
                                    "minutes": [
                                        {"type": "billing", "count": 100},
                                        {"type": "gres", "name": "gpu", "count": 50},
                                    ]
                                }
                            }
                        }
                    },
                }
            ]
        )
        client.set_qos_grp_tres_mins("qos1", {"billing": 200})
        minutes = handler.body(-1)["qos"][0]["limits"]["max"]["tres"]["group"]["minutes"]
        assert {"count": 200, "type": "billing"} in minutes
        assert {"count": 50, "type": "gres", "name": "gpu"} in minutes

    def test_get_qos_grp_tres_mins(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/qos/qos1"] = envelope(
            qos=[
                {
                    "name": "qos1",
                    "limits": {
                        "max": {
                            "tres": {
                                "group": {
                                    "minutes": [
                                        {"type": "billing", "count": 14428800},
                                        {"type": "gres", "name": "gpu", "count": 216000},
                                    ]
                                }
                            }
                        }
                    },
                }
            ]
        )
        assert client.get_qos_grp_tres_mins("qos1") == {
            "billing": 14428800,
            "gres/gpu": 216000,
        }


class TestFairshareAndMisc:
    def test_set_account_fairshare(self, client, handler):
        assert client.set_account_fairshare("acc1", 50) is True
        assert handler.body()["associations"][0]["shares_raw"] == 50

    def test_get_account_fairshare_handles_tristate(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": "", "shares_raw": {"set": True, "number": 7}}
            ]
        )
        assert client.get_account_fairshare("acc1") == 7

    def test_get_account_fairshare_returns_zero_for_parent_sentinel(self, client, handler):
        # An account inheriting fairshare reports the USE_PARENT sentinel;
        # match the CLI client, which renders 'parent' as 0.
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {"account": "acc1", "user": "", "shares_raw": FAIRSHARE_USE_PARENT}
            ]
        )
        assert client.get_account_fairshare("acc1") == 0

    def test_get_account_fairshare_treats_infinite_as_parent(self, client, handler):
        # Tri-state struct with infinite=True is an unlimited/inherited share,
        # not a real number; it must not be reported as the raw sentinel value.
        handler.responses[f"GET /slurmdb/{API}/associations/"] = envelope(
            associations=[
                {
                    "account": "acc1",
                    "user": "",
                    "shares_raw": {"set": True, "infinite": True, "number": 2000000000},
                }
            ]
        )
        assert client.get_account_fairshare("acc1") == 0

    def test_qos_exists_returns_false_on_not_found_error(self, client, handler):
        # Real slurmrestd answers a missing single entity with an error body,
        # not an empty list; qos_exists must treat that as "absent", not crash.
        handler.responses[f"GET /slurmdb/{API}/qos/missing"] = envelope(
            errors=[{"description": "Unable to find QOS missing", "error_number": 2017}]
        )
        assert client.qos_exists("missing") is False

    def test_list_tres(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/tres/"] = envelope(
            TRES=[
                {"type": "cpu", "name": "", "count": 0},
                {"type": "gres", "name": "gpu", "count": 0},
            ]
        )
        assert client.list_tres() == ["cpu", "gres/gpu"]

    def test_list_clusters(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/clusters/"] = envelope(
            clusters=[{"name": "testcluster"}]
        )
        assert client.list_clusters() == ["testcluster"]

    def test_get_version_reads_ping_meta(self, client, handler):
        handler.responses[f"GET /slurm/{API}/ping/"] = envelope(
            meta={"slurm": {"release": "25.11.1"}}, pings=[]
        )
        assert client.get_version() == "slurm 25.11.1"

    def test_validate_slurm_binary_checks_ping_and_cli(self, client, handler):
        # REST mode still shells out to sacct/sacctmgr for usage reporting, so
        # when those binaries are present validation requires BOTH a healthy
        # ping and a real CLI binary.
        with mock.patch.object(client, "_cli_binaries_present", return_value=True), mock.patch.object(
            client._cli, "validate_slurm_binary", return_value=True
        ):
            assert client.validate_slurm_binary() is True
            handler.status_codes[f"GET /slurm/{API}/ping/"] = 500
            assert client.validate_slurm_binary() is False

    def test_validate_slurm_binary_fails_on_emulator_shadow(self, client, handler):
        # Ping is healthy and a CLI binary is present but is an emulator shadow:
        # the guard must fail validation so fabricated usage never reaches billing.
        with mock.patch.object(client, "_cli_binaries_present", return_value=True), mock.patch.object(
            client._cli, "validate_slurm_binary", return_value=False
        ):
            assert client.validate_slurm_binary() is False

    def test_validate_slurm_binary_passes_when_cli_binary_absent(self, client, handler):
        # No sacct/sacctmgr on the host (the REST-only deployment shape): skip
        # the shadow guard — there's nothing to shadow and usage reporting fails
        # loudly at call time rather than fabricating data.
        with mock.patch.object(client, "_cli_binaries_present", return_value=False), mock.patch.object(
            client._cli, "validate_slurm_binary", return_value=False
        ) as cli_validate:
            assert client.validate_slurm_binary() is True
            cli_validate.assert_not_called()


class TestJobs:
    def _jobs_response(self):
        return envelope(
            jobs=[
                {
                    "job_id": 1,
                    "account": "acc1",
                    "user_name": "user1",
                    "job_state": ["RUNNING"],
                },
                {
                    "job_id": 2,
                    "account": "acc1",
                    "user_name": "user2",
                    "job_state": ["PENDING"],
                },
                {
                    "job_id": 3,
                    "account": "acc1",
                    "user_name": "user1",
                    "job_state": ["COMPLETED"],
                },
                {"job_id": 4, "account": "other", "user_name": "user1", "job_state": "RUNNING"},
            ]
        )

    def test_list_active_user_jobs_filters_account_user_and_state(self, client, handler):
        handler.responses[f"GET /slurm/{API}/jobs/"] = self._jobs_response()
        assert client.list_active_user_jobs("acc1", "user1") == ["1"]

    def test_cancel_active_user_jobs_deletes_each_active_job(self, client, handler):
        handler.responses[f"GET /slurm/{API}/jobs/"] = self._jobs_response()
        client.cancel_active_user_jobs("acc1")
        deletes = [r for r in handler.requests if r.method == "DELETE"]
        assert [r.url.path for r in deletes] == [
            f"/slurm/{API}/job/1",
            f"/slurm/{API}/job/2",
        ]

    def test_cancel_skips_jobs_without_job_id(self, client, handler):
        # A job missing job_id must be skipped, not turned into DELETE job/None
        # (404 -> BackendError) which would abort the loop and strand later jobs.
        handler.responses[f"GET /slurm/{API}/jobs/"] = envelope(
            jobs=[
                {"account": "acc1", "user_name": "user1", "job_state": ["RUNNING"]},
                {"job_id": 5, "account": "acc1", "user_name": "user1", "job_state": ["RUNNING"]},
            ]
        )
        client.cancel_active_user_jobs("acc1")
        deletes = [r.url.path for r in handler.requests if r.method == "DELETE"]
        assert deletes == [f"/slurm/{API}/job/5"]

    def test_list_active_jobs_excludes_sibling_cluster_rows(self, client, handler):
        # A same-named account on a sibling cluster (e.g. from an unfiltered
        # fallback fetch) must not have its jobs cancelled.
        handler.responses[f"GET /slurm/{API}/jobs/"] = envelope(
            jobs=[
                {
                    "job_id": 1,
                    "account": "acc1",
                    "user_name": "user1",
                    "job_state": ["RUNNING"],
                    "cluster": "testcluster",
                },
                {
                    "job_id": 2,
                    "account": "acc1",
                    "user_name": "user1",
                    "job_state": ["RUNNING"],
                    "cluster": "sibling",
                },
            ]
        )
        assert client.list_active_user_jobs("acc1", "user1") == ["1"]

    def test_list_active_jobs_falls_back_when_filtered_query_rejected(self, monkeypatch):
        # A controller that rejects the filter params (4xx) must trigger a
        # fallback to the unfiltered fetch; the result is then filtered
        # client-side, so the right jobs still come back.
        monkeypatch.setenv("SLURM_JWT", "test-token")
        calls = {"filtered": 0, "unfiltered": 0}

        def jobs_handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET" and request.url.path == f"/slurm/{API}/jobs/":
                if request.url.query:
                    calls["filtered"] += 1
                    return httpx.Response(400, json=envelope())
                calls["unfiltered"] += 1
                return httpx.Response(
                    200,
                    json=envelope(
                        jobs=[
                            {
                                "job_id": 1,
                                "account": "acc1",
                                "user_name": "user1",
                                "job_state": ["RUNNING"],
                            }
                        ]
                    ),
                )
            return httpx.Response(200, json=envelope())

        client = SlurmRestClient(
            slurm_tres={},
            rest_settings={
                "url": "http://localhost:6820",
                "api_version": API,
                "username": "waldur-agent",
                "token_env": "SLURM_JWT",
            },
            cluster_name="testcluster",
            transport=httpx.MockTransport(jobs_handler),
        )
        assert client.list_active_user_jobs("acc1", "user1") == ["1"]
        assert calls == {"filtered": 1, "unfiltered": 1}


def _job(account, user, elapsed, tres, cluster="testcluster", start=None, end=None):
    times = {"elapsed": elapsed}
    if start is not None:
        times["start"] = start
    if end is not None:
        times["end"] = end
    return {
        "account": account,
        "user": user,
        "cluster": cluster,
        "time": times,
        "tres": {"allocated": tres, "requested": tres},
    }


class TestUsageReport:
    """Usage reports come from slurmdb job records, not from sacct."""

    TRES = {"cpu": {}, "mem": {}, "gres/gpu": {}}

    def _client(self, handler, monkeypatch):
        monkeypatch.setenv("SLURM_JWT", "test-token")
        return SlurmRestClient(
            slurm_tres=self.TRES,
            rest_settings={
                "url": "http://localhost:6820",
                "api_version": API,
                "username": "waldur-agent",
                "token_env": "SLURM_JWT",
            },
            cluster_name="testcluster",
            transport=httpx.MockTransport(handler),
        )

    def test_queries_slurmdb_jobs_for_the_month(self, handler, monkeypatch):
        client = self._client(handler, monkeypatch)
        client.get_usage_report(["acc1", "acc2"], "UTC")
        request = handler.requests[0]
        assert request.url.path == f"/slurmdb/{API}/jobs/"
        params = dict(request.url.params)
        assert params["account"] == "acc1,acc2"
        assert params["cluster"] == "testcluster"
        assert params["start_time"].endswith("T00:00:00")
        assert params["end_time"].endswith("T23:59:59")

    def test_jobs_become_sacct_shaped_lines(self, handler, monkeypatch):
        handler.responses[f"GET /slurmdb/{API}/jobs/"] = envelope(
            jobs=[
                _job(
                    "acc1",
                    "alice",
                    3600,
                    [
                        {"type": "cpu", "count": 4},
                        {"type": "mem", "count": 8192},
                        {"type": "gres", "name": "gpu", "count": 1},
                        {"type": "node", "count": 1},
                    ],
                ),
                # 25 hours: the elapsed field must not wrap at a day.
                _job("acc1", "bob", 90000, [{"type": "cpu", "count": 2}]),
            ]
        )
        client = self._client(handler, monkeypatch)
        lines = client.get_usage_report(["acc1"])
        assert [(line.account, line.user) for line in lines] == [
            ("acc1", "alice"),
            ("acc1", "bob"),
        ]
        alice, bob = lines
        assert alice.duration == 60.0
        assert alice.tres_usage == {"cpu": 240.0, "mem": 8192 * 60 // 1, "gres/gpu": 60.0}
        assert bob.duration == 1500.0
        assert bob.tres_usage == {"cpu": 3000.0}

    def test_foreign_accounts_and_clusters_are_dropped(self, handler, monkeypatch):
        handler.responses[f"GET /slurmdb/{API}/jobs/"] = envelope(
            jobs=[
                _job("other", "alice", 60, [{"type": "cpu", "count": 1}]),
                _job("acc1", "alice", 60, [{"type": "cpu", "count": 1}], cluster="elsewhere"),
                _job("acc1", "alice", 60, [{"type": "cpu", "count": 1}]),
            ]
        )
        client = self._client(handler, monkeypatch)
        assert len(client.get_usage_report(["acc1"])) == 1

    def test_tri_state_elapsed_and_missing_user_are_tolerated(self, handler, monkeypatch):
        handler.responses[f"GET /slurmdb/{API}/jobs/"] = envelope(
            jobs=[
                {
                    "account": "acc1",
                    "user": "alice",
                    "time": {"elapsed": {"set": True, "infinite": False, "number": 120}},
                    "tres": {"allocated": [{"type": "cpu", "count": 1}]},
                },
                {"account": "acc1", "time": {"elapsed": 5}, "tres": {}},
            ]
        )
        client = self._client(handler, monkeypatch)
        lines = client.get_usage_report(["acc1"])
        assert len(lines) == 1
        assert lines[0].duration == 2.0

    def test_job_straddling_the_window_is_truncated_like_sacct(self, handler, monkeypatch):
        """A 56h job across the month boundary bills 52h to May and 4h to June."""
        import datetime as dt

        boundary = int(dt.datetime(2026, 6, 1, 0, 0, 0, tzinfo=dt.timezone.utc).timestamp())
        job = _job(
            "acc1",
            "alice",
            elapsed=56 * 3600,
            tres=[{"type": "cpu", "count": 1}],
            start=boundary - 52 * 3600,
            end=boundary + 4 * 3600,
        )
        handler.responses[f"GET /slurmdb/{API}/jobs/"] = envelope(jobs=[job])
        client = self._client(handler, monkeypatch)

        # historical reports use naive local time; pin the process to UTC so
        # the window edges line up with the epoch boundary above.
        monkeypatch.setenv("TZ", "UTC")
        import time

        time.tzset()
        (may,) = client.get_historical_usage_report(["acc1"], 2026, 5)
        (june,) = client.get_historical_usage_report(["acc1"], 2026, 6)
        # The May window closes at 23:59:59 (the same edge sacct is given), so
        # its share is 52h less the final second; June opens at 00:00:00 and
        # gets its 4h exactly. Nothing is counted twice.
        assert may.duration == pytest.approx(52 * 60 - 1 / 60)
        assert june.duration == 4 * 60
        assert may.duration + june.duration == pytest.approx(56 * 60 - 1 / 60)

    def test_running_job_is_billed_to_the_window_end(self, handler, monkeypatch):
        import datetime as dt

        window_start = int(dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc).timestamp())
        job = _job(
            "acc1",
            "alice",
            elapsed=10 * 3600,
            tres=[{"type": "cpu", "count": 1}],
            start=window_start + 3600,
            end=0,  # still running per slurmdb
        )
        handler.responses[f"GET /slurmdb/{API}/jobs/"] = envelope(jobs=[job])
        client = self._client(handler, monkeypatch)
        monkeypatch.setenv("TZ", "UTC")
        import time

        time.tzset()
        (line,) = client.get_historical_usage_report(["acc1"], 2026, 5)
        # From start to the window's close: 31 days minus 1h (+ the 23:59:59 edge).
        assert line.duration == (31 * 24 * 60 - 60) - 1 / 60

    def test_job_without_start_keeps_raw_elapsed(self, handler, monkeypatch):
        handler.responses[f"GET /slurmdb/{API}/jobs/"] = envelope(
            jobs=[_job("acc1", "alice", 3600, [{"type": "cpu", "count": 1}])]
        )
        client = self._client(handler, monkeypatch)
        (line,) = client.get_usage_report(["acc1"])
        assert line.duration == 60.0

    def test_window_is_read_in_the_offering_timezone(self, handler, monkeypatch):
        client = self._client(handler, monkeypatch)
        start, end = client._window_epochs("2026-05-01T00:00:00", "2026-05-31T23:59:59", "UTC")
        import datetime as dt

        assert start == int(dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc).timestamp())
        assert end - start == 31 * 24 * 3600 - 1
        shifted, _ = client._window_epochs(
            "2026-05-01T00:00:00", "2026-05-31T23:59:59", "Europe/Tallinn"
        )
        assert start - shifted == 3 * 3600  # EEST is UTC+3 in May

    def test_historical_report_uses_the_given_month(self, handler, monkeypatch):
        client = self._client(handler, monkeypatch)
        client.get_historical_usage_report(["acc1"], 2026, 5)
        params = dict(handler.requests[0].url.params)
        assert params["start_time"].startswith("2026-05-01")
        assert params["end_time"].startswith("2026-05-31")

    def test_empty_account_list_makes_no_request(self, handler, monkeypatch):
        client = self._client(handler, monkeypatch)
        assert client.get_usage_report([]) == []
        assert handler.requests == []

    def test_no_sacct_is_run(self, handler, monkeypatch):
        client = self._client(handler, monkeypatch)
        client._cli = mock.Mock(spec=SlurmClient)
        client.get_usage_report(["acc1"])
        client._cli.get_usage_report.assert_not_called()


class TestUserExists:
    """check_user_exists asks slurmdbd, never the local id command."""

    def test_known_user(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/user/alice"] = envelope(
            users=[{"name": "alice", "associations": []}]
        )
        client._cli = mock.Mock(spec=SlurmClient)
        assert client.check_user_exists("alice") is True
        assert handler.requests[0].url.path == f"/slurmdb/{API}/user/alice"
        client._cli.check_user_exists.assert_not_called()

    def test_unknown_user_as_error_envelope(self, client, handler):
        """Real slurmrestd reports a missing user as an error, not an empty list."""
        handler.responses[f"GET /slurmdb/{API}/user/nobody"] = envelope(
            errors=[{"description": "Unable to find user nobody", "error_number": 2017}]
        )
        assert client.check_user_exists("nobody") is False

    def test_unknown_user_as_empty_list(self, client, handler):
        handler.responses[f"GET /slurmdb/{API}/user/nobody"] = envelope(users=[])
        assert client.check_user_exists("nobody") is False

    def test_username_is_url_quoted(self, client, handler):
        # The mock handler keys on the decoded path; the wire carries a%20b.
        handler.responses[f"GET /slurmdb/{API}/user/a b"] = envelope(users=[{"name": "a b"}])
        assert client.check_user_exists("a b") is True
        assert handler.requests[0].url.raw_path.endswith(b"/user/a%20b")


class TestCliDelegation:
    def test_raw_usage_resets_refuse_cleanly_without_sacctmgr(self, client):
        client._cli = mock.Mock(spec=SlurmClient)
        client._cli.slurm_bin_path = "/nonexistent/bin"
        with pytest.raises(BackendError, match="sacctmgr"):
            client.reset_raw_usage("acc1")
        with pytest.raises(BackendError, match="sacctmgr"):
            client.reset_qos_raw_usage("qos1")
        client._cli.reset_raw_usage.assert_not_called()
        client._cli.reset_qos_raw_usage.assert_not_called()

    def test_raw_usage_and_user_lookup_delegate_to_cli_client(self, client):
        client._cli = mock.Mock(spec=SlurmClient)
        client._cli.slurm_bin_path = ""
        with mock.patch("waldur_site_agent_slurm.rest_client.which", return_value="/usr/bin/x"):
            client.reset_raw_usage("acc1")
        client._cli.reset_raw_usage.assert_called_once_with("acc1")

    def test_executed_commands_includes_delegated_cli_commands(self, client, handler):
        # reset_raw_usage runs on the CLI client; its command must still show
        # up in the REST client's executed_commands so audit logs are complete.
        client.list_resources()  # one REST request
        client._cli.executed_commands.append("sacctmgr --immediate modify ... RawUsage=0")
        assert client.executed_commands == [
            f"GET /slurmdb/{API}/accounts/",
            "sacctmgr --immediate modify ... RawUsage=0",
        ]
        client.clear_executed_commands()
        assert client.executed_commands == []
        assert client._cli.executed_commands == []


class TestWalltimeParsing:
    @pytest.mark.parametrize(
        ("value", "minutes"),
        [
            ("180", 180),
            ("01:30:00", 90),
            ("30-00:00:00", 43200),
            ("00:00:90", 1),
            # Colon fields are right-aligned like SLURM: a 2-part value is
            # MM:SS, not HH:MM. "30:00" is 30 minutes (regression: the old
            # left-padding logic read it as 30 hours = 1800 minutes).
            ("30:00", 30),
            ("90:00", 90),
            # D-HH and D-HH:MM forms.
            ("2-12", 3600),
            ("1-02:30", 1590),
        ],
    )
    def test_walltime_formats(self, value, minutes):
        assert _parse_walltime_minutes(value) == minutes

    def test_invalid_walltime_raises(self):
        with pytest.raises(BackendError, match="Unsupported walltime format"):
            _parse_walltime_minutes("yesterday")
