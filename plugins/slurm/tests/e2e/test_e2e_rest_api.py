"""E2E tests for SlurmRestClient against slurm-emulator's slurmrestd API.

slurm-emulator >= 0.7.0 ships a slurmrestd-compatible FastAPI app
(``emulator.api.slurmrestd.app``) serving the ``v0.0.46`` data_parser
schema. This suite starts it as a subprocess with an isolated state
file and drives the real ``SlurmRestClient`` against it over HTTP —
no Waldur instance is needed.

The emulator's account-level associations live on its *current cluster*
("default" unless changed), so the client is configured with
``cluster_name="default"``.

slurm-emulator >= 0.7.1 implements the real slurmrestd request shapes
(association_condition for accounts_association/users_association, and
qos/default-qos/shares_raw on account-level associations);
TestRealRequestShapes exercises the client's production request shapes
directly against them.

Environment variables:
    WALDUR_E2E_TESTS=true   - Gate: skip all if not set
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_slurm.rest_client import SlurmRestClient

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")

API_VERSION = "v0.0.46"  # the only version slurm-emulator serves
# The emulator's add_account() places account-level associations on its
# current cluster, which is "default" out of the box.
CLUSTER = "default"
TOKEN_ENV = "SLURM_E2E_REST_TOKEN"

@pytest.fixture(scope="module")
def slurmrestd_url(tmp_path_factory):
    """Start the slurm-emulator slurmrestd app with isolated state.

    The required emulator (and its uvicorn/fastapi deps) is hard-pinned in
    pyproject (slurm-emulator>=0.7.2, dev group), so it is always present
    when this suite can run; a missing/broken emulator surfaces loudly as a
    subprocess-startup failure below rather than a silent skip.
    """
    state_file = tmp_path_factory.mktemp("slurmrestd-emulator") / "state.json"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, "SLURM_EMULATOR_STATE_FILE": str(state_file)}
    proc = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "uvicorn",
            "emulator.api.slurmrestd.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
    )
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"slurmrestd emulator exited with code {proc.returncode}")
        try:
            response = httpx.get(
                f"{url}/slurm/{API_VERSION}/ping/",
                headers={"X-SLURM-USER-TOKEN": "readiness-probe"},
                timeout=2,
            )
            if response.status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    else:
        proc.terminate()
        pytest.fail("slurmrestd emulator did not become ready within 30s")
    yield url
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="module")
def rest_client(slurmrestd_url):
    """SlurmRestClient pointed at the emulator."""
    os.environ[TOKEN_ENV] = "e2e-token"
    return SlurmRestClient(
        slurm_tres={},
        rest_settings={
            "url": slurmrestd_url,
            "api_version": API_VERSION,
            "username": "root",
            "token_env": TOKEN_ENV,
        },
        cluster_name=CLUSTER,
    )


def seed_account(client: SlurmRestClient, name: str, parent: str = "root") -> None:
    """Create an account through POST /accounts/.

    Lifecycle tests seed accounts with the plain accounts collection
    POST (carrying the emulator-specific ``parent_account`` field) so
    they stay independent from the accounts_association code path,
    which TestRealRequestShapes covers explicitly.
    """
    client._request(
        "POST",
        client._db("accounts/"),
        body={
            "accounts": [
                {
                    "name": name,
                    "description": f"{name} description",
                    "organization": f"{name}-org",
                    "parent_account": parent,
                }
            ]
        },
    )


def seed_user_association(client: SlurmRestClient, user: str, account: str) -> None:
    """Create a user association through POST /associations/.

    POST /associations/ creates the user and the association in both
    the emulator and real slurmdbd; the users_association code path is
    covered explicitly by TestRealRequestShapes.
    """
    client._request(
        "POST",
        client._db("associations/"),
        body={"associations": [{"account": account, "user": user, "cluster": CLUSTER}]},
    )


class TestDiagnostics:
    def test_get_version(self, rest_client):
        version = rest_client.get_version()
        assert version.startswith("slurm ")

    def test_validate_backend(self, rest_client):
        assert rest_client.validate_slurm_binary() is True

    def test_list_tres(self, rest_client):
        tres = rest_client.list_tres()
        assert "cpu" in tres
        assert "mem" in tres

    def test_create_and_list_clusters(self, rest_client):
        rest_client._request(
            "POST", rest_client._db("clusters/"), body={"clusters": [{"name": "e2ecluster"}]}
        )
        assert "e2ecluster" in rest_client.list_clusters()


class TestAccountLifecycle:
    """Account CRUD, hierarchy and limits against the live emulator."""

    def test_account_create_get_list(self, rest_client):
        seed_account(rest_client, "e2e_customer")
        seed_account(rest_client, "e2e_project", parent="e2e_customer")

        account = rest_client.get_resource("e2e_project")
        assert account is not None
        assert account.name == "e2e_project"
        assert account.description == "e2e_project description"
        assert account.organization == "e2e_project-org"
        names = [resource.name for resource in rest_client.list_resources()]
        assert "e2e_customer" in names
        assert "e2e_project" in names

    def test_get_missing_account_returns_none(self, rest_client):
        assert rest_client.get_resource("e2e_nonexistent") is None

    def test_account_parent_roundtrip(self, rest_client):
        assert rest_client.get_account_parent("e2e_project") == "e2e_customer"
        seed_account(rest_client, "e2e_customer2")
        rest_client.set_account_parent("e2e_project", "e2e_customer2")
        assert rest_client.get_account_parent("e2e_project") == "e2e_customer2"

    def test_resource_limits_roundtrip(self, rest_client):
        rest_client.set_resource_limits("e2e_project", {"cpu": 60000, "mem": 61440})
        limits = rest_client.get_resource_limits("e2e_project")
        assert limits.get("cpu") == 60000
        assert limits.get("mem") == 61440

    def test_account_limits_via_limit_type(self, rest_client):
        rest_client.set_account_limits("e2e_project", "GrpTRES", {"cpu": 512})
        limits = rest_client.get_account_limits("e2e_project")
        assert limits["GrpTRES"].get("cpu") == "512"

    def test_delete_account(self, rest_client):
        seed_account(rest_client, "e2e_doomed")
        assert rest_client.get_resource("e2e_doomed") is not None
        rest_client.delete_resource("e2e_doomed")
        assert rest_client.get_resource("e2e_doomed") is None


class TestAssociations:
    def test_association_listing_and_lookup(self, rest_client):
        seed_account(rest_client, "e2e_assoc_acc")
        seed_user_association(rest_client, "e2e_user1", "e2e_assoc_acc")
        seed_user_association(rest_client, "e2e_user2", "e2e_assoc_acc")

        users = rest_client.list_resource_users("e2e_assoc_acc")
        assert sorted(users) == ["e2e_user1", "e2e_user2"]
        assert rest_client.account_has_users("e2e_assoc_acc") is True

        association = rest_client.get_association("e2e_user1", "e2e_assoc_acc")
        assert association is not None
        assert association.account == "e2e_assoc_acc"
        assert association.user == "e2e_user1"
        assert rest_client.get_association("e2e_ghost", "e2e_assoc_acc") is None

    def test_user_limits_roundtrip(self, rest_client):
        rest_client.set_resource_user_limits("e2e_assoc_acc", "e2e_user1", {"cpu": 500})
        user_limits = rest_client.get_resource_user_limits("e2e_assoc_acc")
        assert user_limits["e2e_user1"]["cpu"] == 500

    def test_delete_association(self, rest_client):
        rest_client.delete_association("e2e_user2", "e2e_assoc_acc")
        assert rest_client.list_resource_users("e2e_assoc_acc") == ["e2e_user1"]

    def test_delete_all_users(self, rest_client):
        rest_client.delete_all_users_from_account("e2e_assoc_acc")
        assert rest_client.list_resource_users("e2e_assoc_acc") == []
        assert rest_client.account_has_users("e2e_assoc_acc") is False


class TestQos:
    def test_qos_create_exists_delete(self, rest_client):
        assert rest_client.qos_exists("e2e_qos") is False
        rest_client.create_qos(
            "e2e_qos",
            flags="DenyOnLimit,NoDecay",
            grp_tres="cpu=25600,node=100",
            max_jobs=100,
            max_submit=200,
            max_wall="30-00:00:00",
            min_tres_per_job="gres/gpu=1",
        )
        assert rest_client.qos_exists("e2e_qos") is True
        rest_client.delete_qos("e2e_qos")
        assert rest_client.qos_exists("e2e_qos") is False

    def test_account_qos_read(self, rest_client):
        seed_account(rest_client, "e2e_qos_acc")
        # The emulator reports its account QoS through the association
        # payload; reading must not raise even when unset.
        qos = rest_client.get_current_account_qos("e2e_qos_acc")
        assert isinstance(qos, str)


class TestJobs:
    def test_list_and_cancel_jobs_empty(self, rest_client):
        assert rest_client.list_active_user_jobs("e2e_assoc_acc", "e2e_user1") == []
        # Must be a no-op without raising when there is nothing to cancel.
        rest_client.cancel_active_user_jobs("e2e_assoc_acc")


class TestErrorHandling:
    def test_unsupported_api_version_raises(self, slurmrestd_url):
        os.environ[TOKEN_ENV] = "e2e-token"
        client = SlurmRestClient(
            slurm_tres={},
            rest_settings={
                "url": slurmrestd_url,
                "api_version": "v0.0.40",
                "username": "root",
                "token_env": TOKEN_ENV,
            },
            cluster_name=CLUSTER,
        )
        with pytest.raises(BackendError):
            client.list_resources()

    def test_missing_token_is_rejected(self, slurmrestd_url, monkeypatch):
        monkeypatch.setenv(TOKEN_ENV, "")
        client = SlurmRestClient(
            slurm_tres={},
            rest_settings={
                "url": slurmrestd_url,
                "api_version": API_VERSION,
                "username": "root",
                "token_env": TOKEN_ENV,
            },
            cluster_name=CLUSTER,
        )
        with pytest.raises(BackendError, match="No slurmrestd token available"):
            client.list_resources()


class TestRealRequestShapes:
    """The client's production request shapes against the live emulator.

    These exercise ``accounts_association`` / ``users_association`` with
    the ``association_condition`` shape and the account-level association
    writes (qos/shares_raw), exactly as real slurmrestd parses them
    (data_parser parsers.c). Requires slurm-emulator >= 0.7.1.
    """

    def test_create_resource_via_accounts_association(self, rest_client):
        rest_client.create_resource(
            "e2e_real_shape_acc", "Real shape", "e2e-org", parent_name="root"
        )
        assert rest_client.get_resource("e2e_real_shape_acc") is not None

    def test_create_association_via_users_association(self, rest_client):
        seed_account(rest_client, "e2e_real_shape_assoc")
        rest_client.create_association("e2e_real_user", "e2e_real_shape_assoc", "e2e_default")
        assert rest_client.get_association("e2e_real_user", "e2e_real_shape_assoc") is not None

    def test_account_qos_and_fairshare_writes(self, rest_client):
        seed_account(rest_client, "e2e_parity_acc")
        rest_client.set_account_qos("e2e_parity_acc", "normal")
        rest_client.set_account_fairshare("e2e_parity_acc", 42)
        assert rest_client.get_account_fairshare("e2e_parity_acc") == 42
