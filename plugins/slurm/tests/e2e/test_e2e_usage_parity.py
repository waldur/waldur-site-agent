"""E2E: CLI and REST mode report the same usage, against slurm-emulator.

One emulator state backs both planes: ``sacct`` (what ``SlurmClient`` runs)
and the slurmrestd app (what ``SlurmRestClient`` talks to). A month of jobs
is replayed on the emulator's simulated clock -- jobs that finish, one still
running, one suspended twice and resumed once, one requeued while running,
one held -- and both clients' historical usage reports for that month must
agree with each other and with the usage sacct --truncate implies.

The emulator's job query is checked against a real Slurm 25.11.7 run in
its own repo (slurm-emulator tests/test_jobacct_golden.py); this suite checks
that the agent's two clients read it the same way.

Environment variables:
    WALDUR_E2E_TESTS=true   - Gate: skip all if not set
"""

from __future__ import annotations

import importlib.util
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.rest_client import SlurmRestClient
from waldur_site_agent_slurm.schemas import UsageTresSource

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
pytestmark = [
    pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set"),
    pytest.mark.skipif(
        importlib.util.find_spec("emulator.core.jobacct") is None,
        reason="slurm-emulator without slurmdbd job-query emulation (waldur/slurm-emulator!32)",
    ),
]

EMULATED_SLURM_RELEASE = "25.05"
API_VERSION = "v0.0.43"
CLUSTER = "default"
ACCOUNT = "usage_parity"
TOKEN_ENV = "SLURM_E2E_USAGE_TOKEN"
TRES = {"cpu": {}, "mem": {}}
# A Sunday in May; the report covers the whole month.
T0 = datetime(2026, 5, 10, 9, 0, 0)
QUERY_AT = T0 + timedelta(seconds=60)

# name, user, run seconds, extra job fields
JOBS = [
    ("first", "alice", 20, {}),
    ("second", "alice", 10, {}),
    ("running", "alice", 3000, {}),
    ("suspended", "bob", 3000, {}),
    ("requeued", "bob", 3000, {}),
    ("held", "bob", 30, {"hold": True}),
]
# Seconds after T0, as in the emulator's golden scenario.
EVENTS = [(10, "suspend", "suspended"), (25, "resume", "suspended"), (30, "requeue", "requeued")]
EVENTS.append((40, "suspend", "suspended"))
# What sacct --truncate reports at QUERY_AT: jobs start 1 s after submit; the
# suspended job loses its closed 15 s interval (its open one is not taken
# off); the requeued job's first run is superseded by its pending record and
# the held job is never eligible.
EXPECTED_SECONDS = {"alice": 20 + 10 + 59, "bob": 59 - 15}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def emulator_env(tmp_path_factory):
    """Environment shared by the CLI wrappers, the REST app and this process."""
    root = tmp_path_factory.mktemp("usage-parity")
    env = {
        "SLURM_EMULATOR_STATE_FILE": str(root / "state.json"),
        "SLURM_EMULATOR_TIME_FILE": str(root / "time.json"),
        "SLURM_EMULATOR_SLURM_VERSION": EMULATED_SLURM_RELEASE,
        "SLURM_EMULATOR_JOB_CLOCK": "time",
        "SLURM_EMULATOR_JOB_RUN_DELAY": "1",
        "SLURM_EMULATOR_ACCOUNTING_ENFORCE": "none",
    }
    saved = {key: os.environ.get(key) for key in env}
    os.environ.update(env)
    bin_dir = root / "bin"
    bin_dir.mkdir()
    # The commands SlurmClient runs, whatever the emulator's install layout.
    for command, entry in (("sacct", "sacct_main"), ("sacctmgr", "sacctmgr_main")):
        script = bin_dir / command
        script.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" -c '
            f'"import sys; sys.argv[0] = \'{command}\'; '
            f'from emulator.commands.dispatcher import {entry}; {entry}()" "$@"\n'
        )
        script.chmod(0o755)
    yield {"env": env, "bin": str(bin_dir)}
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture(scope="module")
def slurmrestd_url(emulator_env):
    from emulator.core.time_engine import TimeEngine  # noqa: PLC0415

    TimeEngine().set_time(T0)
    port = _free_port()
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
        env={**os.environ, **emulator_env["env"]},
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
def month_of_jobs(slurmrestd_url, emulator_env):
    """Submit the jobs at T0 and replay the suspend/resume/requeue events."""
    from emulator.commands.scontrol import ScontrolEmulator  # noqa: PLC0415
    from emulator.core.database import SlurmDatabase  # noqa: PLC0415
    from emulator.core.time_engine import TimeEngine  # noqa: PLC0415

    ids = {}
    for name, user, seconds, extra in JOBS:
        response = httpx.post(
            f"{slurmrestd_url}/slurm/{API_VERSION}/job/submit",
            headers={"X-SLURM-USER-TOKEN": "e2e", "X-SLURM-USER-NAME": user},
            json={
                "job": {
                    "name": name,
                    "account": ACCOUNT,
                    "partition": "debug",
                    "time_limit": 60,
                    "current_working_directory": "/tmp",  # noqa: S108
                    "environment": [f"SLURM_EMULATOR_RUN_SECONDS={seconds}"],
                    "script": "#!/bin/bash\ntrue",
                    **extra,
                }
            },
            timeout=10,
        ).json()
        assert response["errors"] == [], response["errors"]
        ids[name] = response["job_id"]
    for offset, command, job in EVENTS:
        TimeEngine().set_time(T0 + timedelta(seconds=offset))
        scontrol = ScontrolEmulator(SlurmDatabase(), TimeEngine())
        scontrol.handle_command([command, str(ids[job])])
        assert scontrol.exit_code == 0, f"scontrol {command} {job}"
    TimeEngine().set_time(QUERY_AT)
    return ids


def _clients(source, slurmrestd_url, bin_dir):
    os.environ[TOKEN_ENV] = "e2e-token"
    cli = SlurmClient(TRES, slurm_bin_path=bin_dir, cluster_name=CLUSTER, usage_tres_source=source)
    rest = SlurmRestClient(
        TRES,
        rest_settings={
            "url": slurmrestd_url,
            "api_version": API_VERSION,
            "username": "root",
            "token_env": TOKEN_ENV,
        },
        cluster_name=CLUSTER,
        slurm_bin_path=bin_dir,
        usage_tres_source=source,
    )
    return cli, rest


def _per_user(lines) -> dict[str, dict[str, float]]:
    totals: dict[str, dict[str, float]] = {}
    for line in lines:
        user = totals.setdefault(line.user, {})
        for tres, value in line.tres_usage.items():
            user[tres] = user.get(tres, 0) + value
    return totals


@pytest.mark.parametrize("source", list(UsageTresSource))
def test_cli_and_rest_report_the_same_month(source, month_of_jobs, slurmrestd_url, emulator_env):
    cli, rest = _clients(source, slurmrestd_url, emulator_env["bin"])
    cli_usage = _per_user(cli.get_historical_usage_report([ACCOUNT], T0.year, T0.month))
    rest_usage = _per_user(rest.get_historical_usage_report([ACCOUNT], T0.year, T0.month))
    assert rest_usage == cli_usage
    # One CPU per job; usage is CPU-minutes.
    assert {user: round(usage["cpu"] * 60) for user, usage in cli_usage.items()} == EXPECTED_SECONDS


def test_requeued_run_is_dropped_but_reported_with_duplicates(month_of_jobs, emulator_env):
    """sacct -D shows the requeued run the agent's query leaves out."""
    sacct = Path(emulator_env["bin"]) / "sacct"
    out = subprocess.check_output(  # noqa: S603
        [
            str(sacct),
            "-X",
            "-P",
            "-n",
            "-D",
            "--truncate",
            f"-j{month_of_jobs['requeued']}",
            "-S",
            T0.isoformat(),
            "-E",
            QUERY_AT.isoformat(),
            "--format=State,ElapsedRaw",
        ],
        text=True,
        env={**os.environ, **emulator_env["env"]},
    )
    assert out.splitlines() == ["REQUEUED|29", "PENDING|0"]
