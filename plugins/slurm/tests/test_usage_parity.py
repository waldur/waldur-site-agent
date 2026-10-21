"""CLI and REST mode must report the same usage for the same jobs.

Each case gives the CLI client what ``sacct --truncate --allocations`` prints
for a set of jobs, and the REST client the slurmdb job records slurmrestd
returns for them, then requires the same per-user usage from both. The sacct
semantics the REST client has to reproduce come from slurmdbd
(slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c):
usage time truncated to the window, the window capped at now, suspended time
taken off, and one record per job ID unless --duplicates is given.
"""

import datetime as dt
import time
from unittest import mock

import httpx
import pytest

from waldur_site_agent_slurm import rest_client as rest_client_module
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.rest_client import SlurmRestClient
from waldur_site_agent_slurm.schemas import UsageTresSource

API = "v0.0.43"
SLURM_TRES = {"cpu": {}, "mem": {}, "gres/gpu": {}, "gres/gpu:h200-141g": {}}

MAY_START = int(dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc).timestamp())
MAY_END = int(dt.datetime(2026, 5, 31, 23, 59, 59, tzinfo=dt.timezone.utc).timestamp())
HOUR = 3600

# Rocket job 1913980: an untyped GPU request on an H200 node, given twice the
# CPUs it asked for.
ROCKET_REQ_TRES = "billing=8,cpu=8,gres/gpu=1,mem=65536M,node=1"
ROCKET_ALLOC_TRES = "billing=16,cpu=16,gres/gpu:h200-141g=1,gres/gpu=1,mem=65536M,node=1"


def _tres_list(tres: str) -> list[dict]:
    """sacct TRES string -> the slurmdb TRES list slurmrestd returns for it."""
    result = []
    for pair in tres.split(","):
        key, value = pair.split("=", 1)
        entry: dict = {"count": int(value.rstrip("M"))}
        if "/" in key:
            entry["type"], entry["name"] = key.split("/", 1)
        else:
            entry["type"] = key
        result.append(entry)
    return result


def _record(  # noqa: PLR0913 - mirrors the slurmdb job record fields
    job_id,
    *,
    start,
    end,
    elapsed,
    req=ROCKET_REQ_TRES,
    alloc=ROCKET_ALLOC_TRES,
    submitted=None,
    state="COMPLETED",
    user="alice",
):
    return {
        "job_id": job_id,
        "account": "acc1",
        "user": user,
        "cluster": "testcluster",
        "state": {"current": [state]},
        "time": {
            "submission": submitted if submitted is not None else start,
            "start": start,
            "end": end,
            "elapsed": elapsed,
        },
        "tres": {"requested": _tres_list(req), "allocated": _tres_list(alloc) if alloc else []},
    }


def _elapsed(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _sacct_line(source, elapsed, req=ROCKET_REQ_TRES, alloc=ROCKET_ALLOC_TRES, user="alice"):
    """A line as sacct prints it for the --format the CLI client asks for."""
    if source is UsageTresSource.ALLOCATED:
        return f"acc1|{alloc}|{req}|{_elapsed(elapsed)}|{user}"
    return f"acc1|{req}|{_elapsed(elapsed)}|{user}"


@pytest.fixture(autouse=True)
def utc(monkeypatch):
    """Historical windows are naive local time; pin it to UTC for the epochs above."""
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


@pytest.fixture
def now(monkeypatch):
    """Freeze the REST client's clock; default after May has closed."""
    clock = {"now": MAY_END + 30 * 24 * HOUR}
    monkeypatch.setattr(rest_client_module.time, "time", lambda: clock["now"])
    return clock


def _cli_usage(source, sacct_lines):
    client = SlurmClient(SLURM_TRES, slurm_bin_path="", usage_tres_source=source)
    with mock.patch.object(client, "execute_command", return_value="\n".join(sacct_lines)):
        lines = client.get_historical_usage_report(["acc1"], 2026, 5)
    return client, lines


def _rest_usage(source, records, monkeypatch):
    monkeypatch.setenv("SLURM_JWT", "test-token")
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"meta": {}, "errors": [], "warnings": [], "jobs": records})

    client = SlurmRestClient(
        SLURM_TRES,
        rest_settings={
            "url": "http://localhost:6820",
            "api_version": API,
            "username": "waldur-agent",
            "token_env": "SLURM_JWT",
        },
        cluster_name="testcluster",
        transport=httpx.MockTransport(handler),
        usage_tres_source=source,
    )
    return client.get_historical_usage_report(["acc1"], 2026, 5), requests


def _usage(lines):
    totals: dict = {}
    for line in lines:
        user = totals.setdefault(line.user, {})
        for tres, value in line.tres_usage.items():
            user[tres] = user.get(tres, 0) + value
    return totals


def _assert_parity(source, sacct_lines, records, monkeypatch):
    _, cli_lines = _cli_usage(source, sacct_lines)
    rest_lines, _ = _rest_usage(source, records, monkeypatch)
    assert _usage(rest_lines) == _usage(cli_lines)
    return _usage(cli_lines)


@pytest.mark.parametrize("source", list(UsageTresSource))
class TestUsageMatchesSacct:
    def test_finished_job(self, source, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        records = [_record(1, start=start, end=start + 2 * HOUR, elapsed=2 * HOUR)]
        _assert_parity(source, [_sacct_line(source, 2 * HOUR)], records, monkeypatch)

    def test_job_running_when_the_report_runs_is_billed_up_to_now(
        self, source, now, monkeypatch
    ):
        start = MAY_START + 10 * HOUR
        now["now"] = start + 5 * HOUR
        # Untruncated slurmdb record of a running job: no end, elapsed so far.
        records = [_record(1, start=start, end=0, elapsed=5 * HOUR, state="RUNNING")]
        usage = _assert_parity(source, [_sacct_line(source, 5 * HOUR)], records, monkeypatch)
        cpus = 16 if source is UsageTresSource.ALLOCATED else 8
        assert usage["alice"]["cpu"] == cpus * 5 * 60

    def test_suspended_time_is_not_billed(self, source, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        # Ran for 4h of wall time, 1h of it suspended; slurmdbd reports 3h.
        records = [_record(1, start=start, end=start + 4 * HOUR, elapsed=3 * HOUR)]
        _assert_parity(source, [_sacct_line(source, 3 * HOUR)], records, monkeypatch)

    def test_job_requeued_within_the_month_bills_its_last_run(self, source, now, monkeypatch):
        first = MAY_START + 10 * HOUR
        second = first + 12 * HOUR
        records = [
            _record(7, start=first, end=first + 10 * HOUR, elapsed=10 * HOUR, state="NODE_FAIL"),
            _record(7, start=second, end=second + 12 * HOUR, elapsed=12 * HOUR, submitted=second),
        ]
        # sacct without --duplicates prints only the most recent record.
        _assert_parity(source, [_sacct_line(source, 12 * HOUR)], records, monkeypatch)

    def test_job_spanning_the_month_start_bills_its_may_part(self, source, now, monkeypatch):
        start = MAY_START - 6 * HOUR
        records = [_record(1, start=start, end=MAY_START + 2 * HOUR, elapsed=8 * HOUR)]
        _assert_parity(source, [_sacct_line(source, 2 * HOUR)], records, monkeypatch)


class TestTresSource:
    def test_requested_is_the_default(self):
        assert SlurmClient({}).usage_tres_source is UsageTresSource.REQUESTED
        backend = SlurmBackend(
            {
                "default_account": "root",
                "customer_prefix": "c_",
                "project_prefix": "p_",
                "allocation_prefix": "a_",
            },
            {},
        )
        assert backend.client.usage_tres_source is UsageTresSource.REQUESTED

    def test_requested_keeps_untyped_gpu_generic(self, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        records = [_record(1, start=start, end=start + HOUR, elapsed=HOUR)]
        usage = _assert_parity(
            UsageTresSource.REQUESTED,
            [_sacct_line(UsageTresSource.REQUESTED, HOUR)],
            records,
            monkeypatch,
        )
        assert usage["alice"] == {"cpu": 8 * 60, "mem": 65536 * 60, "gres/gpu": 60}

    def test_allocated_names_the_gpu_type_and_bills_allocated_cpus(self, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        records = [_record(1, start=start, end=start + HOUR, elapsed=HOUR)]
        usage = _assert_parity(
            UsageTresSource.ALLOCATED,
            [_sacct_line(UsageTresSource.ALLOCATED, HOUR)],
            records,
            monkeypatch,
        )
        # Both the generic total and the typed count: an offering must not
        # configure both as components, or the GPU is billed twice.
        assert usage["alice"] == {
            "cpu": 16 * 60,
            "mem": 65536 * 60,
            "gres/gpu": 60,
            "gres/gpu:h200-141g": 60,
        }

    def test_allocated_falls_back_to_requested_when_nothing_was_allocated(
        self, now, monkeypatch
    ):
        start = MAY_START + 10 * HOUR
        records = [_record(1, start=start, end=start + HOUR, elapsed=HOUR, alloc="")]
        usage = _assert_parity(
            UsageTresSource.ALLOCATED,
            [_sacct_line(UsageTresSource.ALLOCATED, HOUR, alloc="")],
            records,
            monkeypatch,
        )
        assert usage["alice"]["cpu"] == 8 * 60

    def test_cli_requested_runs_the_same_sacct_as_before(self):
        client, _ = _cli_usage(UsageTresSource.REQUESTED, [])
        assert "--format=Account,ReqTRES,Elapsed,User" in client.executed_commands[0]

    def test_cli_allocated_asks_sacct_for_both_tres(self):
        client, _ = _cli_usage(UsageTresSource.ALLOCATED, [])
        assert "--format=Account,AllocTRES,ReqTRES,Elapsed,User" in client.executed_commands[0]


class TestRestRecordSelection:
    """slurmdbd's duplicate rules, repeated client-side for a server that ignores the flag."""

    def test_records_of_different_jobs_are_all_billed(self, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        records = [
            _record(1, start=start, end=start + HOUR, elapsed=HOUR),
            _record(2, start=start, end=start + HOUR, elapsed=HOUR),
        ]
        lines, _ = _rest_usage(UsageTresSource.REQUESTED, records, monkeypatch)
        assert len(lines) == 2

    def test_resizing_records_are_kept(self, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        records = [
            _record(3, start=start, end=start + HOUR, elapsed=HOUR, state="RESIZING"),
            _record(3, start=start + HOUR, end=start + 2 * HOUR, elapsed=HOUR, submitted=start + HOUR),
        ]
        lines, _ = _rest_usage(UsageTresSource.REQUESTED, records, monkeypatch)
        assert len(lines) == 2

    def test_revoked_federated_siblings_are_dropped(self, now, monkeypatch):
        start = MAY_START + 10 * HOUR
        records = [_record(4, start=start, end=start + HOUR, elapsed=HOUR, state="REVOKED")]
        lines, _ = _rest_usage(UsageTresSource.REQUESTED, records, monkeypatch)
        assert lines == []

    def test_query_lets_slurmdbd_truncate_and_deduplicate(self, now, monkeypatch):
        _, requests = _rest_usage(UsageTresSource.REQUESTED, [], monkeypatch)
        params = dict(requests[0].url.params)
        assert params["skip_steps"] == "true"
        assert "show_duplicates" not in params
        assert "disable_truncate_usage_time" not in params
        assert params["start_time"] == str(MAY_START)
        assert params["end_time"] == str(MAY_END)
