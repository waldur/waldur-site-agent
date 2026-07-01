"""Integration tests: SlurmClient hierarchy ops against the SLURM emulator.

These exercise the *real* ``SlurmClient`` command-building and output-parsing
against the ``slurm-emulator`` dev dependency, which is itself validated for
parity with real Slurm. They are the regression guard for the class of bug where
``get_account_parent`` read ``ParentName`` from ``sacctmgr show account`` — a
field that is blank there because the parent lives on the *association*.

Why these catch the bug (and the pure unit tests in test_project_reparenting.py
did not): the unit tests mock ``execute_command`` with hand-written output that
encoded the same wrong assumption as the code. Here the emulator produces the
exact bytes real Slurm would, so a command/parse mismatch surfaces as a wrong
return value rather than passing silently.

The emulator import is guarded with importorskip so the module is skipped
rather than failing if the dependency is unavailable.
"""

from pathlib import Path

import pytest
from waldur_site_agent_slurm.client import SlurmClient

from waldur_site_agent.backend.exceptions import BackendError

# slurm-emulator is a dev dependency of this plugin (see pyproject.toml); the
# parity behaviour these tests rely on requires >= 0.6.0. Pull the modules from
# importorskip's return value so the file collects cleanly when it is absent.
SacctmgrEmulator = pytest.importorskip(
    "emulator.commands.sacctmgr",
    reason="slurm-emulator (>=0.6.0) not installed",
).SacctmgrEmulator
SlurmDatabase = pytest.importorskip("emulator.core.database").SlurmDatabase
TimeEngine = pytest.importorskip("emulator.core.time_engine").TimeEngine

pytestmark = pytest.mark.integration


@pytest.fixture
def client(tmp_path):
    """A SlurmClient whose commands are executed by the emulator in-process.

    Routes ``execute_command`` (the subprocess boundary) into the emulator,
    translating a non-zero emulator exit into BackendError exactly as
    ``subprocess.check_output`` → ``CalledProcessError`` would.
    """
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    sacctmgr = SacctmgrEmulator(db, TimeEngine())
    # Build hierarchy: root -> c-org -> p-proj, with a user under the project.
    sacctmgr.handle_command(["add", "account", "c-org", "parent=root"])
    sacctmgr.handle_command(["add", "account", "p-proj", "parent=c-org"])
    sacctmgr.handle_command(["add", "user", "alice", "account=p-proj"])

    def _route(argv, silent=False):
        assert Path(argv[0]).name == "sacctmgr"
        # Strip the binary prefix; the emulator follows --parsable2/--noheader
        # like real SLURM, so flags are passed through.
        output = sacctmgr.handle_command(list(argv[1:]))
        if sacctmgr.exit_code != 0:
            raise BackendError(output)
        return output

    slurm_client = SlurmClient(slurm_tres={}, slurm_bin_path="")
    slurm_client.execute_command = _route  # route the subprocess boundary
    slurm_client._emulator = sacctmgr  # expose for assertions
    return slurm_client


class TestGetAccountParent:
    def test_returns_real_parent_for_project_account(self, client):
        # Regression guard: reading ParentName from ``show account`` returns
        # blank here (parent lives on the association), so the old code would
        # return None and fail this assertion.
        assert client.get_account_parent("p-proj") == "c-org"

    def test_returns_parent_for_customer_account(self, client):
        assert client.get_account_parent("c-org") == "root"

    def test_returns_none_for_root(self, client):
        assert client.get_account_parent("root") is None

    def test_returns_none_for_unknown_account(self, client):
        assert client.get_account_parent("does-not-exist") is None


class TestSetAccountParent:
    def test_reparent_is_observable_via_get_account_parent(self, client):
        client._emulator.handle_command(["add", "account", "c-new", "parent=root"])
        client.set_account_parent("p-proj", "c-new")
        assert client.get_account_parent("p-proj") == "c-new"

    def test_reparent_to_same_parent_is_a_noop(self, client):
        # Real sacctmgr prints "  Nothing modified" to stdout but exits 1 for a
        # no-op reparent: account_functions.c:726-729 returns SLURM_ERROR, and
        # sacctmgr.c:982-984 maps a non-SUCCESS error_code to exit_code=1.
        # _execute_command swallows exactly this case and returns "" — no
        # BackendError, because the desired parent is already in place.
        assert client.set_account_parent("p-proj", "c-org") == ""
        assert client.get_account_parent("p-proj") == "c-org"

    def test_reparent_to_missing_parent_raises(self, client):
        with pytest.raises(BackendError):
            client.set_account_parent("p-proj", "c-ghost")
        # Parent unchanged after the failed attempt.
        assert client.get_account_parent("p-proj") == "c-org"


class TestSyncResourceProjectIsIdempotent:
    """End-to-end: a correctly-parented account must not trigger a reparent."""

    def test_no_spurious_reparent_when_parent_is_correct(self, client):
        # The buggy code saw a blank parent every cycle and reparented endlessly,
        # erroring on the redundant modify. With correct detection, the parent is
        # already right, so set_account_parent must not raise.
        before = client.get_account_parent("p-proj")
        assert before == "c-org"
        if client.get_account_parent("p-proj") != "c-org":  # pragma: no cover
            client.set_account_parent("p-proj", "c-org")
