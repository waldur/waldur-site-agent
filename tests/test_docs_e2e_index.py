"""Keep docs/e2e-testing.md in step with the E2E suites and CI jobs.

The page lists every E2E test file, the CI job that runs it and the config
variable it reads. Those lists drifted silently before; these tests fail when a
file, job or variable is added, moved or renamed without updating the page.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
DOC = (REPO / "docs" / "e2e-testing.md").read_text(encoding="utf-8")
E2E_FILES = sorted(REPO.glob("plugins/*/tests/e2e/test_e2e_*.py"))

# Short job names used in the page's "Job" column -> CI job name.
JOB_ABBREVIATIONS = {
    "REST": "E2E: REST & STOMP",
    "policy": "E2E: policy, QoS & LDAP",
    "matrix": "E2E: QoS matrix",
}


def _e2e_jobs() -> dict[str, str]:
    """Concrete E2E CI jobs and their script text."""
    ci = yaml.safe_load((REPO / ".gitlab-ci.yml").read_text(encoding="utf-8"))
    return {
        name: "\n".join(str(line) for line in job.get("script") or [])
        for name, job in ci.items()
        if isinstance(job, dict)
        and not name.startswith(".")
        and job.get("extends") == ".E2E base"
    }


def _jobs_running(path: Path, jobs: dict[str, str]) -> list[str]:
    rel = str(path.relative_to(REPO))
    return [name for name, script in jobs.items() if rel in script]


def _section(plugin_dir: str) -> str:
    """Body of the '### … (`plugins/<dir>/tests/e2e/`)' section, up to the next heading."""
    heading = re.search(
        rf"^### [^\n]*`plugins/{re.escape(plugin_dir)}/tests/e2e/`[^\n]*$", DOC, re.MULTILINE
    )
    assert heading, (
        f"docs/e2e-testing.md has no '### … (`plugins/{plugin_dir}/tests/e2e/`)' section"
    )
    end = re.search(r"^#{2,3} ", DOC[heading.end() :], re.MULTILINE)
    return DOC[heading.end() : heading.end() + end.start() if end else len(DOC)]


def _row(path: Path) -> list[str]:
    """Cells of the table row documenting this file in its plugin's section."""
    for line in _section(path.parts[-4]).splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0] == f"`{path.name}`":
            return cells
    raise AssertionError(f"{path.relative_to(REPO)} has no row in docs/e2e-testing.md")


def test_e2e_jobs_and_files_are_found():
    assert E2E_FILES, "no plugins/*/tests/e2e/test_e2e_*.py files found"
    assert _e2e_jobs(), "no CI jobs extending '.E2E base' found in .gitlab-ci.yml"


@pytest.mark.parametrize("path", E2E_FILES, ids=lambda p: f"{p.parts[-4]}/{p.name}")
def test_every_e2e_file_is_documented_with_its_ci_job(path):
    jobs = _e2e_jobs()
    running = _jobs_running(path, jobs)
    assert len(running) <= 1, f"{path.relative_to(REPO)} runs in several jobs: {running}"
    section = _section(path.parts[-4])
    row = _row(path)
    if not running:
        # Only a suite the page declares as manual may stay out of CI.
        assert "not wired into CI" in section, (
            f"{path.relative_to(REPO)} is in no CI job, and its docs section "
            "does not say the suite is manual"
        )
        return
    job_cell = row[2]
    documented = JOB_ABBREVIATIONS.get(job_cell, job_cell.strip("`"))
    assert documented == running[0], (
        f"{path.name}: docs say job {job_cell!r}, CI runs it in {running[0]!r}"
    )


@pytest.mark.parametrize("job", sorted(_e2e_jobs()))
def test_every_e2e_ci_job_is_described(job):
    assert f"`{job}`" in DOC, f"CI job {job!r} is not described in docs/e2e-testing.md"


def test_every_config_variable_is_documented():
    used = set()
    for path in E2E_FILES:
        used.update(re.findall(r"WALDUR_E2E_[A-Z_]*CONFIG", path.read_text(encoding="utf-8")))
    missing = sorted(var for var in used if f"`{var}`" not in DOC)
    assert not missing, f"config variables missing from docs/e2e-testing.md: {missing}"
