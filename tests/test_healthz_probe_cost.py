"""The liveness probe must stay cheap.

The kubelet runs ``waldur_site_healthz`` as a fresh process on every probe
tick. When ``healthz`` imported the generated API client and, through
``common.utils``, every installed backend plugin, a single ``--liveness-only``
invocation cost ~2.3s -- more inside the image, where a read-only root
filesystem stops Python caching the bytecode. That overran ``timeoutSeconds``
and the kubelet restarted the container in a loop.

These tests run in a subprocess because the test process itself has already
imported everything.
"""

import subprocess
import sys
import unittest

# Importing any of these from healthz at module level reintroduces the bug.
FORBIDDEN_MODULES = (
    "waldur_api_client",
    "waldur_site_agent.common.utils",
    "httpx",
    "kubernetes",
)

LIVENESS_SCRIPT = """
import sys

import waldur_site_agent.common.healthz as healthz

healthz.check_liveness(path="/nonexistent-heartbeat-file")
print(",".join(m for m in {forbidden!r} if m in sys.modules))
"""


class TestLivenessProbeCost(unittest.TestCase):
    def test_liveness_path_imports_nothing_heavy(self):
        """The liveness path must not drag in the API client or the plugins."""
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-c", LIVENESS_SCRIPT.format(forbidden=FORBIDDEN_MODULES)],
            capture_output=True,
            text=True,
            check=True,
        )
        leaked = [name for name in result.stdout.strip().split(",") if name]
        self.assertEqual(
            leaked,
            [],
            f"healthz pulled in {leaked} on the liveness path; import them "
            "inside check_readiness instead",
        )

    def test_readiness_timeout_is_below_probe_timeout(self):
        """The readiness HTTP timeout must not outlive the probe that runs it."""
        from waldur_site_agent.common.healthz import DEFAULT_READINESS_TIMEOUT

        # helm values.yaml sets healthCheck.timeoutSeconds: 10 for the exec probe.
        self.assertLess(DEFAULT_READINESS_TIMEOUT, 10)


if __name__ == "__main__":
    unittest.main()
