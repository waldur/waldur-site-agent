"""The liveness and readiness probes must stay cheap.

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
import tempfile
import unittest
from pathlib import Path

import httpx
import respx
import yaml

from waldur_site_agent.common.healthz import check_readiness

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

# Readiness may import httpx, which it needs for the request itself.
READINESS_SCRIPT = """
import sys

import waldur_site_agent.common.healthz as healthz

healthz.check_readiness({config!r}, timeout=0.5)
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

    def test_readiness_path_imports_nothing_heavy(self):
        """Readiness must not load the generated API client or the plugins either."""
        forbidden = tuple(m for m in FORBIDDEN_MODULES if m != "httpx")
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            # Nothing listens on port 9, so the request fails fast.
            config.write_text(
                yaml.safe_dump(
                    {"offerings": [{"waldur_api_url": "http://127.0.0.1:9/api/",
                                    "waldur_api_token": "x"}]}
                )
            )
            result = subprocess.run(  # noqa: S603
                [
                    sys.executable,
                    "-c",
                    READINESS_SCRIPT.format(config=str(config), forbidden=forbidden),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        leaked = [name for name in result.stdout.strip().split(",") if name]
        self.assertEqual(leaked, [], f"healthz pulled in {leaked} on the readiness path")


class TestReadiness(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config_path = Path(tmp.name) / "config.yaml"

    def write_config(self, offerings, **extra):
        self.config_path.write_text(yaml.safe_dump({"offerings": offerings, **extra}))
        return str(self.config_path)

    @respx.mock
    def test_static_token(self):
        route = respx.get("https://waldur.example.com/api/users/me/").mock(
            return_value=httpx.Response(200, json={"uuid": "u"})
        )
        config = self.write_config(
            [{"waldur_api_url": "https://waldur.example.com/api/", "waldur_api_token": "tok"}]
        )
        self.assertTrue(check_readiness(config))
        request = route.calls.last.request
        self.assertEqual(request.headers["Authorization"], "Token tok")
        self.assertEqual(request.url.params["field"], "uuid")

    @respx.mock
    def test_oidc_token(self):
        respx.post("https://idp.example.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "jwt"})
        )
        route = respx.get("https://waldur.example.com/api/users/me/").mock(
            return_value=httpx.Response(200, json={"uuid": "u"})
        )
        config = self.write_config(
            [
                {
                    "waldur_api_url": "https://waldur.example.com/api/",
                    "oidc_token_url": "https://idp.example.com/token",
                    "oidc_client_id": "id",
                    "oidc_client_secret": "secret",
                }
            ]
        )
        self.assertTrue(check_readiness(config))
        self.assertEqual(route.calls.last.request.headers["Authorization"], "Bearer jwt")

    @respx.mock
    def test_falls_through_to_next_offering(self):
        respx.get("https://down.example.com/api/users/me/").mock(
            return_value=httpx.Response(503)
        )
        respx.get("https://up.example.com/api/users/me/").mock(
            return_value=httpx.Response(200, json={"uuid": "u"})
        )
        config = self.write_config(
            [
                {"waldur_api_url": "https://down.example.com/api/", "waldur_api_token": "a"},
                {"waldur_api_url": "https://up.example.com/api/", "waldur_api_token": "b"},
            ]
        )
        self.assertTrue(check_readiness(config))

    @respx.mock
    def test_auth_failure_is_not_ready(self):
        respx.get("https://waldur.example.com/api/users/me/").mock(
            return_value=httpx.Response(401)
        )
        config = self.write_config(
            [{"waldur_api_url": "https://waldur.example.com/api/", "waldur_api_token": "bad"}]
        )
        self.assertFalse(check_readiness(config))

    def test_missing_config_is_not_ready(self):
        self.assertFalse(check_readiness(str(self.config_path)))


if __name__ == "__main__":
    unittest.main()
