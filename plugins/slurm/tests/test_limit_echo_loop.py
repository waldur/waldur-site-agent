"""Tests that demonstrate (and later guard against) the LIMIT-component echo loop.

Two paths interact unsafely on offerings that have an active
``SlurmPeriodicUsagePolicy``:

1. Mastermind's policy task pushes ``GrpTRESMins = resource.limits[type] *
   60 * (1 + grace_ratio)`` to the SLURM backend every 10 minutes.
2. The site agent's ``sync_waldur_resource_limits`` reads the backend value
   back and writes it into ``resource.limits`` via ``set_limits``.

Each round-trip multiplies the value by ``(1 + grace_ratio)`` because the
agent does not subtract the grace on the way back. These tests:

* prove the single-cycle echo (``test_agent_echoes_backend_limit_into_waldur``);
* simulate four full cycles and assert geometric growth
  (``test_multi_cycle_inflation_without_server_gate``);
* then assert convergence when the server-side ``set_limits`` gate is in
  place (``test_multi_cycle_no_inflation_with_server_gate``).

The "e2e" scenarios here are infrastructure-free: they stitch the agent's
real ``sync_waldur_resource_limits`` against a stateful fake Waldur API
(``respx``) and a stateful in-memory SLURM stand-in. This keeps the
loop's mechanics observable without needing the Docker stack.
"""

from __future__ import annotations

import json
import unittest
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest import mock

import httpx
import respx
from waldur_api_client import models
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent.common import utils as common_utils

GRACE_RATIO = 0.3
ORDERED_LIMIT = 18800  # node-hours, mirrors the user order that started s1328
SLURM_UNIT_FACTOR_NODE = 60  # SLURM stores TRESMins; agent reports node-hours


def _serialize(obj: dict[str, Any]) -> bytes:
    return json.dumps(
        obj, default=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
    ).encode()


class _StubBackend:
    """Minimal stand-in for ``SlurmBackend`` exposing only what the
    limit-sync funnel needs: ``get_resource_limits``. The value lives in
    ``self.current_limit`` and is mutated by the test to simulate what the
    mastermind policy would have written to SLURM."""

    def __init__(self, initial_limit_node_hours: int) -> None:
        self.current_limit = {"node": initial_limit_node_hours}

    def get_resource_limits(self, backend_id: str) -> dict[str, int]:
        return dict(self.current_limit)


def _make_resource(limits: dict[str, int]) -> models.Resource:
    return models.Resource(
        uuid=uuid.uuid4(),
        name="test-resource",
        backend_id="s1328",
        resource_uuid=uuid.uuid4(),
        offering_type="Marketplace.Slurm",
        downscaled=False,
        state=ResourceState.OK,
        created=datetime(2026, 5, 1, tzinfo=timezone.utc),
        modified=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_sync=datetime(2026, 5, 1, tzinfo=timezone.utc),
        restrict_member_access=False,
        limits=ResourceLimits.from_dict(limits),
        project_uuid=uuid.uuid4(),
        project_name="proj",
        project_slug="proj",
        customer_uuid=uuid.uuid4(),
        customer_name="cust",
        customer_slug="cust",
    )


BASE_URL = "https://waldur.example.com"
TOKEN = "9e1132b9616ebfe943ddf632ca32bbb7e1109a32"


def _client() -> AuthenticatedClient:
    return AuthenticatedClient(base_url=BASE_URL, token=TOKEN, timeout=600, headers={})


class SingleCycleEchoTest(unittest.TestCase):
    """One pass through ``sync_waldur_resource_limits``: backend wins."""

    def setUp(self) -> None:
        respx.start()
        self.addCleanup(respx.stop)
        self.addCleanup(mock.patch.stopall)

    def test_agent_echoes_backend_limit_into_waldur(self) -> None:
        """Bug demo: backend returns the grace-inflated value, agent writes
        it back into Waldur unchanged. This is the producer of geometric
        growth in production."""
        waldur_resource = _make_resource({"node": ORDERED_LIMIT})
        backend = _StubBackend(
            initial_limit_node_hours=int(ORDERED_LIMIT * (1 + GRACE_RATIO))
        )

        captured: dict[str, Any] = {}

        def _capture(request: httpx.Request) -> httpx.Response:
            captured["payload"] = json.loads(request.content)
            return httpx.Response(200, json={"status": "ok"})

        respx.post(
            f"{BASE_URL}/api/marketplace-provider-resources/"
            f"{waldur_resource.uuid.hex}/set_limits/"
        ).mock(side_effect=_capture)

        common_utils.sync_waldur_resource_limits(backend, _client(), waldur_resource)

        self.assertIn("payload", captured, "set_limits was not called")
        self.assertEqual(
            captured["payload"]["limits"],
            {"node": int(ORDERED_LIMIT * (1 + GRACE_RATIO))},
            "Agent echoes the inflated SLURM value back to Waldur verbatim.",
        )

    def test_agent_skips_set_limits_when_already_in_sync(self) -> None:
        """Sanity check: no echo when SLURM and Waldur agree."""
        waldur_resource = _make_resource({"node": ORDERED_LIMIT})
        backend = _StubBackend(initial_limit_node_hours=ORDERED_LIMIT)

        route = respx.post(
            f"{BASE_URL}/api/marketplace-provider-resources/"
            f"{waldur_resource.uuid.hex}/set_limits/"
        ).respond(200, json={"status": "ok"})

        common_utils.sync_waldur_resource_limits(backend, _client(), waldur_resource)

        self.assertEqual(route.call_count, 0)


class MultiCycleInflationTest(unittest.TestCase):
    """E2E-style: drive the agent through several full mastermind ↔ SLURM
    round-trips against an in-memory fake server. The fake server models
    both the periodic-policy task (Waldur → SLURM with grace) and the
    ``set_limits`` endpoint (with or without the gate)."""

    def setUp(self) -> None:
        respx.start()
        self.addCleanup(respx.stop)
        self.addCleanup(mock.patch.stopall)

    def _run_cycles(
        self,
        *,
        cycles: int,
        server_has_gate: bool,
    ) -> tuple[list[int], list[int]]:
        """Run ``cycles`` full loops and return (waldur_limits, slurm_limits)
        histories — both as node-hours."""
        waldur_resource = _make_resource({"node": ORDERED_LIMIT})
        backend = _StubBackend(initial_limit_node_hours=ORDERED_LIMIT)

        # The fake set_limits endpoint accepts the body, optionally applies
        # the server-side gate, and mutates the in-memory resource. The
        # gate condition mirrors the patch we're about to land in mastermind.
        def _set_limits_endpoint(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            new_limits = body["limits"]
            for component_type, new_value in new_limits.items():
                current_value = waldur_resource.limits.additional_properties.get(
                    component_type
                )
                if server_has_gate and current_value != new_value:
                    # LIMIT-typed + active policy → reject silently.
                    continue
                waldur_resource.limits.additional_properties[component_type] = new_value
            return httpx.Response(200, json={"status": "ok"})

        respx.post(
            f"{BASE_URL}/api/marketplace-provider-resources/"
            f"{waldur_resource.uuid.hex}/set_limits/"
        ).mock(side_effect=_set_limits_endpoint)

        waldur_history = [waldur_resource.limits.additional_properties["node"]]
        slurm_history = [backend.current_limit["node"]]

        for _ in range(cycles):
            # 1) Mastermind policy task: pushes grace-inflated value to SLURM.
            #    Mirrors models.py:670-674 / calculate_slurm_settings.
            current = waldur_resource.limits.additional_properties["node"]
            backend.current_limit = {"node": int(current * (1 + GRACE_RATIO))}

            # 2) Agent reverse-sync: reads SLURM and calls set_limits.
            common_utils.sync_waldur_resource_limits(
                backend, _client(), waldur_resource
            )

            waldur_history.append(waldur_resource.limits.additional_properties["node"])
            slurm_history.append(backend.current_limit["node"])

        return waldur_history, slurm_history

    def test_multi_cycle_inflation_without_server_gate(self) -> None:
        """Bug demo: without the gate, the loop multiplies by 1.3 per
        cycle. After 4 cycles the limit has roughly tripled."""
        waldur, slurm = self._run_cycles(cycles=4, server_has_gate=False)

        # Waldur should be inflated 1.3× per cycle after the policy push.
        self.assertEqual(waldur[0], ORDERED_LIMIT)
        self.assertEqual(waldur[1], int(ORDERED_LIMIT * 1.3))
        self.assertEqual(waldur[2], int(int(ORDERED_LIMIT * 1.3) * 1.3))
        self.assertGreater(
            waldur[-1],
            ORDERED_LIMIT * 2,
            "After 4 cycles the inflated value must be more than 2× the ordered limit.",
        )
        # SLURM is always one grace step ahead of Waldur.
        for w, s in zip(waldur, slurm):
            self.assertGreaterEqual(s, w)

    def test_multi_cycle_no_inflation_with_server_gate(self) -> None:
        """With the gate, the agent's echo is silently dropped on the
        server and the Waldur limit stays equal to the user-ordered value
        across every cycle. SLURM keeps getting the (constant) grace-inflated
        value, which is the intended steady state."""
        waldur, slurm = self._run_cycles(cycles=4, server_has_gate=True)

        for value in waldur:
            self.assertEqual(
                value,
                ORDERED_LIMIT,
                f"Waldur limit drifted: {waldur}",
            )
        # SLURM converges to ORDERED_LIMIT * 1.3 from cycle 1 onward.
        self.assertEqual(slurm[0], ORDERED_LIMIT)
        for value in slurm[1:]:
            self.assertEqual(value, int(ORDERED_LIMIT * 1.3))


if __name__ == "__main__":
    unittest.main()
