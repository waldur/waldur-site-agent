"""Clearing key residue when provisioning lands on a uid the agent already made.

A create that dies after the S3 user exists leaves that user behind, holding
whatever keys the attempt got as far as applying. The retry adopts it, so those
keys survive into a provisioned resource — live credentials with no row in Waldur,
which nothing can later rotate or reveal.

Rotation already prunes exactly this, against the set Waldur holds. Provisioning
does not, and it is the path an interrupted create comes back through.
"""

import unittest
from unittest import mock

from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.testing.mock_backend import MockBackend

COMMON_UTILS = "waldur_site_agent.common.utils"


def _backend(**overrides):
    backend = mock.Mock()
    backend.supports_resource_api_keys = True
    backend.generate_resource_keys.return_value = [
        {"client_id": "cid-new", "api_key": "sk-new"}
    ]
    for name, value in overrides.items():
        setattr(backend, name, value)
    return backend


@mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_report_created")
@mock.patch(f"{COMMON_UTILS}._known_client_ids")
class TestProvisioningPrune(unittest.TestCase):
    def test_residue_outside_what_waldur_holds_is_dropped(self, mock_known, mock_created):
        """The orphan an interrupted attempt left behind must not survive the retry."""
        mock_known.return_value = ["cid-held"]
        backend = _backend()

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        backend.prune_unknown_resource_keys.assert_called_once_with("uid", ["cid-held"])

    def test_a_key_waldur_holds_is_kept(self, mock_known, mock_created):
        """A pair reported before the earlier attempt died is live and revealable.

        Deleting it would break a credential a consumer already has — the failure
        this prune exists to avoid causing.
        """
        mock_known.return_value = ["cid-held"]
        backend = _backend()

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        kept = backend.prune_unknown_resource_keys.call_args.args[1]
        self.assertIn("cid-held", kept)

    def test_an_unreadable_listing_prunes_nothing(self, mock_known, mock_created):
        """None means "unknown", never "Waldur holds none".

        Reading it as an empty set would take out every credential the resource has
        on the first listing failure.
        """
        mock_known.return_value = None
        backend = _backend()

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        backend.prune_unknown_resource_keys.assert_not_called()

    def test_pruning_happens_before_minting(self, mock_known, mock_created):
        """Otherwise the fresh pairs are in the candidate set the prune walks."""
        mock_known.return_value = ["cid-held"]
        calls = []
        backend = _backend()
        backend.prune_unknown_resource_keys.side_effect = lambda *a: calls.append("prune")
        backend.generate_resource_keys.side_effect = lambda *a, **k: (
            calls.append("mint") or [{"client_id": "cid-new", "api_key": "sk-new"}]
        )

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        self.assertEqual(calls, ["prune", "mint"])

    def test_minting_tops_up_to_the_cap_rather_than_adding_a_full_set(
        self, mock_known, mock_created
    ):
        """A re-processed create order must not double the resource's key count.

        The prune keeps what Waldur holds, so minting a full set on top of it ends
        with four live keys against a documented cap of two.
        """
        mock_known.return_value = ["cid-held-1", "cid-held-2"]
        backend = _backend()
        backend.generate_resource_keys.return_value = []

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        backend.generate_resource_keys.assert_called_once_with("uid", count=0)

    def test_an_unreadable_listing_still_mints_the_full_set(self, mock_known, mock_created):
        """None means "unknown", and under-minting is not recoverable without a restore."""
        mock_known.return_value = None
        backend = _backend()

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        backend.generate_resource_keys.assert_called_once_with("uid", count=2)

    def test_an_explicit_count_is_still_reconciled(self, mock_known, mock_created):
        """The caller's count is the target for the resource, not an amount to add."""
        mock_known.return_value = ["cid-held-1"]
        backend = _backend()

        common_utils.provision_resource_api_keys(
            mock.Mock(), "res-uuid-1", "uid", backend, count=3
        )

        backend.generate_resource_keys.assert_called_once_with("uid", count=2)

    def test_a_failed_prune_still_mints(self, mock_known, mock_created):
        """Pruning is hygiene; minting is the reason this function exists.

        Letting a prune failure escape leaves the resource with no keys at all and
        the order still DONE, because the caller swallows everything. There is no
        add command, so that shortfall is only recoverable by a restore.
        """
        mock_known.return_value = ["cid-held"]
        backend = _backend()
        backend.prune_unknown_resource_keys.side_effect = RuntimeError("croit is down")

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        backend.generate_resource_keys.assert_called_once()
        self.assertEqual(mock_created.sync.call_count, 1)

    def test_a_backend_that_does_not_override_the_hook_prunes_nothing(
        self, mock_known, mock_created
    ):
        """A backend whose keys exist only because it just made them has no residue.

        Envoy is the case: it inherits BaseBackend's no-op rather than declaring it,
        so provisioning must complete without it having to say anything.
        """
        mock_known.return_value = ["cid-held"]

        class PlainBackend(MockBackend):
            """Concrete, and silent about pruning — it inherits the default."""

            supports_resource_api_keys = True

            def generate_resource_keys(self, resource_backend_id, count=1):
                return [{"client_id": "cid-new", "api_key": "sk-new"}]

        backend = PlainBackend({}, {})

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "uid", backend)

        self.assertEqual(mock_created.sync.call_count, 1)
