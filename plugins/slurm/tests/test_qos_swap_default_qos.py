"""The pause/downscale/restore QoS swap must move the account DefaultQOS along.

slurmdbd validates the post-modify state of an account and every association
under it: each effective DefaultQOS has to be in its effective QoS list, or the
whole ``sacctmgr modify`` is rolled back with "These associations don't have
access to their default qos". A plain ``set qos=<paused>`` therefore fails on
any cluster that sets a DefaultQOS on accounts and the resource flaps to ERRED
on every sync. The swap now reads the current default and
writes ``defaultqos=`` in the same command when the default would otherwise
fall outside the new list.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from waldur_site_agent_slurm.backend import SlurmBackend


def _backend(**extra):
    settings = {
        "default_account": "root",
        "qos_paused": "stop",
        "qos_downscaled": "limited",
        "qos_default": "normal",
    }
    settings.update(extra)
    backend = SlurmBackend(settings, {"cpu": {"unit": "minutes"}})
    backend.client = MagicMock()
    backend.client.get_association.return_value = None
    return backend


def _with_state(backend, current_qos: str, current_default: str):
    backend.client.get_current_account_qos.return_value = current_qos
    backend.client.get_current_account_default_qos.return_value = current_default
    return backend


class TestSwapMovesDefaultQos:
    def test_pause_moves_default_outside_new_list(self):
        backend = _with_state(_backend(), current_qos="normal", current_default="normal")
        assert backend.pause_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with("a136", "stop", default_qos="stop")

    def test_downscale_moves_default_outside_new_list(self):
        backend = _with_state(_backend(), current_qos="normal", current_default="normal")
        assert backend.downscale_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with(
            "a136", "limited", default_qos="limited"
        )

    def test_restore_moves_default_back(self):
        backend = _with_state(_backend(), current_qos="stop", current_default="stop")
        assert backend.restore_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with(
            "a136", "normal", default_qos="normal"
        )

    def test_default_moves_to_first_entry_of_a_multi_qos_list(self):
        backend = _with_state(
            _backend(qos_paused="stop,debug"), current_qos="normal", current_default="normal"
        )
        assert backend.pause_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with(
            "a136", "stop,debug", default_qos="stop"
        )


class TestSwapLeavesDefaultAlone:
    def test_no_default_keeps_single_field_write(self):
        backend = _with_state(_backend(), current_qos="normal", current_default="")
        assert backend.pause_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with("a136", "stop")

    def test_default_already_in_new_list_is_not_rewritten(self):
        backend = _with_state(
            _backend(qos_paused="stop,normal"), current_qos="normal", current_default="normal"
        )
        assert backend.pause_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with("a136", "stop,normal")

    def test_restore_with_default_already_on_target_is_plain(self):
        backend = _with_state(_backend(), current_qos="stop", current_default="normal")
        assert backend.restore_resource("a136") is True
        backend.client.set_account_qos.assert_called_once_with("a136", "normal")

    def test_already_paused_does_not_query_or_write(self):
        backend = _with_state(_backend(), current_qos="stop", current_default="stop")
        assert backend.pause_resource("a136") is True
        backend.client.get_current_account_default_qos.assert_not_called()
        backend.client.set_account_qos.assert_not_called()
