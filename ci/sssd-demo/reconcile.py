"""Populate the LDAP directory from Waldur through the agent's reconcile path.

Under ``account_source: waldur`` the plugin does not mint usernames, so the
username-generation path core normally drives is deliberately inert. Provisioning
hangs off ``sync_user_profiles``, which the membership processor calls with the
full account list -- ``process_project_user_sync`` is the public entry point that
reaches it. This is the same code an agent runs on its own cycle; nothing here is
test scaffolding.
"""

import sys

from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.common.utils import get_client, load_configuration

EXPECTED_ARGS = 3


def main() -> None:
    """Reconcile every offering in the config against the directory."""
    if len(sys.argv) != EXPECTED_ARGS:
        print(f"usage: {sys.argv[0]} <agent-config.yaml> <project-uuid>", file=sys.stderr)
        raise SystemExit(2)

    config_path, project_uuid = sys.argv[1], sys.argv[2]
    config = load_configuration(config_path, user_agent_suffix="sssd-demo")

    for offering in config.offerings:
        print(f"   reconciling {offering.name}")
        client = get_client(offering.waldur_api_url, offering.waldur_api_token)
        backend = SlurmBackend(offering.backend_settings, offering.backend_components)
        processor = OfferingMembershipProcessor(
            offering=offering,
            waldur_rest_client=client,
            resource_backend=backend,
        )
        processor.process_project_user_sync(project_uuid)


if __name__ == "__main__":
    main()
