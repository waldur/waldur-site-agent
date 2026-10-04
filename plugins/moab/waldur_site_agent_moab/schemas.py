"""Backend settings schema for the moab backend: only the settings core reads."""

from __future__ import annotations

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class MoabBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the ``moab`` backend.

    The plugin itself reads no settings; prefixes and the default account are
    read by the agent core.
    """
