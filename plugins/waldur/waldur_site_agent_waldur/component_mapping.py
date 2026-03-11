"""Component mapping — re-exported from core.

The implementation has been moved to ``waldur_site_agent.common.component_mapping``
so that all plugins can reuse it.  This module re-exports the public API for
backward compatibility.
"""

from waldur_site_agent.common.component_mapping import (  # noqa: F401
    ComponentMapper,
    ReverseMapping,
    TargetMapping,
)
