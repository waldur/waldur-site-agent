"""Component mapping for Waldur-to-Waldur federation.

Handles forward conversion (source limits -> target limits) and
reverse conversion (target usage -> source usage) between Waldur A
and Waldur B component types.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TargetMapping:
    """A mapping from a source component to a target component with a factor."""

    target_component: str
    factor: float


@dataclass
class ReverseMapping:
    """A reverse mapping from a target component back to source components."""

    source_component: str
    factor: float


class ComponentMapper:
    """Maps component types between source (Waldur A) and target (Waldur B).

    Supports two modes:
    - **Passthrough**: When a component has no ``target_components`` configured,
      it maps 1:1 with factor 1.0 (same component name on both sides).
    - **Conversion**: When ``target_components`` is configured, the source component
      maps to one or more target components with configurable factors.

    Forward conversion (limits): ``target_value = source_value * factor``
    Reverse conversion (usage): ``source_value = SUM(target_value / factor)``
    """

    def __init__(self, backend_components: dict[str, dict]) -> None:
        self._forward_map: dict[str, list[TargetMapping]] = {}
        self._reverse_map: dict[str, list[ReverseMapping]] = {}
        self._passthrough_components: set[str] = set()

        for source_comp, comp_config in backend_components.items():
            target_components = comp_config.get("target_components", {})

            if not target_components:
                # Passthrough mode: same component name, factor 1.0
                self._passthrough_components.add(source_comp)
                self._forward_map[source_comp] = [
                    TargetMapping(target_component=source_comp, factor=1.0)
                ]
                self._reverse_map.setdefault(source_comp, []).append(
                    ReverseMapping(source_component=source_comp, factor=1.0)
                )
            else:
                mappings = []
                for target_comp, target_config in target_components.items():
                    factor = float(target_config.get("factor", 1.0))
                    mappings.append(
                        TargetMapping(target_component=target_comp, factor=factor)
                    )
                    self._reverse_map.setdefault(target_comp, []).append(
                        ReverseMapping(source_component=source_comp, factor=factor)
                    )
                self._forward_map[source_comp] = mappings

    def convert_limits_to_target(self, source_limits: dict[str, int]) -> dict[str, int]:
        """Convert source (Waldur A) limits to target (Waldur B) limits.

        For each source component, multiplies by each target's factor.
        If multiple sources map to the same target, contributions are summed.

        Args:
            source_limits: Component-to-value mapping in Waldur A units.

        Returns:
            Component-to-value mapping in Waldur B units.
        """
        target_limits: dict[str, float] = {}

        for source_comp, value in source_limits.items():
            mappings = self._forward_map.get(source_comp)
            if mappings is None:
                logger.warning(
                    "No forward mapping for component %s, passing through", source_comp
                )
                target_limits[source_comp] = target_limits.get(source_comp, 0) + value
                continue

            for mapping in mappings:
                target_comp = mapping.target_component
                target_value = value * mapping.factor
                target_limits[target_comp] = target_limits.get(target_comp, 0) + target_value

        return {k: int(v) for k, v in target_limits.items()}

    def convert_usage_from_target(self, target_usage: dict[str, float]) -> dict[str, float]:
        """Convert target (Waldur B) usage back to source (Waldur A) usage.

        For each source component, sums ``target_value / factor`` across all
        its target components.

        Example:
            If ``node_hours`` maps to ``{gpu_hours: 5x, storage_gb_hours: 10x}``
            and target_usage = ``{gpu_hours: 500, storage_gb_hours: 800}``,
            then ``node_hours = 500/5 + 800/10 = 100 + 80 = 180``.

        Args:
            target_usage: Component-to-value mapping in Waldur B units.

        Returns:
            Component-to-value mapping in Waldur A units.
        """
        source_usage: dict[str, float] = {}

        for target_comp, value in target_usage.items():
            reverse_mappings = self._reverse_map.get(target_comp)
            if reverse_mappings is None:
                logger.warning(
                    "No reverse mapping for target component %s, ignoring", target_comp
                )
                continue

            for mapping in reverse_mappings:
                source_comp = mapping.source_component
                source_value = value / mapping.factor if mapping.factor != 0 else 0
                source_usage[source_comp] = source_usage.get(source_comp, 0) + source_value

        return source_usage

    @property
    def is_passthrough(self) -> bool:
        """True if ALL components are in passthrough mode (no conversion needed)."""
        return len(self._passthrough_components) == len(self._forward_map)

    @property
    def source_components(self) -> set[str]:
        """Return set of source component names."""
        return set(self._forward_map.keys())

    @property
    def target_components(self) -> set[str]:
        """Return set of target component names."""
        targets: set[str] = set()
        for mappings in self._forward_map.values():
            for mapping in mappings:
                targets.add(mapping.target_component)
        return targets
