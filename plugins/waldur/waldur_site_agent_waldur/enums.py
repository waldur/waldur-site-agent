"""Enums for Waldur federation plugin settings."""

from enum import Enum


class EndDateSyncDirection(str, Enum):
    """Direction for end-date synchronization between Waldur instances."""

    A_TO_B = "a_to_b"
    B_TO_A = "b_to_a"
    BIDIRECTIONAL = "bidirectional"
    DISABLED = "disabled"


class LimitSyncDirection(str, Enum):
    """Direction for resource-limit synchronization between Waldur instances."""

    B_TO_A = "b_to_a"
    DISABLED = "disabled"
