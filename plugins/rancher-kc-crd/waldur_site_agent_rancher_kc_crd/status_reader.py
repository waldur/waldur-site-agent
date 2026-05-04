"""Read CRD status into Waldur-shaped structures.

Produces three things the Waldur side cares about: the actual user set
synced into Keycloak, drift signals between Waldur and the operator,
and a compact ``backend_metadata`` snapshot for the resource detail page.
"""

from typing import Any, Optional

PHASE_OK = "Ready"
PHASE_PROGRESSING = {"Pending", "Creating", "Updating"}
PHASE_FAILED = "Error"
PHASE_DELETING = "Deleting"

# Conditions can be reported as either Python bool True or the string
# "True" depending on how the operator serializes them — accept both.
_TRUTHY_CONDITION_VALUES = ("True", True)


def extract_synced_users(status: dict) -> set[str]:
    """Flatten ``status.keycloakRoleBindings[].syncedMembers[]`` into a set.

    Member entries can be either a dict ``{"userIdentifier": "..."}`` or
    a bare string. Returns the set of identifiers that the operator has
    confirmed are in Keycloak.
    """
    out: set[str] = set()
    for rb in (status or {}).get("keycloakRoleBindings") or []:
        for m in rb.get("syncedMembers") or []:
            ident = m.get("userIdentifier") if isinstance(m, dict) else m
            if ident:
                out.add(ident)
    return out


def detect_drift(desired: set[str], synced: set[str]) -> tuple[set[str], set[str]]:
    """Compare what Waldur wants vs what the operator confirms in Keycloak.

    Returns ``(missing_in_keycloak, present_only_in_keycloak)``.

    - ``missing_in_keycloak``: Waldur granted a role but the operator
      hasn't propagated it (could be in-flight or stuck).
    - ``present_only_in_keycloak``: someone added a member in Keycloak
      out-of-band (or via direct CR edit). Caller decides whether to
      reflect-back or reconcile-out.
    """
    return desired - synced, synced - desired


def build_backend_metadata(status: dict) -> dict[str, Any]:
    """Snapshot the CR status into a flat dict for ``Resource.backend_metadata``.

    Keep it small — this gets stored verbatim and shown on the resource
    detail page in homeport.
    """
    if not status:
        return {}

    # status.namespaceName was removed in operator 0.3.0 (the operator
    # no longer owns namespaces). Don't surface it back to Waldur even
    # if a stale CR happens to still carry the field.
    md: dict[str, Any] = {
        "phase": status.get("phase"),
        "rancher_project_id": status.get("rancherProjectId"),
        "keycloak_parent_group_id": status.get("keycloakParentGroupId"),
        "last_reconcile_time": status.get("lastReconcileTime"),
        "observed_generation": status.get("observedGeneration"),
    }

    bad = [
        {
            "type": c.get("type"),
            "status": c.get("status"),
            "reason": c.get("reason"),
            "message": c.get("message"),
        }
        for c in (status.get("conditions") or [])
        if c.get("status") not in _TRUTHY_CONDITION_VALUES
    ]
    if bad:
        md["failing_conditions"] = bad

    bindings_summary = [
        {
            "group_name": rb.get("groupName"),
            "keycloak_group_id": rb.get("keycloakGroupId"),
            "rancher_binding_id": rb.get("rancherBindingId"),
            "member_count": rb.get("memberCount"),
        }
        for rb in (status.get("keycloakRoleBindings") or [])
    ]
    if bindings_summary:
        md["role_bindings"] = bindings_summary

    return {k: v for k, v in md.items() if v is not None}


def is_terminal_failure(status: dict) -> Optional[str]:
    """Return a short error string if the CR is in a terminal failure state.

    Returns ``None`` if the CR isn't failed. Caller surfaces this back
    to Waldur (e.g., via Resource.error_message).
    """
    if not status or status.get("phase") != PHASE_FAILED:
        return None
    msgs = [
        c["message"]
        for c in (status.get("conditions") or [])
        if c.get("status") not in _TRUTHY_CONDITION_VALUES and c.get("message")
    ]
    return "; ".join(msgs) if msgs else "Operator reports Error phase"
