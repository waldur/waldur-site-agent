"""Rancher + Keycloak CRD-driven plugin for Waldur Site Agent.

Writes ManagedRancherProject CRDs to a Kubernetes cluster running the
rancher-keycloak-operator, which reconciles them against Rancher and
Keycloak. Membership-sync only — assumes the Rancher cluster already
exists and is referenced by the offering's backend_settings.cluster_id.
"""
