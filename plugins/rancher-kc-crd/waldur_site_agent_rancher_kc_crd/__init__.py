"""Rancher + Keycloak CRD-driven plugin for Waldur Site Agent.

Writes ManagedRancherProject CRDs to a Kubernetes cluster running the
rancher-keycloak-operator, which reconciles them against Rancher and
Keycloak. Membership-sync only — assumes the Rancher cluster already exists.
Each Waldur Resource is 1:1 with a downstream cluster and the
translator reads the cluster ID from ``resource.backend_id`` at
CR-build time; there is no offering-level ``cluster_id`` setting.
"""
