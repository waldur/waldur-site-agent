"""Order testing framework for Waldur Site Agent.

This module provides tools for testing order processing without requiring
a live Waldur API connection. It supports:

- Template-based order generation with Jinja2
- Mock backend implementations for safe testing
- Order validation and dry-run capabilities
- Multi-step scenario testing

The testing framework is designed to help backend developers and integrators
validate their implementations before deploying to production environments.
"""
