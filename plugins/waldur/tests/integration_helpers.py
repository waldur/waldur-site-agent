"""Helper utilities for Waldur federation integration tests.

Creates and tears down test entities (customers, projects, offerings,
components) on a real Waldur instance for integration testing.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

from waldur_api_client.api.customers import customers_create, customers_destroy
from waldur_api_client.api.marketplace_categories import marketplace_categories_create
from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_set_usage,
)
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_activate,
    marketplace_provider_offerings_create,
    marketplace_provider_offerings_create_offering_component,
    marketplace_provider_offerings_destroy,
)
from waldur_api_client.api.marketplace_service_providers import (
    marketplace_service_providers_create,
    marketplace_service_providers_destroy,
)
from waldur_api_client.api.projects import projects_create, projects_destroy
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.billing_type_enum import BillingTypeEnum
from waldur_api_client.models.component_usage_create_request import ComponentUsageCreateRequest
from waldur_api_client.models.component_usage_item_request import ComponentUsageItemRequest
from waldur_api_client.models.customer_request import CustomerRequest
from waldur_api_client.models.marketplace_category_request import MarketplaceCategoryRequest
from waldur_api_client.models.offering_component_request import OfferingComponentRequest
from waldur_api_client.models.offering_create_request import OfferingCreateRequest
from waldur_api_client.models.order_details import OrderDetails
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.project_request import ProjectRequest
from waldur_api_client.models.provider_plan_details_request import ProviderPlanDetailsRequest
from waldur_api_client.models.service_provider_request import ServiceProviderRequest
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_waldur.client import TERMINAL_ORDER_STATES, WaldurClient

logger = logging.getLogger(__name__)


@dataclass
class OfferingInfo:
    """Details of a created offering."""

    uuid: str
    url: str
    customer_uuid: str
    customer_url: str
    components: list[str] = field(default_factory=list)


@dataclass
class SetupResult:
    """Result of test environment setup."""

    offering_a: OfferingInfo
    offering_b: OfferingInfo
    project_a_uuid: str
    project_a_url: str
    category_uuid: str
    backend_settings: dict
    backend_components: dict


class WaldurTestSetup:
    """Creates and tears down test entities on a real Waldur instance.

    Uses unique prefixes per run to avoid collisions with other test runs.
    Tracks all created entities for cleanup in reverse order.
    """

    def __init__(self, api_url: str, api_token: str) -> None:
        # Normalize: AuthenticatedClient base_url should NOT include /api/
        # since the generated client paths already start with /api/
        self.api_url = api_url.rstrip("/").removesuffix("/api")
        self.api_token = api_token
        self.client = AuthenticatedClient(base_url=self.api_url, token=api_token)
        self._run_id = uuid.uuid4().hex[:8]
        self._created_entities: list[tuple[str, str]] = []  # (type, uuid)

    def _track(self, entity_type: str, entity_uuid: str) -> None:
        """Track a created entity for cleanup."""
        self._created_entities.append((entity_type, entity_uuid))

    # --- Entity Creation ---

    def _create_category(self, title: str) -> str:
        """Create a marketplace category, return its UUID."""
        body = MarketplaceCategoryRequest(title=title)
        result = marketplace_categories_create.sync(client=self.client, body=body)
        cat_uuid = str(result.uuid) if not isinstance(result.uuid, type(UNSET)) else ""
        self._track("category", cat_uuid)
        logger.info("Created category %s: %s", title, cat_uuid)
        return cat_uuid

    def _create_customer(self, name: str) -> tuple[str, str]:
        """Create a customer, return (uuid, url)."""
        body = CustomerRequest(name=name)
        result = customers_create.sync(client=self.client, body=body)
        cust_uuid = str(result.uuid) if not isinstance(result.uuid, type(UNSET)) else ""
        cust_url = result.url if not isinstance(result.url, type(UNSET)) else ""
        self._track("customer", cust_uuid)
        logger.info("Created customer %s: %s", name, cust_uuid)
        return cust_uuid, cust_url

    def _make_service_provider(self, customer_url: str) -> None:
        """Register a customer as a service provider (required for offering creation)."""
        body = ServiceProviderRequest(customer=customer_url)
        result = marketplace_service_providers_create.sync(client=self.client, body=body)
        sp_uuid = str(result.uuid) if not isinstance(result.uuid, type(UNSET)) else ""
        self._track("service_provider", sp_uuid)
        logger.info("Made customer a service provider: %s", sp_uuid)

    def _create_project(self, customer_url: str, name: str) -> tuple[str, str]:
        """Create a project, return (uuid, url)."""
        body = ProjectRequest(name=name, customer=customer_url)
        result = projects_create.sync(client=self.client, body=body)
        proj_uuid = str(result.uuid) if not isinstance(result.uuid, type(UNSET)) else ""
        proj_url = result.url if not isinstance(result.url, type(UNSET)) else ""
        self._track("project", proj_uuid)
        logger.info("Created project %s: %s", name, proj_uuid)
        return proj_uuid, proj_url

    def _create_offering(
        self,
        name: str,
        category_url: str,
        customer_url: str,
        offering_type: str,
    ) -> tuple[str, str]:
        """Create an offering, return (uuid, url)."""
        body = OfferingCreateRequest(
            name=name,
            category=category_url,
            customer=customer_url,
            type_=offering_type,
        )
        result = marketplace_provider_offerings_create.sync(client=self.client, body=body)
        off_uuid = str(result.uuid) if not isinstance(result.uuid, type(UNSET)) else ""
        off_url = result.url if not isinstance(result.url, type(UNSET)) else ""
        self._track("offering", off_uuid)
        logger.info("Created offering %s (%s): %s", name, offering_type, off_uuid)
        return off_uuid, off_url

    def _add_component(
        self,
        offering_uuid: str,
        type_: str,
        name: str,
        billing_type: BillingTypeEnum = BillingTypeEnum.USAGE,
        measured_unit: str = "",
    ) -> None:
        """Add a component to an offering."""
        body = OfferingComponentRequest(
            billing_type=billing_type,
            type_=type_,
            name=name,
            measured_unit=measured_unit,
        )
        response = marketplace_provider_offerings_create_offering_component.sync_detailed(
            uuid=uuid.UUID(offering_uuid),
            client=self.client,
            body=body,
        )
        if response.status_code >= 400:
            msg = f"Failed to add component {type_} to offering {offering_uuid}: {response.status_code}"
            raise RuntimeError(msg)
        logger.info("Added component %s to offering %s", type_, offering_uuid)

    def _create_plan(self, offering_url: str, name: str = "Default") -> str:
        """Create a billing plan for an offering (required before activation).

        Uses raw httpx request to avoid response parsing issues when the
        waldur_api_client version doesn't match the server version.
        """
        body = ProviderPlanDetailsRequest(name=name, offering=offering_url)
        response = self.client.get_httpx_client().request(
            method="post",
            url="/api/marketplace-plans/",
            json=body.to_dict(),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code >= 400:
            msg = f"Failed to create plan: {response.status_code} {response.text}"
            raise RuntimeError(msg)
        data = response.json()
        plan_uuid = str(data.get("uuid", ""))
        self._track("plan", plan_uuid)
        logger.info("Created plan %s for offering: %s", name, plan_uuid)
        return plan_uuid

    def _activate_offering(self, offering_uuid: str) -> None:
        """Activate an offering."""
        marketplace_provider_offerings_activate.sync(
            uuid=uuid.UUID(offering_uuid), client=self.client
        )
        logger.info("Activated offering %s", offering_uuid)

    def _get_category_url(self, category_uuid: str) -> str:
        return f"{self.api_url}/api/marketplace-categories/{category_uuid}/"

    # --- Setup Scenarios ---

    def setup_passthrough(
        self, target_offering_type: str = "Marketplace.Basic"
    ) -> SetupResult:
        """Create entities for passthrough tests (same components on both sides).

        Creates:
        - 1 category
        - 2 customers (A and B)
        - 1 project under customer A
        - Offering A (Marketplace.Slurm) with cpu, mem components
        - Offering B (target_offering_type, default Marketplace.Basic) with cpu, mem components

        Args:
            target_offering_type: Marketplace type for offering B.
                Use "Marketplace.Slurm" when STOMP event subscriptions are needed.
        """
        prefix = f"inttest-pt-{self._run_id}"

        category_uuid = self._create_category(f"{prefix}-category")
        category_url = self._get_category_url(category_uuid)

        cust_a_uuid, cust_a_url = self._create_customer(f"{prefix}-customer-a")
        cust_b_uuid, cust_b_url = self._create_customer(f"{prefix}-customer-b")

        # Customers must be service providers to own offerings
        self._make_service_provider(cust_a_url)
        self._make_service_provider(cust_b_url)

        proj_a_uuid, proj_a_url = self._create_project(cust_a_url, f"{prefix}-project-a")

        # Offering A: Marketplace.Slurm (source side)
        off_a_uuid, off_a_url = self._create_offering(
            f"{prefix}-offering-a", category_url, cust_a_url, "Marketplace.Slurm"
        )
        # Components with billing_type=limit to allow setting limits on orders
        components = [("cpu", "CPU Hours", "Hours"), ("mem", "Memory GB", "GB")]
        for comp_type, comp_name, unit in components:
            self._add_component(
                off_a_uuid, comp_type, comp_name,
                billing_type=BillingTypeEnum.LIMIT, measured_unit=unit,
            )
        self._create_plan(off_a_url)
        self._activate_offering(off_a_uuid)

        # Offering B: target side
        off_b_uuid, off_b_url = self._create_offering(
            f"{prefix}-offering-b", category_url, cust_b_url, target_offering_type
        )
        for comp_type, comp_name, unit in components:
            self._add_component(
                off_b_uuid, comp_type, comp_name,
                billing_type=BillingTypeEnum.LIMIT, measured_unit=unit,
            )
        self._create_plan(off_b_url)
        self._activate_offering(off_b_uuid)

        backend_settings = {
            "target_api_url": self.api_url,
            "target_api_token": self.api_token,
            "target_offering_uuid": off_b_uuid,
            "target_customer_uuid": cust_b_uuid,
            "order_poll_timeout": 60,
            "order_poll_interval": 2,
        }

        backend_components = {
            "cpu": {
                "measured_unit": "Hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "CPU Hours",
            },
            "mem": {
                "measured_unit": "GB",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Memory GB",
            },
        }

        return SetupResult(
            offering_a=OfferingInfo(
                uuid=off_a_uuid,
                url=off_a_url,
                customer_uuid=cust_a_uuid,
                customer_url=cust_a_url,
                components=["cpu", "mem"],
            ),
            offering_b=OfferingInfo(
                uuid=off_b_uuid,
                url=off_b_url,
                customer_uuid=cust_b_uuid,
                customer_url=cust_b_url,
                components=["cpu", "mem"],
            ),
            project_a_uuid=proj_a_uuid,
            project_a_url=proj_a_url,
            category_uuid=category_uuid,
            backend_settings=backend_settings,
            backend_components=backend_components,
        )

    def setup_with_conversion(self) -> SetupResult:
        """Create entities for conversion tests.

        Source has node_hours -> target has gpu_hours + storage_gb_hours.
        """
        prefix = f"inttest-cv-{self._run_id}"

        category_uuid = self._create_category(f"{prefix}-category")
        category_url = self._get_category_url(category_uuid)

        cust_a_uuid, cust_a_url = self._create_customer(f"{prefix}-customer-a")
        cust_b_uuid, cust_b_url = self._create_customer(f"{prefix}-customer-b")

        # Customers must be service providers to own offerings
        self._make_service_provider(cust_a_url)
        self._make_service_provider(cust_b_url)

        proj_a_uuid, proj_a_url = self._create_project(cust_a_url, f"{prefix}-project-a")

        # Offering A: source with node_hours
        off_a_uuid, off_a_url = self._create_offering(
            f"{prefix}-offering-a", category_url, cust_a_url, "Marketplace.Slurm"
        )
        self._add_component(
            off_a_uuid, "node_hours", "Node Hours",
            billing_type=BillingTypeEnum.LIMIT, measured_unit="Hours",
        )
        self._create_plan(off_a_url)
        self._activate_offering(off_a_uuid)

        # Offering B: target with gpu_hours + storage_gb_hours
        off_b_uuid, off_b_url = self._create_offering(
            f"{prefix}-offering-b", category_url, cust_b_url, "Marketplace.Basic"
        )
        self._add_component(
            off_b_uuid, "gpu_hours", "GPU Hours",
            billing_type=BillingTypeEnum.LIMIT, measured_unit="Hours",
        )
        self._add_component(
            off_b_uuid, "storage_gb_hours", "Storage GB Hours",
            billing_type=BillingTypeEnum.LIMIT, measured_unit="GB*Hours",
        )
        self._create_plan(off_b_url)
        self._activate_offering(off_b_uuid)

        backend_settings = {
            "target_api_url": self.api_url,
            "target_api_token": self.api_token,
            "target_offering_uuid": off_b_uuid,
            "target_customer_uuid": cust_b_uuid,
            "order_poll_timeout": 60,
            "order_poll_interval": 2,
        }

        backend_components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Node Hours",
                "target_components": {
                    "gpu_hours": {"factor": 5.0},
                    "storage_gb_hours": {"factor": 10.0},
                },
            },
        }

        return SetupResult(
            offering_a=OfferingInfo(
                uuid=off_a_uuid,
                url=off_a_url,
                customer_uuid=cust_a_uuid,
                customer_url=cust_a_url,
                components=["node_hours"],
            ),
            offering_b=OfferingInfo(
                uuid=off_b_uuid,
                url=off_b_url,
                customer_uuid=cust_b_uuid,
                customer_url=cust_b_url,
                components=["gpu_hours", "storage_gb_hours"],
            ),
            project_a_uuid=proj_a_uuid,
            project_a_url=proj_a_url,
            category_uuid=category_uuid,
            backend_settings=backend_settings,
            backend_components=backend_components,
        )

    def setup_mixed_billing_types(self) -> SetupResult:
        """Create entities for mixed billing type tests.

        Tests that offerings can have components with different billing types:
        - limit: allows setting limits on orders
        - usage: usage-based metering
        - fixed: fixed price per billing period
        - one: one-time charge

        Both offerings A and B get the same set of mixed components.
        Only 'limit' type components support setting limits in orders.
        """
        prefix = f"inttest-bt-{self._run_id}"

        category_uuid = self._create_category(f"{prefix}-category")
        category_url = self._get_category_url(category_uuid)

        cust_a_uuid, cust_a_url = self._create_customer(f"{prefix}-customer-a")
        cust_b_uuid, cust_b_url = self._create_customer(f"{prefix}-customer-b")

        self._make_service_provider(cust_a_url)
        self._make_service_provider(cust_b_url)

        proj_a_uuid, proj_a_url = self._create_project(cust_a_url, f"{prefix}-project-a")

        # Components with different billing types
        mixed_components = [
            ("limit_comp", "Limit Component", BillingTypeEnum.LIMIT, "Units"),
            ("usage_comp", "Usage Component", BillingTypeEnum.USAGE, "Hours"),
            ("fixed_comp", "Fixed Component", BillingTypeEnum.FIXED, "Units"),
            ("one_comp", "One-time Component", BillingTypeEnum.ONE, "Units"),
        ]

        # Offering A: Marketplace.Slurm (source side)
        off_a_uuid, off_a_url = self._create_offering(
            f"{prefix}-offering-a", category_url, cust_a_url, "Marketplace.Slurm"
        )
        for comp_type, comp_name, billing_type, unit in mixed_components:
            self._add_component(
                off_a_uuid, comp_type, comp_name,
                billing_type=billing_type, measured_unit=unit,
            )
        self._create_plan(off_a_url)
        self._activate_offering(off_a_uuid)

        # Offering B: Marketplace.Basic (target side, auto-completes)
        off_b_uuid, off_b_url = self._create_offering(
            f"{prefix}-offering-b", category_url, cust_b_url, "Marketplace.Basic"
        )
        for comp_type, comp_name, billing_type, unit in mixed_components:
            self._add_component(
                off_b_uuid, comp_type, comp_name,
                billing_type=billing_type, measured_unit=unit,
            )
        self._create_plan(off_b_url)
        self._activate_offering(off_b_uuid)

        backend_settings = {
            "target_api_url": self.api_url,
            "target_api_token": self.api_token,
            "target_offering_uuid": off_b_uuid,
            "target_customer_uuid": cust_b_uuid,
            "order_poll_timeout": 60,
            "order_poll_interval": 2,
        }

        backend_components = {
            "limit_comp": {
                "measured_unit": "Units",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Limit Component",
            },
            "usage_comp": {
                "measured_unit": "Hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Usage Component",
            },
            "fixed_comp": {
                "measured_unit": "Units",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Fixed Component",
            },
            "one_comp": {
                "measured_unit": "Units",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "One-time Component",
            },
        }

        component_names = [c[0] for c in mixed_components]

        return SetupResult(
            offering_a=OfferingInfo(
                uuid=off_a_uuid,
                url=off_a_url,
                customer_uuid=cust_a_uuid,
                customer_url=cust_a_url,
                components=component_names,
            ),
            offering_b=OfferingInfo(
                uuid=off_b_uuid,
                url=off_b_url,
                customer_uuid=cust_b_uuid,
                customer_url=cust_b_url,
                components=component_names,
            ),
            project_a_uuid=proj_a_uuid,
            project_a_url=proj_a_url,
            category_uuid=category_uuid,
            backend_settings=backend_settings,
            backend_components=backend_components,
        )

    def setup_usage_only(self) -> SetupResult:
        """Create entities where all components use USAGE billing type.

        Usage-based components do not support setting limits on orders.
        This tests the behavior when orders are created without limits.
        """
        prefix = f"inttest-uo-{self._run_id}"

        category_uuid = self._create_category(f"{prefix}-category")
        category_url = self._get_category_url(category_uuid)

        cust_a_uuid, cust_a_url = self._create_customer(f"{prefix}-customer-a")
        cust_b_uuid, cust_b_url = self._create_customer(f"{prefix}-customer-b")

        self._make_service_provider(cust_a_url)
        self._make_service_provider(cust_b_url)

        proj_a_uuid, proj_a_url = self._create_project(cust_a_url, f"{prefix}-project-a")

        components = [
            ("cpu_hours", "CPU Hours", "Hours"),
            ("gpu_hours", "GPU Hours", "Hours"),
        ]

        # Offering A
        off_a_uuid, off_a_url = self._create_offering(
            f"{prefix}-offering-a", category_url, cust_a_url, "Marketplace.Slurm"
        )
        for comp_type, comp_name, unit in components:
            self._add_component(
                off_a_uuid, comp_type, comp_name,
                billing_type=BillingTypeEnum.USAGE, measured_unit=unit,
            )
        self._create_plan(off_a_url)
        self._activate_offering(off_a_uuid)

        # Offering B
        off_b_uuid, off_b_url = self._create_offering(
            f"{prefix}-offering-b", category_url, cust_b_url, "Marketplace.Basic"
        )
        for comp_type, comp_name, unit in components:
            self._add_component(
                off_b_uuid, comp_type, comp_name,
                billing_type=BillingTypeEnum.USAGE, measured_unit=unit,
            )
        self._create_plan(off_b_url)
        self._activate_offering(off_b_uuid)

        backend_settings = {
            "target_api_url": self.api_url,
            "target_api_token": self.api_token,
            "target_offering_uuid": off_b_uuid,
            "target_customer_uuid": cust_b_uuid,
            "order_poll_timeout": 60,
            "order_poll_interval": 2,
        }

        backend_components = {
            "cpu_hours": {
                "measured_unit": "Hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "CPU Hours",
            },
            "gpu_hours": {
                "measured_unit": "Hours",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "GPU Hours",
            },
        }

        component_names = [c[0] for c in components]

        return SetupResult(
            offering_a=OfferingInfo(
                uuid=off_a_uuid,
                url=off_a_url,
                customer_uuid=cust_a_uuid,
                customer_url=cust_a_url,
                components=component_names,
            ),
            offering_b=OfferingInfo(
                uuid=off_b_uuid,
                url=off_b_url,
                customer_uuid=cust_b_uuid,
                customer_url=cust_b_url,
                components=component_names,
            ),
            project_a_uuid=proj_a_uuid,
            project_a_url=proj_a_url,
            category_uuid=category_uuid,
            backend_settings=backend_settings,
            backend_components=backend_components,
        )

    def inject_usage(self, resource_uuid: str, usages: dict[str, float]) -> None:
        """Inject usage data on a Waldur B resource for reporting tests.

        Args:
            resource_uuid: UUID of the resource on Waldur B.
            usages: Component type -> usage amount mapping.
        """
        items = [
            ComponentUsageItemRequest(type_=comp_type, amount=str(amount))
            for comp_type, amount in usages.items()
        ]
        body = ComponentUsageCreateRequest(
            usages=items,
            resource=uuid.UUID(resource_uuid),
        )
        response = marketplace_component_usages_set_usage.sync_detailed(
            client=self.client, body=body
        )
        if response.status_code >= 400:
            logger.warning(
                "Failed to inject usage for resource %s: %s (status %s)",
                resource_uuid,
                response.content,
                response.status_code,
            )
        else:
            logger.info("Injected usage for resource %s: %s", resource_uuid, usages)

    # --- Order Approval ---

    def approve_and_wait(self, pending_order_id: str, timeout: int = 30) -> None:
        """Approve a pending order and wait for it to complete.

        Used in integration tests after non-blocking create_resource() calls.
        The non-blocking WaldurBackend.create_resource_with_id() returns
        immediately with pending_order_id set. This helper approves the
        pending order on Waldur B and waits for it to reach DONE state.

        Args:
            pending_order_id: Target order UUID on Waldur B.
            timeout: Maximum seconds to wait for completion.
        """
        order_uuid = UUID(pending_order_id)
        self.approve_order(self.client, order_uuid)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            order = marketplace_orders_retrieve.sync(
                client=self.client, uuid=order_uuid.hex
            )
            state = order.state if not isinstance(order.state, type(UNSET)) else None
            if state == OrderState.DONE:
                logger.info("Order %s completed (DONE)", pending_order_id)
                return
            if state in {OrderState.ERRED, OrderState.CANCELED, OrderState.REJECTED}:
                msg = f"Order {pending_order_id} reached terminal error state: {state}"
                raise RuntimeError(msg)
            time.sleep(1)

        msg = f"Order {pending_order_id} timed out after {timeout}s"
        raise RuntimeError(msg)

    # --- Cleanup ---

    @staticmethod
    def approve_order(api_client: AuthenticatedClient, order_uuid: UUID) -> None:
        """Approve and complete a pending-provider order on the test instance.

        In integration tests, Marketplace.Basic orders need manual approval
        since there is no backend processor running. This is a test-only
        operation — the production WaldurClient does not auto-approve orders.
        """
        order_hex = UUID(str(order_uuid)).hex
        try:
            resp = api_client.get_httpx_client().post(
                f"/api/marketplace-orders/{order_hex}/approve_by_provider/",
                json={},
            )
            if resp.status_code == 200:
                logger.info("Test helper: approved order %s by provider", order_uuid)
                resp2 = api_client.get_httpx_client().post(
                    f"/api/marketplace-orders/{order_hex}/set_state_done/",
                    json={},
                )
                if resp2.status_code == 200:
                    logger.info("Test helper: order %s set to done", order_uuid)
                else:
                    logger.debug(
                        "Test helper: could not set order %s to done: %s",
                        order_uuid, resp2.status_code,
                    )
            else:
                logger.debug(
                    "Test helper: could not approve order %s: %s %s",
                    order_uuid, resp.status_code, resp.text[:200],
                )
        except Exception:
            logger.debug("Test helper: failed to approve order %s", order_uuid)

    def cleanup(self) -> None:
        """Delete all created entities in reverse order.

        Silently ignores errors (entities may already be deleted by tests
        or cascade deletions).
        """
        destroy_map = {
            "project": lambda u: projects_destroy.sync_detailed(
                uuid=uuid.UUID(u), client=self.client
            ),
            "offering": lambda u: marketplace_provider_offerings_destroy.sync_detailed(
                uuid=uuid.UUID(u), client=self.client
            ),
            "service_provider": lambda u: marketplace_service_providers_destroy.sync_detailed(
                uuid=uuid.UUID(u), client=self.client
            ),
            "customer": lambda u: customers_destroy.sync_detailed(
                uuid=uuid.UUID(u), client=self.client
            ),
            "plan": lambda _u: None,  # Plans are deleted with their offerings
            "category": lambda _u: None,  # Categories typically can't be deleted via API
        }

        for entity_type, entity_uuid in reversed(self._created_entities):
            if not entity_uuid:
                continue
            destroy_fn = destroy_map.get(entity_type)
            if destroy_fn:
                try:
                    destroy_fn(entity_uuid)
                    logger.info("Deleted %s %s", entity_type, entity_uuid)
                except Exception:
                    logger.debug(
                        "Could not delete %s %s (may already be gone)",
                        entity_type,
                        entity_uuid,
                    )


class AutoApproveWaldurClient(WaldurClient):
    """WaldurClient subclass for integration tests.

    Overrides poll_order_completion to auto-approve pending-provider orders
    on the test Waldur instance. In production, orders on Waldur B are
    approved by Waldur B's own service provider workflow. In tests, there
    is no such workflow running, so we approve them ourselves.
    """

    def poll_order_completion(
        self,
        order_uuid: UUID,
        timeout: int = 300,
        interval: int = 5,
    ) -> OrderDetails:
        """Poll with auto-approval of pending-provider orders for testing."""
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            order = self.get_order(order_uuid)
            state = order.state if not isinstance(order.state, type(UNSET)) else None

            if state in TERMINAL_ORDER_STATES:
                if state == OrderState.DONE:
                    return order
                msg = (
                    f"Order {order_uuid} reached terminal state: {state}. "
                    f"Error: {getattr(order, 'error_message', '')}"
                )
                raise BackendError(msg)

            # Test-only: approve pending-provider orders since there is
            # no backend processor running on the test instance
            if state == OrderState.PENDING_PROVIDER:
                WaldurTestSetup.approve_order(self._api_client, order_uuid)

            logger.debug("Order %s state: %s, waiting...", order_uuid, state)
            time.sleep(interval)

        msg = f"Order {order_uuid} timed out after {timeout}s"
        raise BackendError(msg)
