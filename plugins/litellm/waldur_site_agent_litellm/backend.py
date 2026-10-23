"""LiteLLM backend: manages virtual keys on a LiteLLM proxy.

This is the management (order-processing) backend. Token usage is reported by the
separate ``litellm-usage`` reporting backend, so ``_get_usage_report`` here returns
nothing.

The proxy stores keys sha256-hashed and hands out the plaintext exactly once, in the
``/key/generate`` response. The agent therefore never holds a key after provisioning
(Waldur does, encrypted) and addresses every later operation by the key's alias — the
slot name ``<resource_backend_id>-<n>`` this module assigns — resolving it to the hash
the proxy needs through ``/key/list``.
"""

from __future__ import annotations

import datetime
import logging
import re
import threading
import time
from collections.abc import Iterator
from typing import Optional

from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import ALL_MODELS, DEFAULT_RESOURCE_KEY_COUNT, backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES
from waldur_site_agent.common.spending_limits import resource_spending_limits

from . import ownership
from .client import (
    META_KEY,
    META_USERNAME,
    LiteLLMBackendError,
    LiteLLMClient,
    LiteLLMEnterpriseFeatureError,
)
from .openwebui import ROLE_ACTIVE, ROLE_DISABLED, OpenWebUIClient, OpenWebUIError
from .reporting import chat_spend_by_user
from .usage_client import LiteLLMUsageClient

logger = logging.getLogger(__name__)

# Inference resources get two keys so one can be rotated with no downtime. Taken from
# the core, because common.utils reconciles the count against what Waldur already
# holds and so has to agree with this module on the target.
_DEFAULT_KEY_COUNT = DEFAULT_RESOURCE_KEY_COUNT

# Waldur limit names this backend mirrors onto the key as a metering backstop, and the
# LiteLLM field each maps to. Waldur remains the enforcement authority (report -> pause
# -> block); these only stop a burst between two reporting cycles from outrunning it.
_LIMIT_FIELDS = {
    "token_cost": "max_budget",
    "tpm": "tpm_limit",
    "rpm": "rpm_limit",
}

# The reset period written next to every ``max_budget``. Waldur counts limits per UTC
# calendar month, and ``1mo`` resets the key's spend at 00:00 on the 1st, in the proxy's
# timezone. Any other window disagrees with Waldur's month: a person Waldur resumes on
# the 1st would stay refused by the proxy until its own reset, with nothing in Waldur
# saying why. So the offering's ``budget_duration`` setting is not consulted.
BUDGET_DURATION = "1mo"

# The proxy windows a key's spend is capped in beside ``max_budget``, one per Waldur
# spending-limit period: ``budget_limits`` on the key, each ``{max_budget,
# budget_duration}``. ``1d`` resets at 00:00 UTC and ``7d`` on Monday at 00:00 UTC,
# with Waldur's day and week (checked on LiteLLM 1.98 and 1.106). The month stays on
# ``max_budget`` with BUDGET_DURATION. A quarter or a year has no duration that resets
# with Waldur's -- ``3mo`` is refused, ``90d`` runs from the day it was written, ``1y``
# silently resets daily -- so those limits are Waldur's pause alone.
BUDGET_WINDOWS = {"day": "1d", "week": "7d"}

# What a spending limit on the proxy's spend is mapped to, by period, beside the
# ``_LIMIT_FIELDS`` names: the month joins ``token_cost``'s ``max_budget``, a day or a
# week becomes a window. Kept apart from ``token_cost`` because a limit-based
# ``token_cost`` arrives with an order and a spending limit without one, so each can
# be replaced while the other is kept.
_SPEND_LIMITS = {"month": "spend/month", "day": "spend/day", "week": "spend/week"}
_BUDGET_NAMES = (*_LIMIT_FIELDS, *_SPEND_LIMITS.values())

# What the plugin records in a key's ``metadata``. The proxy has a ``blocked`` flag but
# no reason for it, and one set of limit fields that has to hold the tighter of two
# limits, so the inputs behind both are kept on the key they describe.
#
# ``waldur_key_paused``: the key was paused on its own (a per-key pause), not because
# its resource is paused. A resource restore must leave such a key blocked.
# ``waldur_key_limits``: the key's own limits, by Waldur component name.
# ``waldur_resource_limits``: the resource's limits as last reconciled. A key added
# between two reconciliations starts from them: the per-key command carries only the
# key's own limits, not its resource's.
# ``waldur_key_models``: the key's model list as Waldur sent it (``["*"]``, names, or
# ``[]``). The key's ``models`` is that list resolved against the offering's ``models``
# setting at the time, and Waldur sends nothing when the setting changes -- the key's
# list in Waldur is still the same -- so the membership sync resolves it again from this.
# ``waldur_holder_chat_spend``: the holder's chat this month, taken off the key's
# ``max_budget`` when it was last written. A pass that cannot read chat spend keeps it
# rather than dropping it, so a failed read never loosens the cap.
# ``waldur_holder_chat_spend_windows``: the same for the day and week windows,
# ``{period: spend}``.
META_KEY_PAUSED = "waldur_key_paused"
META_KEY_LIMITS = "waldur_key_limits"
META_RESOURCE_LIMITS = "waldur_resource_limits"
META_KEY_MODELS = "waldur_key_models"
META_HOLDER_CHAT = "waldur_holder_chat_spend"
META_HOLDER_CHAT_WINDOWS = "waldur_holder_chat_spend_windows"

# Held across each read-modify-write of a key's metadata. ``/key/update`` replaces
# ``metadata`` whole, and in event mode the hourly status pass rewrites budgets on the
# main thread while STOMP handlers pause and update keys on theirs: a pause landing
# between another writer's read and write would be dropped, and the next restore would
# unblock the key. Separate agent processes are not covered.
_KEY_METADATA_LOCK = threading.RLock()

# The proxy reads an empty ``models`` on a key as every model, so a key Waldur allows
# no model gets this name, the proxy's reserved one, as its only model. No deployment
# carries it: a call to a real model answers 403 and the key's ``/v1/models`` is empty.
NO_MODELS = "no-default-models"

# Open WebUI groups this plugin owns: one per model of the offering, named after it.
# A person is in the group of every model their key allows, and of no other, so the
# prefix is also what keeps the plugin away from groups an operator made by hand.
CHAT_MODEL_GROUP_PREFIX = "waldur-model-"

# How long the proxy's internal-user listing stays reusable, in seconds. ``/user/list``
# cannot be filtered by metadata, so establishing which users belong to one resource
# means walking every internal user on the proxy -- and the membership processor pulls
# one resource at a time, which without this would repeat that walk per resource per
# pass. Half the membership-sync period for the same reason the usage cache uses half
# the report period: it covers the whole of one pass and expires before the next.
_USER_CACHE_TTL = WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60 * 0.5

# Account provisioning modes for the chat surface.
#
# ``sso``: Open WebUI is fronted by an identity provider (the same one as Waldur), so
# an account materializes on first login with the address the IdP asserts. The agent
# creates nothing -- there is no password to mint, transport or store -- and only ever
# revokes. This is the target state and the default.
#
# ``managed_password``: the agent creates the account itself with an operator-supplied
# temporary password, mirroring what an admin does by hand today. It exists because
# that is where most deployments start, not because it is good: Waldur has no encrypted
# channel to hand a password to an end user, so the same initial secret is used for
# every account and the person is expected to change it on first login.
PROVISIONING_SSO = "sso"
PROVISIONING_MANAGED_PASSWORD = "managed_password"  # noqa: S105 - a mode name, not a secret

# What a new resource's backend id -- and so every key alias beneath it -- is derived from.
#
# ``slug``: whatever the order processor passes in, i.e. ``allocation_prefix`` plus the
# resource slug. The slug comes from a name the ordering user picks, and key aliases
# share the proxy's one global namespace with every hand-made key, so a resource named
# after an existing key's stem would adopt it as one of its slots. Kept as the default
# only because it is what existing deployments already run.
#
# ``uuid``: the resource UUID hex. Nothing but the agent writes aliases of that shape, so
# a hand-made key can never fall inside a resource's slot pattern.
BACKEND_ID_SOURCE_SLUG = "slug"
BACKEND_ID_SOURCE_UUID = "uuid"
_BACKEND_ID_SOURCES = (BACKEND_ID_SOURCE_SLUG, BACKEND_ID_SOURCE_UUID)


class LiteLLMBackend(backends.BaseBackend):
    """Provisions LiteLLM virtual keys from Waldur orders."""

    # LiteLLM mints each key and returns the plaintext once; the agent applies it and
    # only then reports it to Waldur (encrypted) via the provider endpoints. A resource
    # owns many keys, each a "<backend_id>-<n>" alias.
    supports_resource_api_keys = True
    # Each key is its own record on the proxy, so it can be minted, paused, resumed,
    # reconfigured and deleted alone. Its model list is enforced by the proxy; of its
    # limits, those with a proxy field (``_LIMIT_FIELDS``) become a backstop on it.
    supports_resource_api_key_lifecycle = True
    # A key's per-period limits reach the proxy as budget windows (BUDGET_WINDOWS).
    supports_spending_limit_periods = True
    # On an offering with both a chat surface and per-key management, a person's chat
    # follows the key assigned to them: open while it serves, limited to its models.
    # The plugin learns who holds which key from the core -- set per instance below,
    # since an API-only offering has no use for it and each ask costs a Waldur listing.
    requires_resource_key_assignees = False

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Initialize the backend from offering settings and components."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "litellm"

        api_url = backend_settings.get("api_url")
        if not api_url:
            msg = "LiteLLM backend requires 'api_url' in backend_settings"
            raise BackendError(msg)
        self.api_url = str(api_url).rstrip("/")

        # Optional per-offering allowlist pushed onto every key the offering mints.
        # An empty list means "every model the proxy serves"; LiteLLM treats an absent
        # `models` that way, so it is only sent when non-empty.
        models = backend_settings.get("models") or []
        self.models = [str(model) for model in models]
        configured_duration = backend_settings.get("budget_duration")
        if configured_duration not in (None, BUDGET_DURATION):
            logger.warning(
                "Ignoring budget_duration=%r: the max_budget backstop always resets on "
                "the 1st of the month (%r), the month Waldur counts limits in",
                configured_duration,
                BUDGET_DURATION,
            )
        self.budget_duration = BUDGET_DURATION
        # The components metered by the proxy's own spend, whose spending limits the
        # key's budgets mirror: ``token_cost``, and any component ``component_metrics``
        # feeds from it (``inference_cost: token_cost``). A spending limit on another
        # component has no proxy field and is Waldur's to enforce.
        self.spend_components = {"token_cost"} | {
            str(name)
            for name, metric in dict(backend_settings.get("component_metrics") or {}).items()
            if metric == "token_cost"
        }
        self.default_tpm_limit = backend_settings.get("tpm_limit")
        self.default_rpm_limit = backend_settings.get("rpm_limit")

        self.backend_id_source = str(
            backend_settings.get("resource_backend_id_source") or BACKEND_ID_SOURCE_SLUG
        )
        if self.backend_id_source not in _BACKEND_ID_SOURCES:
            msg = (
                f"Unknown resource_backend_id_source {self.backend_id_source!r}; "
                f"expected one of {', '.join(_BACKEND_ID_SOURCES)}"
            )
            raise BackendError(msg)

        # The limits of the resource currently being provisioned, as
        # ``(backend_id, limits)``. The core mints the keys in a separate call
        # (``generate_resource_keys``) that carries only the backend id, so the limits
        # read in ``_provision`` are parked here for it. Same process, same order, a
        # few calls apart; anything that misses this window is repaired by
        # ``sync_resource_limits`` on the next cycle.
        self._pending_limits: Optional[tuple[str, dict]] = None

        # Resources this instance last paused or downscaled, until it restores them.
        # The proxy has no object for a resource, so a resource with no keys has
        # nowhere on it to record a pause. Waldur holds the authoritative flag, and the
        # core re-applies it on this same instance right before every per-key mint
        # (``_apply_resource_pause``), so a key added to a keyless paused resource
        # still lands blocked.
        self._paused_resources: set = set()

        self.litellm_client = LiteLLMClient(backend_settings)

        # Optional chat surface. Absent settings mean "API only": the offering sells
        # keys and nothing here touches Open WebUI, which is why this is a nested block
        # rather than a set of top-level settings -- an offering that does not sell chat
        # should not have to name empty values for it.
        openwebui_settings = backend_settings.get("openwebui") or {}
        self.openwebui_settings = dict(openwebui_settings)
        self.openwebui_client = (
            OpenWebUIClient(self.openwebui_settings) if self.openwebui_settings else None
        )
        self.openwebui_url = str(self.openwebui_settings.get("url") or "").rstrip("/")
        self.requires_resource_key_assignees = self.openwebui_client is not None
        self.account_provisioning = str(
            self.openwebui_settings.get("account_provisioning") or PROVISIONING_SSO
        )
        self.initial_password = self.openwebui_settings.get("initial_password")
        if (
            self.openwebui_client
            and self.account_provisioning == PROVISIONING_MANAGED_PASSWORD
            and not self.initial_password
        ):
            msg = (
                "Open WebUI account_provisioning is 'managed_password' but no "
                "'initial_password' is set; the agent has no other way to give the "
                "person a credential they can sign in with"
            )
            raise BackendError(msg)
        # Whether losing access demotes the account or deletes it. Demotion is the
        # default: it ends access just as completely (Open WebUI refuses a 'pending'
        # account) while keeping the person's chat history, so re-adding them later
        # restores what they had instead of handing back an empty product.
        self.remove_chat_accounts = bool(self.openwebui_settings.get("delete_accounts_on_removal"))

        # How long a removed member's user record outlives their removal, in calendar
        # months counting the current one. The record is what bills the chat usage they
        # had before they left, so it has to cover every month the agent still reports;
        # the default covers the largest ``reporting_periods`` the agent accepts.
        retention = backend_settings.get("ownership_retention_months")
        self.ownership_retention_months = (
            ownership.DEFAULT_RETENTION_MONTHS if retention is None else int(retention)
        )
        if self.ownership_retention_months < 1:
            msg = "ownership_retention_months must be at least 1 (the current month)"
            raise BackendError(msg)

        # Where chat runs on Open WebUI's shared key, the proxy never counts it against a
        # person's own key, so the key's budget is written net of their chat: read from
        # the spend API, like the usage report. An API-only offering has no chat to read.
        # The chat is the calendar month's, the window the budget resets on.
        self.usage_client = (
            LiteLLMUsageClient(backend_settings) if self.openwebui_client is not None else None
        )
        # ``(fetched at, (start, end), {email: spend})``, cached like the user listing.
        self._chat_spend_cache: Optional[tuple[float, tuple, dict]] = None

        # ``/user/list`` results, cached for part of a pass -- see _USER_CACHE_TTL.
        self._users_cache: Optional[tuple[float, dict]] = None
        # ``{model: group id}`` of the chat model groups, and the ids of every group
        # under the plugin's prefix, retired models' included; cached like the users.
        self._model_groups_cache: Optional[tuple[float, dict, frozenset]] = None

    # --- health / introspection -------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Check the LiteLLM proxy is reachable and its key database is live."""
        if self.litellm_client.ping():
            return True
        if raise_exception:
            msg = "LiteLLM backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log backend configuration and report reachability."""
        logger.info("=== LiteLLM backend diagnostics ===")
        logger.info("Proxy URL: %s", self.api_url)
        logger.info("Model allowlist: %s", self.models or "<all models>")
        logger.info(
            "Key backstops: budget_duration=%s tpm=%s rpm=%s",
            self.budget_duration,
            self.default_tpm_limit,
            self.default_rpm_limit,
        )
        logger.info("Components: %s", list(self.backend_components.keys()))
        if self.openwebui_client is None:
            logger.info("Chat surface: not configured (API-only offering)")
        else:
            logger.info(
                "Chat surface: %s (provisioning=%s, removal=%s, reachable=%s)",
                self.openwebui_client.api_url,
                self.account_provisioning,
                "delete" if self.remove_chat_accounts else "disable",
                self.openwebui_client.ping(),
            )
        return self.ping(raise_exception=False)

    def list_components(self) -> list:
        """Return the configured component names (token meters)."""
        return list(self.backend_components.keys())

    # --- key lookup -------------------------------------------------------------

    @staticmethod
    def _alias_prefix(resource_backend_id: str) -> str:
        return f"{resource_backend_id}-"

    @staticmethod
    def _slot_pattern(resource_backend_id: str) -> re.Pattern:
        """Match only this resource's own slots.

        The pattern is anchored on both ends and the number is the whole tail, never a
        prefix: matching by prefix alone lets resource ``proj`` capture ``proj-extra-1``,
        so a pause or a terminate would fan out across resource boundaries.
        """
        return re.compile(rf"^{re.escape(resource_backend_id)}-\d+$")

    def _resource_keys(self, resource_backend_id: str) -> dict:
        """Return ``{alias: record}`` for every key the resource owns.

        ``/key/list`` narrows by substring server-side, which is not precise enough on
        its own, so the exact slot pattern is applied to what comes back.
        """
        pattern = self._slot_pattern(resource_backend_id)
        found = {}
        for record in self.litellm_client.list_keys(self._alias_prefix(resource_backend_id)):
            alias = record.get("key_alias")
            if alias and pattern.match(alias):
                found[alias] = record
        return found

    @staticmethod
    def _token(record: dict) -> Optional[str]:
        """Return the sha256 handle LiteLLM addresses a key by."""
        return record.get("token") or record.get("token_id")

    def list_resource_client_ids(self, resource_backend_id: str) -> list:
        """Return the client-ids (aliases) of every key a resource owns."""
        return sorted(self._resource_keys(resource_backend_id))

    @classmethod
    def _require_token(cls, alias: str, record: dict) -> str:
        """The key's hash, or an error: without it the key cannot be addressed."""
        token = cls._token(record)
        if not token:
            msg = f"LiteLLM returned no token handle for key {alias}"
            raise LiteLLMBackendError(msg)
        return token

    @staticmethod
    def _key_metadata(record: dict) -> dict:
        """A copy of the key's ``metadata``, safe to change and write back whole.

        Always written back whole, merged with what is there: whether ``/key/update``
        merges or replaces ``metadata``, a full dict leaves nothing behind.
        """
        metadata = record.get("metadata")
        return dict(metadata) if isinstance(metadata, dict) else {}

    def _current_metadata(self, token: str, record: dict) -> dict:
        """The key's metadata read again just before it is written back whole.

        ``/key/update`` replaces ``metadata``, so a write built from a listing taken
        earlier would drop whatever another command recorded since -- a key's own pause,
        say, which the next resource restore would then undo. Falls back to the listing
        when the proxy answers with no metadata to merge onto.
        """
        current = self.litellm_client.get_key(token)
        if isinstance(current, dict) and isinstance(current.get("metadata"), dict):
            return dict(current["metadata"])
        return self._key_metadata(record)

    @classmethod
    def _is_self_paused(cls, record: dict) -> bool:
        """Whether the key is paused on its own, rather than with its resource."""
        return bool(cls._key_metadata(record).get(META_KEY_PAUSED))

    def _mapped_limits(
        self, limits: Optional[dict], spending_limits: Optional[dict] = None
    ) -> dict:
        """The limits the proxy has a field for, by Waldur component name (``_BUDGET_NAMES``).

        ``limits`` is flat, ``{component: amount}``, as a resource's limit-based
        components and a key's month limits come. ``spending_limits`` is per period,
        ``{component: {period: amount}}``; only the components metered by the proxy's
        spend reach it, the tightest of them per period, and only the periods it has a
        window for (``_SPEND_LIMITS``).
        """
        mapped = {
            name: value
            for name, value in (limits or {}).items()
            if name in _LIMIT_FIELDS and value is not None
        }
        for component, periods in (spending_limits or {}).items():
            if component not in self.spend_components:
                continue
            for period, amount in periods.items():
                name = _SPEND_LIMITS.get(period)
                if name is not None and amount is not None:
                    mapped[name] = amount if name not in mapped else min(mapped[name], amount)
        return mapped

    def _key_own_limits(self, limits: Optional[dict], spending_limits: Optional[dict]) -> dict:
        """A key's own limits from its command: per period where Waldur sends them.

        A Waldur with per-period key limits sends them as ``spending_limits`` and the
        month alone again as ``limits``; ``spending_limits`` is the whole of it then.
        One without sends ``limits`` only, where a 0 means no limit of the key's own.
        """
        if spending_limits is None:
            return self._mapped_limits(limits)
        monthly = {
            component: periods["month"]
            for component, periods in spending_limits.items()
            if periods.get("month") is not None
        }
        return self._mapped_limits(monthly, spending_limits)

    def _resource_budget_limits(
        self, records: dict, limits: Optional[dict], spending_limits: Optional[dict]
    ) -> dict:
        """The resource's limits for its keys, keeping its spending limits when not given.

        ``spending_limits`` None means they are not at hand -- an order carries the
        limit-based components only, and a listing from a client that predates the
        field leaves them out -- so the ones last stamped on the keys stay.
        """
        mapped = self._mapped_limits(limits, spending_limits)
        if spending_limits is None:
            stamped = self._resource_limits_from_records(records)
            mapped.update(
                {name: value for name, value in stamped.items() if name in _SPEND_LIMITS.values()}
            )
        return mapped

    def _resource_is_paused(self, resource_backend_id: str, records: dict) -> bool:
        """Whether the resource itself is paused, as opposed to some of its keys.

        Used so a key minted onto -- or re-applied to -- a paused resource lands
        blocked rather than silently un-pausing it and serving traffic past its quota.

        A key can be blocked for two reasons now: its resource is paused, or it was
        paused on its own (``_is_self_paused``). Only the first says anything about
        the resource.
        """
        if resource_backend_id in self._paused_resources:
            return True
        # Without the record above (another instance paused it, or the agent
        # restarted since), the keys are the evidence: every key not paused on its own
        # is blocked. No such key left to judge by means not paused.
        blocked = [
            bool(record.get("blocked"))
            for record in records.values()
            if not self._is_self_paused(record)
        ]
        return bool(blocked) and all(blocked)

    # --- provisioning -----------------------------------------------------------

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        del waldur_resource, user_context  # no prerequisite setup

    def _client_id(self, waldur_resource: WaldurResource) -> str:
        return str(getattr(waldur_resource.uuid, "hex", waldur_resource.uuid))

    def _provision(self, backend_id: str, waldur_resource: WaldurResource) -> BackendResourceInfo:
        """Register the resource without minting keys.

        The core generates the keys separately (``generate_resource_keys``) and pushes
        each to Waldur, so nothing secret travels in ``backend_metadata``.
        """
        limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}
        # Handed to the keys minted right after this, so a resource does not spend its
        # first cycle on the offering-wide defaults alone.
        self._pending_limits = (
            backend_id,
            self._mapped_limits(
                limits, resource_spending_limits(waldur_resource) or {}
            ),
        )
        logger.info("Registered LiteLLM resource %s", waldur_resource.uuid)
        return BackendResourceInfo(
            backend_id=backend_id,
            limits=limits,
            backend_metadata={},
            # The chat URL is the offering's, not the proxy's, so it is only surfaced
            # when the operator has actually named one -- otherwise the portal would
            # advertise a chat surface that does not exist for this offering.
            endpoints=self._endpoints(),
        )

    def _endpoints(self) -> list:
        """Access endpoints to surface on the resource in the portal."""
        endpoints = [{"name": "OpenAI API", "url": f"{self.api_url}/v1"}]
        if self.openwebui_url:
            endpoints.append({"name": "Chat", "url": self.openwebui_url})
        return endpoints

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Register the resource under the agent-supplied backend id.

        This is the method the order processor actually calls (it derives the
        backend_id from the resource and passes it in), so provisioning lives here.

        With ``resource_backend_id_source: uuid`` the slug-derived id the processor
        passes is replaced by the resource UUID; the processor records whatever
        comes back as the backend id. A restore passes the resource's existing
        backend id, which is kept as it is: switching the setting on must not move a
        resource that was already provisioned under its slug.
        """
        del user_context
        backend_id = resource_backend_id
        if (
            self.backend_id_source == BACKEND_ID_SOURCE_UUID
            and resource_backend_id != waldur_resource.backend_id
        ):
            backend_id = self._client_id(waldur_resource)
            logger.info(
                "Using resource UUID %s as backend id instead of %s",
                backend_id,
                resource_backend_id,
            )
        return self._provision(backend_id, waldur_resource)

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> BackendResourceInfo:
        """Register the resource with backend_id = resource UUID (direct calls/tests)."""
        del user_context
        return self._provision(self._client_id(waldur_resource), waldur_resource)

    def _pull_backend_resource(self, resource_backend_id: str) -> Optional[BackendResourceInfo]:
        """Report the resource as existing when it owns at least one key.

        The order processor calls ``pull_resource`` before (re)creating a resource that
        already carries a ``backend_id``. Without this the base goes through
        ``UnknownClient`` (always ``None``) and the processor re-provisions a resource
        that is already there.
        """
        if self._resource_keys(resource_backend_id):
            try:
                # The membership processor diffs this against the project team to work
                # out who to add and who has gone stale.
                users = self.list_resource_users(resource_backend_id)
            except BackendError:
                # Existence is decided by the keys alone, so a failed ``/user/list``
                # must not read as "resource missing": ``pull_resource`` swallows the
                # error and the order processor answers ``None`` by re-creating a live
                # resource, minting a second full set of keys for it. An empty member
                # list only makes the next membership pass re-add the members, which
                # is idempotent.
                logger.exception(
                    "Unable to list the users of resource %s; reporting it as existing "
                    "with no members rather than as missing",
                    resource_backend_id,
                )
                users = []
            return BackendResourceInfo(backend_id=resource_backend_id, users=users)
        return None

    def recreate_missing_resource(self, waldur_resource: WaldurResource) -> bool:
        """No automatic recreation.

        The agent does not keep key material — LiteLLM hashes it and Waldur holds the
        only copy — so a key that vanished from the proxy cannot be restored, only
        replaced. Replacement is portal-driven (rotate mints a new one and reports it),
        so a forced sync reports state instead of minting behind the user's back.
        """
        backend_id = waldur_resource.backend_id
        if backend_id and not self._resource_keys(backend_id):
            logger.warning(
                "Resource %s has no keys on the LiteLLM proxy; rotate from the portal "
                "to mint a replacement.",
                waldur_resource.uuid,
            )
        return False

    # --- key management (agent-driven, proxy-minted) ----------------------------

    def generate_resource_keys(
        self, resource_backend_id: str, count: int = _DEFAULT_KEY_COUNT
    ) -> Iterator[dict]:
        """Mint ``count`` new keys, yielding each as soon as the proxy accepts it.

        Aliases are ``<resource_backend_id>-<n>``, numbered past whatever the resource
        already has so a re-run never collides with a live key — and LiteLLM rejects a
        duplicate alias outright, so a colliding slot would fail the whole cycle rather
        than quietly overwrite.

        On a paused resource the new keys are minted ``blocked``: a live key added to a
        paused resource un-pauses it in practice and serves traffic past its quota.

        Yields rather than returning a list so the caller reports each key before the
        next is minted. Minting all of them first strands any key created before a
        mid-loop failure — live at the proxy, with no row in Waldur to rotate it by.
        """
        existing = self._resource_keys(resource_backend_id)
        blocked = self._resource_is_paused(resource_backend_id, existing)
        limits = self._take_pending_limits(resource_backend_id)
        free = self._free_aliases(resource_backend_id, set(existing))
        for _ in range(count):
            alias = next(free)
            response = self._mint(
                alias, blocked=blocked, limits=limits, metadata={META_RESOURCE_LIMITS: limits}
            )
            logger.info("Generated LiteLLM key %s (blocked=%s)", alias, blocked)
            yield {"client_id": alias, "api_key": response["key"]}

    def _free_aliases(self, resource_backend_id: str, taken: set) -> Iterator[str]:
        """The resource's slots in order, skipping those in ``taken``."""
        prefix = self._alias_prefix(resource_backend_id)
        slot = 1
        while True:
            alias = f"{prefix}{slot}"
            slot += 1
            if alias not in taken:
                yield alias

    def _take_pending_limits(self, resource_backend_id: str) -> dict:
        """Consume the limits parked by ``_provision`` for this resource, if any, mapped."""
        pending = self._pending_limits
        if pending is None or pending[0] != resource_backend_id:
            return {}
        self._pending_limits = None
        return pending[1]

    def _backstop_fields(self, limits: dict) -> dict:
        """The full target state of a key's backstop fields for these limits.

        Every field is always present, ``None`` where there is to be no cap, because
        this is a reconciliation target and not a patch. Emitting only the fields the
        resource currently carries makes the backstop one-way: drop ``tpm`` from a
        resource's limits and the old ``tpm_limit`` stays on every key forever,
        throttling it by a limit Waldur no longer holds and which no later cycle can
        clear. A ``None`` here is sent to ``/key/update`` and removes the cap.

        The resource's own limit wins; the offering-wide default is what a field falls
        back to, so clearing a resource limit returns the key to the offering default
        rather than to no cap at all.
        """
        # What a field falls back to when the resource does not set that limit. There
        # is no offering-wide budget, so max_budget falls back to no cap.
        defaults = {
            "max_budget": None,
            "tpm_limit": self.default_tpm_limit,
            "rpm_limit": self.default_rpm_limit,
        }
        fields: dict = {}
        for name, field in _LIMIT_FIELDS.items():
            value = limits.get(name)
            fields[field] = defaults[field] if value is None else value
        # A month spending limit is the same budget as a limit-based ``token_cost``.
        month = limits.get(_SPEND_LIMITS["month"])
        if month is not None:
            budget = fields["max_budget"]
            fields["max_budget"] = month if budget is None else min(budget, month)
        # Only meaningful alongside a budget, and a stale duration on a key that no
        # longer has one would outlive the budget it belonged to.
        fields["budget_duration"] = (
            self.budget_duration if fields["max_budget"] is not None else None
        )
        # ``[]`` where there is no window, so a removed one is cleared too.
        fields["budget_limits"] = [
            {"max_budget": limits[_SPEND_LIMITS[period]], "budget_duration": duration}
            for period, duration in BUDGET_WINDOWS.items()
            if limits.get(_SPEND_LIMITS[period]) is not None
        ]
        return fields

    @staticmethod
    def _windows(value: object) -> list:
        """A key's ``budget_limits`` as ``[(duration, amount)]``, to compare with a target."""
        if not isinstance(value, list):
            return []
        return sorted(
            (str(window.get("budget_duration")), float(window.get("max_budget") or 0.0))
            for window in value
            if isinstance(window, dict)
        )

    @classmethod
    def _fields_match(cls, record: dict, fields: dict) -> bool:
        """Whether the key already carries these backstop fields."""
        return all(
            cls._windows(record.get(field)) == cls._windows(value)
            if field == "budget_limits"
            else record.get(field) == value
            for field, value in fields.items()
        )

    @staticmethod
    def _awaiting_reset(record: dict) -> bool:
        """Whether a window of the key has passed its reset and the proxy has not reset it yet.

        The proxy resets an expired window from a job that runs every ten minutes or
        so, and until then counts the old window's spend. Any ``budget_limits`` write
        sets each window's reset to the next one from now while keeping that spend:
        written at 00:03, a key that spent its day's budget yesterday would be refused
        until the following midnight -- a week, for a weekly window. The windows are
        left as they are until the job has run; the next write puts them right.
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        for window in record.get("budget_limits") or []:
            reset_at = window.get("reset_at") if isinstance(window, dict) else None
            if not reset_at:
                continue
            try:
                moment = datetime.datetime.fromisoformat(str(reset_at).replace("Z", "+00:00"))
            except ValueError:
                continue
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=datetime.timezone.utc)
            if moment <= now:
                return True
        return False

    @classmethod
    def _key_update_fields(cls, record: dict, fields: dict) -> dict:
        """``fields`` for a ``/key/update``, without a ``budget_duration`` the key carries.

        The proxy moves ``budget_reset_at`` to the next reset whenever an update names a
        ``budget_duration``. On the 1st, before its reset job has run, that would skip
        the month's reset and leave the key last month's spend: so it is only sent when
        it changes.

        ``budget_limits`` moves every window's reset the same way, so it is only sent
        when a window changes, and not while one awaits its reset (``_awaiting_reset``).
        A key that has no window and is to have none is never sent the field, which a
        proxy that predates windows may not take.
        """
        dropped = set()
        if record.get("budget_duration") == fields.get("budget_duration"):
            dropped.add("budget_duration")
        if "budget_limits" in fields:
            if cls._windows(record.get("budget_limits")) == cls._windows(fields["budget_limits"]):
                dropped.add("budget_limits")
            elif cls._awaiting_reset(record):
                logger.info(
                    "Leaving the budget windows of key %s as they are until the proxy "
                    "has reset the one past its reset time",
                    record.get("key_alias"),
                )
                dropped.add("budget_limits")
        return {field: value for field, value in fields.items() if field not in dropped}

    def _limits_from_records(self, records: dict) -> dict:
        """Read the backstop back off the keys the proxy already holds.

        A rotation mints a replacement without ever seeing Waldur's limits -- it is
        handed a client_id, not a resource -- so without this the new key comes back
        with no ``max_budget`` while its siblings keep theirs. ``sync_resource_limits``
        would repair it, but only on the next membership_sync pass, leaving an
        uncapped key in circulation for up to a reporting period. The existing keys
        are the authority that is actually in hand here.
        """
        limits: dict = {}
        # A window's name by its duration: ``1d`` is ``spend/day``.
        window_names = {
            duration: _SPEND_LIMITS[period] for period, duration in BUDGET_WINDOWS.items()
        }
        for record in records.values():
            if not isinstance(record, dict):
                continue
            for name, field in _LIMIT_FIELDS.items():
                value = record.get(field)
                if name not in limits and value is not None:
                    limits[name] = value
            for window in record.get("budget_limits") or []:
                if not isinstance(window, dict) or window.get("max_budget") is None:
                    continue
                window_name = window_names.get(str(window.get("budget_duration")))
                if window_name is not None and window_name not in limits:
                    limits[window_name] = window["max_budget"]
        return limits

    def _resource_limits_from_records(self, records: dict) -> dict:
        """The resource's limits as the last reconciliation stamped them on its keys.

        A key minted between two reconciliations has no other source: the per-key
        command carries the key's own limits, not its resource's. Keys minted before
        the stamp existed fall back to their backstop fields.
        """
        for record in records.values():
            stamped = self._key_metadata(record).get(META_RESOURCE_LIMITS)
            if isinstance(stamped, dict):
                return dict(stamped)
        return self._limits_from_records(records)

    def _waldur_or_stamped_limits(
        self,
        records: dict,
        resource_limits: Optional[dict],
        resource_spending_limits: Optional[dict] = None,
    ) -> dict:
        """The resource's limits for a key being minted or rotated.

        Waldur's, when the core read them, win over the ones the last reconciliation
        stamped on the resource's keys: Waldur is the authority, the stamp lags a limit
        change by up to a membership-sync period, and a key that was its resource's
        only one leaves no sibling to read a stamp from. Without either (an older
        core, or a read that carried none) the stamp is used.

        ``resource_limits`` are the limit-based components, ``resource_spending_limits``
        the resource's day/week/month windows. Each part Waldur did not send stays as
        last stamped (``_resource_budget_limits``); ``{}`` is a resource with no windows
        and clears them.
        """
        if resource_limits is None and resource_spending_limits is None:
            return self._resource_limits_from_records(records)
        if resource_limits is None:
            # The windows from Waldur, the flat limits as stamped.
            resource_limits = {
                name: value
                for name, value in self._resource_limits_from_records(records).items()
                if name in _LIMIT_FIELDS
            }
        return self._resource_budget_limits(records, resource_limits, resource_spending_limits)

    @staticmethod
    def _tighter_limits(key_limits: dict, resource_limits: dict) -> dict:
        """The limit each proxy field enforces: the tighter of the key's and the resource's.

        Both map a name of ``_BUDGET_NAMES`` (``token_cost``, ``tpm``, ``rpm`` and the
        spending limits on the proxy's spend, per period) to a value.
        The result is what ``_backstop_fields`` turns into the key's proxy fields; a
        name neither side limits is left out, so the field falls back to the
        offering-wide default.

        In ``key_limits`` a 0 means the key has no limit of its own (the per-key
        contract), so it never wins over the resource's. In ``resource_limits`` a 0
        stays a cap of 0, as it always has on this plugin.
        """
        combined: dict = {}
        for name in _BUDGET_NAMES:
            own = key_limits.get(name)
            if not own:
                own = None
            shared = resource_limits.get(name)
            if own is None:
                value = shared
            elif shared is None:
                value = own
            else:
                value = min(own, shared)
            if value is not None:
                combined[name] = value
        return combined

    def _key_fields(
        self, own_limits: dict, resource_limits: dict, chat_spend: dict
    ) -> tuple[dict, dict]:
        """The key's backstop fields with its holder's chat taken off, and the amounts taken.

        Waldur counts a key's usage as its API spend plus its holder's chat, but the proxy
        checks ``max_budget`` against the key's own spend only: chat arrives on Open
        WebUI's shared key. Writing the budget net of the chat already made this month
        lets the proxy refuse the key once the two together reach the limit. Never below
        0, which refuses the key's next call. A key with no budget has nothing taken off.

        Each window is netted the same way, of the chat in its own period:
        ``chat_spend`` is ``{period: spend}`` for ``month`` and the ``BUDGET_WINDOWS``.
        The amounts taken come back the same way, the periods with none left out.
        """
        fields = self._backstop_fields(self._tighter_limits(own_limits, resource_limits))
        taken: dict = {}
        month = chat_spend.get("month")
        if month and fields["max_budget"] is not None:
            fields["max_budget"] = max(0.0, round(fields["max_budget"] - month, 6))
            taken["month"] = month
        periods = {duration: period for period, duration in BUDGET_WINDOWS.items()}
        for window in fields["budget_limits"]:
            period = periods[window["budget_duration"]]
            spend = chat_spend.get(period)
            if spend:
                window["max_budget"] = max(0.0, round(window["max_budget"] - spend, 6))
                taken[period] = spend
        return fields, taken

    @staticmethod
    def _recorded_chat(metadata: dict) -> dict:
        """The chat last taken off the key's budgets, ``{period: spend}``."""
        recorded: dict = {}
        month = float(metadata.get(META_HOLDER_CHAT) or 0.0)
        if month:
            recorded["month"] = month
        windows = metadata.get(META_HOLDER_CHAT_WINDOWS)
        if isinstance(windows, dict):
            recorded.update(
                {
                    period: float(spend)
                    for period, spend in windows.items()
                    if period in BUDGET_WINDOWS and spend
                }
            )
        return recorded

    @classmethod
    def _chat_deduction(cls, alias: str, chat_spend: Optional[dict], metadata: dict) -> dict:
        """What to take off the key's budgets: its holder's chat, or the last amounts taken.

        ``chat_spend`` is ``None`` when it could not be read. Falling back to no deduction
        would hand a key its holder's chat back as budget until the next pass.
        """
        if chat_spend is None:
            return cls._recorded_chat(metadata)
        return {
            period: round(spend, 6) for period, spend in chat_spend.get(alias, {}).items() if spend
        }

    @staticmethod
    def _record_chat_deduction(metadata: dict, deduction: dict) -> None:
        """Note the deductions on the key, and only those there are to note."""
        if deduction.get("month"):
            metadata[META_HOLDER_CHAT] = deduction["month"]
        else:
            metadata.pop(META_HOLDER_CHAT, None)
        windows = {period: spend for period, spend in deduction.items() if period in BUDGET_WINDOWS}
        if windows:
            metadata[META_HOLDER_CHAT_WINDOWS] = windows
        else:
            metadata.pop(META_HOLDER_CHAT_WINDOWS, None)

    def _holder_chat_spend(self, resource_backend_id: str) -> Optional[dict]:
        """``{alias: {period: spend}}``: each key's holder's chat, ``None`` if unreadable.

        Per period: the month, and the day and week the key's windows cover. The holder
        is whoever the key's alias is stamped on (``META_KEY``), the same person the usage
        report bills the key's chat to. Only the days their address belonged to this
        resource count, as in the report: chat from before they moved here is billed to the
        resource they left, so taking it off this key would refuse the key at an amount
        Waldur does not show. Only read when the resource has holders, so an offering whose
        chat runs by membership never asks.
        """
        if self.usage_client is None:
            return {}
        try:
            holders = {}
            for email, record in self._resource_user_records(resource_backend_id).items():
                metadata = self._user_metadata(record)
                if metadata.get(META_KEY):
                    holders[email] = (str(metadata[META_KEY]), ownership.periods_of(metadata))
            if not holders:
                return {}
            # Read once, so the periods and the rows they are cut from agree at midnight.
            starts = self._chat_period_starts()
            by_user = self._chat_spend_by_user(starts)
        except BackendError:
            logger.exception(
                "Unable to read the chat spend of resource %s's key holders; keeping the "
                "amounts last taken off their keys' budgets",
                resource_backend_id,
            )
            return None
        spend: dict = {}
        for email, (alias, periods) in holders.items():
            for day, amount in by_user.get(email, {}).items():
                # A row with no day goes to the current owner's month, as in the report.
                if day:
                    owner = ownership.owner_on(periods, day)
                else:
                    owner = ownership.current_owner(periods)
                if not amount or owner != resource_backend_id:
                    continue
                totals = spend.setdefault(alias, {})
                for name, start in starts.items():
                    if (day >= start) if day else name == "month":
                        totals[name] = totals.get(name, 0.0) + amount
        return spend

    @staticmethod
    def _chat_period_starts() -> dict:
        """``{period: YYYY-MM-DD}``: the first day of this month and of the ``BUDGET_WINDOWS``.

        Today, and the week from Monday, which can reach into last month.
        """
        today = datetime.datetime.now(tz=datetime.timezone.utc).date()
        return {
            "month": today.replace(day=1).isoformat(),
            "week": (today - datetime.timedelta(days=today.weekday())).isoformat(),
            "day": today.isoformat(),
        }

    def _chat_spend_by_user(self, starts: dict) -> dict:
        """``{email: {day: spend}}`` of chat on the proxy, cached for part of a pass.

        From the start of the earliest of ``starts`` (``_chat_period_starts``) to today.
        One spend-API walk covers every resource, so the membership pass, which asks per
        resource, makes it once. Raises ``BackendError`` when the proxy cannot be read.
        """
        if self.usage_client is None:
            return {}
        period = (min(starts.values()), starts["day"])
        now = time.monotonic()
        cached = self._chat_spend_cache
        if cached is not None and cached[1] == period and now - cached[0] < _USER_CACHE_TTL:
            return cached[2]
        spend = chat_spend_by_user(self.usage_client.get_user_usage_rows(*period))
        self._chat_spend_cache = (now, period, spend)
        return spend

    def _key_models(self, allowed_models: Optional[list]) -> list:
        """The ``models`` to write on a key for the list Waldur sent.

        The proxy reads an empty ``models`` as every model, so the empty list Waldur
        sends for "no models" is written as ``[NO_MODELS]``. Waldur sends it when the
        key's own models are all switched off on its resource or retired from the
        offering, and the key must stop calling them. A name the offering does not
        serve is refused, since the contract has a backend raise rather than accept a
        setting it cannot honour.

        ``*`` stands for every model only alone: beside other names it is not a model
        name, and an unrestricted offering would write it onto the key as one -- where
        the proxy may read it as every model while Waldur shows a narrower list.
        """
        if allowed_models is None or list(allowed_models) == [ALL_MODELS]:
            return list(self.models)
        if not allowed_models:
            return [NO_MODELS]
        names = [str(model) for model in allowed_models]
        if ALL_MODELS in names:
            msg = (
                f"{ALL_MODELS!r} stands for every model only on its own, not beside "
                f"{', '.join(name for name in names if name != ALL_MODELS)}"
            )
            raise BackendError(msg)
        if self.models:
            outside = [name for name in names if name not in self.models]
            if outside:
                msg = (
                    f"The offering does not serve {', '.join(outside)}; a key can only "
                    f"be allowed models from {', '.join(self.models)}"
                )
                raise BackendError(msg)
        return names

    @staticmethod
    def _models_stamp(allowed_models: Optional[list]) -> list:
        """The key's model list as Waldur sent it, for ``META_KEY_MODELS``."""
        if allowed_models is None:
            return [ALL_MODELS]
        return [str(model) for model in allowed_models]

    def _reconciled_models(self, record: dict) -> Optional[list]:
        """The ``models`` a key should carry now, or None when it already does.

        A key's list was resolved against the offering's ``models`` when it was last
        written, and Waldur sends no update when the offering drops a model, so a
        model taken off the offering would stay callable on the key. It is resolved
        again from the list Waldur sent (``META_KEY_MODELS``): every model is the
        offering's list as it is now, and a named list keeps only the names the
        offering still serves, or none.

        A key written before the stamp existed only loses the models the offering no
        longer serves; nothing is added to it, since what Waldur allows it is not known.
        An unrestricted offering (no ``models``) drops nothing from such a key.
        """
        current = record.get("models")
        if not isinstance(current, list):
            current = []
        current = [str(model) for model in current]
        stamp = self._key_metadata(record).get(META_KEY_MODELS)
        if isinstance(stamp, list):
            names = [str(model) for model in stamp]
            if names == [ALL_MODELS]:
                wanted = list(self.models)
            elif not names:
                wanted = [NO_MODELS]
            elif self.models:
                wanted = [name for name in names if name in self.models] or [NO_MODELS]
            else:
                wanted = names
        else:
            if not self.models or not current:
                # An empty list on the proxy is every model; narrowing it would be a
                # guess at what Waldur allows the key.
                return None
            wanted = [
                name for name in current if name == NO_MODELS or name in self.models
            ] or [NO_MODELS]
        if sorted(wanted) == sorted(current):
            return None
        return wanted

    def _mint(
        self,
        alias: str,
        *,
        blocked: bool,
        limits: Optional[dict] = None,
        models: Optional[list] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        """Mint one key with a model allowlist and the rate backstops applied.

        The resource's own limits win over the offering-wide defaults: a key minted
        without them would carry no ``max_budget`` at all until the resource's limits
        were next edited, which is exactly the burst the backstop exists to stop.
        ``models`` defaults to the offering's allowlist.
        """
        fields = self._backstop_fields(limits or {})
        return self.litellm_client.generate_key(
            alias,
            models=self.models if models is None else models,
            blocked=blocked,
            max_budget=fields["max_budget"],
            budget_duration=fields["budget_duration"],
            budget_limits=fields["budget_limits"],
            tpm_limit=fields["tpm_limit"],
            rpm_limit=fields["rpm_limit"],
            metadata=metadata,
        )

    def rotate_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        known_client_ids: Optional[list] = None,
        limits: Optional[dict] = None,
        allowed_models: Optional[list] = None,
        resource_limits: Optional[dict] = None,
        spending_limits: Optional[dict] = None,
        resource_spending_limits: Optional[dict] = None,
    ) -> str:
        """Replace one key's material, keeping its alias, and return the new plaintext.

        The resource's other keys are untouched, so rotation is zero-downtime. The
        alias is a stable slot, so only the secret is returned.

        ``/key/{key}/regenerate`` does this in place but is enterprise-gated, so on an
        open-source proxy the fallback is delete-then-mint under the same alias — the
        delete is what frees the alias, which LiteLLM requires to be globally unique.

        ``known_client_ids`` is accepted and ignored: a rotation reuses an existing
        alias rather than allocating a new one, so a lost reply leaves no key behind
        for Waldur to lose track of.

        Waldur rotates only a key it shows as serving and settles it OK, so a key
        still paused on its own here is the residue of a pause whose acknowledgement
        was lost: its pause is lifted.

        ``limits`` and ``allowed_models`` are the key's own settings in Waldur. When
        they are given they are applied to the rotated key whichever way it is
        rotated, as an update would: Waldur rotates a key that changes hands, and the
        same edit may change its limits and models. Without them (Waldur could not be
        read) the key keeps the model list and limits it has here, and a key the proxy
        lost is minted back failing closed.

        ``resource_limits``, Waldur's limits of the resource, win over the stamp on its
        keys as on a mint: a lost key that was the resource's only one has no sibling
        to read them from; ``resource_spending_limits`` are its windows, as on a mint.
        ``spending_limits`` are the key's own per period, as on a mint
        (``_key_own_limits``).
        """
        del known_client_ids
        records = self._resource_keys(resource_backend_id)
        record = records.get(client_id)
        if record is None:
            return self._mint_lost_key(
                client_id,
                resource_backend_id,
                records,
                limits,
                allowed_models,
                self._waldur_or_stamped_limits(
                    records, resource_limits, resource_spending_limits
                ),
                spending_limits,
            )

        token = self._require_token(client_id, record)
        has_settings = limits is not None or allowed_models is not None
        # Checked before anything changes: a refused list leaves the key as it was.
        models = self._key_models(allowed_models) if has_settings else None
        # Read before anything changes: after the delete below this key is gone from
        # the listing, and a resource whose only other key is blocked would otherwise
        # look unpaused and get a live replacement.
        blocked = self._resource_is_paused(resource_backend_id, records)
        self_paused = self._is_self_paused(record)
        key_limits: Optional[dict] = None
        if has_settings:
            own_limits = self._key_own_limits(limits, spending_limits)
            key_resource_limits = self._waldur_or_stamped_limits(
                records, resource_limits, resource_spending_limits
            )
            # Read before the key's metadata, which is re-read to be written back whole.
            chat_spend = self._holder_chat_spend(resource_backend_id)
        # The delete-and-mint below carries this metadata over outside the lock; a
        # budget written meanwhile is recomputed by the next pass.
        with _KEY_METADATA_LOCK:
            metadata = {**self._current_metadata(token, record), META_KEY_PAUSED: False}
            if has_settings:
                fields, deduction = self._key_fields(
                    own_limits,
                    key_resource_limits,
                    self._chat_deduction(client_id, chat_spend, metadata),
                )
                metadata[META_KEY_LIMITS] = own_limits
                metadata[META_RESOURCE_LIMITS] = key_resource_limits
                metadata[META_KEY_MODELS] = self._models_stamp(allowed_models)
                self._record_chat_deduction(metadata, deduction)
                # What a delete-and-mint below mints with: these fields, read back as
                # limits, so the new key keeps the budget net of its holder's chat.
                key_limits = self._limits_from_records({client_id: fields})
                # Written before the regenerate, which changes the hash this write needs,
                # and lifting the key's own pause as an update does.
                self.litellm_client.update_key(
                    token,
                    {
                        **self._key_update_fields(record, fields),
                        "models": models,
                        "blocked": blocked,
                        "metadata": metadata,
                    },
                )
            elif self_paused:
                # Lifted before the regenerate, which changes the hash this write needs.
                self.litellm_client.update_key(
                    token, {"blocked": blocked, "metadata": metadata}
                )
        if has_settings:
            self._follow_key_chat(resource_backend_id, client_id, None if blocked else models)
        elif self_paused:
            # The core's restore ran while the key was still paused, so it skipped the
            # holder; their chat comes back with the key. A chat-side failure must not
            # fail the rotation: the next membership sync reopens it.
            try:
                current = self.litellm_client.get_key(token)
                for email, holder in self._key_holders(resource_backend_id, client_id).items():
                    self._apply_key_chat(
                        email,
                        None if blocked else (current if isinstance(current, dict) else record),
                        full_name=holder.get("user_alias"),
                    )
            except BackendError:
                logger.exception("Unable to reopen the chat of key %s's holder", client_id)

        try:
            new_key = self.litellm_client.regenerate_key(token)
        except LiteLLMEnterpriseFeatureError:
            logger.info(
                "/key/regenerate is enterprise-only on this proxy; rotating %s by "
                "delete-and-mint",
                client_id,
            )
        else:
            logger.info("Rotated LiteLLM key %s in place", client_id)
            return new_key

        if not has_settings:
            # Same reason the pause state is read above: after the delete the record
            # is gone, and with it the only in-hand copy of this key's backstop, model
            # list and metadata.
            key_limits = self._limits_from_records({client_id: record})
            kept = record.get("models")
            models = list(kept) if isinstance(kept, list) else None
        self.litellm_client.delete_keys([token])
        new_key = self._mint(
            client_id,
            blocked=blocked,
            limits=key_limits,
            models=models,
            metadata=metadata,
        )["key"]
        logger.info("Rotated LiteLLM key %s by delete-and-mint (blocked=%s)", client_id, blocked)
        return new_key

    def _follow_key_chat(
        self, resource_backend_id: str, client_id: str, models: Optional[list]
    ) -> None:
        """Bring the chat of the key's holders in line with its models, or close it.

        ``models`` None closes it. A chat-side failure must not fail the key command
        that called this: the next membership sync repairs it.
        """
        try:
            for email, holder in self._key_holders(resource_backend_id, client_id).items():
                self._apply_key_chat(
                    email,
                    None if models is None else {"models": models},
                    full_name=holder.get("user_alias"),
                )
        except BackendError:
            logger.exception("Unable to bring the chat of key %s's holder in line", client_id)

    def _mint_lost_key(
        self,
        client_id: str,
        resource_backend_id: str,
        records: dict,
        limits: Optional[dict],
        allowed_models: Optional[list],
        resource_limits: dict,
        spending_limits: Optional[dict] = None,
    ) -> str:
        """Mint a slot the proxy lost back, as restricted as the key Waldur shows.

        The slot is gone (removed out-of-band, or a rotation that died between the
        delete and the mint), and its own settings went with it, so they come from
        Waldur: its models and own limits, with the resource's limits as on a mint.
        The resource's pause holds it like any other key.

        With no settings at all (Waldur could not be read) it fails closed: minted
        under a pause of its own with no models, so it serves nothing until an update
        or a resume from Waldur writes its settings and lifts the pause. Minting it
        with the offering's models instead would widen a restricted key, and its chat.
        """
        if limits is None and allowed_models is None:
            logger.warning(
                "Key %s is missing from the proxy and its settings in Waldur are not "
                "known; minting a replacement paused with no models until an update "
                "or a resume from Waldur applies them",
                client_id,
            )
            models = [NO_MODELS]
            blocked = True
            limits_to_apply = resource_limits
            metadata = {META_KEY_PAUSED: True, META_RESOURCE_LIMITS: resource_limits}
        else:
            # Checked before anything is created: a refused list leaves nothing behind.
            models = self._key_models(allowed_models)
            own_limits = self._key_own_limits(limits, spending_limits)
            blocked = self._resource_is_paused(resource_backend_id, records)
            # Net of the holder's chat, as on a rotation: the full budget would let the
            # new key spend what its holder already used in chat until the next pass.
            # Unreadable chat takes nothing off; the lost key's last deduction went
            # with it.
            fields, deduction = self._key_fields(
                own_limits,
                resource_limits,
                self._chat_deduction(
                    client_id, self._holder_chat_spend(resource_backend_id), {}
                ),
            )
            limits_to_apply = self._limits_from_records({client_id: fields})
            metadata = {
                META_KEY_LIMITS: own_limits,
                META_RESOURCE_LIMITS: resource_limits,
                META_KEY_MODELS: self._models_stamp(allowed_models),
            }
            self._record_chat_deduction(metadata, deduction)
            logger.warning("Key %s is missing from the proxy; minting a replacement", client_id)
        new_key = self._mint(
            client_id,
            blocked=blocked,
            limits=limits_to_apply,
            models=models,
            metadata=metadata,
        )["key"]
        # The holder's chat follows the new key, as after an update.
        self._follow_key_chat(resource_backend_id, client_id, None if blocked else models)
        return new_key

    def prune_unknown_resource_keys(self, resource_backend_id: str, keep: list) -> None:
        """Drop the resource's keys that Waldur does not hold.

        Runs at provisioning, before anything is minted, so the only keys in scope are
        the residue of an interrupted earlier create: live at the proxy with no row in
        Waldur, which means nothing could ever rotate or revoke them.

        The core only calls this with a set it could actually read — ``None`` there
        means unknown and nothing is pruned — so an empty ``keep`` here is a genuine
        "Waldur holds none of these", not a failed lookup.
        """
        keep_set = set(keep)
        doomed = [
            token
            for alias, record in self._resource_keys(resource_backend_id).items()
            if alias not in keep_set and (token := self._token(record))
        ]
        if not doomed:
            return
        logger.info(
            "Pruning %d LiteLLM key(s) of resource %s that Waldur does not hold",
            len(doomed),
            resource_backend_id,
        )
        self.litellm_client.delete_keys(doomed)

    # --- per-key lifecycle ------------------------------------------------------

    def mint_resource_key(
        self,
        resource_backend_id: str,
        reserved_client_ids: list,
        limits: Optional[dict] = None,
        allowed_models: Optional[list] = None,
        resource_limits: Optional[dict] = None,
        spending_limits: Optional[dict] = None,
        resource_spending_limits: Optional[dict] = None,
    ) -> dict:
        """Add one key to the resource, as its next free ``<backend_id>-<n>`` slot.

        A slot Waldur still holds -- a deleted key's included -- is skipped even though
        the proxy no longer has it, because usage is attributed by alias. Like
        ``generate_resource_keys``, the key lands blocked on a paused resource.

        ``resource_limits``, Waldur's limits of the resource, win over the ones the
        last reconciliation stamped on its keys: Waldur is the authority, and the
        stamp lags a limit change by up to a membership-sync period. They are also the
        only source for a resource's first key, which has no siblings to read them
        from. ``resource_spending_limits`` are the resource's day/week/month windows
        from the same read, so a first key is held to them from its mint rather than
        from the next membership sync. Without them (an older core) the stamp is used
        as before.
        """
        # Checked before anything is created: a refused list leaves nothing behind.
        models = self._key_models(allowed_models)
        records = self._resource_keys(resource_backend_id)
        blocked = self._resource_is_paused(resource_backend_id, records)
        own_limits = self._key_own_limits(limits, spending_limits)
        resource_limits = self._waldur_or_stamped_limits(
            records, resource_limits, resource_spending_limits
        )
        alias = next(
            self._free_aliases(resource_backend_id, set(records) | set(reserved_client_ids))
        )
        response = self._mint(
            alias,
            blocked=blocked,
            limits=self._tighter_limits(own_limits, resource_limits),
            models=models,
            metadata={
                META_KEY_LIMITS: own_limits,
                META_RESOURCE_LIMITS: resource_limits,
                META_KEY_MODELS: self._models_stamp(allowed_models),
            },
        )
        logger.info("Minted LiteLLM key %s (blocked=%s)", alias, blocked)
        return {"client_id": alias, "api_key": response["key"]}

    def pause_resource_key(self, client_id: str, resource_backend_id: str) -> None:
        """Block one key under its own pause; its siblings keep serving.

        The pause is recorded on the key, so a resource restore leaves it blocked. A
        key the proxy no longer holds cannot serve, so it counts as paused.
        """
        record = self._resource_keys(resource_backend_id).get(client_id)
        if record is None:
            logger.warning(
                "LiteLLM key %s is missing from the proxy; it cannot serve, so it counts "
                "as paused",
                client_id,
            )
            for email in self._key_holders(resource_backend_id, client_id):
                self._apply_key_chat(email, None)
            return
        token = self._require_token(client_id, record)
        with _KEY_METADATA_LOCK:
            metadata = {**self._current_metadata(token, record), META_KEY_PAUSED: True}
            # One write for both, so the key is never blocked without the record that
            # keeps a resource restore from unblocking it, nor recorded paused while
            # serving.
            self.litellm_client.update_key(token, {"blocked": True, "metadata": metadata})
        logger.info("Paused LiteLLM key %s", client_id)
        # A key paused at its limit has to stop its holder's chat too, or the limit
        # holds for API calls only.
        for email in self._key_holders(resource_backend_id, client_id):
            self._apply_key_chat(email, None)

    def resume_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        limits: Optional[dict] = None,
        allowed_models: Optional[list] = None,
        spending_limits: Optional[dict] = None,
    ) -> None:
        """Lift a key's own pause and apply its settings; a paused resource still holds it."""
        self._configure_key(
            client_id, resource_backend_id, limits, allowed_models, spending_limits
        )
        logger.info("Resumed LiteLLM key %s", client_id)

    def update_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        limits: Optional[dict] = None,
        allowed_models: Optional[list] = None,
        spending_limits: Optional[dict] = None,
    ) -> None:
        """Apply a key's own limits and model list."""
        self._configure_key(
            client_id, resource_backend_id, limits, allowed_models, spending_limits
        )
        logger.info("Updated LiteLLM key %s", client_id)

    def _configure_key(
        self,
        client_id: str,
        resource_backend_id: str,
        limits: Optional[dict],
        allowed_models: Optional[list],
        spending_limits: Optional[dict] = None,
    ) -> None:
        """Write a key's own settings and lift its own pause, in one ``/key/update``.

        Resume and update both settle the key OK in Waldur, so either way it must serve
        afterwards, unless its resource is paused, which holds it with its siblings.
        An update lifts a stale pause too: one left by a pause whose acknowledgement
        never reached Waldur. Every field is written whole, so a replay changes nothing.
        """
        models = self._key_models(allowed_models)
        records = self._resource_keys(resource_backend_id)
        record = records.get(client_id)
        if record is None:
            msg = (
                f"LiteLLM key {client_id} is missing from the proxy; rotate it to mint "
                "a replacement"
            )
            raise LiteLLMBackendError(msg)
        token = self._require_token(client_id, record)
        own_limits = self._key_own_limits(limits, spending_limits)
        resource_limits = self._resource_limits_from_records(records)
        # Read before the key's metadata, which is re-read to be written back whole.
        chat_spend = self._holder_chat_spend(resource_backend_id)
        blocked = self._resource_is_paused(resource_backend_id, records)
        with _KEY_METADATA_LOCK:
            current = self._current_metadata(token, record)
            fields, deduction = self._key_fields(
                own_limits, resource_limits, self._chat_deduction(client_id, chat_spend, current)
            )
            metadata = {
                **current,
                META_KEY_PAUSED: False,
                META_KEY_LIMITS: own_limits,
                META_RESOURCE_LIMITS: resource_limits,
                META_KEY_MODELS: self._models_stamp(allowed_models),
            }
            self._record_chat_deduction(metadata, deduction)
            self.litellm_client.update_key(
                token,
                {
                    **self._key_update_fields(record, fields),
                    "models": models,
                    "blocked": blocked,
                    "metadata": metadata,
                },
            )
        # The holder's chat follows: reopened unless the resource holds the key, and
        # narrowed or widened to the key's models.
        for email, holder in self._key_holders(resource_backend_id, client_id).items():
            self._apply_key_chat(
                email,
                None if blocked else {"models": models},
                full_name=holder.get("user_alias"),
            )

    def delete_resource_key(self, client_id: str, resource_backend_id: str) -> None:
        """Revoke one key for good. A key the proxy no longer holds is already revoked.

        Its holder's chat closes with it, and their record stops pointing at it, so a
        resource restore does not reopen chat on a key that is gone.
        """
        record = self._resource_keys(resource_backend_id).get(client_id)
        if record is None:
            logger.info("LiteLLM key %s is already gone from the proxy", client_id)
        else:
            self.litellm_client.delete_keys([self._require_token(client_id, record)])
            logger.info("Deleted LiteLLM key %s", client_id)
        holders = self._key_holders(resource_backend_id, client_id)
        for email, holder in holders.items():
            self._apply_key_chat(email, None)
            self.litellm_client.update_user(
                email, {"metadata": {**self._user_metadata(holder), META_KEY: ""}}
            )
        if holders:
            self._invalidate_users_cache()

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: object) -> None:
        """Remove every one of the resource's keys from the proxy.

        Mirrors :meth:`pause_resource` on the key that cannot be addressed: without the
        hash there is no way to delete it, and it keeps serving after the resource is
        gone from Waldur — an orphan nothing can ever rotate or revoke. Every other key
        is still deleted (one bad record must not strand the rest), and the failure is
        then raised so the terminate order errs instead of reporting a clean removal.
        """
        del kwargs
        backend_id = waldur_resource.backend_id
        if not backend_id:
            logger.warning("No backend_id for resource %s; nothing to delete", waldur_resource.uuid)
            return
        # Members first, keys second. A member left behind after the keys are gone is
        # the dangerous residue: they can still chat (that traffic never touched this
        # resource's keys) and their spend accrues against a resource Waldur has
        # terminated. A key left behind while the members are gone only fails closed.
        self._remove_all_users(backend_id)

        tokens = []
        orphans = []
        for alias, record in self._resource_keys(backend_id).items():
            token = self._token(record)
            if token:
                tokens.append(token)
            else:
                logger.error(
                    "LiteLLM returned no token handle for key %s; it cannot be deleted "
                    "and will keep serving after resource %s is gone",
                    alias,
                    waldur_resource.uuid,
                )
                orphans.append(alias)
        self.litellm_client.delete_keys(tokens)
        logger.info(
            "Deleted %d LiteLLM key(s) for resource %s", len(tokens), waldur_resource.uuid
        )
        if orphans:
            msg = (
                f"LiteLLM key(s) {', '.join(orphans)} of resource {backend_id} have no "
                "token handle and could not be deleted"
            )
            raise LiteLLMBackendError(msg)

    def _remove_all_users(self, resource_backend_id: str) -> None:
        """Revoke every member of a resource that is going away.

        Failures are logged and do not stop the deletion: the keys still have to go,
        and a terminate that aborts halfway leaves a live resource in Waldur's past.
        """
        try:
            owned = self._resource_user_map(resource_backend_id)
        except BackendError:
            # The keys still have to go. Raising here would abort ``delete_resource``
            # before the key-deletion loop, so a listing failure would leave every key
            # of a terminated resource serving.
            logger.exception(
                "Unable to list the users of resource %s; continuing with key deletion",
                resource_backend_id,
            )
            return
        if not owned:
            return
        for email in sorted(owned.values()):
            try:
                self._revoke_chat_access(email, permanent=True)
            except BackendError:
                logger.exception("Unable to revoke chat access for %s", email)
        released = 0
        for email in sorted(owned.values()):
            try:
                self._release_litellm_user(email, resource_backend_id)
            except BackendError:
                logger.exception(
                    "Unable to release LiteLLM user %s from resource %s",
                    email,
                    resource_backend_id,
                )
            else:
                released += 1
        logger.info("Removed %d user(s) from resource %s", released, resource_backend_id)
        self._invalidate_users_cache()

    # --- state transitions ------------------------------------------------------

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Block every key of the resource.

        Reports success only when nothing is left serving. The return value is the
        processor's only success signal (it logs "Pausing is successfully completed"
        on True and nothing retries within the cycle), so reporting True while one key
        of the pair still answers would announce an enforced quota that is not
        enforced. Every key is still attempted — one failure must not strand the rest.

        A key the proxy no longer holds is not a failure: it cannot serve traffic, so
        the resource is paused as far as this is concerned.

        A key already paused on its own is blocked again, harmlessly; its own pause
        is left recorded, so the resource's restore does not lift it.
        """
        self._paused_resources.add(resource_backend_id)
        records = self._resource_keys(resource_backend_id)
        if not records:
            logger.warning(
                "Resource %s owns no keys on the LiteLLM proxy; nothing to pause",
                resource_backend_id,
            )
            return False

        # Both surfaces, because blocking the keys only closes the API one. Chat runs
        # on Open WebUI's shared key, which no per-resource block can reach.
        paused = self._set_chat_access_for_resource(resource_backend_id, enabled=False)
        for alias, record in records.items():
            token = self._token(record)
            if not token:
                # Without the hash there is no way to address the key, and it is still
                # serving — the same outcome as a failed block.
                logger.error("LiteLLM returned no token handle for key %s; cannot pause it", alias)
                paused = False
                continue
            try:
                if not self.litellm_client.block(token):
                    logger.info("Key %s is already gone from the proxy", alias)
            except LiteLLMBackendError:
                # A swallowed pause silently defeats quota enforcement — the over-limit
                # key keeps serving — so this is an error, not a quiet warning.
                logger.exception("Unable to pause (block) key %s", alias)
                paused = False
        return paused

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Unblock every key of the resource.

        The mirror of :meth:`pause_resource`, and asymmetric with it on purpose: a
        pause asks "is anything still serving?", a restore asks "is everything serving
        again?". So a key the proxy has lost fails a restore while it satisfies a
        pause — the resource comes back with fewer working keys than Waldur holds, and
        only a portal-driven rotation can replace one.

        A key paused on its own stays blocked: the resource coming back does not bring
        it back. It counts as restored, since it is in the state Waldur shows.

        Only a key read as blocked is unblocked, and it is read again just before: this
        runs every membership-sync cycle, and a per-key pause landing between the listing
        and the unblock would otherwise be undone -- after Waldur had already been told
        the key is paused, so nothing would ever pause it again.

        Where key budgets have their holders' chat taken off, they are recomputed here
        too (``_write_key_budgets``), from the limits stamped on each key.
        """
        self._paused_resources.discard(resource_backend_id)
        records = self._resource_keys(resource_backend_id)
        if not records:
            logger.warning(
                "Resource %s owns no keys on the LiteLLM proxy; nothing to restore",
                resource_backend_id,
            )
            return False

        restored = self._set_chat_access_for_resource(
            resource_backend_id, enabled=True, key_records=records
        )
        for alias, record in records.items():
            if self._is_self_paused(record) and not record.get("blocked"):
                # Paused on its own but serving: a pause that landed between an earlier
                # restore's re-read and its unblock. Nothing else would ever block it.
                # Read again first, as before an unblock: a resume landing since the
                # listing lifted that pause, and blocking now would undo it.
                token = self._token(record)
                if token:
                    try:
                        current = self.litellm_client.get_key(token)
                        if (
                            isinstance(current, dict)
                            and self._is_self_paused(current)
                            and not current.get("blocked")
                        ):
                            self.litellm_client.block(token)
                    except LiteLLMBackendError as exc:
                        logger.warning("Unable to re-block paused key %s: %s", alias, exc)
                continue
            if self._is_self_paused(record) or not record.get("blocked"):
                continue
            token = self._token(record)
            if not token:
                logger.warning("LiteLLM returned no token handle for key %s", alias)
                restored = False
                continue
            try:
                current = self.litellm_client.get_key(token)
                if current is None:
                    logger.warning("Key %s is gone from the proxy and cannot be restored", alias)
                    restored = False
                    continue
                if self._is_self_paused(current):
                    continue
                if not self.litellm_client.unblock(token):
                    logger.warning("Key %s is gone from the proxy and cannot be restored", alias)
                    restored = False
            except LiteLLMBackendError as exc:
                logger.warning("Unable to restore (unblock) key %s: %s", alias, exc)
                restored = False
        if self.usage_client is not None:
            # Event-based membership sync runs no periodic limits pass, but this runs on
            # every serving resource from the hourly status pass (and every polling
            # cycle): it keeps the chat taken off each key's budget current, and puts it
            # back once the month turns.
            self._write_key_budgets(resource_backend_id, records, None)
        return restored

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """A key has no partial-capacity state; block it as the safe interpretation."""
        return self.pause_resource(resource_backend_id)

    # --- limits -----------------------------------------------------------------

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple:
        # Limits are held and enforced by Waldur; nothing to read back from the proxy.
        del waldur_resource
        return {}, {}

    def set_resource_limits(self, resource_backend_id: str, limits: dict) -> None:
        """Mirror the resource's limits onto its keys as a metering backstop.

        Waldur stays the enforcement authority (report -> pause -> block); this only
        keeps a burst between two reporting cycles from outrunning the metering. Each
        key carries the resource's full budget rather than a share of it: the keys are
        alternatives for one consumer, not separate allowances, and splitting the
        budget would throttle a consumer using a single key to half its entitlement.

        Unlike the base implementation, ``unit_factor`` is deliberately not applied.
        The reporting side does not apply it either -- usage rows come back in the
        proxy's own units -- so both directions speak Waldur units and a limit can
        never end up measured differently from the usage it is compared against.
        Honouring it here alone would produce exactly that mismatch. The base also
        casts to ``int``, which is wrong for ``max_budget``: it is a float on the
        proxy, and rounding a fractional budget down is a silent loss.

        A key with limits of its own gets the tighter of its own and the resource's,
        field by field, and the resource's limits are stamped on every key so a key
        minted before the next cycle can start from them. Where chat follows the key,
        its holder's chat this month is taken off its budget (``_key_fields``); this
        pass is what keeps that current as they chat and as the key changes hands.

        ``limits`` are the limit-based components, which is all an order carries; the
        resource's spending limits stay as last reconciled (``sync_resource_limits``).

        Each key's model list is brought in line with the offering's ``models`` in the
        same write (``_reconciled_models``): a model dropped from the offering stops
        being callable on the keys that had it, without an update from Waldur.
        """
        records = self._resource_keys(resource_backend_id)
        self._write_key_budgets(
            resource_backend_id, records, self._resource_budget_limits(records, limits, None)
        )

    def _write_key_budgets(
        self, resource_backend_id: str, records: dict, resource_limits: Optional[dict]
    ) -> None:
        """Write each key's backstop fields, net of its holder's chat, where they changed.

        ``resource_limits`` ``None`` takes each key's stamped resource limits: the status
        pass refreshes the chat taken off without Waldur's limits at hand.
        """
        chat_spend = self._holder_chat_spend(resource_backend_id) if records else {}
        for alias, record in records.items():
            token = self._token(record)
            if not token:
                continue
            metadata = self._key_metadata(record)
            if resource_limits is None and META_RESOURCE_LIMITS not in metadata:
                # Never reconciled: its budget is the next limits sync's to write.
                continue
            own_limits = metadata.get(META_KEY_LIMITS) or {}
            key_resource_limits = (
                (metadata.get(META_RESOURCE_LIMITS) or {})
                if resource_limits is None
                else resource_limits
            )
            fields, deduction = self._key_fields(
                own_limits, key_resource_limits, self._chat_deduction(alias, chat_spend, metadata)
            )
            if (
                (metadata.get(META_RESOURCE_LIMITS) or {}) == key_resource_limits
                and self._recorded_chat(metadata) == deduction
                and self._fields_match(record, fields)
                and self._reconciled_models(record) is None
            ):
                # Reconciliation runs every membership-sync cycle; writing values the
                # key already carries would be one /key/update per key per cycle.
                continue
            try:
                with _KEY_METADATA_LOCK:
                    current = self.litellm_client.get_key(token)
                    if not isinstance(current, dict):
                        current = record
                    metadata = self._key_metadata(current)
                    own_limits = metadata.get(META_KEY_LIMITS) or {}
                    if resource_limits is None:
                        key_resource_limits = metadata.get(META_RESOURCE_LIMITS) or {}
                    fields, deduction = self._key_fields(
                        own_limits,
                        key_resource_limits,
                        self._chat_deduction(alias, chat_spend, metadata),
                    )
                    metadata[META_RESOURCE_LIMITS] = key_resource_limits
                    self._record_chat_deduction(metadata, deduction)
                    body = {**self._key_update_fields(current, fields), "metadata": metadata}
                    models = self._reconciled_models(current)
                    if models is not None:
                        logger.info("Bringing the models of key %s in line: %s", alias, models)
                        body["models"] = models
                    self.litellm_client.update_key(token, body)
                    self._keep_concurrent_pause(
                        alias, token, metadata, bool(current.get("blocked"))
                    )
            except LiteLLMBackendError as exc:
                # The backstop failing is not the same as enforcement failing: Waldur
                # still pauses on the reported usage, so this does not fail the order.
                logger.warning("Unable to apply limits to key %s: %s", alias, exc)

    def _keep_concurrent_pause(
        self, alias: str, token: str, written: dict, blocked_before: bool
    ) -> None:
        """Put back a key's own pause that a metadata write may have dropped.

        ``/key/update`` replaces ``metadata`` and the proxy has no compare-and-set, so
        a per-key pause (another process: the event loop) landing between this write's
        read and the write itself is overwritten, and the next resource restore would
        unblock a key Waldur shows paused. The key is read once more:

        - recorded paused, though this write said not: the pause landed after the
          write, so the flag stands but the pause wrote back a stale stamp -- the
          stamp is merged onto what is there now;
        - blocked since the read, but not recorded paused: something blocked it inside
          the window, and if it was a per-key pause its flag is what was dropped. It
          cannot be told from a resource pause, so it is recorded as the key's own --
          failing closed: a key that should serve stays blocked until Waldur resumes
          or updates it, rather than a paused key serving again.

        It narrows the window rather than closing it; a pause landing between this
        read and a later write still needs the next pass.
        """
        current = self.litellm_client.get_key(token)
        if not isinstance(current, dict):
            return
        metadata = self._key_metadata(current)
        paused_now = bool(metadata.get(META_KEY_PAUSED))
        if paused_now and not written.get(META_KEY_PAUSED):
            if metadata.get(META_RESOURCE_LIMITS) == written.get(META_RESOURCE_LIMITS):
                return
            metadata[META_RESOURCE_LIMITS] = written.get(META_RESOURCE_LIMITS)
        elif current.get("blocked") and not blocked_before and not paused_now:
            logger.warning(
                "Key %s was blocked while its limits were written; keeping it paused on "
                "its own in case the write dropped a key pause",
                alias,
            )
            metadata[META_KEY_PAUSED] = True
        else:
            return
        self.litellm_client.update_key(token, {"metadata": metadata})

    def sync_resource_limits(
        self, waldur_resource: WaldurResource, waldur_rest_client: AuthenticatedClient
    ) -> None:
        """Push Waldur's limits onto the resource's keys.

        The base implementation reconciles the other way — it pulls the backend's
        limits into Waldur — which is meaningless here: the proxy holds no limits of
        its own, so every cycle would read an empty set and log "No limits found in
        the backend".

        Waldur is the authority, so the reconciliation runs from Waldur outwards. It
        is what keeps a key minted before a limit change, or one re-minted by a
        rotation, carrying the resource's current backstop.
        """
        del waldur_rest_client
        backend_id = waldur_resource.backend_id
        if not backend_id:
            return
        # An empty limit set is reconciled too, rather than skipped: it is how a
        # resource whose limits were cleared gets its keys back to the offering
        # defaults. Skipping it would leave the removed caps in place indefinitely.
        limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}
        records = self._resource_keys(backend_id)
        self._write_key_budgets(
            backend_id,
            records,
            self._resource_budget_limits(
                records, limits, resource_spending_limits(waldur_resource)
            ),
        )

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Report whether any of the resource's keys is currently serving."""
        records = self._resource_keys(resource_backend_id)
        return {
            "backend_type": self.backend_type,
            "active": any(not record.get("blocked") for record in records.values()),
        }

    # --- users and chat access --------------------------------------------------
    #
    # A LiteLLM *user* is not a credential and does not gate anything. It exists so
    # that chat traffic can be billed: Open WebUI authenticates every upstream call
    # with one shared virtual key and forwards the signed-in person's address, which
    # the proxy stamps onto the request's ``user_id`` -- after authentication, without
    # looking the address up. Two consequences shape everything below.
    #
    # First, the user record is an *attribution* record. Budgets, rate limits and model
    # allowlists written onto it are not consulted for chat traffic, because that
    # request authenticated as the shared key and was checked against the shared key's
    # owner. So this plugin writes nothing onto the user but identity and ownership.
    # A ``max_budget`` here would read as an enforced cap that enforces nothing at all.
    # Enforcement stays where it already is -- Waldur meters, pauses, and the
    # agent blocks the keys and demotes the chat account.
    #
    # Second, removing the LiteLLM user does not end chat access. An address the proxy
    # has never seen is not rejected; it accrues daily spend rows against a user row
    # that does not exist. Revocation therefore has to happen in Open WebUI, which is
    # why every removal path here touches both systems.
    #
    # Third, the record outlives the membership. The reporting backend recomputes the
    # month on every pass and attributes each day's chat usage through the ownership
    # recorded here, so removing a member *releases* the record -- closes their period
    # on it -- rather than deleting it. Deleting took the month's usage they already
    # had off the resource's bill. The record goes once its last period is older than
    # any month the agent still reports (``_prune_expired_users``).

    @staticmethod
    def _email_of(record: dict) -> str:
        """The address a managed user is keyed by, normalized for comparison."""
        return str(record.get("user_id") or "").strip().lower()

    @staticmethod
    def _user_metadata(record: dict) -> dict:
        metadata = record.get("metadata")
        return metadata if isinstance(metadata, dict) else {}

    def _managed_users(self) -> dict:
        """Return ``{email: record}`` for every internal user on the proxy.

        The whole listing rather than one resource's slice, because ``/user/list``
        cannot filter on metadata and ownership only lives there. It is cached for part
        of a pass so the per-resource walk does not repeat it.
        """
        now = time.monotonic()
        cached = self._users_cache
        if cached is not None and now - cached[0] < _USER_CACHE_TTL:
            return cached[1]
        users = {}
        for record in self.litellm_client.list_users():
            email = self._email_of(record)
            if email:
                users[email] = record
        self._prune_expired_users(users)
        self._users_cache = (now, users)
        return users

    def _prune_expired_users(self, users: dict) -> None:
        """Delete released records that can no longer bill any reportable month.

        Runs on a fresh listing, so at most once per membership pass. Without it every
        member ever removed would keep a record on the proxy forever. A record is only
        deleted once it is released and its last period ended before the oldest month
        in the retention window; a hand-made user, which carries no periods, never is.

        Each candidate is read again just before the delete. The listing can be most of
        a pass old by now, and an address re-claimed in the meantime -- the person
        re-added to some resource -- has an open period again; deleting it on the
        strength of the stale listing would take that resource's chat billing with it.

        Pruned records are dropped from ``users`` in place. A failed delete is logged and
        retried on the next pass: the record is released, so leaving it costs nothing but
        the space.
        """
        cutoff = ownership.retention_cutoff(ownership.today(), self.ownership_retention_months)

        def is_expired(record: Optional[dict]) -> bool:
            if record is None:
                return False
            return ownership.expired(ownership.periods_of(self._user_metadata(record)), cutoff)

        candidates = sorted(email for email, record in users.items() if is_expired(record))
        if not candidates:
            return
        try:
            expired = [
                email for email in candidates if is_expired(self.litellm_client.get_user(email))
            ]
        except BackendError:
            logger.exception("Unable to re-read expired LiteLLM users; pruning skipped")
            return
        if not expired:
            return
        try:
            self.litellm_client.delete_users(expired)
        except BackendError:
            logger.exception("Unable to delete %d expired LiteLLM user(s)", len(expired))
            return
        logger.info(
            "Deleted %d LiteLLM user(s) released before %s: %s",
            len(expired),
            cutoff,
            ", ".join(expired),
        )
        for email in expired:
            users.pop(email, None)

    def _invalidate_users_cache(self) -> None:
        """Drop the cached listing after a write that changes it.

        Without this an add followed by a pull inside the same pass would read the
        pre-add listing and report the new member as still missing, which the processor
        would answer by adding them again.
        """
        self._users_cache = None

    def _resource_user_map(self, resource_backend_id: str) -> dict:
        """Return ``{offering_username: email}`` for the users this resource owns.

        Keyed by offering username because that is the currency the membership
        processor diffs in: it compares what this returns against the project team's
        offering usernames. The email is what the proxy is addressed by, so both halves
        of the mapping are needed and both are read back out of the user's metadata
        rather than recomputed -- a person can change their address in Waldur, and the
        record on the proxy is the one that says which address is actually billing.

        Only the open ownership period counts. A removed member's record stays on the
        proxy with their period closed, so it still bills what they used, but they are
        no longer provisioned: listing them would have the membership diff remove them
        again on every pass, and pause and restore act on them.
        """
        found = {}
        for email, record in self._resource_user_records(resource_backend_id).items():
            username = self._user_metadata(record).get(META_USERNAME)
            if username:
                found[str(username)] = email
        return found

    def _resource_user_records(self, resource_backend_id: str) -> dict:
        """Return ``{email: record}`` for the users this resource holds open."""
        return {
            email: record
            for email, record in self._managed_users().items()
            if ownership.current_owner(ownership.periods_of(self._user_metadata(record)))
            == resource_backend_id
        }

    def list_resource_users(self, resource_backend_id: str) -> list:
        """Offering usernames of the people currently provisioned for the resource."""
        return sorted(self._resource_user_map(resource_backend_id))

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set, **kwargs: object
    ) -> set:
        """Provision each new member on the proxy and, if configured, in Open WebUI.

        ``user_ids`` are offering usernames; the addresses come alongside them in
        ``user_emails``, which the membership processor builds from the offering users.
        A member with no address cannot be provisioned at all -- the email *is* the
        identifier both systems agree on -- so they are skipped loudly rather than
        provisioned under something that would never receive their usage.
        """
        backend_id = waldur_resource.backend_id
        if not backend_id:
            logger.warning("No backend_id for resource %s; cannot add users", waldur_resource.uuid)
            return set()
        if not user_ids:
            logger.info("No new users to add")
            return set()

        # The processor threads these through as plain dicts; ``**kwargs: object`` is
        # the base signature, so they are narrowed here rather than trusted.
        user_emails = kwargs.get("user_emails")
        user_emails = user_emails if isinstance(user_emails, dict) else {}
        user_attributes = kwargs.get("user_attributes")
        user_attributes = user_attributes if isinstance(user_attributes, dict) else {}
        # Joining the project opens chat only where chat is granted by membership. Where
        # it follows the key, the person gets no chat until a key is assigned to them
        # (``sync_resource_key_assignees``), and their record says so from the start.
        follows_key = self._chat_follows_keys(waldur_resource)
        added = set()
        for username in sorted(user_ids):
            # ``user_emails`` is only built by the membership processor; the order
            # processor and the service/course-account syncs pass the addresses inside
            # ``user_attributes`` instead. Reading only the former skipped every member
            # on those paths, so a CREATE order provisioned nobody until the next
            # membership pass -- and never, in order-only mode.
            attributes = user_attributes.get(username)
            attributes = attributes if isinstance(attributes, dict) else {}
            raw_email = user_emails.get(username) or attributes.get("email") or ""
            email = str(raw_email).strip().lower()
            if not email:
                logger.error(
                    "Offering user %s has no email address; LiteLLM and Open WebUI are "
                    "married by email, so they cannot be added to resource %s",
                    username,
                    backend_id,
                )
                continue
            attributes = user_attributes.get(username)
            attributes = attributes if isinstance(attributes, dict) else {}
            full_name = str(attributes.get("full_name") or username)
            try:
                if not self._ensure_litellm_user(
                    email, username, full_name, backend_id, chat_follows_key=follows_key
                ):
                    continue
                if not follows_key:
                    self._ensure_chat_account(email, full_name)
            except BackendError:
                logger.exception(
                    "Unable to add user %s (%s) to resource %s", username, email, backend_id
                )
                continue
            added.add(username)
        if added:
            self._invalidate_users_cache()
        return added

    def _ensure_litellm_user(
        self,
        email: str,
        username: str,
        full_name: str,
        backend_id: str,
        *,
        chat_follows_key: bool = False,
    ) -> bool:
        """Create or adopt the proxy-side user record. False means "not ours to touch".

        A LiteLLM user id is global, and here it is an email address, so one person has
        exactly one record on the proxy no matter how many resources they hold. That is
        the per-person model's load-bearing constraint: usage arriving under an address
        can only be billed to one resource, so a second resource claiming an address
        another one already owns is **refused**, not taken over. Stealing it would move
        the first resource's chat billing onto the second one silently, and the person
        would keep using the chat throughout.

        A record with no owner stamped on it is adopted rather than refused: it is
        either a user an admin added by hand before Waldur managed this offering, or one
        left behind by an interrupted add, and in both cases the alternative is a
        resource that can never provision its own member.

        "Owned" means an open ownership period. A record whose period on another
        resource has closed -- the person was removed there -- is free to claim: the
        claim opens a new period from today, and the closed one keeps billing the days
        before it to the resource the person left.

        ``chat_follows_key`` stamps an empty ``META_KEY``: chat follows a key here and
        none is assigned yet. An existing stamp is kept -- the metadata is carried over,
        not replaced, so a re-add never loses the key the person holds.
        """
        day = ownership.today()
        existing = self.litellm_client.get_user(email)
        if existing is None:
            initial = {META_KEY: ""} if chat_follows_key else {}
            self.litellm_client.create_user(
                email,
                user_email=email,
                user_alias=full_name,
                metadata=ownership.stamp(initial, ownership.claim([], backend_id, day), username),
            )
            logger.info("Created LiteLLM user %s for resource %s", email, backend_id)
            return True

        metadata = self._user_metadata(existing)
        periods = ownership.periods_of(metadata)
        owner = ownership.current_owner(periods)
        if owner and owner != backend_id:
            logger.error(
                "LiteLLM user %s is already owned by resource %s; refusing to move it "
                "to %s. A person's usage can only be billed to one resource, so the "
                "chat surface supports one entitlement per person -- terminate the "
                "other resource first.",
                email,
                owner,
                backend_id,
            )
            return False

        # Re-stamped on every add, not only on adoption: the offering username can
        # change (Waldur regenerates it under some policies), and the membership diff
        # is keyed on it, so a stale one would report the member as absent forever.
        # The same write trims periods too old to bill anything, so a person who comes
        # and goes does not grow their record without bound.
        periods = ownership.claim(self._trim_ownership(periods, day), backend_id, day)
        if chat_follows_key and META_KEY not in metadata:
            metadata = {**metadata, META_KEY: ""}
        self.litellm_client.update_user(
            email,
            {
                "user_email": email,
                "user_alias": full_name,
                "metadata": ownership.stamp(metadata, periods, username),
            },
        )
        logger.info("Adopted existing LiteLLM user %s for resource %s", email, backend_id)
        return True

    def _ensure_chat_account(
        self,
        email: str,
        full_name: Optional[str] = None,
        *,
        create_missing: bool = True,
        new_role: str = ROLE_ACTIVE,
    ) -> bool:
        """Make sure the person can sign in to the chat surface, if there is one.

        Under ``sso`` no account is created: the identity provider does that on first
        login. What still runs is the *re-enable*, because a previous removal demoted
        the account and an SSO login into a demoted account does not restore it.

        ``create_missing=False`` re-enables only. Restore uses it: an account that is
        not there was not paused by this plugin, and restore has no display name to
        create one under -- it knows the offering username, not the person's name.

        A missing account is created with ``new_role``; returns whether one was.
        """
        if self.openwebui_client is None:
            return False
        account = self.openwebui_client.find_user(email)
        if account is None:
            if not create_missing:
                logger.info("No Open WebUI account for %s; nothing to re-enable", email)
                return False
            if self.account_provisioning != PROVISIONING_MANAGED_PASSWORD:
                logger.info(
                    "No Open WebUI account for %s; it will be created on first sign-in "
                    "through the identity provider",
                    email,
                )
                return False
            self.openwebui_client.create_user(
                email, str(full_name or email), str(self.initial_password), role=new_role
            )
            logger.info("Created Open WebUI account for %s", email)
            return True

        if str(account.get("role")) == ROLE_DISABLED:
            self.openwebui_client.set_role(account, ROLE_ACTIVE)
            logger.info("Re-enabled the Open WebUI account of %s", email)
        return False

    def remove_users_from_resource(
        self, waldur_resource: WaldurResource, usernames: set, **kwargs: object
    ) -> list:
        """Revoke a member's chat access and release their LiteLLM record.

        Both halves matter and in this order they are both required: releasing only the
        LiteLLM user leaves the person chatting through the shared key with their usage
        landing on rows nothing reconciles, and demoting only the chat account leaves the
        address owned by a resource the person is no longer entitled to.

        The record is released, not deleted: its closed period is what keeps billing the
        chat usage the person had on this resource before they left.
        """
        del kwargs
        backend_id = waldur_resource.backend_id
        if not usernames:
            logger.info("No users to remove")
            return []

        owned = self._resource_user_map(backend_id) if backend_id else {}
        removed = []
        for username in sorted(usernames):
            email = owned.get(username)
            if email is None:
                # Already absent from the proxy. Reporting it as removed is correct --
                # the desired end state holds -- and reporting a failure instead would
                # have the processor retry it on every pass forever.
                logger.info(
                    "No LiteLLM user for %s on resource %s; nothing to remove",
                    username,
                    backend_id,
                )
                removed.append(username)
                continue
            try:
                self._revoke_chat_access(email, permanent=True)
                self._release_litellm_user(email, backend_id)
            except BackendError:
                logger.exception(
                    "Unable to remove user %s (%s) from resource %s", username, email, backend_id
                )
                continue
            logger.info("Removed user %s (%s) from resource %s", username, email, backend_id)
            removed.append(username)
        if removed:
            self._invalidate_users_cache()
        return removed

    def _trim_ownership(self, periods: list, day: str) -> list:
        """Drop the periods that ended before the retention window."""
        return ownership.trim(
            periods, ownership.retention_cutoff(day, self.ownership_retention_months)
        )

    def _release_litellm_user(self, email: str, backend_id: str) -> None:
        """Close the resource's ownership period on the address's record, as of today.

        Read fresh rather than from the cached listing: the write replaces the whole
        metadata, so writing back a stale copy would drop a period opened since.

        A record that is gone, or that this resource no longer holds open, needs nothing:
        the end state -- not owned by this resource -- already holds.
        """
        existing = self.litellm_client.get_user(email)
        if existing is None:
            return
        metadata = self._user_metadata(existing)
        periods = ownership.periods_of(metadata)
        if ownership.current_owner(periods) != backend_id:
            logger.info(
                "LiteLLM user %s is not held open by resource %s; nothing to release",
                email,
                backend_id,
            )
            return
        day = ownership.today()
        periods = ownership.release(self._trim_ownership(periods, day), backend_id, day)
        self.litellm_client.update_user(
            email, {"metadata": ownership.stamp(metadata, periods)}
        )
        logger.info("Released LiteLLM user %s from resource %s on %s", email, backend_id, day)

    def _revoke_chat_access(self, email: str, *, permanent: bool = False) -> None:
        """End one person's access to the chat surface.

        Demotion by default, deletion only when the offering asks for it *and* the
        entitlement is actually gone -- ``permanent`` is what says so, and only the
        member-removal and terminate paths pass it. A pause is a temporary state, so
        it always demotes however ``delete_accounts_on_removal`` is set: deleting there
        would destroy the person's conversations over an unpaid invoice, and under
        ``sso`` restore creates nothing back, so the loss would be permanent.

        Both forms end access equally; demotion additionally keeps the person's
        conversations so a re-add gives back what they had.

        A missing account is not an error. Under ``sso`` a person may never have signed
        in, and there is nothing to revoke for someone who never had access.
        """
        if self.openwebui_client is None:
            return
        account = self.openwebui_client.find_user(email)
        if account is None:
            return
        role = str(account.get("role") or "")
        if role not in (ROLE_ACTIVE, ROLE_DISABLED):
            # An admin (or any other elevated role) can also be a project member. This
            # plugin only ever grants ``user``, so anything else was set outside it and
            # is not ours to take away: demoting it would lock the operator out of the
            # admin API -- possibly with the very token this agent authenticates with --
            # and restore only ever promotes back to ``user``, so the loss is permanent.
            logger.info(
                "Open WebUI account of %s has role %s; leaving it untouched", email, role
            )
            return
        if permanent and self.remove_chat_accounts:
            self.openwebui_client.delete_user(str(account.get("id")))
            logger.info("Deleted the Open WebUI account of %s", email)
            return
        if role == ROLE_DISABLED:
            # Chat that follows the key closes it for every member without one on
            # every pass; rewriting the role each time would be a write per person.
            return
        self.openwebui_client.set_role(account, ROLE_DISABLED)
        logger.info("Disabled the Open WebUI account of %s", email)

    def _set_chat_access_for_resource(
        self, resource_backend_id: str, *, enabled: bool, key_records: Optional[dict] = None
    ) -> bool:
        """Enable or disable the chat surface for every member of one resource.

        Called from pause and restore. Blocking the resource's keys does **not** stop
        its members chatting: that traffic authenticates as Open WebUI's own shared key,
        which this plugin neither owns nor may block -- blocking it would cut off every
        other tenant on the proxy. So a paused resource has to be paused person by
        person, on the chat side.

        Where chat follows the key, a restore re-enables only the people whose key
        comes back with the resource: one paused on its own keeps their chat closed,
        and someone with no key never had it open. ``key_records`` are the resource's
        keys, read by the caller before it changes them.
        """
        if self.openwebui_client is None:
            return True
        succeeded = True
        try:
            members = self._resource_user_records(resource_backend_id)
        except BackendError:
            # Raising would abort pause/restore before a single key is blocked, and in
            # the membership pass it would also skip the rest of the per-resource work.
            # Report the failure instead and let the caller block the keys anyway.
            logger.exception(
                "Unable to list the users of resource %s; chat access is unchanged",
                resource_backend_id,
            )
            return False
        keys = key_records or {}
        for email in sorted(members):
            metadata = self._user_metadata(members[email])
            if enabled and META_KEY in metadata:
                key = keys.get(str(metadata.get(META_KEY) or ""))
                if key is None or self._is_self_paused(key):
                    continue
            try:
                if enabled:
                    self._ensure_chat_account(email, create_missing=False)
                else:
                    self._revoke_chat_access(email)
            except (BackendError, OpenWebUIError):
                # A pause that half-worked still leaves someone spending past their
                # quota, so this is an error and the caller reports the pause as failed.
                logger.exception(
                    "Unable to %s chat access for %s",
                    "restore" if enabled else "revoke",
                    email,
                )
                succeeded = False
        return succeeded

    # --- chat that follows the key ----------------------------------------------
    #
    # On an offering with a chat surface and per-key management, a person's chat is
    # governed by the key assigned to them rather than by project membership: open
    # while that key serves, closed while it is paused, deleted or assigned to someone
    # else, and limited to the key's models. The key's alias is stamped on the person's
    # LiteLLM user record (``META_KEY``) so the per-key verbs, a resource restore and
    # the usage report can all find it without asking Waldur.
    #
    # Chat traffic still authenticates as Open WebUI's shared key, so nothing on the
    # person's key applies to it at the proxy. Their chat is held to the key's models
    # by Open WebUI group membership, and to the key's limit by Waldur, which meters
    # the person's chat onto the key and pauses it.

    def _chat_follows_keys(self, waldur_resource: WaldurResource) -> bool:
        """Whether this resource's members chat on their key rather than by membership."""
        # Imported here: the core module imports the backend package at load time.
        from waldur_site_agent.common.resource_api_keys import (  # noqa: PLC0415
            manages_api_keys,
        )

        return self.openwebui_client is not None and manages_api_keys(waldur_resource)

    @staticmethod
    def _slot(alias: str) -> int:
        tail = alias.rsplit("-", 1)[-1]
        return int(tail) if tail.isdigit() else 0

    def _chat_key_choice(self, assignments: dict, records: dict) -> dict:
        """Return ``{username: alias}``: the one key that carries each person's chat.

        One per person, because chat arrives under an address, not a key, so it can be
        metered onto one key only. Waldur allows a person several keys on a resource;
        their chat follows the lowest-numbered -- the oldest -- of those the proxy
        holds, **whatever its state**. Moving chat to another key while that one is
        paused would let it dodge the key's limit: the paused key's usage drops back to
        its API calls, Waldur resumes it under the limit, the other key is paused in
        turn, and the person's chat never stops.
        """
        candidates: dict = {}
        for alias, username in assignments.items():
            if username and alias in records:
                candidates.setdefault(username, []).append((self._slot(alias), alias))
        return {username: min(options)[1] for username, options in candidates.items()}

    def sync_resource_key_assignees(
        self, waldur_resource: WaldurResource, assignments: dict
    ) -> None:
        """Point each member's chat at the key assigned to them, and open or close it.

        Runs on every membership sync, which is what carries a reassignment (Waldur
        sends no command for one), reconciles a missed command, and catches the
        account that only appeared at the person's first sign-in. A member with no
        key assigned has their chat closed: that is the rule this offering runs under,
        including for people who chatted by membership before key management was on.

        Assignees are matched by offering username, which is stamped on each member's
        record, not by address: the membership diff keys on it too, and an address the
        person changed in Waldur is not the one their record and chat account carry.
        """
        if not self._chat_follows_keys(waldur_resource):
            return
        backend_id = waldur_resource.backend_id
        if not backend_id:
            return
        records = self._resource_keys(backend_id)
        chosen = self._chat_key_choice(assignments, records)
        # Read from Waldur rather than off the keys: the status sync that blocks the
        # keys of a newly paused resource runs after this in the same pass.
        held = getattr(waldur_resource, "paused", None) is True or (
            getattr(waldur_resource, "downscaled", None) is True
        )
        members = self._resource_user_records(backend_id)
        stamped = False
        for email in sorted(members):
            record = members[email]
            username = str(self._user_metadata(record).get(META_USERNAME) or "")
            alias = chosen.get(username, "") if username else ""
            metadata = self._user_metadata(record)
            try:
                if metadata.get(META_KEY) != alias:
                    self.litellm_client.update_user(
                        email, {"metadata": {**metadata, META_KEY: alias}}
                    )
                    stamped = True
                key = records.get(alias) if alias else None
                # Judged by the key's own pause and Waldur's flag, not by ``blocked``:
                # the keys of a resource restored this pass are still blocked until the
                # status sync, which runs after this one, and reading them as paused
                # would strip the person's model groups that restore does not give back.
                serves = key is not None and not self._is_self_paused(key) and not held
                if serves and key is not None:
                    # Read again just before opening: a pause at the key's limit that
                    # landed since the listing closed this chat, and must stay closed.
                    current = self.litellm_client.get_key(self._require_token(alias, key))
                    if current is None or self._is_self_paused(current):
                        serves = False
                    else:
                        # Its model list as it is now, too: an update may have narrowed it.
                        key = current
                self._apply_key_chat(
                    email, key if serves else None, full_name=record.get("user_alias")
                )
            except BackendError:
                # One person's failure must not leave everyone after them unreconciled;
                # the next pass retries.
                logger.exception("Unable to apply the API key of %s to their chat", email)
        if stamped:
            self._invalidate_users_cache()
            if self.usage_client is not None:
                # A new stamp changes whose chat a key's budget is net of, and a key
                # minted for someone is stamped here, right after the mint: nothing
                # else recomputes it before the next status or limits pass.
                self._write_key_budgets(backend_id, records, None)
        provisioned = {
            str(self._user_metadata(record).get(META_USERNAME) or "") for record in members.values()
        }
        waiting = sorted(set(chosen) - provisioned)
        if waiting:
            logger.info(
                "Key assignee(s) of resource %s have no LiteLLM user on it, so no chat: %s",
                backend_id,
                ", ".join(waiting),
            )

    def _key_holders(self, resource_backend_id: str, alias: str) -> dict:
        """Return ``{email: record}`` of the members whose chat runs on this key."""
        if self.openwebui_client is None or not alias:
            return {}
        return {
            email: record
            for email, record in self._resource_user_records(resource_backend_id).items()
            if self._user_metadata(record).get(META_KEY) == alias
        }

    def _apply_key_chat(
        self, email: str, key: Optional[dict], *, full_name: Optional[str] = None
    ) -> None:
        """Open the person's chat with the key's models, or close it when ``key`` is None.

        Closed first and groups after; groups first and opened after. Either way a
        failed Open WebUI call leaves the account closed, never serving with the groups
        an earlier, wider key left it in; the error propagates and the next pass retries.
        """
        if key is None:
            self._revoke_chat_access(email)
            self._set_model_groups(email, [])
            return
        models = self._chat_models(key)
        self._set_model_groups(email, models)
        if self._ensure_chat_account(email, full_name, new_role=ROLE_DISABLED):
            # Made closed just now, so the groups above had no account to go on.
            self._set_model_groups(email, models)
            self._ensure_chat_account(email, full_name)

    def _chat_models(self, key: dict) -> list:
        """The models a key allows, as chat sees them.

        An empty ``models`` on a key is every model the proxy serves, which in chat is
        every model of the offering: chat only offers those.
        """
        models = key.get("models")
        names = [str(model) for model in models] if isinstance(models, list) else []
        return names or list(self.models)

    def _set_model_groups(self, email: str, models: list) -> None:
        """Make the person a member of exactly the model groups of ``models``.

        Only the plugin's own groups are touched: those named with its prefix. That
        includes the groups of models since dropped from the offering's ``models``,
        which the person leaves, or the model would stay open to them in chat. Needs the
        offering's ``models`` setting: without it the plugin has no list of models to
        make groups for, and chat model access is left to the operator.
        """
        if self.openwebui_client is None or not self.models:
            return
        account = self.openwebui_client.find_user(email)
        if account is None or not account.get("id"):
            # Under SSO the account appears at first sign-in; the next pass gets it.
            return
        user_id = str(account["id"])
        groups = self._chat_model_groups()
        wanted = {groups[model] for model in models if model in groups}
        current = self.openwebui_client.user_group_ids(user_id) & self._own_model_group_ids()
        # Removals first: a failure part way leaves the person with fewer models, not more.
        for group_id in sorted(current - wanted):
            self.openwebui_client.remove_user_from_group(group_id, user_id)
        for group_id in sorted(wanted - current):
            self.openwebui_client.add_user_to_group(group_id, user_id)

    def _chat_model_groups(self) -> dict:
        """Return ``{model: group id}``, creating the groups and granting their models.

        The grants are written again on every refresh, not only when a group is made:
        setting a model to Public in Open WebUI's admin UI, or editing its access,
        replaces them, and the model would then be open to everyone or to no one.
        Cached like the user listing, so a pass writes them once.
        """
        if self.openwebui_client is None:
            return {}
        now = time.monotonic()
        cached = self._model_groups_cache
        if cached is not None and now - cached[0] < _USER_CACHE_TTL:
            return cached[1]
        existing = {
            str(group.get("name")): str(group["id"])
            for group in self.openwebui_client.list_groups()
            if group.get("id")
        }
        own = {
            group_id
            for name, group_id in existing.items()
            if name.startswith(CHAT_MODEL_GROUP_PREFIX)
        }
        groups = {}
        for model in self.models:
            name = f"{CHAT_MODEL_GROUP_PREFIX}{model}"
            group_id = existing.get(name)
            if group_id is None:
                created = self.openwebui_client.create_group(
                    name, f"Chat access to {model}; membership is managed by Waldur"
                )
                group_id = str(created["id"])
                logger.info("Created Open WebUI group %s", name)
            self.openwebui_client.grant_model_to_group(model, group_id)
            groups[model] = group_id
        own.update(groups.values())
        self._model_groups_cache = (now, groups, frozenset(own))
        return groups

    def _own_model_group_ids(self) -> frozenset:
        """Ids of every group under the plugin's prefix, retired models' included."""
        self._chat_model_groups()
        cached = self._model_groups_cache
        return cached[2] if cached is not None else frozenset()

    # --- usage reporting --------------------------------------------------------

    def _get_usage_report(self, resource_backend_ids: list) -> dict:
        """Usage is reported by the separate ``litellm-usage`` reporting backend."""
        del resource_backend_ids
        return {}
