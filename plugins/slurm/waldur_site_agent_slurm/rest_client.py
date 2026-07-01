"""REST API client for SLURM cluster (slurmrestd).

Implements ``SlurmClientInterface`` against the slurmrestd
``/slurmdb/<version>`` and ``/slurm/<version>`` endpoints instead of
shelling out to sacctmgr/sacct/scancel. Operations without a direct
REST equivalent (sacct usage reports, RawUsage reset, local ``id``
lookups) are delegated to an internal CLI ``SlurmClient`` — see
docs/slurm-rest-api-design.md.

JSON field paths follow the data_parser plugin conventions: a path like
``max/tres/group/minutes`` denotes nested objects
``{"max": {"tres": {"group": {"minutes": ...}}}}``. TRES values are
encoded as lists of ``{"type": ..., "name": ..., "count": ...}``
objects. Numeric "maybe-unset" fields may arrive as plain numbers or as
tri-state ``{"set": bool, "infinite": bool, "number": N}`` structs;
plain numbers are accepted by the server on input.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from shutil import which
from typing import Any, Optional
from urllib.parse import quote, urlencode

import httpx

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import Association, ClientResource
from waldur_site_agent_slurm.client import _PARTITION_NAME_RE, SlurmClient
from waldur_site_agent_slurm.interface import SlurmClientInterface

# SLURMDB_FS_USE_PARENT in slurm/slurmdb.h — the shares_raw sentinel
# equivalent to ``sacctmgr ... Share=parent``.
FAIRSHARE_USE_PARENT = 0x7FFFFFFF

DEFAULT_API_VERSION = "v0.0.43"

# sacctmgr QoS flag spellings -> data_parser enum spellings.
_QOS_FLAG_MAP = {
    "denyonlimit": "DENY_LIMIT",
    "nodecay": "NO_DECAY",
    "noreserve": "NO_RESERVE",
    "enforceusagethreshold": "ENFORCE_USAGE_THRESHOLD",
    "requiresreservation": "REQUIRED_RESERVATION",
    "overpartqos": "OVERRIDE_PARTITION_QOS",
    "partitionminnodes": "PARTITION_MINIMUM_NODE",
    "partitionmaxnodes": "PARTITION_MAXIMUM_NODE",
    "partitiontimelimit": "PARTITION_TIME_LIMIT",
    "usagefactorsafe": "USAGE_FACTOR_SAFE",
    "relative": "RELATIVE",
}

# Job states that count as "active" for job listing/cancellation.
_ACTIVE_JOB_STATES = frozenset(
    {"PENDING", "RUNNING", "SUSPENDED", "COMPLETING", "CONFIGURING", "REQUEUED", "RESIZING"}
)

_HTTP_UNAUTHORIZED = 401


def _parse_walltime_minutes(value: str) -> int:
    """Convert a sacctmgr walltime string to minutes.

    Mirrors the SLURM time formats accepted by sacctmgr:
    ``minutes``, ``MM:SS``, ``HH:MM:SS``, ``D-HH``, ``D-HH:MM`` and
    ``D-HH:MM:SS``. Colon-separated fields are *right-aligned* like
    SLURM: without a ``D-`` day prefix the rightmost field is seconds,
    so ``"30:00"`` is 30 minutes (MM:SS), not 30 hours. With a day
    prefix the leftmost colon field is hours (``D-HH:MM:SS``).
    """
    value = value.strip()
    if value.isdigit():
        return int(value)
    max_time_parts = 3
    has_days = "-" in value
    days = 0
    if has_days:
        days_str, value = value.split("-", 1)
        days = int(days_str)
    parts = value.split(":")
    if len(parts) > max_time_parts or not all(part.isdigit() for part in parts):
        msg = f"Unsupported walltime format: {value!r}"
        raise BackendError(msg)
    nums = [int(part) for part in parts]
    if has_days:
        # D-HH[:MM[:SS]] — leftmost colon field is hours.
        hours = nums[0] if len(nums) >= 1 else 0
        minutes = nums[1] if len(nums) >= 2 else 0  # noqa: PLR2004
        seconds = nums[2] if len(nums) >= 3 else 0  # noqa: PLR2004
    elif len(nums) == max_time_parts:
        hours, minutes, seconds = nums  # HH:MM:SS
    elif len(nums) == 2:  # noqa: PLR2004
        hours, minutes, seconds = 0, nums[0], nums[1]  # MM:SS
    else:
        hours, minutes, seconds = 0, nums[0], 0  # plain minutes
    total_seconds = ((days * 24 + hours) * 60 + minutes) * 60 + seconds
    return total_seconds // 60


class SlurmRestClient(SlurmClientInterface):
    """SLURM client backed by the slurmrestd REST API."""

    def __init__(
        self,
        slurm_tres: dict,
        rest_settings: dict,
        cluster_name: str,
        slurm_bin_path: str = "/usr/bin",
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        """Init REST transport, auth settings and the internal CLI client.

        Args:
            slurm_tres: TRES configuration (same as for SlurmClient).
            rest_settings: the ``backend_settings.rest_api`` mapping.
            cluster_name: cluster the agent manages; REST association
                payloads always carry an explicit cluster, so this is required.
            slurm_bin_path: path to SLURM binaries for delegated CLI calls.
            transport: optional httpx transport override (used by tests).
        """
        if not cluster_name:
            msg = "cluster_name is required when SLURM execution_mode is 'rest'"
            raise BackendError(msg)
        self.slurm_tres = slurm_tres
        self.cluster_name: Optional[str] = cluster_name
        self._executed_commands: list[str] = []
        # TRES is static for the cluster lifetime; cache it to avoid a
        # GET /tres round trip on every per-user limit write.
        self._tres_keys_cache: Optional[list[str]] = None
        self.api_version: str = rest_settings.get("api_version", DEFAULT_API_VERSION)
        self.username: str = rest_settings.get("username", "")
        self._token_file: Optional[str] = rest_settings.get("token_file")
        self._token_env: Optional[str] = rest_settings.get("token_env")
        self._token: Optional[str] = None
        url: str = rest_settings["url"]
        timeout = rest_settings.get("timeout", 30)
        if transport is None and url.startswith("unix://"):
            transport = httpx.HTTPTransport(uds=url[len("unix://") :])
            # The host part is a placeholder — requests go over the socket.
            url = "http://slurmrestd"
        self._http = httpx.Client(
            base_url=url,
            timeout=timeout,
            verify=rest_settings.get("verify_ssl", True),
            transport=transport,
            # Tolerate trailing-slash redirects (307 preserves method and body).
            follow_redirects=True,
        )
        # CLI client for operations without a REST equivalent (sacct usage
        # reports, RawUsage reset) and for local OS commands (id, homedirs).
        self._cli = SlurmClient(
            slurm_tres, slurm_bin_path=slurm_bin_path, cluster_name=cluster_name
        )

    @property
    def slurm_bin_path(self) -> str:
        """Path to SLURM binaries used by the delegated CLI client."""
        return self._cli.slurm_bin_path

    @property
    def executed_commands(self) -> list[str]:
        """Diagnostics log of REST requests plus any delegated CLI commands.

        Usage reporting and reset_raw_usage run on the internal CLI client,
        which records onto its own list; merge it so callers auditing applied
        actions (e.g. periodic-settings ``commands_executed``) see the full
        picture rather than just the REST traffic.
        """
        return [*self._executed_commands, *self._cli.executed_commands]

    def clear_executed_commands(self) -> None:
        """Clear both the REST request log and the delegated CLI client's log."""
        self._executed_commands = []
        self._cli.clear_executed_commands()

    # ===== TRANSPORT =====

    def _get_token(self, force_reload: bool = False) -> str:
        if self._token and not force_reload:
            return self._token
        if self._token_file:
            try:
                self._token = Path(self._token_file).read_text(encoding="utf-8").strip()
            except OSError as e:
                msg = f"Cannot read slurmrestd token file {self._token_file}: {e}"
                raise BackendError(msg) from e
        elif self._token_env:
            self._token = os.environ.get(self._token_env, "").strip()
        if not self._token:
            msg = (
                "No slurmrestd token available: set rest_api.token_file or "
                "rest_api.token_env in backend_settings"
            )
            raise BackendError(msg)
        return self._token

    def _headers(self, force_token_reload: bool = False) -> dict[str, str]:
        headers = {"X-SLURM-USER-TOKEN": self._get_token(force_token_reload)}
        if self.username:
            headers["X-SLURM-USER-NAME"] = self.username
        return headers

    def _request(
        self,
        method: str,
        path: str,
        query: Optional[dict[str, str]] = None,
        body: Optional[dict] = None,
        allow_errors: bool = False,
    ) -> dict:
        """Send a request to slurmrestd and return the parsed JSON body.

        Raises BackendError on transport failures, non-2xx statuses and —
        importantly — on a non-empty ``errors`` array in an HTTP 200 body,
        which is how slurmrestd commonly reports failures.

        ``allow_errors=True`` downgrades the errors-array / non-2xx checks
        to a debug log and returns the (possibly empty) payload instead of
        raising. Use it for single-entity existence lookups
        (``GET .../qos/<name>``, ``GET .../account/<name>``): real
        slurmrestd answers a missing entity with an 'Unable to find …'
        error rather than an empty list, which would otherwise turn a
        "does it exist?" check into a crash.
        """
        request_repr = f"{method} {path}" + (f"?{urlencode(query)}" if query else "")
        self._executed_commands.append(request_repr)
        logger.debug("slurmrestd request: %s", request_repr)
        try:
            response = self._http.request(
                method, path, params=query, json=body, headers=self._headers()
            )
            if response.status_code == _HTTP_UNAUTHORIZED:
                # Token may have rotated on disk — re-read it once and retry.
                response = self._http.request(
                    method,
                    path,
                    params=query,
                    json=body,
                    headers=self._headers(force_token_reload=True),
                )
        except httpx.HTTPError as e:
            msg = f"slurmrestd request failed ({request_repr}): {e}"
            raise BackendError(msg) from e

        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        for warning in payload.get("warnings") or []:
            description = (
                warning.get("description", warning) if isinstance(warning, dict) else warning
            )
            logger.warning("slurmrestd warning (%s): %s", request_repr, description)
        errors = payload.get("errors") or []
        if errors:
            details = "; ".join(
                str(err.get("description") or err.get("error") or err)
                if isinstance(err, dict)
                else str(err)
                for err in errors
            )
            if allow_errors:
                logger.debug("slurmrestd reported errors (%s): %s", request_repr, details)
                return payload
            msg = f"slurmrestd error ({request_repr}): {details}"
            raise BackendError(msg)
        if response.status_code >= httpx.codes.BAD_REQUEST:
            if allow_errors:
                logger.debug(
                    "slurmrestd non-2xx (%s): HTTP %s", request_repr, response.status_code
                )
                return payload
            msg = f"slurmrestd error ({request_repr}): HTTP {response.status_code}"
            raise BackendError(msg)
        return payload

    def _db(self, path: str) -> str:
        return f"/slurmdb/{self.api_version}/{path}"

    def _ctld(self, path: str) -> str:
        return f"/slurm/{self.api_version}/{path}"

    # ===== TRES ENCODING HELPERS =====

    @staticmethod
    def _tres_key(tres: dict) -> str:
        """Return the CLI-style TRES key ('cpu', 'mem', 'gres/gpu') for a TRES object."""
        name = tres.get("name") or ""
        tres_type = tres.get("type", "")
        return f"{tres_type}/{name}" if name else tres_type

    @staticmethod
    def _tres_count(tres: dict) -> Optional[int]:
        """Extract a count from a TRES object, tolerating tri-state structs."""
        count = tres.get("count")
        if isinstance(count, dict):
            if not count.get("set") or count.get("infinite"):
                return None
            count = count.get("number")
        if count is None:
            return None
        return int(count)

    @classmethod
    def _tres_list_to_dict(cls, tres_list: Optional[list[dict]]) -> dict[str, int]:
        result = {}
        for tres in tres_list or []:
            count = cls._tres_count(tres)
            if count is not None:
                result[cls._tres_key(tres)] = count
        return result

    @staticmethod
    def _tres_dict_to_list(limits: dict) -> list[dict]:
        """Convert {'cpu': 100, 'gres/gpu': 2} to a TRES object list."""
        tres_list = []
        for key in sorted(limits):
            entry: dict[str, Any] = {"count": int(limits[key])}
            if "/" in key:
                entry["type"], entry["name"] = key.split("/", 1)
            else:
                entry["type"] = key
            tres_list.append(entry)
        return tres_list

    @classmethod
    def _tres_str_to_list(cls, tres_str: str) -> list[dict]:
        """Convert 'cpu=100,gres/gpu=2' to a TRES object list."""
        limits = {}
        for item in tres_str.split(","):
            if "=" in item:
                key, value = item.split("=", 1)
                try:
                    limits[key.strip()] = int(value)
                except ValueError as e:
                    msg = f"Invalid TRES value in {item!r}: {value.strip()!r} is not an integer"
                    raise BackendError(msg) from e
        return cls._tres_dict_to_list(limits)

    # ===== ASSOCIATION HELPERS =====

    def _list_associations(
        self, account: Optional[str] = None, user: Optional[str] = None
    ) -> list[dict]:
        query = {}
        if account is not None:
            query["account"] = account
        if user is not None:
            query["user"] = user
        if self.cluster_name:
            query["cluster"] = self.cluster_name
        payload = self._request("GET", self._db("associations/"), query=query)
        return payload.get("associations") or []

    def _get_account_association(self, account: str) -> Optional[dict]:
        """Return the account-level association (the one with no user)."""
        for assoc in self._list_associations(account=account):
            if not assoc.get("user"):
                return assoc
        return None

    def _post_association(self, account: str, fields: dict, user: Optional[str] = None) -> str:
        """Create/update an association identified by account(+user) on this cluster.

        ``user`` is a required field of the ASSOC parser (add_parse_req,
        data_parser parsers.c) — omitting it fails the request-body parse.
        Account-level associations are addressed with ``"user": ""``, which
        is also how the update handler matches them (it fills unset
        condition fields with empty strings, associations.c
        _foreach_update_assoc).
        """
        assoc: dict[str, Any] = {
            "account": account,
            "cluster": self.cluster_name,
            "user": user or "",
        }
        assoc.update(fields)
        self._request("POST", self._db("associations/"), body={"associations": [assoc]})
        return ""

    @staticmethod
    def _nested(path: Sequence[str], value: Any) -> dict:  # noqa: ANN401
        """Build a nested dict from a data_parser field path."""
        result: Any = value
        for key in reversed(path):
            result = {key: result}
        return result

    @classmethod
    def _merge(cls, dst: dict, src: dict) -> dict:
        """Deep-merge src into dst (nested limit paths may share prefixes)."""
        for key, value in src.items():
            if isinstance(value, dict) and isinstance(dst.get(key), dict):
                cls._merge(dst[key], value)
            else:
                dst[key] = value
        return dst

    @staticmethod
    def _dig(obj: Optional[dict], *path: str) -> Any:  # noqa: ANN401
        """Walk a nested dict along the path, returning None when absent."""
        current: Any = obj
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    # ===== DIAGNOSTICS =====

    def get_version(self) -> str:
        """Return the SLURM version reported by the controller ping endpoint."""
        payload = self._request("GET", self._ctld("ping/"))
        release = self._dig(payload.get("meta"), "slurm", "release") or "unknown"
        return f"slurm {release}"

    def _cli_binaries_present(self) -> bool:
        """Whether the delegated CLI's sacctmgr binary is resolvable on this host.

        Mirrors how the CLI client resolves the binary: under ``slurm_bin_path``
        when set, otherwise via ``PATH``.
        """
        bin_path = self._cli.slurm_bin_path
        if bin_path:
            return (Path(bin_path) / "sacctmgr").exists()
        return which("sacctmgr") is not None

    def validate_slurm_binary(self) -> bool:
        """Verify slurmrestd responds to ping, and guard the delegated CLI.

        REST mode still shells out to sacct/sacctmgr via the internal CLI
        client for usage reporting and RawUsage reset. When those binaries are
        present, run the emulator-shadow guard — an emulator (or wrong-path
        sacct) shadowing the real binary would feed fabricated or zero usage
        straight to billing while ping happily succeeds. When they're absent,
        skip the guard: there is nothing to shadow, and execute_command now
        raises a clear BackendError at usage time rather than fabricating data.
        """
        try:
            self._request("GET", self._ctld("ping/"))
        except BackendError:
            return False
        if self._cli_binaries_present():
            return self._cli.validate_slurm_binary()
        return True

    # ===== ACCOUNTS =====

    def list_resources(self) -> list[ClientResource]:
        """Return a list of accounts in the SLURM cluster."""
        payload = self._request("GET", self._db("accounts/"))
        return [self._parse_account(account) for account in payload.get("accounts") or []]

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Return the account with the given name, or None when absent."""
        payload = self._request(
            "GET", self._db(f"account/{quote(resource_id)}"), allow_errors=True
        )
        accounts = payload.get("accounts") or []
        if not accounts:
            return None
        return self._parse_account(accounts[0])

    @staticmethod
    def _parse_account(account: dict) -> ClientResource:
        return ClientResource(
            name=account.get("name", ""),
            description=account.get("description", ""),
            organization=account.get("organization", ""),
        )

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Create an account together with its cluster association.

        Uses ``accounts_association`` — the REST equivalent of
        ``sacctmgr add account`` — so the account becomes usable on the
        cluster in a single call. Requires SLURM >= 25.11 for reliable
        behavior of this endpoint.
        """
        condition: dict[str, Any] = {
            "accounts": [name],
            "clusters": [self.cluster_name],
        }
        if parent_name:
            condition["association"] = {"parent": parent_name}
        body = {
            "association_condition": condition,
            "account": {"description": description, "organization": organization},
        }
        self._request("POST", self._db("accounts_association/"), body=body)
        return name

    def delete_resource(self, name: str) -> str:
        """Delete the account with the specified name."""
        self._request("DELETE", self._db(f"account/{quote(name)}"))
        return name

    def get_account_parent(self, account: str) -> Optional[str]:
        """Return the parent account name read from the account-level association."""
        assoc = self._get_account_association(account)
        if assoc is None:
            return None
        return assoc.get("parent_account") or None

    def set_account_parent(self, account: str, new_parent: str) -> str:
        """Reparent a SLURM account under new_parent."""
        return self._post_association(account, {"parent_account": new_parent})

    def account_has_users(self, account: str) -> bool:
        """Check if the account has user associations."""
        return any(assoc.get("user") for assoc in self._list_associations(account=account))

    def delete_all_users_from_account(self, name: str) -> str:
        """Remove all user associations from the account."""
        users = self.list_resource_users(name)
        if not users:
            return ""
        query = {"account": name, "user": ",".join(users)}
        if self.cluster_name:
            query["cluster"] = self.cluster_name
        self._request("DELETE", self._db("associations/"), query=query)
        return ""

    # ===== LIMITS =====

    def set_resource_limits(self, resource_id: str, limits_dict: dict[str, int]) -> Optional[str]:
        """Set GrpTRESMins limits on the account association."""
        tres = self._tres_dict_to_list(limits_dict)
        return self._post_association(
            resource_id, self._nested(("max", "tres", "group", "minutes"), tres)
        )

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Return GrpTRESMins limits of the account association."""
        assoc = self._get_account_association(resource_id)
        if assoc is None:
            return {}
        return self._tres_list_to_dict(self._dig(assoc, "max", "tres", "group", "minutes"))

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Return per-user MaxTRESMins limits for the account."""
        result = {}
        for assoc in self._list_associations(account=resource_id):
            user = assoc.get("user")
            if not user:
                continue
            limits = self._tres_list_to_dict(
                self._dig(assoc, "max", "tres", "minutes", "per", "job")
            )
            if limits:
                result[user] = limits
        return result

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Set MaxTRESMins limits for a specific user association.

        Mirrors the CLI client: TRES absent from limits_dict are set to -1,
        which clears the corresponding limit.
        """
        limits = {tres: limits_dict.get(tres, -1) for tres in self._cached_tres_keys()}
        tres = self._tres_dict_to_list(limits)
        return self._post_association(
            resource_id,
            self._nested(("max", "tres", "minutes", "per", "job"), tres),
            user=username,
        )

    def set_account_limits(self, account: str, limit_type: str, limits: dict) -> bool:
        """Set GrpTRESMins, MaxTRESMins, GrpTRES or MaxTRES limits.

        The CLI path issues one ``sacctmgr modify … set <type>=<tres>=<value>``
        per TRES, which is a partial update that leaves unmentioned TRES
        intact. slurmrestd replaces the whole TRES list at the target path,
        so to preserve CLI parity we read the account's current TRES at that
        path and merge the incoming values over it before POSTing.
        """
        paths = {
            "GrpTRESMins": ("max", "tres", "group", "minutes"),
            "MaxTRESMins": ("max", "tres", "minutes", "per", "job"),
            "GrpTRES": ("max", "tres", "total"),
            "MaxTRES": ("max", "tres", "per", "job"),
        }
        path = paths.get(limit_type)
        if path is None:
            # Types such as GrpWall/MaxWall/GrpSubmitJobs are valid in sacctmgr
            # but are not TRES-shaped and have no mapping here yet. Log loudly so
            # operators migrating from CLI mode notice the gap rather than only
            # seeing a success=False flag swallowed by the caller.
            logger.warning(
                "Limit type %r is not supported in REST execution mode; "
                "limits %s for account %s were not applied",
                limit_type,
                limits,
                account,
            )
            msg = f"Unsupported limit type for REST execution mode: {limit_type}"
            raise BackendError(msg)
        assoc = self._get_account_association(account)
        merged = self._tres_list_to_dict(self._dig(assoc, *path)) if assoc else {}
        merged.update({key: int(value) for key, value in limits.items()})
        tres = self._tres_dict_to_list(merged)
        self._post_association(account, self._nested(path, tres))
        return True

    def get_account_limits(self, account: str) -> dict:
        """Get current account limits (GrpTRES, GrpTRESMins, MaxTRES, MaxTRESMins)."""
        assoc = self._get_account_association(account)
        paths = {
            "GrpTRES": ("max", "tres", "total"),
            "GrpTRESMins": ("max", "tres", "group", "minutes"),
            "MaxTRES": ("max", "tres", "per", "job"),
            "MaxTRESMins": ("max", "tres", "minutes", "per", "job"),
        }
        # String values for CLI client parity (see SlurmClient._parse_tres_string)
        return {
            limit_type: {
                key: str(value)
                for key, value in self._tres_list_to_dict(self._dig(assoc, *path)).items()
            }
            for limit_type, path in paths.items()
        }

    # ===== ASSOCIATIONS =====

    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Return the association between the user and the account, if it exists."""
        associations = self._list_associations(account=resource_id, user=user)
        if not associations:
            return None
        assoc = associations[0]
        limits = self._tres_list_to_dict(self._dig(assoc, "max", "tres", "group", "minutes"))
        return Association(
            account=assoc.get("account", ""),
            user=assoc.get("user", ""),
            value=limits.get("cpu", 0),
        )

    def _add_user_associations(
        self,
        username: str,
        resource_id: str,
        default_account: Optional[str],
        partitions: Optional[Sequence[str]] = None,
        use_parent_fairshare: bool = True,
    ) -> str:
        """POST users_association — the REST equivalent of ``sacctmgr add user``."""
        condition: dict[str, Any] = {
            "users": [username],
            "accounts": [resource_id],
            "clusters": [self.cluster_name],
        }
        if partitions:
            condition["partitions"] = list(partitions)
        if use_parent_fairshare:
            condition["association"] = {"fairshare": FAIRSHARE_USE_PARENT}
        body: dict[str, Any] = {"association_condition": condition}
        if default_account:
            body["user"] = {"default": {"account": default_account}}
        self._request("POST", self._db("users_association/"), body=body)
        return ""

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = ""
    ) -> str:
        """Create an association between the account and the user."""
        return self._add_user_associations(username, resource_id, default_account)

    def create_association_with_partition(
        self,
        username: str,
        resource_id: str,
        partition: str,
        default_account: Optional[str] = "",
    ) -> str:
        """Create a user-account association with a specific partition."""
        if not _PARTITION_NAME_RE.match(partition):
            msg = f"Invalid SLURM partition name: {partition!r}"
            raise BackendError(msg)
        return self._add_user_associations(
            username,
            resource_id,
            default_account,
            partitions=[partition],
            use_parent_fairshare=False,
        )

    def create_association_with_partitions(
        self,
        username: str,
        resource_id: str,
        partitions: Sequence[str],
        default_account: Optional[str] = "",
    ) -> str:
        """Create a user-account association restricted to the given partitions."""
        if not partitions:
            msg = "partitions must be non-empty"
            raise BackendError(msg)
        for name in partitions:
            if not _PARTITION_NAME_RE.match(name):
                msg = f"Invalid SLURM partition name: {name!r}"
                raise BackendError(msg)
        return self._add_user_associations(
            username, resource_id, default_account, partitions=sorted(partitions)
        )

    def delete_association(self, username: str, resource_id: str) -> str:
        """Delete the association between the account and the user."""
        query = {"account": resource_id, "user": username}
        if self.cluster_name:
            query["cluster"] = self.cluster_name
        self._request("DELETE", self._db("associations/"), query=query)
        return ""

    def list_resource_users(self, resource_id: str) -> list[str]:
        """Return the list of users associated with the account."""
        return [
            assoc["user"] for assoc in self._list_associations(account=resource_id)
            if assoc.get("user")
        ]

    # ===== QOS MANAGEMENT =====

    def qos_exists(self, qos_name: str) -> bool:
        """Check if a QoS exists in the SLURM cluster."""
        payload = self._request("GET", self._db(f"qos/{quote(qos_name)}"), allow_errors=True)
        return bool(payload.get("qos"))

    @staticmethod
    def _map_qos_flags(flags: str) -> list[str]:
        """Map sacctmgr QoS flag spellings to data_parser enum spellings."""
        mapped = []
        for flag in flags.split(","):
            flag = flag.strip()
            if not flag:
                continue
            mapped.append(_QOS_FLAG_MAP.get(flag.lower(), flag.upper()))
        return mapped

    def create_qos(
        self,
        name: str,
        flags: Optional[str] = None,
        grp_tres: Optional[str] = None,
        max_jobs: Optional[int] = None,
        max_submit: Optional[int] = None,
        max_wall: Optional[str] = None,
        min_tres_per_job: Optional[str] = None,
    ) -> None:
        """Create a QoS with the specified parameters in a single request."""
        qos: dict[str, Any] = {"name": name}
        if flags:
            qos["flags"] = self._map_qos_flags(flags)
        limits: dict[str, Any] = {}
        if grp_tres:
            self._merge(
                limits, self._nested(("max", "tres", "total"), self._tres_str_to_list(grp_tres))
            )
        if max_jobs is not None:
            # MaxJobsPerUser path is limits/max/jobs/active_jobs/per/user in
            # every supported API version (verified against data_parser
            # v0.0.43-v0.0.46 parsers.c).
            self._merge(
                limits, self._nested(("max", "jobs", "active_jobs", "per", "user"), max_jobs)
            )
        if max_submit is not None:
            self._merge(limits, self._nested(("max", "jobs", "per", "user"), max_submit))
        if max_wall is not None:
            wall_minutes = _parse_walltime_minutes(max_wall)
            self._merge(limits, self._nested(("max", "wall_clock", "per", "job"), wall_minutes))
        if min_tres_per_job:
            min_tres = self._tres_str_to_list(min_tres_per_job)
            self._merge(limits, self._nested(("min", "tres", "per", "job"), min_tres))
        if limits:
            qos["limits"] = limits
        self._request("POST", self._db("qos/"), body={"qos": [qos]})

    def delete_qos(self, name: str) -> None:
        """Delete a QoS from the SLURM cluster."""
        self._request("DELETE", self._db(f"qos/{quote(name)}"))

    def set_account_qos(self, account: str, qos: str) -> None:
        """Set the QoS list for the account (comma-separated string accepted)."""
        qos_list = [item.strip() for item in qos.split(",") if item.strip()]
        self._post_association(account, {"qos": qos_list})

    def get_current_account_qos(self, account: str) -> str:
        """Return the QoS of the account as a comma-separated string."""
        assoc = self._get_account_association(account)
        if assoc is None:
            return ""
        return ",".join(assoc.get("qos") or [])

    def set_account_qos_list(self, account: str, qos_list: list[str]) -> None:
        """Set the full QoS list for the account."""
        self._post_association(account, {"qos": qos_list})

    def add_account_qos(self, account: str, qos_name: str) -> None:
        """Add a QoS to the account's QoS list.

        slurmrestd has no atomic ``qos+=`` equivalent to the CLI's
        ``sacctmgr modify account set qos+=<name>``, so this is a
        read-merge-write. It is therefore NOT safe under concurrent
        invocation for the same account: two overlapping calls can both read
        the same base list and the second POST wins, dropping the first
        addition. Callers must serialize QoS mutations per account (the
        periodic-settings loop does so today).
        """
        current = self.get_current_account_qos(account)
        qos_list = [item for item in current.split(",") if item]
        if qos_name not in qos_list:
            qos_list.append(qos_name)
            self._post_association(account, {"qos": qos_list})

    def set_account_default_qos(self, account: str, qos_name: str) -> None:
        """Set the default QoS for the account."""
        self._post_association(account, {"default": {"qos": qos_name}})

    # ===== FAIRSHARE =====

    def set_account_fairshare(self, account: str, fairshare: int) -> bool:
        """Set fairshare (shares_raw) for the account."""
        try:
            self._post_association(account, {"shares_raw": fairshare})
        except BackendError as e:
            msg = f"Failed to set fairshare for account {account}: {e}"
            raise BackendError(msg) from e
        return True

    def get_account_fairshare(self, account: str) -> int:
        """Get the current fairshare value for the account."""
        assoc = self._get_account_association(account)
        if assoc is None:
            return 0
        shares = assoc.get("shares_raw")
        if isinstance(shares, dict):
            # Tri-state struct {"set", "infinite", "number"}: an unset or
            # infinite value is not a real share count — treat it as parent.
            shares = (
                shares.get("number")
                if shares.get("set") and not shares.get("infinite")
                else None
            )
        if shares is None or int(shares) == FAIRSHARE_USE_PARENT:
            # An account inheriting fairshare reports the USE_PARENT sentinel
            # (0x7FFFFFFF); the CLI client renders 'parent' as 0, so match it.
            return 0
        return int(shares)

    # ===== TRES / CLUSTERS =====

    def list_tres(self) -> list[str]:
        """Return a list of TRES available in the cluster."""
        payload = self._request("GET", self._db("tres/"))
        tres_objects = payload.get("TRES") or payload.get("tres") or []
        return [self._tres_key(tres) for tres in tres_objects]

    def _cached_tres_keys(self) -> list[str]:
        """Return TRES keys, fetching once and caching for the client lifetime."""
        if self._tres_keys_cache is None:
            self._tres_keys_cache = self.list_tres()
        return self._tres_keys_cache

    def list_clusters(self) -> list[str]:
        """Return a list of cluster names known to SLURM."""
        payload = self._request("GET", self._db("clusters/"))
        return [cluster.get("name", "") for cluster in payload.get("clusters") or []]

    # ===== JOB CONTROL =====

    @staticmethod
    def _job_states(job: dict) -> set[str]:
        """Normalize job_state, which is a list of strings in recent API versions."""
        state = job.get("job_state")
        if isinstance(state, str):
            return {state}
        if isinstance(state, list):
            return set(state)
        return set()

    def _list_active_jobs(self, account: str, user: Optional[str] = None) -> list[dict]:
        # Ask slurmrestd to filter server-side so we don't download the whole
        # cluster job table; older controllers ignore or reject these params,
        # so fall back to the full fetch and always re-filter client-side.
        query: dict[str, str] = {"account": account}
        if user is not None:
            query["users"] = user
        if self.cluster_name:
            query["cluster"] = self.cluster_name
        try:
            payload = self._request("GET", self._ctld("jobs/"), query=query)
        except BackendError as e:
            # Older controllers reject the filter params; fall back to the full
            # job table and re-filter client-side. Log the original error so an
            # auth/transient failure (which _request already retried once on 401)
            # is not silently masked, and flag the unfiltered fetch since it can
            # be large on a busy cluster.
            logger.warning(
                "Filtered job query for account %s failed (%s); falling back to "
                "an unfiltered job-table fetch and filtering client-side",
                account,
                e,
            )
            payload = self._request("GET", self._ctld("jobs/"))
        jobs = []
        for job in payload.get("jobs") or []:
            if job.get("account") != account:
                continue
            if user is not None and job.get("user_name") != user:
                continue
            # Defensively drop jobs from sibling clusters in case the fallback
            # fetch (or a multi-cluster controller) returned cross-cluster rows.
            if self.cluster_name and job.get("cluster") not in (None, "", self.cluster_name):
                continue
            if not (self._job_states(job) & _ACTIVE_JOB_STATES):
                continue
            jobs.append(job)
        return jobs

    def list_active_user_jobs(self, account: str, user: str) -> list[str]:
        """List active job IDs for the account and user."""
        return [str(job.get("job_id")) for job in self._list_active_jobs(account, user)]

    def cancel_active_user_jobs(self, account: str, user: Optional[str] = None) -> None:
        """Cancel active jobs for the account (and user, when given)."""
        for job in self._list_active_jobs(account, user):
            job_id = job.get("job_id")
            if job_id is None:
                # A job without an id (e.g. an array step, or an API-version key
                # mismatch) would build "job/None" and 404; skip it rather than
                # abort the loop and leave the remaining jobs running.
                logger.warning(
                    "Active job for account %s has no job_id; skipping cancel: %s",
                    account,
                    job,
                )
                continue
            self._request("DELETE", self._ctld(f"job/{job_id}"))

    # ===== DELEGATED TO CLI (no direct REST equivalent) =====

    def get_usage_report(self, resource_ids: list[str], timezone: Optional[str] = None) -> list:
        """Per-user usage report — delegated to sacct (no sreport REST equivalent)."""
        return self._cli.get_usage_report(resource_ids, timezone)

    def get_historical_usage_report(
        self, resource_ids: list[str], year: int, month: int
    ) -> list:
        """Historical usage report — delegated to sacct (no sreport REST equivalent)."""
        return self._cli.get_historical_usage_report(resource_ids, year, month)

    def reset_raw_usage(self, account: str) -> bool:
        """Reset raw usage — delegated to sacctmgr (no REST equivalent)."""
        return self._cli.reset_raw_usage(account)

    def check_user_exists(self, username: str) -> bool:
        """Check if the user exists in the local system (local ``id`` lookup)."""
        return self._cli.check_user_exists(username)
