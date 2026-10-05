"""Route the STOMP WebSocket through the configured global_proxy.

stomp.py's ``WSTransport.attempt_connection`` calls ``websocket.create_connection``
with only a URL, timeout, headers and SSL options, so the proxy the agent uses for
its REST calls never reached the broker connection. websocket-client already
supports HTTP CONNECT and SOCKS proxies through ``create_connection`` keyword
options; this module supplies them.

Rather than copying stomp.py's connect loop (it relies on name-mangled private
attributes), ``ProxiedWSTransport`` sets the options for the duration of its own
``attempt_connection`` call in a thread-local, and a thin stand-in for the
``websocket`` module that ``stomp.adapter.ws`` imported adds them to
``create_connection``. Connections without a proxy, and code outside a proxied
connect attempt, see the unchanged websocket-client behaviour, including its
``http_proxy`` / ``https_proxy`` environment handling.
"""

from __future__ import annotations

import threading
from types import ModuleType
from typing import Any, Optional
from urllib.parse import unquote, urlsplit

import stomp
import websocket
from stomp.adapter import ws as stomp_ws
from stomp.connect import BaseConnection
from stomp.protocol import Protocol12

# global_proxy schemes the agent accepts: the ones httpx can use for REST.
SUPPORTED_PROXY_SCHEMES = frozenset({"http", "https", "socks5", "socks5h"})
_DEFAULT_PORTS = {"http": 80, "socks5h": 1080}

# A non-empty no_proxy list that matches no host. websocket-client falls back to the
# NO_PROXY environment variable when given none, but httpx sends every request
# through an explicit proxy, so the WebSocket must not bypass it either.
NO_BYPASS = ["no-proxy-bypass.invalid"]

_active = threading.local()


def redacted_proxy(proxy_url: str) -> str:
    """``scheme://host[:port]`` of a proxy URL, without credentials."""
    parts = urlsplit(proxy_url)
    host = f"[{parts.hostname}]" if parts.hostname and ":" in parts.hostname else parts.hostname
    return f"{parts.scheme}://{host}" + (f":{parts.port}" if parts.port else "")


def websocket_proxy_options(proxy_url: Optional[str]) -> dict[str, Any]:
    """Translate a proxy URL into websocket-client ``create_connection`` options.

    An empty value means no explicit proxy. websocket-client cannot speak TLS to
    the proxy itself, so an ``https://`` proxy also yields no options: the
    WebSocket then connects as without ``global_proxy`` (environment proxies
    honoured); the config loader warns about it once. ``socks5`` becomes
    ``socks5h`` so the proxy resolves the broker name, as httpx does for REST.
    """
    if not proxy_url:
        return {}
    parts = urlsplit(proxy_url)
    scheme = parts.scheme.lower()
    if scheme == "https":
        return {}
    if scheme not in SUPPORTED_PROXY_SCHEMES or not parts.hostname:
        msg = f"Unsupported global_proxy for the STOMP WebSocket: {redacted_proxy(proxy_url)}"
        raise ValueError(msg)
    proxy_type = "socks5h" if scheme == "socks5" else scheme
    options: dict[str, Any] = {
        "proxy_type": proxy_type,
        "http_proxy_host": parts.hostname,
        "http_proxy_port": parts.port or _DEFAULT_PORTS[proxy_type],
        "http_no_proxy": NO_BYPASS,
    }
    if parts.username:
        options["http_proxy_auth"] = (unquote(parts.username), unquote(parts.password or ""))
    return options


class _WebsocketModule(ModuleType):
    """Stand-in for ``websocket`` inside ``stomp.adapter.ws``.

    Delegates everything to the real module; ``create_connection`` gains the
    proxy options of the connect attempt running on this thread, if any.
    """

    def __init__(self) -> None:
        super().__init__("websocket")

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        return getattr(websocket, name)

    @staticmethod
    def create_connection(url: str, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        options = getattr(_active, "options", None)
        if options:
            kwargs = {**options, **kwargs}
        return websocket.create_connection(url, *args, **kwargs)


def _install() -> None:
    """Replace the ``websocket`` name inside ``stomp.adapter.ws`` with the stand-in.

    Process-wide but inert: the stand-in delegates every attribute to the real
    module and only adds options while a ``ProxiedWSTransport`` connect attempt
    runs on the calling thread, so other stomp.py connections are unaffected.
    Idempotent; done the first time a proxied transport is created.
    """
    if not isinstance(stomp_ws.websocket, _WebsocketModule):
        stomp_ws.websocket = _WebsocketModule()


class ProxiedWSTransport(stomp_ws.WSTransport):
    """``WSTransport`` whose connect attempts go through an explicit proxy."""

    def __init__(
        self,
        *args: Any,  # noqa: ANN401
        proxy_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Create the transport; ``proxy_options`` go to ``create_connection``."""
        super().__init__(*args, **kwargs)
        self.proxy_options = proxy_options or {}
        if self.proxy_options:
            _install()

    def attempt_connection(self) -> None:
        """Connect as stomp.py does, with this transport's proxy options applied."""
        _active.options = self.proxy_options
        try:
            super().attempt_connection()
        finally:
            _active.options = None


# WSStompConnection parameters that belong to the protocol layer; every other
# one is a WSTransport parameter of the same name.
_PROTOCOL_PARAMS = ("heartbeats", "auto_content_length", "heart_beat_receive_scale")


class ProxiedWSStompConnection(stomp.WSStompConnection):
    """``WSStompConnection`` built on ``ProxiedWSTransport``.

    Takes the same keyword arguments as ``WSStompConnection`` and wires them the
    same way, so the protocol layer (heartbeats, vhost) uses the proxied transport
    from the start.
    """

    def __init__(
        self,
        proxy_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Create the connection; ``kwargs`` are ``WSStompConnection`` arguments."""
        protocol_kwargs = {k: kwargs.pop(k) for k in _PROTOCOL_PARAMS if k in kwargs}
        # Accepted but unused by WSStompConnection itself; WSTransport has no such parameter.
        kwargs.pop("ws", None)
        transport = ProxiedWSTransport(proxy_options=proxy_options, **kwargs)
        BaseConnection.__init__(self, transport)
        Protocol12.__init__(self, transport, **protocol_kwargs)


def ws_stomp_connection(proxy_url: Optional[str] = None, **kwargs: Any) -> stomp.WSStompConnection:  # noqa: ANN401
    """Build the agent's STOMP WebSocket connection, through ``proxy_url`` if set.

    Takes ``WSStompConnection`` keyword arguments. Without a usable proxy (none, or
    an ``https://`` one) the result is a plain ``WSStompConnection``.
    """
    options = websocket_proxy_options(proxy_url)
    if not options:
        return stomp.WSStompConnection(**kwargs)
    return ProxiedWSStompConnection(proxy_options=options, **kwargs)
