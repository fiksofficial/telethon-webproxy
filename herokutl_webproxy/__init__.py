"""
herokutl-webproxy — Telegram WEB Proxy connector for Telethon.

Supports all four carrier modes:
  • ``websocket``       — single multiplexed WebSocket (default)
  • ``websocket-lanes`` — one WebSocket per stream (best isolation)
  • ``https``           — HTTP long-polling (widest compatibility)
  • ``https-lanes``     — per-stream HTTP long-polling

Auto-reconnect is available via :class:`ReconnectingCarrier`.
"""

from __future__ import annotations

__version__ = "0.2.0"

# ── Protocol core ─────────────────────────────────────────────────────────────
from .protocol import (
    FrameType,
    Frame,
    compute_capability,
    compute_capability_from_hex,
    encode_frame,
    encode_hello,
    parse_frames,
)

# ── Carrier base ──────────────────────────────────────────────────────────────
from .carrier_base import (
    BaseCarrier,
    CarrierError,
    HandshakeError,
    StreamClosedError,
    RelayByeError,
)

# ── Carrier implementations ──────────────────────────────────────────────────
from .carrier import WebSocketCarrier
from .carrier_https import HTTPSCarrier
from .carrier_lanes import WebSocketLanesCarrier
from .reconnect import ReconnectingCarrier

# ── Telethon connectors ──────────────────────────────────────────────────────
from .connector_v1 import ConnectionWebProxy
from .connector_v2 import make_web_proxy_connector, WebProxyStream

# ── Version auto-detect ──────────────────────────────────────────────────────
_herokutl_major: int | None = None

try:
    import importlib.metadata as _meta
    _tv = _meta.version("herokutl")
    _herokutl_major = int(_tv.split(".")[0])
except Exception:
    pass

if _herokutl_major is not None and _herokutl_major >= 2:
    WebProxyConnector = make_web_proxy_connector
else:
    WebProxyConnector = ConnectionWebProxy

__all__ = [
    # Protocol
    "FrameType", "Frame",
    "compute_capability", "compute_capability_from_hex",
    "encode_frame", "encode_hello", "parse_frames",
    # Carrier base
    "BaseCarrier", "CarrierError", "HandshakeError",
    "StreamClosedError", "RelayByeError",
    # Carriers
    "WebSocketCarrier", "HTTPSCarrier",
    "WebSocketLanesCarrier", "ReconnectingCarrier",
    # Telethon connectors
    "ConnectionWebProxy", "make_web_proxy_connector",
    "WebProxyStream", "WebProxyConnector",
    # Meta
    "__version__",
]
