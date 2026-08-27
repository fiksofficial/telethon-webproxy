"""
Base carrier ABC and shared bootstrap logic.

All four carrier modes (https, https-lanes, websocket, websocket-lanes)
share the same HTTPS bootstrap:  GET bridge → POST /session → mode-specific transport.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import re
import ssl
from typing import Optional

import aiohttp

from .protocol import (
    INITIAL_WINDOW,
    SESSION_PATH,
    FrameType,
    compute_capability_from_hex,
    encode_frame,
    encode_hello,
    encode_window,
    parse_frames,
    parse_window,
)

log = logging.getLogger(__name__)

# ── Exceptions ────────────────────────────────────────────────────────────────


class CarrierError(Exception):
    """Base exception for carrier-level errors."""


class HandshakeError(CarrierError):
    """The relay rejected the HELLO or returned an unexpected WELCOME."""


class StreamClosedError(CarrierError):
    """The relay sent CLOSE for a stream we care about."""


class RelayByeError(CarrierError):
    """The relay sent BYE, tearing down the whole session."""


# ── Bootstrap result ──────────────────────────────────────────────────────────


class SessionInfo:
    """Result of a successful HTTPS bootstrap."""

    __slots__ = ("token", "carrier_mode", "base_url", "host")

    def __init__(self, token: str, carrier_mode: str, base_url: str, host: str):
        self.token = token
        self.carrier_mode = carrier_mode
        self.base_url = base_url
        self.host = host


# ── Stream bookkeeping mixin ─────────────────────────────────────────────────


class StreamManager:
    """Per-stream state shared by all carrier implementations."""

    def __init__(self) -> None:
        self._next_stream_id: int = 1
        self._recv_queues: dict[int, asyncio.Queue[bytes]] = {}
        self._send_windows: dict[int, int] = {}
        self._recv_windows: dict[int, int] = {}
        self._open_streams: set[int] = set()
        self._window_events: dict[int, asyncio.Event] = {}

    def _alloc_stream(self) -> int:
        sid = self._next_stream_id
        self._next_stream_id += 1
        self._recv_queues[sid] = asyncio.Queue()
        self._send_windows[sid] = INITIAL_WINDOW
        self._recv_windows[sid] = INITIAL_WINDOW
        self._window_events[sid] = asyncio.Event()
        self._window_events[sid].set()
        self._open_streams.add(sid)
        return sid

    def _remove_stream(self, sid: int) -> None:
        self._open_streams.discard(sid)
        self._recv_queues.pop(sid, None)
        self._send_windows.pop(sid, None)
        self._recv_windows.pop(sid, None)
        ev = self._window_events.pop(sid, None)
        if ev:
            ev.set()  # unblock any waiters

    def _clear_streams(self) -> None:
        for q in self._recv_queues.values():
            try:
                q.put_nowait(b"")
            except asyncio.QueueFull:
                pass
        for ev in self._window_events.values():
            ev.set()
        self._open_streams.clear()
        self._recv_queues.clear()
        self._send_windows.clear()
        self._recv_windows.clear()
        self._window_events.clear()

    def _dispatch_frame(self, frame_type, sid: int, payload: bytes) -> Optional[bytes]:
        """Handle one parsed frame, returns PONG bytes to send or None."""
        if frame_type == FrameType.DATA and sid in self._recv_queues:
            self._recv_queues[sid].put_nowait(payload)

        elif frame_type == FrameType.WINDOW and sid in self._send_windows:
            delta = parse_window(payload)
            self._send_windows[sid] += delta
            ev = self._window_events.get(sid)
            if ev:
                ev.set()

        elif frame_type == FrameType.CLOSE and sid in self._open_streams:
            self._open_streams.discard(sid)
            q = self._recv_queues.get(sid)
            if q:
                try:
                    q.put_nowait(b"")
                except asyncio.QueueFull:
                    pass

        elif frame_type == FrameType.PING and sid == 0:
            return encode_frame(FrameType.PONG, 0, payload)

        elif frame_type == FrameType.BYE:
            log.warning("Relay BYE: %s", payload.decode(errors="replace"))
            return None  # caller should disconnect

        return None

    async def _wait_send_window(self, sid: int, needed: int, timeout: float = 30.0) -> None:
        """Block until the send window for *sid* has at least *needed* bytes."""
        deadline = asyncio.get_event_loop().time() + timeout
        while self._send_windows.get(sid, 0) < needed:
            if sid not in self._open_streams:
                raise StreamClosedError(f"Stream {sid} closed while waiting for window")
            ev = self._window_events.get(sid)
            if not ev:
                raise StreamClosedError(f"Stream {sid} gone")
            ev.clear()
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise CarrierError("Send window timeout")
            try:
                await asyncio.wait_for(ev.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                raise CarrierError("Send window timeout")

    async def _recv_data_from_queue(self, sid: int) -> bytes:
        """Dequeue one DATA payload and return WINDOW credit to grant."""
        q = self._recv_queues.get(sid)
        if q is None:
            raise StreamClosedError(f"Stream {sid} not open")

        data = await q.get()
        if not data:
            raise StreamClosedError(f"Stream {sid} closed")

        self._recv_windows[sid] -= len(data)
        return data

    def _compute_window_grant(self, sid: int) -> Optional[bytes]:
        """If enough credit has been consumed, return a WINDOW frame to send."""
        current = self._recv_windows.get(sid, INITIAL_WINDOW)
        consumed = INITIAL_WINDOW - current
        if consumed >= INITIAL_WINDOW // 4:
            self._recv_windows[sid] = INITIAL_WINDOW
            return encode_window(sid, consumed)
        return None


# ── HTTPS bootstrap ───────────────────────────────────────────────────────────


async def bootstrap_session(
    host: str,
    secret_hex: str,
    session: aiohttp.ClientSession,
    ssl_ctx: Optional[ssl.SSLContext] = None,
) -> SessionInfo:
    """Perform the HTTPS bootstrap common to all carrier modes.

    1. GET /?bridge=<cap>  → extract bootstrap token from the bridge page.
    2. POST /api/v1/session with HELLO → receive session token + WELCOME.
    """
    cap = compute_capability_from_hex(host, secret_hex)
    base_url = f"https://{host}"

    # 1) Bridge page
    async with session.get(
        f"{base_url}/?bridge={cap}",
        ssl=ssl_ctx,
    ) as resp:
        if resp.status != 200:
            raise HandshakeError(f"Bridge page returned HTTP {resp.status}")
        body = await resp.read()
        bootstrap = _extract_bootstrap(body, cap)
        if not bootstrap:
            raise HandshakeError("Could not extract bootstrap token from bridge page")

    # 2) Session creation
    async with session.post(
        f"{base_url}{SESSION_PATH}",
        headers={
            "Authorization": f"Bearer {bootstrap}",
            "Content-Type": "application/octet-stream",
        },
        data=encode_hello(),
        ssl=ssl_ctx,
    ) as resp:
        if resp.status != 200:
            raise HandshakeError(f"Session creation returned HTTP {resp.status}")

        token = resp.headers.get("X-Session-Token")
        carrier_mode = resp.headers.get("X-Carrier-Mode", "https")
        welcome_body = await resp.read()

        frames = parse_frames(welcome_body)
        if (
            len(frames) != 1
            or frames[0].type != FrameType.WELCOME
            or frames[0].stream_id != 0
        ):
            raise HandshakeError("Invalid WELCOME from relay")

    log.info(
        "Session bootstrapped (mode=%s, token=%s…)",
        carrier_mode,
        token[:8] if token else "?",
    )
    return SessionInfo(token, carrier_mode, base_url, host)


def _extract_bootstrap(page_body: bytes, bridge_cap: str) -> Optional[str]:
    """Extract the bootstrap bearer token from the bridge HTML."""
    text = page_body.decode("utf-8", errors="replace")
    match = re.search(r'Bearer\s+([A-Za-z0-9_-]{43})', text)
    if match:
        return match.group(1)
    for m in re.finditer(r'"([A-Za-z0-9_-]{43})"', text):
        candidate = m.group(1)
        if candidate != bridge_cap:
            return candidate
    return None


# ── Abstract Carrier ──────────────────────────────────────────────────────────


class BaseCarrier(StreamManager, abc.ABC):
    """Abstract base for all carrier modes."""

    def __init__(
        self,
        host: str,
        secret_hex: str,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__()
        self._host = host
        self._secret_hex = secret_hex
        self._ssl = ssl_context
        self._http: Optional[aiohttp.ClientSession] = None
        self._session_info: Optional[SessionInfo] = None
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def carrier_mode(self) -> Optional[str]:
        return self._session_info.carrier_mode if self._session_info else None

    async def connect(self) -> None:
        """Bootstrap and start the carrier-specific transport."""
        self._http = aiohttp.ClientSession()
        try:
            self._session_info = await bootstrap_session(
                self._host, self._secret_hex, self._http, self._ssl,
            )
            await self._start_transport()
            self._connected = True
        except Exception:
            await self._http.close()
            self._http = None
            raise

    async def disconnect(self) -> None:
        """Tear down everything."""
        self._connected = False
        try:
            await self._stop_transport()
        except Exception:
            pass
        self._clear_streams()
        if self._http and not self._http.closed:
            await self._http.close()
        self._http = None
        self._session_info = None

    @abc.abstractmethod
    async def _start_transport(self) -> None:
        """Start the carrier-specific transport after bootstrap."""

    @abc.abstractmethod
    async def _stop_transport(self) -> None:
        """Stop the carrier-specific transport."""

    @abc.abstractmethod
    async def open_stream(self) -> int:
        """Open a new stream. Returns stream id."""

    @abc.abstractmethod
    async def close_stream(self, stream_id: int) -> None:
        """Send CLOSE for a stream."""

    @abc.abstractmethod
    async def send_data(self, stream_id: int, data: bytes) -> None:
        """Send DATA on a stream, respecting flow control."""

    async def recv_data(self, stream_id: int) -> bytes:
        """Receive one DATA payload from a stream."""
        data = await self._recv_data_from_queue(stream_id)
        grant = self._compute_window_grant(stream_id)
        if grant:
            await self._send_control(grant)
        return data

    @abc.abstractmethod
    async def _send_control(self, frame_bytes: bytes) -> None:
        """Send a control frame (WINDOW, PONG) via the transport."""
