"""
Auto-reconnecting carrier wrapper.

Wraps any :class:`BaseCarrier` and transparently re-establishes the
session + transport when the underlying carrier drops.

Reconnect semantics by carrier mode:
  • ``websocket``: WS loss kills the session → full reconnect
  • ``websocket-lanes``: lane loss kills only that stream → lane reconnect
  • ``https``: session survives connection drops → poll retries only
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Optional, Type

from .carrier_base import BaseCarrier, CarrierError, StreamClosedError

log = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE = 1.0   # seconds
DEFAULT_BACKOFF_MAX = 30.0   # seconds


class ReconnectingCarrier(BaseCarrier):
    """Wrapper that auto-reconnects a failed carrier.

    Usage::

        from herokutl_webproxy.carrier import WebSocketCarrier
        from herokutl_webproxy.reconnect import ReconnectingCarrier

        carrier = ReconnectingCarrier(
            WebSocketCarrier,
            host="proxy.example.com",
            secret_hex="dd00…",
        )
        await carrier.connect()
    """

    def __init__(
        self,
        carrier_cls: Type[BaseCarrier],
        host: str,
        secret_hex: str,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        backoff_max: float = DEFAULT_BACKOFF_MAX,
    ) -> None:
        # Don't call super().__init__ — we delegate everything to _inner
        self._carrier_cls = carrier_cls
        self._host = host
        self._secret_hex = secret_hex
        self._ssl = ssl_context
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max

        self._inner: Optional[BaseCarrier] = None
        self._connected = False
        self._reconnect_lock = asyncio.Lock()
        self._stream_registry: dict[int, int] = {}  # outer_sid → inner_sid

    @property
    def connected(self) -> bool:
        return self._connected and self._inner is not None and self._inner.connected

    @property
    def carrier_mode(self) -> Optional[str]:
        return self._inner.carrier_mode if self._inner else None

    async def connect(self) -> None:
        self._inner = self._carrier_cls(
            self._host,
            self._secret_hex,
            ssl_context=self._ssl,
        )
        await self._inner.connect()
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        if self._inner:
            await self._inner.disconnect()
            self._inner = None
        self._stream_registry.clear()

    async def _start_transport(self) -> None:
        pass  # delegated to _inner

    async def _stop_transport(self) -> None:
        pass  # delegated to _inner

    async def open_stream(self) -> int:
        return await self._with_reconnect(self._inner.open_stream)

    async def close_stream(self, stream_id: int) -> None:
        if self._inner:
            await self._inner.close_stream(stream_id)

    async def send_data(self, stream_id: int, data: bytes) -> None:
        await self._with_reconnect(
            lambda: self._inner.send_data(stream_id, data)
        )

    async def recv_data(self, stream_id: int) -> bytes:
        return await self._with_reconnect(
            lambda: self._inner.recv_data(stream_id)
        )

    async def _send_control(self, frame_bytes: bytes) -> None:
        if self._inner:
            await self._inner._send_control(frame_bytes)

    # ── Reconnect logic ───────────────────────────────────────────────────

    async def _with_reconnect(self, coro_fn):
        """Execute *coro_fn()*; on carrier failure, reconnect and retry."""
        last_exc = None
        for attempt in range(self._max_retries + 1):
            try:
                if not self._connected:
                    raise ConnectionError("Carrier disconnected")
                if self._inner is None or not self._inner.connected:
                    await self._do_reconnect()
                return await coro_fn()
            except (CarrierError, StreamClosedError, ConnectionError, OSError) as exc:
                last_exc = exc
                log.warning(
                    "Carrier error (attempt %d/%d): %s",
                    attempt + 1,
                    self._max_retries + 1,
                    exc,
                )
                if not self._connected:
                    raise
                if attempt < self._max_retries:
                    await self._do_reconnect()

        raise CarrierError(
            f"Reconnect failed after {self._max_retries + 1} attempts"
        ) from last_exc

    async def _do_reconnect(self) -> None:
        """Tear down and rebuild the inner carrier."""
        async with self._reconnect_lock:
            # Double-check inside lock
            if self._inner and self._inner.connected:
                return

            delay = self._backoff_base
            for attempt in range(self._max_retries):
                try:
                    if self._inner:
                        try:
                            await self._inner.disconnect()
                        except Exception:
                            pass

                    log.info("Reconnecting to %s (attempt %d)…", self._host, attempt + 1)
                    self._inner = self._carrier_cls(
                        self._host,
                        self._secret_hex,
                        ssl_context=self._ssl,
                    )
                    await self._inner.connect()
                    log.info("Reconnected successfully")
                    return

                except Exception as exc:
                    log.warning("Reconnect attempt %d failed: %s", attempt + 1, exc)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._backoff_max)

            raise CarrierError(
                f"Could not reconnect after {self._max_retries} attempts"
            )
