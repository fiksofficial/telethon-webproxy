"""
HTTPS long-polling carrier (``https`` carrier mode).

Two concurrent loops:
  • **uplink** — serialized POST /api/v1/up with X-Up-Seq
  • **downlink** — POST /api/v1/down with X-Down-Cursor (long poll)

Reference: PROTOCOL.md §Serialized HTTPS
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Optional

import aiohttp

from .carrier_base import BaseCarrier, CarrierError, StreamClosedError
from .protocol import (
    DATA_CHUNK,
    UP_PATH,
    DOWN_PATH,
    FrameType,
    encode_frame,
    parse_frames,
)

log = logging.getLogger(__name__)

RETRY_BUDGET = 90.0  # seconds, per spec
RETRY_DELAY = 1.0    # Retry-After: 1


class HTTPSCarrier(BaseCarrier):
    """HTTPS long-polling carrier.

    Uplink requests are serialized (one at a time, X-Up-Seq increments).
    Downlink uses a single long poll (newest-poll-wins).
    """

    def __init__(
        self,
        host: str,
        secret_hex: str,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__(host, secret_hex, ssl_context=ssl_context)
        self._up_seq: int = 1
        self._down_cursor: int = 0
        self._up_lock = asyncio.Lock()
        self._poll_task: Optional[asyncio.Task] = None

    # ── Transport lifecycle ───────────────────────────────────────────────

    async def _start_transport(self) -> None:
        si = self._session_info
        self._up_seq = 1
        self._down_cursor = 0
        self._poll_task = asyncio.get_event_loop().create_task(
            self._poll_loop()
        )
        log.info("HTTPS carrier connected to %s", si.host)

    async def _stop_transport(self) -> None:
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None

    # ── Stream operations ─────────────────────────────────────────────────

    async def open_stream(self) -> int:
        sid = self._alloc_stream()
        await self._send_up(encode_frame(FrameType.OPEN, sid))
        return sid

    async def close_stream(self, stream_id: int) -> None:
        if stream_id in self._open_streams:
            self._open_streams.discard(stream_id)
            await self._send_up(encode_frame(FrameType.CLOSE, stream_id))

    async def send_data(self, stream_id: int, data: bytes) -> None:
        offset = 0
        while offset < len(data):
            chunk_size = min(DATA_CHUNK, len(data) - offset)
            await self._wait_send_window(stream_id, chunk_size)
            chunk = data[offset : offset + chunk_size]
            self._send_windows[stream_id] -= len(chunk)
            await self._send_up(encode_frame(FrameType.DATA, stream_id, chunk))
            offset += chunk_size

    async def _send_control(self, frame_bytes: bytes) -> None:
        await self._send_up(frame_bytes)

    # ── Uplink ────────────────────────────────────────────────────────────

    async def _send_up(self, frame_data: bytes) -> None:
        """POST one frame batch to /api/v1/up with retry on 503."""
        si = self._session_info
        deadline = asyncio.get_event_loop().time() + RETRY_BUDGET

        async with self._up_lock:
            seq = self._up_seq
            while True:
                async with self._http.post(
                    f"{si.base_url}{UP_PATH}",
                    headers={
                        "Authorization": f"Bearer {si.token}",
                        "Content-Type": "application/octet-stream",
                        "X-Up-Seq": str(seq),
                    },
                    data=frame_data,
                    ssl=self._ssl,
                ) as resp:
                    if resp.status == 204:
                        self._up_seq = seq + 1
                        return
                    elif resp.status == 503:
                        if asyncio.get_event_loop().time() >= deadline:
                            raise CarrierError("Uplink 503 retry budget exhausted")
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    else:
                        raise CarrierError(f"Uplink returned HTTP {resp.status}")

    # ── Downlink ──────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Continuously long-poll /api/v1/down."""
        si = self._session_info
        try:
            while self._connected:
                async with self._http.post(
                    f"{si.base_url}{DOWN_PATH}",
                    headers={
                        "Authorization": f"Bearer {si.token}",
                        "X-Down-Cursor": str(self._down_cursor),
                        "Content-Type": "",
                    },
                    ssl=self._ssl,
                ) as resp:
                    if resp.status == 200:
                        body = await resp.read()
                        new_cursor = resp.headers.get("X-Down-Cursor")
                        if new_cursor:
                            self._down_cursor = int(new_cursor)
                        if body:
                            await self._on_batch(body)
                    elif resp.status == 204:
                        new_cursor = resp.headers.get("X-Down-Cursor")
                        if new_cursor:
                            self._down_cursor = int(new_cursor)
                    elif resp.status == 503:
                        await asyncio.sleep(RETRY_DELAY)
                    else:
                        raise CarrierError(f"Downlink returned HTTP {resp.status}")
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.warning("HTTPS poll loop error: %s", exc)
        finally:
            if self._connected:
                await self.disconnect()

    async def _on_batch(self, data: bytes) -> None:
        """Dispatch a downlink frame batch."""
        try:
            frames = parse_frames(data)
        except ValueError as exc:
            log.warning("Malformed downlink batch: %s", exc)
            return
        for frame in frames:
            pong = self._dispatch_frame(frame.type, frame.stream_id, frame.payload)
            if pong:
                await self._send_up(pong)
            if frame.type == FrameType.BYE:
                await self.disconnect()
                return
