"""
Multiplexed WebSocket carrier (``websocket`` carrier mode).

Reference: PROTOCOL.md §Multiplexed WebSocket
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Optional

import aiohttp

from .carrier_base import BaseCarrier, StreamClosedError
from .protocol import (
    DATA_CHUNK,
    WS_SUBPROTOCOL_PREFIX,
    WS_UPGRADE_PATH,
    FrameType,
    encode_frame,
    parse_frames,
)

log = logging.getLogger(__name__)


class WebSocketCarrier(BaseCarrier):
    """Single multiplexed WebSocket carrier.

    All streams share one WSS connection. A WebSocket loss closes the
    entire relay session (per the spec).
    """

    def __init__(
        self,
        host: str,
        secret_hex: str,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__(host, secret_hex, ssl_context=ssl_context)
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None

    # ── Transport lifecycle ───────────────────────────────────────────────

    async def _start_transport(self) -> None:
        si = self._session_info
        subprotocol = WS_SUBPROTOCOL_PREFIX + si.token
        self._ws = await self._http.ws_connect(
            f"wss://{si.host}{WS_UPGRADE_PATH}",
            protocols=[subprotocol],
            ssl=self._ssl,
            max_msg_size=4 * 1024 * 1024,
            origin=f"https://{si.host}",
        )
        self._reader_task = asyncio.get_event_loop().create_task(
            self._reader_loop()
        )
        log.info("WebSocket carrier connected to %s", si.host)

    async def _stop_transport(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reader_task = None
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

    # ── Stream operations ─────────────────────────────────────────────────

    async def open_stream(self) -> int:
        sid = self._alloc_stream()
        await self._ws_send(encode_frame(FrameType.OPEN, sid))
        return sid

    async def close_stream(self, stream_id: int) -> None:
        if stream_id in self._open_streams:
            self._open_streams.discard(stream_id)
            await self._ws_send(encode_frame(FrameType.CLOSE, stream_id))

    async def send_data(self, stream_id: int, data: bytes) -> None:
        offset = 0
        while offset < len(data):
            chunk_size = min(DATA_CHUNK, len(data) - offset)
            await self._wait_send_window(stream_id, chunk_size)
            chunk = data[offset : offset + chunk_size]
            self._send_windows[stream_id] -= len(chunk)
            await self._ws_send(encode_frame(FrameType.DATA, stream_id, chunk))
            offset += chunk_size

    async def _send_control(self, frame_bytes: bytes) -> None:
        await self._ws_send(frame_bytes)

    # ── Internals ─────────────────────────────────────────────────────────

    async def _ws_send(self, data: bytes) -> None:
        if self._ws and not self._ws.closed:
            await self._ws.send_bytes(data)

    async def _reader_loop(self) -> None:
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    await self._on_batch(msg.data)
                elif msg.type == aiohttp.WSMsgType.PING:
                    await self._ws.pong(msg.data)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.warning("WebSocket reader error: %s", exc)
        finally:
            if self._connected:
                await self.disconnect()

    async def _on_batch(self, data: bytes) -> None:
        try:
            frames = parse_frames(data)
        except ValueError as exc:
            log.warning("Malformed relay batch: %s", exc)
            return
        for frame in frames:
            pong = self._dispatch_frame(frame.type, frame.stream_id, frame.payload)
            if pong:
                await self._ws_send(pong)
            if frame.type == FrameType.BYE:
                await self.disconnect()
                return
