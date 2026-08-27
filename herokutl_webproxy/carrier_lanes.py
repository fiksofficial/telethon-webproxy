"""
WebSocket-lanes carrier (``websocket-lanes`` carrier mode).

One WebSocket per stream — each has independent ordering, buffering, and
failure isolation.

Reference: PROTOCOL.md §Stream-aware WebSocket lanes
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
    INITIAL_WINDOW,
    WS_LANE_SUBPROTOCOL_PREFIX,
    WS_UPGRADE_PATH,
    FrameType,
    encode_frame,
    encode_window,
    parse_frames,
    parse_window,
)

log = logging.getLogger(__name__)


class _LaneSocket:
    """State for one per-stream WebSocket lane."""

    __slots__ = ("ws", "reader_task", "stream_id")

    def __init__(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        stream_id: int,
    ) -> None:
        self.ws = ws
        self.stream_id = stream_id
        self.reader_task: Optional[asyncio.Task] = None


class WebSocketLanesCarrier(BaseCarrier):
    """Per-stream WebSocket lanes carrier.

    Each ``open_stream()`` call opens a dedicated WSS connection.
    Lane failure closes only that stream; the session survives.
    """

    def __init__(
        self,
        host: str,
        secret_hex: str,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__(host, secret_hex, ssl_context=ssl_context)
        self._lanes: dict[int, _LaneSocket] = {}

    # ── Transport lifecycle ───────────────────────────────────────────────

    async def _start_transport(self) -> None:
        log.info("WebSocket-lanes carrier ready on %s", self._session_info.host)

    async def _stop_transport(self) -> None:
        for lane in list(self._lanes.values()):
            await self._close_lane(lane)
        self._lanes.clear()

    # ── Stream operations ─────────────────────────────────────────────────

    async def open_stream(self) -> int:
        sid = self._alloc_stream()
        si = self._session_info

        subprotocol = f"{WS_LANE_SUBPROTOCOL_PREFIX}{si.token}.{sid}"
        try:
            ws = await self._http.ws_connect(
                f"wss://{si.host}{WS_UPGRADE_PATH}",
                protocols=[subprotocol],
                ssl=self._ssl,
                max_msg_size=4 * 1024 * 1024,
                origin=f"https://{si.host}",
            )
        except Exception as exc:
            self._remove_stream(sid)
            raise CarrierError(f"Failed to open lane WebSocket: {exc}") from exc

        lane = _LaneSocket(ws, sid)
        self._lanes[sid] = lane

        # First message MUST begin with OPEN
        await ws.send_bytes(encode_frame(FrameType.OPEN, sid))

        # Start per-lane reader
        lane.reader_task = asyncio.get_event_loop().create_task(
            self._lane_reader(lane)
        )

        log.debug("Lane %d opened", sid)
        return sid

    async def close_stream(self, stream_id: int) -> None:
        lane = self._lanes.get(stream_id)
        if lane:
            try:
                await lane.ws.send_bytes(
                    encode_frame(FrameType.CLOSE, stream_id)
                )
            except Exception:
                pass
            await self._close_lane(lane)
            self._lanes.pop(stream_id, None)
            self._open_streams.discard(stream_id)

    async def send_data(self, stream_id: int, data: bytes) -> None:
        lane = self._lanes.get(stream_id)
        if not lane:
            raise StreamClosedError(f"Lane {stream_id} not open")

        offset = 0
        while offset < len(data):
            chunk_size = min(DATA_CHUNK, len(data) - offset)
            await self._wait_send_window(stream_id, chunk_size)
            chunk = data[offset : offset + chunk_size]
            self._send_windows[stream_id] -= len(chunk)
            await lane.ws.send_bytes(
                encode_frame(FrameType.DATA, stream_id, chunk)
            )
            offset += chunk_size

    async def recv_data(self, stream_id: int) -> bytes:
        data = await self._recv_data_from_queue(stream_id)
        grant = self._compute_window_grant(stream_id)
        if grant:
            lane = self._lanes.get(stream_id)
            if lane and not lane.ws.closed:
                await lane.ws.send_bytes(grant)
        return data

    async def _send_control(self, frame_bytes: bytes) -> None:
        # Control frames (WINDOW, PONG) go to the relevant lane
        # Parse the stream_id from the frame to route it
        if len(frame_bytes) >= 4:
            sid = (frame_bytes[1] << 16) | (frame_bytes[2] << 8) | frame_bytes[3]
            lane = self._lanes.get(sid)
            if lane and not lane.ws.closed:
                await lane.ws.send_bytes(frame_bytes)

    # ── Lane internals ────────────────────────────────────────────────────

    async def _close_lane(self, lane: _LaneSocket) -> None:
        if lane.reader_task:
            lane.reader_task.cancel()
            try:
                await lane.reader_task
            except (asyncio.CancelledError, Exception):
                pass
        if not lane.ws.closed:
            await lane.ws.close()

    async def _lane_reader(self, lane: _LaneSocket) -> None:
        """Per-lane background reader."""
        sid = lane.stream_id
        try:
            async for msg in lane.ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    try:
                        frames = parse_frames(msg.data)
                    except ValueError as exc:
                        log.warning("Lane %d: malformed batch: %s", sid, exc)
                        break  # invalid relay message → parent-carrier failure per spec
                    for frame in frames:
                        if frame.stream_id != sid and frame.stream_id != 0:
                            log.warning("Lane %d: cross-lane frame sid=%d", sid, frame.stream_id)
                            continue
                        pong = self._dispatch_frame(
                            frame.type, frame.stream_id, frame.payload
                        )
                        if pong and not lane.ws.closed:
                            await lane.ws.send_bytes(pong)
                elif msg.type == aiohttp.WSMsgType.PING:
                    await lane.ws.pong(msg.data)
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
            log.warning("Lane %d reader error: %s", sid, exc)
        finally:
            # Lane loss closes only this stream
            self._open_streams.discard(sid)
            q = self._recv_queues.get(sid)
            if q:
                try:
                    q.put_nowait(b"")
                except asyncio.QueueFull:
                    pass
