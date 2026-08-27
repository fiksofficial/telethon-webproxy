"""
WEB proxy protocol v1 — frame codec and capability derivation.

Wire format reference: tproxy-server PROTOCOL.md
Frame codec reference: tdesktop web_proxy_frame.h, tproxy-server frame.go

All binary integers are unsigned big-endian.
"""

from __future__ import annotations

import hashlib
import hmac
import struct
from base64 import urlsafe_b64encode
from dataclasses import dataclass
from enum import IntEnum
from typing import List

# ── Protocol constants ────────────────────────────────────────────────────────

HEADER_SIZE = 8
MAX_PAYLOAD = 1 << 20           # 1 MiB
DATA_CHUNK = 64 * 1024          # 64 KiB — relay DATA ceiling
INITIAL_WINDOW = 4 * 1024 * 1024  # 4 MiB per direction per stream
MAX_STREAM_ID = 0xFF_FF_FF
MAX_BATCH_FRAMES = 4096

BRIDGE_CONTEXT_PREFIX = b"tdesktop-web-proxy-bridge-v1\n"
DD_PREFIX = b"\xdd"

WS_PATH = "/api/v1/socket"
SESSION_PATH = "/api/v1/session"
UP_PATH = "/api/v1/up"
DOWN_PATH = "/api/v1/down"
WS_UPGRADE_PATH = "/api/v1/ws"

WS_SUBPROTOCOL_PREFIX = "tproxy-v1."
WS_LANE_SUBPROTOCOL_PREFIX = "tproxy-lane-v1."


# ── Frame types ───────────────────────────────────────────────────────────────

class FrameType(IntEnum):
    """Shared frame types (PROTOCOL.md §Shared frames)."""
    OPEN    = 0x01
    DATA    = 0x02
    CLOSE   = 0x03
    WINDOW  = 0x04
    PING    = 0x05
    PONG    = 0x06
    HELLO   = 0x10
    WELCOME = 0x11
    BYE     = 0x1F


# ── Frame dataclass ───────────────────────────────────────────────────────────

@dataclass(slots=True)
class Frame:
    """One parsed shared frame."""
    type: FrameType
    stream_id: int
    payload: bytes


# ── Encoding ──────────────────────────────────────────────────────────────────

def encode_frame(
    frame_type: FrameType | int,
    stream_id: int,
    payload: bytes = b"",
) -> bytes:
    """Encode a single shared frame.

    Layout: type:u8 | stream_id:u24 | payload_length:u32 | payload
    """
    if stream_id > MAX_STREAM_ID:
        raise ValueError(f"stream_id {stream_id} exceeds 24-bit maximum")
    return (
        bytes([int(frame_type)])
        + stream_id.to_bytes(3, "big")
        + struct.pack(">I", len(payload))
        + payload
    )


def encode_hello() -> bytes:
    """Encode the client HELLO frame (version byte 0x01)."""
    return encode_frame(FrameType.HELLO, 0, b"\x01")


def encode_welcome() -> bytes:
    """Encode the relay WELCOME frame (empty payload)."""
    return encode_frame(FrameType.WELCOME, 0)


def encode_window(stream_id: int, amount: int) -> bytes:
    """Encode a WINDOW credit grant."""
    return encode_frame(FrameType.WINDOW, stream_id, struct.pack(">I", amount))


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_frames(
    data: bytes,
    *,
    max_payload: int = MAX_PAYLOAD,
) -> List[Frame]:
    """Parse a contiguous batch of shared frames.

    Raises ValueError on malformed input (matching tdesktop/tproxy-server
    rejection semantics).
    """
    if not data:
        raise ValueError("empty frame batch")

    frames: List[Frame] = []
    offset = 0

    while offset < len(data):
        if len(frames) >= MAX_BATCH_FRAMES:
            raise ValueError("too many frames in batch")
        remaining = len(data) - offset
        if remaining < HEADER_SIZE:
            raise ValueError("incomplete frame header")

        raw_type = data[offset]
        stream_id = (
            (data[offset + 1] << 16)
            | (data[offset + 2] << 8)
            | data[offset + 3]
        )
        payload_len = struct.unpack_from(">I", data, offset + 4)[0]

        if payload_len > max_payload:
            raise ValueError(
                f"payload length {payload_len} exceeds limit {max_payload}"
            )

        end = offset + HEADER_SIZE + payload_len
        if end > len(data):
            raise ValueError("incomplete frame payload")

        try:
            frame_type = FrameType(raw_type)
        except ValueError:
            frame_type = raw_type  # unknown type — pass through

        frames.append(Frame(
            type=frame_type,
            stream_id=stream_id,
            payload=data[offset + HEADER_SIZE : end],
        ))
        offset = end

    return frames


def parse_window(payload: bytes) -> int:
    """Extract the delta from a WINDOW frame payload."""
    if len(payload) != 4:
        raise ValueError("WINDOW payload must be exactly 4 bytes")
    value = struct.unpack(">I", payload)[0]
    if value == 0:
        raise ValueError("WINDOW delta must be nonzero")
    return value


# ── Capability derivation ─────────────────────────────────────────────────────

def compute_capability(host: str, secret: bytes) -> str:
    """Derive the bridge capability from host and secret.

    The secret must include the leading 0xdd byte when the user's MTProxy
    link uses random-padding mode (which all WEB links require).

    Reference: PROTOCOL.md §Bridge URL
    """
    context = BRIDGE_CONTEXT_PREFIX + host.encode("utf-8")
    mac = hmac.new(secret, context, hashlib.sha256).digest()
    return urlsafe_b64encode(mac).rstrip(b"=").decode("ascii")


def compute_capability_from_hex(host: str, secret_hex: str) -> str:
    """Convenience: accepts the hex-encoded secret as stored in proxy links.
    """
    secret = bytes.fromhex(secret_hex)
    return compute_capability(host, secret)
