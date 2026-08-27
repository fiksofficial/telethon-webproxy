"""Tests for the protocol module — capability derivation and frame codec."""

from herokutl_webproxy.protocol import (
    FrameType,
    compute_capability,
    compute_capability_from_hex,
    encode_frame,
    encode_hello,
    encode_welcome,
    parse_frames,
)


# ── Capability vectors from PROTOCOL.md ───────────────────────────────────────

def test_capability_raw_secret():
    """Raw secret (no dd prefix) — PROTOCOL.md row 1."""
    result = compute_capability(
        "proxy.example.com",
        bytes.fromhex("000102030405060708090a0b0c0d0e0f"),
    )
    assert result == "MHLEY5PmW1GWqJkSrlmJpvJUiLhBH_QKy6yKg8a0JPk"


def test_capability_dd_secret():
    """dd-prefixed secret — PROTOCOL.md row 2."""
    result = compute_capability(
        "proxy.example.com",
        bytes.fromhex("dd000102030405060708090a0b0c0d0e0f"),
    )
    assert result == "IpJrt3e7sKtzPyoXy6w-Zj6GGEvsvclN66JzQEfPYLA"


def test_capability_from_hex_auto_dd():
    """compute_capability_from_hex auto-prepends dd if missing."""
    result = compute_capability_from_hex(
        "proxy.example.com",
        "000102030405060708090a0b0c0d0e0f",
    )
    # Should match the dd-prefixed vector
    assert result == "IpJrt3e7sKtzPyoXy6w-Zj6GGEvsvclN66JzQEfPYLA"


def test_capability_from_hex_already_dd():
    """compute_capability_from_hex passes through existing dd prefix."""
    result = compute_capability_from_hex(
        "proxy.example.com",
        "dd000102030405060708090a0b0c0d0e0f",
    )
    assert result == "IpJrt3e7sKtzPyoXy6w-Zj6GGEvsvclN66JzQEfPYLA"


# ── Frame codec ───────────────────────────────────────────────────────────────

def test_hello_round_trip():
    raw = encode_hello()
    frames = parse_frames(raw)
    assert len(frames) == 1
    assert frames[0].type == FrameType.HELLO
    assert frames[0].stream_id == 0
    assert frames[0].payload == b"\x01"


def test_welcome_round_trip():
    raw = encode_welcome()
    frames = parse_frames(raw)
    assert len(frames) == 1
    assert frames[0].type == FrameType.WELCOME
    assert frames[0].stream_id == 0
    assert frames[0].payload == b""


def test_data_frame():
    payload = b"hello world"
    raw = encode_frame(FrameType.DATA, 42, payload)
    frames = parse_frames(raw)
    assert len(frames) == 1
    assert frames[0].type == FrameType.DATA
    assert frames[0].stream_id == 42
    assert frames[0].payload == payload


def test_batch_parsing():
    """Multiple frames concatenated into one batch."""
    batch = (
        encode_frame(FrameType.OPEN, 1)
        + encode_frame(FrameType.DATA, 1, b"abc")
        + encode_frame(FrameType.CLOSE, 1)
    )
    frames = parse_frames(batch)
    assert len(frames) == 3
    assert frames[0].type == FrameType.OPEN
    assert frames[1].type == FrameType.DATA
    assert frames[1].payload == b"abc"
    assert frames[2].type == FrameType.CLOSE


def test_max_stream_id():
    """Stream ID at the 24-bit ceiling."""
    raw = encode_frame(FrameType.OPEN, 0xFFFFFF)
    frames = parse_frames(raw)
    assert frames[0].stream_id == 0xFFFFFF


def test_encode_rejects_oversize_stream_id():
    try:
        encode_frame(FrameType.OPEN, 0x1000000)
        assert False, "should have raised"
    except ValueError:
        pass


def test_parse_rejects_empty():
    try:
        parse_frames(b"")
        assert False, "should have raised"
    except ValueError as e:
        assert "empty" in str(e).lower()
