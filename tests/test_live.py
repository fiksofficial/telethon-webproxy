import asyncio
import os
import struct
import time

import pytest

from telethon_webproxy.carrier import WebSocketCarrier
from telethon_webproxy.carrier_lanes import WebSocketLanesCarrier
from telethon_webproxy.carrier_https import HTTPSCarrier
from telethon_webproxy.mtproxy import MTProxyObfuscator, pack_padded_frame, unpack_padded_frame


@pytest.mark.asyncio
async def test_live_proxy():
    """Live E2E test against a real proxy server.
    
    Needs PROXY_HOST and PROXY_SECRET environment variables.
    """
    host = os.environ.get("PROXY_HOST")
    secret = os.environ.get("PROXY_SECRET")
    mode = os.environ.get("PROXY_MODE", "websocket")
    
    if not host or not secret:
        pytest.skip("PROXY_HOST and PROXY_SECRET must be set for live test")
        
    print(f"\nConnecting to {host} (mode: {mode})...")
    
    if mode == "https":
        carrier = HTTPSCarrier(host, secret)
    elif mode == "websocket-lanes":
        carrier = WebSocketLanesCarrier(host, secret)
    else:
        carrier = WebSocketCarrier(host, secret)
    try:
        await carrier.connect()
        print("Carrier connected and bootstrapped.")
        
        stream_id = await carrier.open_stream()
        print(f"Opened stream_id={stream_id}")
        
        # Initialize MTProxy obfuscation
        obf = MTProxyObfuscator(bytes.fromhex(secret))
        
        # Send MTProxy header
        await carrier.send_data(stream_id, obf.header)
        
        # Construct req_pq_multi payload (no encryption, just raw bytes)
        nonce = os.urandom(16)
        # req_pq_multi constructor: 0xbe7e8ef1
        body = struct.pack("<I", 0xbe7e8ef1) + nonce
        
        # Envelope it in an unencrypted message
        msg_id = (int(time.time()) << 32) & ~3
        msg = (
            struct.pack("<q", 0) +          # auth_key_id = 0
            struct.pack("<Q", msg_id) +     # message_id
            struct.pack("<I", len(body)) +  # message_length
            body
        )
        
        # Pack into padded intermediate frame and encrypt
        frame = pack_padded_frame(msg)
        enc = obf.encrypt(frame)
        
        print("Sending req_pq_multi...")
        await carrier.send_data(stream_id, enc)
        
        # Receive response
        chunk = await carrier.recv_data(stream_id)
        dec = obf.decrypt(chunk)
        
        # The response is a padded intermediate frame. We need the first 4 bytes (length).
        if len(dec) < 4:
            # Just in case it's fragmented
            chunk2 = await carrier.recv_data(stream_id)
            dec += obf.decrypt(chunk2)
            
        length = struct.unpack_from("<i", dec)[0]
        print(f"Received frame of length {length}")
        
        # Extract payload
        frame_body = dec[4 : 4 + length]
        payload = unpack_padded_frame(frame_body)
        
        # Ensure it's res_pq (0x05162463)
        # The payload is auth_key_id (8), msg_id (8), msg_len (4), body...
        auth_key_id = struct.unpack_from("<q", payload, 0)[0]
        msg_id_resp = struct.unpack_from("<Q", payload, 8)[0]
        msg_len = struct.unpack_from("<I", payload, 16)[0]
        constructor = struct.unpack_from("<I", payload, 20)[0]
        
        print(f"Response: auth_key_id={auth_key_id}, constructor={hex(constructor)}")
        
        assert auth_key_id == 0, "auth_key_id should be 0 for res_pq"
        assert constructor == 0x05162463, "Response must be res_pq"
        
        await carrier.close_stream(stream_id)
        print("Test successful! The WEB proxy works.")
    finally:
        await carrier.disconnect()


if __name__ == "__main__":
    # If run directly without pytest
    import sys
    if "PROXY_HOST" not in os.environ or "PROXY_SECRET" not in os.environ:
        print("Please set PROXY_HOST and PROXY_SECRET environment variables.", file=sys.stderr)
        sys.exit(1)
    
    asyncio.run(test_live_proxy())
