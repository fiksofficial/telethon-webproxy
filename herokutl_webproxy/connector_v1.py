import asyncio
import struct
from typing import Optional, Type
import logging
log = logging.getLogger(__name__)

from .carrier_base import BaseCarrier
from .carrier import WebSocketCarrier
from .carrier_https import HTTPSCarrier
from .carrier_lanes import WebSocketLanesCarrier
from .reconnect import ReconnectingCarrier
from .mtproxy import MTProxyObfuscator, pack_padded_frame, unpack_padded_frame

class _WebProxyCodec:
    tag = None
    def __init__(self, connection):
        self._conn = connection
    def encode_packet(self, data: bytes) -> bytes:
        frame = pack_padded_frame(data)
        return self._conn._obfuscator.encrypt(frame)
    async def read_packet(self, reader) -> bytes:
        return await reader.read()

class _WebProxyReader:
    def __init__(self, carrier: BaseCarrier, stream_id: int, obfuscator: MTProxyObfuscator):
        self._carrier = carrier
        self._stream_id = stream_id
        self._obfuscator = obfuscator
        self._buffer = bytearray()
    async def read(self) -> bytes:
        while True:
            if len(self._buffer) >= 4:
                msg_len = struct.unpack_from("<i", self._buffer)[0]
                total = 4 + msg_len
                if len(self._buffer) >= total:
                    frame_body = bytes(self._buffer[4:total])
                    del self._buffer[:total]
                    return unpack_padded_frame(frame_body)
            chunk = await self._carrier.recv_data(self._stream_id)
            import logging; logging.debug(f"CARRIER RECV CHUNK: {len(chunk)}")
            decrypted_chunk = self._obfuscator.decrypt(chunk)
            self._buffer.extend(decrypted_chunk)
    async def readexactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            chunk = await self._carrier.recv_data(self._stream_id)
            import logging; logging.debug(f"CARRIER RECV CHUNK: {len(chunk)}")
            decrypted_chunk = self._obfuscator.decrypt(chunk)
            self._buffer.extend(decrypted_chunk)
        result = bytes(self._buffer[:n])
        del self._buffer[:n]
        return result

def _select_carrier_cls(mode: str) -> Type[BaseCarrier]:
    if mode == "websocket": return WebSocketCarrier
    elif mode == "websocket-lanes": return WebSocketLanesCarrier
    elif mode in ("https", "https-lanes"): return HTTPSCarrier
    raise ValueError(f"Unknown mode {mode}")

try:
    import herokutl
    from herokutl.network.connection.connection import Connection
    class ConnectionWebProxy(Connection):
        packet_codec = None
        def __init__(self, ip: str, port: int, dc_id: int, *, loggers, proxy=None, local_addr=None):
            self._ip = ip
            self._port = port
            self._dc_id = dc_id
            self._log = loggers[__name__] if isinstance(loggers, dict) else log
            self._proxy = proxy
            self._local_addr = local_addr
            self._carrier = None
            self._stream_id = None
            self._reader = None
            self._codec = None
            self._connected = False
            self._send_task = None
            self._recv_task = None
            self._send_queue = asyncio.Queue(1)
            self._recv_queue = asyncio.Queue(1)
            if not proxy or len(proxy) < 2:
                raise ValueError("proxy must be ('host', 'secret_hex')")
            self._proxy_host = proxy[0]
            self._proxy_secret = proxy[1]
            self._options = proxy[2] if len(proxy) > 2 else {}
        async def connect(self, timeout=None, ssl=None):
            mode = self._options.get("mode", "websocket")
            use_reconnect = self._options.get("reconnect", True)
            carrier_cls = _select_carrier_cls(mode)
            if use_reconnect:
                self._carrier = ReconnectingCarrier(carrier_cls, self._proxy_host, self._proxy_secret)
            else:
                self._carrier = carrier_cls(self._proxy_host, self._proxy_secret)
            await asyncio.wait_for(self._carrier.connect(), timeout=timeout or 30)
            self._stream_id = await self._carrier.open_stream()
            self._obfuscator = MTProxyObfuscator(bytes.fromhex(self._proxy_secret), dc_idx=self._dc_id)
            await self._carrier.send_data(self._stream_id, self._obfuscator.header)
            self._reader = _WebProxyReader(self._carrier, self._stream_id, self._obfuscator)
            self._codec = _WebProxyCodec(self)
            self._connected = True
            loop = asyncio.get_running_loop()
            self._send_task = loop.create_task(self._send_loop())
            self._recv_task = loop.create_task(self._recv_loop())
        async def disconnect(self):
            self._connected = False
            for task in (self._send_task, self._recv_task):
                if task:
                    task.cancel()
                    try:
                        await task
                    except Exception: pass
            if self._carrier:
                if self._stream_id is not None:
                    try:
                        await self._carrier.close_stream(self._stream_id)
                    except Exception: pass
                await self._carrier.disconnect()
                self._carrier = None
        def send(self, data):
            if not self._connected:
                raise ConnectionError("Not connected")
            return self._send_queue.put(data)
        async def recv(self):
            while self._connected:
                result, err = await self._recv_queue.get()
                if err:
                    raise err
                if result:
                    return result
            raise ConnectionError("Not connected")
        async def _send_loop(self):
            try:
                while self._connected:
                    data = await self._send_queue.get()
                    encoded = self._codec.encode_packet(data)
                    await self._carrier.send_data(self._stream_id, encoded)
                    self._log.debug(f"SENT {len(encoded)} BYTES! {encoded.hex()[:32]}...")
            except asyncio.CancelledError: pass
            except Exception as e:
                self._log.warning(f"CRASH IN RECV: {e}")
                self._log.info("Send loop error: %s", e)
                await self.disconnect()
        async def _recv_loop(self):
            try:
                while self._connected:
                    try:
                        data = await self._reader.read()
                    except Exception as e:
                        self._log.warning(f"CRASH IN RECV: {e}")
                        from .carrier_base import StreamClosedError
                        if isinstance(e, StreamClosedError):
                            e = ConnectionError(str(e))
                        await self._recv_queue.put((None, e))
                        await self.disconnect()
                        return
                    else:
                        self._log.debug(f"RECV {len(data)} BYTES!")
                        await self._recv_queue.put((data, None))
            except asyncio.CancelledError: pass
            finally:
                await self.disconnect()
        def __str__(self):
            return f"{self._proxy_host}/WebProxy"
except ImportError:
    pass
