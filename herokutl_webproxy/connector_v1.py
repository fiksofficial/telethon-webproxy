import asyncio
import struct
from typing import Optional, Type
import logging
log = logging.getLogger(__name__)

from .carrier_base import BaseCarrier
from .carrier import WebSocketCarrier
from .carrier_https import HTTPSCarrier
from .carrier_lanes import WebSocketLanesCarrier
from .mtproxy import MTProxyObfuscator, pack_frame, unpack_frame

class _WebProxyCodec:
    tag = None
    def __init__(self, connection):
        self._conn = connection
    def encode_packet(self, data: bytes) -> bytes:
        frame = pack_frame(data, self._conn._obfuscator.is_randomized)
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
                    return unpack_frame(frame_body, self._obfuscator.is_randomized)
            chunk = await self._carrier.recv_data(self._stream_id)
            await self._carrier.grant_window(self._stream_id, len(chunk))
            decrypted_chunk = self._obfuscator.decrypt(chunk)
            self._buffer.extend(decrypted_chunk)
    async def readexactly(self, n: int) -> bytes:
        while len(self._buffer) < n:
            chunk = await self._carrier.recv_data(self._stream_id)
            await self._carrier.grant_window(self._stream_id, len(chunk))
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

_TELETHON_FOUND = True
try:
    from telethon.network.connection.connection import Connection
except ImportError:
    try:
        from herokutl.network.connection.connection import Connection
    except ImportError:
        try:
            from hikkatl.network.connection.connection import Connection
        except ImportError:
            _TELETHON_FOUND = False
            class Connection:
                packet_codec = None

class ConnectionWebProxy(Connection):
    packet_codec = None

    def __init__(self, ip: str, port: int, dc_id: int, *, loggers=None, proxy=None, local_addr=None):
        if not _TELETHON_FOUND:
            raise ImportError(
                "Neither telethon nor herokutl is installed. "
                "Please install telethon or herokutl to use ConnectionWebProxy."
            )
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
        carrier_cls = _select_carrier_cls(mode)
        self._carrier = carrier_cls(self._proxy_host, self._proxy_secret)
        try:
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
        except BaseException:
            try:
                await asyncio.shield(self.disconnect())
            except (Exception, asyncio.CancelledError):
                pass
            raise

    async def disconnect(self):
        self._connected = False
        cur_task = None
        try:
            cur_task = asyncio.current_task()
        except Exception:
            pass

        for task in (self._send_task, self._recv_task):
            if task and task is not cur_task and not task.done():
                task.cancel()
                try:
                    await task
                except (Exception, asyncio.CancelledError):
                    pass
        self._send_task = None
        self._recv_task = None

        carrier = self._carrier
        self._carrier = None
        stream_id = self._stream_id
        self._stream_id = None

        if carrier:
            try:
                if stream_id is not None:
                    try:
                        await asyncio.shield(carrier.close_stream(stream_id))
                    except (Exception, asyncio.CancelledError):
                        pass
            finally:
                try:
                    await asyncio.shield(carrier.disconnect())
                except (Exception, asyncio.CancelledError):
                    pass

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
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log.warning(f"CRASH IN SEND: {e}")
            self._log.info("Send loop error: %s", e)
        finally:
            if self._connected:
                try:
                    await self.disconnect()
                except (Exception, asyncio.CancelledError):
                    pass

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
                    return
                else:
                    self._log.debug(f"RECV {len(data)} BYTES!")
                    await self._recv_queue.put((data, None))
        except asyncio.CancelledError:
            pass
        finally:
            if self._connected:
                try:
                    await self.disconnect()
                except (Exception, asyncio.CancelledError):
                    pass

    def __str__(self):
        return f"{self._proxy_host}/WebProxy"

