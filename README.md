# herokutl-webproxy

**Telegram WEB Proxy connector for Telethon (v1 & v2).**

Allows connecting [Telethon](https://codeberg.org/Lonami/Telethon) to Telegram through a **WEB Proxy** (`tdesktop-web-proxy-bridge-v1`), introduced in Telegram Desktop 7.1.

> [!IMPORTANT]
> The library communicates with the relay server directly over WebSocket.
> **No browser required.** Memory usage is a few hundred KB per connection.

## Installation

```bash
pip install herokutl-webproxy
```

Or with a specific Telethon version:

```bash
pip install "herokutl-webproxy[herokutl-v1]"   # Telethon 1.x
pip install "herokutl-webproxy[herokutl-v2]"   # Telethon 2.x
```

## Quick Start

### Telethon v1

```python
from herokutl import TelegramClient
from herokutl_webproxy import ConnectionWebProxy

client = TelegramClient(
    "session",
    api_id,
    api_hash,
    connection=ConnectionWebProxy,
    # Third tuple element is an options dict (optional)
    proxy=("proxy.example.com", "dd00...", {"mode": "websocket-lanes"}),
)

async def main():
    await client.start()
    me = await client.get_me()
    print(me.first_name)

import asyncio
asyncio.run(main())
```

### Telethon v2

```python
from herokutl import Client
from herokutl_webproxy import make_web_proxy_connector

connector = make_web_proxy_connector(
    host="proxy.example.com",
    secret_hex="dd00...",
    mode="websocket-lanes",
)

client = Client("session", api_id, api_hash, connector=connector)
```

### Auto-detection

```python
from herokutl_webproxy import WebProxyConnector

# WebProxyConnector is ConnectionWebProxy for Telethon v1
# or make_web_proxy_connector for Telethon v2.
# Detected automatically at import time.
```

## How It Works

```
┌──────────────┐       WSS / HTTPS    ┌───────────────┐      TCP       ┌──────────────┐
│  Your script │ ◄──────────────────► │  tproxy-server│ ◄────────────► │   Telegram   │
│  (Telethon)  │   WEB Proxy v1 frames│  (relay)      │   MTProto      │   DC         │
└──────────────┘                      └───────────────┘                └──────────────┘
```

1. The library computes a `capability` — HMAC-SHA256 of the secret and the domain.
2. Fetches the bridge page (`GET /?bridge=<cap>`) and extracts the bootstrap token.
3. Creates a session (`POST /api/v1/session`) and receives a session token.
4. Opens the selected transport (WSS multiplex, WSS per stream, or HTTP long-polling).
5. Multiplexes MTProto streams through OPEN/DATA/CLOSE/WINDOW frames.

## Proxy Options

| Parameter | Description |
|:----------|:------------|
| `host` | Domain name of the WEB proxy server (e.g. `proxy.example.com`) |
| `secret_hex` | Hex string of the MTProxy secret. If it doesn't start with `dd`, the library adds it. |
| `mode` | Transport mode: `websocket` (default), `websocket-lanes`, `https`. |

## Low-Level API

If you don't need Telethon integration, use `WebSocketCarrier` directly:

```python
import asyncio
from herokutl_webproxy import WebSocketCarrier

async def main():
    carrier = WebSocketCarrier("proxy.example.com", "dd00…")
    await carrier.connect()

    stream_id = await carrier.open_stream()
    await carrier.send_data(stream_id, b"\x00\x00\x00\x00...")  # raw MTProto
    response = await carrier.recv_data(stream_id)

    await carrier.close_stream(stream_id)
    await carrier.disconnect()

asyncio.run(main())
```

## Supported Servers

- [telegramdesktop/tproxy-server](https://github.com/telegramdesktop/tproxy-server) — reference Go implementation by the Telegram team
- [sleep3r/mtproto.zig](https://github.com/sleep3r/mtproto.zig) — high-performance Zig implementation

## Testing

```bash
pip install -e ".[dev]"
pytest tests/
```

## License

MIT
