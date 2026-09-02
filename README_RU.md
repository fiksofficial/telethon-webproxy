# telethon-webproxy

**Коннектор Telegram WEB Proxy для Telethon (v1 и v2).**

Позволяет подключать [Telethon](https://codeberg.org/Lonami/Telethon) к Telegram через **WEB Proxy** (`tdesktop-web-proxy-bridge-v1`), появившийся в Telegram Desktop 7.1.

> [!IMPORTANT]
> Библиотека общается с relay-сервером напрямую по WebSocket.
> **Браузер не нужен.** Потребление памяти — несколько сотен КБ на соединение.

## Установка

```bash
pip install telethon-webproxy
```

Или с указанием версии Telethon:

```bash
pip install "telethon-webproxy[telethon-v1]"   # Telethon 1.x
pip install "telethon-webproxy[telethon-v2]"   # Telethon 2.x
```

## Быстрый старт

### Telethon v1

```python
from telethon import TelegramClient
from telethon_webproxy import ConnectionWebProxy

client = TelegramClient(
    "session",
    api_id,
    api_hash,
    connection=ConnectionWebProxy,
    # Третий элемент кортежа — словарь с опциями (опционально)
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
from telethon import Client
from telethon_webproxy import make_web_proxy_connector

connector = make_web_proxy_connector(
    host="proxy.example.com",
    secret_hex="dd00...",
    mode="websocket-lanes",
)

client = Client("session", api_id, api_hash, connector=connector)
```

### Автоопределение версии

```python
from telethon_webproxy import WebProxyConnector

# WebProxyConnector — это ConnectionWebProxy для Telethon v1
# или make_web_proxy_connector для Telethon v2.
# Определяется автоматически при импорте.
```

## Как это работает

```
┌──────────────┐       WSS / HTTPS    ┌───────────────┐      TCP       ┌──────────────┐
│  Ваш скрипт  │ ◄──────────────────► │  tproxy-server│ ◄────────────► │   Telegram   │
│  (Telethon)  │  Фреймы WEB Proxy v1 │  (relay)      │   MTProto      │   DC         │
└──────────────┘                      └───────────────┘                └──────────────┘
```

1. Библиотека вычисляет `capability` — HMAC-SHA256 от секрета и домена.
2. Запрашивает bridge-страницу (`GET /?bridge=<cap>`) и извлекает bootstrap-токен.
3. Создает сессию (`POST /api/v1/session`) и получает session-токен.
4. Открывает выбранный транспорт (WSS мультиплекс, WSS на каждый поток или HTTP long-polling).
5. Мультиплексирует MTProto-потоки через фреймы OPEN/DATA/CLOSE/WINDOW.

## Параметры прокси

| Параметр | Описание |
|:---------|:---------|
| `host` | Доменное имя WEB-прокси сервера (например `proxy.example.com`) |
| `secret_hex` | Hex-строка секрета MTProxy. Если не начинается с `dd`, библиотека добавит его. |
| `mode` | Режим транспорта: `websocket` (по умолч.), `websocket-lanes`, `https`. |

## Низкоуровневый API

Если вам не нужна интеграция с Telethon, используйте `WebSocketCarrier` напрямую:

```python
import asyncio
from telethon_webproxy import WebSocketCarrier

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

## Поддерживаемые серверы

- [telegramdesktop/tproxy-server](https://github.com/telegramdesktop/tproxy-server) — эталонная реализация на Go от команды Telegram
- [sleep3r/mtproto.zig](https://github.com/sleep3r/mtproto.zig) — высокопроизводительная реализация на Zig

## Тестирование

```bash
pip install -e ".[dev]"
pytest tests/
```

## Лицензия

MIT
