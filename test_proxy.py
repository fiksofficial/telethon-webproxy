import asyncio
from telethon import TelegramClient
from telethon_webproxy import ConnectionWebProxy

api_id = 4
api_hash = '014b35b6184100b085b0d0572f9b5103'

proxy = ('tgweb-de.mooo.com', 'a1dbf94c153a10f494ff0cac58086da3', {'mode': 'https', 'reconnect': False})

async def main():
    client = TelegramClient('anon', api_id, api_hash, connection=ConnectionWebProxy, proxy=proxy)
    print("Connecting...")
    await client.connect()
    print("Connected! is_user_authorized:", await client.is_user_authorized())

asyncio.run(main())
