"""A thin wrapper around the asyncio stream API."""

import asyncio

from . import messages
from . import tracker


class Stream:
    def __init__(self, peer: tracker.Peer):
        self._peer = peer
        self._reader = None
        self._writer = None

    async def __aenter__(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._peer.address, self._peer.port
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False

    async def read_handshake(self) -> messages.Handshake:
        return messages.Handshake.from_bytes(await self._reader.readexactly(68))

    async def read(self) -> messages.Message:
        prefix = await self._reader.readexactly(4)
        data = await self._reader.readexactly(int.from_bytes(prefix, "big"))
        return messages.parse(prefix + data)

    async def write(self, message: messages.Message):
        self._writer.write(message.to_bytes())
        await self._writer.drain()

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()
