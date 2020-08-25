from typing import Optional

import asyncio

from . import messages
from . import tracker


class Stream:
    """A stream interface for BitTorrent connections.

    This class wraps `asyncio.StreamReader` and `asyncio.StreamWriter`; instead
    of bytes, it reads and writes BitTorrent messages. It also features an async
    `close` method and support for the async context manager protocol.
    """

    def __init__(self, peer: tracker.Peer):
        # I don't inject the reader and writer because they don't support the
        # async context manager protocol.
        self._peer = peer
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def __aenter__(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._peer.address, self._peer.port
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()
        return False

    async def read_handshake(self) -> messages.Handshake:
        return messages.parse_handshake(await self._reader.readexactly(68))

    async def read(self) -> messages.Message:
        prefix = await self._reader.readexactly(4)
        data = await self._reader.readexactly(int.from_bytes(prefix, "big"))
        return messages.parse(prefix + data)

    async def write(self, message: messages.Message):
        self._writer.write(message.to_bytes())
        await self._writer.drain()
