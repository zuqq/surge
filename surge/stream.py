import asyncio
import contextlib

from . import messages


class Stream:
    """A stream interface for BitTorrent connections.

    This class wraps `asyncio.StreamReader` and `asyncio.StreamWriter`; instead
    of bytes, it reads and writes BitTorrent messages (i.e., the classes defined
    in the `messages` module).
    """

    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer

    async def read_handshake(self):
        return messages.parse_handshake(await self._reader.readexactly(68))

    async def read(self):
        prefix = await self._reader.readexactly(4)
        data = await self._reader.readexactly(int.from_bytes(prefix, "big"))
        return messages.parse(prefix + data)

    async def write(self, message):
        self._writer.write(message.to_bytes())
        await self._writer.drain()


@contextlib.asynccontextmanager
async def open_stream(peer):
    reader, writer = await asyncio.open_connection(peer.address, peer.port)
    try:
        yield Stream(reader, writer)
    finally:
        writer.close()
        await writer.wait_closed()
