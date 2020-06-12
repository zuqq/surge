import asyncio

from . import _extension
from . import _peer
from .. import state


class Closed(state.StateMachineMixin, asyncio.Protocol):
    def __init__(self, info_hash, peer_id):
        super().__init__()

        self._info_hash = info_hash
        self._peer_id = peer_id

        self._transport = None
        self._exception = None
        self._closed = None
        self._buffer = bytearray(int.to_bytes(68, 4, "big"))

        self._data = asyncio.Queue()

    ### asyncio.Protocol

    def connection_made(self, transport):
        self.state = Open
        self._transport = transport
        self._closed = asyncio.get_event_loop().create_future()
        self._write(_peer.Handshake(self._info_hash, self._peer_id))
        self._write(_peer.ExtensionProtocol(_extension.Handshake()))

    def connection_lost(self, exc):
        if self._exception is None:
            self._exception = exc
        self.state = Closed
        self._transport = None
        if self._closed is not None:
            self._closed.set_result(None)

    def data_received(self, data):
        self._buffer.extend(data)
        self._read()

    ### Stream

    def _read(self):
        raise ConnectionError("Reading from closed connection.")

    def _write(self, message):
        raise ConnectionError("Writing to closed connection.")

    ### Interface

    async def request(self, block):
        raise ConnectionError("Requesting on closed connection.")

    async def receive(self):
        if self._exception is not None:
            raise self._exception
        return await self._data.get()

    async def close(self):
        if self._transport is not None:
            self._transport.close()
        if self._closed is not None:
            await self._closed


class Open(Closed):
    def _read(self):
        while len(self._buffer) >= 4:
            n = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + n:
                break
            data = bytes(self._buffer[: 4 + n])
            del self._buffer[: 4 + n]
            self.feed(_peer.parse(data))

    def _write(self, message):
        self._transport.write(message.to_bytes())


class Established(Open):
    pass


class Choked(Established):
    async def request(self, block):
        if self._exception is not None:
            raise self._exception
        # Register with Unchoked so that we are woken up if the unchoke happens.
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[_peer.Unchoke].add(waiter)
        self._write(_peer.Interested())
        # Wait for the unchoke to happen.
        await waiter
        self._write(_peer.Request(block))


class Unchoked(Established):
    async def request(self, block):
        if self._exception is not None:
            raise self._exception
        self._write(_peer.Request(block))


class Protocol(Closed):
    def __init__(self, info_hash, peer_id, pieces):
        super().__init__(info_hash, peer_id)

        loop = asyncio.get_event_loop()
        self.handshake = loop.create_future()
        self._waiters[_peer.Handshake].add(self.handshake)

        self.available = set()
        self.bitfield = loop.create_future()

        def on_bitfield(message):
            self.available = message.available(pieces)
            self.bitfield.set_result(None)

        def on_have(message):
            self.available.add(message.piece(pieces))

        def on_block(message):
            self._data.put_nowait((message.block(pieces), message.data))

        self._transition = {
            (Open, _peer.Handshake): (None, Established),
            (Established, _peer.Bitfield): (on_bitfield, Choked),
            (Choked, _peer.Unchoke): (None, Unchoked),
            (Choked, _peer.Have): (on_have, Choked),
            (Choked, _peer.Block): (on_block, Choked),
            (Unchoked, _peer.Choke): (None, Choked),
            (Unchoked, _peer.Have): (on_have, Unchoked),
            (Unchoked, _peer.Block): (on_block, Unchoked),
        }

        self.state = Closed
