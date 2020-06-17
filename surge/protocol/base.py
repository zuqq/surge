import asyncio
import collections
import itertools
import weakref

from . import _peer
from .. import state


class Protocol(asyncio.Protocol):
    def __init__(self, stream):
        self._transport = None
        self._closing = False
        self._closed = asyncio.get_event_loop().create_future()
        self._buffer = bytearray(int.to_bytes(68, 4, "big"))

        self._paused = False
        self._drain_waiter = None

        stream.protocol = self
        self._stream = weakref.ref(stream)

    # asyncio.BaseProtocol

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if not self._closing:
            self._stream().set_exception(exc or ConnectionError("Unexpected EOF."))
        if not self._closed.done():
            self._closed.set_result(None)
        self._transport = None

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False
        if waiter := self._drain_waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    # asyncio.Protocol

    def data_received(self, data):
        self._buffer.extend(data)
        while len(self._buffer) >= 4:
            n = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + n:
                break
            message = bytes(self._buffer[: 4 + n])
            del self._buffer[: 4 + n]
            self._stream().feed(_peer.parse(message))

    def eof_received(self):
        return False

    # Interface

    async def write(self, message):
        self._transport.write(message.to_bytes())
        if not self._paused:
            return
        waiter = asyncio.get_running_loop().create_future()
        self._drain_waiter = waiter
        await waiter

    async def close(self):
        self._closing = True
        if self._transport is not None:
            self._transport.close()
        await self._closed


class Closed(state.StateMachineMixin):
    def __init__(self, info_hash, peer_id):
        super().__init__()

        self._info_hash = info_hash
        self._peer_id = peer_id

        self.protocol = None
        self._exception = None
        self._queue = collections.deque(maxlen=1024)

    def set_exception(self, exc):
        self._exception = exc
        for waiter in itertools.chain.from_iterable(self._waiters.values()):
            if not waiter.done():
                waiter.set_exception(exc)
        self._waiters.clear()

    async def establish(self):
        if exc := self._exception is not None:
            raise exc
        await self.protocol.write(_peer.Handshake(self._info_hash, self._peer_id))
        return await self._bitfield

    async def request(self, block):
        raise NotImplementedError

    async def receive(self):
        if exc := self._exception is not None:
            raise exc
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiters[_peer.Block].add(waiter)
            await waiter
        return self._queue.popleft()

    async def close(self):
        if self.protocol is not None:
            await self.protocol.close()


class Open(Closed):
    pass


class Choked(Closed):
    async def request(self, block):
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[_peer.Unchoke].add(waiter)
        await self.protocol.write(_peer.Interested())
        await waiter
        await self.protocol.write(_peer.Request(block))


class Unchoked(Closed):
    async def request(self, block):
        await self.protocol.write(_peer.Request(block))


class Stream(Closed):
    def __init__(self, info_hash, peer_id, pieces):
        super().__init__(info_hash, peer_id)

        self.available = set()
        self._bitfield = asyncio.get_event_loop().create_future()
        self._waiters[_peer.Bitfield].add(self._bitfield)

        def on_bitfield(message):
            self.available = message.available(pieces)

        def on_have(message):
            self.available.add(message.piece(pieces))

        def on_block(message):
            self._queue.append((message.block(pieces), message.data))

        self._transition = {
            (Closed, _peer.Handshake): (None, Open),
            (Open, _peer.Bitfield): (on_bitfield, Choked),
            (Choked, _peer.Unchoke): (None, Unchoked),
            (Choked, _peer.Have): (on_have, Choked),
            (Choked, _peer.Block): (on_block, Choked),
            (Unchoked, _peer.Choke): (None, Choked),
            (Unchoked, _peer.Have): (on_have, Unchoked),
            (Unchoked, _peer.Block): (on_block, Unchoked),
        }

        self.state = Closed
