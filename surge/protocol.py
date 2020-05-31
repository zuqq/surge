import asyncio
import collections

from . import extension_protocol
from . import peer_protocol


class StateMachineMixin:
    def __init__(self):
        # Maps `state` to the set of `Future`s waiting for `state`.
        self._waiters = collections.defaultdict(set)

        # Maps `(start_state, message_type)` to `(side_effect, end_state)`.
        # Transitions from a state to itself with no side effect are implicit.
        # The `Open` state is treated separately because the handshake message
        # doesn't have a length prefix.
        self._transition = {}

    @property
    def _state(self):
        return self.__class__

    @_state.setter
    def _state(self, state):
        self.__class__ = state

    def _feed(self, message):
        start_state = self._state
        (side_effect, end_state) = self._transition.get(
            (start_state, type(message)), (None, start_state)
        )
        self._state = end_state
        if side_effect is not None:
            side_effect(message)
        if end_state in self._waiters:
            for waiter in self._waiters.pop(end_state):
                waiter.set_result(None)


class Closed(StateMachineMixin, asyncio.Protocol):
    def __init__(self, info_hash, peer_id):
        super().__init__()

        self._info_hash = info_hash
        self._peer_id = peer_id

        self._transport = None
        self._exc = None
        self._closed = asyncio.Event()
        self._buffer = bytearray()

        self.handshake = asyncio.get_event_loop().create_future()

        self._data = asyncio.Queue()

    def _read_messages(self):
        while len(self._buffer) >= 4:
            n = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + n:
                break
            message = bytes(self._buffer[: 4 + n])
            del self._buffer[: 4 + n]
            self._feed(peer_protocol.parse(message))

    ### asyncio.Protocol

    def connection_made(self, transport):
        self._state = Open
        self._transport = transport
        self._write(peer_protocol.Handshake(self._info_hash, self._peer_id))
        self._write(peer_protocol.ExtensionProtocol(extension_protocol.Handshake()))

    def connection_lost(self, exc):
        if self._exc is None:
            self._exc = exc
        self._state = Closed
        self._closed.set()

    def data_received(self, data):
        self._buffer.extend(data)
        self._read()

    ### Stream

    def _read(self):
        raise ConnectionError("Reading from closed connection.")

    def _write(self, message):
        self._transport.write(message.to_bytes())

    ### Interface

    async def request(self, block):
        raise ConnectionError("Requesting on closed connection.")

    async def receive(self):
        if self._exc is not None:
            raise self._exc
        return await self._data.get()

    def close(self):
        if self._transport is not None:
            self._transport.close()

    async def wait_closed(self):
        # Don't raise here, because we're closing the connection anyway.
        await self._closed.wait()


class Open(Closed):
    def _read(self):
        if len(self._buffer) < 68:
            return
        self._state = Established
        self.handshake.set_result(peer_protocol.Handshake.from_bytes(self._buffer[:68]))
        del self._buffer[:68]
        self._read_messages()


class Protocol(Closed):
    def __init__(self, info_hash, peer_id, pieces):
        super().__init__(info_hash, peer_id)

        self.available = set()
        self.bitfield = asyncio.get_event_loop().create_future()

        def bitfield_cb(message):
            self.available = message.available(pieces)
            self.bitfield.set_result(None)

        def have_cb(message):
            self.available.add(message.piece(pieces))

        def block_cb(message):
            self._data.put_nowait((message.block(pieces), message.data))

        self._transition = {
            (Established, peer_protocol.Bitfield): (bitfield_cb, Choked),
            (Choked, peer_protocol.Unchoke): (None, Unchoked),
            (Choked, peer_protocol.Have): (have_cb, Choked),
            (Choked, peer_protocol.Block): (block_cb, Choked),
            (Unchoked, peer_protocol.Choke): (None, Choked),
            (Unchoked, peer_protocol.Have): (have_cb, Unchoked),
            (Unchoked, peer_protocol.Block): (block_cb, Unchoked),
        }


class Established(Protocol):
    def _read(self):
        self._read_messages()


class Choked(Established):
    async def request(self, block):
        if self._exc is not None:
            raise self._exc
        # Register with Unchoked so that we are woken up if the unchoke happens.
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[Unchoked].add(waiter)
        self._write(peer_protocol.Interested())
        # Wait for the unchoke to happen.
        await waiter
        self._write(peer_protocol.Request(block))


class Unchoked(Established):
    async def request(self, block):
        if self._exc is not None:
            raise self._exc
        self._write(peer_protocol.Request(block))
