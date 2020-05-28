from typing import Tuple

import asyncio

from . import metadata
from . import peer_protocol


class InsufficientData(Exception):
    pass


class Protocol(asyncio.Protocol):
    def __init__(self, info_hash, peer_id, pieces):
        self._info_hash = info_hash
        self._peer_id = peer_id
        self._pieces = pieces

        self._transport = None
        self._exc = None
        self._closed = asyncio.Event()
        self._buffer = bytearray()

        self._block_data = asyncio.Queue()

        loop = asyncio.get_running_loop()
        self.handshake = loop.create_future()
        self.bitfield = loop.create_future()

        # Transitions from a state to itself with no side effect are implicit.
        # The `Open` state is treated separately because the handshake message
        # doesn't have a length prefix.
        self._successor = {
            (Established, peer_protocol.Message.BITFIELD): Choked,
            (Choked, peer_protocol.Message.UNCHOKE): Unchoked,
            (Choked, peer_protocol.Message.BLOCK): Choked,
            (Unchoked, peer_protocol.Message.CHOKE): Choked,
            (Unchoked, peer_protocol.Message.BLOCK): Unchoked,
        }

        # Since there is at most one edge between any two states, a mapping
        # from (start_state, end_state) to effect is enough. The `Open` state
        # is again special.
        self._effect = {
            (Established, Choked): self.bitfield.set_result,
            (Choked, Choked): self._block_data.put_nowait,
            (Unchoked, Unchoked): self._block_data.put_nowait,
        }

    ### State machine

    @property
    def _state(self):
        return self.__class__

    @_state.setter
    def _state(self, next_state):
        self.__class__ = next_state

    def _transition(self, message_type, payload):
        start_state = self._state
        if (start_state, message_type) not in self._successor:
            return
        end_state = self._successor[(start_state, message_type)]
        self._state = end_state
        if (start_state, end_state) in self._effect:
            self._effect[start_state, end_state](payload)
        if end_state.waiter is not None:
            end_state.waiter.set_result(None)
            end_state.waiter = None

    def _read_handshake(self):
        if len(self._buffer) < 68:
            raise InsufficientData
        self._state = Established
        try:
            result = peer_protocol.parse_handshake(self._buffer[:68])
            self.handshake.set_result(result)
        except peer_protocol.InvalidHandshake as e:
            self.handshake.set_exception(e)
        del self._buffer[:68]

    def _read_messages(self):
        while len(self._buffer) >= 4:
            n = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + n:
                break
            message = bytes(self._buffer[4 : 4 + n])
            del self._buffer[: 4 + n]
            self._transition(*peer_protocol.parse(message, self._pieces))

    ### asyncio.Protocol implementation

    def connection_made(self, transport):
        self._state = Open
        self._transport = transport
        self._write(peer_protocol.handshake(self._info_hash, self._peer_id))

    def connection_lost(self, exc):
        if self._exc is None:
            self._exc = exc
        self._state = Protocol
        self._closed.set()

    def data_received(self, data):
        self._buffer.extend(data)
        self._read()

    ### Stream interface

    def _read(self):
        raise ConnectionError("Reading from closed connection.")

    def _write(self, data):
        if self._exc is not None:
            raise self._exc
        self._transport.write(data)

    ### Public interface

    async def request(self, block: metadata.Block):
        raise ConnectionError("Requesting on closed connection.")

    async def receive(self) -> Tuple[metadata.Block, bytes]:
        return await self._block_data.get()

    def close(self):
        if self._transport is not None:
            self._transport.close()

    async def wait_closed(self):
        # Don't raise here, because we're closing the connection anyway.
        await self._closed.wait()


class Open(Protocol):
    def _read(self):
        try:
            self._read_handshake()
        except InsufficientData:
            return
        # There might be more messages in the buffer, so we should consume them.
        self._read_messages()


class Established(Protocol):
    waiter = None

    def _read(self):
        self._read_messages()


class Choked(Established):
    async def request(self, block):
        # Register with Unchoked so that we are woken up if the unchoke happens.
        loop = asyncio.get_running_loop()
        Unchoked.waiter = loop.create_future()
        self._write(peer_protocol.interested())
        # Wait for the unchoke to happen.
        await Unchoked.waiter
        self._write(peer_protocol.request(block))


class Unchoked(Established):
    async def request(self, block):
        self._write(peer_protocol.request(block))
