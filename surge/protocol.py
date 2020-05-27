from typing import Tuple

import asyncio

from . import metadata
from . import peer_protocol


class InsufficientData(Exception):
    pass


class State:
    waiter = None

class Closed(State):
    pass


class Open(State):
    @staticmethod
    def read(conn):
        try:
            conn.read_handshake()
        except InsufficientData:
            pass
        else:
            conn.read_messages()


class Established(State):
    @staticmethod
    def read(conn):
        conn.read_messages()


class Choked(Established):
    @staticmethod
    async def request(conn, block):
        # Register with Unchoked so that we are woken up if the unchoke happens.
        loop = asyncio.get_running_loop()
        Unchoked.waiter = loop.create_future()
        conn.write(peer_protocol.interested())
        # Wait for the unchoke to happen.
        await Unchoked.waiter
        conn.write(peer_protocol.request(block))


class Unchoked(Established):
    @staticmethod
    async def request(conn, block):
        conn.write(peer_protocol.request(block))


class Protocol(asyncio.Protocol):
    def __init__(self, info_hash, peer_id, pieces):
        self.state = Closed

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

        # This doesn't contain `Open` because in that state we only want to
        # receive the handshake message, which is special because it doesn't
        # have a length prefix.
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

    ### State machine interface

    def transition(self, message_type, payload):
        print(message_type)
        start_state = self.state
        if (start_state, message_type) not in self._successor:
            return
        end_state = self._successor[(start_state, message_type)]
        self.state = end_state
        if (start_state, end_state) in self._effect:
            self._effect[start_state, end_state](payload)
        if end_state.waiter is not None:
            end_state.waiter.set_result(None)
            end_state.waiter = None

    def read_handshake(self):
        if len(self._buffer) < 68:
            raise InsufficientData
        # TODO: Validate the handshake.
        self.state = Established
        self.handshake.set_result(bytes(self._buffer[:68]))
        del self._buffer[:68]

    def read_messages(self):
        while len(self._buffer) >= 4:
            n = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + n:
                break
            message = bytes(self._buffer[4 : 4 + n])
            del self._buffer[: 4 + n]
            self.transition(*peer_protocol.parse(message, self._pieces))

    ### asyncio.Protocol interface

    def connection_made(self, transport):
        self.state = Open
        self._transport = transport
        self.write(peer_protocol.handshake(self._info_hash, self._peer_id))

    def connection_lost(self, exc):
        if self._exc is None:
            self._exc = exc
        self._closed.set()

    def data_received(self, data):
        self._buffer.extend(data)
        self.state.read(self)

    ### Stream interface

    def write(self, data):
        if self._exc is not None:
            raise self._exc
        self._transport.write(data)

    def close(self):
        if self._transport is not None:
            self._transport.close()

    async def wait_closed(self):
        # Don't raise here, because we're closing the connection anyway.
        await self._closed.wait()

    ### Protocol interface

    async def request(self, block: metadata.Block):
        await self.state.request(self, block)

    async def receive(self) -> Tuple[metadata.Block, bytes]:
        return await self._block_data.get()
