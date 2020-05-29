import asyncio

from .peer_protocol import (
    InvalidHandshake,
    Message,
    handshake,
    interested,
    parse,
    parse_handshake,
    request,
)


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

        self.available = set()

        # Maps (start_state, message_type) to (side_effect, end_state).
        # Transitions from a state to itself with no side effect are implicit.
        # The `Open` state is treated separately because the handshake message
        # doesn't have a length prefix.
        self._transition = {
            (Established, Message.BITFIELD): (self._set_bitfield, Choked),
            (Choked, Message.UNCHOKE): (None, Unchoked),
            (Choked, Message.HAVE): (self.available.add, Choked),
            (Choked, Message.BLOCK): (self._block_data.put_nowait, Choked),
            (Unchoked, Message.HAVE): (self.available.add, Unchoked),
            (Unchoked, Message.CHOKE): (None, Choked),
            (Unchoked, Message.BLOCK): (self._block_data.put_nowait, Unchoked),
        }

    ### State machine

    @property
    def _state(self):
        return self.__class__

    @_state.setter
    def _state(self, next_state):
        self.__class__ = next_state

    def _feed(self, message_type, payload):
        start_state = self._state
        if (start_state, message_type) not in self._transition:
            return
        (side_effect, end_state) = self._transition[(start_state, message_type)]
        self._state = end_state
        if side_effect is not None:
            side_effect(payload)
        if end_state.waiter is not None:
            end_state.waiter.set_result(None)
            end_state.waiter = None

    def _set_bitfield(self, available):
        self.available = available
        self.bitfield.set_result(None)

    def _read_handshake(self):
        if len(self._buffer) < 68:
            raise InsufficientData
        self._state = Established
        try:
            result = parse_handshake(self._buffer[:68])
            self.handshake.set_result(result)
        except InvalidHandshake as e:
            self.handshake.set_exception(e)
        del self._buffer[:68]

    def _read_messages(self):
        while len(self._buffer) >= 4:
            n = int.from_bytes(self._buffer[:4], "big")
            if len(self._buffer) < 4 + n:
                break
            message = bytes(self._buffer[4 : 4 + n])
            del self._buffer[: 4 + n]
            self._feed(*parse(message, self._pieces))

    ### asyncio.Protocol implementation

    def connection_made(self, transport):
        self._state = Open
        self._transport = transport
        self._write(handshake(self._info_hash, self._peer_id))

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
        self._transport.write(data)

    ### Public interface

    async def request(self, block):
        raise ConnectionError("Requesting on closed connection.")

    async def receive(self):
        if self._exc is not None:
            raise self._exc
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
        if self._exc is not None:
            raise self._exc
        # Register with Unchoked so that we are woken up if the unchoke happens.
        loop = asyncio.get_running_loop()
        Unchoked.waiter = loop.create_future()
        self._write(interested())
        # Wait for the unchoke to happen.
        await Unchoked.waiter
        self._write(request(block))


class Unchoked(Established):
    async def request(self, block):
        if self._exc is not None:
            raise self._exc
        self._write(request(block))
