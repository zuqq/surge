import asyncio
import collections
import itertools
import weakref

from . import _peer
from .. import state


class Protocol(asyncio.Protocol):
    def __init__(self, stream):
        self._transport = None
        self._closed = asyncio.get_event_loop().create_future()
        self._buffer = bytearray(int.to_bytes(68, 4, "big"))

        self._paused = False
        self._waiter = None

        stream.protocol = self
        self._stream = weakref.ref(stream)

    def _wake_up(self, exc=None):
        if (waiter := self._waiter) is None:
            return
        self._waiter = None
        if waiter.done():
            return
        if exc is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(exc)

    # asyncio.BaseProtocol

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if not self._transport.is_closing():
            if exc is None:
                exc = ConnectionError(
                    f"Unexpected EOF {self._transport.get_extra_info('peername')}"
                )
            self._wake_up(exc)
            self._stream().set_exception(exc)
        if not self._closed.done():
            self._closed.set_result(None)

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False
        self._wake_up()

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
        self._waiter = waiter
        await waiter

    async def close(self):
        self._transport.close()
        await self._closed


class State:
    @staticmethod
    async def establish(stream):
        raise NotImplementedError

    @staticmethod
    async def request(stream, block):
        raise NotImplementedError

    @staticmethod
    async def receive(stream):
        raise NotImplementedError


class Stream(state.StateMachine):
    def __init__(self, info_hash, peer_id):
        super().__init__()

        self._info_hash = info_hash
        self._peer_id = peer_id

        self.protocol = None
        self._exception = None

        self.queue = collections.deque(maxlen=1024)

    def set_exception(self, exc):
        self._exception = exc
        for waiter in itertools.chain.from_iterable(self._waiters.values()):
            if not waiter.done():
                waiter.set_exception(exc)
        self._waiters.clear()

    async def close(self):
        if self.protocol is not None:
            await self.protocol.close()

    # Methods that delegate to `self.state`.

    async def establish(self):
        if self._exception is not None:
            raise self._exception
        await self.protocol.write(_peer.Handshake(self._info_hash, self._peer_id))
        return await self.state.establish(self)

    async def request(self, block):
        await self.state.request(self, block)

    async def receive(self):
        if self._exception is not None:
            raise self._exception
        return await self.state.receive(self)
