from __future__ import annotations

import asyncio
import collections
import secrets
import struct

from . import metadata


# Messages ---------------------------------------------------------------------

class Request:
    def to_bytes(self) -> bytes:
        raise NotImplementedError


class ConnectRequest(Request):
    value = 0

    def __init__(self, transaction_id: bytes):
        self.transaction_id = transaction_id

    def to_bytes(self) -> bytes:
        return struct.pack(">ql4s", 0x41727101980, self.value, self.transaction_id)


class AnnounceRequest(Request):
    value = 1

    def __init__(self,
                 transaction_id: bytes,
                 connection_id: bytes,
                 params: metadata.Parameters):
        self.transaction_id = transaction_id
        self.connection_id = connection_id
        self.params = params

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">8sl4s20s20sqqqlL4slH",
            self.connection_id,
            self.value,
            self.transaction_id,
            self.params.info_hash,
            self.params.peer_id,
            self.params.downloaded,
            self.params.left,
            self.params.uploaded,
            0,
            0,
            secrets.token_bytes(4),
            -1,
            6881,
        )


class Response:
    @classmethod
    def from_bytes(cls, data: bytes) -> Response:
        raise NotImplementedError


class ConnectResponse(Response):
    value = 0

    def __init__(self, connection_id: bytes):
        self.connection_id = connection_id

    @classmethod
    def from_bytes(cls, data: bytes) -> ConnectResponse:
        _, _, connection_id = struct.unpack(">l4s8s", data)
        return cls(connection_id)


class AnnounceResponse(Response):
    value = 1

    def __init__(self, response: metadata.Response):
        self.response = response

    @classmethod
    def from_bytes(cls, data: bytes) -> AnnounceResponse:
        _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])
        return cls(metadata.Response.from_bytes(interval, data[20:]))


def parse(data: bytes) -> Response:
    if len(data) < 4:
        raise ValueError("Not enough bytes.")
    value = int.from_bytes(data[:4], "big")
    if value == ConnectResponse.value:
        return ConnectResponse.from_bytes(data)
    if value == AnnounceResponse.value:
        return AnnounceResponse.from_bytes(data)
    raise ValueError("Unkown message identifier.")


# Protocol ---------------------------------------------------------------------

class _BaseProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()

        self._transport = None
        self._closing = False
        self._closed = asyncio.get_event_loop().create_future()
        self._exception = None

        self._waiter = None
        self._queue = collections.deque()

    def _write(self, message):
        self._transport.sendto(message.to_bytes())

    async def _read(self):
        if exc := self._exception is not None:
            raise exc
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return self._queue.popleft()

    def _wake_up(self, exc=None):
        if waiter := self._waiter is None:
            return
        self._waiter = None
        if waiter.done():
            return
        if exc is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(exc)

    async def close(self):
        if self._transport is not None:
            self._transport.close()
        await self._closed

    # asyncio.BaseProtocol

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if not self._closing:
            self._wake_up(exc or ConnectionError("Unexpected EOF."))
        if not self._closed.done():
            self._closed.set_result(None)
        self._transport = None

    # asyncio.DatagramProtocol

    def datagram_received(self, data, addr):
        self._queue.append(data)
        self._wake_up()

    def error_received(self, exc):
        self._exception = exc
        self._wake_up(exc)


class Protocol(_BaseProtocol):
    def __init__(self, params: metadata.Parameters):
        super().__init__()

        self._params = params

    async def request(self) -> metadata.Response:
        transaction_id = secrets.token_bytes(4)
        self._write(ConnectRequest(transaction_id))
        connection_id = parse(await self._read()).connection_id
        self._write(AnnounceRequest(transaction_id, connection_id, self._params))
        return parse(await self._read()).response
