from __future__ import annotations

import asyncio
import collections
import dataclasses
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
        params = dataclasses.asdict(self.params)
        params.update(
            {
                "connection_id": self.connection_id,
                "value": self.value,
                "transaction_id": self.transaction_id,
                "event": 0,
                "ip": 0,
                "key": secrets.token_bytes(4),
                "num_want": -1,
            }
        )
        return struct.pack(
            ">8sl4s20s20sqqqlL4slH",
            params["connection_id"],
            params["value"],
            params["transaction_id"],
            params["info_hash"],
            params["peer_id"],
            params["downloaded"],
            params["left"],
            params["uploaded"],
            params["event"],
            params["ip"],
            params["key"],
            params["num_want"],
            params["port"],
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
        self._closed = asyncio.get_event_loop().create_future()
        self._exception = None

        self._waiter = None
        self._queue = collections.deque()

    def _write(self, message):
        self._transport.sendto(message.to_bytes())

    async def _read(self):
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return self._queue.popleft()

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

    async def close(self):
        if self._transport is not None:
            self._transport.close()
        await self._closed

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
