"""UDP Tracker Protocol (BEP 15)

Minimal message flow:

    Us                       Tracker
     |      ConnectRequest      |
     |------------------------->|
     |     ConnectResponse      |
     |<-------------------------|
     |      AnnounceRequest     |
     |------------------------->|
     |     AnnounceResponse     |
     |<-------------------------|

Retransmission:

If the tracker doesn't respond to a request within `15 * 2 ** n` seconds, where
`n` starts at `0` and is incremented every time this happens, then the request
is retransmitted; if `n` reaches `9`, we give up.
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import enum
import functools
import secrets
import struct
import time
import urllib

from . import _metadata


# Messages ---------------------------------------------------------------------


class Request:
    def to_bytes(self) -> bytes:
        raise NotImplementedError  # pragma: no cover


class ConnectRequest(Request):
    value = 0

    def __init__(self, transaction_id: bytes):
        self.transaction_id = transaction_id

    def to_bytes(self) -> bytes:
        return struct.pack(">ql4s", 0x41727101980, self.value, self.transaction_id)


class AnnounceRequest(Request):
    value = 1

    def __init__(
            self,
            transaction_id: bytes,
            connection_id: bytes,
            params: _metadata.Parameters):
        self.transaction_id = transaction_id
        self.connection_id = connection_id
        self.params = params

    def to_bytes(self) -> bytes:
        ps = dataclasses.asdict(self.params)
        ps.update(
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
            ps["connection_id"],
            ps["value"],
            ps["transaction_id"],
            ps["info_hash"],
            ps["peer_id"],
            ps["downloaded"],
            ps["left"],
            ps["uploaded"],
            ps["event"],
            ps["ip"],
            ps["key"],
            ps["num_want"],
            ps["port"],
        )


class Response:
    @classmethod
    def from_bytes(cls, data: bytes) -> Response:
        raise NotImplementedError  # pragma: no cover


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

    def __init__(self, response: _metadata.Response):
        self.response = response

    @classmethod
    def from_bytes(cls, data: bytes) -> AnnounceResponse:
        _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])
        return cls(_metadata.Response.from_bytes(interval, data[20:]))


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


class Protocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()

        self._transport = None
        self._closed = asyncio.get_event_loop().create_future()
        self._exception = None

        # If this limit is ever hit, something went very wrong: we only expect
        # two datagrams from the tracker!
        self._queue = collections.deque(maxlen=10)
        self._waiter = None

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

    async def read(self) -> Response:
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return parse(self._queue.popleft())

    def write(self, message: Request):
        # There's no flow control for writes because we only send two datagrams
        # (plus retransmits, but those use exponential backoff).
        self._transport.sendto(message.to_bytes())

    async def close(self):
        self._transport.close()
        await self._closed

    # asyncio.BaseProtocol

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if not self._transport.is_closing():
            if exc is None:
                peer = self._transport.get_extra_info("peername")
                exc = ConnectionError(f"Unexpected EOF {peer}.")
            self._exception = exc
            self._wake_up(exc)
        if not self._closed.done():
            self._closed.set_result(None)

    # asyncio.DatagramProtocol

    def datagram_received(self, data, addr):
        self._queue.append(data)
        self._wake_up()

    def error_received(self, exc):
        self._exception = exc
        self._wake_up(exc)


# Transducer -------------------------------------------------------------------


class State(enum.IntEnum):
    CONNECT = 0
    ANNOUNCE = 1


def udp(params: _metadata.Parameters):
    state = State.CONNECT
    for n in range(9):
        if state is State.CONNECT:
            transaction_id = secrets.token_bytes(4)
            received = yield (ConnectRequest(transaction_id), 15 * 2 ** n)
            if isinstance(received, ConnectResponse):
                connection_id = received.connection_id
                # TODO: Pass in the time.
                connection_time = time.monotonic()
                state = State.ANNOUNCE
        if state is State.ANNOUNCE:
            message = AnnounceRequest(transaction_id, connection_id, params)
            received = yield (message, 15 * 2 ** n)
            if isinstance(received, AnnounceResponse):
                return received.response
            if time.monotonic() - connection_time >= 60:
                state = State.CONNECT
    raise ConnectionError("Maximal number of retries reached.")
