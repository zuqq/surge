"""Implementation of the UDP tracker protocol.

Specification: [BEP 0015]

The UDP tracker protocol is a lightweight alternative to the original HTTP-based
way of communicating with trackers. It is a simple two-step protocol built on
top of UDP, with a custom retransmission mechanism using exponential backoff.

Typical message flow:

    Us                       Tracker
     |      ConnectRequest      |
     |------------------------->|
     |     ConnectResponse      |
     |<-------------------------|
     |      AnnounceRequest     |
     |------------------------->|
     |     AnnounceResponse     |
     |<-------------------------|

[BEP 0015]: http://bittorrent.org/beps/bep_0015.html
"""

from __future__ import annotations
from typing import Union

import asyncio
import collections
import dataclasses
import enum
import functools
import secrets
import struct
import urllib

from . import _metadata


# Messages ---------------------------------------------------------------------


class ConnectRequest:
    value = 0

    def __init__(self, transaction_id: bytes):
        self.transaction_id = transaction_id

    def to_bytes(self) -> bytes:
        return struct.pack(">ql4s", 0x41727101980, self.value, self.transaction_id)


class AnnounceRequest:
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
            self.params.port,
        )


class ConnectResponse:
    value = 0

    def __init__(self, connection_id: bytes):
        self.connection_id = connection_id

    @classmethod
    def from_bytes(cls, data: bytes) -> ConnectResponse:
        _, _, connection_id = struct.unpack(">l4s8s", data)
        return cls(connection_id)


class AnnounceResponse:
    value = 1

    def __init__(self, response: _metadata.Response):
        self.response = response

    @classmethod
    def from_bytes(cls, data: bytes) -> AnnounceResponse:
        _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])
        return cls(_metadata.Response.from_bytes(interval, data[20:]))


def parse(data: bytes) -> Union[ConnectResponse, AnnounceResponse]:
    if len(data) < 4:
        raise ValueError("Not enough bytes.")
    value = int.from_bytes(data[:4], "big")
    if value == ConnectResponse.value:
        return ConnectResponse.from_bytes(data)
    if value == AnnounceResponse.value:
        return AnnounceResponse.from_bytes(data)
    raise ValueError("Unkown message identifier.")


# Protocol ---------------------------------------------------------------------


# TODO: Implement the async context manager protocol.
class Protocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()

        self._transport = None
        self._closed = asyncio.get_event_loop().create_future()
        self._exception = None

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

    async def read(self) -> Union[ConnectResponse, AnnounceResponse]:
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return parse(self._queue.popleft())

    def write(self, message: Union[ConnectRequest, AnnounceRequest]):
        # I'm omitting flow control because every write is followed by a read.
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


class TimeoutError(Exception):
    pass


# TODO: Instead of sending `None` if a timeout happens, thrown an exception.
def udp(params: _metadata.Parameters):
    connected = False
    for n in range(9):
        if not connected:
            transaction_id = secrets.token_bytes(4)
            received, time = yield (ConnectRequest(transaction_id), 15 * 2 ** n)
            if isinstance(received, ConnectResponse):
                connected = True
                connection_id = received.connection_id
                connection_time = time
        if connected:
            message = AnnounceRequest(transaction_id, connection_id, params)
            received, time = yield (message, 15 * 2 ** n)
            if isinstance(received, AnnounceResponse):
                return received.response
            if time - connection_time >= 60:
                connected = False
    raise TimeoutError("Maximal number of retries reached.")
