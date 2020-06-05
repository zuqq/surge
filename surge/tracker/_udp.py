from __future__ import annotations

import asyncio
import secrets
import struct

from . import metadata
from .. import state


class Request:
    def to_bytes(self) -> bytes:
        raise NotImplementedError


class Response:
    @classmethod
    def from_bytes(cls, _: bytes) -> Response:
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
            secrets.token_bytes(4),  # TODO: Semantics of this value?
            -1,
            6881,
        )


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
    value = int.from_bytes(data[:4], "big")
    if value == ConnectResponse.value:
        return ConnectResponse.from_bytes(data)
    if value == AnnounceResponse.value:
        return AnnounceResponse.from_bytes(data)
    raise ValueError(data)


class Closed(state.StateMachineMixin, asyncio.DatagramProtocol):
    def __init__(self, params):
        super().__init__()

        self._transport = None
        self._exception = None
        self._closed = None

        self._transaction_id = None
        self.response = asyncio.get_event_loop().create_future()

        def on_connect(message):
            # TODO: Check the returned `transaction_id`.
            self._write(
                AnnounceRequest(self._transaction_id, message.connection_id, params)
            )

        def on_announce(message):
            # TODO: Check the returned `connection_id`.
            self.response.set_result(message.response)

        self._transition = {
            (Open, ConnectResponse): (on_connect, Established),
            (Established, AnnounceResponse): (on_announce, Established),
        }

    ### asyncio.BaseProtocol

    def connection_made(self, transport):
        self._set_state(Open)
        self._transport = transport
        self._closed = asyncio.get_event_loop().create_future()
        self._transaction_id = secrets.token_bytes(4)
        self._write(ConnectRequest(self._transaction_id))

    def connection_lost(self, exc):
        if self._exception is None:
            self._exception = exc
        self._set_state(Protocol)
        self._transport = None
        if self._closed is not None:
            self._closed.set_result(None)

    ### asyncio.DatagramProtocol

    def datagram_received(self, data, addr):
        self.feed(parse(data))

    def error_received(self, exc):
        if self._exception is None:
            self._exception = exc

    ### Stream

    def _write(self, message):
        raise ConnectionError("Writing to closed connection.")

    ### Interface

    async def close(self):
        if self._transport is not None:
            self._transport.close()
        if self._closed is not None:
            await self._closed


class Open(Closed):
    def _write(self, message):
        self._transport.sendto(message.to_bytes())


class Established(Open):
    pass


class Protocol(Closed):
    def __init__(self, params):
        super().__init__(params)

        self._set_state(Closed)
