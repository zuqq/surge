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

    def __init__(self, trans_id: bytes):
        self.trans_id = trans_id

    def to_bytes(self) -> bytes:
        return struct.pack(">ql4s", 0x41727101980, self.value, self.trans_id)


class AnnounceRequest(Request):
    value = 1

    def __init__(self, trans_id: bytes, conn_id: bytes, params: metadata.Parameters):
        self.trans_id = trans_id
        self.conn_id = conn_id
        self.params = params

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">8sl4s20s20sqqqlL4slH",
            self.conn_id,
            self.value,
            self.trans_id,
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

    def __init__(self, conn_id: bytes):
        self.conn_id = conn_id

    @classmethod
    def from_bytes(cls, data: bytes) -> ConnectResponse:
        _, _, conn_id = struct.unpack(">l4s8s", data)
        return cls(conn_id)


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


class Protocol(state.StateMachineMixin, asyncio.DatagramProtocol):
    def __init__(self, params):
        super().__init__()

        self._params = params

        self._transport = None
        self._exc = None
        self._closed = asyncio.Event()

        self._trans_id = None

        self.resp = asyncio.get_event_loop().create_future()

        def connect_cb(message):
            # TODO: Check the returned `trans_id`.
            self._write(AnnounceRequest(self._trans_id, message.conn_id, self._params))

        def announce_cb(message):
            # TODO: Check the returned `conn_id`.
            self.resp.set_result(message.response)

        self._transition = {
            (Open, ConnectResponse): (connect_cb, Established),
            (Established, AnnounceResponse): (announce_cb, Established),
        }

    ### asyncio.BaseProtocol

    def connection_made(self, transport):
        self._state = Open
        self._transport = transport
        self._trans_id = secrets.token_bytes(4)
        self._write(ConnectRequest(self._trans_id))

    def connection_lost(self, exc):
        self._exc = self._exc or exc
        self._closed.set()

    ### asyncio.DatagramProtocol

    def datagram_received(self, data, addr):
        self._feed(parse(data))

    def error_received(self, exc):
        self._exc = self._exc or exc

    ### Interface

    def _write(self, message):
        self._transport.sendto(message.to_bytes())

    def close(self):
        if self._transport is not None:
            self._transport.close()

    async def wait_closed(self):
        await self._closed.wait()


class Open(Protocol):
    pass


class Established(Open):
    pass
