"""Tracker protocol.

This module provides the `request_peers_http` and `request_peers_udp` coroutine
functions that implement the original tracker protocol over HTTP and its
UDP-based variant.
"""

from typing import ClassVar, List

import asyncio
import collections
import contextlib
import dataclasses
import functools
import http.client
import secrets
import struct
import time
import urllib.parse

from . import bencoding


@dataclasses.dataclass
class Parameters:
    info_hash: bytes
    peer_id: bytes
    port: int = 0
    uploaded: int = 0
    downloaded: int = 0
    # This is initialized to 0 because we also need to connect to the tracker
    # before downloading the metadata.
    left: int = 0
    compact: int = 1  # See BEP 23.


@dataclasses.dataclass(frozen=True)
class Peer:
    address: str
    port: int

    @classmethod
    def from_bytes(cls, bs):
        return cls(".".join(str(b) for b in bs[:4]), int.from_bytes(bs[4:], "big"))

    @classmethod
    def from_dict(cls, d):
        return cls(d[b"ip"].decode(), d[b"port"])


def _parse_peers(raw_peers):
    peers = []
    for i in range(0, len(raw_peers), 6):
        peers.append(Peer.from_bytes(raw_peers[i : i + 6]))
    return peers


@dataclasses.dataclass
class Result:
    interval: int
    peers: List[Peer]

    @classmethod
    def from_bytes(cls, interval, raw_peers):
        return cls(interval, _parse_peers(raw_peers))

    @classmethod
    def from_dict(cls, resp):
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            peers = _parse_peers(resp[b"peers"])
        return cls(resp[b"interval"], peers)


def get(url, parameters):
    # I'm using `http.client` instead of the `urllib.request` wrapper here
    # because the latter is not thread-safe.
    q = urllib.parse.parse_qs(url.query)
    q.update(dataclasses.asdict(parameters))
    path = url._replace(scheme="", netloc="", query=urllib.parse.urlencode(q)).geturl()
    if url.scheme == "http":
        conn = http.client.HTTPConnection(url.netloc, timeout=30)
    elif url.scheme == "https":
        conn = http.client.HTTPSConnection(url.netloc, timeout=30)
    else:
        raise ValueError("Wrong scheme.")
    try:
        conn.request("GET", path)
        return conn.getresponse().read()
    finally:
        conn.close()


class UDPTrackerProtocol(asyncio.DatagramProtocol):
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

    async def read(self):
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return parse(self._queue.popleft())

    def write(self, message):
        # I'm omitting flow control because every write is followed by a read.
        self._transport.sendto(message.to_bytes())

    async def close(self):
        self._transport.close()
        await self._closed

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

    def datagram_received(self, data, addr):
        self._queue.append(data)
        self._wake_up()

    def error_received(self, exc):
        self._exception = exc
        self._wake_up(exc)


@contextlib.asynccontextmanager
async def create_udp_tracker_protocol(url):
    _, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
        functools.partial(UDPTrackerProtocol), remote_addr=(url.hostname, url.port)
    )
    try:
        yield protocol
    finally:
        await protocol.close()


async def request_peers_http(root, url, parameters):
    try:
        loop = asyncio.get_running_loop()
        while True:
            # I'm running the synchronous HTTP client from the standard library
            # in a separate thread here because HTTP requests only happen
            # sporadically and `aiohttp` is a hefty dependency.
            d = bencoding.decode(
                await loop.run_in_executor(
                    None, functools.partial(get, url, parameters)
                )
            )
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            result = Result.from_dict(d)
            for peer in result.peers:
                await root.put_peer(peer)
            await asyncio.sleep(result.interval)
    except Exception:
        pass
    finally:
        root.remove_tracker(asyncio.current_task())


@dataclasses.dataclass
class ConnectRequest:
    value: ClassVar[int] = 0
    transaction_id: bytes

    def to_bytes(self):
        return struct.pack(">ql4s", 0x41727101980, self.value, self.transaction_id)


@dataclasses.dataclass
class AnnounceRequest:
    value: ClassVar[int] = 1
    transaction_id: bytes
    connection_id: bytes
    parameters: Parameters

    def to_bytes(self):
        return struct.pack(
            ">8sl4s20s20sqqqlL4slH",
            self.connection_id,
            self.value,
            self.transaction_id,
            self.parameters.info_hash,
            self.parameters.peer_id,
            self.parameters.downloaded,
            self.parameters.left,
            self.parameters.uploaded,
            0,
            0,
            secrets.token_bytes(4),
            -1,
            self.parameters.port,
        )


@dataclasses.dataclass
class ConnectResponse:
    value: ClassVar[int] = 0
    connection_id: bytes

    @classmethod
    def from_bytes(cls, data):
        _, _, connection_id = struct.unpack(">l4s8s", data)
        return cls(connection_id)


@dataclasses.dataclass
class AnnounceResponse:
    value: ClassVar[int] = 1
    result: Result

    @classmethod
    def from_bytes(cls, data):
        _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])
        return cls(Result.from_bytes(interval, data[20:]))


def parse(data):
    if len(data) < 4:
        raise ValueError("Not enough bytes.")
    value = int.from_bytes(data[:4], "big")
    if value == ConnectResponse.value:
        return ConnectResponse.from_bytes(data)
    if value == AnnounceResponse.value:
        return AnnounceResponse.from_bytes(data)
    raise ValueError("Unkown message identifier.")


async def request_peers_udp(root, url, parameters):
    try:
        while True:
            async with create_udp_tracker_protocol(url) as protocol:
                connected = False
                for n in range(9):
                    timeout = 15 * 2 ** n
                    if not connected:
                        transaction_id = secrets.token_bytes(4)
                        protocol.write(ConnectRequest(transaction_id))
                        try:
                            received = await asyncio.wait_for(protocol.read(), timeout)
                        except asyncio.TimeoutError:
                            continue
                        if isinstance(received, ConnectResponse):
                            connected = True
                            connection_id = received.connection_id
                            connection_time = time.monotonic()
                    if connected:
                        protocol.write(
                            AnnounceRequest(transaction_id, connection_id, parameters)
                        )
                        try:
                            received = await asyncio.wait_for(protocol.read(), timeout)
                        except asyncio.TimeoutError:
                            continue
                        if isinstance(received, AnnounceResponse):
                            result = received.result
                            break
                        if time.monotonic() - connection_time >= 60:
                            connected = False
                else:
                    raise RuntimeError("Maximal number of retries reached.")
            for peer in result.peers:
                await root.put_peer(peer)
            await asyncio.sleep(result.interval)
    except Exception:
        pass
    finally:
        root.remove_tracker(asyncio.current_task())
