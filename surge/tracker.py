"""Implementation of the tracker protocol.

Specification: [BEP 0003], [BEP 0015], [BEP 0023]

This module provides implementations of the HTTP and UDP tracker protocols;
they are exposed via `Trackers`.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0015]: http://bittorrent.org/beps/bep_0015.html
[BEP 0023]: http://bittorrent.org/beps/bep_0023.html
"""

from __future__ import annotations

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
from collections.abc import AsyncGenerator, Iterable
from types import TracebackType
from typing import Any, ClassVar, Self

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
    compact: int = 1


@dataclasses.dataclass(frozen=True)
class Peer:
    address: str
    port: int

    @classmethod
    def from_bytes(cls, bs: bytes) -> Self:
        return cls(".".join(str(b) for b in bs[:4]), int.from_bytes(bs[4:], "big"))

    @classmethod
    def from_dict(cls, d: dict[bytes, Any]) -> Self:
        return cls(d[b"ip"].decode(), d[b"port"])


def _parse_peers(raw_peers: bytes) -> list[Peer]:
    peers: list[Peer] = []
    for i in range(0, len(raw_peers), 6):
        peers.append(Peer.from_bytes(raw_peers[i : i + 6]))
    return peers


@dataclasses.dataclass
class Result:
    interval: int
    peers: list[Peer]

    @classmethod
    def from_bytes(cls, interval: int, raw_peers: bytes) -> Self:
        return cls(interval, _parse_peers(raw_peers))

    @classmethod
    def from_dict(cls, resp: dict[bytes, Any]) -> Self:
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            peers = _parse_peers(resp[b"peers"])
        return cls(resp[b"interval"], peers)


def get(url: urllib.parse.ParseResult, parameters: Parameters) -> bytes:
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
    _transport: asyncio.DatagramTransport

    def __init__(self) -> None:
        super().__init__()

        self._closed: asyncio.Future[None] = asyncio.get_event_loop().create_future()

        self._exception: BaseException | None = None
        self._queue: collections.deque[bytes] = collections.deque(maxlen=10)
        self._waiter: asyncio.Future[None] | None = None

    def _wake_up(self, exc: BaseException | None = None) -> None:
        if (waiter := self._waiter) is None:
            return
        self._waiter = None
        if waiter.done():
            return
        if exc is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(exc)

    async def read(self) -> UDPResponse:
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return parse_udp_message(self._queue.popleft())

    def write(self, message: UDPRequest) -> None:
        # I'm omitting flow control because every write is followed by a read.
        self._transport.sendto(message.to_bytes())

    async def close(self) -> None:
        self._transport.close()
        await self._closed

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport

    def connection_lost(self, exc: Exception | None) -> None:
        if not self._transport.is_closing():
            if exc is None:
                peer = self._transport.get_extra_info("peername")
                exc = ConnectionError(f"Unexpected EOF {peer}.")
            self._exception = exc
            self._wake_up(exc)
        if not self._closed.done():
            self._closed.set_result(None)

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        self._queue.append(data)
        self._wake_up()

    def error_received(self, exc: Exception) -> None:
        self._exception = exc
        self._wake_up(exc)


@contextlib.asynccontextmanager
async def create_udp_tracker_protocol(
    url: urllib.parse.ParseResult,
) -> AsyncGenerator[UDPTrackerProtocol, None]:
    loop = asyncio.get_running_loop()
    if url.hostname is None:
        raise ValueError("Missing 'hostname'.")
    if url.port is None:
        raise ValueError("Missing 'port'.")
    _, protocol = await loop.create_datagram_endpoint(
        UDPTrackerProtocol, remote_addr=(url.hostname, url.port)
    )
    try:
        yield protocol
    finally:
        await protocol.close()


async def request_peers_http(
    root: Trackers, url: urllib.parse.ParseResult, parameters: Parameters
) -> None:
    loop = asyncio.get_running_loop()
    while True:
        # I'm running the synchronous HTTP client from the standard library
        # in a separate thread here because HTTP requests only happen
        # sporadically and `aiohttp` is a hefty dependency.
        d: Any = bencoding.decode(
            await loop.run_in_executor(None, functools.partial(get, url, parameters))
        )
        if b"failure reason" in d:
            raise ConnectionError(d[b"failure reason"].decode())
        result = Result.from_dict(d)
        for peer in result.peers:
            await root.put_peer(peer)
        await asyncio.sleep(result.interval)


@dataclasses.dataclass
class UDPConnectRequest:
    value: ClassVar[int] = 0
    transaction_id: bytes

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">ql4s",
            0x41727101980,
            self.value,
            self.transaction_id,
        )


@dataclasses.dataclass
class UDPAnnounceRequest:
    value: ClassVar[int] = 1
    transaction_id: bytes
    connection_id: bytes
    parameters: Parameters

    def to_bytes(self) -> bytes:
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


type UDPRequest = UDPConnectRequest | UDPAnnounceRequest


@dataclasses.dataclass
class UDPConnectResponse:
    value: ClassVar[int] = 0
    connection_id: bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        _, _, connection_id = struct.unpack(">l4s8s", data)
        return cls(connection_id)


@dataclasses.dataclass
class UDPAnnounceResponse:
    value: ClassVar[int] = 1
    result: Result

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])
        return cls(Result.from_bytes(interval, data[20:]))


type UDPResponse = UDPConnectResponse | UDPAnnounceResponse


def parse_udp_message(data: bytes) -> UDPResponse:
    if len(data) < 4:
        raise ValueError("Not enough bytes.")
    value = int.from_bytes(data[:4], "big")
    if value == UDPConnectResponse.value:
        return UDPConnectResponse.from_bytes(data)
    if value == UDPAnnounceResponse.value:
        return UDPAnnounceResponse.from_bytes(data)
    raise ValueError("Unknown message identifier.")


@dataclasses.dataclass
class UDPConnection:
    transaction_id: bytes
    connection_id: bytes
    established: float


async def request_peers_udp(
    root: Trackers, url: urllib.parse.ParseResult, parameters: Parameters
) -> None:
    while True:
        async with create_udp_tracker_protocol(url) as protocol:
            connection: UDPConnection | None = None
            for n in range(9):
                timeout = 15 * 2**n
                if connection is None:
                    transaction_id = secrets.token_bytes(4)
                    protocol.write(UDPConnectRequest(transaction_id))
                    try:
                        received = await asyncio.wait_for(protocol.read(), timeout)
                    except TimeoutError:
                        continue
                    if isinstance(received, UDPConnectResponse):
                        connection = UDPConnection(
                            transaction_id, received.connection_id, time.monotonic()
                        )
                if connection is not None:
                    protocol.write(
                        UDPAnnounceRequest(
                            connection.transaction_id,
                            connection.connection_id,
                            parameters,
                        )
                    )
                    try:
                        received = await asyncio.wait_for(protocol.read(), timeout)
                    except TimeoutError:
                        continue
                    if isinstance(received, UDPAnnounceResponse):
                        result = received.result
                        break
                    if time.monotonic() - connection.established >= 60:
                        connection = None
            else:
                raise ConnectionError("Maximal number of retries reached.")
        for peer in result.peers:
            await root.put_peer(peer)
        await asyncio.sleep(result.interval)


async def request_peers(
    root: Trackers, url: urllib.parse.ParseResult, parameters: Parameters
) -> None:
    try:
        if url.scheme in ("http", "https"):
            return await request_peers_http(root, url, parameters)
        if url.scheme == "udp":
            return await request_peers_udp(root, url, parameters)
        raise ValueError("Invalid scheme.")
    finally:
        root.tracker_disconnected(url)


class Trackers:
    def __init__(
        self,
        info_hash: bytes,
        peer_id: bytes,
        announce_list: Iterable[str],
        max_peers: int,
    ) -> None:
        self._parameters = Parameters(info_hash, peer_id)
        self._announce_list = announce_list
        self._new_peers: asyncio.Queue[Peer] = asyncio.Queue(max_peers)
        self._seen_peers: set[Peer] = set()
        self._trackers: dict[urllib.parse.ParseResult, asyncio.Task[None]] = {}

    async def __aenter__(self) -> Self:
        for url in map(urllib.parse.urlparse, self._announce_list):
            self._trackers[url] = asyncio.create_task(
                request_peers(self, url, self._parameters)
            )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        tasks = self._trackers.values()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def connected_trackers(self) -> int:
        """The number of connected trackers."""
        return len(self._trackers)

    async def get_peer(self) -> Peer:
        return await self._new_peers.get()

    async def put_peer(self, peer: Peer) -> None:
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        await self._new_peers.put(peer)

    def tracker_disconnected(self, url: urllib.parse.ParseResult) -> None:
        self._trackers.pop(url)
