"""Actors for the tracker protocols.

This package contains support for two protocols: the original tracker protocol
over HTTP and its UDP-based variant.

An instance of `HTTPTrackerConnection` or `UDPTrackerConnection` represents a
connection with a single tracker; `PeerQueue` manages multiple connections and
exposes an `asyncio.Queue`-like interface for the received peers.
"""

from typing import Iterable, Optional, Set

import asyncio
import collections
import dataclasses
import functools
import http.client
import logging
import time
import urllib.parse

from . import _udp
from .. import actor
from .. import bencoding
from ._metadata import Parameters, Peer, Response


__all__ = ("Parameters", "Peer", "PeerQueue")


class PeerQueue(actor.Actor):
    def __init__(self,
                 parent: Optional[actor.Actor],
                 announce_list: Iterable[str],
                 params: Parameters):
        super().__init__(parent)
        for announce in announce_list:
            url = urllib.parse.urlparse(announce)
            if url.scheme in ("http", "https"):
                self.children.add(HTTPTrackerConnection(self, url, params))
            elif url.scheme == "udp":
                self.children.add(UDPTrackerConnection(self, url, params))
            else:
                logging.warning("%r is invalid", announce)

        self._peers = asyncio.Queue(len(self.children) * 200)  # type: ignore
        self._seen: Set[Peer] = set()

    def _on_child_crash(self, child):
        pass

    async def get(self) -> Peer:
        """Return a fresh peer."""
        return await self._peers.get()

    async def put(self, peer: Peer) -> None:
        if peer in self._seen:
            return
        self._seen.add(peer)
        await self._peers.put(peer)


def get(url: urllib.parse.ParseResult, params: Parameters) -> bytes:
    # This uses `http.client` instead of the `urllib.request` wrapper because
    # the latter is not thread-safe.
    q = urllib.parse.parse_qs(url.query)
    q.update(dataclasses.asdict(params))
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


class HTTPTrackerConnection(actor.Actor):
    def __init__(self,
                 parent: PeerQueue,
                 url: urllib.parse.ParseResult,
                 params: Parameters):
        super().__init__(parent)
        self._coros.add(self._main(params))

        self.url = url

    def __repr__(self):
        class_name = self.__module__ + "." + self.__class__.__qualname__
        url = self.url.geturl()
        return f"<{class_name} with url={repr(url)}>"

    async def _main(self, params):
        loop = asyncio.get_running_loop()
        while True:
            # I'm running a synchronous HTTP client in a separate thread here
            # because HTTP requests only happen sporadically.
            d = bencoding.decode(
                await loop.run_in_executor(
                    None, functools.partial(get, self.url, params)
                )
            )
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            response = Response.from_dict(d)
            logging.info("%r received %r peers", self, len(response.peers))
            for peer in response.peers:
                await self.parent.put(peer)
            await asyncio.sleep(response.interval)


# TODO: Implement the async context manager protocol.
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

    async def read(self) -> _udp.Response:
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return _udp.parse(self._queue.popleft())

    def write(self, message: _udp.Request) -> None:
        # I'm omitting flow control because every write is followed by a read.
        self._transport.sendto(message.to_bytes())

    async def close(self) -> None:
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


class UDPTrackerConnection(actor.Actor):
    def __init__(self,
                 parent: PeerQueue,
                 url: urllib.parse.ParseResult,
                 params: Parameters):
        super().__init__(parent)
        self._coros.add(self._main(params))

        self.url = url

    def __repr__(self):
        class_name = self.__module__ + "." + self.__class__.__qualname__
        address = hex(id(self))
        url = self.url.geturl()
        return f"<{class_name} object at {address} with url={repr(url)}>"

    async def _main(self, params):
        loop = asyncio.get_running_loop()
        while True:
            _, protocol = await loop.create_datagram_endpoint(
                functools.partial(UDPTrackerProtocol),
                remote_addr=(self.url.hostname, self.url.port),
            )
            try:
                transducer = _udp.udp(params)
                message, timeout = transducer.send(None)
                while True:
                    protocol.write(message)
                    try:
                        received = await asyncio.wait_for(protocol.read(), timeout)
                    except asyncio.TimeoutError:
                        received = None
                    message, timeout = transducer.send((received, time.monotonic()))
            except StopIteration as exc:
                response = exc.value
            finally:
                await protocol.close()
            logging.info("%r received %r peers", self, len(response.peers))
            for peer in response.peers:
                await self.parent.put(peer)
            await asyncio.sleep(response.interval)
