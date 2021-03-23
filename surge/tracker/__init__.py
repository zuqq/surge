from typing import Optional, Union

import asyncio
import collections
import contextlib
import dataclasses
import functools
import http.client
import secrets
import time
import urllib.parse

from . import messages
from .. import bencoding
from ._metadata import Parameters, Peer, Result


__all__ = ("Parameters", "Peer", "Result", "request_peers_http", "request_peers_udp")


def get(url: urllib.parse.ParseResult, parameters: Parameters) -> bytes:
    # This uses `http.client` instead of the `urllib.request` wrapper because
    # the latter is not thread-safe.
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

    async def read(self) -> messages.Response:
        if self._exception is not None:
            raise self._exception
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiter = waiter
            await waiter
        return messages.parse(self._queue.popleft())

    def write(self, message: messages.Request) -> None:
        # I'm omitting flow control because every write is followed by a read.
        self._transport.sendto(message.to_bytes())

    async def close(self) -> None:
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


async def request_peers_udp(root, url, parameters):
    try:
        while True:
            async with create_udp_tracker_protocol(url) as protocol:
                connected = False
                for n in range(9):
                    timeout = 15 * 2 ** n
                    if not connected:
                        transaction_id = secrets.token_bytes(4)
                        protocol.write(messages.ConnectRequest(transaction_id))
                        try:
                            received = await asyncio.wait_for(protocol.read(), timeout)
                        except asyncio.TimeoutError:
                            continue
                        if isinstance(received, messages.ConnectResponse):
                            connected = True
                            connection_id = received.connection_id
                            connection_time = time.monotonic()
                    if connected:
                        protocol.write(
                            messages.AnnounceRequest(
                                transaction_id, connection_id, parameters
                            )
                        )
                        try:
                            received = await asyncio.wait_for(protocol.read(), timeout)
                        except asyncio.TimeoutError:
                            continue
                        if isinstance(received, messages.AnnounceResponse):
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
