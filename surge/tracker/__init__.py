from typing import Iterable, Optional, Set

import asyncio
import dataclasses
import functools
import logging
import time
import urllib.parse
import urllib.request

from . import _udp
from .. import actor
from .. import bencoding
from ._metadata import Parameters, Peer, Response


__all__ = ("Parameters", "Peer", "PeerQueue")


class PeerQueue(actor.Actor):
    def __init__(
            self,
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

    async def put(self, peer: Peer):
        if peer in self._seen:
            return
        self._seen.add(peer)
        await self._peers.put(peer)


def get(url):
    with urllib.request.urlopen(url) as f:
        return f.read()


class HTTPTrackerConnection(actor.Actor):
    def __init__(
            self,
            parent: PeerQueue,
            url: urllib.parse.ParseResult,
            params: Parameters):
        super().__init__(parent)
        self._coros.add(self._main(params))

        self.url = url

    def __repr__(self):
        class_name = self.__module__ + '.' + self.__class__.__qualname__
        url = self.url.geturl()
        return f"<{class_name} with url={repr(url)}>"

    async def _main(self, params):
        loop = asyncio.get_running_loop()
        while True:
            ps = urllib.parse.parse_qs(self.url.query)
            ps.update(dataclasses.asdict(params))
            q = urllib.parse.urlencode(ps)
            d = bencoding.decode(
                await loop.run_in_executor(
                    None, functools.partial(get, self.url._replace(query=q).geturl())
                )
            )
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            response = Response.from_dict(d)
            logging.info("%r received %r peers", self, len(response.peers))
            for peer in response.peers:
                await self.parent.put(peer)
            await asyncio.sleep(response.interval)


class UDPTrackerConnection(actor.Actor):
    def __init__(
            self,
            parent: PeerQueue,
            url: urllib.parse.ParseResult,
            params: Parameters):
        super().__init__(parent)
        self._coros.add(self._main(params))

        self.url = url

    def __repr__(self):
        class_name = self.__module__ + '.' + self.__class__.__qualname__
        address = hex(id(self))
        url = self.url.geturl()
        return f"<{class_name} object at {address} with url={repr(url)}>"

    async def _main(self, params):
        loop = asyncio.get_running_loop()
        while True:
            _, protocol = await loop.create_datagram_endpoint(
                functools.partial(_udp.Protocol),
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
