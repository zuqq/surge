from typing import Iterable, Optional, Set

import asyncio
import dataclasses
import functools
import logging
import urllib.parse

import aiohttp

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

        self._peers = asyncio.Queue(200)  # type: ignore
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


class HTTPTrackerConnection(actor.Actor):
    def __init__(
            self,
            parent: PeerQueue,
            url: urllib.parse.ParseResult,
            params: Parameters):
        super().__init__(parent)

        self._coros.add(self._main(url, params))

    async def _main(self, url, params):
        while True:
            d = urllib.parse.parse_qs(url.query)
            d.update(dataclasses.asdict(params))
            url = url._replace(query=urllib.parse.urlencode(d))
            async with aiohttp.ClientSession() as session:
                async with session.get(url.geturl()) as request:
                    raw_response = await request.read()
            d = bencoding.decode(raw_response)
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            response = Response.from_dict(d)
            logging.debug("%r received %r peers", self, len(response.peers))
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

        self._coros.add(self._main(url, params))

    async def _main(self, url, params):
        while True:
            for i in range(9):  # See BEP 15, "Time outs".
                try:
                    loop = asyncio.get_running_loop()
                    _, protocol = await loop.create_datagram_endpoint(
                        functools.partial(_udp.Protocol, params),
                        remote_addr=(url.hostname, url.port),
                    )
                    response = await asyncio.wait_for(protocol.request(), 5)
                except Exception as e:
                    logging.warning("%r failed with %r", url.geturl(), e)
                    await asyncio.sleep(15 * 2 ** i - 5)
                else:
                    break
            else:
                raise ConnectionError("Tracker unreachable.")
            logging.debug("%r received %r peers", self, len(response.peers))
            for peer in response.peers:
                await self.parent.put(peer)
            await asyncio.sleep(response.interval)
