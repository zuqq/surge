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
        while True:
            ps = urllib.parse.parse_qs(self.url.query)
            ps.update(dataclasses.asdict(params))
            url = self.url._replace(query=urllib.parse.urlencode(ps))
            async with aiohttp.ClientSession() as session:
                async with session.get(url.geturl()) as request:
                    raw_response = await request.read()
            d = bencoding.decode(raw_response)
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
        while True:
            response = await _udp.request(self.url, params)
            logging.info("%r received %r peers", self, len(response.peers))
            for peer in response.peers:
                await self.parent.put(peer)
            await asyncio.sleep(response.interval)
