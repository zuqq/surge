from typing import Iterable, Optional, Set

import asyncio
import dataclasses
import functools
import logging
import urllib.parse

import aiohttp

from . import _udp
from . import metadata
from .. import actor
from .. import bencoding


class PeerQueue(actor.Supervisor):
    def __init__(self,
                 parent: Optional[actor.Actor],
                 params: metadata.Parameters,
                 announce_list: Iterable[str]):
        super().__init__(parent)

        self._announce_list = announce_list
        self._params = params

        self._peers = asyncio.Queue()  # type: ignore
        self._seen_peers: Set[metadata.Peer] = set()

    def __repr__(self):
        cls = self.__class__.__name__
        return f"<{cls} object at {hex(id(self))}>"

    ### Actor implementation

    async def _main(self):
        for announce in self._announce_list:
            url = urllib.parse.urlparse(announce)
            if url.scheme in ("http", "https"):
                await self.spawn_child(HTTPTrackerConnection(self, self._params, url))
            elif url.scheme == "udp":
                await self.spawn_child(UDPTrackerConnection(self, self._params, url))
            else:
                logging.warning("%r is invalid", announce)

    async def _on_child_crash(self, child):
        pass

    ### Queue interface

    async def get(self) -> metadata.Peer:
        """Return a fresh peer."""
        return await self._peers.get()

    def put_nowait(self, peer: metadata.Peer):
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        self._peers.put_nowait(peer)


class _BaseTrackerConnection(actor.Actor):
    def __init__(self,
                 parent: PeerQueue,
                 params: metadata.Parameters,
                 url: urllib.parse.ParseResult):
        super().__init__(parent)

        self._url = url
        self._params = params

    def __repr__(self):
        cls = self.__class__.__name__
        url = f"url={repr(self._url.geturl())}"
        return f"<{cls} object at {hex(id(self))} with {url}>"


class HTTPTrackerConnection(_BaseTrackerConnection):
    async def _main(self):
        while True:
            params = urllib.parse.parse_qs(self._url.query)
            params.update(dataclasses.asdict(self._params))
            url = self._url._replace(query=urllib.parse.urlencode(params))
            async with aiohttp.ClientSession() as session:
                async with session.get(url.geturl()) as request:
                    raw_response = await request.read()
            d = bencoding.decode(raw_response)
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            response = metadata.Response.from_dict(d)
            for peer in response.peers:
                self.parent.put_nowait(peer)
            await asyncio.sleep(response.interval)


class UDPTrackerConnection(_BaseTrackerConnection):
    async def _main(self):
        while True:
            for i in range(9):  # See BEP 15, 'Time outs'.
                try:
                    loop = asyncio.get_running_loop()
                    _, protocol = await loop.create_datagram_endpoint(
                        functools.partial(_udp.Protocol, self._params),
                        remote_addr=(self._url.hostname, self._url.port),
                    )
                    response = await asyncio.wait_for(protocol.request(), timeout=5)
                except Exception as e:
                    logging.warning("%r failed with %r", self._url.geturl(), e)
                    await asyncio.sleep(15 * 2 ** i - 5)
                else:
                    break
            else:
                raise ConnectionError("Tracker unreachable.")
            for peer in response.peers:
                self.parent.put_nowait(peer)
            await asyncio.sleep(response.interval)
