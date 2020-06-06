from typing import List, Set

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
    def __init__(self, announce_list: List[str], params: metadata.Parameters):
        super().__init__()

        self._announce_list = announce_list
        self._params = params

        self._peers = asyncio.Queue()  # type: ignore
        self._seen_peers = set()  # type: Set[metadata.Peer]

    def __repr__(self):
        cls = self.__class__.__name__
        return f"<{cls} object at {hex(id(self))}>"

    ### Actor implementation

    async def _main(self):
        for announce in self._announce_list:
            url = urllib.parse.urlparse(announce)
            if url.scheme in ("http", "https"):
                await self.spawn_child(HTTPTrackerConnection(url, self._params))
            elif url.scheme == "udp":
                await self.spawn_child(UDPTrackerConnection(url, self._params))
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
                 url: urllib.parse.ParseResult,
                 params: metadata.Parameters,
                 *,
                 max_tries: int = 5):
        super().__init__()

        self._url = url
        self._params = params
        self._max_tries = max_tries

    def __repr__(self):
        cls = self.__class__.__name__
        info = [f"url={repr(self._url.geturl())}"]
        return f"<{cls} object at {hex(id(self))} with {', '.join(info)}>"


class HTTPTrackerConnection(_BaseTrackerConnection):
    async def _main(self):
        while True:
            params = urllib.parse.parse_qs(self._url.query)
            params.update(dataclasses.asdict(self._params))
            req_url = self._url._replace(query=urllib.parse.urlencode(params))
            async with aiohttp.ClientSession() as session:
                async with session.get(req_url.geturl()) as req:
                    raw_resp = await req.read()
            d = bencoding.decode(raw_resp)
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            resp = metadata.Response.from_dict(d)
            for peer in resp.peers:
                self.parent.put_nowait(peer)
            await asyncio.sleep(resp.interval)


class UDPTrackerConnection(_BaseTrackerConnection):
    async def _main(self):
        while True:
            tries = 0
            while tries < self._max_tries:
                try:
                    loop = asyncio.get_running_loop()
                    _, protocol = await loop.create_datagram_endpoint(
                        functools.partial(_udp.Protocol, self._params),
                        remote_addr=(self._url.hostname, self._url.port),
                    )
                    resp = await asyncio.wait_for(protocol.response, timeout=5)
                except Exception as e:
                    logging.warning("%r failed with %r", self._url.geturl(), e)
                    tries += 1
                else:
                    break
            else:
                raise ConnectionError("Tracker unreachable.")
            for peer in resp.peers:
                self.parent.put_nowait(peer)
            await asyncio.sleep(resp.interval)
